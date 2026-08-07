using System.Text.Json;

namespace CodexProviderSync.Core.Tests;

public sealed class TransactionJournalTests
{
    [Fact]
    public async Task AppendAsync_FailureBeforeWriteDoesNotConsumeSequence()
    {
        JournalFixture fixture = await JournalFixture.CreateAsync(1);
        bool fail = true;
        fixture.Journal.AppendFaultInjector = (phase, state) =>
        {
            if (fail && phase == "before-write" && state == "applying")
            {
                fail = false;
                throw new IOException("injected before write");
            }
            return Task.CompletedTask;
        };

        await Assert.ThrowsAsync<IOException>(
            () => fixture.Journal.ApplyingAsync("rollout", fixture.Targets[0]));
        PendingTransactionInfo prepared = await fixture.Journal.ReadCurrentInfoAsync();
        Assert.Equal(1, prepared.LastSequence);
        Assert.Equal("prepared", prepared.State);

        fixture.Journal.AppendFaultInjector = null;
        await fixture.Journal.ApplyingAsync("rollout", fixture.Targets[0]);
        await fixture.Journal.AppliedAsync("rollout", fixture.Targets[0]);
        await fixture.Journal.CommittedAsync();

        PendingTransactionInfo committed = await fixture.Journal.ReadCurrentInfoAsync();
        Assert.Equal(4, committed.LastSequence);
        Assert.True(committed.Terminal);
    }

    [Fact]
    public async Task AppendAsync_FailureAfterReadableWriteResynchronizesSequence()
    {
        JournalFixture fixture = await JournalFixture.CreateAsync(1);
        fixture.Journal.AppendFaultInjector = (phase, state) =>
        {
            if (phase == "after-write-before-flush" && state == "applying")
            {
                throw new IOException("write reported failure after bytes became readable");
            }
            return Task.CompletedTask;
        };

        await Assert.ThrowsAsync<IOException>(
            () => fixture.Journal.ApplyingAsync("rollout", fixture.Targets[0]));
        PendingTransactionInfo applying = await fixture.Journal.ReadCurrentInfoAsync();
        Assert.Equal(2, applying.LastSequence);
        Assert.Equal("applying", applying.State);

        fixture.Journal.AppendFaultInjector = null;
        await fixture.Journal.AppliedAsync("rollout", fixture.Targets[0]);
        await fixture.Journal.CommittedAsync();
        Assert.True((await fixture.Journal.ReadCurrentInfoAsync()).Terminal);
    }

    [Fact]
    public async Task AppendAsync_ResynchronizesFromValidExternallyAppendedRecord()
    {
        JournalFixture fixture = await JournalFixture.CreateAsync(1);
        PendingTransactionInfo prepared = await fixture.Journal.ReadCurrentInfoAsync();
        string applying = JsonSerializer.Serialize(new
        {
            protocolVersion = 1,
            operationId = prepared.OperationId,
            sequence = 2,
            state = "applying",
            recordedAt = DateTimeOffset.UtcNow,
            kind = "rollout",
            targetPath = Path.GetFullPath(fixture.Targets[0])
        });
        await File.AppendAllTextAsync(fixture.Journal.FilePath, applying + "\n");

        await fixture.Journal.AppliedAsync("rollout", fixture.Targets[0]);
        PendingTransactionInfo applied = await fixture.Journal.ReadCurrentInfoAsync();
        Assert.Equal(3, applied.LastSequence);
        Assert.Equal("applied", applied.State);
        Assert.False(applied.InvalidTail);
        Assert.Equal(1, fixture.Journal.AppendFullJournalValidationCount);
    }

    [Fact]
    public async Task NormalProgressAppends_DoNotReparseGrowingJournal()
    {
        JournalFixture fixture = await JournalFixture.CreateAsync(32, owned: true);

        foreach (string target in fixture.Targets)
        {
            await fixture.Journal.ApplyingAsync("rollout", target);
            await fixture.Journal.AppliedAsync("rollout", target);
        }

        Assert.Equal(
            OperatingSystem.IsWindows() ? 0 : 64,
            fixture.Journal.AppendFullJournalValidationCount);
        await fixture.Journal.CommittedAsync();
        Assert.Equal(
            OperatingSystem.IsWindows() ? 2 : 66,
            fixture.Journal.AppendFullJournalValidationCount);

        PendingTransactionInfo committed = await fixture.Journal.ReadCurrentInfoAsync();
        Assert.True(committed.Terminal);
        Assert.Equal(66, committed.LastSequence);
        Assert.All(committed.AffectedTargets, target => Assert.Equal("applied", target.State));
    }

    [Fact]
    public async Task AppendAsync_RejectsSameLengthInvalidTailBeforeWriting()
    {
        JournalFixture fixture = await JournalFixture.CreateAsync(1);
        byte[] before = await File.ReadAllBytesAsync(fixture.Journal.FilePath);
        Assert.Equal((byte)'\n', before[^1]);
        before[^1] = (byte)' ';
        await File.WriteAllBytesAsync(fixture.Journal.FilePath, before);
        long invalidLength = new FileInfo(fixture.Journal.FilePath).Length;

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Journal.ApplyingAsync("rollout", fixture.Targets[0]));

        Assert.Contains("invalid", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(invalidLength, new FileInfo(fixture.Journal.FilePath).Length);
        Assert.Equal(1, fixture.Journal.AppendFullJournalValidationCount);
        Assert.True((await fixture.Journal.ReadCurrentInfoAsync()).InvalidTail);
    }

    [Fact]
    public async Task AppendAsync_RejectsSameLengthPreparedRewriteBeforeWriting()
    {
        JournalFixture fixture = await JournalFixture.CreateAsync(1);
        string originalTarget = Path.GetFullPath(fixture.Targets[0]);
        string before = await File.ReadAllTextAsync(fixture.Journal.FilePath);
        string rewritten = before.Replace("rollout-0.jsonl", "rollout-0.jsonx", StringComparison.Ordinal);
        Assert.Equal(before.Length, rewritten.Length);
        Assert.NotEqual(before, rewritten);
        await File.WriteAllTextAsync(fixture.Journal.FilePath, rewritten);
        long rewrittenLength = new FileInfo(fixture.Journal.FilePath).Length;

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Journal.ApplyingAsync("rollout", originalTarget));

        Assert.Contains("prepared", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(rewrittenLength, new FileInfo(fixture.Journal.FilePath).Length);
    }

    [Fact]
    public async Task OwnedJournal_BlocksExternalSameLengthRewriteOnWindows()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }
        JournalFixture fixture = await JournalFixture.CreateAsync(1, owned: true);
        await Assert.ThrowsAsync<IOException>(
            async () =>
            {
                await using FileStream _ = new(
                    fixture.Journal.FilePath,
                    FileMode.Open,
                    FileAccess.Write,
                    FileShare.ReadWrite | FileShare.Delete);
            });
        await fixture.Journal.ApplyingAsync("rollout", fixture.Targets[0]);
    }

    [Fact]
    public async Task AppendAsync_ValidatesTargetTransitionBeforeWriting()
    {
        JournalFixture fixture = await JournalFixture.CreateAsync(1);
        string before = await File.ReadAllTextAsync(fixture.Journal.FilePath);

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Journal.AppliedAsync("rollout", fixture.Targets[0]));

        Assert.Contains("must be applying", error.Message);
        Assert.Equal(before, await File.ReadAllTextAsync(fixture.Journal.FilePath));
    }

    [Fact]
    public async Task AppendAsync_SerializesConcurrentAppendsAgainstFreshJournalState()
    {
        JournalFixture fixture = await JournalFixture.CreateAsync(2);

        await Task.WhenAll(
            fixture.Journal.ApplyingAsync("rollout", fixture.Targets[0]),
            fixture.Journal.ApplyingAsync("rollout", fixture.Targets[1]));

        PendingTransactionInfo info = await fixture.Journal.ReadCurrentInfoAsync();
        Assert.Equal(3, info.LastSequence);
        Assert.Equal(2, info.AffectedTargets.Count(static target => target.State == "applying"));
        Assert.False(info.InvalidTail);
    }

    [Fact]
    public async Task TerminalAppend_ReReadsJournalAndRejectsConcurrentTail()
    {
        JournalFixture fixture = await JournalFixture.CreateAsync(0);
        fixture.Journal.AppendFaultInjector = async (phase, state) =>
        {
            if (phase != "after-flush-before-verify" || state != "committed")
            {
                return;
            }
            PendingTransactionInfo committed = await FileTransactionJournal.ReadInfoAsync(
                fixture.Journal.FilePath);
            string impossibleTail = JsonSerializer.Serialize(new
            {
                protocolVersion = 1,
                operationId = committed.OperationId,
                sequence = committed.LastSequence + 1,
                state = "rollingBack",
                recordedAt = DateTimeOffset.UtcNow,
                originalError = "forged after commit"
            });
            await File.AppendAllTextAsync(fixture.Journal.FilePath, impossibleTail + "\n");
        };

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Journal.CommittedAsync());

        Assert.Contains("could not be verified", error.Message);
        PendingTransactionInfo pending = await fixture.Journal.ReadCurrentInfoAsync();
        Assert.True(pending.InvalidTail);
        Assert.False(pending.Terminal);
    }

    [Fact]
    public async Task CommittedAppend_ApiFailureAfterDurableWriteReconcilesWithoutRollback()
    {
        JournalFixture fixture = await JournalFixture.CreateAsync(0);
        fixture.Journal.AppendFaultInjector = (phase, state) =>
            phase == "after-flush-before-verify" && state == "committed"
                ? Task.FromException(new IOException("injected post-flush reporting failure"))
                : Task.CompletedTask;

        await fixture.Journal.CommittedAsync();

        PendingTransactionInfo committed = await fixture.Journal.ReadCurrentInfoAsync();
        Assert.True(committed.Terminal);
        Assert.Equal("committed", committed.State);
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Journal.RollingBackAsync(new IOException("must not roll back")));
    }

    [Fact]
    public async Task CommittedJournal_RejectsRollbackWithoutAppendingOrLosingTerminalState()
    {
        JournalFixture fixture = await JournalFixture.CreateAsync(0);
        await fixture.Journal.CommittedAsync();
        string committedBytes = await File.ReadAllTextAsync(fixture.Journal.FilePath);

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Journal.RollingBackAsync(new IOException("too late")));

        Assert.Contains("already terminal", error.Message);
        Assert.Equal(committedBytes, await File.ReadAllTextAsync(fixture.Journal.FilePath));
        PendingTransactionInfo committed = await fixture.Journal.ReadCurrentInfoAsync();
        Assert.True(committed.Terminal);
        Assert.Equal("committed", committed.State);
    }

    private sealed record JournalFixture(
        string Root,
        FileTransactionJournal Journal,
        IReadOnlyList<string> Targets)
    {
        internal static async Task<JournalFixture> CreateAsync(int targetCount, bool owned = false)
        {
            string root = Path.Combine(
                Path.GetTempPath(),
                $"codex-provider-journal-{Guid.NewGuid():N}");
            string backupDir = Path.Combine(root, "backup");
            string codexHome = Path.Combine(root, ".codex");
            Directory.CreateDirectory(backupDir);
            Directory.CreateDirectory(codexHome);
            string[] targets = Enumerable.Range(0, targetCount)
                .Select(index => Path.Combine(codexHome, $"rollout-{index}.jsonl"))
                .ToArray();
            FileTransactionJournal journal = owned
                ? await FileTransactionJournal.CreateOwnedAsync(
                    backupDir,
                    codexHome,
                    "target-provider",
                    targets)
                : await FileTransactionJournal.CreateAsync(
                    backupDir,
                    codexHome,
                    "target-provider",
                    targets);
            return new JournalFixture(root, journal, targets);
        }
    }
}
