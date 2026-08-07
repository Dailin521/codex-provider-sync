using System.Diagnostics;
using Xunit.Abstractions;

namespace CodexProviderSync.Core.Tests;

public sealed class TransactionJournalPerformanceTests(ITestOutputHelper output)
{
    [Fact]
    [Trait("Category", "Performance")]
    public async Task EightHundredTargets_AppendWithoutGrowingFullJournalReads()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }
        if (!string.Equals(
                Environment.GetEnvironmentVariable("CODEX_PROVIDER_SYNC_RUN_PERF_TESTS"),
                "1",
                StringComparison.Ordinal))
        {
            return;
        }

        string root = Path.Combine(
            Path.GetTempPath(),
            $"codex-provider-journal-perf-{Guid.NewGuid():N}");
        string backupDir = Path.Combine(root, "backup");
        string codexHome = Path.Combine(root, ".codex");
        Directory.CreateDirectory(backupDir);
        Directory.CreateDirectory(codexHome);
        string[] targets = Enumerable.Range(0, 800)
            .Select(index => Path.Combine(codexHome, $"rollout-{index:D4}.jsonl"))
            .ToArray();

        try
        {
            await using FileTransactionJournal journal = await FileTransactionJournal.CreateOwnedAsync(
                backupDir,
                codexHome,
                "target-provider",
                targets);
            Stopwatch timer = Stopwatch.StartNew();
            foreach (string target in targets)
            {
                await journal.ApplyingAsync("rollout", target);
                await journal.AppliedAsync("rollout", target);
            }
            timer.Stop();

            Assert.Equal(0, journal.AppendFullJournalValidationCount);
            await journal.CommittedAsync();
            Assert.Equal(2, journal.AppendFullJournalValidationCount);
            output.WriteLine(
                $"targets=800 appends=1600 elapsedMs={timer.ElapsedMilliseconds} fullJournalReads={journal.AppendFullJournalValidationCount}");
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }
}
