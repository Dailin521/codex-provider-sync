using System.Text;
using System.Text.Json;

namespace CodexProviderSync.Core.Tests;

public sealed class RestoreJournalServiceTests
{
    [Fact]
    public async Task WriterAndReader_CompleteCanonicalRestoreStateMachine()
    {
        RestoreJournalFixture fixture = RestoreJournalFixture.Create();
        RestoreJournal journal = await RestoreJournal.CreateAsync(
            fixture.SnapshotDir,
            fixture.OperationId,
            fixture.Prepared);

        await journal.ApplyingAsync();
        await journal.TargetIntentAsync(fixture.Target.Id);
        await journal.TargetCompletedAsync(fixture.Target.Id, fixture.Target.ExpectedPost.Digest);
        await journal.CommittingAsync("post-manifest");
        await journal.CommittedPendingAckAsync("post-manifest");
        await journal.CompletedAsync();

        RestoreJournalInfo info = await RestoreJournalService.ReadInfoAsync(journal.FilePath);
        Assert.False(info.InvalidTail);
        Assert.True(info.Terminal);
        Assert.False(info.Blocking);
        Assert.Equal("completed", info.State);
        Assert.Equal(7, info.LastSequence);
        Assert.Equal("completed", info.TargetPhases[fixture.Target.Id]);
        Assert.Equal(fixture.SourceDir, info.Prepared!.SourceBackup.BackupDir);
        Assert.EndsWith("\n", await File.ReadAllTextAsync(journal.FilePath), StringComparison.Ordinal);

        string before = await File.ReadAllTextAsync(journal.FilePath);
        await Assert.ThrowsAsync<InvalidOperationException>(() => journal.RollbackPendingAsync("too-late"));
        Assert.Equal(before, await File.ReadAllTextAsync(journal.FilePath));
    }

    [Fact]
    public async Task Reader_AcceptsNodeStylePreparedEventAndTargetTransitions()
    {
        RestoreJournalFixture fixture = RestoreJournalFixture.Create();
        string journalPath = Path.Combine(fixture.SnapshotDir, RestoreJournal.FileName);
        object[] events =
        [
            fixture.NodePreparedEvent(sequence: 1),
            fixture.NodeEvent(sequence: 2, state: "applying"),
            fixture.NodeEvent(sequence: 3, state: "applying", new
            {
                targetId = fixture.Target.Id,
                targetPhase = "intent"
            }),
            fixture.NodeEvent(sequence: 4, state: "applying", new
            {
                targetId = fixture.Target.Id,
                targetPhase = "completed",
                targetDigest = fixture.Target.ExpectedPost.Digest
            }),
            fixture.NodeEvent(sequence: 5, state: "committing", new { postManifestSha256 = "manifest" }),
            fixture.NodeEvent(sequence: 6, state: "committed-pending-ack", new { postManifestSha256 = "manifest" })
        ];
        await File.WriteAllTextAsync(
            journalPath,
            string.Join("\n", events.Select(static value => JsonSerializer.Serialize(value))) + "\n",
            new UTF8Encoding(false));

        RestoreJournalInfo info = await RestoreJournalService.ReadInfoAsync(journalPath);

        Assert.False(info.InvalidTail);
        Assert.False(info.Terminal);
        Assert.True(info.Blocking);
        Assert.Equal("committed-pending-ack", info.State);
        Assert.Equal("completed", info.TargetPhases[fixture.Target.Id]);
    }

    [Fact]
    public async Task Reader_UnknownSchemaFailsClosedWithoutLosingRawProtectionReferences()
    {
        RestoreJournalFixture fixture = RestoreJournalFixture.Create();
        string journalPath = Path.Combine(fixture.SnapshotDir, RestoreJournal.FileName);
        object value = new
        {
            schemaVersion = 99,
            protocolVersion = 99,
            operationKind = "restore",
            operationId = fixture.OperationId,
            sequence = 1,
            state = "prepared",
            recordedAt = DateTimeOffset.UtcNow,
            sourceBackup = new { backupId = "source", backupDir = fixture.SourceDir, revision = "source-revision" },
            preRestoreSnapshot = new
            {
                backupId = "snapshot",
                backupDir = fixture.SnapshotDir,
                revision = "snapshot-revision",
                manifestSha256 = "manifest"
            }
        };
        string raw = JsonSerializer.Serialize(value) + "\n";
        await File.WriteAllTextAsync(journalPath, raw, new UTF8Encoding(false));

        RestoreJournalInfo info = await RestoreJournalService.ReadInfoAsync(journalPath);

        Assert.True(info.InvalidTail);
        Assert.True(info.Blocking);
        Assert.False(info.Terminal);
        Assert.Equal("recovery-required", info.State);
        Assert.Equal(fixture.SourceDir, info.ProtectionReferences.SourceBackupDirectory);
        Assert.Equal(fixture.SnapshotDir, info.ProtectionReferences.PreRestoreSnapshotDirectory);
        Assert.False(info.ProtectionReferences.IsUnverifiable);
        Assert.Equal(raw, await File.ReadAllTextAsync(journalPath));
    }

    [Fact]
    public async Task Reader_TruncatedFirstRecordMakesPruneReferencesUnverifiable()
    {
        RestoreJournalFixture fixture = RestoreJournalFixture.Create();
        string journalPath = Path.Combine(fixture.SnapshotDir, RestoreJournal.FileName);
        byte[] raw = Encoding.UTF8.GetBytes("{\"schemaVersion\":2,\"sourceBackup\":");
        await File.WriteAllBytesAsync(journalPath, raw);

        RestoreJournalInfo info = await RestoreJournalService.ReadInfoAsync(journalPath);

        Assert.True(info.InvalidTail);
        Assert.True(info.Blocking);
        Assert.True(info.ProtectionReferences.IsUnverifiable);
        Assert.Equal(raw, await File.ReadAllBytesAsync(journalPath));
    }

    [Fact]
    public async Task Scan_ResolvedJournalNoLongerBlocksButStillProtectsNonterminalEvidence()
    {
        string codexHome = Path.Combine(Path.GetTempPath(), $"cps-restore-scan-{Guid.NewGuid():N}");
        string backupRoot = AppConstants.DefaultBackupRoot(codexHome);
        string oldSnapshot = Path.Combine(backupRoot, "restore-v2-old");
        string newSnapshot = Path.Combine(backupRoot, "restore-v2-new");
        string source = Path.Combine(backupRoot, "source");
        Directory.CreateDirectory(oldSnapshot);
        Directory.CreateDirectory(newSnapshot);
        Directory.CreateDirectory(source);

        RestoreJournalFixture oldFixture = RestoreJournalFixture.CreateAt(
            codexHome,
            oldSnapshot,
            source,
            "old-operation");
        RestoreJournal oldJournal = await RestoreJournal.CreateAsync(
            oldSnapshot,
            oldFixture.OperationId,
            oldFixture.Prepared);
        await oldJournal.ApplyingAsync();

        RestoreJournalFixture newFixture = RestoreJournalFixture.CreateAt(
            codexHome,
            newSnapshot,
            source,
            "new-operation",
            resolvesOperationIds: [oldFixture.OperationId]);
        RestoreJournal newJournal = await RestoreJournal.CreateAsync(
            newSnapshot,
            newFixture.OperationId,
            newFixture.Prepared);
        await newJournal.ApplyingAsync();
        await newJournal.TargetIntentAsync(newFixture.Target.Id);
        await newJournal.TargetCompletedAsync(newFixture.Target.Id, newFixture.Target.ExpectedPost.Digest);
        await newJournal.CommittingAsync("post");
        await newJournal.CommittedPendingAckAsync("post");
        await newJournal.CompletedAsync();

        RestoreJournalScan scan = await RestoreJournalService.ScanAsync(codexHome);

        Assert.Empty(scan.BlockingJournals);
        Assert.Contains(oldFixture.OperationId, scan.ResolvedOperationIds);
        Assert.Contains(Path.GetFullPath(oldSnapshot), scan.ProtectedDirectories);
        Assert.Contains(Path.GetFullPath(source), scan.ProtectedDirectories);
    }

    [Fact]
    public async Task Scan_MismatchedCompletedResolverCannotHidePendingRestore()
    {
        string codexHome = Path.Combine(Path.GetTempPath(), $"cps-restore-scan-{Guid.NewGuid():N}");
        string backupRoot = AppConstants.DefaultBackupRoot(codexHome);
        string oldSnapshot = Path.Combine(backupRoot, "restore-v2-old");
        string resolverSnapshot = Path.Combine(backupRoot, "restore-v2-resolver");
        string source = Path.Combine(backupRoot, "source");
        Directory.CreateDirectory(oldSnapshot);
        Directory.CreateDirectory(resolverSnapshot);
        Directory.CreateDirectory(source);

        RestoreJournalFixture oldFixture = RestoreJournalFixture.CreateAt(
            codexHome,
            oldSnapshot,
            source,
            "old-operation");
        RestoreJournal oldJournal = await RestoreJournal.CreateAsync(
            oldSnapshot,
            oldFixture.OperationId,
            oldFixture.Prepared);
        await oldJournal.ApplyingAsync();

        RestoreJournalFixture resolverFixture = RestoreJournalFixture.CreateAt(
            codexHome,
            resolverSnapshot,
            source,
            "resolver-operation",
            resolvesOperationIds: [oldFixture.OperationId]);
        RestoreJournalPrepared mismatchedPrepared = resolverFixture.Prepared with
        {
            SourceBackup = resolverFixture.Prepared.SourceBackup with
            {
                Revision = resolverFixture.Prepared.SourceBackup.Revision + "-different"
            }
        };
        RestoreJournal resolver = await RestoreJournal.CreateAsync(
            resolverSnapshot,
            resolverFixture.OperationId,
            mismatchedPrepared);
        await resolver.ApplyingAsync();
        await resolver.TargetIntentAsync(resolverFixture.Target.Id);
        await resolver.TargetCompletedAsync(
            resolverFixture.Target.Id,
            resolverFixture.Target.ExpectedPost.Digest);
        await resolver.CommittingAsync("post");
        await resolver.CommittedPendingAckAsync("post");
        await resolver.CompletedAsync();

        RestoreJournalScan scan = await RestoreJournalService.ScanAsync(codexHome);

        RestoreJournalInfo pending = Assert.Single(scan.BlockingJournals);
        Assert.Equal(oldFixture.OperationId, pending.OperationId);
        Assert.DoesNotContain(oldFixture.OperationId, scan.ResolvedOperationIds);
    }

    [Fact]
    public async Task Scan_DifferentPersistedPhysicalHomeCannotHidePendingRestore()
    {
        string codexHome = Path.Combine(Path.GetTempPath(), $"cps-restore-scan-{Guid.NewGuid():N}");
        string backupRoot = AppConstants.DefaultBackupRoot(codexHome);
        string oldSnapshot = Path.Combine(backupRoot, "restore-v2-old");
        string resolverSnapshot = Path.Combine(backupRoot, "restore-v2-resolver");
        string source = Path.Combine(backupRoot, "source");
        string otherPhysicalHome = Path.Combine(Path.GetTempPath(), $"cps-restore-other-{Guid.NewGuid():N}");
        Directory.CreateDirectory(oldSnapshot);
        Directory.CreateDirectory(resolverSnapshot);
        Directory.CreateDirectory(source);
        Directory.CreateDirectory(otherPhysicalHome);

        RestoreJournalFixture oldFixture = RestoreJournalFixture.CreateAt(
            codexHome,
            oldSnapshot,
            source,
            "old-operation");
        RestoreJournal oldJournal = await RestoreJournal.CreateAsync(
            oldSnapshot,
            oldFixture.OperationId,
            oldFixture.Prepared);
        await oldJournal.ApplyingAsync();

        RestoreJournalFixture resolverFixture = RestoreJournalFixture.CreateAt(
            codexHome,
            resolverSnapshot,
            source,
            "resolver-operation",
            resolvesOperationIds: [oldFixture.OperationId]);
        RestoreJournalPrepared resolverPrepared = resolverFixture.Prepared with
        {
            Storage = resolverFixture.Prepared.Storage with
            {
                CodexHomePhysical = StateDbLockResource.ResolveExistingPhysicalPath(
                    otherPhysicalHome,
                    directory: true)
            }
        };
        RestoreJournal resolver = await RestoreJournal.CreateAsync(
            resolverSnapshot,
            resolverFixture.OperationId,
            resolverPrepared);
        await resolver.ApplyingAsync();
        await resolver.TargetIntentAsync(resolverFixture.Target.Id);
        await resolver.TargetCompletedAsync(
            resolverFixture.Target.Id,
            resolverFixture.Target.ExpectedPost.Digest);
        await resolver.CommittingAsync("post");
        await resolver.CommittedPendingAckAsync("post");
        await resolver.CompletedAsync();

        RestoreJournalScan scan = await RestoreJournalService.ScanAsync(codexHome);

        RestoreJournalInfo pending = Assert.Single(scan.BlockingJournals);
        Assert.Equal(oldFixture.OperationId, pending.OperationId);
        Assert.DoesNotContain(oldFixture.OperationId, scan.ResolvedOperationIds);
    }

    [Fact]
    public async Task Reader_CompletedResolverWithoutTargetEvidenceFailsClosed()
    {
        RestoreJournalFixture fixture = RestoreJournalFixture.Create();
        string journalPath = Path.Combine(fixture.SnapshotDir, RestoreJournal.FileName);
        object[] events =
        [
            fixture.NodePreparedEvent(sequence: 1),
            fixture.NodeEvent(sequence: 2, state: "applying"),
            fixture.NodeEvent(sequence: 3, state: "committing", new { postManifestSha256 = "post" }),
            fixture.NodeEvent(
                sequence: 4,
                state: "committed-pending-ack",
                new { postManifestSha256 = "post" }),
            fixture.NodeEvent(sequence: 5, state: "completed")
        ];
        await File.WriteAllTextAsync(
            journalPath,
            string.Join("\n", events.Select(static value => JsonSerializer.Serialize(value))) + "\n",
            new UTF8Encoding(false));

        RestoreJournalInfo info = await RestoreJournalService.ReadInfoAsync(journalPath);

        Assert.True(info.InvalidTail);
        Assert.True(info.Blocking);
        Assert.False(info.Terminal);
        Assert.Contains("every declared target", info.ValidationError, StringComparison.Ordinal);
    }

    private sealed record RestoreJournalFixture(
        string CodexHome,
        string SourceDir,
        string SnapshotDir,
        string OperationId,
        RestoreJournalTarget Target,
        RestoreJournalPrepared Prepared)
    {
        internal static RestoreJournalFixture Create()
        {
            string root = Path.Combine(Path.GetTempPath(), $"cps-restore-journal-{Guid.NewGuid():N}");
            string codexHome = Path.Combine(root, ".codex");
            string source = Path.Combine(root, "source");
            string snapshot = Path.Combine(root, "snapshot");
            Directory.CreateDirectory(codexHome);
            Directory.CreateDirectory(source);
            Directory.CreateDirectory(snapshot);
            return CreateAt(codexHome, snapshot, source, Guid.NewGuid().ToString("D"));
        }

        internal static RestoreJournalFixture CreateAt(
            string codexHome,
            string snapshot,
            string source,
            string operationId,
            IReadOnlyList<string>? resolvesOperationIds = null)
        {
            Directory.CreateDirectory(codexHome);
            Directory.CreateDirectory(snapshot);
            Directory.CreateDirectory(source);
            string targetPath = Path.Combine(codexHome, "config.toml");
            RestoreJournalTarget target = new(
                "target-id",
                "config",
                targetPath,
                new RestoreDigest(false, "absent", "pre"),
                new RestoreDigest(true, "sha256-file", "post", 4),
                "config.toml");
            RestoreJournalPrepared prepared = new(
                new RestoreBackupIdentity("source", Path.GetFullPath(source), "source-revision"),
                new RestorePreSnapshotIdentity(
                    "snapshot",
                    Path.GetFullPath(snapshot),
                    "snapshot-revision",
                    "manifest"),
                new RestoreStorageIdentity(
                    Path.GetFullPath(codexHome),
                    StateDbLockResource.ResolveExistingPhysicalPath(codexHome, directory: true),
                    Path.Combine(Path.GetFullPath(codexHome), "sqlite"),
                    null,
                    null),
                ["config"],
                resolvesOperationIds ?? [],
                [target]);
            return new RestoreJournalFixture(
                Path.GetFullPath(codexHome),
                Path.GetFullPath(source),
                Path.GetFullPath(snapshot),
                operationId,
                target,
                prepared);
        }

        internal object NodePreparedEvent(int sequence) => new
        {
            schemaVersion = 2,
            protocolVersion = 2,
            operationKind = "restore",
            operationId = OperationId,
            sequence,
            state = "prepared",
            recordedAt = DateTimeOffset.UtcNow,
            sourceBackup = new
            {
                backupId = Prepared.SourceBackup.BackupId,
                backupDir = Prepared.SourceBackup.BackupDir,
                revision = Prepared.SourceBackup.Revision
            },
            preRestoreSnapshot = new
            {
                backupId = Prepared.PreRestoreSnapshot.BackupId,
                backupDir = Prepared.PreRestoreSnapshot.BackupDir,
                revision = Prepared.PreRestoreSnapshot.Revision,
                manifestSha256 = Prepared.PreRestoreSnapshot.ManifestSha256
            },
            storage = new
            {
                codexHome = Prepared.Storage.CodexHome,
                codexHomePhysical = Prepared.Storage.CodexHomePhysical,
                sqliteHome = Prepared.Storage.SqliteHome,
                stateDbResourceKey = Prepared.Storage.StateDbResourceKey,
                targetStateDbPath = Prepared.Storage.TargetStateDbPath
            },
            requiredTargetKinds = Prepared.RequiredTargetKinds,
            resolvesOperationIds = Prepared.ResolvesOperationIds,
            targets = Prepared.Targets.Select(target => new
            {
                id = target.Id,
                kind = target.Kind,
                targetPath = target.TargetPath,
                pre = target.Pre,
                expectedPost = target.ExpectedPost,
                snapshotPath = target.SnapshotPath,
                snapshotEntryIndex = target.SnapshotEntryIndex
            })
        };

        internal object NodeEvent(int sequence, string state, object? details = null)
        {
            Dictionary<string, object?> value = new()
            {
                ["schemaVersion"] = 2,
                ["protocolVersion"] = 2,
                ["operationKind"] = "restore",
                ["operationId"] = OperationId,
                ["sequence"] = sequence,
                ["state"] = state,
                ["recordedAt"] = DateTimeOffset.UtcNow
            };
            if (details is not null)
            {
                foreach (var property in details.GetType().GetProperties())
                {
                    value[property.Name] = property.GetValue(details);
                }
            }
            return value;
        }
    }
}
