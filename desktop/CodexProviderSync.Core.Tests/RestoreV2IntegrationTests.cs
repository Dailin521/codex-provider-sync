using System.Text;
using System.Text.Json;

namespace CodexProviderSync.Core.Tests;

public sealed class RestoreV2IntegrationTests
{
    [Fact]
    public async Task MidTargetFailure_CompensatesAndPersistsRolledBack()
    {
        RestoreFixture fixture = await RestoreFixture.CreateAsync();
        string beforeRestore = await File.ReadAllTextAsync(fixture.RolloutPath);
        CodexSyncService service = new();
        service.FaultInjector = (point, target, _) =>
        {
            if (point == "after_restore_target_write_before_complete"
                && target is not null
                && Path.GetFileName(target).StartsWith("rollout-", StringComparison.Ordinal))
            {
                throw new IOException("injected Restore target failure");
            }
            return Task.CompletedTask;
        };

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunRestoreAsync(
                fixture.CodexHome,
                fixture.SourceBackup,
                new RestoreBackupOptions { RestoreDatabase = false }));

        Assert.Equal("SYNC_FAILED_ROLLED_BACK", error.Code);
        Assert.False(error.RecoveryRequired);
        Assert.Equal(beforeRestore, await File.ReadAllTextAsync(fixture.RolloutPath));
        RestoreJournalInfo journal = Assert.Single(await RestoreJournalService.FindAsync(fixture.CodexHome));
        Assert.Equal("rolled-back", journal.State);
        Assert.True(journal.Terminal);
        Assert.False(journal.Blocking);
        Assert.Empty(await RestoreJournalService.FindBlockingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task LostFinalAcknowledgement_IsRecoveredForwardWithoutRollback()
    {
        RestoreFixture fixture = await RestoreFixture.CreateAsync();
        CodexSyncService service = new();
        service.FaultInjector = (point, _, _) =>
        {
            if (point == "after_restore_committed_pending_ack_before_completed")
            {
                throw new IOException("injected lost acknowledgement");
            }
            return Task.CompletedTask;
        };

        RestoreResult result = await service.RunRestoreAsync(
            fixture.CodexHome,
            fixture.SourceBackup,
            new RestoreBackupOptions { RestoreDatabase = false });

        Assert.Equal(2, result.RestoreVersion);
        Assert.Equal("completed", result.RestoreJournalState);
        Assert.True(result.CommitAcknowledgementRecovered);
        Assert.Contains("apigather", await File.ReadAllTextAsync(fixture.RolloutPath));
        RestoreJournalInfo journal = Assert.Single(await RestoreJournalService.FindAsync(fixture.CodexHome));
        Assert.Equal("completed", journal.State);
        Assert.DoesNotContain(
            journal.Events,
            static item => item.State is "rollback-pending" or "rolled-back");
    }

    [Fact]
    public async Task PreSnapshotFailure_HasNoMutationAndLeavesNoRestoreJournal()
    {
        RestoreFixture fixture = await RestoreFixture.CreateAsync();
        string configBefore = await File.ReadAllTextAsync(Path.Combine(fixture.CodexHome, "config.toml"));
        string rolloutBefore = await File.ReadAllTextAsync(fixture.RolloutPath);
        CodexSyncService service = new();
        service.FaultInjector = (point, _, _) =>
        {
            if (point == "after_restore_pre_snapshot_target_before_hash")
            {
                throw new IOException("injected pre-snapshot failure");
            }
            return Task.CompletedTask;
        };

        await Assert.ThrowsAsync<IOException>(() => service.RunRestoreAsync(
            fixture.CodexHome,
            fixture.SourceBackup,
            new RestoreBackupOptions { RestoreDatabase = false }));

        Assert.Equal(configBefore, await File.ReadAllTextAsync(Path.Combine(fixture.CodexHome, "config.toml")));
        Assert.Equal(rolloutBefore, await File.ReadAllTextAsync(fixture.RolloutPath));
        Assert.Empty(await RestoreJournalService.FindAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task UnknownRestoreSchema_BlocksWritesAndMakesPruneNoOpWithoutRewritingEvidence()
    {
        RestoreFixture fixture = await RestoreFixture.CreateAsync();
        string snapshotDir = Path.Combine(
            AppConstants.DefaultBackupRoot(fixture.CodexHome),
            "restore-v2-unknown");
        Directory.CreateDirectory(snapshotDir);
        await File.WriteAllTextAsync(
            Path.Combine(snapshotDir, "metadata.json"),
            JsonSerializer.Serialize(new
            {
                version = 2,
                @namespace = AppConstants.BackupNamespace,
                codexHome = fixture.CodexHome,
                targetProvider = "openai",
                createdAt = DateTimeOffset.UtcNow,
                dbFiles = Array.Empty<string>(),
                sqliteDbFiles = Array.Empty<string>(),
                changedSessionFiles = 0
            }));
        string journalPath = Path.Combine(snapshotDir, RestoreJournal.FileName);
        string raw = JsonSerializer.Serialize(new
        {
            schemaVersion = 99,
            protocolVersion = 99,
            operationKind = "restore",
            operationId = Guid.NewGuid().ToString("D"),
            sequence = 1,
            state = "prepared",
            recordedAt = DateTimeOffset.UtcNow,
            sourceBackup = new
            {
                backupId = Path.GetFileName(fixture.SourceBackup),
                backupDir = fixture.SourceBackup,
                revision = "unknown"
            },
            preRestoreSnapshot = new
            {
                backupId = Path.GetFileName(snapshotDir),
                backupDir = snapshotDir,
                revision = "unknown",
                manifestSha256 = "unknown"
            }
        }) + "\n";
        await File.WriteAllTextAsync(journalPath, raw, new UTF8Encoding(false));
        string[] before = Directory.GetDirectories(AppConstants.DefaultBackupRoot(fixture.CodexHome));

        RecoveryRequiredException blocked = await Assert.ThrowsAsync<RecoveryRequiredException>(
            () => new CodexSyncService().RunSyncAsync(fixture.CodexHome, provider: "openai"));
        Assert.Equal("RECOVERY_REQUIRED", blocked.Code);

        BackupPruneResult pruned = await new CodexSyncService().RunPruneBackupsAsync(
            fixture.CodexHome,
            keepCount: 0);
        string[] after = Directory.GetDirectories(AppConstants.DefaultBackupRoot(fixture.CodexHome));
        Assert.Equal(0, pruned.DeletedCount);
        Assert.Equal(before.Order(StringComparer.Ordinal), after.Order(StringComparer.Ordinal));
        Assert.Equal(raw, await File.ReadAllTextAsync(journalPath));
        Assert.True(Directory.Exists(fixture.SourceBackup));
        Assert.True(Directory.Exists(snapshotDir));
    }

    private sealed record RestoreFixture(
        string CodexHome,
        string RolloutPath,
        string SourceBackup)
    {
        internal static async Task<RestoreFixture> CreateAsync()
        {
            TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
            await fixture.WriteConfigAsync("model_provider = \"openai\"");
            string rolloutPath = fixture.RolloutPath(
                "sessions",
                "rollout-restore-v2-integration.jsonl");
            await fixture.WriteRolloutAsync(
                rolloutPath,
                "restore-v2-integration",
                "apigather");
            SyncResult sync = await new CodexSyncService().RunSyncAsync(
                fixture.CodexHome,
                provider: "openai");
            Assert.Contains("openai", await File.ReadAllTextAsync(rolloutPath));
            return new RestoreFixture(fixture.CodexHome, rolloutPath, sync.BackupDir);
        }
    }
}
