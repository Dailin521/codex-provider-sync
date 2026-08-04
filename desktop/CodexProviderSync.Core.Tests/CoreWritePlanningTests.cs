using System.Security.Cryptography;
using Microsoft.Data.Sqlite;

namespace CodexProviderSync.Core.Tests;

public sealed class CoreWritePlanningTests
{
    [Fact]
    public async Task CheckedSync_RejectsDriftBeforeBackupOrMutation()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-plan-drift.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-plan-drift", "relay");
        await fixture.WriteStateDbAsync([("thread-plan-drift", "relay", false)]);

        CodexSyncService service = new();
        CoreWritePlanSnapshot plan = await service.CreateSyncPlanSnapshotAsync(
            fixture.CodexHome,
            provider: "openai");
        await File.AppendAllTextAsync(rolloutPath, "{\"type\":\"event_msg\",\"payload\":{}}\n");
        string drifted = await File.ReadAllTextAsync(rolloutPath);

        await Assert.ThrowsAsync<CoreWritePlanStaleException>(() =>
            service.RunSyncCheckedAsync(
                plan,
                fixture.CodexHome,
                provider: "openai"));

        Assert.Equal(drifted, await File.ReadAllTextAsync(rolloutPath));
        Assert.False(Directory.Exists(fixture.BackupRoot()));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task CheckedSync_ExecutesTheExistingCoreWorkflowForAnExactSnapshot()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-plan-exact.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-plan-exact", "relay");
        await fixture.WriteStateDbAsync([("thread-plan-exact", "relay", false)]);

        CodexSyncService service = new();
        CoreWritePlanSnapshot plan = await service.CreateSyncPlanSnapshotAsync(
            fixture.CodexHome,
            provider: "openai");

        SyncResult result = await service.RunSyncCheckedAsync(
            plan,
            fixture.CodexHome,
            provider: "openai");

        Assert.Equal("openai", result.TargetProvider);
        Assert.Equal(1, result.ChangedSessionFiles);
        Assert.True(Directory.Exists(result.BackupDir));
        Assert.Contains("\"model_provider\":\"openai\"", await File.ReadAllTextAsync(rolloutPath));
    }

    [Fact]
    public async Task SyncAndSwitchPlans_ListExactAutomaticDeletionTargets()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-auto-prune-plan.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-auto-prune-plan", "relay");
        await fixture.WriteStateDbAsync([("thread-auto-prune-plan", "relay", false)]);
        await fixture.WriteBackupAsync("20260801T000000000Z");
        await fixture.WriteBackupAsync("20260802T000000000Z");
        string futureManaged = fixture.BackupPath("99991231T235959999Z");
        await fixture.WriteBackupAsync("99991231T235959999Z");
        string unmanaged = fixture.BackupPath("manual-sentinel");
        Directory.CreateDirectory(unmanaged);
        await File.WriteAllTextAsync(Path.Combine(unmanaged, "keep.txt"), "sentinel");

        CodexSyncService service = new();
        CoreWritePlanSnapshot syncPlan = await service.CreateSyncPlanSnapshotAsync(
            fixture.CodexHome,
            provider: "openai",
            keepCount: 1);
        CoreWritePlanSnapshot switchPlan = await service.CreateSwitchPlanSnapshotAsync(
            fixture.CodexHome,
            "apigather",
            keepCount: 1);

        string[] expected =
        [
            fixture.BackupPath("20260801T000000000Z"),
            fixture.BackupPath("20260802T000000000Z"),
            futureManaged
        ];
        AssertAutomaticDeletionTargets(syncPlan, expected);
        AssertAutomaticDeletionTargets(switchPlan, expected);
        Assert.True(Directory.Exists(unmanaged));
    }

    [Fact]
    public async Task CheckedSync_AutomaticPruneDeletesOnlyPlannedTargetsAndPreservesNewBackup()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-auto-prune-apply.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-auto-prune-apply", "relay");
        await fixture.WriteStateDbAsync([("thread-auto-prune-apply", "relay", false)]);
        await fixture.WriteBackupAsync("20260801T000000000Z");
        await fixture.WriteBackupAsync("20260802T000000000Z");
        await fixture.WriteBackupAsync("99991231T235959999Z");
        string unmanaged = fixture.BackupPath("manual-sentinel");
        Directory.CreateDirectory(unmanaged);
        await File.WriteAllTextAsync(Path.Combine(unmanaged, "keep.txt"), "sentinel");

        CodexSyncService service = new();
        CoreWritePlanSnapshot plan = await service.CreateSyncPlanSnapshotAsync(
            fixture.CodexHome,
            provider: "openai",
            keepCount: 1);
        string[] plannedDeletes = plan.AutoPruneDeletionTargets.Select(static target => target.Path).ToArray();

        SyncResult result = await service.RunSyncCheckedAsync(
            plan,
            fixture.CodexHome,
            provider: "openai",
            keepCount: 1);

        Assert.NotNull(result.AutoPruneResult);
        Assert.Equal(plannedDeletes.Length, result.AutoPruneResult!.DeletedCount);
        Assert.All(plannedDeletes, path => Assert.False(Directory.Exists(path)));
        Assert.True(Directory.Exists(result.BackupDir));
        Assert.True(Directory.Exists(unmanaged));
    }

    [Fact]
    public async Task CheckedSync_AutomaticPruneWithKeepTwoPreservesFutureDatedAndNewBackups()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-auto-prune-keep-two.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-auto-prune-keep-two", "relay");
        await fixture.WriteStateDbAsync([("thread-auto-prune-keep-two", "relay", false)]);
        string oldOne = fixture.BackupPath("20260801T000000000Z");
        string oldTwo = fixture.BackupPath("20260802T000000000Z");
        string future = fixture.BackupPath("99991231T235959999Z");
        await fixture.WriteBackupAsync("20260801T000000000Z");
        await fixture.WriteBackupAsync("20260802T000000000Z");
        await fixture.WriteBackupAsync("99991231T235959999Z");

        CodexSyncService service = new();
        CoreWritePlanSnapshot plan = await service.CreateSyncPlanSnapshotAsync(
            fixture.CodexHome,
            provider: "openai",
            keepCount: 2);
        SyncResult result = await service.RunSyncCheckedAsync(
            plan,
            fixture.CodexHome,
            provider: "openai",
            keepCount: 2);

        AssertAutomaticDeletionTargets(plan, [oldOne, oldTwo]);
        Assert.False(Directory.Exists(oldOne));
        Assert.False(Directory.Exists(oldTwo));
        Assert.True(Directory.Exists(future));
        Assert.True(Directory.Exists(result.BackupDir));
        Assert.Equal(2, Directory.EnumerateDirectories(fixture.BackupRoot())
            .Count(directory => File.Exists(Path.Combine(directory, "metadata.json"))));
    }

    [Fact]
    public async Task CheckedSync_AutomaticPruneSkipsWholeBatchWhenPlannedBackupFingerprintChangesAfterCommit()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-auto-prune-fingerprint-drift.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-auto-prune-fingerprint-drift", "relay");
        await fixture.WriteStateDbAsync([("thread-auto-prune-fingerprint-drift", "relay", false)]);
        await fixture.WriteBackupAsync("20260801T000000000Z", ("note.txt", "original"));
        await fixture.WriteBackupAsync("20260802T000000000Z");

        CodexSyncService service = new();
        CoreWritePlanSnapshot plan = await service.CreateSyncPlanSnapshotAsync(
            fixture.CodexHome,
            provider: "openai",
            keepCount: 1);
        string changedBackup = plan.AutoPruneDeletionTargets[0].Path;
        service.FaultInjector = async (point, _, _) =>
        {
            if (point == "after_transaction_commit")
            {
                await File.AppendAllTextAsync(Path.Combine(changedBackup, "note.txt"), "-changed");
            }
        };

        SyncResult result = await service.RunSyncCheckedAsync(
            plan,
            fixture.CodexHome,
            provider: "openai",
            keepCount: 1);

        Assert.Null(result.AutoPruneResult);
        Assert.False(string.IsNullOrWhiteSpace(result.AutoPruneWarning));
        Assert.All(plan.AutoPruneDeletionTargets, target => Assert.True(Directory.Exists(target.Path)));
        Assert.True(Directory.Exists(result.BackupDir));
    }

    [Fact]
    public async Task CheckedSync_AutomaticPruneSkipsWholeBatchWhenCandidateSetChangesAfterCommit()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-auto-prune-candidate-drift.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-auto-prune-candidate-drift", "relay");
        await fixture.WriteStateDbAsync([("thread-auto-prune-candidate-drift", "relay", false)]);
        await fixture.WriteBackupAsync("20260801T000000000Z");
        await fixture.WriteBackupAsync("20260802T000000000Z");

        CodexSyncService service = new();
        CoreWritePlanSnapshot plan = await service.CreateSyncPlanSnapshotAsync(
            fixture.CodexHome,
            provider: "openai",
            keepCount: 1);
        string addedBackup = fixture.BackupPath("20260803T000000000Z");
        service.FaultInjector = async (point, _, _) =>
        {
            if (point == "after_transaction_commit")
            {
                await fixture.WriteBackupAsync("20260803T000000000Z");
            }
        };

        SyncResult result = await service.RunSyncCheckedAsync(
            plan,
            fixture.CodexHome,
            provider: "openai",
            keepCount: 1);

        Assert.Null(result.AutoPruneResult);
        Assert.False(string.IsNullOrWhiteSpace(result.AutoPruneWarning));
        Assert.True(Directory.Exists(addedBackup));
        Assert.All(plan.AutoPruneDeletionTargets, target => Assert.True(Directory.Exists(target.Path)));
    }

    [Fact]
    public async Task CheckedSync_RejectsAutomaticPruneInventoryDriftBeforeMutation()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-auto-prune-drift.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-auto-prune-drift", "relay");
        await fixture.WriteStateDbAsync([("thread-auto-prune-drift", "relay", false)]);
        await fixture.WriteBackupAsync("20260801T000000000Z");
        await fixture.WriteBackupAsync("20260802T000000000Z");

        CodexSyncService service = new();
        CoreWritePlanSnapshot plan = await service.CreateSyncPlanSnapshotAsync(
            fixture.CodexHome,
            provider: "openai",
            keepCount: 1);
        await fixture.WriteBackupAsync("20260803T000000000Z");
        string before = await File.ReadAllTextAsync(rolloutPath);

        await Assert.ThrowsAsync<CoreWritePlanStaleException>(() =>
            service.RunSyncCheckedAsync(
                plan,
                fixture.CodexHome,
                provider: "openai",
                keepCount: 1));

        Assert.Equal(before, await File.ReadAllTextAsync(rolloutPath));
        Assert.Equal(3, Directory.EnumerateDirectories(fixture.BackupRoot()).Count());
    }

    [Fact]
    public async Task CheckedPrune_RejectsBackupInventoryDriftBeforeDeletion()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteBackupAsync("20260801T000000000Z");
        await fixture.WriteBackupAsync("20260802T000000000Z");

        CodexSyncService service = new();
        CoreWritePlanSnapshot plan = await service.CreatePrunePlanSnapshotAsync(
            fixture.CodexHome,
            keepCount: 1);
        string addedBackup = fixture.BackupPath("20260803T000000000Z");
        await fixture.WriteBackupAsync("20260803T000000000Z");

        await Assert.ThrowsAsync<CoreWritePlanStaleException>(() =>
            service.RunPruneBackupsCheckedAsync(
                plan,
                fixture.CodexHome,
                keepCount: 1));

        Assert.True(Directory.Exists(addedBackup));
        Assert.Equal(3, Directory.EnumerateDirectories(fixture.BackupRoot()).Count());
    }

    [Fact]
    public async Task CheckedSwitch_UsesTheExistingSwitchWorkflowForAnExactSnapshot()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-switch-plan.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-switch-plan", "openai");
        await fixture.WriteStateDbAsync([("thread-switch-plan", "openai", false)]);

        CodexSyncService service = new();
        CoreWritePlanSnapshot plan = await service.CreateSwitchPlanSnapshotAsync(
            fixture.CodexHome,
            "apigather");
        SyncResult result = await service.RunSwitchCheckedAsync(
            plan,
            fixture.CodexHome,
            "apigather");

        Assert.True(result.ConfigUpdated);
        Assert.Equal("apigather", result.TargetProvider);
        Assert.Contains(
            "model_provider = \"apigather\"",
            await File.ReadAllTextAsync(Path.Combine(fixture.CodexHome, "config.toml")));
        Assert.Contains("\"model_provider\":\"apigather\"", await File.ReadAllTextAsync(rolloutPath));
    }

    [Fact]
    public async Task CheckedRestore_RejectsSourceBackupDriftBeforeRestoringAnything()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-restore-plan.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-restore-plan", "relay");
        await fixture.WriteStateDbAsync([("thread-restore-plan", "relay", false)]);

        CodexSyncService service = new();
        SyncResult sync = await service.RunSyncAsync(fixture.CodexHome, provider: "openai");
        RestoreBackupOptions options = new();
        CoreWritePlanSnapshot plan = await service.CreateRestorePlanSnapshotAsync(
            fixture.CodexHome,
            sync.BackupDir,
            options);
        await File.AppendAllTextAsync(Path.Combine(sync.BackupDir, "metadata.json"), "\n");
        string before = await File.ReadAllTextAsync(rolloutPath);

        await Assert.ThrowsAsync<CoreWritePlanStaleException>(() =>
            service.RunRestoreCheckedAsync(
                plan,
                fixture.CodexHome,
                sync.BackupDir,
                options));

        Assert.Equal(before, await File.ReadAllTextAsync(rolloutPath));
        Assert.Contains("\"model_provider\":\"openai\"", before);
    }

    [Fact]
    public async Task CheckedSync_RechecksExpiryInsideTheCoreLockBeforeBackup()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-expired-plan.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-expired-plan", "relay");
        await fixture.WriteStateDbAsync([("thread-expired-plan", "relay", false)]);
        CodexSyncService service = new();
        CoreWritePlanSnapshot plan = await service.CreateSyncPlanSnapshotAsync(
            fixture.CodexHome,
            provider: "openai");

        await Assert.ThrowsAsync<CoreWritePlanExpiredException>(() =>
            service.RunSyncCheckedAsync(
                plan,
                fixture.CodexHome,
                provider: "openai",
                snapshotExpiresAtUtc: DateTimeOffset.UtcNow.AddSeconds(-1)));

        Assert.False(Directory.Exists(fixture.BackupRoot()));
        Assert.Contains("\"model_provider\":\"relay\"", await File.ReadAllTextAsync(rolloutPath));
    }

    [Fact]
    public async Task CheckedRestore_PrioritizesForeignPendingRecoveryOverTheOldPlan()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-foreign-pending.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-foreign-pending", "relay");
        await fixture.WriteStateDbAsync([("thread-foreign-pending", "relay", false)]);
        CodexSyncService service = new();
        SyncResult sync = await service.RunSyncAsync(fixture.CodexHome, provider: "openai");
        RestoreBackupOptions options = new();
        CoreWritePlanSnapshot plan = await service.CreateRestorePlanSnapshotAsync(
            fixture.CodexHome,
            sync.BackupDir,
            options);
        string foreignBackup = fixture.BackupPath("99991231T235959999Z");
        Directory.CreateDirectory(foreignBackup);
        await FileTransactionJournal.CreateAsync(
            foreignBackup,
            fixture.CodexHome,
            "openai",
            [rolloutPath]);
        string before = await File.ReadAllTextAsync(rolloutPath);

        RecoveryRequiredException error = await Assert.ThrowsAsync<RecoveryRequiredException>(() =>
            service.RunRestoreCheckedAsync(
                plan,
                fixture.CodexHome,
                sync.BackupDir,
                options));

        Assert.Contains(foreignBackup, error.PendingBackupDirectories);
        Assert.Equal(before, await File.ReadAllTextAsync(rolloutPath));
    }

    [Fact]
    public async Task SqliteCheckedPlans_TrackMainAndWalButExcludeVolatileShm()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-sqlite-plan-targets.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-sqlite-plan-targets", "relay");
        await fixture.WriteStateDbAsync([("thread-sqlite-plan-targets", "relay", false)]);

        string databasePath = Path.GetFullPath(fixture.StateDbPath());
        CodexSyncService service = new();
        CoreWritePlanSnapshot syncPlan = await service.CreateSyncPlanSnapshotAsync(
            fixture.CodexHome,
            provider: "openai");

        Assert.Contains(syncPlan.Targets, target => target.Path == databasePath);
        Assert.Contains(syncPlan.Targets, target => target.Path == databasePath + "-wal");
        Assert.DoesNotContain(syncPlan.Targets, target => target.Path == databasePath + "-shm");

        SyncResult sync = await service.RunSyncAsync(fixture.CodexHome, provider: "openai");
        CoreWritePlanSnapshot restorePlan = await service.CreateRestorePlanSnapshotAsync(
            fixture.CodexHome,
            sync.BackupDir,
            new RestoreBackupOptions());

        Assert.Contains(restorePlan.Targets, target => target.Path == databasePath);
        Assert.Contains(restorePlan.Targets, target => target.Path == databasePath + "-wal");
        Assert.DoesNotContain(restorePlan.Targets, target => target.Path == databasePath + "-shm");
    }

    [Fact]
    public async Task CheckedRestore_AllowsSqliteMetadataAndShmChurnFromReadOnlyStatus()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-restore-shm-churn.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-restore-shm-churn", "relay");
        await fixture.WriteStateDbAsync([("thread-restore-shm-churn", "relay", false)]);

        CodexSyncService service = new();
        SyncResult sync = await service.RunSyncAsync(fixture.CodexHome, provider: "openai");
        await using SqliteConnection keeper = fixture.OpenSqliteConnection();
        await keeper.OpenAsync();
        await EnableWalAsync(keeper);
        await SetProviderAsync(keeper, "live-before-restore");

        string databasePath = fixture.StateDbPath();
        string walPath = databasePath + "-wal";
        string shmPath = databasePath + "-shm";
        Assert.True(File.Exists(walPath));
        Assert.True(File.Exists(shmPath));
        CoreWritePlanSnapshot plan = await service.CreateRestorePlanSnapshotAsync(
            fixture.CodexHome,
            sync.BackupDir,
            new RestoreBackupOptions());
        string mainBefore = await DigestFileAsync(databasePath);
        string walBefore = await DigestFileAsync(walPath);

        _ = await service.GetStatusAsync(fixture.CodexHome);
        Assert.Equal(mainBefore, await DigestFileAsync(databasePath));
        Assert.Equal(walBefore, await DigestFileAsync(walPath));

        // File timestamps are not logical SQLite state. Force deterministic
        // metadata-only drift on the durable main file and the derived SHM
        // coordination file; neither main nor WAL content changes here.
        DateTime mainWriteTime = File.GetLastWriteTimeUtc(databasePath);
        File.SetLastWriteTimeUtc(databasePath, mainWriteTime.AddSeconds(2));
        DateTime shmWriteTime = File.GetLastWriteTimeUtc(shmPath);
        File.SetLastWriteTimeUtc(shmPath, shmWriteTime.AddSeconds(2));
        Assert.Equal(mainBefore, await DigestFileAsync(databasePath));
        Assert.Equal(walBefore, await DigestFileAsync(walPath));

        RestoreResult result = await service.RunRestoreCheckedAsync(
            plan,
            fixture.CodexHome,
            sync.BackupDir,
            new RestoreBackupOptions());

        Assert.Equal(sync.BackupDir, result.BackupDir);
        Assert.Equal("relay", await ReadProviderAsync(keeper));
    }

    [Fact]
    public async Task SqliteWalFingerprint_TreatsMissingAndZeroLengthAsEquivalent()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        string walPath = Path.Combine(fixture.Root, "sqlite", AppConstants.DbFileBasename + "-wal");
        CoreWriteTargetSpec walTarget = new(
            walPath,
            "update-if-present",
            CoreWriteFingerprintMode.SqliteWalContent);

        CoreWritePlanSnapshot missing = await CoreWriteSnapshotBuilder.BuildAsync(
            "sync",
            "sqlite-wal-normalization",
            [walTarget]);
        Directory.CreateDirectory(Path.GetDirectoryName(walPath)!);
        await File.WriteAllBytesAsync(walPath, []);
        CoreWritePlanSnapshot empty = await CoreWriteSnapshotBuilder.BuildAsync(
            "sync",
            "sqlite-wal-normalization",
            [walTarget]);

        CoreWriteSnapshotBuilder.AssertExactMatch(missing, empty);
        Assert.Equal(
            Assert.Single(missing.Targets).Fingerprint,
            Assert.Single(empty.Targets).Fingerprint);

        File.Delete(walPath);
        CoreWritePlanSnapshot missingAgain = await CoreWriteSnapshotBuilder.BuildAsync(
            "sync",
            "sqlite-wal-normalization",
            [walTarget]);
        CoreWriteSnapshotBuilder.AssertExactMatch(empty, missingAgain);
    }

    [Fact]
    public async Task CheckedRestore_RejectsCommittedWalDriftBeforeMutation()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-restore-wal-drift.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-restore-wal-drift", "relay");
        await fixture.WriteStateDbAsync([("thread-restore-wal-drift", "relay", false)]);

        CodexSyncService service = new();
        SyncResult sync = await service.RunSyncAsync(fixture.CodexHome, provider: "openai");
        await using SqliteConnection keeper = fixture.OpenSqliteConnection();
        await keeper.OpenAsync();
        await EnableWalAsync(keeper);
        await SetProviderAsync(keeper, "live-before-plan");

        string databasePath = fixture.StateDbPath();
        string walPath = databasePath + "-wal";
        CoreWritePlanSnapshot plan = await service.CreateRestorePlanSnapshotAsync(
            fixture.CodexHome,
            sync.BackupDir,
            new RestoreBackupOptions());
        string mainBefore = await DigestFileAsync(databasePath);
        string walBefore = await DigestFileAsync(walPath);

        await SetProviderAsync(keeper, "committed-after-plan");

        Assert.Equal(mainBefore, await DigestFileAsync(databasePath));
        Assert.NotEqual(walBefore, await DigestFileAsync(walPath));
        await Assert.ThrowsAsync<CoreWritePlanStaleException>(() =>
            service.RunRestoreCheckedAsync(
                plan,
                fixture.CodexHome,
                sync.BackupDir,
                new RestoreBackupOptions()));
        Assert.Equal("committed-after-plan", await ReadProviderAsync(keeper));
    }

    private static async Task EnableWalAsync(SqliteConnection connection)
    {
        await using SqliteCommand journalMode = connection.CreateCommand();
        journalMode.CommandText = "PRAGMA journal_mode = WAL";
        Assert.Equal("wal", Convert.ToString(await journalMode.ExecuteScalarAsync()));

        await using SqliteCommand autoCheckpoint = connection.CreateCommand();
        autoCheckpoint.CommandText = "PRAGMA wal_autocheckpoint = 0";
        await autoCheckpoint.ExecuteNonQueryAsync();
    }

    private static async Task SetProviderAsync(SqliteConnection connection, string provider)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "UPDATE threads SET model_provider = $provider";
        command.Parameters.AddWithValue("$provider", provider);
        Assert.Equal(1, await command.ExecuteNonQueryAsync());
    }

    private static async Task<string> ReadProviderAsync(SqliteConnection connection)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT model_provider FROM threads LIMIT 1";
        return Convert.ToString(await command.ExecuteScalarAsync())!;
    }

    private static async Task<string> DigestFileAsync(string path)
    {
        await using FileStream stream = new(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            64 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        byte[] digest = await SHA256.HashDataAsync(stream);
        return Convert.ToHexString(digest);
    }

    private static void AssertAutomaticDeletionTargets(
        CoreWritePlanSnapshot plan,
        IReadOnlyList<string> expectedPaths)
    {
        Assert.Equal(
            expectedPaths.Order(StringComparer.Ordinal),
            plan.AutoPruneDeletionTargets.Select(static target => target.Path).Order(StringComparer.Ordinal));
        Assert.All(plan.AutoPruneDeletionTargets, target =>
        {
            Assert.Equal("delete", target.Action);
            Assert.StartsWith("sha256:", target.Fingerprint, StringComparison.Ordinal);
        });
    }
}
