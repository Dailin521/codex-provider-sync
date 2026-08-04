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
}
