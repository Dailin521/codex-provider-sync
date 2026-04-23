using Microsoft.Data.Sqlite;

namespace CodexProviderSync.Core.Tests;

public sealed class CoreIntegrationTests
{
    [Fact]
    public async Task RunSync_RewritesRolloutFilesAndSqlite_ThenRestoreRevertsBoth()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        string archivedPath = fixture.RolloutPath("archived_sessions", "rollout-b.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "apigather");
        await fixture.WriteRolloutAsync(archivedPath, "thread-b", "newapi");
        await fixture.WriteStateDbAsync(
        [
            ("thread-a", "apigather", false),
            ("thread-b", "newapi", true)
        ]);

        CodexSyncService service = new();
        SyncResult syncResult = await service.RunSyncAsync(fixture.CodexHome);

        Assert.Equal("openai", syncResult.TargetProvider);
        Assert.Equal(2, syncResult.ChangedSessionFiles);
        Assert.Empty(syncResult.SkippedLockedRolloutFiles);
        Assert.Equal(2, syncResult.SqliteRowsUpdated);

        string syncedSession = await File.ReadAllTextAsync(sessionPath);
        string syncedArchived = await File.ReadAllTextAsync(archivedPath);
        Assert.Contains("\"model_provider\":\"openai\"", syncedSession);
        Assert.Contains("\"model_provider\":\"openai\"", syncedArchived);

        await using (SqliteConnection connection = fixture.OpenSqliteConnection())
        {
            await connection.OpenAsync();
            SqliteCommand command = connection.CreateCommand();
            command.CommandText = "SELECT id, model_provider FROM threads ORDER BY id";
            await using SqliteDataReader reader = await command.ExecuteReaderAsync();
            List<(string Id, string Provider)> rows = [];
            while (await reader.ReadAsync())
            {
                rows.Add((reader.GetString(0), reader.GetString(1)));
            }

            Assert.Equal(
            [
                ("thread-a", "openai"),
                ("thread-b", "openai")
            ], rows);
        }

        RestoreResult restoreResult = await service.RunRestoreAsync(fixture.CodexHome, syncResult.BackupDir);
        Assert.Equal("openai", restoreResult.TargetProvider);

        string restoredSession = await File.ReadAllTextAsync(sessionPath);
        string restoredArchived = await File.ReadAllTextAsync(archivedPath);
        Assert.Contains("\"model_provider\":\"apigather\"", restoredSession);
        Assert.Contains("\"model_provider\":\"newapi\"", restoredArchived);
    }

    [Fact]
    public async Task RunSwitch_UpdatesConfigAndSyncsProviderMetadata()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync(string.Empty);
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "openai");
        await fixture.WriteStateDbAsync(
        [
            ("thread-a", "openai", false)
        ]);

        CodexSyncService service = new();
        SyncResult result = await service.RunSwitchAsync(fixture.CodexHome, "apigather");

        Assert.Equal("apigather", result.TargetProvider);
        Assert.True(result.ConfigUpdated);

        string configText = await File.ReadAllTextAsync(Path.Combine(fixture.CodexHome, "config.toml"));
        Assert.Contains("model_provider = \"apigather\"", configText);
        string rollout = await File.ReadAllTextAsync(sessionPath);
        Assert.Contains("\"model_provider\":\"apigather\"", rollout);
    }

    [Fact]
    public async Task RunSync_NormalizesRolloutLastWriteTimeToSqliteUpdatedAtMs()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        const string rolloutActivityIso = "2026-03-22T15:45:30.000Z";
        const string sqliteActivityIso = "2026-03-21T09:10:11.123Z";
        await fixture.WriteRolloutLinesAsync(
            sessionPath,
            """
            {"timestamp":"2026-03-19T00:00:20.000Z","type":"session_meta","payload":{"id":"thread-a","timestamp":"2026-03-19T00:00:00.000Z","cwd":"C:\\AITemp","source":"cli","cli_version":"0.115.0","model_provider":"apigather"}}
            """,
            $$$"""
            {"timestamp":"{{{rolloutActivityIso}}}","type":"event_msg","payload":{"type":"assistant_message","message":"latest"}}
            """);
        DateTime wrongTimestamp = new(2026, 4, 1, 0, 0, 0, DateTimeKind.Utc);
        File.SetLastWriteTimeUtc(sessionPath, wrongTimestamp);
        DateTimeOffset sqliteActivity = DateTimeOffset.Parse(sqliteActivityIso, null, System.Globalization.DateTimeStyles.RoundtripKind).ToUniversalTime();
        await fixture.WriteStateDbAsync(
        [
            ("thread-a", "apigather", false, sqliteActivity.ToUnixTimeSeconds(), sqliteActivity.ToUnixTimeMilliseconds())
        ]);

        CodexSyncService service = new();
        await service.RunSyncAsync(fixture.CodexHome);

        DateTime expectedUtc = sqliteActivity.UtcDateTime;
        DateTime actualUtc = File.GetLastWriteTimeUtc(sessionPath);
        Assert.InRange(Math.Abs((actualUtc - expectedUtc).TotalSeconds), 0, 2);
    }

    [Fact]
    public async Task RunSync_PreservesRolloutLastWriteTime_WhenSqliteActivityIsUnavailable()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutLinesAsync(
            sessionPath,
            """
            {"timestamp":"2026-03-19T00:00:20.000Z","type":"session_meta","payload":{"id":"thread-a","timestamp":"2026-03-19T00:00:00.000Z","cwd":"C:\\AITemp","source":"cli","cli_version":"0.115.0","model_provider":"apigather"}}
            """,
            """
            {"timestamp":"2026-03-25T12:00:00.000Z","type":"event_msg","payload":{"type":"assistant_message","message":"later in rollout only"}}
            """);
        DateTime preservedTimestamp = new(2026, 4, 1, 0, 0, 0, DateTimeKind.Utc);
        File.SetLastWriteTimeUtc(sessionPath, preservedTimestamp);
        await fixture.WriteStateDbAsync(
        [
            ("thread-a", "apigather", false)
        ]);

        CodexSyncService service = new();
        await service.RunSyncAsync(fixture.CodexHome);

        DateTime actualUtc = File.GetLastWriteTimeUtc(sessionPath);
        Assert.InRange(Math.Abs((actualUtc - preservedTimestamp).TotalSeconds), 0, 2);
    }

    [Fact]
    public async Task GetStatus_ReportsImplicitDefaultProviderAndCounts()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync(string.Empty);
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        string archivedPath = fixture.RolloutPath("archived_sessions", "rollout-b.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "apigather");
        await fixture.WriteRolloutAsync(archivedPath, "thread-b", "openai");
        long backupOneBytes = await fixture.WriteBackupAsync("20260319T000000000Z", ("note.txt", "backup-one"));
        long backupTwoBytes = await fixture.WriteBackupAsync("20260320T000000000Z", ("note.txt", "backup-two"));
        await fixture.WriteStateDbAsync(
        [
            ("thread-a", "apigather", false),
            ("thread-b", "openai", true)
        ]);

        CodexSyncService service = new();
        StatusSnapshot status = await service.GetStatusAsync(fixture.CodexHome);

        Assert.Equal("openai", status.CurrentProvider.Provider);
        Assert.True(status.CurrentProvider.Implicit);
        Assert.Equal(1, status.RolloutCounts.Sessions["apigather"]);
        Assert.Equal(1, status.SqliteCounts!.ArchivedSessions["openai"]);
        Assert.Equal(2, status.BackupSummary.Count);
        Assert.Equal(backupOneBytes + backupTwoBytes, status.BackupSummary.TotalBytes);
    }

    [Fact]
    public async Task RunSwitch_RejectsUnknownCustomProviders()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync(string.Empty);
        CodexSyncService service = new();

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.RunSwitchAsync(fixture.CodexHome, "missing"));
        Assert.Contains("Provider \"missing\" is not available", error.Message);
    }

    [Fact]
    public async Task RunSync_LeavesRolloutsUntouched_WhenSqliteIsLocked()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "apigather");
        await fixture.WriteStateDbAsync(
        [
            ("thread-a", "apigather", false)
        ]);

        CodexSyncService service = new();
        await using SqliteConnection connection = fixture.OpenSqliteConnection();
        await connection.OpenAsync();
        SqliteCommand begin = connection.CreateCommand();
        begin.CommandText = "BEGIN IMMEDIATE";
        await begin.ExecuteNonQueryAsync();

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.RunSyncAsync(fixture.CodexHome, sqliteBusyTimeoutMs: 0));
        Assert.Contains("state_5.sqlite is currently in use", error.Message);

        string rollout = await File.ReadAllTextAsync(sessionPath);
        Assert.Contains("\"model_provider\":\"apigather\"", rollout);
    }

    [Fact]
    public async Task RunSync_SkipsLockedRolloutFiles_AndStillUpdatesSqlite()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "apigather");
        await fixture.WriteStateDbAsync(
        [
            ("thread-a", "apigather", false)
        ]);

        CodexSyncService service = new();
        SyncResult result;
        using (FileStream lockStream = new(sessionPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            result = await service.RunSyncAsync(fixture.CodexHome, sqliteBusyTimeoutMs: 0);
        }

        Assert.Equal(0, result.ChangedSessionFiles);
        Assert.Equal(1, result.SqliteRowsUpdated);
        Assert.Equal([sessionPath], result.SkippedLockedRolloutFiles);

        string rollout = await File.ReadAllTextAsync(sessionPath);
        Assert.Contains("\"model_provider\":\"apigather\"", rollout);

        await using SqliteConnection connection = fixture.OpenSqliteConnection();
        await connection.OpenAsync();
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT model_provider FROM threads WHERE id = 'thread-a'";
        string provider = (string)(await command.ExecuteScalarAsync())!;
        Assert.Equal("openai", provider);
    }

    [Fact]
    public async Task ApplySessionChanges_SkipsFile_WhenRolloutChangesAfterCollection()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "apigather");

        SessionRolloutService service = new();
        SessionChangeCollection collected = await service.CollectSessionChangesAsync(fixture.CodexHome, "openai");

        await File.AppendAllTextAsync(
            sessionPath,
            "{\"timestamp\":\"2026-03-19T00:00:01.000Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"assistant_message\",\"message\":\"later\"}}\n");

        SessionApplyResult result = await service.ApplySessionChangesAsync(collected.Changes);

        Assert.Equal(0, result.AppliedCount);
        Assert.Equal([sessionPath], result.SkippedPaths);

        string rollout = await File.ReadAllTextAsync(sessionPath);
        Assert.Contains("\"model_provider\":\"apigather\"", rollout);
        Assert.Contains("\"message\":\"later\"", rollout);
    }

    [Fact]
    public async Task ApplySessionChanges_RewritesFile_WhenRolloutIsUnchanged()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "apigather");

        SessionRolloutService service = new();
        SessionChangeCollection collected = await service.CollectSessionChangesAsync(fixture.CodexHome, "openai");

        SessionApplyResult result = await service.ApplySessionChangesAsync(collected.Changes);

        Assert.Equal(1, result.AppliedCount);
        Assert.Empty(result.SkippedPaths);

        string rollout = await File.ReadAllTextAsync(sessionPath);
        Assert.Contains("\"model_provider\":\"openai\"", rollout);
    }

    [Fact]
    public async Task RestoreBackup_OnlyRestoresRolloutFilesThatWereActuallyApplied()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string configPath = Path.Combine(fixture.CodexHome, "config.toml");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "apigather");

        SessionRolloutService sessionService = new();
        SessionChangeCollection collected = await sessionService.CollectSessionChangesAsync(fixture.CodexHome, "openai");
        BackupService backupService = new(sessionService, new SqliteStateService());
        string backupDir = await backupService.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            collected.Changes,
            configPath);

        await backupService.UpdateSessionBackupManifestAsync(backupDir, []);
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "manual");

        await backupService.RestoreBackupAsync(
            backupDir,
            fixture.CodexHome,
            new RestoreBackupOptions
            {
                RestoreConfig = false,
                RestoreDatabase = false,
                RestoreSessions = true
            });

        string rollout = await File.ReadAllTextAsync(sessionPath);
        Assert.Contains("\"model_provider\":\"manual\"", rollout);
    }

    [Fact]
    public async Task RunSync_SkipsRolloutFile_WhenAnotherWriterAllowsSharing()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "apigather");
        await fixture.WriteStateDbAsync(
        [
            ("thread-a", "apigather", false)
        ]);

        CodexSyncService service = new();
        SyncResult result;
        using (FileStream writer = new(sessionPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete))
        {
            result = await service.RunSyncAsync(fixture.CodexHome, sqliteBusyTimeoutMs: 0);
        }

        Assert.Equal(0, result.ChangedSessionFiles);
        Assert.Equal(1, result.SqliteRowsUpdated);
        Assert.Equal([sessionPath], result.SkippedLockedRolloutFiles);

        string rollout = await File.ReadAllTextAsync(sessionPath);
        Assert.Contains("\"model_provider\":\"apigather\"", rollout);
    }

    [Fact]
    public async Task RunPruneBackups_RemovesOldestBackupDirectories()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        long oldestBytes = await fixture.WriteBackupAsync(
            "20260319T000000000Z",
            ("note.txt", "oldest"),
            ("db/state_5.sqlite", "sqlite"));
        await fixture.WriteBackupAsync("20260320T000000000Z", ("note.txt", "middle"));
        await fixture.WriteBackupAsync("20260321T000000000Z", ("note.txt", "newest"));

        CodexSyncService service = new();
        BackupPruneResult result = await service.RunPruneBackupsAsync(fixture.CodexHome, 2);

        Assert.Equal(fixture.BackupRoot(), result.BackupRoot);
        Assert.Equal(1, result.DeletedCount);
        Assert.Equal(2, result.RemainingCount);
        Assert.Equal(oldestBytes, result.FreedBytes);
        Assert.False(Directory.Exists(fixture.BackupPath("20260319T000000000Z")));
        Assert.True(Directory.Exists(fixture.BackupPath("20260320T000000000Z")));
        Assert.True(Directory.Exists(fixture.BackupPath("20260321T000000000Z")));
    }

    [Fact]
    public async Task RunPruneBackups_IgnoresDirectoriesWithoutManagedBackupMetadata()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteBackupAsync(
            "20260320T000000000Z",
            ("metadata.json", $$"""
                {
                  "version": 1,
                  "namespace": "provider-sync",
                  "codexHome": "{{fixture.CodexHome.Replace("\\", "\\\\")}}",
                  "targetProvider": "openai",
                  "createdAt": "2026-03-24T00:00:00.0000000+00:00",
                  "dbFiles": [],
                  "changedSessionFiles": 0
                }
                """));
        string junkDirectory = fixture.BackupPath("manual-notes");
        Directory.CreateDirectory(junkDirectory);
        await File.WriteAllTextAsync(Path.Combine(junkDirectory, "readme.txt"), "keep me");

        CodexSyncService service = new();
        BackupPruneResult result = await service.RunPruneBackupsAsync(fixture.CodexHome, 0);

        Assert.Equal(1, result.DeletedCount);
        Assert.Equal(0, result.RemainingCount);
        Assert.True(Directory.Exists(junkDirectory));
    }

    [Fact]
    public async Task RunSync_AutoPrunesBackupsToDefaultRetention()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "apigather");
        await fixture.WriteStateDbAsync(
        [
            ("thread-a", "apigather", false)
        ]);

        for (int index = 0; index < AppConstants.DefaultBackupRetentionCount; index += 1)
        {
            await fixture.WriteBackupAsync(
                $"20240101T0000{index:00}000Z",
                ("note.txt", $"backup-{index}"));
        }

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);

        string[] backupDirs = Directory.GetDirectories(fixture.BackupRoot());
        Assert.Equal(AppConstants.DefaultBackupRetentionCount, backupDirs.Length);
        Assert.True(Directory.Exists(result.BackupDir));
        Assert.NotNull(result.AutoPruneResult);
        Assert.Equal(1, result.AutoPruneResult!.DeletedCount);
        Assert.Equal(AppConstants.DefaultBackupRetentionCount, result.AutoPruneResult.RemainingCount);
        Assert.True(string.IsNullOrWhiteSpace(result.AutoPruneWarning));
    }

    [Fact]
    public async Task RunSync_UsesCustomAutomaticBackupRetentionCount()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "apigather");
        await fixture.WriteStateDbAsync(
        [
            ("thread-a", "apigather", false)
        ]);

        for (int index = 0; index < 4; index += 1)
        {
            await fixture.WriteBackupAsync(
                $"20240101T0000{index:00}000Z",
                ("note.txt", $"backup-{index}"));
        }

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome, keepCount: 2);

        string[] backupDirs = Directory.GetDirectories(fixture.BackupRoot());
        Assert.Equal(2, backupDirs.Length);
        Assert.True(Directory.Exists(result.BackupDir));
        Assert.NotNull(result.AutoPruneResult);
        Assert.Equal(3, result.AutoPruneResult!.DeletedCount);
        Assert.Equal(2, result.AutoPruneResult.RemainingCount);
    }
}
