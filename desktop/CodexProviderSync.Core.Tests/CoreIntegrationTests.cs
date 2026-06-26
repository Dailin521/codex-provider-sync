using System.Text.Json;
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
        Assert.Empty(syncResult.SkippedUnreadableRolloutFiles);
        Assert.Equal(2, syncResult.SqliteRowsUpdated);
        BackupMetadataFile backupMetadata = JsonSerializer.Deserialize<BackupMetadataFile>(
            await File.ReadAllTextAsync(Path.Combine(syncResult.BackupDir, "metadata.json")),
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase })!;
        Assert.Equal(
        [
            Path.Combine(AppConstants.SqliteDirBasename, AppConstants.DbFileBasename)
        ],
            backupMetadata.DbFiles);

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
    public async Task RunSync_RepairsSqliteHasUserEventFromRolloutUserMessages()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "openai");
        await fixture.WriteStateDbWithUserEventColumnAsync(
        [
            ("thread-a", "openai", false, false)
        ]);

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);

        Assert.Equal(0, result.ChangedSessionFiles);
        Assert.Equal(1, result.SqliteRowsUpdated);
        Assert.Equal(1, result.SqliteUserEventRowsUpdated);

        await using SqliteConnection connection = fixture.OpenSqliteConnection();
        await connection.OpenAsync();
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT has_user_event FROM threads WHERE id = 'thread-a'";
        long hasUserEvent = (long)(await command.ExecuteScalarAsync())!;
        Assert.Equal(1, hasUserEvent);
    }

    [Fact]
    public async Task RunSync_RepairsSqliteCwdFromRolloutSessionMetadata()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-cwd.jsonl");
        await fixture.WriteRolloutAsync(
            sessionPath,
            "thread-cwd",
            "openai",
            @"D:\GitHubProject\oss-maintainer-hub");
        await fixture.WriteStateDbWithCwdAsync(
        [
            ("thread-cwd", "openai", false, @"\\?\D:\GitHubProject\oss-maintainer-hub")
        ]);

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);

        Assert.Equal(0, result.ChangedSessionFiles);
        Assert.Equal(1, result.SqliteRowsUpdated);
        Assert.Equal(1, result.SqliteCwdRowsUpdated);

        await using SqliteConnection connection = fixture.OpenSqliteConnection();
        await connection.OpenAsync();
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT cwd FROM threads WHERE id = 'thread-cwd'";
        string cwd = (string)(await command.ExecuteScalarAsync())!;
        Assert.Equal(@"D:\GitHubProject\oss-maintainer-hub", cwd);
    }

    [Fact]
    public async Task RunSync_NormalizesExtendedRolloutCwd_BeforeRepairingSqlite()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-cwd-extended.jsonl");
        await fixture.WriteRolloutAsync(
            sessionPath,
            "thread-cwd-extended",
            "openai",
            @"\\?\E:\GitHubProject\lin-framework");
        await fixture.WriteStateDbWithCwdAsync(
        [
            ("thread-cwd-extended", "openai", false, @"\\?\E:\GitHubProject\lin-framework")
        ]);

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);

        Assert.Equal(1, result.SqliteRowsUpdated);
        Assert.Equal(1, result.SqliteCwdRowsUpdated);

        await using SqliteConnection connection = fixture.OpenSqliteConnection();
        await connection.OpenAsync();
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT cwd FROM threads WHERE id = 'thread-cwd-extended'";
        string cwd = (string)(await command.ExecuteScalarAsync())!;
        Assert.Equal(@"E:\GitHubProject\lin-framework", cwd);
    }

    [Fact]
    public async Task RunSync_RestoresWorkspaceRootsFromProjectOrder_NormalizesForDesktop_AndRestoreRevertsGlobalState()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteGlobalStateAsync(new Dictionary<string, object?>
        {
            ["electron-saved-workspace-roots"] = new[]
            {
                @"\\?\D:\GitHubProject\codex-provider-sync"
            },
            ["project-order"] = new[]
            {
                @"\\?\D:\GitHubProject\codex-provider-sync",
                @"\\?\E:\NewRich\BrainLife\Code\BrainLife\Assets"
            },
            ["active-workspace-roots"] = new[]
            {
                @"\\?\D:\GitHubProject\codex-provider-sync"
            },
            ["electron-workspace-root-labels"] = new Dictionary<string, string>
            {
                [@"\\?\E:\NewRich\BrainLife\Code\BrainLife\Assets"] = "BrainLifeAssets"
            }
        });
        await fixture.WriteStateDbWithCwdAsync(
        [
            ("thread-a", "openai", false, @"\\?\D:\GitHubProject\codex-provider-sync"),
            ("thread-b", "openai", false, @"\\?\E:\NewRich\BrainLife\Code\BrainLife\Assets")
        ]);

        CodexSyncService service = new();
        SyncResult syncResult = await service.RunSyncAsync(fixture.CodexHome);

        Assert.Equal(2, syncResult.UpdatedWorkspaceRoots);

        JsonDocument syncedState = JsonDocument.Parse(
            await File.ReadAllTextAsync(Path.Combine(fixture.CodexHome, AppConstants.GlobalStateFileBasename)));
        Assert.Equal(
        [
            @"D:\GitHubProject\codex-provider-sync",
            @"E:\NewRich\BrainLife\Code\BrainLife\Assets"
        ],
            syncedState.RootElement.GetProperty("electron-saved-workspace-roots").EnumerateArray().Select(static entry => entry.GetString()!).ToArray());
        Assert.Equal(
        [
            @"D:\GitHubProject\codex-provider-sync",
            @"E:\NewRich\BrainLife\Code\BrainLife\Assets"
        ],
            syncedState.RootElement.GetProperty("project-order").EnumerateArray().Select(static entry => entry.GetString()!).ToArray());
        Assert.Equal(
            @"D:\GitHubProject\codex-provider-sync",
            syncedState.RootElement.GetProperty("active-workspace-roots")[0].GetString());
        Assert.Equal(
            "BrainLifeAssets",
            syncedState.RootElement.GetProperty("electron-workspace-root-labels")
                .GetProperty(@"E:\NewRich\BrainLife\Code\BrainLife\Assets")
                .GetString());

        await service.RunRestoreAsync(fixture.CodexHome, syncResult.BackupDir);

        JsonDocument restoredState = JsonDocument.Parse(
            await File.ReadAllTextAsync(Path.Combine(fixture.CodexHome, AppConstants.GlobalStateFileBasename)));
        Assert.Equal(
        [
            @"\\?\D:\GitHubProject\codex-provider-sync"
        ],
            restoredState.RootElement.GetProperty("electron-saved-workspace-roots").EnumerateArray().Select(static entry => entry.GetString()!).ToArray());
        Assert.Equal(
        [
            @"\\?\D:\GitHubProject\codex-provider-sync",
            @"\\?\E:\NewRich\BrainLife\Code\BrainLife\Assets"
        ],
            restoredState.RootElement.GetProperty("project-order").EnumerateArray().Select(static entry => entry.GetString()!).ToArray());
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
        Assert.NotNull(status.StateDbLocation);
        Assert.Equal("sqlite-dir", status.StateDbLocation!.Source);
        Assert.Equal(fixture.StateDbPath(), status.StateDbLocation.Path);
        Assert.Equal(2, status.BackupSummary.Count);
        Assert.Equal(backupOneBytes + backupTwoBytes, status.BackupSummary.TotalBytes);
        Assert.Contains($"database: {fixture.StateDbPath()}", TextFormatter.FormatStatus(status));
    }

    [Fact]
    public async Task GetStatus_FallsBackToLegacyRootSqliteDatabase()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync(string.Empty);
        await using (SqliteConnection connection = fixture.OpenLegacySqliteConnection())
        {
            await connection.OpenAsync();
            SqliteCommand create = connection.CreateCommand();
            create.CommandText = """
                CREATE TABLE threads (
                  id TEXT PRIMARY KEY,
                  model_provider TEXT,
                  archived INTEGER NOT NULL DEFAULT 0
                )
                """;
            await create.ExecuteNonQueryAsync();
            SqliteCommand insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO threads (id, model_provider, archived) VALUES ('legacy-thread', 'openai', 0)";
            await insert.ExecuteNonQueryAsync();
        }

        CodexSyncService service = new();
        StatusSnapshot status = await service.GetStatusAsync(fixture.CodexHome);

        Assert.NotNull(status.StateDbLocation);
        Assert.Equal("legacy-root", status.StateDbLocation!.Source);
        Assert.Equal(fixture.LegacyStateDbPath(), status.StateDbLocation.Path);
        Assert.Equal(1, status.SqliteCounts!.Sessions["openai"]);
        Assert.Contains("legacy root", TextFormatter.FormatStatus(status));
    }

    [Fact]
    public async Task GetStatus_ReportsPendingSqliteUserEventAndCwdRepairs()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-repair-status.jsonl");
        await fixture.WriteRolloutAsync(
            sessionPath,
            "thread-repair-status",
            "openai",
            @"E:\GitHubProject\lin-framework");
        await fixture.WriteStateDbWithUserEventAndCwdAsync(
        [
            ("thread-repair-status", "openai", false, false, @"\\?\E:\GitHubProject\lin-framework")
        ]);

        CodexSyncService service = new();
        StatusSnapshot status = await service.GetStatusAsync(fixture.CodexHome);

        Assert.NotNull(status.SqliteRepairStats);
        Assert.Equal(1, status.SqliteRepairStats!.UserEventRowsNeedingRepair);
        Assert.Equal(1, status.SqliteRepairStats.CwdRowsNeedingRepair);
        string formatted = TextFormatter.FormatStatus(status);
        Assert.Contains("user-event flags needing repair: 1", formatted);
        Assert.Contains("cwd paths needing repair: 1", formatted);
    }

    [Fact]
    public async Task GetStatus_ReportsProjectVisibilityRanksAndCwdExactMatchDiagnostics()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"dal\"");
        await fixture.WriteGlobalStateAsync(new Dictionary<string, object?>
        {
            ["electron-saved-workspace-roots"] = new[]
            {
                @"E:\GitHubProject\lin-framework"
            }
        });

        List<(string Id, string ModelProvider, string Cwd, string Source, bool Archived, string FirstUserMessage, long UpdatedAtMs)> rows = [];
        for (int index = 0; index < 51; index += 1)
        {
            rows.Add(($"thread-other-{index:00}", "dal", @"D:\OtherProject", "cli", false, "hello", 1000 - index));
        }
        rows.Add(("thread-lin", "dal", @"\\?\E:\GitHubProject\lin-framework", "cli", false, "hello", 1));
        await fixture.WriteStateDbForProjectVisibilityAsync(rows);

        CodexSyncService service = new();
        StatusSnapshot status = await service.GetStatusAsync(fixture.CodexHome);
        ProjectThreadVisibility project = Assert.Single(status.ProjectThreadVisibility);

        Assert.Equal(@"E:\GitHubProject\lin-framework", project.Root);
        Assert.Equal(1, project.InteractiveThreads);
        Assert.Equal(0, project.FirstPageThreads);
        Assert.Equal([52], project.Ranks);
        Assert.Equal(0, project.ExactCwdMatches);
        Assert.Equal(1, project.VerbatimCwdRows);

        string formatted = TextFormatter.FormatStatus(status);
        Assert.Contains("Project visibility:", formatted);
        Assert.Contains("first page 0/50, ranks 52, exact cwd 0/1, verbatim cwd 1", formatted);
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
        Assert.Empty(result.SkippedUnreadableRolloutFiles);

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
        Assert.Empty(result.SkippedUnreadableRolloutFiles);

        string rollout = await File.ReadAllTextAsync(sessionPath);
        Assert.Contains("\"model_provider\":\"apigather\"", rollout);
    }

    [Fact]
    public async Task Status_SkipsLockedRolloutFile_WhenAnotherWriterAllowsSharing()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-status-locked.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-status-locked", "openai");

        CodexSyncService service = new();
        using FileStream writer = new(sessionPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
        StatusSnapshot status = await service.GetStatusAsync(fixture.CodexHome);

        Assert.Equal([sessionPath], status.LockedRolloutFiles);
        Assert.Empty(status.UnreadableRolloutFiles);
        Assert.Contains("Locked rollout files skipped during status scan: 1", TextFormatter.FormatStatus(status));
    }

    [Fact]
    public void FormatStatus_ReportsUnreadableRolloutFiles()
    {
        StatusSnapshot status = new()
        {
            CodexHome = @"C:\Users\test\.codex",
            CurrentProvider = new CurrentProviderInfo("openai", false),
            ConfiguredProviders = ["openai"],
            RolloutCounts = new ProviderCounts(),
            LockedRolloutFiles = [],
            UnreadableRolloutFiles = [@"C:\Users\test\.codex\sessions\rollout-bad.jsonl"],
            EncryptedContentCounts = new ProviderCounts(),
            SqliteCounts = null,
            BackupRoot = @"C:\Users\test\.codex\backups_state\provider-sync",
            BackupSummary = new BackupSummary
            {
                Count = 0,
                TotalBytes = 0
            }
        };

        Assert.Contains("Unreadable rollout files skipped during status scan: 1", TextFormatter.FormatStatus(status));
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

    [Fact]
    public async Task ApplySessionChanges_RestoresOriginalLastWriteTime()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        string sessionPath = fixture.RolloutPath("sessions", "rollout-mtime.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-mtime", "apigather");
        DateTime originalTime = new(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        File.SetLastWriteTimeUtc(sessionPath, originalTime);

        SessionRolloutService service = new();
        SessionChangeCollection collected = await service.CollectSessionChangesAsync(fixture.CodexHome, "openai");
        SessionApplyResult result = await service.ApplySessionChangesAsync(collected.Changes);

        Assert.Equal(1, result.AppliedCount);
        Assert.Equal(originalTime, File.GetLastWriteTimeUtc(sessionPath));
    }

    [Fact]
    public async Task Status_ReportsEncryptedContentCountsAndWarning()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-enc.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-enc", "apigather");
        await fixture.AppendEncryptedContentAsync(sessionPath);

        CodexSyncService service = new();
        StatusSnapshot status = await service.GetStatusAsync(fixture.CodexHome);

        Assert.Equal(1, status.EncryptedContentCounts.Sessions["apigather"]);
        Assert.Contains("invalid_encrypted_content", status.EncryptedContentWarning);
    }

    [Fact]
    public async Task CollectSessionChanges_StreamsLargeRolloutContent()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        string sessionPath = fixture.RolloutPath("sessions", "rollout-streamed.jsonl");
        object payload = new
        {
            id = "thread-streamed",
            timestamp = "2026-03-19T00:00:00.000Z",
            cwd = "C:\\AITemp",
            source = "cli",
            cli_version = "0.115.0",
            model_provider = "apigather"
        };
        string firstLine = JsonSerializer.Serialize(new
        {
            timestamp = "2026-03-19T00:00:00.000Z",
            type = "session_meta",
            payload
        });
        await File.WriteAllTextAsync(sessionPath, firstLine + "\n");

        const int chunkBytes = 1024 * 1024;
        const string tokenPrefix = "encrypted_";
        string userEvent = JsonSerializer.Serialize(new
        {
            type = "event_msg",
            payload = new
            {
                type = "user_message",
                message = "after large content"
            }
        });
        await File.AppendAllTextAsync(
            sessionPath,
            $"{new string('x', chunkBytes - tokenPrefix.Length)}{tokenPrefix}content\n{userEvent}\n");

        SessionRolloutService service = new();
        SessionChangeCollection collected = await service.CollectSessionChangesAsync(fixture.CodexHome, "openai");

        Assert.Equal(1, collected.EncryptedContentCounts.Sessions["apigather"]);
        Assert.Contains("thread-streamed", collected.UserEventThreadIds);
    }

    [Fact]
    public async Task RunSync_RewritesPerThreadModelColumnFromConfig()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\nmodel = \"MiniMax-M3\"\n");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "openai");
        await fixture.WriteStateDbAsync(
        [
            ("thread-a", "openai", false)
        ],
            model: "gpt-5.4-mini");

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);

        Assert.Equal(1, result.SqliteModelRowsUpdated);
        await using SqliteConnection connection = new($"Data Source={fixture.StateDbPath()};Mode=ReadOnly;Pooling=False");
        await connection.OpenAsync();
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT model, model_provider FROM threads WHERE id = 'thread-a'";
        await using SqliteDataReader reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("MiniMax-M3", reader.GetString(0));
        Assert.Equal("openai", reader.GetString(1));
    }

    [Fact]
    public async Task RunSync_LeavesPerThreadModelAlone_WhenNoRootModelConfigured()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\n");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "openai");
        await fixture.WriteStateDbAsync(
        [
            ("thread-a", "openai", false)
        ],
            model: "gpt-5.4-mini");

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);

        Assert.Equal(0, result.SqliteModelRowsUpdated);
        await using SqliteConnection connection = new($"Data Source={fixture.StateDbPath()};Mode=ReadOnly;Pooling=False");
        await connection.OpenAsync();
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT model FROM threads WHERE id = 'thread-a'";
        Assert.Equal("gpt-5.4-mini", Convert.ToString(await command.ExecuteScalarAsync()));
    }

    [Fact]
    public async Task RunSwitch_PropagatesNewModelToSqlitePerThreadColumn()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("""
            model_provider = "openai"
            model = "gpt-5.4"

            [model_providers.apigather]
            name = "apigather"
            base_url = "https://example.com"
            model = "MiniMax-M3"
            """);
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-a", "openai");
        await fixture.WriteStateDbAsync(
        [
            ("thread-a", "openai", false)
        ],
            model: "gpt-5.4");

        CodexSyncService service = new();
        SyncResult result = await service.RunSwitchAsync(
            fixture.CodexHome,
            "apigather",
            keepRootModel: false,
            model: null);

        Assert.True(result.ModelSync.Applied);
        Assert.Equal("MiniMax-M3", result.ModelSync.Model);
        Assert.Equal(1, result.SqliteModelRowsUpdated);

        await using SqliteConnection connection = new($"Data Source={fixture.StateDbPath()};Mode=ReadOnly;Pooling=False");
        await connection.OpenAsync();
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT model, model_provider FROM threads WHERE id = 'thread-a'";
        await using SqliteDataReader reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("MiniMax-M3", reader.GetString(0));
        Assert.Equal("apigather", reader.GetString(1));
    }

    [Fact]
    public async Task RunSync_RewritesTurnContextModelFieldInRolloutFiles()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\nmodel = \"MiniMax-M3\"\n");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutWithTurnContextAsync(sessionPath, "thread-a", "apigather", "gpt-5.4");

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);

        Assert.Equal(1, result.ChangedSessionFiles);
        string rewritten = await File.ReadAllTextAsync(sessionPath);
        using StringReader reader = new(rewritten);
        string? line;
        int turnContextCount = 0;
        while ((line = reader.ReadLine()) is not null)
        {
            if (!line.Contains("\"turn_context\"", StringComparison.Ordinal))
            {
                continue;
            }
            using JsonDocument doc = JsonDocument.Parse(line);
            string model = doc.RootElement.GetProperty("payload").GetProperty("model").GetString()!;
            string collabModel = doc.RootElement
                .GetProperty("payload")
                .GetProperty("collaboration_mode")
                .GetProperty("settings")
                .GetProperty("model")
                .GetString()!;
            Assert.Equal("MiniMax-M3", model);
            Assert.Equal("MiniMax-M3", collabModel);
            turnContextCount += 1;
        }
        Assert.Equal(2, turnContextCount);
    }

    [Fact]
    public async Task RunSync_UpdatesBothLegacyRootAndNewSqliteStateDatabases()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\nmodel = \"MiniMax-M3\"\n");

        // Plant a thread in BOTH the legacy root database and the
        // newer `sqlite/state_5.sqlite` location. Codex keeps the
        // legacy database alive as long as it is on disk, and the
        // GUI still reads from it for older project sessions.
        await fixture.WriteStateDbAsync(
        [
            ("legacy-thread", "codexzh", false)
        ],
            model: "gpt-5.4");
        Directory.CreateDirectory(Path.GetDirectoryName(fixture.LegacyStateDbPath())!);
        await using (SqliteConnection legacy = new($"Data Source={fixture.LegacyStateDbPath()};Mode=ReadWriteCreate;Pooling=False"))
        {
            await legacy.OpenAsync();
            using SqliteCommand create = legacy.CreateCommand();
            create.CommandText = """
                CREATE TABLE threads (
                  id TEXT PRIMARY KEY,
                  model_provider TEXT,
                  cwd TEXT NOT NULL DEFAULT '',
                  archived INTEGER NOT NULL DEFAULT 0,
                  first_user_message TEXT NOT NULL DEFAULT '',
                  model TEXT
                )
                """;
            await create.ExecuteNonQueryAsync();
            using SqliteCommand insert = legacy.CreateCommand();
            insert.CommandText = """
                INSERT INTO threads (id, model_provider, cwd, archived, first_user_message, model)
                VALUES ($id, $provider, 'C:\AITemp', 0, 'hi', $model)
                """;
            insert.Parameters.AddWithValue("$id", "legacy-thread");
            insert.Parameters.AddWithValue("$provider", "codexzh");
            insert.Parameters.AddWithValue("$model", "gpt-5.4");
            await insert.ExecuteNonQueryAsync();
        }

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);

        Assert.True(result.SqlitePresent);
        // The provider + model column on the legacy row must now
        // match the active root-level values, otherwise the GUI
        // will keep showing the old provider label for the older
        // project sessions it loaded from this database.
        await using SqliteConnection legacyReader = new($"Data Source={fixture.LegacyStateDbPath()};Mode=ReadOnly;Pooling=False");
        await legacyReader.OpenAsync();
        await using SqliteCommand legacyCommand = legacyReader.CreateCommand();
        legacyCommand.CommandText = "SELECT model_provider, model FROM threads WHERE id = 'legacy-thread'";
        await using SqliteDataReader reader = await legacyCommand.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("openai", reader.GetString(0));
        Assert.Equal("MiniMax-M3", reader.GetString(1));
    }

    [Fact]
    public async Task RunSync_RewritesTurnContextModelFieldWhenProviderAlreadyMatches()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\nmodel = \"MiniMax-M3\"\n");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutWithTurnContextAsync(sessionPath, "thread-a", "openai", "gpt-5.4");

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);

        Assert.Equal(1, result.ChangedSessionFiles);
        string rewritten = await File.ReadAllTextAsync(sessionPath);
        using StringReader reader = new(rewritten);
        string? line;
        int turnContextCount = 0;
        while ((line = reader.ReadLine()) is not null)
        {
            if (!line.Contains("\"turn_context\"", StringComparison.Ordinal))
            {
                continue;
            }
            using JsonDocument doc = JsonDocument.Parse(line);
            string model = doc.RootElement.GetProperty("payload").GetProperty("model").GetString()!;
            Assert.Equal("MiniMax-M3", model);
            turnContextCount += 1;
        }
        Assert.Equal(2, turnContextCount);
    }

    [Fact]
    public async Task RunSync_LeavesTurnContextModelFieldAlone_WhenNoRootModelConfigured()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\n");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        await fixture.WriteRolloutWithTurnContextAsync(sessionPath, "thread-a", "apigather", "gpt-5.4");

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);

        Assert.Equal(1, result.ChangedSessionFiles);
        string rewritten = await File.ReadAllTextAsync(sessionPath);
        using StringReader reader = new(rewritten);
        string? line;
        int turnContextCount = 0;
        while ((line = reader.ReadLine()) is not null)
        {
            if (!line.Contains("\"turn_context\"", StringComparison.Ordinal))
            {
                continue;
            }
            using JsonDocument doc = JsonDocument.Parse(line);
            string model = doc.RootElement.GetProperty("payload").GetProperty("model").GetString()!;
            Assert.Equal("gpt-5.4", model);
            turnContextCount += 1;
        }
        Assert.Equal(2, turnContextCount);
    }

    [Fact]
    public async Task Status_ReturnsMalformedSqliteAsUnreadable()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        Directory.CreateDirectory(Path.GetDirectoryName(fixture.StateDbPath())!);
        await File.WriteAllTextAsync(fixture.StateDbPath(), "not sqlite");

        CodexSyncService service = new();
        StatusSnapshot status = await service.GetStatusAsync(fixture.CodexHome);

        Assert.True(status.SqliteCounts!.Unreadable);
        Assert.Contains("malformed", TextFormatter.FormatStatus(status));
    }

    [Fact]
    public async Task RestoreBackup_CanSkipConfigDatabaseAndSessions()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-skip.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-skip", "apigather");

        SessionRolloutService sessionService = new();
        SessionChangeCollection collected = await sessionService.CollectSessionChangesAsync(fixture.CodexHome, "openai");
        BackupService backupService = new(sessionService, new SqliteStateService());
        string backupDir = await backupService.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            collected.Changes,
            Path.Combine(fixture.CodexHome, "config.toml"));

        await fixture.WriteConfigAsync("model_provider = \"manual\"");
        await fixture.WriteRolloutAsync(sessionPath, "thread-skip", "manual");
        await backupService.RestoreBackupAsync(
            backupDir,
            fixture.CodexHome,
            new RestoreBackupOptions
            {
                RestoreConfig = false,
                RestoreDatabase = false,
                RestoreSessions = false
            });

        Assert.Contains("model_provider = \"manual\"", await File.ReadAllTextAsync(Path.Combine(fixture.CodexHome, "config.toml")));
        Assert.Contains("\"model_provider\":\"manual\"", await File.ReadAllTextAsync(sessionPath));
    }
}
