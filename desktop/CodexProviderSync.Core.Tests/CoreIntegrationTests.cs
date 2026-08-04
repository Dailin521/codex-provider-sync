using System.Text.Json;
using System.Diagnostics;
using Microsoft.Data.Sqlite;

namespace CodexProviderSync.Core.Tests;

public sealed class CoreIntegrationTests
{
    [Fact]
    public async Task RunSync_RollsBackFirstRollout_WhenLaterTargetFails_Issue69()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string firstPath = fixture.RolloutPath("sessions", "rollout-a.jsonl");
        string secondPath = fixture.RolloutPath("sessions", "rollout-b.jsonl");
        await fixture.WriteRolloutAsync(firstPath, "thread-a", "apigather");
        await fixture.WriteRolloutAsync(secondPath, "thread-b", "apigather");
        await fixture.WriteStateDbAsync([
            ("thread-a", "apigather", false),
            ("thread-b", "apigather", false)
        ]);
        string firstBefore = await File.ReadAllTextAsync(firstPath);
        string secondBefore = await File.ReadAllTextAsync(secondPath);

        CodexSyncService service = new();
        service.FaultInjector = (point, _, appliedCount) =>
        {
            if (point == "before_rollout_apply" && appliedCount == 2)
            {
                throw new IOException("injected second-target failure");
            }
            return Task.CompletedTask;
        };

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunSyncAsync(fixture.CodexHome, provider: "openai"));
        Assert.Contains("injected second-target failure", error.OriginalError.Message);
        Assert.Equal("complete", error.RollbackStatus);
        Assert.False(error.RecoveryRequired);
        Assert.Equal(
            RelativeTargetIdentity(fixture.CodexHome, firstPath),
            RelativeTargetIdentity(fixture.CodexHome, Assert.Single(error.CompletedTargets)));
        Assert.Equal(firstBefore, await File.ReadAllTextAsync(firstPath));
        Assert.Equal(secondBefore, await File.ReadAllTextAsync(secondPath));
        Assert.Equal("apigather", await ReadProviderAsync(fixture.StateDbPath(), "thread-a"));
        Assert.Equal("apigather", await ReadProviderAsync(fixture.StateDbPath(), "thread-b"));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task RunSync_RestoresGlobalStatePrimary_WhenBackupWriteFails_Issue69()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteGlobalStateAsync(new Dictionary<string, object?>
        {
            ["electron-saved-workspace-roots"] = new[] { @"\\?\D:\Workspace\sample" },
            ["project-order"] = new[] { @"\\?\D:\Workspace\sample" },
            ["active-workspace-roots"] = new[] { @"\\?\D:\Workspace\sample" }
        });
        await fixture.WriteStateDbWithCwdAsync([
            ("thread-global", "openai", false, @"\\?\D:\Workspace\sample")
        ]);
        string primaryPath = Path.Combine(fixture.CodexHome, AppConstants.GlobalStateFileBasename);
        string backupPath = Path.Combine(fixture.CodexHome, AppConstants.GlobalStateBackupFileBasename);
        string primaryBefore = await File.ReadAllTextAsync(primaryPath);
        string backupBefore = await File.ReadAllTextAsync(backupPath);

        CodexSyncService service = new();
        service.FaultInjector = (point, appliedPath, _) =>
        {
            if (point == "after_global_state_apply"
                && string.Equals(appliedPath, primaryPath, StringComparison.Ordinal))
            {
                throw new IOException("injected global-state backup failure");
            }
            return Task.CompletedTask;
        };

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunSyncAsync(fixture.CodexHome));
        Assert.Contains("injected global-state backup failure", error.OriginalError.Message);
        Assert.Equal("complete", error.RollbackStatus);
        Assert.False(error.RecoveryRequired);
        Assert.Equal(primaryBefore, await File.ReadAllTextAsync(primaryPath));
        Assert.Equal(backupBefore, await File.ReadAllTextAsync(backupPath));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task FailureAfterSqliteCommit_RestoresRolloutAndDatabase()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-after-sqlite.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-after-sqlite", "apigather");
        await fixture.WriteStateDbAsync([("thread-after-sqlite", "apigather", false)]);
        string before = await File.ReadAllTextAsync(sessionPath);

        CodexSyncService service = new();
        service.FaultInjector = (point, _, _) =>
        {
            if (point == "after_sqlite_commit")
            {
                throw new IOException("injected post-SQLite failure");
            }
            return Task.CompletedTask;
        };

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunSyncAsync(fixture.CodexHome, provider: "openai"));

        Assert.Contains("injected post-SQLite failure", error.OriginalError.Message);
        Assert.Equal("complete", error.RollbackStatus);
        Assert.False(error.RecoveryRequired);
        Assert.Contains(
            RelativeTargetIdentity(fixture.CodexHome, sessionPath),
            error.CompletedTargets.Select(path => RelativeTargetIdentity(fixture.CodexHome, path)));
        Assert.Contains(
            RelativeTargetIdentity(fixture.CodexHome, fixture.StateDbPath()),
            error.CompletedTargets.Select(path => RelativeTargetIdentity(fixture.CodexHome, path)));
        Assert.Equal(before, await File.ReadAllTextAsync(sessionPath));
        Assert.Equal("apigather", await ReadProviderAsync(fixture.StateDbPath(), "thread-after-sqlite"));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task SqliteCommitAcknowledgementFailure_RestoresConfigRolloutAndDatabase()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync(
            "model_provider = \"openai\"\n"
            + "model = \"gpt-5.4-mini\"\n\n"
            + "[model_providers.apigather]\n"
            + "model = \"apigather-prod\"\n"
            + "base_url = \"https://example.com\"\n");
        string configPath = Path.Combine(fixture.CodexHome, "config.toml");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-commit-ack-unknown.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-commit-ack-unknown", "openai");
        await fixture.WriteStateDbAsync([("thread-commit-ack-unknown", "openai", false)]);
        string configBefore = await File.ReadAllTextAsync(configPath);
        string rolloutBefore = await File.ReadAllTextAsync(sessionPath);
        bool durableSqliteMutationObserved = false;

        CodexSyncService service = new();
        service.FaultInjector = async (point, _, _) =>
        {
            if (point != "after_sqlite_commit_before_ack")
            {
                return;
            }

            durableSqliteMutationObserved = string.Equals(
                "apigather",
                await ReadProviderAsync(fixture.StateDbPath(), "thread-commit-ack-unknown"),
                StringComparison.Ordinal);
            throw new IOException("injected lost SQLite commit acknowledgement");
        };

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunSwitchAsync(fixture.CodexHome, "apigather"));

        Assert.True(durableSqliteMutationObserved);
        Assert.Contains("injected lost SQLite commit acknowledgement", error.OriginalError.Message);
        Assert.Equal("complete", error.RollbackStatus);
        Assert.False(error.RecoveryRequired);
        Assert.Equal(configBefore, await File.ReadAllTextAsync(configPath));
        Assert.Equal(rolloutBefore, await File.ReadAllTextAsync(sessionPath));
        Assert.Equal(
            "openai",
            await ReadProviderAsync(fixture.StateDbPath(), "thread-commit-ack-unknown"));

        string journalPath = Path.Combine(error.BackupDirectory, FileTransactionJournal.FileName);
        PendingTransactionInfo journal = await FileTransactionJournal.ReadInfoAsync(journalPath);
        Assert.True(journal.Terminal);
        Assert.False(journal.InvalidTail);
        Assert.Equal("rolledBack", journal.State);
        Assert.Contains(journal.AffectedTargets, target =>
            target.Kind == "sqlite" && target.State == "applying");
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task CancellationAfterSqliteCommit_RestoresRolloutAndDatabase()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-cancel-after-sqlite.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-cancel-after-sqlite", "apigather");
        await fixture.WriteStateDbAsync([("thread-cancel-after-sqlite", "apigather", false)]);
        string before = await File.ReadAllTextAsync(sessionPath);
        using CancellationTokenSource cancellation = new();

        CodexSyncService service = new();
        service.FaultInjector = (point, _, _) =>
        {
            if (point == "after_sqlite_commit")
            {
                cancellation.Cancel();
            }
            return Task.CompletedTask;
        };

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunSyncAsync(
                fixture.CodexHome,
                provider: "openai",
                cancellationToken: cancellation.Token));

        Assert.IsType<OperationCanceledException>(error.OriginalError);
        Assert.True(error.WasCanceled);
        Assert.Equal("complete", error.RollbackStatus);
        Assert.False(error.RecoveryRequired);
        Assert.Equal(before, await File.ReadAllTextAsync(sessionPath));
        Assert.Equal("apigather", await ReadProviderAsync(fixture.StateDbPath(), "thread-cancel-after-sqlite"));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task CancellationAfterTransactionCommit_DoesNotRollBackCommittedState()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-cancel-after-commit.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-cancel-after-commit", "apigather");
        await fixture.WriteStateDbAsync([("thread-cancel-after-commit", "apigather", false)]);
        using CancellationTokenSource cancellation = new();

        CodexSyncService service = new();
        service.FaultInjector = (point, _, _) =>
        {
            if (point == "after_transaction_commit")
            {
                cancellation.Cancel();
            }
            return Task.CompletedTask;
        };

        SyncResult result = await service.RunSyncAsync(
            fixture.CodexHome,
            provider: "openai",
            cancellationToken: cancellation.Token);

        Assert.True(cancellation.IsCancellationRequested);
        Assert.Equal(1, result.ChangedSessionFiles);
        Assert.Equal("openai", await ReadProviderAsync(fixture.StateDbPath(), "thread-cancel-after-commit"));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task FailureBeforeTransactionCommit_RollsBackBeforePruningOldBackups()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-before-commit-failure.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-before-commit", "apigather");
        await fixture.WriteStateDbAsync([("thread-before-commit", "apigather", false)]);
        string before = await File.ReadAllTextAsync(sessionPath);
        const string oldBackupName = "20260319T000000000Z";
        await fixture.WriteBackupAsync(oldBackupName, ("note.txt", "must survive rollback"));
        string oldBackupDir = fixture.BackupPath(oldBackupName);

        CodexSyncService service = new();
        service.FaultInjector = (point, _, _) =>
        {
            if (point == "before_transaction_commit")
            {
                throw new IOException("injected transaction-commit failure");
            }
            return Task.CompletedTask;
        };

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunSyncAsync(fixture.CodexHome, provider: "openai", keepCount: 1));

        Assert.Contains("injected transaction-commit failure", error.OriginalError.Message);
        Assert.Equal("complete", error.RollbackStatus);
        Assert.False(error.RecoveryRequired);
        Assert.True(Directory.Exists(oldBackupDir));
        Assert.Equal(before, await File.ReadAllTextAsync(sessionPath));
        Assert.Equal("apigather", await ReadProviderAsync(fixture.StateDbPath(), "thread-before-commit"));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task SqliteRollbackFailure_PreservesRecoveryEvidence_AndManualRestoreRecovers()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-sqlite-rollback-failure.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-sqlite-rollback", "apigather");
        await fixture.WriteStateDbAsync([("thread-sqlite-rollback", "apigather", false)]);
        string before = await File.ReadAllTextAsync(sessionPath);

        CodexSyncService service = new();
        service.FaultInjector = (point, _, _) =>
        {
            if (point == "after_sqlite_commit")
            {
                throw new IOException("injected post-SQLite failure");
            }
            if (point == "before_sqlite_rollback")
            {
                throw new IOException("injected SQLite rollback failure");
            }
            return Task.CompletedTask;
        };

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunSyncAsync(fixture.CodexHome, provider: "openai"));

        Assert.True(error.RecoveryRequired);
        Assert.Equal("incomplete", error.RollbackStatus);
        Assert.Contains(error.RollbackErrors, value => value.Contains("injected SQLite rollback failure"));
        Assert.Equal(before, await File.ReadAllTextAsync(sessionPath));
        Assert.Equal("openai", await ReadProviderAsync(fixture.StateDbPath(), "thread-sqlite-rollback"));
        Assert.Single(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));

        await service.RunRestoreAsync(fixture.CodexHome, error.BackupDirectory);
        Assert.Equal(before, await File.ReadAllTextAsync(sessionPath));
        Assert.Equal("apigather", await ReadProviderAsync(fixture.StateDbPath(), "thread-sqlite-rollback"));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task UnfinishedJournal_BlocksWrites_UntilBoundBackupIsRestored()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteStateDbAsync([("thread-recovery", "openai", false)]);
        string configPath = Path.Combine(fixture.CodexHome, "config.toml");
        BackupService backupService = new(new SessionRolloutService(), new SqliteStateService());
        string backupDir = await backupService.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            [],
            configPath);
        await FileTransactionJournal.CreateAsync(
            backupDir,
            fixture.CodexHome,
            "openai",
            [configPath]);

        CodexSyncService service = new();
        StatusSnapshot status = await service.GetStatusAsync(fixture.CodexHome);
        Assert.Single(status.PendingTransactions);
        Assert.Contains("Recovery required:", TextFormatter.FormatStatus(status));
        Assert.Contains("需要恢复:", TextFormatter.FormatStatus(status, TextFormatter.ChineseSimplified));
        RecoveryRequiredException error = await Assert.ThrowsAsync<RecoveryRequiredException>(
            () => service.RunSyncAsync(fixture.CodexHome));
        Assert.Single(error.PendingBackupDirectories);

        await service.RunRestoreAsync(fixture.CodexHome, backupDir);
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);
        Assert.Equal("openai", result.TargetProvider);
    }

    [Fact]
    public async Task CrashRecovery_RestoresActuallyMutatedRolloutAndDatabase_FromPendingJournal()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-crash-recovery.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-crash-recovery", "apigather");
        await fixture.WriteStateDbAsync([("thread-crash-recovery", "apigather", false)]);
        string before = await File.ReadAllTextAsync(sessionPath);

        SessionRolloutService rolloutService = new();
        SqliteStateService sqliteService = new();
        SessionChangeCollection changes = await rolloutService.CollectSessionChangesAsync(
            fixture.CodexHome,
            "openai");
        BackupService backupService = new(rolloutService, sqliteService);
        string backupDir = await backupService.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            changes.Changes,
            Path.Combine(fixture.CodexHome, "config.toml"));
        FileTransactionJournal journal = await FileTransactionJournal.CreateAsync(
            backupDir,
            fixture.CodexHome,
            "openai",
            [sessionPath, fixture.StateDbPath()]);

        await journal.ApplyingAsync("rollout", sessionPath);
        await rolloutService.ApplySessionChangesAsync(changes.Changes);
        await journal.AppliedAsync("rollout", sessionPath);
        await journal.ApplyingAsync("sqlite", fixture.StateDbPath());
        await sqliteService.UpdateSqliteProviderAsync(fixture.CodexHome, "openai");
        await journal.AppliedAsync("sqlite", fixture.StateDbPath());

        Assert.NotEqual(before, await File.ReadAllTextAsync(sessionPath));
        Assert.Equal("openai", await ReadProviderAsync(fixture.StateDbPath(), "thread-crash-recovery"));
        CodexSyncService service = new();
        Assert.Single((await service.GetStatusAsync(fixture.CodexHome)).PendingTransactions);
        await Assert.ThrowsAsync<RecoveryRequiredException>(
            () => service.RunSyncAsync(fixture.CodexHome));

        await service.RunRestoreAsync(fixture.CodexHome, backupDir);

        Assert.Equal(before, await File.ReadAllTextAsync(sessionPath));
        Assert.Equal("apigather", await ReadProviderAsync(fixture.StateDbPath(), "thread-crash-recovery"));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task RollbackFailure_PreservesBothErrors_AndManualRecoveryEvidence()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-rollback-failure.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-rollback", "apigather");
        await fixture.WriteStateDbAsync([("thread-rollback", "apigather", false)]);

        CodexSyncService service = new();
        service.FaultInjector = (point, _, _) =>
        {
            if (point == "after_rollout_apply")
            {
                throw new IOException("injected original failure");
            }
            if (point == "before_rollout_rollback")
            {
                throw new IOException("injected rollback failure");
            }
            return Task.CompletedTask;
        };

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunSyncAsync(fixture.CodexHome));
        Assert.Contains("injected original failure", error.OriginalError.Message);
        Assert.Contains(error.RollbackErrors, value => value.Contains("injected rollback failure"));
        Assert.Equal("incomplete", error.RollbackStatus);
        Assert.True(error.RecoveryRequired);
        Assert.Single(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));

        await service.RunRestoreAsync(fixture.CodexHome, error.BackupDirectory);
        using JsonDocument restored = JsonDocument.Parse(
            (await File.ReadAllTextAsync(sessionPath)).Split('\n', StringSplitOptions.RemoveEmptyEntries)[0]);
        Assert.Equal("apigather", restored.RootElement.GetProperty("payload").GetProperty("model_provider").GetString());
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task Cancellation_AfterFirstTarget_RollsBackDiskAndSqlite_WithStructuredEvidence()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string firstPath = fixture.RolloutPath("sessions", "rollout-cancel-a.jsonl");
        string secondPath = fixture.RolloutPath("sessions", "rollout-cancel-b.jsonl");
        await fixture.WriteRolloutAsync(firstPath, "thread-cancel-a", "apigather");
        await fixture.WriteRolloutAsync(secondPath, "thread-cancel-b", "apigather");
        await fixture.WriteStateDbAsync([
            ("thread-cancel-a", "apigather", false),
            ("thread-cancel-b", "apigather", false)
        ]);
        string firstBefore = await File.ReadAllTextAsync(firstPath);
        string secondBefore = await File.ReadAllTextAsync(secondPath);
        using CancellationTokenSource cancellation = new();
        CodexSyncService service = new();
        string? cancelledAfterPath = null;
        service.FaultInjector = (point, appliedPath, appliedCount) =>
        {
            if (point == "after_rollout_apply" && appliedCount == 1)
            {
                cancelledAfterPath = appliedPath;
                cancellation.Cancel();
            }
            return Task.CompletedTask;
        };

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunSyncAsync(
                fixture.CodexHome,
                provider: "openai",
                cancellationToken: cancellation.Token));

        Assert.IsType<OperationCanceledException>(error.OriginalError);
        Assert.True(error.WasCanceled);
        Assert.Equal("SYNC_FAILED_ROLLED_BACK", error.Code);
        Assert.Equal("complete", error.RollbackStatus);
        Assert.False(error.RecoveryRequired);
        string observedRelativeTarget = RelativeTargetIdentity(
            fixture.CodexHome,
            Assert.IsType<string>(cancelledAfterPath));
        Assert.Equal(
            RelativeTargetIdentity(fixture.CodexHome, firstPath),
            observedRelativeTarget);
        Assert.Equal(
            RelativeTargetIdentity(fixture.CodexHome, firstPath),
            RelativeTargetIdentity(fixture.CodexHome, Assert.Single(error.CompletedTargets)));
        Assert.Equal(firstBefore, await File.ReadAllTextAsync(firstPath));
        Assert.Equal(secondBefore, await File.ReadAllTextAsync(secondPath));
        Assert.Equal("apigather", await ReadProviderAsync(fixture.StateDbPath(), "thread-cancel-a"));
        Assert.Equal("apigather", await ReadProviderAsync(fixture.StateDbPath(), "thread-cancel-b"));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task Rollback_ContinuesPerTarget_WhenOneRolloutRestoreFails()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string firstPath = fixture.RolloutPath("sessions", "rollout-per-target-a.jsonl");
        string secondPath = fixture.RolloutPath("sessions", "rollout-per-target-b.jsonl");
        await fixture.WriteRolloutAsync(firstPath, "thread-per-target-a", "apigather");
        await fixture.WriteRolloutAsync(secondPath, "thread-per-target-b", "apigather");
        string firstBefore = await File.ReadAllTextAsync(firstPath);
        string secondBefore = await File.ReadAllTextAsync(secondPath);
        CodexSyncService service = new();
        service.FaultInjector = (point, path, count) =>
        {
            if (point == "after_rollout_apply" && count == 2)
            {
                throw new IOException("injected after both rollout writes");
            }
            if (point == "before_rollout_rollback"
                && string.Equals(Path.GetFullPath(path!), Path.GetFullPath(secondPath), StringComparison.OrdinalIgnoreCase))
            {
                throw new IOException("injected second rollout restore failure");
            }
            return Task.CompletedTask;
        };

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunSyncAsync(fixture.CodexHome));

        Assert.True(error.RecoveryRequired);
        Assert.Equal(firstBefore, await File.ReadAllTextAsync(firstPath));
        Assert.NotEqual(secondBefore, await File.ReadAllTextAsync(secondPath));
        Assert.Contains(error.RollbackErrors, failure =>
            failure.Contains(secondPath, StringComparison.Ordinal)
            && failure.Contains("injected second rollout restore failure", StringComparison.Ordinal));

        await new CodexSyncService().RunRestoreAsync(
            fixture.CodexHome,
            error.BackupDirectory,
            new RestoreBackupOptions
            {
                RestoreConfig = false,
                RestoreDatabase = false,
                RestoreSessions = true
            });
        Assert.Equal(firstBefore, await File.ReadAllTextAsync(firstPath));
        Assert.Equal(secondBefore, await File.ReadAllTextAsync(secondPath));
    }

    [Fact]
    public async Task Rollback_DoesNotRestoreSqlite_WhenItsTransactionNeverCommitted()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-sqlite-not-committed.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-sqlite-not-committed", "apigather");
        await fixture.WriteStateDbAsync([("thread-sqlite-not-committed", "apigather", false)]);
        bool concurrentMarkerWritten = false;
        bool sqliteCommitAttemptObserved = false;
        CodexSyncService service = new();
        service.FaultInjector = async (point, _, _) =>
        {
            if (point == "after_rollout_apply")
            {
                throw new IOException("injected before SQLite commit");
            }
            if (point == "before_rollout_rollback" && !concurrentMarkerWritten)
            {
                await using SqliteConnection connection = fixture.OpenSqliteConnection();
                await connection.OpenAsync();
                SqliteCommand command = connection.CreateCommand();
                command.CommandText = """
                    INSERT INTO threads (id, model_provider, cwd, archived, first_user_message)
                    VALUES ('concurrent-marker', 'external', '', 0, 'external')
                    """;
                await command.ExecuteNonQueryAsync();
                concurrentMarkerWritten = true;
            }
            if (point == "after_sqlite_commit_before_ack")
            {
                sqliteCommitAttemptObserved = true;
            }
        };

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunSyncAsync(fixture.CodexHome));

        Assert.False(error.RecoveryRequired);
        Assert.True(concurrentMarkerWritten);
        Assert.False(sqliteCommitAttemptObserved);
        Assert.Equal("apigather", await ReadProviderAsync(fixture.StateDbPath(), "thread-sqlite-not-committed"));
        Assert.Equal("external", await ReadProviderAsync(fixture.StateDbPath(), "concurrent-marker"));
    }

    [Fact]
    public async Task Cancellation_AfterOnlyRollout_IsObservedBeforeSqliteCommit()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-cancel-only.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-cancel-only", "apigather");
        await fixture.WriteStateDbAsync([("thread-cancel-only", "apigather", false)]);
        string before = await File.ReadAllTextAsync(sessionPath);
        using CancellationTokenSource cancellation = new();
        CodexSyncService service = new();
        service.FaultInjector = (point, _, appliedCount) =>
        {
            if (point == "after_rollout_apply" && appliedCount == 1)
            {
                cancellation.Cancel();
            }
            return Task.CompletedTask;
        };

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunSyncAsync(
                fixture.CodexHome,
                provider: "openai",
                cancellationToken: cancellation.Token));

        Assert.IsType<OperationCanceledException>(error.OriginalError);
        Assert.True(error.WasCanceled);
        Assert.Equal("complete", error.RollbackStatus);
        Assert.False(error.RecoveryRequired);
        Assert.Equal(
            RelativeTargetIdentity(fixture.CodexHome, sessionPath),
            RelativeTargetIdentity(fixture.CodexHome, Assert.Single(error.CompletedTargets)));
        Assert.Equal(before, await File.ReadAllTextAsync(sessionPath));
        Assert.Equal("apigather", await ReadProviderAsync(fixture.StateDbPath(), "thread-cancel-only"));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task ConcurrentSync_IsRejectedByOperationLock_WithoutCompetingMutation()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-concurrent.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-concurrent", "apigather");
        await fixture.WriteStateDbAsync([("thread-concurrent", "apigather", false)]);
        TaskCompletionSource<bool> firstMutationObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<bool> releaseFirstOperation = new(TaskCreationOptions.RunContinuationsAsynchronously);
        CodexSyncService firstService = new();
        firstService.FaultInjector = async (point, _, appliedCount) =>
        {
            if (point == "after_rollout_apply" && appliedCount == 1)
            {
                firstMutationObserved.TrySetResult(true);
                await releaseFirstOperation.Task;
            }
        };

        Task<SyncResult> firstSync = firstService.RunSyncAsync(fixture.CodexHome, provider: "openai");
        await firstMutationObserved.Task.WaitAsync(TimeSpan.FromSeconds(10));
        try
        {
            InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
                () => new CodexSyncService().RunSyncAsync(fixture.CodexHome, provider: "openai"));
            Assert.Contains("Lock already exists", error.Message);
        }
        finally
        {
            releaseFirstOperation.TrySetResult(true);
        }

        SyncResult result = await firstSync.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(1, result.ChangedSessionFiles);
        Assert.Equal("openai", await ReadProviderAsync(fixture.StateDbPath(), "thread-concurrent"));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task RepeatedSync_IsIdempotentForRolloutAndSqliteState()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-idempotent.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-idempotent", "apigather");
        await fixture.WriteStateDbAsync([("thread-idempotent", "apigather", false)]);
        CodexSyncService service = new();

        SyncResult first = await service.RunSyncAsync(fixture.CodexHome, provider: "openai");
        string afterFirst = await File.ReadAllTextAsync(sessionPath);
        SyncResult second = await service.RunSyncAsync(fixture.CodexHome, provider: "openai");

        Assert.Equal(1, first.ChangedSessionFiles);
        Assert.Equal(1, first.SqliteProviderRowsUpdated);
        Assert.Equal(0, second.ChangedSessionFiles);
        Assert.Equal(0, second.SqliteProviderRowsUpdated);
        Assert.Equal(0, second.SqliteRowsUpdated);
        Assert.Equal(afterFirst, await File.ReadAllTextAsync(sessionPath));
        Assert.Equal("openai", await ReadProviderAsync(fixture.StateDbPath(), "thread-idempotent"));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task BackupFailure_OccursBeforeJournalOrTargetMutation()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-backup-failure.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-backup-failure", "apigather");
        await fixture.WriteStateDbAsync([("thread-backup-failure", "apigather", false)]);
        string before = await File.ReadAllTextAsync(sessionPath);
        CodexSyncService service = new();
        service.FaultInjector = (point, _, _) =>
        {
            if (point == "before_backup")
            {
                throw new IOException("injected backup creation failure");
            }
            return Task.CompletedTask;
        };

        IOException error = await Assert.ThrowsAsync<IOException>(
            () => service.RunSyncAsync(fixture.CodexHome));
        Assert.Contains("injected backup creation failure", error.Message);
        Assert.Equal(before, await File.ReadAllTextAsync(sessionPath));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
        Assert.False(Directory.Exists(fixture.BackupRoot()));
    }

    [Fact]
    public async Task AtomicReplacementFailure_PreservesOriginalAndRemovesStaging()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        string targetPath = Path.Combine(fixture.Root, "atomic-target.txt");
        await File.WriteAllTextAsync(targetPath, "before");
        await using (FileStream locked = new(
            targetPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.None))
        {
            await Assert.ThrowsAnyAsync<Exception>(
                () => AtomicFile.WriteAllTextAsync(targetPath, "after"));
        }
        Assert.Equal("before", await File.ReadAllTextAsync(targetPath));
        Assert.Empty(Directory.GetFiles(fixture.Root, "*.provider-sync.*.tmp"));
    }

    [Theory]
    [InlineData("before_stage_write")]
    [InlineData("before_atomic_replace")]
    public async Task AtomicWriter_InjectedFailure_PreservesOriginalAndRemovesStaging(string faultPoint)
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        string targetPath = Path.Combine(fixture.Root, $"atomic-{faultPoint}.txt");
        await File.WriteAllTextAsync(targetPath, "before");

        IOException error = await Assert.ThrowsAsync<IOException>(
            () => AtomicFile.WriteAllTextAsync(
                targetPath,
                "after",
                faultInjector: (point, _, _) =>
                {
                    if (point == faultPoint)
                    {
                        throw new IOException($"injected {faultPoint}");
                    }
                    return Task.CompletedTask;
                }));

        Assert.Contains($"injected {faultPoint}", error.Message);
        Assert.Equal("before", await File.ReadAllTextAsync(targetPath));
        Assert.Empty(Directory.GetFiles(fixture.Root, "*.provider-sync.*.tmp"));
    }

    [Fact]
    public async Task RestoreSqlite_OnlineBackupFailurePreservesCurrentWalDatabase()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteStateDbAsync([
            ("thread-atomic-restore", "openai", false)
        ]);
        SessionRolloutService rollouts = new();
        BackupService backups = new(rollouts, new SqliteStateService());
        string backupDir = await backups.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            [],
            Path.Combine(fixture.CodexHome, "config.toml"));
        string stateDbPath = fixture.StateDbPath();
        await using SqliteConnection connection = fixture.OpenSqliteConnection();
        await connection.OpenAsync();
        await using (SqliteCommand wal = connection.CreateCommand())
        {
            wal.CommandText = "PRAGMA journal_mode = WAL";
            Assert.Equal("wal", Convert.ToString(await wal.ExecuteScalarAsync()));
        }
        await using (SqliteCommand update = connection.CreateCommand())
        {
            update.CommandText = "UPDATE threads SET model_provider = 'apigather' WHERE id = 'thread-atomic-restore'";
            Assert.Equal(1, await update.ExecuteNonQueryAsync());
        }
        Assert.True(new FileInfo(stateDbPath + "-wal").Length > 0);
        string backupDbPath = Path.Combine(
            backupDir,
            "db",
            "sqlite-home",
            AppConstants.DbFileBasename);
        await File.WriteAllTextAsync(backupDbPath, "not a sqlite database");

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => backups.RestoreBackupAsync(
                backupDir,
                fixture.CodexHome,
                new RestoreBackupOptions
                {
                    RestoreConfig = false,
                    RestoreDatabase = true,
                    RestoreSessions = false
                }));

        Assert.Contains("malformed", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(File.Exists(stateDbPath + "-wal"));
        Assert.Equal("apigather", await ReadProviderAsync(stateDbPath, "thread-atomic-restore"));
    }

    [Fact]
    public async Task PruneBackups_NeverDeletesBackupReferencedByUnfinishedTransaction()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteBackupAsync("20260319T000000000Z", ("note.txt", "pending"));
        await fixture.WriteBackupAsync("20260320T000000000Z", ("note.txt", "terminal"));
        string pendingDir = fixture.BackupPath("20260319T000000000Z");
        await FileTransactionJournal.CreateAsync(
            pendingDir,
            fixture.CodexHome,
            "openai",
            []);

        BackupPruneResult result = await new BackupService(
            new SessionRolloutService(),
            new SqliteStateService()).PruneBackupsAsync(fixture.CodexHome, 0);

        Assert.Equal(1, result.DeletedCount);
        Assert.Equal(1, result.RemainingCount);
        Assert.True(Directory.Exists(pendingDir));
        Assert.False(Directory.Exists(fixture.BackupPath("20260320T000000000Z")));
    }

    [Fact]
    public async Task GetStatus_ReportsWindowsWslUncSqliteHomeWithoutOpeningDatabase()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sqliteHome = $@"\\wsl.localhost\Ubuntu\tmp\codex-provider-sync-{Guid.NewGuid():N}";
        Stopwatch timer = Stopwatch.StartNew();

        StatusSnapshot status = await new CodexSyncService().GetStatusAsync(fixture.CodexHome, sqliteHome);

        timer.Stop();
        Assert.False(status.SqliteAccess.Supported);
        Assert.Null(status.StateDbLocation);
        Assert.Null(status.SqliteCounts);
        Assert.Contains("Windows cannot safely access SQLite", TextFormatter.FormatStatus(status));
        string chineseStatus = TextFormatter.FormatStatus(status, TextFormatter.ChineseSimplified);
        Assert.Contains("Windows 进程无法通过 WSL UNC 路径安全访问 SQLite", chineseStatus);
        Assert.Contains("请在 WSL 内运行 codex-provider", chineseStatus);
        Assert.DoesNotContain("currently in use", TextFormatter.FormatStatus(status));
        Assert.True(timer.Elapsed < TimeSpan.FromSeconds(5), $"Status took {timer.Elapsed}.");
    }

    [Fact]
    public async Task RunSync_BlocksWindowsWslUncBeforeCreatingBackup()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new CodexSyncService().RunSyncAsync(
                fixture.CodexHome,
                explicitSqliteHome: @"\\wsl.localhost\Ubuntu\home\user\.codex\sqlite"));

        Assert.Contains("Cannot sync", error.Message);
        Assert.Contains("Run codex-provider inside WSL", error.Message);
        Assert.False(Directory.Exists(fixture.BackupRoot()));
    }

    [Fact]
    public async Task RunSwitch_BlocksWindowsWslUncBeforeUpdatingConfig()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string configPath = Path.Combine(fixture.CodexHome, "config.toml");
        string originalConfig = await File.ReadAllTextAsync(configPath);

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new CodexSyncService().RunSwitchAsync(
                fixture.CodexHome,
                "apigather",
                explicitSqliteHome: @"\\wsl$\Ubuntu\home\user\.codex\sqlite"));

        Assert.Contains("Cannot switch", error.Message);
        Assert.Equal(originalConfig, await File.ReadAllTextAsync(configPath));
        Assert.False(Directory.Exists(fixture.BackupRoot()));
    }

    [Fact]
    public async Task RunRestore_BlocksWindowsWslUncBeforeReadingBackup()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new CodexSyncService().RunRestoreAsync(
                fixture.CodexHome,
                Path.Combine(fixture.Root, "missing-backup"),
                @"\\wsl.localhost\Ubuntu\home\user\.codex\sqlite"));

        Assert.Contains("Cannot restore", error.Message);
        Assert.Contains("Run codex-provider inside WSL", error.Message);
    }

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
    public async Task RunSync_UpdatesLegacyRootSqliteDatabase_WhenSqliteDirStateIsStale()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-active-a.jsonl");
        string archivedPath = fixture.RolloutPath("archived_sessions", "rollout-active-b.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-active-a", "dal");
        await fixture.WriteRolloutAsync(archivedPath, "thread-active-b", "dal");
        await fixture.WriteStateDbAsync(
        [
            ("thread-active-a", "dal", false)
        ]);
        await fixture.WriteLegacyStateDbAsync(
        [
            ("thread-active-a", "dal", false),
            ("thread-active-b", "dal", true)
        ]);

        CodexSyncService service = new();
        SyncResult syncResult = await service.RunSyncAsync(fixture.CodexHome);

        Assert.Equal(2, syncResult.SqliteRowsUpdated);
        BackupMetadataFile backupMetadata = JsonSerializer.Deserialize<BackupMetadataFile>(
            await File.ReadAllTextAsync(Path.Combine(syncResult.BackupDir, "metadata.json")),
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase })!;
        Assert.Equal([AppConstants.DbFileBasename], backupMetadata.DbFiles);

        await using (SqliteConnection connection = fixture.OpenLegacySqliteConnection())
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
                ("thread-active-a", "openai"),
                ("thread-active-b", "openai")
            ], rows);
        }

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

            Assert.Equal([("thread-active-a", "dal")], rows);
        }
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
    public async Task RunSwitch_BackupCapturesPreSwitchProviderAndModel()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\nmodel = \"gpt-5.4-mini\"");
        string configPath = Path.Combine(fixture.CodexHome, "config.toml");
        string originalConfig = await File.ReadAllTextAsync(configPath);

        SyncResult result = await new CodexSyncService().RunSwitchAsync(
            fixture.CodexHome,
            "apigather",
            model: "apigather-prod");

        Assert.Equal(
            originalConfig,
            await File.ReadAllTextAsync(Path.Combine(result.BackupDir, "config.toml")));
        string switchedConfig = await File.ReadAllTextAsync(configPath);
        Assert.Contains("model_provider = \"apigather\"", switchedConfig);
        Assert.Contains("model = \"apigather-prod\"", switchedConfig);
    }

    [Fact]
    public async Task RunSwitch_DoesNotTouchConfig_WhenPreSwitchBackupCreationFails()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\nmodel = \"gpt-5.4-mini\"");
        string configPath = Path.Combine(fixture.CodexHome, "config.toml");
        string originalConfig = await File.ReadAllTextAsync(configPath);
        DateTime pinnedLastWriteTime = new(2001, 2, 3, 4, 5, 6, DateTimeKind.Utc);
        File.SetLastWriteTimeUtc(configPath, pinnedLastWriteTime);

        Directory.CreateDirectory(Path.GetDirectoryName(fixture.BackupRoot())!);
        await File.WriteAllTextAsync(fixture.BackupRoot(), "blocks backup directory creation");

        await Assert.ThrowsAnyAsync<Exception>(
            () => new CodexSyncService().RunSwitchAsync(fixture.CodexHome, "apigather"));
        Assert.Equal(originalConfig, await File.ReadAllTextAsync(configPath));
        Assert.Equal(pinnedLastWriteTime, File.GetLastWriteTimeUtc(configPath));
    }

    [Fact]
    public async Task RunSwitch_RestoresConfig_AfterPostBackupSyncFailure()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\nmodel = \"gpt-5.4-mini\"");
        string configPath = Path.Combine(fixture.CodexHome, "config.toml");
        string originalConfig = await File.ReadAllTextAsync(configPath);
        await File.WriteAllTextAsync(
            Path.Combine(fixture.CodexHome, AppConstants.GlobalStateFileBasename),
            "{not-json");

        await Assert.ThrowsAnyAsync<Exception>(
            () => new CodexSyncService().RunSwitchAsync(fixture.CodexHome, "apigather"));
        Assert.Equal(originalConfig, await File.ReadAllTextAsync(configPath));

        string backupDir = Assert.Single(Directory.GetDirectories(fixture.BackupRoot()));
        Assert.Equal(
            originalConfig,
            await File.ReadAllTextAsync(Path.Combine(backupDir, "config.toml")));
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
    public async Task GetStatus_ChoosesLegacyRootSqliteDatabase_WhenSqliteDirStateIsStale()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync(string.Empty);
        await fixture.WriteRolloutAsync(
            fixture.RolloutPath("sessions", "rollout-active-a.jsonl"),
            "thread-active-a",
            "openai");
        await fixture.WriteRolloutAsync(
            fixture.RolloutPath("sessions", "rollout-active-b.jsonl"),
            "thread-active-b",
            "openai");
        await fixture.WriteRolloutAsync(
            fixture.RolloutPath("archived_sessions", "rollout-active-c.jsonl"),
            "thread-active-c",
            "openai");
        await fixture.WriteStateDbAsync(
        [
            ("thread-active-a", "custom", false)
        ]);
        await fixture.WriteLegacyStateDbAsync(
        [
            ("thread-active-a", "openai", false),
            ("thread-active-b", "openai", false),
            ("thread-active-c", "openai", true)
        ]);

        CodexSyncService service = new();
        StatusSnapshot status = await service.GetStatusAsync(fixture.CodexHome);

        Assert.NotNull(status.StateDbLocation);
        Assert.Equal("legacy-root", status.StateDbLocation!.Source);
        Assert.Equal(fixture.LegacyStateDbPath(), status.StateDbLocation.Path);
        Assert.Equal(2, status.SqliteCounts!.Sessions["openai"]);
        Assert.Equal(1, status.SqliteCounts.ArchivedSessions["openai"]);
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
    public async Task RunSwitch_KeepRootModelStillAlignsRolloutAndSqlite()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\nmodel = \"kept-root-model\"\n");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-keep-root.jsonl");
        await fixture.WriteRolloutWithTurnContextAsync(
            sessionPath,
            "thread-keep-root",
            "openai",
            "old-model");
        await fixture.WriteStateDbAsync(
        [
            ("thread-keep-root", "openai", false)
        ],
            model: "old-model");

        CodexSyncService service = new();
        SyncResult result = await service.RunSwitchAsync(
            fixture.CodexHome,
            "apigather",
            keepRootModel: true);

        Assert.False(result.ModelSync.Applied);
        Assert.Equal(1, result.SqliteModelRowsUpdated);
        foreach (string line in (await File.ReadAllLinesAsync(sessionPath))
            .Where(line => line.Contains("\"turn_context\"", StringComparison.Ordinal)))
        {
            using JsonDocument document = JsonDocument.Parse(line);
            Assert.Equal(
                "kept-root-model",
                document.RootElement.GetProperty("payload").GetProperty("model").GetString());
        }
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
    public async Task ApplySessionChanges_RejectsTurnContextAppendedBetweenProviderAndModelRewrite()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        string sessionPath = fixture.RolloutPath("sessions", "rollout-concurrent-append.jsonl");
        await fixture.WriteRolloutWithTurnContextAsync(
            sessionPath,
            "thread-concurrent-append",
            "apigather",
            "old-model");

        SessionRolloutService service = new();
        SessionChangeCollection collected = await service.CollectSessionChangesAsync(
            fixture.CodexHome,
            "openai",
            targetModel: "target-model");
        Assert.Single(collected.Changes);
        string appendedLine = JsonSerializer.Serialize(new
        {
            timestamp = "2026-08-04T00:00:00Z",
            type = "turn_context",
            payload = new
            {
                turn_id = "late-turn",
                model = "late-model",
                collaboration_mode = new
                {
                    settings = new { model = "late-collaboration-model" }
                }
            }
        });
        service.ApplyFaultInjector = async (phase, _) =>
        {
            if (phase == "after-provider-before-model")
            {
                await File.AppendAllTextAsync(sessionPath, appendedLine + "\n");
            }
        };

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.ApplySessionChangesAsync(collected.Changes, "target-model"));

        Assert.Contains("changed after it was scanned", error.Message);
        string[] lines = await File.ReadAllLinesAsync(sessionPath);
        using (JsonDocument header = JsonDocument.Parse(lines[0]))
        {
            Assert.Equal(
                "apigather",
                header.RootElement.GetProperty("payload").GetProperty("model_provider").GetString());
        }
        string late = Assert.Single(lines, line => line.Contains("late-turn", StringComparison.Ordinal));
        using JsonDocument lateRecord = JsonDocument.Parse(late);
        Assert.Equal(
            "late-model",
            lateRecord.RootElement.GetProperty("payload").GetProperty("model").GetString());
        Assert.Equal(
            "late-collaboration-model",
            lateRecord.RootElement
                .GetProperty("payload")
                .GetProperty("collaboration_mode")
                .GetProperty("settings")
                .GetProperty("model")
                .GetString());
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

    [Fact]
    public async Task RunSync_RewritesTurnContextModelField_LinesLargerThan64KB()
    {
        // Regression guard for the long-line reader. Codex can pack a
        // `developer_instructions` blob into a single `turn_context`
        // payload, easily pushing the encoded JSON past 64 KB. The
        // previous 64 KB scanner silently returned null for those
        // files, so the rollout model rewrite was a no-op for
        // sessions whose first turn was a long planning step.
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\nmodel = \"MiniMax-M3\"\n");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-huge.jsonl");

        await fixture.WriteRolloutWithTurnContextPayloadAsync(
            sessionPath,
            "thread-huge",
            "apigather",
            "gpt-5.4",
            new Dictionary<string, object>
            {
                ["developer_instructions"] = new string('x', 150 * 1024)
            });

        string onDisk = await File.ReadAllTextAsync(sessionPath);
        long longestLine = onDisk.Split('\n').Where(line => !string.IsNullOrEmpty(line)).Max(line => (long)line.Length);
        Assert.True(longestLine > 64 * 1024, $"test setup: longest line should exceed 64 KB; got {longestLine}");

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);
        Assert.Equal(1, result.ChangedSessionFiles);

        string rewritten = await File.ReadAllTextAsync(sessionPath);
        int rewrittenCount = 0;
        using (StringReader reader = new(rewritten))
        {
            string? line;
            while ((line = reader.ReadLine()) is not null)
            {
                if (!line.Contains("\"turn_context\"", StringComparison.Ordinal))
                {
                    continue;
                }

                using JsonDocument doc = JsonDocument.Parse(line);
                Assert.Equal("MiniMax-M3", doc.RootElement.GetProperty("payload").GetProperty("model").GetString());
                Assert.Equal("MiniMax-M3", doc.RootElement
                    .GetProperty("payload")
                    .GetProperty("collaboration_mode")
                    .GetProperty("settings")
                    .GetProperty("model")
                    .GetString());
                rewrittenCount += 1;
            }
        }

        Assert.Equal(2, rewrittenCount);
    }

    [Fact]
    public async Task RunSync_RewritesTurnContextModelField_WithRegexMetacharactersInModelName()
    {
        // Regression guard for regex escaping in the per-turn
        // rewrite. A model name containing '.', '+', '*', '?', or
        // '(' is a regex hazard: '.' is a regex any-char, '+' is a
        // quantifier, and an unbalanced '{' would refuse to compile.
        // The rewrite must match literally and not poison a decoy
        // sibling whose pattern over-matches.
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\nmodel = \"weird(target)+v2\"\n");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-rewrite.jsonl");
        await fixture.WriteRolloutWithTurnContextAsync(sessionPath, "thread-rewrite", "apigather", "weird(target)+v2");
        await File.AppendAllTextAsync(sessionPath, JsonSerializer.Serialize(new
        {
            timestamp = "2026-06-09T09:16:03.881Z",
            type = "turn_context",
            payload = new
            {
                turn_id = "decoy",
                model = "weirdAtargetAv2"
            }
        }) + "\n");

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);
        Assert.Equal(1, result.ChangedSessionFiles);

        string rewritten = await File.ReadAllTextAsync(sessionPath);
        int totalContext = 0;
        using (StringReader reader = new(rewritten))
        {
            string? line;
            while ((line = reader.ReadLine()) is not null)
            {
                if (!line.Contains("\"turn_context\"", StringComparison.Ordinal))
                {
                    continue;
                }
                totalContext += 1;
                using JsonDocument doc = JsonDocument.Parse(line);
                string turnId = doc.RootElement.GetProperty("payload").GetProperty("turn_id").GetString()!;
                string model = doc.RootElement.GetProperty("payload").GetProperty("model").GetString()!;
                if (turnId == "decoy")
                {
                    Assert.Equal("weird(target)+v2", model);
                }
                else
                {
                    Assert.Equal("weird(target)+v2", model);
                }
            }
        }
        Assert.Equal(3, totalContext);
    }

    [Fact]
    public async Task RunSync_RewritesModelOnlyChange_AndRestorePreservesOriginalFile()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\r\nmodel = \"MiniMax-M3\"\r\n");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-model-only.jsonl");
        await fixture.WriteRolloutWithTurnContextAsync(
            sessionPath,
            "thread-model-only",
            "openai",
            "gpt-5.4");
        string crlfContent = (await File.ReadAllTextAsync(sessionPath)).Replace("\n", "\r\n", StringComparison.Ordinal);
        await File.WriteAllTextAsync(sessionPath, crlfContent);
        DateTime originalTime = new(2026, 6, 9, 9, 0, 0, DateTimeKind.Utc);
        File.SetLastWriteTimeUtc(sessionPath, originalTime);
        await fixture.WriteStateDbAsync(
        [
            ("thread-model-only", "openai", false)
        ],
            model: "gpt-5.4");

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);

        Assert.Equal(1, result.ChangedSessionFiles);
        Assert.Equal(1, result.SqliteModelRowsUpdated);
        byte[] syncedBytes = await File.ReadAllBytesAsync(sessionPath);
        Assert.Contains((byte)'\r', syncedBytes);
        Assert.EndsWith("\r\n", await File.ReadAllTextAsync(sessionPath), StringComparison.Ordinal);
        Assert.Equal(originalTime, File.GetLastWriteTimeUtc(sessionPath));

        await service.RunRestoreAsync(fixture.CodexHome, result.BackupDir);

        Assert.Equal(crlfContent, await File.ReadAllTextAsync(sessionPath));
        Assert.Equal(originalTime, File.GetLastWriteTimeUtc(sessionPath));
    }

    [Fact]
    public async Task RunSync_DetectsStaleModelsAfterAnAlreadyMatchingFirstTurn()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\nmodel = \"target-model\"\n");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-mixed-models.jsonl");
        await fixture.WriteRolloutWithTurnContextAsync(
            sessionPath,
            "thread-mixed-models",
            "openai",
            "target-model");
        await File.AppendAllTextAsync(sessionPath, JsonSerializer.Serialize(new
        {
            timestamp = "2026-06-09T11:16:03.880Z",
            type = "turn_context",
            payload = new
            {
                turn_id = "stale-turn",
                model = "stale-top-level",
                collaboration_mode = new
                {
                    mode = "default",
                    settings = new
                    {
                        model = "stale-nested"
                    }
                }
            }
        }) + "\n");

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(fixture.CodexHome);

        Assert.Equal(1, result.ChangedSessionFiles);
        foreach (string line in (await File.ReadAllLinesAsync(sessionPath))
            .Where(line => line.Contains("\"turn_context\"", StringComparison.Ordinal)))
        {
            using JsonDocument document = JsonDocument.Parse(line);
            JsonElement payload = document.RootElement.GetProperty("payload");
            Assert.Equal("target-model", payload.GetProperty("model").GetString());
            Assert.Equal(
                "target-model",
                payload.GetProperty("collaboration_mode").GetProperty("settings").GetProperty("model").GetString());
        }

        await service.RunRestoreAsync(
            fixture.CodexHome,
            result.BackupDir,
            new RestoreBackupOptions { RestoreDatabase = false });
        string restoredStaleLine = (await File.ReadAllLinesAsync(sessionPath))
            .Single(line => line.Contains("\"stale-turn\"", StringComparison.Ordinal));
        using JsonDocument restoredDocument = JsonDocument.Parse(restoredStaleLine);
        JsonElement restoredPayload = restoredDocument.RootElement.GetProperty("payload");
        Assert.Equal("stale-top-level", restoredPayload.GetProperty("model").GetString());
        Assert.Equal(
            "stale-nested",
            restoredPayload.GetProperty("collaboration_mode").GetProperty("settings").GetProperty("model").GetString());
    }

    [Fact]
    public async Task RestoreBackup_AcceptsVersionOneSessionManifest()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-legacy-manifest.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-legacy-manifest", "apigather");
        string originalFirstLine = (await File.ReadAllLinesAsync(sessionPath))[0];
        await fixture.WriteRolloutAsync(sessionPath, "thread-legacy-manifest", "openai");

        string sessionManifest = JsonSerializer.Serialize(new
        {
            version = 1,
            @namespace = AppConstants.BackupNamespace,
            codexHome = fixture.CodexHome,
            targetProvider = "openai",
            createdAt = DateTimeOffset.UtcNow,
            files = new[]
            {
                new
                {
                    path = sessionPath,
                    originalFirstLine,
                    originalSeparator = "\n"
                }
            }
        });
        await fixture.WriteBackupAsync(
            "20260723T000000000Z",
            ("session-meta-backup.json", sessionManifest),
            ("config.toml", await File.ReadAllTextAsync(Path.Combine(fixture.CodexHome, "config.toml"))));

        CodexSyncService service = new();
        await service.RunRestoreAsync(
            fixture.CodexHome,
            fixture.BackupPath("20260723T000000000Z"),
            new RestoreBackupOptions { RestoreDatabase = false });

        Assert.Equal(originalFirstLine, (await File.ReadAllLinesAsync(sessionPath))[0]);
    }

    [Fact]
    public async Task RunSync_UsesExplicitSqliteHomeWithoutTouchingDefaultDatabase()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteStateDbAsync([("default-thread", "default-provider", false)]);
        string sqliteHome = Path.Combine(fixture.Root, "external-sqlite");
        string externalDbPath = Path.Combine(sqliteHome, AppConstants.DbFileBasename);
        await fixture.WriteStateDbAtAsync(
            externalDbPath,
            [("external-thread", "custom", false)],
            model: "old-model");

        CodexSyncService service = new();
        SyncResult result = await service.RunSyncAsync(
            fixture.CodexHome,
            model: "new-model",
            explicitSqliteHome: sqliteHome);

        Assert.Equal(Path.GetFullPath(sqliteHome), result.SqliteHome);
        Assert.Equal("gui", result.SqliteHomeSource);
        Assert.Equal("openai", await ReadProviderAsync(externalDbPath, "external-thread"));
        Assert.Equal("default-provider", await ReadProviderAsync(fixture.StateDbPath(), "default-thread"));

        BackupMetadataFile metadata = JsonSerializer.Deserialize<BackupMetadataFile>(
            await File.ReadAllTextAsync(Path.Combine(result.BackupDir, "metadata.json")),
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase })!;
        Assert.Equal(2, metadata.Version);
        Assert.Equal(Path.GetFullPath(sqliteHome), metadata.SqliteHome);
        Assert.Empty(metadata.DbFiles);
        Assert.Equal([AppConstants.DbFileBasename], metadata.SqliteDbFiles);
    }

    [Fact]
    public async Task ConfiguredSqliteHomeWithoutDatabase_IsDiagnosticForStatusButBlocksSync()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        string sqliteHome = Path.Combine(fixture.Root, "missing-sqlite");
        await fixture.WriteConfigAsync($"model_provider = \"openai\"\nsqlite_home = '{sqliteHome}'");
        await fixture.WriteStateDbAsync([("stale-thread", "custom", false)]);

        CodexSyncService service = new();
        StatusSnapshot status = await service.GetStatusAsync(fixture.CodexHome);

        Assert.Equal(Path.GetFullPath(sqliteHome), status.SqliteHome);
        Assert.Equal("config", status.SqliteHomeSource);
        Assert.Null(status.StateDbLocation);
        Assert.Single(status.CheckedStateDbPaths);
        await Assert.ThrowsAsync<InvalidOperationException>(() => service.RunSyncAsync(fixture.CodexHome));
        Assert.Equal("custom", await ReadProviderAsync(fixture.StateDbPath(), "stale-thread"));
    }

    [Fact]
    public async Task RestoreVersionTwo_RebuildsMissingDefaultSqliteDatabase()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteStateDbAsync([("thread-missing-default", "custom", false)]);

        CodexSyncService service = new();
        SyncResult syncResult = await service.RunSyncAsync(fixture.CodexHome);
        File.Delete(fixture.StateDbPath());

        await service.RunRestoreAsync(
            fixture.CodexHome,
            syncResult.BackupDir,
            new RestoreBackupOptions
            {
                RestoreConfig = false,
                RestoreDatabase = true,
                RestoreSessions = false
            });

        Assert.Equal("custom", await ReadProviderAsync(fixture.StateDbPath(), "thread-missing-default"));
    }

    [Fact]
    public async Task RestoreVersionTwo_RebuildsMissingLegacyRootSqliteDatabaseInPlace()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteLegacyStateDbAsync([("thread-missing-legacy", "custom", false)]);

        CodexSyncService service = new();
        SyncResult syncResult = await service.RunSyncAsync(fixture.CodexHome);
        File.Delete(fixture.LegacyStateDbPath());

        await service.RunRestoreAsync(
            fixture.CodexHome,
            syncResult.BackupDir,
            new RestoreBackupOptions
            {
                RestoreConfig = false,
                RestoreDatabase = true,
                RestoreSessions = false
            });

        Assert.False(File.Exists(fixture.StateDbPath()));
        Assert.Equal("custom", await ReadProviderAsync(fixture.LegacyStateDbPath(), "thread-missing-legacy"));
    }

    [Fact]
    public async Task RestoreVersionOne_RebuildsMissingDefaultSqliteDatabase()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string backupDir = fixture.BackupPath("20260728T010000000Z");
        string relativeDbPath = Path.Combine(AppConstants.SqliteDirBasename, AppConstants.DbFileBasename);
        string backupDbPath = Path.Combine(backupDir, "db", relativeDbPath);
        await fixture.WriteStateDbAtAsync(
            backupDbPath,
            [("thread-v1-missing", "custom", false)],
            model: null);
        string metadata = JsonSerializer.Serialize(new
        {
            version = 1,
            @namespace = AppConstants.BackupNamespace,
            codexHome = fixture.CodexHome,
            targetProvider = "custom",
            createdAt = DateTimeOffset.UtcNow,
            dbFiles = new[] { relativeDbPath },
            changedSessionFiles = 0
        });
        await File.WriteAllTextAsync(Path.Combine(backupDir, "metadata.json"), metadata);

        CodexSyncService service = new();
        await service.RunRestoreAsync(
            fixture.CodexHome,
            backupDir,
            new RestoreBackupOptions
            {
                RestoreConfig = false,
                RestoreDatabase = true,
                RestoreSessions = false
            });

        Assert.Equal("custom", await ReadProviderAsync(fixture.StateDbPath(), "thread-v1-missing"));
    }

    [Fact]
    public async Task RestoreVersionTwo_RequiresExplicitRelocationConfirmation()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sourceSqliteHome = Path.Combine(fixture.Root, "source-sqlite");
        string sourceDbPath = Path.Combine(sourceSqliteHome, AppConstants.DbFileBasename);
        await fixture.WriteStateDbAtAsync(sourceDbPath, [("thread-a", "custom", false)], model: null);

        CodexSyncService service = new();
        SyncResult syncResult = await service.RunSyncAsync(
            fixture.CodexHome,
            explicitSqliteHome: sourceSqliteHome);

        string targetSqliteHome = Path.Combine(fixture.Root, "target-sqlite");
        string targetDbPath = Path.Combine(targetSqliteHome, AppConstants.DbFileBasename);
        await fixture.WriteStateDbAtAsync(targetDbPath, [("thread-a", "target", false)], model: null);
        RestoreBackupOptions deniedOptions = new()
        {
            RestoreConfig = false,
            RestoreDatabase = true,
            RestoreSessions = false
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => service.RunRestoreAsync(
            fixture.CodexHome,
            syncResult.BackupDir,
            deniedOptions,
            targetSqliteHome));
        Assert.Equal("target", await ReadProviderAsync(targetDbPath, "thread-a"));

        InvalidOperationException configError = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.RunRestoreAsync(
                fixture.CodexHome,
                syncResult.BackupDir,
                new RestoreBackupOptions
                {
                    RestoreConfig = true,
                    RestoreDatabase = true,
                    RestoreSessions = false,
                    AllowSqliteHomeRelocation = true
                },
                targetSqliteHome));
        Assert.Contains("Cannot restore config.toml while relocating SQLite home", configError.Message);
        Assert.Equal("target", await ReadProviderAsync(targetDbPath, "thread-a"));

        await service.RunRestoreAsync(
            fixture.CodexHome,
            syncResult.BackupDir,
            new RestoreBackupOptions
            {
                RestoreConfig = false,
                RestoreDatabase = true,
                RestoreSessions = false,
                AllowSqliteHomeRelocation = true
            },
            targetSqliteHome);
        Assert.Equal("custom", await ReadProviderAsync(targetDbPath, "thread-a"));
    }

    [Fact]
    public async Task RestoreVersionTwo_ValidatesDatabaseFilesBeforeRestoringConfig()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"current\"");
        await fixture.WriteStateDbAsync([("thread-a", "current", false)]);
        string backupDir = fixture.BackupPath("20260728T000000000Z");
        string metadata = JsonSerializer.Serialize(new
        {
            version = 2,
            @namespace = AppConstants.BackupNamespace,
            codexHome = fixture.CodexHome,
            sqliteHome = Path.GetDirectoryName(fixture.StateDbPath()),
            targetProvider = "backup",
            createdAt = DateTimeOffset.UtcNow,
            dbFiles = Array.Empty<string>(),
            sqliteDbFiles = new[] { AppConstants.DbFileBasename },
            changedSessionFiles = 0
        });
        await fixture.WriteBackupAsync(
            "20260728T000000000Z",
            ("metadata.json", metadata),
            ("config.toml", "model_provider = \"backup\"\n"));

        CodexSyncService service = new();
        await Assert.ThrowsAsync<InvalidOperationException>(() => service.RunRestoreAsync(
            fixture.CodexHome,
            backupDir,
            new RestoreBackupOptions { RestoreSessions = false }));

        Assert.Contains(
            "model_provider = \"current\"",
            await File.ReadAllTextAsync(Path.Combine(fixture.CodexHome, "config.toml")));
    }

    [Fact]
    public async Task CrashWindow_AfterSecondRolloutMutationBeforeApplied_ExplicitRestoreUsesImmutableManifest()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string firstPath = fixture.RolloutPath("sessions", "rollout-crash-window-a.jsonl");
        string secondPath = fixture.RolloutPath("sessions", "rollout-crash-window-b.jsonl");
        await fixture.WriteRolloutAsync(firstPath, "thread-crash-window-a", "apigather");
        await fixture.WriteRolloutAsync(secondPath, "thread-crash-window-b", "apigather");
        string firstBefore = await File.ReadAllTextAsync(firstPath);
        string secondBefore = await File.ReadAllTextAsync(secondPath);
        SessionRolloutService rollouts = new();
        SessionChangeCollection changes = await rollouts.CollectSessionChangesAsync(
            fixture.CodexHome,
            "openai");
        BackupService backups = new(rollouts, new SqliteStateService());
        string backupDir = await backups.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            changes.Changes,
            Path.Combine(fixture.CodexHome, "config.toml"));
        FileTransactionJournal journal = await FileTransactionJournal.CreateAsync(
            backupDir,
            fixture.CodexHome,
            "openai",
            [firstPath, secondPath]);
        int mutated = 0;

        await Assert.ThrowsAsync<IOException>(() => rollouts.ApplySessionChangesAsync(
            changes.Changes,
            onBeforeApply: change => journal.ApplyingAsync("rollout", change.Path),
            onApplied: async change =>
            {
                mutated += 1;
                if (mutated == 2)
                {
                    throw new IOException("simulated process loss before applied record");
                }
                await journal.AppliedAsync("rollout", change.Path);
            },
            onSkipped: change => journal.SkippedAsync("rollout", change.Path)));

        Assert.NotEqual(firstBefore, await File.ReadAllTextAsync(firstPath));
        Assert.NotEqual(secondBefore, await File.ReadAllTextAsync(secondPath));
        PendingTransactionInfo pending = Assert.Single(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
        Assert.Equal(2, pending.AffectedTargets.Count(static target => target.Kind == "rollout"));

        await new CodexSyncService().RunRestoreAsync(
            fixture.CodexHome,
            backupDir,
            new RestoreBackupOptions
            {
                RestoreConfig = false,
                RestoreDatabase = false,
                RestoreSessions = true
            });

        Assert.Equal(firstBefore, await File.ReadAllTextAsync(firstPath));
        Assert.Equal(secondBefore, await File.ReadAllTextAsync(secondPath));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task Journal_SkippedTargetResolvesApplying_AndCanCommit()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-raced-skip.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-raced-skip", "apigather");
        SessionRolloutService rollouts = new();
        SessionChangeCollection changes = await rollouts.CollectSessionChangesAsync(fixture.CodexHome, "openai");
        BackupService backups = new(rollouts, new SqliteStateService());
        string backupDir = await backups.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            changes.Changes,
            Path.Combine(fixture.CodexHome, "config.toml"));
        FileTransactionJournal journal = await FileTransactionJournal.CreateAsync(
            backupDir,
            fixture.CodexHome,
            "openai",
            [sessionPath]);

        await fixture.WriteRolloutAsync(sessionPath, "thread-raced-skip", "changed-by-codex");
        SessionApplyResult result = await rollouts.ApplySessionChangesAsync(
            changes.Changes,
            onBeforeApply: change => journal.ApplyingAsync("rollout", change.Path),
            onApplied: change => journal.AppliedAsync("rollout", change.Path),
            onSkipped: change => journal.SkippedAsync("rollout", change.Path));
        await journal.CommittedAsync();

        Assert.Empty(result.AppliedPaths);
        PendingTransactionInfo info = await FileTransactionJournal.ReadInfoAsync(journal.FilePath);
        Assert.True(info.Terminal);
        Assert.Empty(info.AffectedTargets);
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task ForeignOperationCommittedTail_RemainsPending_UntilExplicitRestoreRepairsJournal()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-foreign-operation.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-foreign-operation", "apigather");
        string before = await File.ReadAllTextAsync(sessionPath);
        SyncResult sync = await new CodexSyncService().RunSyncAsync(fixture.CodexHome);
        string journalPath = Path.Combine(sync.BackupDir, FileTransactionJournal.FileName);
        PendingTransactionInfo committed = await FileTransactionJournal.ReadInfoAsync(journalPath);
        string foreignOperationId = Guid.NewGuid().ToString("D");
        string foreignRecord = JsonSerializer.Serialize(new
        {
            protocolVersion = 1,
            operationId = foreignOperationId,
            sequence = committed.LastSequence + 1,
            state = "committed",
            recordedAt = DateTimeOffset.UtcNow
        });
        await File.AppendAllTextAsync(journalPath, foreignRecord + "\n");

        PendingTransactionInfo corrupted = Assert.Single(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
        Assert.True(corrupted.InvalidTail);
        Assert.Equal("committed", corrupted.LastValidState);
        await Assert.ThrowsAsync<RecoveryRequiredException>(
            () => new CodexSyncService().RunSyncAsync(fixture.CodexHome));

        await new CodexSyncService().RunRestoreAsync(
            fixture.CodexHome,
            sync.BackupDir,
            new RestoreBackupOptions
            {
                RestoreConfig = true,
                RestoreDatabase = false,
                RestoreSessions = true
            });

        Assert.Equal(before, await File.ReadAllTextAsync(sessionPath));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
        PendingTransactionInfo repaired = await FileTransactionJournal.ReadInfoAsync(journalPath);
        Assert.True(repaired.Terminal);
        Assert.Equal("rolledBack", repaired.State);
        Assert.DoesNotContain(foreignOperationId, await File.ReadAllTextAsync(journalPath));
        Assert.Single(Directory.EnumerateFiles(
            sync.BackupDir,
            "transaction-journal.invalid.*.jsonl",
            SearchOption.TopDirectoryOnly));
    }

    [Fact]
    public async Task ExplicitRestore_RepairsCompletelyUnreadableJournal_AndArchivesRawEvidence()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        BackupService backups = new(new SessionRolloutService(), new SqliteStateService());
        string backupDir = await backups.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            [],
            Path.Combine(fixture.CodexHome, "config.toml"));
        string journalPath = Path.Combine(backupDir, FileTransactionJournal.FileName);
        const string corruptRaw = "this is not JSON\n";
        await File.WriteAllTextAsync(journalPath, corruptRaw);

        Assert.Single(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
        await new CodexSyncService().RunRestoreAsync(
            fixture.CodexHome,
            backupDir,
            new RestoreBackupOptions
            {
                RestoreConfig = true,
                RestoreDatabase = false,
                RestoreSessions = false
            });

        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
        PendingTransactionInfo repaired = await FileTransactionJournal.ReadInfoAsync(journalPath);
        Assert.True(repaired.Terminal);
        Assert.Equal("rolledBack", repaired.State);
        string archivePath = Assert.Single(Directory.EnumerateFiles(
            backupDir,
            "transaction-journal.invalid.*.jsonl",
            SearchOption.TopDirectoryOnly));
        Assert.Equal(corruptRaw, await File.ReadAllTextAsync(archivePath));
    }

    [Fact]
    public async Task JournalMissingFinalLf_IsPending_AndCannotAppendUntilExplicitRestoreRepairsIt()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        BackupService backups = new(new SessionRolloutService(), new SqliteStateService());
        string backupDir = await backups.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            [],
            Path.Combine(fixture.CodexHome, "config.toml"));
        FileTransactionJournal journal = await FileTransactionJournal.CreateAsync(
            backupDir,
            fixture.CodexHome,
            "openai",
            []);
        string completeLineWithoutLf = (await File.ReadAllTextAsync(journal.FilePath)).TrimEnd('\n');
        await File.WriteAllTextAsync(journal.FilePath, completeLineWithoutLf);

        PendingTransactionInfo pending = Assert.Single(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
        Assert.True(pending.InvalidTail);
        InvalidOperationException appendError = await Assert.ThrowsAsync<InvalidOperationException>(
            () => journal.CommittedAsync());
        Assert.Contains("cannot commit", appendError.Message);

        await new CodexSyncService().RunRestoreAsync(
            fixture.CodexHome,
            backupDir,
            new RestoreBackupOptions
            {
                RestoreConfig = true,
                RestoreDatabase = false,
                RestoreSessions = false
            });
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
        Assert.EndsWith("\n", await File.ReadAllTextAsync(journal.FilePath));
        Assert.Single(Directory.EnumerateFiles(
            backupDir,
            "transaction-journal.invalid.*.jsonl",
            SearchOption.TopDirectoryOnly));
    }

    [Fact]
    public async Task RecoveryRequiredThenCommitted_CannotForgeTerminalJournal()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteBackupAsync("20260804T000000000Z");
        string backupDir = fixture.BackupPath("20260804T000000000Z");
        FileTransactionJournal journal = await FileTransactionJournal.CreateAsync(
            backupDir,
            fixture.CodexHome,
            "openai",
            []);
        IOException original = new("injected");
        await journal.RollingBackAsync(original);
        await journal.RecoveryRequiredAsync(original, ["rollback failed"]);
        PendingTransactionInfo recovery = await FileTransactionJournal.ReadInfoAsync(journal.FilePath);
        string forged = JsonSerializer.Serialize(new
        {
            protocolVersion = 1,
            operationId = recovery.OperationId,
            sequence = recovery.LastSequence + 1,
            state = "committed",
            recordedAt = DateTimeOffset.UtcNow
        });
        await File.AppendAllTextAsync(journal.FilePath, forged + "\n");

        PendingTransactionInfo pending = Assert.Single(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
        Assert.True(pending.InvalidTail);
        Assert.Equal("recoveryRequired", pending.State);
        await Assert.ThrowsAsync<RecoveryRequiredException>(
            () => FileTransactionJournal.AssertNoPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task Switch_ConfigCompensationFailure_PreservesStructuredRecoveryEvidence()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string configPath = Path.Combine(fixture.CodexHome, "config.toml");
        string before = await File.ReadAllTextAsync(configPath);
        CodexSyncService service = new();
        service.FaultInjector = (point, _, _) => point switch
        {
            "after_config_mutation_before_applied" => Task.FromException(new IOException("injected post-config failure")),
            "before_config_rollback" => Task.FromException(new IOException("injected config compensation failure")),
            _ => Task.CompletedTask
        };

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunSwitchAsync(fixture.CodexHome, "apigather"));

        Assert.True(error.RecoveryRequired);
        Assert.Contains("injected post-config failure", error.OriginalError.Message);
        Assert.Contains(error.RollbackErrors, failure =>
            failure.Contains("config", StringComparison.Ordinal)
            && failure.Contains("injected config compensation failure", StringComparison.Ordinal));
        Assert.Contains("model_provider = \"apigather\"", await File.ReadAllTextAsync(configPath));
        Assert.Single(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));

        await new CodexSyncService().RunRestoreAsync(
            fixture.CodexHome,
            error.BackupDirectory,
            new RestoreBackupOptions { RestoreDatabase = false });
        Assert.Equal(before, await File.ReadAllTextAsync(configPath));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task Rollback_RemovesGlobalStateBackup_WhenItDidNotExistBeforeSync()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteStateDbWithCwdAsync([
            ("thread-global-bak", "apigather", false, @"C:\AITemp")
        ]);
        string statePath = Path.Combine(fixture.CodexHome, AppConstants.GlobalStateFileBasename);
        string stateBackupPath = Path.Combine(fixture.CodexHome, AppConstants.GlobalStateBackupFileBasename);
        string originalState = JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["electron-saved-workspace-roots"] = new[] { @"\\?\C:\AITemp" },
            ["project-order"] = new[] { @"\\?\C:\AITemp" },
            ["active-workspace-roots"] = new[] { @"\\?\C:\AITemp" }
        });
        await File.WriteAllTextAsync(statePath, originalState);
        Assert.False(File.Exists(stateBackupPath));
        CodexSyncService service = new();
        service.FaultInjector = (point, path, _) =>
            point == "after_global_state_apply"
            && string.Equals(Path.GetFullPath(path!), Path.GetFullPath(stateBackupPath), StringComparison.OrdinalIgnoreCase)
                ? Task.FromException(new IOException("injected after global-state backup write"))
                : Task.CompletedTask;

        SyncTransactionException error = await Assert.ThrowsAsync<SyncTransactionException>(
            () => service.RunSyncAsync(fixture.CodexHome));

        Assert.False(error.RecoveryRequired);
        Assert.Equal(originalState, await File.ReadAllTextAsync(statePath));
        Assert.False(File.Exists(stateBackupPath));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task RestoreGlobalState_DeclaredOriginalMissingFromBackup_IsFailure()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteGlobalStateAsync(new { project_order = Array.Empty<string>() });
        BackupService backups = new(new SessionRolloutService(), new SqliteStateService());
        string backupDir = await backups.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            [],
            Path.Combine(fixture.CodexHome, "config.toml"));
        File.Delete(Path.Combine(backupDir, AppConstants.GlobalStateBackupFileBasename));

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => backups.RestoreGlobalStateFilesAsync(backupDir, fixture.CodexHome));
        Assert.Contains("declares an original file", error.Message);
    }

    [Fact]
    public async Task AtomicManifestUpdateFailure_PreservesPreviousManifestAndMetadata()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-atomic-manifest.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-atomic-manifest", "apigather");
        SessionRolloutService rollouts = new();
        SessionChangeCollection changes = await rollouts.CollectSessionChangesAsync(fixture.CodexHome, "openai");
        BackupService backups = new(rollouts, new SqliteStateService());
        string backupDir = await backups.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            changes.Changes,
            Path.Combine(fixture.CodexHome, "config.toml"));
        string manifestPath = Path.Combine(backupDir, "session-meta-backup.json");
        string metadataPath = Path.Combine(backupDir, "metadata.json");
        string manifestBefore = await File.ReadAllTextAsync(manifestPath);
        string metadataBefore = await File.ReadAllTextAsync(metadataPath);
        backups.AtomicWriteFaultInjector = (point, targetPath, _) =>
            point == "before_atomic_replace"
            && string.Equals(targetPath, Path.GetFullPath(manifestPath), StringComparison.OrdinalIgnoreCase)
                ? Task.FromException(new IOException("injected manifest replace failure"))
                : Task.CompletedTask;

        await Assert.ThrowsAsync<IOException>(() => backups.UpdateSessionBackupManifestAsync(backupDir, []));

        Assert.Equal(manifestBefore, await File.ReadAllTextAsync(manifestPath));
        Assert.Equal(metadataBefore, await File.ReadAllTextAsync(metadataPath));
        Assert.Empty(Directory.EnumerateFiles(backupDir, "*.tmp", SearchOption.TopDirectoryOnly));
    }

    [Fact]
    public async Task Restore_RejectsSessionManifestPathOutsideCodexHome()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        SessionRolloutService rollouts = new();
        BackupService backups = new(rollouts, new SqliteStateService());
        string backupDir = await backups.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            [],
            Path.Combine(fixture.CodexHome, "config.toml"));
        string outsidePath = Path.Combine(fixture.Root, "rollout-outside.jsonl");
        await File.WriteAllTextAsync(outsidePath, "outside must remain unchanged\n");
        string outsideBefore = await File.ReadAllTextAsync(outsidePath);
        string maliciousManifest = JsonSerializer.Serialize(new
        {
            version = 2,
            @namespace = AppConstants.BackupNamespace,
            codexHome = fixture.CodexHome,
            targetProvider = "openai",
            createdAt = DateTimeOffset.UtcNow,
            files = new[]
            {
                new
                {
                    path = outsidePath,
                    originalFirstLine = "attacker controlled",
                    originalSeparator = "\n",
                    originalLastWriteTimeUtcTicks = (long?)null,
                    modelOnlyChange = false,
                    originalTurnContextModels = Array.Empty<object>()
                }
            }
        });
        await AtomicFile.WriteAllTextAsync(
            Path.Combine(backupDir, "session-meta-backup.json"),
            maliciousManifest);

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => backups.RestoreBackupAsync(
                backupDir,
                fixture.CodexHome,
                new RestoreBackupOptions
                {
                    RestoreConfig = false,
                    RestoreDatabase = false,
                    RestoreSessions = true
                }));

        Assert.Contains("escapes the Codex rollout directories", error.Message);
        Assert.Equal(outsideBefore, await File.ReadAllTextAsync(outsidePath));
    }

    [Fact]
    public async Task TruncatedMetadata_WithPendingJournal_BlocksWritesAndPreservesRecoveryEvidence()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        BackupService backups = new(new SessionRolloutService(), new SqliteStateService());
        string backupDir = await backups.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            [],
            Path.Combine(fixture.CodexHome, "config.toml"));
        await FileTransactionJournal.CreateAsync(
            backupDir,
            fixture.CodexHome,
            "openai",
            []);
        await File.WriteAllTextAsync(Path.Combine(backupDir, "metadata.json"), "{\"version\":2");

        Assert.Single(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
        await Assert.ThrowsAnyAsync<JsonException>(() => new CodexSyncService().RunRestoreAsync(
            fixture.CodexHome,
            backupDir,
            new RestoreBackupOptions
            {
                RestoreConfig = false,
                RestoreDatabase = false,
                RestoreSessions = false
            }));
        Assert.Single(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
        await Assert.ThrowsAsync<RecoveryRequiredException>(
            () => new CodexSyncService().RunSyncAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task PendingTransaction_PartialRestoreCannotUnlockUnrestoredSqlite()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-partial-recovery.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-partial-recovery", "apigather");
        await fixture.WriteStateDbAsync([("thread-partial-recovery", "apigather", false)]);
        string sessionBefore = await File.ReadAllTextAsync(sessionPath);
        SessionRolloutService rollouts = new();
        SqliteStateService sqlite = new();
        SessionChangeCollection changes = await rollouts.CollectSessionChangesAsync(fixture.CodexHome, "openai");
        BackupService backups = new(rollouts, sqlite);
        string backupDir = await backups.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            changes.Changes,
            Path.Combine(fixture.CodexHome, "config.toml"));
        FileTransactionJournal journal = await FileTransactionJournal.CreateAsync(
            backupDir,
            fixture.CodexHome,
            "openai",
            [sessionPath, fixture.StateDbPath()]);
        await journal.ApplyingAsync("rollout", sessionPath);
        await rollouts.ApplySessionChangesAsync(changes.Changes);
        await journal.AppliedAsync("rollout", sessionPath);
        await journal.ApplyingAsync("sqlite", fixture.StateDbPath());
        await sqlite.UpdateSqliteProviderAsync(fixture.CodexHome, "openai");
        await journal.AppliedAsync("sqlite", fixture.StateDbPath());

        CodexSyncService service = new();
        InvalidOperationException partialError = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.RunRestoreAsync(
                fixture.CodexHome,
                backupDir,
                new RestoreBackupOptions
                {
                    RestoreConfig = false,
                    RestoreDatabase = false,
                    RestoreSessions = true
                }));

        Assert.Contains("partial restore", partialError.Message);
        Assert.Contains("SQLite", partialError.Message);
        Assert.NotEqual(sessionBefore, await File.ReadAllTextAsync(sessionPath));
        Assert.Equal("openai", await ReadProviderAsync(fixture.StateDbPath(), "thread-partial-recovery"));
        Assert.Single(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));

        await service.RunRestoreAsync(
            fixture.CodexHome,
            backupDir,
            new RestoreBackupOptions
            {
                RestoreConfig = false,
                RestoreDatabase = true,
                RestoreSessions = true
            });
        Assert.Equal(sessionBefore, await File.ReadAllTextAsync(sessionPath));
        Assert.Equal("apigather", await ReadProviderAsync(fixture.StateDbPath(), "thread-partial-recovery"));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task Journal_InvalidTargetKind_CannotBecomeTerminal()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteBackupAsync("20260804T010000000Z");
        string backupDir = fixture.BackupPath("20260804T010000000Z");
        string targetPath = Path.Combine(fixture.CodexHome, "config.toml");
        FileTransactionJournal journal = await FileTransactionJournal.CreateAsync(
            backupDir,
            fixture.CodexHome,
            "openai",
            [targetPath]);
        PendingTransactionInfo prepared = await FileTransactionJournal.ReadInfoAsync(journal.FilePath);
        string invalidKind = JsonSerializer.Serialize(new
        {
            protocolVersion = 1,
            operationId = prepared.OperationId,
            sequence = prepared.LastSequence + 1,
            state = "applying",
            kind = "arbitraryFile",
            targetPath,
            recordedAt = DateTimeOffset.UtcNow
        });
        await File.AppendAllTextAsync(journal.FilePath, invalidKind + "\n");

        PendingTransactionInfo pending = Assert.Single(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
        Assert.True(pending.InvalidTail);
        Assert.Empty(pending.AffectedTargets);
        await Assert.ThrowsAsync<RecoveryRequiredException>(
            () => FileTransactionJournal.AssertNoPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task AbruptChildProcessExit_AfterRolloutMutation_BlocksWritesUntilExplicitRestore()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-real-process-crash.jsonl");
        await fixture.WriteRolloutAsync(sessionPath, "thread-real-process-crash", "apigather");
        string before = await File.ReadAllTextAsync(sessionPath);
        string configuration = new DirectoryInfo(AppContext.BaseDirectory).Parent?.Name ?? "Debug";
        string testProjectDirectory = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", ".."));
        string crashHostPath = Path.Combine(
            testProjectDirectory,
            "CrashHost",
            "bin",
            configuration,
            "net10.0",
            "CodexProviderSync.CrashHost.dll");
        Assert.True(File.Exists(crashHostPath), $"Crash host was not built: {crashHostPath}");
        string dotnetHost = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet";
        ProcessStartInfo startInfo = new(dotnetHost)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };
        startInfo.ArgumentList.Add(crashHostPath);
        startInfo.ArgumentList.Add(fixture.CodexHome);
        using Process child = Process.Start(startInfo)
            ?? throw new InvalidOperationException("Failed to start the crash-test child process.");
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(20));
        try
        {
            await child.WaitForExitAsync(timeout.Token);
        }
        catch
        {
            if (!child.HasExited)
            {
                child.Kill(entireProcessTree: true);
            }
            throw;
        }

        Assert.NotEqual(0, child.ExitCode);
        Assert.NotEqual(before, await File.ReadAllTextAsync(sessionPath));
        PendingTransactionInfo pending = Assert.Single(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
        Assert.Contains(pending.AffectedTargets, target =>
            target.Kind == "rollout"
            && string.Equals(Path.GetFullPath(target.TargetPath), Path.GetFullPath(sessionPath), StringComparison.OrdinalIgnoreCase)
            && target.State == "applying");
        await Assert.ThrowsAsync<RecoveryRequiredException>(
            () => new CodexSyncService().RunSyncAsync(fixture.CodexHome));

        await new CodexSyncService().RunRestoreAsync(
            fixture.CodexHome,
            pending.BackupDir,
            new RestoreBackupOptions
            {
                RestoreConfig = false,
                RestoreDatabase = false,
                RestoreSessions = true
            });
        Assert.Equal(before, await File.ReadAllTextAsync(sessionPath));
        Assert.Empty(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Fact]
    public async Task AtomicFile_PreservesUnixOwnerOnlyModeOnReplace()
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        string directory = Path.Combine(Path.GetTempPath(), $"codex-provider-mode-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "sensitive.json");
        await File.WriteAllTextAsync(path, "before");
        UnixFileMode ownerOnly = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        File.SetUnixFileMode(path, ownerOnly);

        await AtomicFile.WriteAllTextAsync(path, "after");

        Assert.Equal(ownerOnly, File.GetUnixFileMode(path));
    }

    [Fact]
    public async Task RunSync_PreservesUnixOwnerOnlyModeOnRolloutReplace()
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"\nmodel = \"target-model\"");
        string sessionPath = fixture.RolloutPath("sessions", "rollout-mode-0600.jsonl");
        await fixture.WriteRolloutWithTurnContextAsync(
            sessionPath,
            "thread-mode-0600",
            "apigather",
            "old-model");
        UnixFileMode ownerOnly = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        File.SetUnixFileMode(sessionPath, ownerOnly);

        await new CodexSyncService().RunSyncAsync(fixture.CodexHome);

        Assert.Equal(ownerOnly, File.GetUnixFileMode(sessionPath));
    }

    private static async Task<string> ReadProviderAsync(string dbPath, string threadId)
    {
        SqliteConnectionStringBuilder builder = new()
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadOnly,
            Pooling = false
        };
        await using SqliteConnection connection = new(builder.ConnectionString);
        await connection.OpenAsync();
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT model_provider FROM threads WHERE id = $id";
        command.Parameters.AddWithValue("$id", threadId);
        return Convert.ToString(await command.ExecuteScalarAsync())!;
    }

    private static string RelativeTargetIdentity(string codexHome, string targetPath)
    {
        string relative = Path.GetRelativePath(
                Path.GetFullPath(codexHome),
                Path.GetFullPath(targetPath))
            .Replace('\\', '/');
        if (relative != ".." && !relative.StartsWith("../", StringComparison.Ordinal))
        {
            return relative;
        }

        // macOS temp roots can be exposed through both /var and
        // /private/var. Preserve the complete logical path within the test
        // Codex Home if the two absolute spellings cross that symlink alias.
        string normalizedTarget = Path.GetFullPath(targetPath).Replace('\\', '/');
        string homeMarker = $"/{Path.GetFileName(Path.TrimEndingDirectorySeparator(codexHome))}/";
        int markerIndex = normalizedTarget.LastIndexOf(homeMarker, StringComparison.Ordinal);
        return markerIndex >= 0
            ? normalizedTarget[(markerIndex + homeMarker.Length)..]
            : relative;
    }
}
