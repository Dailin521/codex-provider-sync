namespace CodexProviderSync.Core.Tests;

public sealed class TextFormatterTests
{
    [Fact]
    public void FormatStatus_ChineseLocalizesDiagnosticsAndPreservesTechnicalIdentifiers()
    {
        StatusSnapshot status = new()
        {
            CodexHome = @"C:\Users\test\.codex",
            CurrentProvider = new CurrentProviderInfo("openai", Implicit: true),
            ConfiguredProviders = ["openai", "relay"],
            RolloutCounts = Counts(
                sessions: new() { ["openai"] = 3 },
                archived: []),
            LockedRolloutFiles = [@"C:\locked.jsonl"],
            UnreadableRolloutFiles = [@"C:\broken.jsonl"],
            EncryptedContentCounts = Counts(
                sessions: new() { ["relay"] = 2 },
                archived: []),
            EncryptedContentWarning = "English warning should be localized.",
            SqliteCounts = Counts(
                sessions: new() { ["openai"] = 3 },
                archived: []),
            StateDbLocation = new StateDbLocation(
                @"C:\Users\test\.codex\state_5.sqlite",
                "state_5.sqlite",
                "legacy-root"),
            SqliteRepairStats = new SqliteRepairStats
            {
                UserEventRowsNeedingRepair = 1,
                CwdRowsNeedingRepair = 2
            },
            ProjectThreadVisibility =
            [
                new ProjectThreadVisibility
                {
                    Root = @"C:\Project",
                    InteractiveThreads = 3,
                    FirstPageThreads = 2,
                    ExactCwdMatches = 1,
                    VerbatimCwdRows = 2,
                    Ranks = [],
                    RankPreview = string.Empty,
                    ProviderCounts = new() { ["openai"] = 3 }
                }
            ],
            BackupRoot = @"C:\Users\test\.codex\backups_state\provider-sync",
            BackupSummary = new BackupSummary { Count = 2, TotalBytes = 2048 }
        };

        string formatted = TextFormatter.FormatStatus(status, TextFormatter.ChineseSimplified);

        Assert.Contains("当前 Provider: openai（隐式默认）", formatted);
        Assert.Contains("状态扫描时跳过被占用的 rollout 文件: 1", formatted);
        Assert.Contains("状态扫描时跳过不可读的 rollout 文件: 1", formatted);
        Assert.Contains("encrypted_content 警告:", formatted);
        Assert.Contains("SQLite 状态:", formatted);
        Assert.Contains("（旧版根目录）", formatted);
        Assert.Contains("需要修复的 user-event 标记: 1", formatted);
        Assert.Contains("需要修复的 cwd 路径: 2", formatted);
        Assert.Contains("项目可见性:", formatted);
        Assert.Contains("交互会话 3", formatted);
        Assert.Contains("排序位置 无", formatted);
        Assert.DoesNotContain("Current provider", formatted);
        Assert.DoesNotContain("Locked rollout files", formatted);
        Assert.DoesNotContain("(none)", formatted);
    }

    [Fact]
    public void FormatStatus_DefaultOverloadRemainsEnglish()
    {
        StatusSnapshot status = MinimalStatus();

        string formatted = TextFormatter.FormatStatus(status);

        Assert.Contains("Current provider: openai", formatted);
        Assert.Contains("SQLite state:", formatted);
        Assert.DoesNotContain("当前 Provider", formatted);
    }

    [Fact]
    public void ResultFormatters_ChineseCoverSyncRestoreAndBackupCleanup()
    {
        SyncResult sync = new()
        {
            CodexHome = @"C:\Codex",
            TargetProvider = "openai",
            PreviousProvider = "relay",
            BackupDir = @"C:\Backup",
            ChangedSessionFiles = 2,
            SkippedLockedRolloutFiles = [@"C:\locked.jsonl"],
            SkippedUnreadableRolloutFiles = [@"C:\broken.jsonl"],
            SqliteRowsUpdated = 4,
            SqliteProviderRowsUpdated = 2,
            SqliteModelRowsUpdated = 1,
            SqliteUserEventRowsUpdated = 1,
            SqliteCwdRowsUpdated = 1,
            UpdatedWorkspaceRoots = 2,
            SqlitePresent = true,
            RolloutCountsBefore = Counts([], []),
            EncryptedContentCounts = Counts(new() { ["relay"] = 1 }, []),
            EncryptedContentWarning = "English warning should be localized.",
            AutoPruneResult = new BackupPruneResult
            {
                BackupRoot = @"C:\Backup",
                DeletedCount = 1,
                RemainingCount = 5,
                FreedBytes = 1024
            }
        };

        string syncText = TextFormatter.FormatSyncResult(
            sync,
            "已同步",
            TextFormatter.ChineseSimplified);
        Assert.Contains("已同步 Provider: openai", syncText);
        Assert.Contains("已更新 rollout 文件: 2", syncText);
        Assert.Contains("跳过被占用的 rollout 文件: 1", syncText);
        Assert.Contains("跳过不可读的 rollout 文件: 1", syncText);
        Assert.Contains("备份清理: 删除 1，剩余 5", syncText);
        Assert.DoesNotContain("Updated rollout files", syncText);

        RestoreResult restore = new()
        {
            CodexHome = @"C:\Codex",
            BackupDir = @"C:\Backup",
            TargetProvider = "relay",
            ChangedSessionFiles = 2,
            CreatedAt = DateTimeOffset.Parse("2026-07-23T00:00:00Z")
        };
        string restoreText = TextFormatter.FormatRestoreResult(restore, TextFormatter.ChineseSimplified);
        Assert.Contains("已从备份恢复", restoreText);
        Assert.Contains("备份创建时间", restoreText);

        BackupPruneResult prune = new()
        {
            BackupRoot = @"C:\Backup",
            DeletedCount = 2,
            RemainingCount = 3,
            FreedBytes = 2048
        };
        string pruneText = TextFormatter.FormatBackupPruneResult(prune, TextFormatter.ChineseSimplified);
        Assert.Contains("已删除备份: 2", pruneText);
        Assert.Contains("释放空间: 2 KB", pruneText);
    }

    private static StatusSnapshot MinimalStatus() => new()
    {
        CodexHome = @"C:\Codex",
        CurrentProvider = new CurrentProviderInfo("openai", Implicit: false),
        ConfiguredProviders = ["openai"],
        RolloutCounts = Counts([], []),
        LockedRolloutFiles = [],
        UnreadableRolloutFiles = [],
        EncryptedContentCounts = Counts([], []),
        SqliteCounts = null,
        BackupRoot = @"C:\Backup",
        BackupSummary = new BackupSummary { Count = 0, TotalBytes = 0 }
    };

    private static ProviderCounts Counts(
        Dictionary<string, int> sessions,
        Dictionary<string, int> archived) => new()
        {
            Sessions = sessions,
            ArchivedSessions = archived
        };
}
