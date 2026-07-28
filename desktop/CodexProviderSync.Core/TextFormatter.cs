using System.Collections.Generic;
using System.Linq;

namespace CodexProviderSync.Core;

public static class TextFormatter
{
    public const string English = "en";
    public const string ChineseSimplified = "zh-Hans";

    public static string FormatStatus(StatusSnapshot status) => FormatStatus(status, English);

    public static string FormatStatus(StatusSnapshot status, string language)
    {
        return IsChinese(language)
            ? FormatStatusChinese(status)
            : FormatStatusEnglish(status);
    }

    public static string FormatSyncResult(SyncResult result, string label) =>
        FormatSyncResult(result, label, English);

    public static string FormatSyncResult(SyncResult result, string label, string language)
    {
        return IsChinese(language)
            ? FormatSyncResultChinese(result, label)
            : FormatSyncResultEnglish(result, label);
    }

    public static string FormatRestoreResult(RestoreResult result) =>
        FormatRestoreResult(result, English);

    public static string FormatRestoreResult(RestoreResult result, string language)
    {
        List<string> lines = IsChinese(language)
            ?
            [
                $"已从备份恢复: {result.BackupDir}",
                $"Codex Home: {result.CodexHome}",
                $"备份时的 Provider: {result.TargetProvider}",
                $"备份的 rollout 文件数量: {result.ChangedSessionFiles}"
            ]
            :
            [
                $"Restored backup from {result.BackupDir}",
                $"Codex home: {result.CodexHome}",
                $"Provider at backup time: {result.TargetProvider}",
                $"Backed up rollout file count: {result.ChangedSessionFiles}"
            ];

        if (result.CreatedAt is not null)
        {
            lines.Add(IsChinese(language)
                ? $"备份创建时间: {result.CreatedAt:O}"
                : $"Backup created at: {result.CreatedAt:O}");
        }

        return string.Join(Environment.NewLine, lines);
    }

    public static string FormatBackupPruneResult(BackupPruneResult result) =>
        FormatBackupPruneResult(result, English);

    public static string FormatBackupPruneResult(BackupPruneResult result, string language)
    {
        return IsChinese(language)
            ? string.Join(Environment.NewLine, new[]
            {
                $"备份根目录: {result.BackupRoot}",
                $"已删除备份: {result.DeletedCount}",
                $"剩余备份: {result.RemainingCount}",
                $"释放空间: {FormatBytes(result.FreedBytes)}"
            })
            : string.Join(Environment.NewLine, new[]
            {
                $"Backup root: {result.BackupRoot}",
                $"Deleted backups: {result.DeletedCount}",
                $"Remaining backups: {result.RemainingCount}",
                $"Freed space: {FormatBytes(result.FreedBytes)}"
            });
    }

    public static string FormatProviderSources(ProviderOption option) =>
        FormatProviderSources(option, English);

    public static string FormatProviderSources(ProviderOption option, string language)
    {
        bool chinese = IsChinese(language);
        return string.Join(", ", option.Sources.Select(source => source switch
        {
            ProviderSource.Config => chinese ? "配置" : "Config",
            ProviderSource.Rollout => "Rollout",
            ProviderSource.Sqlite => "SQLite",
            ProviderSource.Manual => chinese ? "手动" : "Manual",
            _ => source.ToString()
        }));
    }

    public static bool IsChinese(string? language) =>
        string.Equals(language, ChineseSimplified, StringComparison.Ordinal);

    private static string FormatStatusEnglish(StatusSnapshot status)
    {
        List<string> lines =
        [
            $"Codex home: {status.CodexHome}",
            $"SQLite home: {status.SqliteHome} (source: {status.SqliteHomeSource})",
            $"Current provider: {status.CurrentProvider.Provider}{(status.CurrentProvider.Implicit ? " (implicit default)" : string.Empty)}",
            $"Configured providers: {string.Join(", ", status.ConfiguredProviders)}",
            $"Backups: {status.BackupSummary.Count} ({FormatBytes(status.BackupSummary.TotalBytes)})",
            $"Backup root: {status.BackupRoot}",
            string.Empty,
            "Rollout files:",
            $"  sessions: {FormatCounts(status.RolloutCounts.Sessions, false)}",
            $"  archived_sessions: {FormatCounts(status.RolloutCounts.ArchivedSessions, false)}",
            $"  encrypted_content sessions: {FormatCounts(status.EncryptedContentCounts.Sessions, false)}",
            $"  encrypted_content archived_sessions: {FormatCounts(status.EncryptedContentCounts.ArchivedSessions, false)}",
            string.Empty,
            "SQLite state:"
        ];

        List<string> rolloutNotes = [];
        if (status.LockedRolloutFiles.Count > 0)
        {
            rolloutNotes.Add($"  Locked rollout files skipped during status scan: {status.LockedRolloutFiles.Count}");
        }
        if (status.UnreadableRolloutFiles.Count > 0)
        {
            rolloutNotes.Add($"  Unreadable rollout files skipped during status scan: {status.UnreadableRolloutFiles.Count}");
        }
        if (!string.IsNullOrWhiteSpace(status.EncryptedContentWarning))
        {
            rolloutNotes.Add($"  {status.EncryptedContentWarning}");
        }
        lines.InsertRange(12, rolloutNotes);

        AppendSqliteStatus(lines, status, chinese: false);
        AppendProjectVisibility(lines, status, chinese: false);
        return string.Join(Environment.NewLine, lines);
    }

    private static string FormatStatusChinese(StatusSnapshot status)
    {
        List<string> lines =
        [
            $"Codex Home: {status.CodexHome}",
            $"SQLite Home: {status.SqliteHome}（来源: {status.SqliteHomeSource}）",
            $"当前 Provider: {status.CurrentProvider.Provider}{(status.CurrentProvider.Implicit ? "（隐式默认）" : string.Empty)}",
            $"配置中的 Provider: {string.Join(", ", status.ConfiguredProviders)}",
            $"备份: {status.BackupSummary.Count}（{FormatBytes(status.BackupSummary.TotalBytes)}）",
            $"备份根目录: {status.BackupRoot}",
            string.Empty,
            "Rollout 文件:",
            $"  sessions: {FormatCounts(status.RolloutCounts.Sessions, true)}",
            $"  archived_sessions: {FormatCounts(status.RolloutCounts.ArchivedSessions, true)}",
            $"  包含 encrypted_content 的 sessions: {FormatCounts(status.EncryptedContentCounts.Sessions, true)}",
            $"  包含 encrypted_content 的 archived_sessions: {FormatCounts(status.EncryptedContentCounts.ArchivedSessions, true)}",
            string.Empty,
            "SQLite 状态:"
        ];

        List<string> rolloutNotes = [];
        if (status.LockedRolloutFiles.Count > 0)
        {
            rolloutNotes.Add($"  状态扫描时跳过被占用的 rollout 文件: {status.LockedRolloutFiles.Count}");
        }
        if (status.UnreadableRolloutFiles.Count > 0)
        {
            rolloutNotes.Add($"  状态扫描时跳过不可读的 rollout 文件: {status.UnreadableRolloutFiles.Count}");
        }
        if (!string.IsNullOrWhiteSpace(status.EncryptedContentWarning))
        {
            rolloutNotes.Add($"  {FormatEncryptedContentWarningChinese(
                status.EncryptedContentCounts,
                status.CurrentProvider.Provider)}");
        }
        lines.InsertRange(12, rolloutNotes);

        AppendSqliteStatus(lines, status, chinese: true);
        AppendProjectVisibility(lines, status, chinese: true);
        return string.Join(Environment.NewLine, lines);
    }

    private static void AppendSqliteStatus(List<string> lines, StatusSnapshot status, bool chinese)
    {
        if (status.StateDbLocation is not null)
        {
            string legacyNote = status.StateDbLocation.Source == "legacy-root"
                ? chinese ? "（旧版根目录）" : " (legacy root)"
                : string.Empty;
            lines.Add(chinese
                ? $"  数据库: {status.StateDbLocation.Path}{legacyNote}"
                : $"  database: {status.StateDbLocation.Path}{legacyNote}");
        }
        else
        {
            string checkedPaths = string.Join(", ", status.CheckedStateDbPaths);
            lines.Add(chinese
                ? $"  未找到数据库（已检查: {checkedPaths}）"
                : $"  database: not found (checked: {checkedPaths})");
        }

        if (status.SqliteCounts?.Unreadable == true)
        {
            lines.Add(chinese
                ? $"  state_5.sqlite 损坏或不可读{FormatRawDetail(status.SqliteCounts.Error)}"
                : $"  {status.SqliteCounts.Error ?? "state_5.sqlite is malformed or unreadable"}");
        }
        else if (status.SqliteCounts is null)
        {
            lines.Add(chinese ? "  未找到 state_5.sqlite" : "  state_5.sqlite not found");
        }
        else
        {
            lines.Add($"  sessions: {FormatCounts(status.SqliteCounts.Sessions, chinese)}");
            lines.Add($"  archived_sessions: {FormatCounts(status.SqliteCounts.ArchivedSessions, chinese)}");
            if (status.SqliteRepairStats?.UserEventRowsNeedingRepair > 0)
            {
                lines.Add(chinese
                    ? $"  需要修复的 user-event 标记: {status.SqliteRepairStats.UserEventRowsNeedingRepair}"
                    : $"  user-event flags needing repair: {status.SqliteRepairStats.UserEventRowsNeedingRepair}");
            }
            if (status.SqliteRepairStats?.CwdRowsNeedingRepair > 0)
            {
                lines.Add(chinese
                    ? $"  需要修复的 cwd 路径: {status.SqliteRepairStats.CwdRowsNeedingRepair}"
                    : $"  cwd paths needing repair: {status.SqliteRepairStats.CwdRowsNeedingRepair}");
            }
        }
    }

    private static void AppendProjectVisibility(List<string> lines, StatusSnapshot status, bool chinese)
    {
        if (status.ProjectThreadVisibility.Count == 0)
        {
            return;
        }

        lines.Add(string.Empty);
        lines.Add(chinese ? "项目可见性:" : "Project visibility:");
        foreach (ProjectThreadVisibility project in status.ProjectThreadVisibility)
        {
            string rankText = string.IsNullOrWhiteSpace(project.RankPreview)
                ? chinese ? "无" : "(none)"
                : project.RankPreview;
            lines.Add(chinese
                ? $"  {project.Root}: 交互会话 {project.InteractiveThreads}，首屏 {project.FirstPageThreads}/50，排序位置 {rankText}，精确 cwd {project.ExactCwdMatches}/{project.InteractiveThreads}，原始 cwd 行 {project.VerbatimCwdRows}，Provider {FormatCounts(project.ProviderCounts, true)}"
                : $"  {project.Root}: interactive {project.InteractiveThreads}, first page {project.FirstPageThreads}/50, ranks {rankText}, exact cwd {project.ExactCwdMatches}/{project.InteractiveThreads}, verbatim cwd {project.VerbatimCwdRows}, providers {FormatCounts(project.ProviderCounts, false)}");
        }
    }

    private static string FormatSyncResultEnglish(SyncResult result, string label)
    {
        List<string> lines =
        [
            $"{label} provider: {result.TargetProvider}",
            $"Codex home: {result.CodexHome}",
            $"SQLite home: {result.SqliteHome} (source: {result.SqliteHomeSource})",
            $"Backup: {result.BackupDir}",
            $"Updated rollout files: {result.ChangedSessionFiles}",
            $"Updated SQLite rows: {result.SqliteRowsUpdated}{(result.SqlitePresent ? string.Empty : " (state_5.sqlite not found)")}"
        ];

        AppendSyncDetails(lines, result, chinese: false);
        return string.Join(Environment.NewLine, lines);
    }

    private static string FormatSyncResultChinese(SyncResult result, string label)
    {
        List<string> lines =
        [
            $"{label} Provider: {result.TargetProvider}",
            $"Codex Home: {result.CodexHome}",
            $"SQLite Home: {result.SqliteHome}（来源: {result.SqliteHomeSource}）",
            $"备份目录: {result.BackupDir}",
            $"已更新 rollout 文件: {result.ChangedSessionFiles}",
            $"已更新 SQLite 行: {result.SqliteRowsUpdated}{(result.SqlitePresent ? string.Empty : "（未找到 state_5.sqlite）")}"
        ];

        AppendSyncDetails(lines, result, chinese: true);
        return string.Join(Environment.NewLine, lines);
    }

    private static void AppendSyncDetails(List<string> lines, SyncResult result, bool chinese)
    {
        if (result.SqliteUserEventRowsUpdated > 0)
        {
            lines.Add(chinese
                ? $"已更新 SQLite user-event 标记: {result.SqliteUserEventRowsUpdated}"
                : $"Updated SQLite user-event flags: {result.SqliteUserEventRowsUpdated}");
        }
        if (result.SqliteModelRowsUpdated > 0)
        {
            lines.Add(chinese
                ? $"已更新 SQLite 每线程 model: {result.SqliteModelRowsUpdated}"
                : $"Updated SQLite per-thread models: {result.SqliteModelRowsUpdated}");
        }
        if (result.SqliteCwdRowsUpdated > 0)
        {
            lines.Add(chinese
                ? $"已更新 SQLite cwd 路径: {result.SqliteCwdRowsUpdated}"
                : $"Updated SQLite cwd paths: {result.SqliteCwdRowsUpdated}");
        }
        if (result.UpdatedWorkspaceRoots > 0)
        {
            lines.Add(chinese
                ? $"已更新 workspace roots: {result.UpdatedWorkspaceRoots}"
                : $"Updated workspace roots: {result.UpdatedWorkspaceRoots}");
        }

        if (result.SkippedLockedRolloutFiles.Count > 0)
        {
            string preview = string.Join(", ", result.SkippedLockedRolloutFiles.Take(5));
            int extraCount = result.SkippedLockedRolloutFiles.Count - Math.Min(result.SkippedLockedRolloutFiles.Count, 5);
            lines.Add(chinese
                ? $"跳过被占用的 rollout 文件: {result.SkippedLockedRolloutFiles.Count}"
                : $"Skipped locked rollout files: {result.SkippedLockedRolloutFiles.Count}");
            lines.Add(chinese
                ? $"被占用的文件: {preview}{(extraCount > 0 ? $"（另有 {extraCount} 个）" : string.Empty)}"
                : $"Locked file(s): {preview}{(extraCount > 0 ? $" (+{extraCount} more)" : string.Empty)}");
        }
        if (result.SkippedUnreadableRolloutFiles.Count > 0)
        {
            string preview = string.Join(", ", result.SkippedUnreadableRolloutFiles.Take(5));
            int extraCount = result.SkippedUnreadableRolloutFiles.Count - Math.Min(result.SkippedUnreadableRolloutFiles.Count, 5);
            lines.Add(chinese
                ? $"跳过不可读的 rollout 文件: {result.SkippedUnreadableRolloutFiles.Count}"
                : $"Skipped unreadable rollout files: {result.SkippedUnreadableRolloutFiles.Count}");
            lines.Add(chinese
                ? $"不可读的文件: {preview}{(extraCount > 0 ? $"（另有 {extraCount} 个）" : string.Empty)}"
                : $"Unreadable file(s): {preview}{(extraCount > 0 ? $" (+{extraCount} more)" : string.Empty)}");
        }

        if (!string.IsNullOrWhiteSpace(result.EncryptedContentWarning))
        {
            lines.Add(chinese
                ? FormatEncryptedContentWarningChinese(result.EncryptedContentCounts, result.TargetProvider)
                : result.EncryptedContentWarning);
        }
        if (result.AutoPruneResult is not null)
        {
            lines.Add(chinese
                ? $"备份清理: 删除 {result.AutoPruneResult.DeletedCount}，剩余 {result.AutoPruneResult.RemainingCount}，释放 {FormatBytes(result.AutoPruneResult.FreedBytes)}"
                : $"Backup cleanup: deleted {result.AutoPruneResult.DeletedCount}, remaining {result.AutoPruneResult.RemainingCount}, freed {FormatBytes(result.AutoPruneResult.FreedBytes)}");
        }
        if (!string.IsNullOrWhiteSpace(result.AutoPruneWarning))
        {
            lines.Add(chinese
                ? $"备份清理警告: {result.AutoPruneWarning}"
                : $"Backup cleanup warning: {result.AutoPruneWarning}");
        }
    }

    private static string FormatEncryptedContentWarningChinese(
        ProviderCounts encryptedContentCounts,
        string targetProvider)
    {
        HashSet<string> riskyProviders = new(StringComparer.Ordinal);
        foreach (Dictionary<string, int> counts in new[]
                 {
                     encryptedContentCounts.Sessions,
                     encryptedContentCounts.ArchivedSessions
                 })
        {
            foreach ((string provider, int count) in counts)
            {
                if (count > 0 && !string.Equals(provider, targetProvider, StringComparison.Ordinal))
                {
                    riskyProviders.Add(provider);
                }
            }
        }

        int total = encryptedContentCounts.Sessions.Values.Sum()
            + encryptedContentCounts.ArchivedSessions.Values.Sum();
        string providers = riskyProviders.Count == 0
            ? "未知 Provider"
            : string.Join(", ", riskyProviders.Order(StringComparer.Ordinal));
        return $"encrypted_content 警告: {total} 个 rollout 文件包含来自 {providers} 的 encrypted_content。元数据可以同步到 {targetProvider}，但继续对话或 compact 仍可能失败；如需可靠续聊，请切回原 Provider/账号或新建会话。";
    }

    private static string FormatRawDetail(string? detail) =>
        string.IsNullOrWhiteSpace(detail) ? string.Empty : $"（详细信息: {detail}）";

    private static string FormatCounts(Dictionary<string, int> counts, bool chinese)
    {
        return counts.Count == 0
            ? chinese ? "无" : "(none)"
            : string.Join(", ", counts.OrderBy(pair => pair.Key, StringComparer.Ordinal).Select(pair => $"{pair.Key}: {pair.Value}"));
    }

    private static string FormatBytes(long bytes)
    {
        string[] units = ["B", "KB", "MB", "GB", "TB"];
        double value = bytes;
        int unitIndex = 0;
        while (value >= 1024 && unitIndex < units.Length - 1)
        {
            value /= 1024;
            unitIndex += 1;
        }

        return unitIndex == 0 ? $"{bytes} B" : $"{value:0.##} {units[unitIndex]}";
    }
}
