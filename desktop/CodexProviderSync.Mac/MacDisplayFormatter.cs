using CodexProviderSync.Core;

namespace CodexProviderSync.Mac;

internal static class MacDisplayFormatter
{
    public static string FormatStatus(StatusSnapshot status, string language)
    {
        if (!MacUiText.IsChinese(language))
        {
            return TextFormatter.FormatStatus(status);
        }

        List<string> lines =
        [
            $"Codex Home: {status.CodexHome}",
            $"当前 Provider: {status.CurrentProvider.Provider}{(status.CurrentProvider.Implicit ? "（隐式默认）" : string.Empty)}",
            $"配置中的 Provider: {string.Join(", ", status.ConfiguredProviders)}",
            $"备份: {status.BackupSummary.Count}（{FormatBytes(status.BackupSummary.TotalBytes)}）",
            $"备份目录: {status.BackupRoot}",
            string.Empty,
            "Rollout 文件:",
            $"  sessions: {FormatCounts(status.RolloutCounts.Sessions)}",
            $"  archived_sessions: {FormatCounts(status.RolloutCounts.ArchivedSessions)}",
            $"  encrypted_content sessions: {FormatCounts(status.EncryptedContentCounts.Sessions)}",
            $"  encrypted_content archived_sessions: {FormatCounts(status.EncryptedContentCounts.ArchivedSessions)}",
            string.Empty,
            "SQLite 状态:"
        ];

        List<string> rolloutNotes = [];
        if (status.LockedRolloutFiles.Count > 0)
        {
            rolloutNotes.Add($"  状态扫描时跳过 locked rollout 文件: {status.LockedRolloutFiles.Count}");
        }
        if (status.UnreadableRolloutFiles.Count > 0)
        {
            rolloutNotes.Add($"  状态扫描时跳过不可读 rollout 文件: {status.UnreadableRolloutFiles.Count}");
        }
        if (!string.IsNullOrWhiteSpace(status.EncryptedContentWarning))
        {
            rolloutNotes.Add($"  {status.EncryptedContentWarning}");
        }
        lines.InsertRange(11, rolloutNotes);

        if (status.StateDbLocation is not null)
        {
            string legacyNote = status.StateDbLocation.Source == "legacy-root" ? " (legacy root)" : string.Empty;
            lines.Add($"  database: {status.StateDbLocation.Path}{legacyNote}");
        }
        else
        {
            lines.Add("  database: not found (checked sqlite/state_5.sqlite, state_5.sqlite)");
        }

        if (status.SqliteCounts?.Unreadable == true)
        {
            lines.Add($"  {status.SqliteCounts.Error ?? "state_5.sqlite 损坏或不可读"}");
        }
        else if (status.SqliteCounts is null)
        {
            lines.Add("  未找到 state_5.sqlite");
        }
        else
        {
            lines.Add($"  sessions: {FormatCounts(status.SqliteCounts.Sessions)}");
            lines.Add($"  archived_sessions: {FormatCounts(status.SqliteCounts.ArchivedSessions)}");
            if (status.SqliteRepairStats?.UserEventRowsNeedingRepair > 0)
            {
                lines.Add($"  需要修复的 user-event 标记: {status.SqliteRepairStats.UserEventRowsNeedingRepair}");
            }
            if (status.SqliteRepairStats?.CwdRowsNeedingRepair > 0)
            {
                lines.Add($"  需要修复的 cwd 路径: {status.SqliteRepairStats.CwdRowsNeedingRepair}");
            }
        }

        if (status.ProjectThreadVisibility.Count > 0)
        {
            lines.Add(string.Empty);
            lines.Add("项目可见性:");
            foreach (ProjectThreadVisibility project in status.ProjectThreadVisibility)
            {
                string rankText = string.IsNullOrWhiteSpace(project.RankPreview) ? "（无）" : project.RankPreview;
                lines.Add(
                    $"  {project.Root}: interactive {project.InteractiveThreads}, 首屏 {project.FirstPageThreads}/50, ranks {rankText}, 精确 cwd {project.ExactCwdMatches}/{project.InteractiveThreads}, 原始 cwd 行 {project.VerbatimCwdRows}, providers {FormatCounts(project.ProviderCounts)}");
            }
        }

        return string.Join(Environment.NewLine, lines);
    }

    public static string FormatSyncResult(SyncResult result, string label, string language)
    {
        if (!MacUiText.IsChinese(language))
        {
            return TextFormatter.FormatSyncResult(result, label);
        }

        List<string> lines =
        [
            $"{label} provider: {result.TargetProvider}",
            $"Codex Home: {result.CodexHome}",
            $"备份: {result.BackupDir}",
            $"已更新 rollout 文件: {result.ChangedSessionFiles}",
            $"已更新 SQLite 行: {result.SqliteRowsUpdated}{(result.SqlitePresent ? string.Empty : "（未找到 state_5.sqlite）")}"
        ];

        if (result.SqliteUserEventRowsUpdated > 0)
        {
            lines.Add($"已更新 SQLite user-event 标记: {result.SqliteUserEventRowsUpdated}");
        }
        if (result.SqliteModelRowsUpdated > 0)
        {
            lines.Add($"已更新 SQLite 每线程 model: {result.SqliteModelRowsUpdated}");
        }
        if (result.SqliteCwdRowsUpdated > 0)
        {
            lines.Add($"已更新 SQLite cwd 路径: {result.SqliteCwdRowsUpdated}");
        }
        if (result.UpdatedWorkspaceRoots > 0)
        {
            lines.Add($"已更新 workspace roots: {result.UpdatedWorkspaceRoots}");
        }
        if (result.SkippedLockedRolloutFiles.Count > 0)
        {
            string preview = string.Join(", ", result.SkippedLockedRolloutFiles.Take(5));
            int extraCount = result.SkippedLockedRolloutFiles.Count - Math.Min(result.SkippedLockedRolloutFiles.Count, 5);
            lines.Add($"跳过 locked rollout 文件: {result.SkippedLockedRolloutFiles.Count}");
            lines.Add($"Locked 文件: {preview}{(extraCount > 0 ? $"（另有 {extraCount} 个）" : string.Empty)}");
        }
        if (result.SkippedUnreadableRolloutFiles.Count > 0)
        {
            string preview = string.Join(", ", result.SkippedUnreadableRolloutFiles.Take(5));
            int extraCount = result.SkippedUnreadableRolloutFiles.Count - Math.Min(result.SkippedUnreadableRolloutFiles.Count, 5);
            lines.Add($"跳过不可读 rollout 文件: {result.SkippedUnreadableRolloutFiles.Count}");
            lines.Add($"不可读文件: {preview}{(extraCount > 0 ? $"（另有 {extraCount} 个）" : string.Empty)}");
        }
        if (!string.IsNullOrWhiteSpace(result.EncryptedContentWarning))
        {
            lines.Add(result.EncryptedContentWarning);
        }
        if (result.AutoPruneResult is not null)
        {
            lines.Add(
                $"备份清理: 删除 {result.AutoPruneResult.DeletedCount}, 剩余 {result.AutoPruneResult.RemainingCount}, 释放 {FormatBytes(result.AutoPruneResult.FreedBytes)}");
        }
        if (!string.IsNullOrWhiteSpace(result.AutoPruneWarning))
        {
            lines.Add($"备份清理警告: {result.AutoPruneWarning}");
        }

        return string.Join(Environment.NewLine, lines);
    }

    public static string FormatRestoreResult(RestoreResult result, string language)
    {
        if (!MacUiText.IsChinese(language))
        {
            return TextFormatter.FormatRestoreResult(result);
        }

        List<string> lines =
        [
            $"已从备份恢复: {result.BackupDir}",
            $"Codex Home: {result.CodexHome}",
            $"备份时的 Provider: {result.TargetProvider}",
            $"备份的 rollout 文件数量: {result.ChangedSessionFiles}"
        ];

        if (result.CreatedAt is not null)
        {
            lines.Add($"备份创建时间: {result.CreatedAt:O}");
        }

        return string.Join(Environment.NewLine, lines);
    }

    public static string FormatBackupPruneResult(BackupPruneResult result, string language)
    {
        if (!MacUiText.IsChinese(language))
        {
            return TextFormatter.FormatBackupPruneResult(result);
        }

        return string.Join(Environment.NewLine, new[]
        {
            $"备份根目录: {result.BackupRoot}",
            $"已删除备份: {result.DeletedCount}",
            $"剩余备份: {result.RemainingCount}",
            $"释放空间: {FormatBytes(result.FreedBytes)}"
        });
    }

    public static string FormatProviderSources(ProviderOption option, string language)
    {
        return string.Join(", ", option.Sources.Select(source => source switch
        {
            ProviderSource.Config => MacUiText.IsChinese(language) ? "配置" : "Config",
            ProviderSource.Rollout => "Rollout",
            ProviderSource.Sqlite => "SQLite",
            ProviderSource.Manual => MacUiText.IsChinese(language) ? "手动" : "Manual",
            _ => source.ToString()
        }));
    }

    private static string FormatCounts(Dictionary<string, int> counts)
    {
        return counts.Count == 0
            ? "(none)"
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
