using System.Collections.Generic;
using System.Linq;

namespace CodexProviderSync.Core;

public static class TextFormatter
{
    public static string FormatStatus(StatusSnapshot status)
    {
        List<string> lines =
        [
            $"Codex home: {status.CodexHome}",
            $"Current provider: {status.CurrentProvider.Provider}{(status.CurrentProvider.Implicit ? " (implicit default)" : string.Empty)}",
            $"Configured providers: {string.Join(", ", status.ConfiguredProviders)}",
            $"Backups: {status.BackupSummary.Count} ({FormatBytes(status.BackupSummary.TotalBytes)})",
            $"Backup root: {status.BackupRoot}",
            string.Empty,
            "Rollout files:",
            $"  sessions: {FormatCounts(status.RolloutCounts.Sessions)}",
            $"  archived_sessions: {FormatCounts(status.RolloutCounts.ArchivedSessions)}",
            $"  encrypted_content sessions: {FormatCounts(status.EncryptedContentCounts.Sessions)}",
            $"  encrypted_content archived_sessions: {FormatCounts(status.EncryptedContentCounts.ArchivedSessions)}",
            string.Empty,
            "SQLite state:"
        ];

        if (!string.IsNullOrWhiteSpace(status.EncryptedContentWarning))
        {
        lines.Insert(11, $"  {status.EncryptedContentWarning}");
        }

        if (status.LockedRolloutFiles.Count > 0)
        {
            lines.Insert(11, $"  Locked rollout files skipped during status scan: {status.LockedRolloutFiles.Count}");
        }

        if (status.SqliteCounts?.Unreadable == true)
        {
            lines.Add($"  {status.SqliteCounts.Error ?? "state_5.sqlite is malformed or unreadable"}");
        }
        else if (status.SqliteCounts is null)
        {
            lines.Add("  state_5.sqlite not found");
        }
        else
        {
            lines.Add($"  sessions: {FormatCounts(status.SqliteCounts.Sessions)}");
            lines.Add($"  archived_sessions: {FormatCounts(status.SqliteCounts.ArchivedSessions)}");
        }

        return string.Join(Environment.NewLine, lines);
    }

    public static string FormatSyncResult(SyncResult result, string label)
    {
        List<string> lines =
        [
            $"{label} provider: {result.TargetProvider}",
            $"Codex home: {result.CodexHome}",
            $"Backup: {result.BackupDir}",
            $"Updated rollout files: {result.ChangedSessionFiles}",
            $"Updated SQLite rows: {result.SqliteRowsUpdated}{(result.SqlitePresent ? string.Empty : " (state_5.sqlite not found)")}"
        ];

        if (result.SkippedLockedRolloutFiles.Count > 0)
        {
            string preview = string.Join(", ", result.SkippedLockedRolloutFiles.Take(5));
            int extraCount = result.SkippedLockedRolloutFiles.Count - Math.Min(result.SkippedLockedRolloutFiles.Count, 5);
            lines.Add($"Skipped locked rollout files: {result.SkippedLockedRolloutFiles.Count}");
            lines.Add($"Locked file(s): {preview}{(extraCount > 0 ? $" (+{extraCount} more)" : string.Empty)}");
        }

        if (!string.IsNullOrWhiteSpace(result.EncryptedContentWarning))
        {
            lines.Add(result.EncryptedContentWarning);
        }

        if (result.AutoPruneResult is not null)
        {
            lines.Add(
                $"Backup cleanup: deleted {result.AutoPruneResult.DeletedCount}, remaining {result.AutoPruneResult.RemainingCount}, freed {FormatBytes(result.AutoPruneResult.FreedBytes)}");
        }

        if (!string.IsNullOrWhiteSpace(result.AutoPruneWarning))
        {
            lines.Add($"Backup cleanup warning: {result.AutoPruneWarning}");
        }

        return string.Join(Environment.NewLine, lines);
    }

    public static string FormatRestoreResult(RestoreResult result)
    {
        List<string> lines =
        [
            $"Restored backup from {result.BackupDir}",
            $"Codex home: {result.CodexHome}",
            $"Provider at backup time: {result.TargetProvider}",
            $"Backed up rollout file count: {result.ChangedSessionFiles}"
        ];

        if (result.CreatedAt is not null)
        {
            lines.Add($"Backup created at: {result.CreatedAt:O}");
        }

        return string.Join(Environment.NewLine, lines);
    }

    public static string FormatBackupPruneResult(BackupPruneResult result)
    {
        List<string> lines =
        [
            $"Backup root: {result.BackupRoot}",
            $"Deleted backups: {result.DeletedCount}",
            $"Remaining backups: {result.RemainingCount}",
            $"Freed space: {FormatBytes(result.FreedBytes)}"
        ];

        return string.Join(Environment.NewLine, lines);
    }

    public static string FormatProviderSources(ProviderOption option)
    {
        return string.Join(", ", option.Sources.Select(source => source switch
        {
            ProviderSource.Config => "配置",
            ProviderSource.Rollout => "Rollout",
            ProviderSource.Sqlite => "SQLite",
            ProviderSource.Manual => "手动",
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
