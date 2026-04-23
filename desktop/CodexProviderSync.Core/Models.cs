using System;
using System.Collections.Generic;

namespace CodexProviderSync.Core;

public sealed record CurrentProviderInfo(string Provider, bool Implicit);

public sealed class ProviderCounts
{
    public Dictionary<string, int> Sessions { get; init; } = new(StringComparer.Ordinal);
    public Dictionary<string, int> ArchivedSessions { get; init; } = new(StringComparer.Ordinal);
}

public sealed class StatusSnapshot
{
    public required string CodexHome { get; init; }
    public required CurrentProviderInfo CurrentProvider { get; init; }
    public required IReadOnlyList<string> ConfiguredProviders { get; init; }
    public required ProviderCounts RolloutCounts { get; init; }
    public required ProviderCounts? SqliteCounts { get; init; }
    public required string BackupRoot { get; init; }
    public required BackupSummary BackupSummary { get; init; }
}

public sealed class BackupSummary
{
    public required int Count { get; init; }
    public required long TotalBytes { get; init; }
}

public sealed class BackupPruneResult
{
    public required string BackupRoot { get; init; }
    public required int DeletedCount { get; init; }
    public required int RemainingCount { get; init; }
    public required long FreedBytes { get; init; }
}

public sealed class SessionChange
{
    public required string Path { get; init; }
    public string? ThreadId { get; init; }
    public required string Directory { get; init; }
    public required string OriginalFirstLine { get; init; }
    public required string OriginalSeparator { get; init; }
    public required int OriginalOffset { get; init; }
    public required long OriginalFileLength { get; init; }
    public required long OriginalLastWriteTimeUtcTicks { get; init; }
    public required long NormalizedLastWriteTimeUtcTicks { get; set; }
    public required string UpdatedFirstLine { get; init; }
}

public sealed class SessionChangeCollection
{
    public required IReadOnlyList<SessionChange> Changes { get; init; }
    public required IReadOnlyList<string> LockedPaths { get; init; }
    public required ProviderCounts ProviderCounts { get; init; }
}

public sealed class SyncResult
{
    public required string CodexHome { get; init; }
    public required string TargetProvider { get; init; }
    public required string PreviousProvider { get; init; }
    public required string BackupDir { get; init; }
    public required int ChangedSessionFiles { get; init; }
    public required IReadOnlyList<string> SkippedLockedRolloutFiles { get; init; }
    public required int SqliteRowsUpdated { get; init; }
    public required bool SqlitePresent { get; init; }
    public required ProviderCounts RolloutCountsBefore { get; init; }
    public bool ConfigUpdated { get; init; }
    public BackupPruneResult? AutoPruneResult { get; init; }
    public string? AutoPruneWarning { get; init; }
}

public sealed class SessionApplyResult
{
    public required int AppliedCount { get; init; }
    public required IReadOnlyList<string> AppliedPaths { get; init; }
    public required IReadOnlyList<string> SkippedPaths { get; init; }
}

public sealed class RestoreResult
{
    public required string CodexHome { get; init; }
    public required string BackupDir { get; init; }
    public required string TargetProvider { get; init; }
    public DateTimeOffset? CreatedAt { get; init; }
    public int ChangedSessionFiles { get; init; }
}

public enum ProviderSource
{
    Config,
    Rollout,
    Sqlite,
    Manual
}

public sealed class ProviderOption
{
    public required string Id { get; init; }
    public required IReadOnlyList<ProviderSource> Sources { get; init; }
    public bool IsCurrentProvider { get; init; }
    public bool IsManual { get; init; }
    public bool IsSaved { get; init; }
}

public sealed class WindowBoundsState
{
    public int X { get; init; }
    public int Y { get; init; }
    public int Width { get; init; }
    public int Height { get; init; }
    public bool Maximized { get; init; }
}

public sealed class AppSettings
{
    public List<string> RecentCodexHomes { get; init; } = [];
    public string? LastCodexHome { get; init; }
    public List<string> SavedProviders { get; init; } = [];
    public List<string> ManualProviders { get; init; } = [];
    public string? LastSelectedProvider { get; init; }
    public string? LastBackupDirectory { get; init; }
    public int BackupRetentionCount { get; init; } = AppConstants.DefaultBackupRetentionCount;
    public WindowBoundsState? WindowBounds { get; init; }
}

public sealed class RestoreBackupOptions
{
    public bool RestoreConfig { get; init; } = true;
    public bool RestoreDatabase { get; init; } = true;
    public bool RestoreSessions { get; init; } = true;
}

internal sealed class BackupMetadataFile
{
    public int Version { get; init; }
    public required string Namespace { get; init; }
    public required string CodexHome { get; init; }
    public required string TargetProvider { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
    public required List<string> DbFiles { get; init; }
    public int ChangedSessionFiles { get; init; }
}

internal sealed class SessionBackupManifest
{
    public int Version { get; init; }
    public required string Namespace { get; init; }
    public required string CodexHome { get; init; }
    public required string TargetProvider { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
    public required List<SessionBackupManifestEntry> Files { get; init; }
}

internal sealed class SessionBackupManifestEntry
{
    public required string Path { get; init; }
    public required string OriginalFirstLine { get; init; }
    public required string OriginalSeparator { get; init; }
    public long? NormalizedLastWriteTimeUtcTicks { get; init; }
}
