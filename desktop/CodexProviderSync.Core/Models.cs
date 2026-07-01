using System;
using System.Collections.Generic;

namespace CodexProviderSync.Core;

public sealed record CurrentProviderInfo(string Provider, bool Implicit);

public sealed class ProviderCounts
{
    public Dictionary<string, int> Sessions { get; init; } = new(StringComparer.Ordinal);
    public Dictionary<string, int> ArchivedSessions { get; init; } = new(StringComparer.Ordinal);
    public bool Unreadable { get; init; }
    public string? Error { get; init; }
}

public sealed class StatusSnapshot
{
    public required string CodexHome { get; init; }
    public required CurrentProviderInfo CurrentProvider { get; init; }
    public required IReadOnlyList<string> ConfiguredProviders { get; init; }
    public required ProviderCounts RolloutCounts { get; init; }
    public required IReadOnlyList<string> LockedRolloutFiles { get; init; }
    public required IReadOnlyList<string> UnreadableRolloutFiles { get; init; }
    public required ProviderCounts EncryptedContentCounts { get; init; }
    public string? EncryptedContentWarning { get; init; }
    public required ProviderCounts? SqliteCounts { get; init; }
    public StateDbLocation? StateDbLocation { get; init; }
    public SqliteRepairStats? SqliteRepairStats { get; init; }
    public IReadOnlyList<ProjectThreadVisibility> ProjectThreadVisibility { get; init; } = [];
    public required string BackupRoot { get; init; }
    public required BackupSummary BackupSummary { get; init; }
}

public sealed record StateDbLocation(string Path, string RelativePath, string Source);

public sealed class SqliteRepairStats
{
    public required int UserEventRowsNeedingRepair { get; init; }
    public required int CwdRowsNeedingRepair { get; init; }
}

public sealed class ProjectThreadVisibility
{
    public required string Root { get; init; }
    public required int InteractiveThreads { get; init; }
    public required int FirstPageThreads { get; init; }
    public required int ExactCwdMatches { get; init; }
    public required int VerbatimCwdRows { get; init; }
    public required IReadOnlyList<int> Ranks { get; init; }
    public required string RankPreview { get; init; }
    public required Dictionary<string, int> ProviderCounts { get; init; }
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
    public required string OriginalProvider { get; init; }
    // The model that the rollout's first `turn_context` event
    // currently advertises. Used by the rewrite pass to know which
    // value to swap out across every turn_context line in the file
    // so that the Codex GUI bottom-right of an old conversation
    // reflects the active provider's model. Null when the rollout
    // does not expose a model field we recognise (e.g. legacy or
    // unusually formatted files), in which case the model rewrite
    // pass is a no-op.
    public string? OriginalModel { get; init; }
    // True when the provider line in `session_meta` needs to be
    // rewritten to the new provider; false when the rollout is
    // already on the right provider and only the per-turn `model`
    // field has drifted. When false, `UpdatedFirstLine` is null and
    // the first-line rewrite pass is skipped.
    public bool ProviderNeedsUpdate { get; init; } = true;
    public string? UpdatedFirstLine { get; init; }
}

public sealed class SessionChangeCollection
{
    public required IReadOnlyList<SessionChange> Changes { get; init; }
    public required IReadOnlyList<string> LockedPaths { get; init; }
    public required IReadOnlyList<string> UnreadablePaths { get; init; }
    public required ProviderCounts ProviderCounts { get; init; }
    public required ProviderCounts EncryptedContentCounts { get; init; }
    public required IReadOnlyCollection<string> UserEventThreadIds { get; init; }
    public required IReadOnlyDictionary<string, string> ThreadCwdsById { get; init; }
}

public sealed class SyncResult
{
    public required string CodexHome { get; init; }
    public required string TargetProvider { get; init; }
    public required string PreviousProvider { get; init; }
    public required string BackupDir { get; init; }
    public required int ChangedSessionFiles { get; init; }
    public required IReadOnlyList<string> SkippedLockedRolloutFiles { get; init; }
    public required IReadOnlyList<string> SkippedUnreadableRolloutFiles { get; init; }
    public required int SqliteRowsUpdated { get; init; }
    public int SqliteProviderRowsUpdated { get; init; }
    public int SqliteModelRowsUpdated { get; init; }
    public int SqliteUserEventRowsUpdated { get; init; }
    public int SqliteCwdRowsUpdated { get; init; }
    public int UpdatedWorkspaceRoots { get; init; }
    public int SavedWorkspaceRootCount { get; init; }
    public required bool SqlitePresent { get; init; }
    public required ProviderCounts RolloutCountsBefore { get; init; }
    public required ProviderCounts EncryptedContentCounts { get; init; }
    public string? EncryptedContentWarning { get; init; }
    public bool ConfigUpdated { get; init; }
    public ModelSyncOutcome ModelSync { get; init; } = ModelSyncOutcome.NotApplicable();
    public BackupPruneResult? AutoPruneResult { get; init; }
    public string? AutoPruneWarning { get; init; }
}

public sealed class ModelSyncOutcome
{
    public required bool Applied { get; init; }
    public string Source { get; init; } = "none";
    public string? Model { get; init; }
    public string? Warning { get; init; }

    public static ModelSyncOutcome CreateApplied(string source, string model) => new()
    {
        Applied = true,
        Source = source,
        Model = model
    };

    public static ModelSyncOutcome CreateSkipped(string source, string? warning) => new()
    {
        Applied = false,
        Source = source,
        Warning = warning
    };

    public static ModelSyncOutcome NotApplicable() => new()
    {
        Applied = false,
        Source = "not-applicable"
    };
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
    public string UiLanguage { get; init; } = "en";
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
    public long? OriginalLastWriteTimeUtcTicks { get; init; }
}

public sealed class WorkspaceRootSyncResult
{
    public required bool Present { get; init; }
    public required bool Updated { get; init; }
    public required int UpdatedWorkspaceRoots { get; init; }
    public required int SavedWorkspaceRootCount { get; init; }
}

public sealed class ThreadCwdStat
{
    public required string Cwd { get; init; }
    public required string NormalizedCwd { get; init; }
    public required long Count { get; init; }
    public required long UpdatedAtMs { get; init; }
}
