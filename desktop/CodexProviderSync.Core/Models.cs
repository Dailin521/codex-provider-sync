using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

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
    public string SqliteHome { get; init; } = string.Empty;
    public string SqliteHomeSource { get; init; } = "default";
    public SqliteAccessInfo SqliteAccess { get; init; } = SqliteAccessInfo.Direct;
    public IReadOnlyList<string> CheckedStateDbPaths { get; init; } = [];
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
    public IReadOnlyList<TransactionRecoveryInfo> PendingTransactions { get; init; } = [];
    [JsonIgnore]
    public StatusPerformanceMetrics PerformanceMetrics { get; init; } = new();
}

public sealed class StatusPerformanceMetrics
{
    public long TotalDurationMs { get; init; }
    public long RolloutScanDurationMs { get; init; }
    public long BackupSummaryDurationMs { get; init; }
    public SessionScanMetrics RolloutScan { get; init; } = new();
}

public sealed record TransactionRecoveryInfo(
    string? OperationId,
    string State,
    string BackupDirectory,
    string JournalPath);

public sealed record StateDbLocation(string Path, string RelativePath, string Source);

public sealed record SqliteAccessInfo(bool Supported, string? Reason, string? Message)
{
    public static SqliteAccessInfo Direct { get; } = new(true, null, null);
}

public sealed record CodexStorageLayout
{
    public required string CodexHome { get; init; }
    public required string SqliteHome { get; init; }
    public string SqliteHomeSource { get; init; } = "default";
    public required bool AllowLegacyRootFallback { get; init; }
    public required IReadOnlyList<StateDbLocation> StateDbCandidates { get; init; }
    public StateDbLocation? StateDbLocation { get; init; }
    public SqliteAccessInfo SqliteAccess { get; init; } = SqliteAccessInfo.Direct;

    public bool HasConfiguredSqliteHome => !string.Equals(SqliteHomeSource, "default", StringComparison.Ordinal);

    public void EnsureSqliteAccessSupported(string operation)
    {
        if (!SqliteAccess.Supported)
        {
            throw new InvalidOperationException($"Cannot {operation}: {SqliteAccess.Message}");
        }
    }
}

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
    public required string UpdatedFirstLine { get; init; }
    public bool ModelOnlyChange { get; init; }
    public IReadOnlyList<TurnContextModelBackup> OriginalTurnContextModels { get; set; } = [];
    [JsonIgnore]
    public string? ContentFingerprint { get; init; }
}

public sealed class TurnContextModelBackup
{
    public required int LineIndex { get; init; }
    public required string OriginalModel { get; init; }
    public IReadOnlyList<string> OriginalModels { get; init; } = [];
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
    public SessionScanMetrics ScanMetrics { get; init; } = new();
}

public sealed class SessionScanMetrics
{
    public int EnumeratedRolloutFiles { get; init; }
    public int ParsedSessionFiles { get; init; }
    public int ContentScanPasses { get; init; }
    public int ModelScanFiles { get; init; }
    public long DurationMs { get; init; }
}

public sealed class SyncResult
{
    public required string CodexHome { get; init; }
    public string SqliteHome { get; init; } = string.Empty;
    public string SqliteHomeSource { get; init; } = "default";
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
    [JsonIgnore]
    public SyncPerformanceMetrics PerformanceMetrics { get; init; } = new();
}

public sealed class SyncPerformanceMetrics
{
    public long TotalDurationMs { get; init; }
    public long PreparationDurationMs { get; init; }
    public long CheckedPlanValidationDurationMs { get; init; }
    public long BackupDurationMs { get; init; }
    public long MutationDurationMs { get; init; }
    public long PruneDurationMs { get; init; }
    public int JournalFullValidationCount { get; init; }
    public SessionScanMetrics RolloutScan { get; init; } = new();
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

public sealed class BackupStorageInfo
{
    public required int Version { get; init; }
    public string? SqliteHome { get; init; }
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
    public Dictionary<string, string> SqliteHomeOverrides { get; init; } = new(
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
    public List<string> SavedProviders { get; init; } = [];
    public List<string> ManualProviders { get; init; } = [];
    public string? LastSelectedProvider { get; init; }
    public string? LastBackupDirectory { get; init; }
    public int BackupRetentionCount { get; init; } = AppConstants.DefaultBackupRetentionCount;
    public string UiLanguage { get; init; } = "en";
    public DateOnly? LastAutomaticUpdateCheckDate { get; init; }
    public WindowBoundsState? WindowBounds { get; init; }
}

public sealed class RestoreBackupOptions
{
    public bool RestoreConfig { get; init; } = true;
    public bool RestoreDatabase { get; init; } = true;
    public bool RestoreSessions { get; init; } = true;
    public bool AllowSqliteHomeRelocation { get; init; }
}

internal sealed class BackupMetadataFile
{
    public int Version { get; init; }
    public required string Namespace { get; init; }
    public required string CodexHome { get; init; }
    public string? SqliteHome { get; init; }
    public required string TargetProvider { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
    public required List<string> DbFiles { get; init; }
    public List<string> SqliteDbFiles { get; init; } = [];
    public int ChangedSessionFiles { get; init; }
    public Dictionary<string, bool>? GlobalStateFiles { get; init; }
    public bool? GlobalStateFilePresent { get; init; }
    public bool? GlobalStateBackupFilePresent { get; init; }
    public long? SizeBytes { get; init; }
    public int? FileCount { get; init; }
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
    public string? OriginalLastWriteTimeUtc { get; init; }
    public double? OriginalMtimeMs { get; init; }
    [JsonConverter(typeof(NullableInt64DecimalStringJsonConverter))]
    public long? OriginalLastWriteTimeUtcTicks { get; init; }
    public bool ModelOnlyChange { get; init; }
    public List<TurnContextModelBackup> OriginalTurnContextModels { get; init; } = [];

    internal long? ResolveOriginalLastWriteTimeUtcTicks()
    {
        long? isoTicks = null;
        if (!string.IsNullOrWhiteSpace(OriginalLastWriteTimeUtc))
        {
            if (!DateTimeOffset.TryParseExact(
                    OriginalLastWriteTimeUtc,
                    "yyyy-MM-dd'T'HH:mm:ss.fff'Z'",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                    out DateTimeOffset parsed))
            {
                throw new InvalidOperationException(
                    $"Session backup has invalid originalLastWriteTimeUtc for {Path}.");
            }
            isoTicks = parsed.UtcTicks;
        }

        long? mtimeTicks = null;
        if (OriginalMtimeMs is double mtimeMs)
        {
            if (!double.IsFinite(mtimeMs))
            {
                throw new InvalidOperationException(
                    $"Session backup has non-finite originalMtimeMs for {Path}.");
            }
            double truncated = Math.Truncate(mtimeMs);
            if (truncated < DateTimeOffset.MinValue.ToUnixTimeMilliseconds()
                || truncated > DateTimeOffset.MaxValue.ToUnixTimeMilliseconds())
            {
                throw new InvalidOperationException(
                    $"Session backup originalMtimeMs is out of range for {Path}.");
            }
            mtimeTicks = DateTimeOffset.FromUnixTimeMilliseconds(checked((long)truncated)).UtcTicks;
        }

        long? ticksAtMillisecond = OriginalLastWriteTimeUtcTicks is long ticks
            ? ticks - (ticks % TimeSpan.TicksPerMillisecond)
            : null;
        long? expected = isoTicks ?? mtimeTicks ?? ticksAtMillisecond;
        if ((isoTicks is not null && isoTicks != expected)
            || (mtimeTicks is not null && mtimeTicks != expected)
            || (ticksAtMillisecond is not null && ticksAtMillisecond != expected))
        {
            throw new InvalidOperationException(
                $"Session backup timestamp fields disagree for {Path}.");
        }
        return OriginalLastWriteTimeUtcTicks ?? expected;
    }

    internal static SessionBackupManifestEntry FromChange(SessionChange change)
    {
        DateTimeOffset original = new(
            new DateTime(change.OriginalLastWriteTimeUtcTicks, DateTimeKind.Utc));
        long unixMilliseconds = original.ToUnixTimeMilliseconds();
        return new SessionBackupManifestEntry
        {
            Path = change.Path,
            OriginalFirstLine = change.OriginalFirstLine,
            OriginalSeparator = change.OriginalSeparator,
            OriginalLastWriteTimeUtc = original.ToString(
                "yyyy-MM-dd'T'HH:mm:ss.fff'Z'",
                CultureInfo.InvariantCulture),
            OriginalMtimeMs = unixMilliseconds,
            OriginalLastWriteTimeUtcTicks = change.OriginalLastWriteTimeUtcTicks,
            ModelOnlyChange = change.ModelOnlyChange,
            OriginalTurnContextModels = [.. change.OriginalTurnContextModels]
        };
    }
}

internal sealed class NullableInt64DecimalStringJsonConverter : JsonConverter<long?>
{
    public override long? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            return null;
        }
        if (reader.TokenType == JsonTokenType.String
            && long.TryParse(reader.GetString(), NumberStyles.None, CultureInfo.InvariantCulture, out long textValue))
        {
            return textValue;
        }
        if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out long numericValue))
        {
            return numericValue;
        }
        throw new JsonException("Expected a decimal string or Int64 JSON number.");
    }

    public override void Write(Utf8JsonWriter writer, long? value, JsonSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNullValue();
            return;
        }
        writer.WriteStringValue(value.Value.ToString(CultureInfo.InvariantCulture));
    }
}

public sealed class WorkspaceRootSyncResult
{
    public required bool Present { get; init; }
    public required bool Updated { get; init; }
    public required int UpdatedWorkspaceRoots { get; init; }
    public required int SavedWorkspaceRootCount { get; init; }
}

public sealed class SyncTransactionException : InvalidOperationException
{
    public SyncTransactionException(
        Exception originalError,
        IReadOnlyList<string> rollbackErrors,
        string backupDirectory,
        IReadOnlyList<string> completedTargets,
        IReadOnlyList<string> uncompletedTargets,
        string rollbackStatus = "incomplete",
        bool recoveryRequired = true)
        : base(
            recoveryRequired
                ? $"Failed to restore state after sync error. Original error: {originalError.Message}. Restore error: {string.Join("; ", rollbackErrors)}"
                : $"Provider sync failed and all observed changes were rolled back. Original error: {originalError.Message}",
            originalError)
    {
        OriginalError = originalError;
        RollbackErrors = rollbackErrors;
        BackupDirectory = backupDirectory;
        CompletedTargets = completedTargets;
        UncompletedTargets = uncompletedTargets;
        RollbackStatus = rollbackStatus;
        RecoveryRequired = recoveryRequired;
    }

    public string Code => RecoveryRequired ? "RECOVERY_REQUIRED" : "SYNC_FAILED_ROLLED_BACK";
    public Exception OriginalError { get; }
    public IReadOnlyList<string> RollbackErrors { get; }
    public string BackupDirectory { get; }
    public IReadOnlyList<string> CompletedTargets { get; }
    public IReadOnlyList<string> UncompletedTargets { get; }
    public string RollbackStatus { get; }
    public bool RecoveryRequired { get; }
    public bool WasCanceled => OriginalError is OperationCanceledException;
    public string RecoveryInstructions =>
        RecoveryRequired
            ? $"Restore the managed backup at {BackupDirectory}, inspect the pending transaction journal, then retry."
            : "No manual recovery is required. Inspect the original error, correct its cause, and retry.";
}

public sealed class ThreadCwdStat
{
    public required string Cwd { get; init; }
    public required string NormalizedCwd { get; init; }
    public required long Count { get; init; }
    public required long UpdatedAtMs { get; init; }
}
