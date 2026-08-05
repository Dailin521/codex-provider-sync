using CodexProviderSync.Core;
using System.Text.Json.Serialization;

namespace CodexProviderSync.Application;

public static class ApplicationProtocol
{
    public const string Version = "0.4";
}

public enum ApplicationOperationKind
{
    Describe,
    Status,
    Plan,
    Sync,
    Switch,
    Restore,
    Prune
}

public enum ApplicationWriteKind
{
    Sync,
    Switch,
    Restore,
    Prune
}

public enum ApplicationOperationLifecycle
{
    Accepted,
    Validating,
    Planning,
    ReadyToApply,
    Applying,
    Succeeded,
    Failed,
    Cancelled,
    Rejected,
    RecoveryRequired
}

public sealed record ApplicationLifecycleEvent(
    ApplicationOperationLifecycle Lifecycle,
    DateTimeOffset TimestampUtc);

public sealed record ApplicationWarning(string Code, string Message);

public sealed record ApplicationError(
    string Code,
    string Message,
    bool RecoveryRequired = false,
    string? RollbackStatus = null,
    string? EvidencePath = null);

public sealed record ApplicationOutcome<T>(
    string OperationId,
    ApplicationOperationKind Operation,
    ApplicationOperationLifecycle Lifecycle,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset CompletedAtUtc,
    T? Data,
    IReadOnlyList<ApplicationWarning> Warnings,
    IReadOnlyList<ApplicationError> Errors,
    IReadOnlyList<ApplicationLifecycleEvent> Timeline)
    where T : class
{
    public bool IsSuccess => Lifecycle is
        ApplicationOperationLifecycle.Succeeded or
        ApplicationOperationLifecycle.ReadyToApply;
}

public sealed record ApplicationDescription(
    string ProtocolVersion,
    IReadOnlyList<string> Commands,
    bool WritesDefaultToDryRun,
    bool ExplicitApplyRequired,
    bool ExactPlanDigestRequired,
    bool PlansAreSingleUse);

public sealed record ApplicationStatusRequest(
    string CodexHome,
    string? SqliteHomeOverride = null);

[JsonPolymorphic(TypeDiscriminatorPropertyName = "kind")]
[JsonDerivedType(typeof(SyncIntent), "sync")]
[JsonDerivedType(typeof(SwitchIntent), "switch")]
[JsonDerivedType(typeof(RestoreIntent), "restore")]
[JsonDerivedType(typeof(PruneIntent), "prune")]
public abstract record ApplicationWriteIntent(
    string CodexHome,
    string? SqliteHomeOverride)
{
    [JsonIgnore]
    public abstract ApplicationWriteKind Kind { get; }
}

public sealed record SyncIntent(
    string CodexHome,
    string? SqliteHomeOverride,
    string ProviderId,
    int BackupRetentionCount = AppConstants.DefaultBackupRetentionCount)
    : ApplicationWriteIntent(CodexHome, SqliteHomeOverride)
{
    [JsonIgnore]
    public override ApplicationWriteKind Kind => ApplicationWriteKind.Sync;
}

public sealed record SwitchIntent(
    string CodexHome,
    string? SqliteHomeOverride,
    string ProviderId,
    SwitchModelSelection ModelSelection,
    int BackupRetentionCount = AppConstants.DefaultBackupRetentionCount)
    : ApplicationWriteIntent(CodexHome, SqliteHomeOverride)
{
    [JsonIgnore]
    public override ApplicationWriteKind Kind => ApplicationWriteKind.Switch;
}

public sealed record RestoreIntent(
    string CodexHome,
    string? SqliteHomeOverride,
    string BackupDirectory,
    bool RestoreConfig = true,
    bool RestoreDatabase = true,
    bool RestoreSessions = true,
    bool AllowSqliteHomeRelocation = false)
    : ApplicationWriteIntent(CodexHome, SqliteHomeOverride)
{
    [JsonIgnore]
    public override ApplicationWriteKind Kind => ApplicationWriteKind.Restore;
}

public sealed record PruneIntent(
    string CodexHome,
    string? SqliteHomeOverride,
    int BackupRetentionCount = AppConstants.DefaultBackupRetentionCount)
    : ApplicationWriteIntent(CodexHome, SqliteHomeOverride)
{
    [JsonIgnore]
    public override ApplicationWriteKind Kind => ApplicationWriteKind.Prune;
}

public sealed record CreateApplicationPlanRequest(ApplicationWriteIntent Intent);

public sealed record ApplicationPlanTarget(
    string Path,
    string Action,
    string Fingerprint);

public sealed record ApplicationPlanPreview(
    ApplicationWriteIntent NormalizedIntent,
    string StateFingerprint,
    string ExecutionToken,
    IReadOnlyList<ApplicationPlanTarget> Targets,
    IReadOnlyList<ApplicationPlanTarget>? AutoPruneDeletionTargets = null,
    IReadOnlyList<ApplicationWarning>? Warnings = null);

public sealed record ApplicationOperationPlan(
    string ProtocolVersion,
    string PlanId,
    string CreatedByOperationId,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset ExpiresAtUtc,
    ApplicationWriteIntent Intent,
    string StateFingerprint,
    string ExecutionToken,
    IReadOnlyList<ApplicationPlanTarget> Targets,
    IReadOnlyList<ApplicationPlanTarget> AutoPruneDeletionTargets,
    IReadOnlyList<ApplicationWarning> Warnings,
    string Digest);

public sealed record ApplicationApplyAuthorization(
    bool Apply = false,
    ApplicationOperationPlan? Plan = null,
    string? PlanDigest = null)
{
    public static ApplicationApplyAuthorization DryRun { get; } = new();
}

public sealed record SyncApplicationRequest(
    SyncIntent Intent,
    ApplicationApplyAuthorization? Authorization = null);

public sealed record SwitchApplicationRequest(
    SwitchIntent Intent,
    ApplicationApplyAuthorization? Authorization = null);

public sealed record RestoreApplicationRequest(
    RestoreIntent Intent,
    ApplicationApplyAuthorization? Authorization = null);

public sealed record PruneApplicationRequest(
    PruneIntent Intent,
    ApplicationApplyAuthorization? Authorization = null);

public sealed record ApplicationWriteResult<T>(
    ApplicationOperationPlan Plan,
    bool Applied,
    T? Result)
    where T : class;

public enum ApplicationPlanClaimStatus
{
    Claimed,
    NotFound,
    DigestMismatch,
    AlreadyUsed
}

public sealed record ApplicationPlanClaimResult(ApplicationPlanClaimStatus Status);

public sealed class ApplicationPortException : InvalidOperationException
{
    public ApplicationPortException(
        string code,
        string message,
        bool recoveryRequired = false,
        string? rollbackStatus = null,
        Exception? innerException = null)
        : base(message, innerException)
    {
        if (string.IsNullOrWhiteSpace(code))
        {
            throw new ArgumentException("An error code is required.", nameof(code));
        }

        Code = code;
        RecoveryRequired = recoveryRequired;
        RollbackStatus = rollbackStatus;
    }

    public string Code { get; }

    public bool RecoveryRequired { get; }

    public string? RollbackStatus { get; }
}
