using CodexProviderSync.Core;

namespace CodexProviderSync.Application;

public interface IApplicationService
{
    Task<ApplicationOutcome<ApplicationDescription>> DescribeAsync(
        CancellationToken cancellationToken = default);

    Task<ApplicationOutcome<StatusSnapshot>> GetStatusAsync(
        ApplicationStatusRequest request,
        CancellationToken cancellationToken = default);

    Task<ApplicationOutcome<ApplicationOperationPlan>> CreatePlanAsync(
        CreateApplicationPlanRequest request,
        CancellationToken cancellationToken = default);

    Task<ApplicationOutcome<ApplicationWriteResult<SyncResult>>> SyncAsync(
        SyncApplicationRequest request,
        CancellationToken cancellationToken = default);

    Task<ApplicationOutcome<ApplicationWriteResult<SyncResult>>> SwitchAsync(
        SwitchApplicationRequest request,
        CancellationToken cancellationToken = default);

    Task<ApplicationOutcome<ApplicationWriteResult<RestoreResult>>> RestoreAsync(
        RestoreApplicationRequest request,
        CancellationToken cancellationToken = default);

    Task<ApplicationOutcome<ApplicationWriteResult<BackupPruneResult>>> PruneAsync(
        PruneApplicationRequest request,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Pure read boundary. Implementations must not persist GUI settings or mutate
/// any Codex state while serving <see cref="GetStatusAsync"/>.
/// </summary>
public interface IApplicationStatusPort
{
    Task<StatusSnapshot> GetStatusAsync(
        ApplicationStatusRequest request,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Authoritative Core write boundary. A plan preview must be produced from
/// the same Core discovery rules used by execution. Every Execute method must
/// acquire the Core operation lock, revalidate the normalized intent and state
/// fingerprint while holding that lock, and reject drift before mutation.
/// </summary>
public interface IApplicationWritePort
{
    /// <summary>
    /// Pure, deterministic input normalization used on both plan and apply.
    /// Implementations must not perform I/O or derive values from mutable
    /// storage; discovered values belong in the Core snapshot instead.
    /// </summary>
    ApplicationWriteIntent NormalizeIntent(ApplicationWriteIntent intent) => intent;

    Task<ApplicationPlanPreview> CreatePlanAsync(
        ApplicationWriteIntent intent,
        string operationId,
        CancellationToken cancellationToken = default);

    Task<SyncResult> ExecuteSyncAsync(
        SyncIntent intent,
        ApplicationOperationPlan plan,
        string operationId,
        CancellationToken cancellationToken = default);

    Task<SyncResult> ExecuteSwitchAsync(
        SwitchIntent intent,
        ApplicationOperationPlan plan,
        string operationId,
        CancellationToken cancellationToken = default);

    Task<RestoreResult> ExecuteRestoreAsync(
        RestoreIntent intent,
        ApplicationOperationPlan plan,
        string operationId,
        CancellationToken cancellationToken = default);

    Task<BackupPruneResult> ExecutePruneAsync(
        PruneIntent intent,
        ApplicationOperationPlan plan,
        string operationId,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Single-use plan ledger. Business Automation must supply a durable
/// implementation so plan state survives its one-shot process boundary. A
/// claim must atomically persist the exact registered digest before Core can
/// mutate any target; a claimed plan may never become available again.
/// </summary>
public interface IApplicationPlanLedger
{
    Task RegisterAsync(
        ApplicationOperationPlan plan,
        CancellationToken cancellationToken = default);

    Task<ApplicationPlanClaimResult> TryClaimAsync(
        string planId,
        string digest,
        CancellationToken cancellationToken = default);

    Task CompleteAsync(
        string planId,
        ApplicationOperationLifecycle lifecycle,
        CancellationToken cancellationToken = default);
}
