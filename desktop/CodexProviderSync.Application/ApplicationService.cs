using System.Security.Cryptography;
using System.Text;
using CodexProviderSync.Core;

namespace CodexProviderSync.Application;

public sealed class ApplicationService : IApplicationService
{
    private const string PlanLedgerCorruptionMessage =
        "Durable plan-ledger evidence is corrupt. No further operation is authorized; inspect the retained evidence before retrying.";

    private static readonly IReadOnlyList<string> SupportedCommands = Array.AsReadOnly(
        new[] { "describe", "status", "plan", "sync", "switch", "restore", "prune" });

    private readonly IApplicationStatusPort _statusPort;
    private readonly IApplicationWritePort _writePort;
    private readonly IApplicationPlanLedger _planLedger;
    private readonly TimeProvider _timeProvider;
    private readonly Func<string> _idFactory;
    private readonly TimeSpan _planLifetime;
    private int _operationInProgress;

    public ApplicationService(
        IApplicationStatusPort statusPort,
        IApplicationWritePort writePort,
        IApplicationPlanLedger planLedger,
        TimeProvider? timeProvider = null,
        Func<string>? idFactory = null,
        TimeSpan? planLifetime = null)
    {
        _statusPort = statusPort ?? throw new ArgumentNullException(nameof(statusPort));
        _writePort = writePort ?? throw new ArgumentNullException(nameof(writePort));
        _planLedger = planLedger ?? throw new ArgumentNullException(nameof(planLedger));
        _timeProvider = timeProvider ?? TimeProvider.System;
        _idFactory = idFactory ?? (static () => Guid.NewGuid().ToString("N"));
        _planLifetime = planLifetime ?? TimeSpan.FromMinutes(10);
        if (_planLifetime <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(planLifetime), "Plan lifetime must be positive.");
        }
    }

    public Task<ApplicationOutcome<ApplicationDescription>> DescribeAsync(
        CancellationToken cancellationToken = default)
    {
        return RunExclusiveAsync(
            ApplicationOperationKind.Describe,
            (context, token) =>
            {
                token.ThrowIfCancellationRequested();
                ApplicationDescription description = new(
                    ApplicationProtocol.Version,
                    SupportedCommands,
                    WritesDefaultToDryRun: true,
                    ExplicitApplyRequired: true,
                    ExactPlanDigestRequired: true,
                    PlansAreSingleUse: true);
                return Task.FromResult(OperationResult<ApplicationDescription>.Succeeded(description));
            },
            cancellationToken);
    }

    public Task<ApplicationOutcome<StatusSnapshot>> GetStatusAsync(
        ApplicationStatusRequest request,
        CancellationToken cancellationToken = default)
    {
        return RunExclusiveAsync(
            ApplicationOperationKind.Status,
            async (context, token) =>
            {
                context.MoveTo(ApplicationOperationLifecycle.Validating);
                ValidateStatusRequest(request);
                StatusSnapshot status = await _statusPort.GetStatusAsync(
                    Freeze(request),
                    token);
                return OperationResult<StatusSnapshot>.Succeeded(status);
            },
            cancellationToken,
            useExclusiveGate: false);
    }

    public Task<ApplicationOutcome<ApplicationOperationPlan>> CreatePlanAsync(
        CreateApplicationPlanRequest request,
        CancellationToken cancellationToken = default)
    {
        return RunExclusiveAsync(
            ApplicationOperationKind.Plan,
            async (context, token) =>
            {
                if (request is null)
                {
                    throw new ApplicationRequestException("request_required", "A plan request is required.");
                }

                if (request.Intent is null)
                {
                    throw new ApplicationRequestException("intent_required", "A write intent is required.");
                }

                ApplicationOperationPlan plan = await CreatePlanCoreAsync(
                    Freeze(request.Intent),
                    context,
                    token);
                return OperationResult<ApplicationOperationPlan>.ReadyToApply(plan, plan.Warnings);
            },
            cancellationToken);
    }

    public Task<ApplicationOutcome<ApplicationWriteResult<SyncResult>>> SyncAsync(
        SyncApplicationRequest request,
        CancellationToken cancellationToken = default)
    {
        return RunWriteAsync<SyncIntent, SyncResult>(
            ApplicationOperationKind.Sync,
            request?.Intent,
            request?.Authorization,
            (intent, plan, operationId, token) => _writePort.ExecuteSyncAsync(
                intent,
                plan,
                operationId,
                token),
            cancellationToken);
    }

    public Task<ApplicationOutcome<ApplicationWriteResult<SyncResult>>> SwitchAsync(
        SwitchApplicationRequest request,
        CancellationToken cancellationToken = default)
    {
        return RunWriteAsync<SwitchIntent, SyncResult>(
            ApplicationOperationKind.Switch,
            request?.Intent,
            request?.Authorization,
            (intent, plan, operationId, token) => _writePort.ExecuteSwitchAsync(
                intent,
                plan,
                operationId,
                token),
            cancellationToken);
    }

    public Task<ApplicationOutcome<ApplicationWriteResult<RestoreResult>>> RestoreAsync(
        RestoreApplicationRequest request,
        CancellationToken cancellationToken = default)
    {
        return RunWriteAsync<RestoreIntent, RestoreResult>(
            ApplicationOperationKind.Restore,
            request?.Intent,
            request?.Authorization,
            (intent, plan, operationId, token) => _writePort.ExecuteRestoreAsync(
                intent,
                plan,
                operationId,
                token),
            cancellationToken);
    }

    public Task<ApplicationOutcome<ApplicationWriteResult<BackupPruneResult>>> PruneAsync(
        PruneApplicationRequest request,
        CancellationToken cancellationToken = default)
    {
        return RunWriteAsync<PruneIntent, BackupPruneResult>(
            ApplicationOperationKind.Prune,
            request?.Intent,
            request?.Authorization,
            (intent, plan, operationId, token) => _writePort.ExecutePruneAsync(
                intent,
                plan,
                operationId,
                token),
            cancellationToken);
    }

    private Task<ApplicationOutcome<ApplicationWriteResult<TResult>>> RunWriteAsync<TIntent, TResult>(
        ApplicationOperationKind operation,
        TIntent? intent,
        ApplicationApplyAuthorization? authorization,
        Func<TIntent, ApplicationOperationPlan, string, CancellationToken, Task<TResult>> execute,
        CancellationToken cancellationToken)
        where TIntent : ApplicationWriteIntent
        where TResult : class
    {
        return RunExclusiveAsync(
            operation,
            async (context, token) =>
            {
                context.MoveTo(ApplicationOperationLifecycle.Validating);
                if (intent is null)
                {
                    throw new ApplicationRequestException("request_required", "A write request is required.");
                }

                TIntent requestIntent = (TIntent)Freeze(intent);
                ValidateIntent(requestIntent);
                ApplicationApplyAuthorization apply = Freeze(authorization)
                    ?? ApplicationApplyAuthorization.DryRun;
                if (!apply.Apply)
                {
                    if (apply.Plan is not null || apply.PlanDigest is not null)
                    {
                        throw new ApplicationRequestException(
                            "apply_required",
                            "A supplied plan is only accepted when apply is explicitly enabled.");
                    }

                    ApplicationOperationPlan dryRunPlan = await CreatePlanCoreAsync(requestIntent, context, token);
                    ApplicationWriteResult<TResult> dryRun = new(dryRunPlan, Applied: false, Result: null);
                    return OperationResult<ApplicationWriteResult<TResult>>.ReadyToApply(
                        dryRun,
                        dryRunPlan.Warnings);
                }

                TIntent frozenIntent = NormalizeIntent(requestIntent);
                ApplicationOperationPlan plan = ValidateApplyAuthorization(frozenIntent, apply);
                if (_timeProvider.GetUtcNow() >= plan.ExpiresAtUtc)
                {
                    throw new ApplicationRequestException("plan_expired", "The supplied plan has expired.");
                }

                ApplicationPlanClaimResult claim = await _planLedger.TryClaimAsync(
                    plan.PlanId,
                    plan.Digest,
                    token);
                if (claim.Status != ApplicationPlanClaimStatus.Claimed)
                {
                    throw new ApplicationRequestException(
                        claim.Status switch
                        {
                            ApplicationPlanClaimStatus.NotFound => "plan_not_registered",
                            ApplicationPlanClaimStatus.DigestMismatch => "plan_digest_mismatch",
                            ApplicationPlanClaimStatus.AlreadyUsed => "plan_already_used",
                            _ => "plan_invalid"
                        },
                        "The supplied plan is unavailable, does not match, or has already been used.");
                }

                context.MoveTo(ApplicationOperationLifecycle.Applying);
                try
                {
                    TResult result = await execute(frozenIntent, plan, context.OperationId, token);
                    ApplicationWarning? ledgerWarning = await TryCompletePlanAsync(
                        plan.PlanId,
                        ApplicationOperationLifecycle.Succeeded);
                    IReadOnlyList<ApplicationWarning> warnings = ledgerWarning is null
                        ? plan.Warnings
                        : Array.AsReadOnly(plan.Warnings.Concat([ledgerWarning]).ToArray());
                    return OperationResult<ApplicationWriteResult<TResult>>.Succeeded(
                        new ApplicationWriteResult<TResult>(plan, Applied: true, result),
                        warnings);
                }
                catch (ApplicationPlanLedgerCorruptionException)
                {
                    // Further ledger transitions cannot be trusted after
                    // corruption is observed. Preserve the original evidence
                    // and let the outer boundary return a recovery outcome.
                    throw;
                }
                catch (OperationCanceledException)
                {
                    await TryCompletePlanAsync(plan.PlanId, ApplicationOperationLifecycle.Cancelled);
                    throw;
                }
                catch (SyncTransactionException error) when (error.WasCanceled && !error.RecoveryRequired)
                {
                    await TryCompletePlanAsync(plan.PlanId, ApplicationOperationLifecycle.Cancelled);
                    throw;
                }
                catch (ApplicationPortException error) when (
                    !error.RecoveryRequired
                    && (error.Code is "target_busy" or "lock_unverifiable"
                        || error.Code.StartsWith("plan_", StringComparison.Ordinal)))
                {
                    await TryCompletePlanAsync(plan.PlanId, ApplicationOperationLifecycle.Rejected);
                    throw;
                }
                catch (Exception error)
                {
                    ApplicationOperationLifecycle terminal = IsRecoveryRequired(error)
                        ? ApplicationOperationLifecycle.RecoveryRequired
                        : ApplicationOperationLifecycle.Failed;
                    await TryCompletePlanAsync(plan.PlanId, terminal);
                    throw;
                }
            },
            cancellationToken);
    }

    private async Task<ApplicationOperationPlan> CreatePlanCoreAsync(
        ApplicationWriteIntent intent,
        OperationContext context,
        CancellationToken cancellationToken)
    {
        context.MoveTo(ApplicationOperationLifecycle.Validating);
        intent = NormalizeIntent(intent);
        ValidateIntent(intent);
        context.MoveTo(ApplicationOperationLifecycle.Planning);
        ApplicationPlanPreview preview = await _writePort.CreatePlanAsync(
            intent,
            context.OperationId,
            cancellationToken);
        ValidatePreview(intent.Kind, preview);

        DateTimeOffset createdAt = _timeProvider.GetUtcNow();
        IReadOnlyList<ApplicationPlanTarget> targets = preview.Targets
            .Select(static target => target with { })
            .OrderBy(static target => target.Path, StringComparer.Ordinal)
            .ThenBy(static target => target.Action, StringComparer.Ordinal)
            .ThenBy(static target => target.Fingerprint, StringComparer.Ordinal)
            .ToList()
            .AsReadOnly();
        IReadOnlyList<ApplicationWarning> warnings = (preview.Warnings ?? [])
            .Select(static warning => warning with { })
            .ToList()
            .AsReadOnly();
        IReadOnlyList<ApplicationPlanTarget> autoPruneTargets = (preview.AutoPruneDeletionTargets ?? [])
            .Select(static target => target with { })
            .OrderBy(static target => target.Path, StringComparer.Ordinal)
            .ThenBy(static target => target.Action, StringComparer.Ordinal)
            .ThenBy(static target => target.Fingerprint, StringComparer.Ordinal)
            .ToList()
            .AsReadOnly();
        ApplicationOperationPlan unsigned = new(
            ApplicationProtocol.Version,
            NewId(),
            context.OperationId,
            createdAt,
            createdAt.Add(_planLifetime),
            Freeze(preview.NormalizedIntent),
            preview.StateFingerprint,
            preview.ExecutionToken,
            targets,
            autoPruneTargets,
            warnings,
            Digest: string.Empty);
        ApplicationOperationPlan plan = unsigned with { Digest = ComputeDigest(unsigned) };
        await _planLedger.RegisterAsync(plan, cancellationToken);
        context.MoveTo(ApplicationOperationLifecycle.ReadyToApply);
        return plan;
    }

    private TIntent NormalizeIntent<TIntent>(TIntent intent)
        where TIntent : ApplicationWriteIntent
    {
        ApplicationWriteIntent normalized = _writePort.NormalizeIntent(Freeze(intent));
        if (normalized is not TIntent typed || normalized.Kind != intent.Kind)
        {
            throw new ApplicationPortException(
                "invalid_normalized_intent",
                "Core returned a mismatched normalized write intent.");
        }
        return (TIntent)Freeze(typed);
    }

    private ApplicationWriteIntent NormalizeIntent(ApplicationWriteIntent intent)
    {
        ApplicationWriteIntent normalized = _writePort.NormalizeIntent(Freeze(intent));
        if (normalized is null || normalized.Kind != intent.Kind)
        {
            throw new ApplicationPortException(
                "invalid_normalized_intent",
                "Core returned a mismatched normalized write intent.");
        }
        return Freeze(normalized);
    }

    private ApplicationOperationPlan ValidateApplyAuthorization(
        ApplicationWriteIntent intent,
        ApplicationApplyAuthorization authorization)
    {
        if (authorization.Plan is null || string.IsNullOrWhiteSpace(authorization.PlanDigest))
        {
            throw new ApplicationRequestException(
                "plan_required",
                "Explicit apply requires both the plan document and its exact digest.");
        }

        ApplicationOperationPlan plan = authorization.Plan;
        if (!string.Equals(plan.ProtocolVersion, ApplicationProtocol.Version, StringComparison.Ordinal))
        {
            throw new ApplicationRequestException("plan_version_mismatch", "The plan protocol version is not supported.");
        }
        if (plan.Intent != intent || plan.Intent.Kind != intent.Kind)
        {
            throw new ApplicationRequestException("plan_input_mismatch", "The supplied plan does not match the request inputs.");
        }

        string computedDigest = ComputeDigest(plan);
        if (!FixedTimeEquals(plan.Digest, computedDigest)
            || !FixedTimeEquals(authorization.PlanDigest, plan.Digest))
        {
            throw new ApplicationRequestException("plan_digest_mismatch", "The supplied plan digest is invalid.");
        }

        return plan;
    }

    private async Task<ApplicationWarning?> TryCompletePlanAsync(
        string planId,
        ApplicationOperationLifecycle lifecycle)
    {
        try
        {
            // Claiming makes the plan non-reusable. Completion is audit state
            // and must still be attempted after caller cancellation.
            await _planLedger.CompleteAsync(planId, lifecycle, CancellationToken.None);
            return null;
        }
        catch (ApplicationPlanLedgerCorruptionException)
        {
            // Corruption is not an optional audit-write failure. The plan's
            // single-use state can no longer be trusted, so surface it through
            // the fail-closed structured outcome below.
            throw;
        }
        catch (Exception error)
        {
            return new ApplicationWarning(
                "plan_ledger_completion_failed",
                $"The plan remains consumed, but its terminal audit state could not be recorded: {error.Message}");
        }
    }

    private async Task<ApplicationOutcome<T>> RunExclusiveAsync<T>(
        ApplicationOperationKind operation,
        Func<OperationContext, CancellationToken, Task<OperationResult<T>>> run,
        CancellationToken cancellationToken,
        bool useExclusiveGate = true)
        where T : class
    {
        string operationId = NewId();
        DateTimeOffset startedAt = _timeProvider.GetUtcNow();
        OperationContext context = new(operationId, startedAt, _timeProvider);
        bool gateAcquired = false;
        if (useExclusiveGate
            && Interlocked.CompareExchange(ref _operationInProgress, 1, 0) != 0)
        {
            context.MoveTo(ApplicationOperationLifecycle.Rejected);
            return BuildOutcome<T>(
                operation,
                context,
                ApplicationOperationLifecycle.Rejected,
                null,
                [],
                [new ApplicationError("operation_busy", "Another Application operation is already in progress.")]);
        }
        gateAcquired = useExclusiveGate;

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            OperationResult<T> result = await run(context, cancellationToken);
            context.MoveTo(result.Lifecycle);
            return BuildOutcome(
                operation,
                context,
                result.Lifecycle,
                result.Data,
                result.Warnings,
                []);
        }
        catch (ApplicationRequestException error)
        {
            context.MoveTo(ApplicationOperationLifecycle.Rejected);
            return BuildOutcome<T>(
                operation,
                context,
                ApplicationOperationLifecycle.Rejected,
                null,
                [],
                [new ApplicationError(error.Code, error.Message)]);
        }
        catch (ApplicationPlanLedgerCorruptionException error)
        {
            context.MoveTo(ApplicationOperationLifecycle.RecoveryRequired);
            return BuildOutcome<T>(
                operation,
                context,
                ApplicationOperationLifecycle.RecoveryRequired,
                null,
                [],
                [new ApplicationError(
                    error.Code,
                    PlanLedgerCorruptionMessage,
                    RecoveryRequired: true,
                    EvidencePath: error.EvidencePath)]);
        }
        catch (SyncTransactionException error) when (error.WasCanceled && !error.RecoveryRequired)
        {
            context.MoveTo(ApplicationOperationLifecycle.Cancelled);
            return BuildOutcome<T>(
                operation,
                context,
                ApplicationOperationLifecycle.Cancelled,
                null,
                [],
                [new ApplicationError("cancelled", error.Message, RollbackStatus: error.RollbackStatus)]);
        }
        catch (OperationCanceledException error)
        {
            context.MoveTo(ApplicationOperationLifecycle.Cancelled);
            return BuildOutcome<T>(
                operation,
                context,
                ApplicationOperationLifecycle.Cancelled,
                null,
                [],
                [new ApplicationError("cancelled", error.Message)]);
        }
        catch (SyncTransactionException error)
        {
            ApplicationOperationLifecycle lifecycle = error.RecoveryRequired
                ? ApplicationOperationLifecycle.RecoveryRequired
                : ApplicationOperationLifecycle.Failed;
            context.MoveTo(lifecycle);
            return BuildOutcome<T>(
                operation,
                context,
                lifecycle,
                null,
                [],
                [new ApplicationError(
                    error.Code.ToLowerInvariant(),
                    error.Message,
                    error.RecoveryRequired,
                    error.RollbackStatus)]);
        }
        catch (RecoveryRequiredException error)
        {
            context.MoveTo(ApplicationOperationLifecycle.RecoveryRequired);
            return BuildOutcome<T>(
                operation,
                context,
                ApplicationOperationLifecycle.RecoveryRequired,
                null,
                [],
                [new ApplicationError("recovery_required", error.Message, RecoveryRequired: true)]);
        }
        catch (ApplicationPortException error)
        {
            ApplicationOperationLifecycle lifecycle = error.RecoveryRequired
                ? ApplicationOperationLifecycle.RecoveryRequired
                : error.Code is "target_busy" or "lock_unverifiable"
                    || error.Code.StartsWith("plan_", StringComparison.Ordinal)
                        ? ApplicationOperationLifecycle.Rejected
                        : ApplicationOperationLifecycle.Failed;
            context.MoveTo(lifecycle);
            return BuildOutcome<T>(
                operation,
                context,
                lifecycle,
                null,
                [],
                [new ApplicationError(
                    error.Code,
                    error.Message,
                    error.RecoveryRequired,
                    error.RollbackStatus)]);
        }
        catch (Exception error)
        {
            context.MoveTo(ApplicationOperationLifecycle.Failed);
            return BuildOutcome<T>(
                operation,
                context,
                ApplicationOperationLifecycle.Failed,
                null,
                [],
                [new ApplicationError("operation_failed", error.Message)]);
        }
        finally
        {
            if (gateAcquired)
            {
                Volatile.Write(ref _operationInProgress, 0);
            }
        }
    }

    private ApplicationOutcome<T> BuildOutcome<T>(
        ApplicationOperationKind operation,
        OperationContext context,
        ApplicationOperationLifecycle lifecycle,
        T? data,
        IReadOnlyList<ApplicationWarning> warnings,
        IReadOnlyList<ApplicationError> errors)
        where T : class
    {
        return new ApplicationOutcome<T>(
            context.OperationId,
            operation,
            lifecycle,
            context.StartedAtUtc,
            _timeProvider.GetUtcNow(),
            data,
            warnings.Select(static warning => warning with { }).ToList().AsReadOnly(),
            errors.Select(static error => error with { }).ToList().AsReadOnly(),
            context.Timeline);
    }

    private static void ValidateStatusRequest(ApplicationStatusRequest? request)
    {
        if (request is null)
        {
            throw new ApplicationRequestException("request_required", "A status request is required.");
        }
        if (string.IsNullOrWhiteSpace(request.CodexHome))
        {
            throw new ApplicationRequestException("codex_home_required", "Codex Home is required.");
        }
    }

    private static void ValidateIntent(ApplicationWriteIntent? intent)
    {
        if (intent is null)
        {
            throw new ApplicationRequestException("intent_required", "A write intent is required.");
        }
        if (string.IsNullOrWhiteSpace(intent.CodexHome))
        {
            throw new ApplicationRequestException("codex_home_required", "Codex Home is required.");
        }

        switch (intent)
        {
            case SyncIntent sync when string.IsNullOrWhiteSpace(sync.ProviderId):
                throw new ApplicationRequestException("provider_required", "A provider id is required.");
            case SyncIntent sync when sync.BackupRetentionCount < 1:
                throw new ApplicationRequestException("retention_invalid", "Backup retention must be at least one.");
            case SwitchIntent change when string.IsNullOrWhiteSpace(change.ProviderId):
                throw new ApplicationRequestException("provider_required", "A provider id is required.");
            case SwitchIntent change when change.ModelSelection is null:
                throw new ApplicationRequestException("model_selection_required", "A switch model selection is required.");
            case SwitchIntent change when change.ModelSelection is CustomModelSelection custom
                && string.IsNullOrWhiteSpace(custom.Model):
                throw new ApplicationRequestException("custom_model_required", "A custom model is required.");
            case SwitchIntent change when change.BackupRetentionCount < 1:
                throw new ApplicationRequestException("retention_invalid", "Backup retention must be at least one.");
            case RestoreIntent restore when string.IsNullOrWhiteSpace(restore.BackupDirectory):
                throw new ApplicationRequestException("backup_required", "A backup directory is required.");
            case RestoreIntent restore when !restore.RestoreConfig
                && !restore.RestoreDatabase
                && !restore.RestoreSessions:
                throw new ApplicationRequestException("restore_selection_required", "At least one restore target is required.");
            case PruneIntent prune when prune.BackupRetentionCount < 1:
                throw new ApplicationRequestException("retention_invalid", "Backup retention must be at least one.");
        }
    }

    private static void ValidatePreview(ApplicationWriteKind expectedKind, ApplicationPlanPreview? preview)
    {
        if (preview is null
            || preview.NormalizedIntent is null
            || preview.NormalizedIntent.Kind != expectedKind
            || string.IsNullOrWhiteSpace(preview.StateFingerprint)
            || string.IsNullOrWhiteSpace(preview.ExecutionToken))
        {
            throw new ApplicationPortException(
                "invalid_plan_preview",
                "Core returned an incomplete or mismatched plan preview.");
        }
        if (preview.Targets is null
            || preview.Targets.Any(static target =>
                target is null
                || string.IsNullOrWhiteSpace(target.Path)
                || string.IsNullOrWhiteSpace(target.Action)
                || string.IsNullOrWhiteSpace(target.Fingerprint)))
        {
            throw new ApplicationPortException(
                "invalid_plan_targets",
                "Core returned an invalid plan target.");
        }
        if (preview.Targets
            .GroupBy(static target => (target.Path, target.Action))
            .Any(static group => group.Count() > 1))
        {
            throw new ApplicationPortException(
                "duplicate_plan_target",
                "Core returned a duplicate plan target.");
        }
        if (preview.AutoPruneDeletionTargets is not null
            && preview.AutoPruneDeletionTargets.Any(static target =>
                target is null
                || string.IsNullOrWhiteSpace(target.Path)
                || !string.Equals(target.Action, "delete", StringComparison.Ordinal)
                || string.IsNullOrWhiteSpace(target.Fingerprint)))
        {
            throw new ApplicationPortException(
                "invalid_auto_prune_targets",
                "Core returned an invalid automatic-prune target.");
        }
        if (preview.AutoPruneDeletionTargets is not null
            && preview.AutoPruneDeletionTargets
                .GroupBy(static target => target.Path, StringComparer.Ordinal)
                .Any(static group => group.Count() > 1))
        {
            throw new ApplicationPortException(
                "duplicate_auto_prune_target",
                "Core returned a duplicate automatic-prune target.");
        }
        if (preview.Warnings is not null
            && preview.Warnings.Any(static warning =>
                warning is null
                || string.IsNullOrWhiteSpace(warning.Code)
                || string.IsNullOrWhiteSpace(warning.Message)))
        {
            throw new ApplicationPortException(
                "invalid_plan_warning",
                "Core returned an invalid plan warning.");
        }
    }

    private static ApplicationStatusRequest Freeze(ApplicationStatusRequest request)
    {
        return new ApplicationStatusRequest(request.CodexHome, request.SqliteHomeOverride);
    }

    private static ApplicationApplyAuthorization? Freeze(ApplicationApplyAuthorization? authorization)
    {
        return authorization is null
            ? null
            : new ApplicationApplyAuthorization(
                authorization.Apply,
                authorization.Plan is null ? null : Freeze(authorization.Plan),
                authorization.PlanDigest);
    }

    private static ApplicationOperationPlan Freeze(ApplicationOperationPlan plan)
    {
        return plan with
        {
            Intent = plan.Intent is null
                ? throw new ApplicationRequestException("plan_invalid", "The supplied plan has no intent.")
                : Freeze(plan.Intent),
            Targets = (plan.Targets ?? [])
                .Select(static target => target with { })
                .ToList()
                .AsReadOnly(),
            AutoPruneDeletionTargets = (plan.AutoPruneDeletionTargets ?? [])
                .Select(static target => target with { })
                .ToList()
                .AsReadOnly(),
            Warnings = (plan.Warnings ?? [])
                .Select(static warning => warning with { })
                .ToList()
                .AsReadOnly()
        };
    }

    private static ApplicationWriteIntent Freeze(ApplicationWriteIntent intent)
    {
        ArgumentNullException.ThrowIfNull(intent);
        return intent switch
        {
            SyncIntent sync => sync with { },
            SwitchIntent change => change with
            {
                ModelSelection = change.ModelSelection switch
                {
                    CustomModelSelection custom => custom with { },
                    FollowProviderModelSelection follow => follow with { },
                    KeepRootModelSelection keep => keep with { },
                    null => null!,
                    _ => throw new ArgumentOutOfRangeException(nameof(intent), "Unknown model selection.")
                }
            },
            RestoreIntent restore => restore with { },
            PruneIntent prune => prune with { },
            _ => throw new ArgumentOutOfRangeException(nameof(intent), "Unknown write intent.")
        };
    }

    private string NewId()
    {
        string id = _idFactory();
        if (string.IsNullOrWhiteSpace(id))
        {
            throw new InvalidOperationException("The operation id factory returned an empty id.");
        }
        return id;
    }

    private static bool IsRecoveryRequired(Exception error)
    {
        return error is ApplicationPlanLedgerCorruptionException
            || error is RecoveryRequiredException
            || error is SyncTransactionException { RecoveryRequired: true }
            || error is ApplicationPortException { RecoveryRequired: true };
    }

    private static bool FixedTimeEquals(string left, string right)
    {
        byte[] leftBytes = Encoding.UTF8.GetBytes(left);
        byte[] rightBytes = Encoding.UTF8.GetBytes(right);
        return leftBytes.Length == rightBytes.Length
            && CryptographicOperations.FixedTimeEquals(leftBytes, rightBytes);
    }

    internal static string ComputeDigest(ApplicationOperationPlan plan)
    {
        StringBuilder canonical = new();
        Append(canonical, "protocolVersion", plan.ProtocolVersion);
        Append(canonical, "planId", plan.PlanId);
        Append(canonical, "createdByOperationId", plan.CreatedByOperationId);
        Append(canonical, "createdAtUtc", plan.CreatedAtUtc.ToUniversalTime().ToString("O"));
        Append(canonical, "expiresAtUtc", plan.ExpiresAtUtc.ToUniversalTime().ToString("O"));
        AppendIntent(canonical, plan.Intent);
        Append(canonical, "stateFingerprint", plan.StateFingerprint);
        Append(canonical, "executionToken", plan.ExecutionToken);

        foreach (ApplicationPlanTarget target in plan.Targets
                     .OrderBy(static item => item.Path, StringComparer.Ordinal)
                     .ThenBy(static item => item.Action, StringComparer.Ordinal)
                     .ThenBy(static item => item.Fingerprint, StringComparer.Ordinal))
        {
            Append(canonical, "target.path", target.Path);
            Append(canonical, "target.action", target.Action);
            Append(canonical, "target.fingerprint", target.Fingerprint);
        }
        foreach (ApplicationPlanTarget target in plan.AutoPruneDeletionTargets
                     .OrderBy(static item => item.Path, StringComparer.Ordinal)
                     .ThenBy(static item => item.Action, StringComparer.Ordinal)
                     .ThenBy(static item => item.Fingerprint, StringComparer.Ordinal))
        {
            Append(canonical, "autoPruneTarget.path", target.Path);
            Append(canonical, "autoPruneTarget.action", target.Action);
            Append(canonical, "autoPruneTarget.fingerprint", target.Fingerprint);
        }
        foreach (ApplicationWarning warning in plan.Warnings
                     .OrderBy(static item => item.Code, StringComparer.Ordinal)
                     .ThenBy(static item => item.Message, StringComparer.Ordinal))
        {
            Append(canonical, "warning.code", warning.Code);
            Append(canonical, "warning.message", warning.Message);
        }

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical.ToString())))
            .ToLowerInvariant();
    }

    private static void AppendIntent(StringBuilder canonical, ApplicationWriteIntent intent)
    {
        Append(canonical, "intent.kind", intent.Kind switch
        {
            ApplicationWriteKind.Sync => "sync",
            ApplicationWriteKind.Switch => "switch",
            ApplicationWriteKind.Restore => "restore",
            ApplicationWriteKind.Prune => "prune",
            _ => throw new ArgumentOutOfRangeException(nameof(intent), "Unknown write kind.")
        });
        Append(canonical, "intent.codexHome", intent.CodexHome);
        Append(canonical, "intent.sqliteHomeOverride", intent.SqliteHomeOverride);
        switch (intent)
        {
            case SyncIntent sync:
                Append(canonical, "intent.providerId", sync.ProviderId);
                Append(canonical, "intent.backupRetentionCount", sync.BackupRetentionCount.ToString(System.Globalization.CultureInfo.InvariantCulture));
                break;
            case SwitchIntent change:
                Append(canonical, "intent.providerId", change.ProviderId);
                Append(canonical, "intent.backupRetentionCount", change.BackupRetentionCount.ToString(System.Globalization.CultureInfo.InvariantCulture));
                switch (change.ModelSelection)
                {
                    case FollowProviderModelSelection:
                        Append(canonical, "intent.modelMode", "followProvider");
                        break;
                    case KeepRootModelSelection:
                        Append(canonical, "intent.modelMode", "keepRootModel");
                        break;
                    case CustomModelSelection custom:
                        Append(canonical, "intent.modelMode", "custom");
                        Append(canonical, "intent.model", custom.Model);
                        break;
                    default:
                        Append(canonical, "intent.modelMode", null);
                        break;
                }
                break;
            case RestoreIntent restore:
                Append(canonical, "intent.backupDirectory", restore.BackupDirectory);
                Append(canonical, "intent.restoreConfig", restore.RestoreConfig ? "true" : "false");
                Append(canonical, "intent.restoreDatabase", restore.RestoreDatabase ? "true" : "false");
                Append(canonical, "intent.restoreSessions", restore.RestoreSessions ? "true" : "false");
                Append(canonical, "intent.allowSqliteHomeRelocation", restore.AllowSqliteHomeRelocation ? "true" : "false");
                break;
            case PruneIntent prune:
                Append(canonical, "intent.backupRetentionCount", prune.BackupRetentionCount.ToString(System.Globalization.CultureInfo.InvariantCulture));
                break;
        }
    }

    private static void Append(StringBuilder canonical, string key, string? value)
    {
        canonical.Append(key.Length).Append(':').Append(key).Append('=');
        if (value is null)
        {
            canonical.Append("-1:");
        }
        else
        {
            canonical.Append(value.Length).Append(':').Append(value);
        }
        canonical.Append(';');
    }

    private sealed class OperationContext(
        string operationId,
        DateTimeOffset startedAtUtc,
        TimeProvider timeProvider)
    {
        private readonly List<ApplicationLifecycleEvent> _timeline =
            [new(ApplicationOperationLifecycle.Accepted, startedAtUtc)];

        public string OperationId { get; } = operationId;

        public DateTimeOffset StartedAtUtc { get; } = startedAtUtc;

        public IReadOnlyList<ApplicationLifecycleEvent> Timeline => _timeline
            .Select(static item => item with { })
            .ToList()
            .AsReadOnly();

        public void MoveTo(ApplicationOperationLifecycle lifecycle)
        {
            if (_timeline[^1].Lifecycle != lifecycle)
            {
                _timeline.Add(new ApplicationLifecycleEvent(lifecycle, timeProvider.GetUtcNow()));
            }
        }
    }

    private sealed record OperationResult<T>(
        T Data,
        ApplicationOperationLifecycle Lifecycle,
        IReadOnlyList<ApplicationWarning> Warnings)
        where T : class
    {
        public static OperationResult<T> Succeeded(
            T data,
            IReadOnlyList<ApplicationWarning>? warnings = null)
        {
            return new OperationResult<T>(
                data,
                ApplicationOperationLifecycle.Succeeded,
                warnings ?? []);
        }

        public static OperationResult<T> ReadyToApply(
            T data,
            IReadOnlyList<ApplicationWarning>? warnings = null)
        {
            return new OperationResult<T>(
                data,
                ApplicationOperationLifecycle.ReadyToApply,
                warnings ?? []);
        }
    }

    private sealed class ApplicationRequestException(string code, string message)
        : InvalidOperationException(message)
    {
        public string Code { get; } = code;
    }
}
