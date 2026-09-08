using CodexProviderSync.Core;

namespace CodexProviderSync.Application;

/// <summary>
/// Production write adapter. Planning and checked execution are both owned by
/// Core; this adapter only normalizes Application inputs and maps immutable
/// contracts between the two layers.
/// </summary>
public sealed class CoreApplicationWritePort : IApplicationWritePort
{
    private readonly CodexSyncService _syncService;
    private readonly CodexHomeService _codexHomeService;

    public CoreApplicationWritePort()
        : this(new CodexSyncService(), new CodexHomeService())
    {
    }

    public CoreApplicationWritePort(
        CodexSyncService syncService,
        CodexHomeService codexHomeService)
    {
        _syncService = syncService ?? throw new ArgumentNullException(nameof(syncService));
        _codexHomeService = codexHomeService ?? throw new ArgumentNullException(nameof(codexHomeService));
    }

    public async Task<ApplicationPlanPreview> CreatePlanAsync(
        ApplicationWriteIntent intent,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(intent);
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ApplicationWriteIntent normalized = NormalizeIntent(intent);
        CoreWritePlanSnapshot snapshot = await MapCoreFailuresAsync(
            () => CreateCoreSnapshotAsync(normalized, cancellationToken));
        return new ApplicationPlanPreview(
            normalized,
            snapshot.StateFingerprint,
            snapshot.ExecutionToken,
            snapshot.Targets.Select(MapTarget).ToArray(),
            snapshot.AutoPruneDeletionTargets.Select(MapTarget).ToArray(),
            snapshot.Warnings.Select(MapWarning).ToArray());
    }

    public Task<SyncResult> ExecuteSyncAsync(
        SyncIntent intent,
        ApplicationOperationPlan plan,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        SyncIntent normalized = (SyncIntent)NormalizeIntent(intent);
        CoreWritePlanSnapshot expected = MapExpectedSnapshot("sync", plan);
        return MapCoreFailuresAsync(() => _syncService.RunSyncCheckedAsync(
            expected,
            normalized.CodexHome,
            normalized.ProviderId,
            normalized.BackupRetentionCount,
            explicitSqliteHome: normalized.SqliteHomeOverride,
            snapshotExpiresAtUtc: plan.ExpiresAtUtc,
            cancellationToken: cancellationToken));
    }

    public Task<SyncResult> ExecuteSwitchAsync(
        SwitchIntent intent,
        ApplicationOperationPlan plan,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        SwitchIntent normalized = (SwitchIntent)NormalizeIntent(intent);
        (string? model, bool keepRootModel) = MapModelSelection(normalized.ModelSelection);
        CoreWritePlanSnapshot expected = MapExpectedSnapshot("switch", plan);
        return MapCoreFailuresAsync(() => _syncService.RunSwitchCheckedAsync(
            expected,
            explicitCodexHome: normalized.CodexHome,
            provider: normalized.ProviderId,
            keepCount: normalized.BackupRetentionCount,
            model,
            keepRootModel,
            explicitSqliteHome: normalized.SqliteHomeOverride,
            snapshotExpiresAtUtc: plan.ExpiresAtUtc,
            cancellationToken));
    }

    public Task<RestoreResult> ExecuteRestoreAsync(
        RestoreIntent intent,
        ApplicationOperationPlan plan,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        RestoreIntent normalized = (RestoreIntent)NormalizeIntent(intent);
        CoreWritePlanSnapshot expected = MapExpectedSnapshot("restore", plan);
        return MapCoreFailuresAsync(() => _syncService.RunRestoreCheckedAsync(
            expected,
            explicitCodexHome: normalized.CodexHome,
            backupDir: normalized.BackupDirectory,
            options: MapRestoreOptions(normalized),
            explicitSqliteHome: normalized.SqliteHomeOverride,
            snapshotExpiresAtUtc: plan.ExpiresAtUtc,
            cancellationToken));
    }

    public Task<BackupPruneResult> ExecutePruneAsync(
        PruneIntent intent,
        ApplicationOperationPlan plan,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        PruneIntent normalized = (PruneIntent)NormalizeIntent(intent);
        CoreWritePlanSnapshot expected = MapExpectedSnapshot("prune", plan);
        return MapCoreFailuresAsync(() => _syncService.RunPruneBackupsCheckedAsync(
            expected,
            explicitCodexHome: normalized.CodexHome,
            keepCount: normalized.BackupRetentionCount,
            snapshotExpiresAtUtc: plan.ExpiresAtUtc,
            cancellationToken));
    }

    private Task<CoreWritePlanSnapshot> CreateCoreSnapshotAsync(
        ApplicationWriteIntent intent,
        CancellationToken cancellationToken)
    {
        return intent switch
        {
            SyncIntent sync => _syncService.CreateSyncPlanSnapshotAsync(
                sync.CodexHome,
                sync.ProviderId,
                sync.BackupRetentionCount,
                explicitSqliteHome: sync.SqliteHomeOverride,
                cancellationToken: cancellationToken),
            SwitchIntent change => CreateSwitchSnapshotAsync(change, cancellationToken),
            RestoreIntent restore => _syncService.CreateRestorePlanSnapshotAsync(
                restore.CodexHome,
                restore.BackupDirectory,
                MapRestoreOptions(restore),
                restore.SqliteHomeOverride,
                cancellationToken),
            PruneIntent prune => _syncService.CreatePrunePlanSnapshotAsync(
                prune.CodexHome,
                prune.BackupRetentionCount,
                cancellationToken),
            _ => throw new ApplicationPortException(
                "operation_unsupported",
                "The requested Core write operation is not supported.")
        };
    }

    private Task<CoreWritePlanSnapshot> CreateSwitchSnapshotAsync(
        SwitchIntent intent,
        CancellationToken cancellationToken)
    {
        (string? model, bool keepRootModel) = MapModelSelection(intent.ModelSelection);
        return _syncService.CreateSwitchPlanSnapshotAsync(
            intent.CodexHome,
            intent.ProviderId,
            intent.BackupRetentionCount,
            model,
            keepRootModel,
            intent.SqliteHomeOverride,
            cancellationToken);
    }

    public ApplicationWriteIntent NormalizeIntent(ApplicationWriteIntent intent)
    {
        string codexHome = _codexHomeService.NormalizeCodexHome(intent.CodexHome);
        string? sqliteHome = NormalizeOptionalPath(intent.SqliteHomeOverride);
        return intent switch
        {
            SyncIntent sync => new SyncIntent(
                codexHome,
                sqliteHome,
                sync.ProviderId.Trim(),
                sync.BackupRetentionCount),
            SwitchIntent change => new SwitchIntent(
                codexHome,
                sqliteHome,
                change.ProviderId.Trim(),
                NormalizeModelSelection(change.ModelSelection),
                change.BackupRetentionCount),
            RestoreIntent restore => new RestoreIntent(
                codexHome,
                sqliteHome,
                Path.GetFullPath(restore.BackupDirectory.Trim()),
                restore.RestoreConfig,
                restore.RestoreDatabase,
                restore.RestoreSessions,
                restore.AllowSqliteHomeRelocation),
            PruneIntent prune => new PruneIntent(
                codexHome,
                sqliteHome,
                prune.BackupRetentionCount),
            _ => throw new ApplicationPortException(
                "operation_unsupported",
                "The requested Core write operation is not supported.")
        };
    }

    private static SwitchModelSelection NormalizeModelSelection(SwitchModelSelection selection)
    {
        return selection switch
        {
            FollowProviderModelSelection => new FollowProviderModelSelection(),
            KeepRootModelSelection => new KeepRootModelSelection(),
            CustomModelSelection custom => new CustomModelSelection(custom.Model.Trim()),
            _ => throw new ApplicationPortException(
                "model_selection_invalid",
                "The switch model selection is not supported.")
        };
    }

    private static (string? Model, bool KeepRootModel) MapModelSelection(
        SwitchModelSelection selection)
    {
        return selection switch
        {
            FollowProviderModelSelection => (null, false),
            KeepRootModelSelection => (null, true),
            CustomModelSelection custom => (custom.Model, false),
            _ => throw new ApplicationPortException(
                "model_selection_invalid",
                "The switch model selection is not supported.")
        };
    }

    private static RestoreBackupOptions MapRestoreOptions(RestoreIntent intent)
    {
        return new RestoreBackupOptions
        {
            RestoreConfig = intent.RestoreConfig,
            RestoreDatabase = intent.RestoreDatabase,
            RestoreSessions = intent.RestoreSessions,
            AllowSqliteHomeRelocation = intent.AllowSqliteHomeRelocation
        };
    }

    private static CoreWritePlanSnapshot MapExpectedSnapshot(
        string operation,
        ApplicationOperationPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        return new CoreWritePlanSnapshot(
            operation,
            plan.StateFingerprint,
            plan.ExecutionToken,
            plan.Targets.Select(MapTarget).ToArray(),
            plan.AutoPruneDeletionTargets.Select(MapTarget).ToArray(),
            plan.Warnings.Select(MapWarning).ToArray());
    }

    private static ApplicationPlanTarget MapTarget(CoreWritePlanTarget target) =>
        new(target.Path, target.Action, target.Fingerprint);

    private static CoreWritePlanTarget MapTarget(ApplicationPlanTarget target) =>
        new(target.Path, target.Action, target.Fingerprint);

    private static ApplicationWarning MapWarning(CoreWritePlanWarning warning) =>
        new(warning.Code, warning.Message);

    private static CoreWritePlanWarning MapWarning(ApplicationWarning warning) =>
        new(warning.Code, warning.Message);

    private static string? NormalizeOptionalPath(string? value)
    {
        return string.IsNullOrWhiteSpace(value)
            ? null
            : Path.GetFullPath(value.Trim());
    }

    private static async Task<T> MapCoreFailuresAsync<T>(Func<Task<T>> operation)
    {
        try
        {
            return await operation();
        }
        catch (CoreWritePlanStaleException error)
        {
            throw new ApplicationPortException(
                "plan_stale",
                error.Message,
                innerException: error);
        }
        catch (CoreWritePlanExpiredException error)
        {
            throw new ApplicationPortException(
                "plan_expired",
                error.Message,
                innerException: error);
        }
        catch (InvalidOperationException error) when (LockService.IsOperationBusy(error))
        {
            throw new ApplicationPortException(
                "target_busy",
                error.Message,
                innerException: error);
        }
        catch (InvalidOperationException error) when (LockService.IsLockUnverifiable(error))
        {
            throw new ApplicationPortException(
                "lock_unverifiable",
                error.Message,
                innerException: error);
        }
    }
}
