using System.Collections.Concurrent;
using System.Diagnostics;

namespace CodexProviderSync.Core;

public sealed class CodexSyncService
{
    private static readonly ConcurrentDictionary<string, StatusSnapshot> LastCompleteStatus = new(
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
    private readonly CodexHomeService _codexHomeService;
    private readonly ConfigFileService _configFileService;
    private readonly SessionRolloutService _sessionRolloutService;
    private readonly SqliteStateService _sqliteStateService;
    private readonly GlobalStateService _globalStateService;
    private readonly BackupService _backupService;
    private readonly LockService _lockService;
    private readonly ProviderDiscoveryService _providerDiscoveryService;
    private readonly CodexStorageLayoutService _storageLayoutService;

    internal Func<string, string?, int, Task>? FaultInjector { get; set; }

    public CodexSyncService()
        : this(
            new CodexHomeService(),
            new ConfigFileService(),
            new SessionRolloutService(),
            new SqliteStateService(),
            new GlobalStateService(),
            new LockService(),
            new ProviderDiscoveryService())
    {
    }

    public CodexSyncService(
        CodexHomeService codexHomeService,
        ConfigFileService configFileService,
        SessionRolloutService sessionRolloutService,
        SqliteStateService sqliteStateService,
        GlobalStateService globalStateService,
        LockService lockService,
        ProviderDiscoveryService providerDiscoveryService)
    {
        _codexHomeService = codexHomeService;
        _configFileService = configFileService;
        _sessionRolloutService = sessionRolloutService;
        _sqliteStateService = sqliteStateService;
        _globalStateService = globalStateService;
        _lockService = lockService;
        _providerDiscoveryService = providerDiscoveryService;
        _storageLayoutService = new CodexStorageLayoutService(codexHomeService, configFileService);
        _backupService = new BackupService(sessionRolloutService, sqliteStateService);
    }

    public async Task<StatusSnapshot> GetStatusAsync(
        string? explicitCodexHome = null,
        string? explicitSqliteHome = null)
    {
        long totalStarted = Stopwatch.GetTimestamp();
        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);
        string cacheKey = BuildStatusCacheKey(codexHome, explicitSqliteHome);
        LockInspection homeBefore = await _lockService.InspectLockAsync(codexHome);
        if (!homeBefore.IsAbsent)
        {
            return BuildBlockedStatus(
                cacheKey,
                codexHome,
                explicitSqliteHome,
                homeBefore,
                "codex-home-lock");
        }

        string configText = await _configFileService.ReadConfigTextAsync(_codexHomeService.ConfigPath(codexHome));
        CodexStorageLayout storage = await PrepareStorageAsync(codexHome, explicitSqliteHome, configText);
        LockInspection homeResolved = await _lockService.InspectLockAsync(codexHome);
        if (!SameAbsentObservation(homeBefore, homeResolved))
        {
            return BuildBlockedStatus(
                cacheKey,
                codexHome,
                explicitSqliteHome,
                homeResolved,
                "codex-home-lock");
        }

        StateDbLockResource? stateResource = null;
        LockInspection? stateBefore = null;
        if (storage.StateDbLocation is { } stateDbLocation)
        {
            try
            {
                stateResource = await StateDbLockResource.ResolveAsync(stateDbLocation.Path);
                stateBefore = await _lockService.InspectStateDbLockAsync(stateResource);
            }
            catch (Exception error) when (error is InvalidOperationException
                or IOException
                or UnauthorizedAccessException)
            {
                return BuildBlockedStatus(
                    cacheKey,
                    codexHome,
                    explicitSqliteHome,
                    SyntheticUnverifiableInspection("state-db", error),
                    "state-db-lock");
            }
            if (!stateBefore.IsAbsent)
            {
                return BuildBlockedStatus(
                    cacheKey,
                    codexHome,
                    explicitSqliteHome,
                    stateBefore,
                    "state-db-lock");
            }
        }

        CoreWritePlanSnapshot revisionBefore;
        try
        {
            revisionBefore = await BuildStatusRevisionAsync(codexHome, storage);
        }
        catch (Exception error) when (error is IOException
            or UnauthorizedAccessException
            or CoreWritePlanStaleException)
        {
            return BuildBlockedStatus(
                cacheKey,
                codexHome,
                explicitSqliteHome,
                SyntheticUnverifiableInspection("codex-home", error),
                "revision-unverifiable");
        }

        StatusSnapshot snapshot = await ScanStatusAsync(
            codexHome,
            configText,
            storage,
            revisionBefore.StateFingerprint,
            totalStarted);

        LockInspection homeAfter = await _lockService.InspectLockAsync(codexHome);
        if (!SameAbsentObservation(homeBefore, homeAfter))
        {
            return BuildBlockedStatus(
                cacheKey,
                codexHome,
                explicitSqliteHome,
                homeAfter,
                "codex-home-lock");
        }
        LockInspection? stateAfter = null;
        if (stateResource is not null)
        {
            stateAfter = await _lockService.InspectStateDbLockAsync(stateResource);
            if (!SameAbsentObservation(stateBefore!, stateAfter))
            {
                return BuildBlockedStatus(
                    cacheKey,
                    codexHome,
                    explicitSqliteHome,
                    stateAfter,
                    "state-db-lock");
            }
        }

        try
        {
            CoreWritePlanSnapshot revisionAfter = await BuildStatusRevisionAsync(codexHome, storage);
            CoreWriteSnapshotBuilder.AssertExactMatch(revisionBefore, revisionAfter);
        }
        catch (Exception error) when (error is IOException
            or UnauthorizedAccessException
            or CoreWritePlanStaleException)
        {
            return BuildBlockedStatus(
                cacheKey,
                codexHome,
                explicitSqliteHome,
                SyntheticUnverifiableInspection("codex-home", error),
                "state-changed-during-status",
                revisionBefore.StateFingerprint);
        }

        LockInspection homeFinal = await _lockService.InspectLockAsync(codexHome);
        if (!SameAbsentObservation(homeBefore, homeFinal))
        {
            return BuildBlockedStatus(
                cacheKey,
                codexHome,
                explicitSqliteHome,
                homeFinal,
                "codex-home-lock");
        }
        if (stateResource is not null)
        {
            LockInspection stateFinal = await _lockService.InspectStateDbLockAsync(stateResource);
            if (!SameAbsentObservation(stateBefore!, stateFinal))
            {
                return BuildBlockedStatus(
                    cacheKey,
                    codexHome,
                    explicitSqliteHome,
                    stateFinal,
                    "state-db-lock");
            }
        }

        // Keep the cache isolated from mutable DTO collections returned to
        // callers. A consumer must never be able to poison the safety snapshot
        // that is served while a write lock is active.
        LastCompleteStatus[cacheKey] = CopyStatusSnapshot(snapshot);
        return snapshot;
    }

    private async Task<StatusSnapshot> ScanStatusAsync(
        string codexHome,
        string configText,
        CodexStorageLayout storage,
        string storageRevision,
        long totalStarted)
    {
        CurrentProviderInfo currentProvider = _configFileService.ReadCurrentProviderFromConfigText(configText);
        IReadOnlyList<string> configuredProviders = _configFileService.ListConfiguredProviderIds(configText);
        long rolloutScanStarted = Stopwatch.GetTimestamp();
        SessionChangeCollection rolloutInfo = await _sessionRolloutService.CollectSessionChangesAsync(codexHome, "__status_only__", skipLockedReads: true);
        long rolloutScanDurationMs = ElapsedMilliseconds(rolloutScanStarted);
        StateDbLocation? stateDbLocation = storage.StateDbLocation;
        ProviderCounts? sqliteCounts = storage.SqliteAccess.Supported
            ? await _sqliteStateService.ReadSqliteProviderCountsAsync(storage)
            : null;
        SqliteRepairStats? sqliteRepairStats = sqliteCounts is not null && !sqliteCounts.Unreadable
            ? await _sqliteStateService.ReadSqliteRepairStatsAsync(
                storage,
                rolloutInfo.UserEventThreadIds,
                rolloutInfo.ThreadCwdsById)
            : null;
        IReadOnlyList<ProjectThreadVisibility> projectThreadVisibility = !storage.SqliteAccess.Supported
            || sqliteCounts?.Unreadable == true
            ? []
            : await _globalStateService.ReadProjectThreadVisibilityAsync(storage);
        long backupSummaryStarted = Stopwatch.GetTimestamp();
        BackupSummary backupSummary = await _backupService.GetBackupSummaryAsync(codexHome);
        long backupSummaryDurationMs = ElapsedMilliseconds(backupSummaryStarted);
        IReadOnlyList<PendingTransactionInfo> pendingTransactions = await FileTransactionJournal.FindPendingAsync(codexHome);

        return new StatusSnapshot
        {
            SnapshotAt = DateTimeOffset.UtcNow,
            StorageRevision = storageRevision,
            CodexHome = codexHome,
            SqliteHome = storage.SqliteHome,
            SqliteHomeSource = storage.SqliteHomeSource,
            SqliteAccess = storage.SqliteAccess,
            CheckedStateDbPaths = storage.StateDbCandidates.Select(static candidate => candidate.Path).ToList(),
            CurrentProvider = currentProvider,
            ConfiguredProviders = configuredProviders,
            RolloutCounts = rolloutInfo.ProviderCounts,
            LockedRolloutFiles = rolloutInfo.LockedPaths,
            UnreadableRolloutFiles = rolloutInfo.UnreadablePaths,
            EncryptedContentCounts = rolloutInfo.EncryptedContentCounts,
            EncryptedContentWarning = BuildEncryptedContentWarning(rolloutInfo.EncryptedContentCounts, currentProvider.Provider),
            SqliteCounts = sqliteCounts,
            StateDbLocation = stateDbLocation,
            SqliteRepairStats = sqliteRepairStats,
            ProjectThreadVisibility = projectThreadVisibility,
            BackupRoot = _codexHomeService.BackupRoot(codexHome),
            BackupSummary = backupSummary,
            PendingTransactions = pendingTransactions
                .Select(static item => new TransactionRecoveryInfo(
                    item.OperationId,
                    item.State,
                    item.BackupDir,
                    item.JournalPath))
                .ToArray(),
            RolloutScanComplete = rolloutInfo.LockedPaths.Count == 0
                && rolloutInfo.UnreadablePaths.Count == 0,
            PerformanceMetrics = new StatusPerformanceMetrics
            {
                TotalDurationMs = ElapsedMilliseconds(totalStarted),
                RolloutScanDurationMs = rolloutScanDurationMs,
                BackupSummaryDurationMs = backupSummaryDurationMs,
                RolloutScan = rolloutInfo.ScanMetrics
            }
        };
    }

    private async Task<CoreWritePlanSnapshot> BuildStatusRevisionAsync(
        string codexHome,
        CodexStorageLayout storage)
    {
        List<CoreWriteTargetSpec> targets =
        [
            new(_codexHomeService.ConfigPath(codexHome), "read"),
            new(
                Path.Combine(codexHome, "sessions"),
                "scan",
                CoreWriteFingerprintMode.RecursiveInventory),
            new(
                Path.Combine(codexHome, "archived_sessions"),
                "scan",
                CoreWriteFingerprintMode.RecursiveInventory),
            new(_globalStateService.StatePath(codexHome), "read-if-present"),
            new(_globalStateService.BackupPath(codexHome), "read-if-present"),
            new(
                _codexHomeService.BackupRoot(codexHome),
                "inventory",
                CoreWriteFingerprintMode.RecursiveInventory)
        ];
        if (storage.StateDbLocation is { } stateDb)
        {
            targets.Add(new CoreWriteTargetSpec(
                stateDb.Path,
                "read",
                CoreWriteFingerprintMode.SqliteMainContent));
            targets.Add(new CoreWriteTargetSpec(
                stateDb.Path + "-wal",
                "read-if-present",
                CoreWriteFingerprintMode.SqliteWalContent));
        }

        string binding = System.Text.Json.JsonSerializer.Serialize(new
        {
            codexHome,
            storage.SqliteHome,
            storage.SqliteHomeSource,
            stateDbPath = storage.StateDbLocation?.Path,
            storage.SqliteAccess.Supported,
            storage.SqliteAccess.Reason
        });
        return await CoreWriteSnapshotBuilder.BuildAsync(
            "status",
            binding,
            targets);
    }

    private StatusSnapshot BuildBlockedStatus(
        string cacheKey,
        string codexHome,
        string? explicitSqliteHome,
        LockInspection inspection,
        string reason,
        string? revision = null)
    {
        StatusOperationInfo operation = new(
            inspection.Owner?.InstanceId,
            string.IsNullOrWhiteSpace(inspection.Owner?.Label) ? "unknown" : inspection.Owner.Label!,
            "external",
            inspection.Owner?.Runtime,
            inspection.Owner?.StartedAt,
            inspection.Scope,
            inspection.State == "active" ? "active" : "unverifiable",
            inspection.ErrorCode);
        StatusReadBlockedInfo blocked = new(
            reason,
            operation.LockState,
            revision);
        if (LastCompleteStatus.TryGetValue(cacheKey, out StatusSnapshot? cached))
        {
            return CopyStatusSnapshot(cached, operation, blocked);
        }
        return CreateUnavailableStatus(
            codexHome,
            explicitSqliteHome,
            operation,
            blocked);
    }

    private static StatusSnapshot CopyStatusSnapshot(
        StatusSnapshot source,
        StatusOperationInfo? operation = null,
        StatusReadBlockedInfo? blocked = null)
    {
        return new StatusSnapshot
        {
            SchemaVersion = source.SchemaVersion,
            SnapshotAt = source.SnapshotAt,
            StorageRevision = source.StorageRevision,
            CodexHome = source.CodexHome,
            SqliteHome = source.SqliteHome,
            SqliteHomeSource = source.SqliteHomeSource,
            SqliteAccess = source.SqliteAccess with { },
            CheckedStateDbPaths = source.CheckedStateDbPaths.ToArray(),
            CurrentProvider = source.CurrentProvider with { },
            ConfiguredProviders = source.ConfiguredProviders.ToArray(),
            RolloutCounts = CopyProviderCounts(source.RolloutCounts),
            LockedRolloutFiles = source.LockedRolloutFiles.ToArray(),
            UnreadableRolloutFiles = source.UnreadableRolloutFiles.ToArray(),
            EncryptedContentCounts = CopyProviderCounts(source.EncryptedContentCounts),
            EncryptedContentWarning = source.EncryptedContentWarning,
            SqliteCounts = source.SqliteCounts is null ? null : CopyProviderCounts(source.SqliteCounts),
            StateDbLocation = source.StateDbLocation is null ? null : source.StateDbLocation with { },
            SqliteRepairStats = source.SqliteRepairStats is null
                ? null
                : new SqliteRepairStats
                {
                    UserEventRowsNeedingRepair = source.SqliteRepairStats.UserEventRowsNeedingRepair,
                    CwdRowsNeedingRepair = source.SqliteRepairStats.CwdRowsNeedingRepair
                },
            ProjectThreadVisibility = source.ProjectThreadVisibility
                .Select(static item => new ProjectThreadVisibility
                {
                    Root = item.Root,
                    InteractiveThreads = item.InteractiveThreads,
                    FirstPageThreads = item.FirstPageThreads,
                    ExactCwdMatches = item.ExactCwdMatches,
                    VerbatimCwdRows = item.VerbatimCwdRows,
                    Ranks = item.Ranks.ToArray(),
                    RankPreview = item.RankPreview,
                    ProviderCounts = new Dictionary<string, int>(item.ProviderCounts, StringComparer.Ordinal)
                })
                .ToArray(),
            BackupRoot = source.BackupRoot,
            BackupSummary = new BackupSummary
            {
                Count = source.BackupSummary.Count,
                TotalBytes = source.BackupSummary.TotalBytes
            },
            PendingTransactions = source.PendingTransactions.Select(static item => item with { }).ToArray(),
            OperationInProgress = operation,
            StatusReadBlocked = blocked,
            RolloutScanComplete = source.RolloutScanComplete,
            PerformanceMetrics = new StatusPerformanceMetrics
            {
                TotalDurationMs = source.PerformanceMetrics.TotalDurationMs,
                RolloutScanDurationMs = source.PerformanceMetrics.RolloutScanDurationMs,
                BackupSummaryDurationMs = source.PerformanceMetrics.BackupSummaryDurationMs,
                RolloutScan = new SessionScanMetrics
                {
                    EnumeratedRolloutFiles = source.PerformanceMetrics.RolloutScan.EnumeratedRolloutFiles,
                    ParsedSessionFiles = source.PerformanceMetrics.RolloutScan.ParsedSessionFiles,
                    ContentScanPasses = source.PerformanceMetrics.RolloutScan.ContentScanPasses,
                    ModelScanFiles = source.PerformanceMetrics.RolloutScan.ModelScanFiles,
                    DurationMs = source.PerformanceMetrics.RolloutScan.DurationMs
                }
            }
        };
    }

    private static ProviderCounts CopyProviderCounts(ProviderCounts source)
    {
        return new ProviderCounts
        {
            Sessions = new Dictionary<string, int>(source.Sessions, StringComparer.Ordinal),
            ArchivedSessions = new Dictionary<string, int>(source.ArchivedSessions, StringComparer.Ordinal),
            Unreadable = source.Unreadable,
            Error = source.Error
        };
    }

    private StatusSnapshot CreateUnavailableStatus(
        string codexHome,
        string? explicitSqliteHome,
        StatusOperationInfo operation,
        StatusReadBlockedInfo blocked)
    {
        string sqliteHome = string.IsNullOrWhiteSpace(explicitSqliteHome)
            ? string.Empty
            : Path.GetFullPath(explicitSqliteHome.Trim());
        ProviderCounts unavailableCounts = new()
        {
            Unreadable = true,
            Error = "Status scanning is blocked by an active or unverifiable write lock."
        };
        return new StatusSnapshot
        {
            SnapshotAt = DateTimeOffset.UtcNow,
            CodexHome = codexHome,
            SqliteHome = sqliteHome,
            SqliteHomeSource = string.IsNullOrWhiteSpace(explicitSqliteHome) ? "unresolved" : "explicit",
            SqliteAccess = new SqliteAccessInfo(
                false,
                "status-blocked",
                "Status scanning is blocked by an active or unverifiable write lock."),
            CurrentProvider = new CurrentProviderInfo("unknown", false),
            ConfiguredProviders = [],
            RolloutCounts = unavailableCounts,
            LockedRolloutFiles = [],
            UnreadableRolloutFiles = [],
            EncryptedContentCounts = new ProviderCounts
            {
                Unreadable = true,
                Error = unavailableCounts.Error
            },
            SqliteCounts = null,
            BackupRoot = _codexHomeService.BackupRoot(codexHome),
            BackupSummary = new BackupSummary { Count = 0, TotalBytes = 0 },
            OperationInProgress = operation,
            StatusReadBlocked = blocked,
            RolloutScanComplete = false
        };
    }

    private static string BuildStatusCacheKey(string codexHome, string? explicitSqliteHome)
    {
        string selector = string.IsNullOrWhiteSpace(explicitSqliteHome)
            ? "<resolved>"
            : Path.GetFullPath(explicitSqliteHome.Trim());
        return Path.GetFullPath(codexHome) + "\0" + selector;
    }

    private static bool SameAbsentObservation(LockInspection left, LockInspection right)
    {
        return left.IsAbsent
            && right.IsAbsent
            && string.Equals(
                left.ObservationRevision,
                right.ObservationRevision,
                StringComparison.Ordinal);
    }

    private static LockInspection SyntheticUnverifiableInspection(string scope, Exception error)
    {
        return new LockInspection(
            "unverifiable",
            scope,
            null,
            string.Empty,
            null,
            LockService.LockUnverifiableErrorCode,
            error.Message);
    }

    public IReadOnlyList<ProviderOption> BuildProviderOptions(StatusSnapshot status, AppSettings settings)
    {
        return _providerDiscoveryService.BuildProviderOptions(status, settings);
    }

    public IReadOnlyList<string> ExtractDetectedProviderIds(StatusSnapshot status)
    {
        return _providerDiscoveryService.ExtractDetectedProviderIds(status);
    }

    public Task<SyncResult> RunSyncAsync(
        string? explicitCodexHome = null,
        string? provider = null,
        string? configBackupText = null,
        int keepCount = AppConstants.DefaultBackupRetentionCount,
        int? sqliteBusyTimeoutMs = null,
        string? model = null,
        string? explicitSqliteHome = null,
        CancellationToken cancellationToken = default)
    {
        return RunSyncCoreAsync(
            explicitCodexHome,
            provider,
            configBackupText,
            keepCount,
            sqliteBusyTimeoutMs,
            model,
            explicitSqliteHome,
            switchPreparationFactory: null,
            expectedSnapshot: null,
            snapshotExpiresAtUtc: null,
            cancellationToken);
    }

    public async Task<CoreWritePlanSnapshot> CreateSyncPlanSnapshotAsync(
        string? explicitCodexHome = null,
        string? provider = null,
        int keepCount = AppConstants.DefaultBackupRetentionCount,
        int? sqliteBusyTimeoutMs = null,
        string? model = null,
        string? explicitSqliteHome = null,
        CancellationToken cancellationToken = default)
    {
        ValidateAutomaticRetention(keepCount);
        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);
        await FileTransactionJournal.AssertNoPendingAsync(codexHome);
        SyncPreparation preparation = await PrepareSyncAsync(
            codexHome,
            provider,
            sqliteBusyTimeoutMs,
            model,
            explicitSqliteHome,
            switchPreparationFactory: null,
            cancellationToken);
        return await BuildSyncPlanSnapshotAsync(preparation, keepCount, cancellationToken);
    }

    public Task<SyncResult> RunSyncCheckedAsync(
        CoreWritePlanSnapshot expectedSnapshot,
        string? explicitCodexHome = null,
        string? provider = null,
        int keepCount = AppConstants.DefaultBackupRetentionCount,
        int? sqliteBusyTimeoutMs = null,
        string? model = null,
        string? explicitSqliteHome = null,
        DateTimeOffset? snapshotExpiresAtUtc = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(expectedSnapshot);
        return RunSyncCoreAsync(
            explicitCodexHome,
            provider,
            configBackupText: null,
            keepCount,
            sqliteBusyTimeoutMs,
            model,
            explicitSqliteHome,
            switchPreparationFactory: null,
            expectedSnapshot,
            snapshotExpiresAtUtc,
            cancellationToken);
    }

    private async Task<SyncPreparation> PrepareSyncAsync(
        string codexHome,
        string? provider,
        int? sqliteBusyTimeoutMs,
        string? model,
        string? explicitSqliteHome,
        Func<string, SwitchPreparation>? switchPreparationFactory,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string configPath = _codexHomeService.ConfigPath(codexHome);
        string configText = await _configFileService.ReadConfigTextAsync(configPath);
        SwitchPreparation? switchPreparation = switchPreparationFactory?.Invoke(configText);
        CodexStorageLayout storage = await PrepareStorageAsync(codexHome, explicitSqliteHome, configText);
        storage.EnsureSqliteAccessSupported(switchPreparation is null ? "sync" : "switch");
        EnsureWritableStorage(storage);
        CurrentProviderInfo current = _configFileService.ReadCurrentProviderFromConfigText(configText);
        string targetProvider = switchPreparation?.Provider
            ?? provider
            ?? current.Provider
            ?? AppConstants.DefaultProvider;

        // When the caller did not pin a model, mirror the active root-level
        // `model = "..."` field from config.toml into the per-thread SQLite
        // `model` column. Without this, old sessions keep showing the model
        // they were created with in Codex's bottom-right UI label, even after
        // the root-level `model` changes.
        string? targetModel = switchPreparation?.ThreadModel ?? model;
        if (string.IsNullOrEmpty(targetModel))
        {
            targetModel = _configFileService.ReadRootModelFromConfigText(configText);
        }

        cancellationToken.ThrowIfCancellationRequested();
        SessionChangeCollection sessionInfo = await _sessionRolloutService.CollectSessionChangesAsync(
            codexHome,
            targetProvider,
            skipLockedReads: true,
            targetModel: targetModel);
        IReadOnlyList<ThreadCwdStat> workspaceCwdStats = await _globalStateService.ReadThreadCwdStatsAsync(storage);
        string? encryptedContentWarning = BuildEncryptedContentWarning(sessionInfo.EncryptedContentCounts, targetProvider);
        (IReadOnlyList<SessionChange> writableChanges, IReadOnlyList<SessionChange> lockedChanges) =
            await _sessionRolloutService.SplitLockedSessionChangesAsync(sessionInfo.Changes);
        List<string> skippedRolloutFiles = [.. sessionInfo.LockedPaths, .. lockedChanges.Select(static change => change.Path)];
        IReadOnlyList<string> skippedUnreadableRolloutFiles = sessionInfo.UnreadablePaths
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToList();

        await _sqliteStateService.AssertSqliteWritableAsync(storage, sqliteBusyTimeoutMs);
        cancellationToken.ThrowIfCancellationRequested();
        return new SyncPreparation(
            codexHome,
            configPath,
            configText,
            switchPreparation,
            storage,
            current,
            targetProvider,
            targetModel,
            sessionInfo,
            workspaceCwdStats,
            encryptedContentWarning,
            writableChanges,
            skippedRolloutFiles,
            skippedUnreadableRolloutFiles);
    }

    private async Task<CoreWritePlanSnapshot> BuildSyncPlanSnapshotAsync(
        SyncPreparation preparation,
        int keepCount,
        CancellationToken cancellationToken,
        bool useContentFingerprintHints = true)
    {
        string operation = preparation.SwitchPreparation is null ? "sync" : "switch";
        List<CoreWriteTargetSpec> targets =
        [
            new(preparation.ConfigPath, preparation.SwitchPreparation is null ? "read" : "replace"),
            new(
                Path.Combine(preparation.CodexHome, "sessions"),
                "scan",
                CoreWriteFingerprintMode.RecursiveInventory),
            new(
                Path.Combine(preparation.CodexHome, "archived_sessions"),
                "scan",
                CoreWriteFingerprintMode.RecursiveInventory),
            new(_globalStateService.StatePath(preparation.CodexHome), "replace-if-present"),
            new(_globalStateService.BackupPath(preparation.CodexHome), "replace-if-present"),
            new(
                _codexHomeService.BackupRoot(preparation.CodexHome),
                $"create-and-prune-to-{keepCount}",
                CoreWriteFingerprintMode.RecursiveInventory)
        ];
        targets.AddRange(preparation.WritableChanges.Select(
            static change => new CoreWriteTargetSpec(change.Path, "replace")));
        if (preparation.Storage.StateDbLocation is { } stateDb)
        {
            targets.Add(new CoreWriteTargetSpec(
                stateDb.Path,
                "update",
                CoreWriteFingerprintMode.SqliteMainContent));
            targets.Add(new CoreWriteTargetSpec(
                stateDb.Path + "-wal",
                "update-if-present",
                CoreWriteFingerprintMode.SqliteWalContent));
        }

        List<CoreWritePlanWarning> warnings = [];
        if (preparation.SkippedRolloutFiles.Count > 0)
        {
            warnings.Add(new CoreWritePlanWarning(
                "locked_rollout_files",
                $"{preparation.SkippedRolloutFiles.Count} rollout file(s) are locked and will be skipped."));
        }
        if (preparation.SkippedUnreadableRolloutFiles.Count > 0)
        {
            warnings.Add(new CoreWritePlanWarning(
                "unreadable_rollout_files",
                $"{preparation.SkippedUnreadableRolloutFiles.Count} rollout file(s) are unreadable and will be skipped."));
        }
        if (preparation.EncryptedContentWarning is not null)
        {
            warnings.Add(new CoreWritePlanWarning("encrypted_content", preparation.EncryptedContentWarning));
        }
        if (preparation.SwitchPreparation?.ModelSync.Warning is { } modelWarning)
        {
            warnings.Add(new CoreWritePlanWarning("model_sync", modelWarning));
        }

        string binding = System.Text.Json.JsonSerializer.Serialize(new
        {
            operation,
            preparation.CodexHome,
            preparation.Storage.SqliteHome,
            preparation.Storage.SqliteHomeSource,
            preparation.TargetProvider,
            preparation.TargetModel,
            keepCount,
            modelSync = preparation.SwitchPreparation?.ModelSync.Source
        });
        IReadOnlyList<string> automaticDeletionCandidates = await _backupService
            .GetAutomaticPruneDeletionCandidatesAsync(preparation.CodexHome, keepCount);
        return await CoreWriteSnapshotBuilder.BuildAsync(
            operation,
            binding,
            targets,
            automaticDeletionCandidates.Select(static path => new CoreWriteTargetSpec(
                path,
                "delete",
                CoreWriteFingerprintMode.RecursiveInventory)),
            warnings: warnings,
            contentFingerprintHints: useContentFingerprintHints
                ? preparation.WritableChanges
                    .Where(static change => change.ContentFingerprint is not null)
                    .Select(static change => new CoreWriteContentFingerprintHint(
                        change.Path,
                        change.ContentFingerprint!,
                        change.OriginalFileLength,
                        change.OriginalLastWriteTimeUtcTicks))
                : null,
            cancellationToken: cancellationToken);
    }

    private static void ValidateAutomaticRetention(int keepCount)
    {
        if (keepCount < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(keepCount),
                keepCount,
                "keepCount must be 1 or greater for automatic cleanup.");
        }
    }

    private async Task<SyncResult> RunSyncCoreAsync(
        string? explicitCodexHome,
        string? provider,
        string? configBackupText,
        int keepCount,
        int? sqliteBusyTimeoutMs,
        string? model,
        string? explicitSqliteHome,
        Func<string, SwitchPreparation>? switchPreparationFactory,
        CoreWritePlanSnapshot? expectedSnapshot,
        DateTimeOffset? snapshotExpiresAtUtc,
        CancellationToken cancellationToken = default)
    {
        long totalStarted = Stopwatch.GetTimestamp();
        ValidateAutomaticRetention(keepCount);

        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);
        await using LockHandle homeLock = await _lockService.AcquireLockAsync(codexHome, "sync");
        string operationLabel = switchPreparationFactory is null ? "sync" : "switch";
        (StateDbLockResource? lockedStateDb, LockHandle? stateDbHandle) =
            await AcquireCurrentStateDbLockAsync(codexHome, explicitSqliteHome, operationLabel, cancellationToken);
        await using LockHandle? stateDbLock = stateDbHandle;
        await FileTransactionJournal.AssertNoPendingAsync(codexHome);
        long preparationStarted = Stopwatch.GetTimestamp();
        SyncPreparation preparation = await PrepareSyncAsync(
            codexHome,
            provider,
            sqliteBusyTimeoutMs,
            model,
            explicitSqliteHome,
            switchPreparationFactory,
            cancellationToken);
        await AssertStateDbLockMatchesAsync(preparation.Storage, lockedStateDb, cancellationToken);
        long preparationDurationMs = ElapsedMilliseconds(preparationStarted);
        string configPath = preparation.ConfigPath;
        string configText = preparation.ConfigText;
        SwitchPreparation? switchPreparation = preparation.SwitchPreparation;
        CodexStorageLayout storage = preparation.Storage;
        CurrentProviderInfo current = preparation.Current;
        string targetProvider = preparation.TargetProvider;
        string? targetModel = preparation.TargetModel;
        SessionChangeCollection sessionInfo = preparation.SessionInfo;
        IReadOnlyList<ThreadCwdStat> workspaceCwdStats = preparation.WorkspaceCwdStats;
        string? encryptedContentWarning = preparation.EncryptedContentWarning;
        IReadOnlyList<SessionChange> writableChanges = preparation.WritableChanges;
        List<string> skippedRolloutFiles = [.. preparation.SkippedRolloutFiles];
        IReadOnlyList<string> skippedUnreadableRolloutFiles = preparation.SkippedUnreadableRolloutFiles;
        IReadOnlyList<CoreWritePlanTarget>? checkedAutoPruneDeletionTargets = null;
        long checkedPlanValidationDurationMs = 0;
        if (expectedSnapshot is not null)
        {
            long checkedPlanValidationStarted = Stopwatch.GetTimestamp();
            AssertSnapshotFresh(snapshotExpiresAtUtc);
            CoreWritePlanSnapshot actualSnapshot = await BuildSyncPlanSnapshotAsync(
                preparation,
                keepCount,
                cancellationToken,
                useContentFingerprintHints: false);
            CoreWriteSnapshotBuilder.AssertExactMatch(expectedSnapshot, actualSnapshot);
            AssertSnapshotFresh(snapshotExpiresAtUtc);
            checkedAutoPruneDeletionTargets = expectedSnapshot.AutoPruneDeletionTargets;
            checkedPlanValidationDurationMs = ElapsedMilliseconds(checkedPlanValidationStarted);
        }
        cancellationToken.ThrowIfCancellationRequested();
        if (FaultInjector is not null)
        {
            await FaultInjector("before_backup", null, 0);
        }
        string? effectiveConfigBackupText = switchPreparation is null ? configBackupText : configText;
        long backupStarted = Stopwatch.GetTimestamp();
        string backupDir = await _backupService.CreateBackupAsync(
            storage,
            targetProvider,
            writableChanges,
            configPath,
            effectiveConfigBackupText);
        long backupDurationMs = ElapsedMilliseconds(backupStarted);
        long mutationStarted = Stopwatch.GetTimestamp();
        List<SessionChange> appliedSessionChanges = [];
        bool sqliteMutationCommitted = false;
        bool sqliteCommitAttempted = false;
        string globalStatePath = _globalStateService.StatePath(codexHome);
        string globalStateBackupPath = _globalStateService.BackupPath(codexHome);
        string[] potentialTargets = writableChanges.Select(static change => Path.GetFullPath(change.Path))
            .Concat(File.Exists(globalStatePath)
                ? [Path.GetFullPath(globalStatePath), Path.GetFullPath(globalStateBackupPath)]
                : [])
            .Concat(switchPreparation is null ? [] : [Path.GetFullPath(configPath)])
            .Concat(storage.StateDbLocation is null ? [] : [Path.GetFullPath(storage.StateDbLocation.Path)])
            .ToArray();
        await using FileTransactionJournal journal = await FileTransactionJournal.CreateOwnedAsync(
            backupDir,
            codexHome,
            targetProvider,
            potentialTargets);
        List<string> completedTargets = [];
        HashSet<string> observedMutatedTargets = new(PathComparer);
        bool transactionCommitted = false;
        void RecordCompletedTarget(string targetPath)
        {
            string fullPath = Path.GetFullPath(targetPath);
            if (!completedTargets.Contains(fullPath, StringComparer.Ordinal))
            {
                completedTargets.Add(fullPath);
            }
        }
        WorkspaceRootSyncResult workspaceRootResult = new()
        {
            Present = false,
            Updated = false,
            UpdatedWorkspaceRoots = 0,
            SavedWorkspaceRootCount = 0
        };
        try
        {
            if (switchPreparation is not null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await journal.ApplyingAsync("config", configPath);
                if (FaultInjector is not null)
                {
                    await FaultInjector("before_config_apply", configPath, 0);
                }
                await _configFileService.WriteConfigTextAsync(configPath, switchPreparation.NextConfigText);
                observedMutatedTargets.Add(Path.GetFullPath(configPath));
                if (FaultInjector is not null)
                {
                    await FaultInjector("after_config_mutation_before_applied", configPath, 1);
                }
                await journal.AppliedAsync("config", configPath);
                RecordCompletedTarget(configPath);
                if (FaultInjector is not null)
                {
                    await FaultInjector("after_config_apply", configPath, 1);
                }
            }

            SessionApplyResult? applyResult = null;
            string? sqliteTargetPath = storage.StateDbLocation?.Path;
            if (sqliteTargetPath is not null)
            {
                await journal.ApplyingAsync("sqlite", sqliteTargetPath);
            }
            (int updatedRows, int providerRowsUpdated, int modelRowsUpdated, int userEventRowsUpdated, int cwdRowsUpdated, bool databasePresent) = await _sqliteStateService.UpdateSqliteProviderAsync(
                storage,
                targetProvider,
                targetModel,
                async _ =>
                {
                    if (writableChanges.Count > 0)
                    {
                        applyResult = await _sessionRolloutService.ApplySessionChangesAsync(
                            writableChanges,
                            targetModel,
                            async change =>
                            {
                                cancellationToken.ThrowIfCancellationRequested();
                                await journal.ApplyingAsync("rollout", change.Path);
                                if (FaultInjector is not null)
                                {
                                    await FaultInjector(
                                        "before_rollout_apply",
                                        change.Path,
                                        appliedSessionChanges.Count + 1);
                                }
                            },
                            async change =>
                            {
                                observedMutatedTargets.Add(Path.GetFullPath(change.Path));
                                if (FaultInjector is not null)
                                {
                                    await FaultInjector(
                                        "after_rollout_mutation_before_applied",
                                        change.Path,
                                        appliedSessionChanges.Count + 1);
                                }
                                appliedSessionChanges.Add(change);
                                await journal.AppliedAsync("rollout", change.Path);
                                RecordCompletedTarget(change.Path);
                                if (FaultInjector is not null)
                                {
                                    await FaultInjector("after_rollout_apply", change.Path, appliedSessionChanges.Count);
                                }
                            },
                            async change =>
                            {
                                await journal.SkippedAsync("rollout", change.Path);
                            });
                    }
                    workspaceRootResult = await _globalStateService.SyncWorkspaceRootsAsync(
                        storage,
                        workspaceCwdStats,
                        async targetPath =>
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            await journal.ApplyingAsync("globalState", targetPath);
                        },
                        async targetPath =>
                        {
                            observedMutatedTargets.Add(Path.GetFullPath(targetPath));
                            if (FaultInjector is not null)
                            {
                                await FaultInjector("after_global_state_mutation_before_applied", targetPath, 1);
                            }
                            await journal.AppliedAsync("globalState", targetPath);
                            RecordCompletedTarget(targetPath);
                            if (FaultInjector is not null)
                            {
                                await FaultInjector("after_global_state_apply", targetPath, 1);
                            }
                        });
                    cancellationToken.ThrowIfCancellationRequested();
                },
                sqliteBusyTimeoutMs,
                sessionInfo.UserEventThreadIds,
                sessionInfo.ThreadCwdsById,
                result =>
                {
                    sqliteCommitAttempted = result.DatabasePresent && result.UpdatedRows > 0;
                },
                async () =>
                {
                    if (FaultInjector is not null)
                    {
                        await FaultInjector(
                            "after_sqlite_commit_before_ack",
                            sqliteTargetPath ?? storage.SqliteHome,
                            1);
                    }
                });
            sqliteMutationCommitted = databasePresent && updatedRows > 0;
            if (sqliteMutationCommitted)
            {
                observedMutatedTargets.Add(Path.GetFullPath(sqliteTargetPath!));
                RecordCompletedTarget(sqliteTargetPath!);
            }
            cancellationToken.ThrowIfCancellationRequested();
            if (sqliteTargetPath is not null)
            {
                if (FaultInjector is not null)
                {
                    await FaultInjector("after_sqlite_mutation_before_applied", sqliteTargetPath, 1);
                }
                await journal.AppliedAsync("sqlite", sqliteTargetPath);
            }
            if (FaultInjector is not null)
            {
                await FaultInjector("after_sqlite_commit", sqliteTargetPath ?? storage.SqliteHome, 1);
            }
            cancellationToken.ThrowIfCancellationRequested();

            skippedRolloutFiles.AddRange(applyResult?.SkippedPaths ?? []);
            skippedRolloutFiles = skippedRolloutFiles.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();

            cancellationToken.ThrowIfCancellationRequested();
            if (FaultInjector is not null)
            {
                await FaultInjector("before_transaction_commit", null, completedTargets.Count);
            }
            await journal.CommittedAsync();
            transactionCommitted = true;
            await journal.DisposeAsync();
            // The transaction is already committed and every target is on disk.
            // Refreshing the backup inventory only corrects the recorded size
            // and file count in metadata.json, so a failure here must degrade
            // to a warning: throwing would report a successful sync as failed
            // and skip the automatic backup pruning below.
            string? backupInventoryWarning = null;
            try
            {
                await _backupService.RefreshMetadataInventoryAsync(backupDir);
            }
            catch (Exception error)
            {
                backupInventoryWarning = $"Backup inventory refresh failed: {error.Message}";
            }
            long mutationDurationMs = ElapsedMilliseconds(mutationStarted);
            if (FaultInjector is not null)
            {
                await FaultInjector("after_transaction_commit", null, completedTargets.Count);
            }

            BackupPruneResult? autoPruneResult = null;
            string? autoPruneWarning = null;
            long pruneStarted = Stopwatch.GetTimestamp();
            try
            {
                autoPruneResult = await _backupService.PruneAutomaticBackupsAsync(
                    codexHome,
                    keepCount,
                    backupDir,
                    checkedAutoPruneDeletionTargets);
            }
            catch (Exception error)
            {
                autoPruneWarning = $"Automatic backup cleanup failed: {error.Message}";
            }
            long pruneDurationMs = ElapsedMilliseconds(pruneStarted);
            autoPruneWarning = JoinBackupWarnings(backupInventoryWarning, autoPruneWarning);

            SyncResult result = new()
            {
                CodexHome = codexHome,
                SqliteHome = storage.SqliteHome,
                SqliteHomeSource = storage.SqliteHomeSource,
                TargetProvider = targetProvider,
                PreviousProvider = current.Provider ?? AppConstants.DefaultProvider,
                BackupDir = backupDir,
                ChangedSessionFiles = applyResult?.AppliedCount ?? 0,
                SkippedLockedRolloutFiles = skippedRolloutFiles,
                SkippedUnreadableRolloutFiles = skippedUnreadableRolloutFiles,
                SqliteRowsUpdated = updatedRows,
                SqliteProviderRowsUpdated = providerRowsUpdated,
                SqliteModelRowsUpdated = modelRowsUpdated,
                SqliteUserEventRowsUpdated = userEventRowsUpdated,
                SqliteCwdRowsUpdated = cwdRowsUpdated,
                UpdatedWorkspaceRoots = workspaceRootResult.UpdatedWorkspaceRoots,
                SavedWorkspaceRootCount = workspaceRootResult.SavedWorkspaceRootCount,
                SqlitePresent = databasePresent,
                RolloutCountsBefore = sessionInfo.ProviderCounts,
                EncryptedContentCounts = sessionInfo.EncryptedContentCounts,
                EncryptedContentWarning = encryptedContentWarning,
                AutoPruneResult = autoPruneResult,
                AutoPruneWarning = autoPruneWarning,
                ConfigUpdated = switchPreparation is not null,
                ModelSync = switchPreparation?.ModelSync ?? ModelSyncOutcome.NotApplicable(),
                PerformanceMetrics = new SyncPerformanceMetrics
                {
                    TotalDurationMs = ElapsedMilliseconds(totalStarted),
                    PreparationDurationMs = preparationDurationMs,
                    CheckedPlanValidationDurationMs = checkedPlanValidationDurationMs,
                    BackupDurationMs = backupDurationMs,
                    MutationDurationMs = mutationDurationMs,
                    PruneDurationMs = pruneDurationMs,
                    JournalFullValidationCount = journal.AppendFullJournalValidationCount,
                    RolloutScan = sessionInfo.ScanMetrics
                }
            };
            return result;
        }
        catch (Exception error)
        {
            if (transactionCommitted)
            {
                throw;
            }

            List<string> restoreFailures = [];
            bool sqliteRollbackNeeded = sqliteMutationCommitted || sqliteCommitAttempted;
            IReadOnlyList<TransactionTargetInfo> affectedTargets;
            try
            {
                PendingTransactionInfo persisted = await journal.ReadCurrentInfoAsync();
                affectedTargets = persisted.AffectedTargets;
            }
            catch (Exception journalReadError)
            {
                restoreFailures.Add($"transaction journal read: {journalReadError.Message}");
                affectedTargets = BuildConservativeRollbackTargets(
                    writableChanges,
                    switchPreparation is not null ? configPath : null,
                    File.Exists(globalStatePath) ? globalStatePath : null,
                    File.Exists(globalStateBackupPath) ? globalStateBackupPath : null,
                    sqliteRollbackNeeded ? storage.StateDbLocation?.Path : null);
            }
            try
            {
                await journal.RollingBackAsync(error);
            }
            catch (Exception journalError)
            {
                restoreFailures.Add($"transaction journal: {journalError.Message}");
            }
            restoreFailures.AddRange(await RollBackTargetsAsync(
                affectedTargets
                    .Where(target => target.Kind != "sqlite" || sqliteRollbackNeeded)
                    .ToArray(),
                backupDir,
                codexHome,
                storage));

            if (restoreFailures.Count == 0)
            {
                try
                {
                    await journal.RolledBackAsync();
                }
                catch (Exception journalError)
                {
                    restoreFailures.Add($"transaction journal: {journalError.Message}");
                }
            }
            if (restoreFailures.Count > 0)
            {
                try
                {
                    await journal.RecoveryRequiredAsync(error, restoreFailures);
                }
                catch
                {
                    // Preserve the original and rollback failures when the
                    // journal itself is no longer writable.
                }
                await TryRefreshBackupInventoryAsync(backupDir);
                IReadOnlyList<string> reportedCompletedTargets = BuildReportedCompletedTargets(
                    completedTargets,
                    observedMutatedTargets);
                HashSet<string> completedTargetSet = new(reportedCompletedTargets, PathComparer);
                IReadOnlyList<string> uncompletedTargets = potentialTargets
                    .Where(targetPath => !completedTargetSet.Contains(targetPath))
                    .ToArray();
                throw new SyncTransactionException(
                    error,
                    restoreFailures,
                    backupDir,
                    reportedCompletedTargets,
                    uncompletedTargets,
                    rollbackStatus: "incomplete",
                    recoveryRequired: true);
            }

            await TryRefreshBackupInventoryAsync(backupDir);
            IReadOnlyList<string> completedAfterRollback = BuildReportedCompletedTargets(
                completedTargets,
                observedMutatedTargets);
            HashSet<string> completedSet = new(completedAfterRollback, PathComparer);
            throw new SyncTransactionException(
                error,
                [],
                backupDir,
                completedAfterRollback,
                potentialTargets.Where(targetPath => !completedSet.Contains(targetPath)).ToArray(),
                rollbackStatus: "complete",
                recoveryRequired: false);
        }
    }

    private async Task<IReadOnlyList<string>> RollBackTargetsAsync(
        IReadOnlyList<TransactionTargetInfo> affectedTargets,
        string backupDir,
        string codexHome,
        CodexStorageLayout storage)
    {
        List<string> failures = [];
        Dictionary<string, SessionBackupManifestEntry>? sessionEntries = null;
        HashSet<string> restoredTargets = new(PathComparer);
        foreach (TransactionTargetInfo target in affectedTargets.Reverse())
        {
            string normalizedTarget = Path.GetFullPath(target.TargetPath);
            string targetKey = target.Kind + "\0" + normalizedTarget;
            if (!restoredTargets.Add(targetKey))
            {
                continue;
            }

            try
            {
                switch (target.Kind)
                {
                    case "rollout":
                        if (FaultInjector is not null)
                        {
                            await FaultInjector("before_rollout_rollback", normalizedTarget, 1);
                        }
                        if (sessionEntries is null)
                        {
                            sessionEntries = (await _backupService.ReadSessionBackupEntriesAsync(backupDir, codexHome))
                                .ToDictionary(
                                    static entry => Path.GetFullPath(entry.Path),
                                    static entry => entry,
                                    PathComparer);
                        }
                        if (!sessionEntries.TryGetValue(normalizedTarget, out SessionBackupManifestEntry? entry))
                        {
                            throw new InvalidOperationException(
                                $"Immutable session backup does not contain rollback target {normalizedTarget}.");
                        }
                        await _sessionRolloutService.RestoreSessionChangesAsync([entry]);
                        break;

                    case "globalState":
                        if (FaultInjector is not null)
                        {
                            await FaultInjector("before_global_state_rollback", normalizedTarget, 1);
                        }
                        await _backupService.RestoreGlobalStateTargetAsync(backupDir, codexHome, normalizedTarget);
                        break;

                    case "config":
                        if (FaultInjector is not null)
                        {
                            await FaultInjector("before_config_rollback", normalizedTarget, 1);
                        }
                        await _backupService.RestoreConfigFileAsync(backupDir, codexHome);
                        break;

                    case "sqlite":
                        if (FaultInjector is not null)
                        {
                            await FaultInjector("before_sqlite_rollback", normalizedTarget, 1);
                        }
                        await _backupService.RestoreBackupAsync(
                            backupDir,
                            storage,
                            new RestoreBackupOptions
                            {
                                RestoreConfig = false,
                                RestoreDatabase = true,
                                RestoreSessions = false
                            });
                        break;

                    default:
                        throw new InvalidOperationException(
                            $"Unsupported transaction rollback target kind \"{target.Kind}\".");
                }
            }
            catch (Exception restoreError)
            {
                failures.Add($"{target.Kind} {normalizedTarget}: {restoreError.Message}");
            }
        }
        return failures;
    }

    private static IReadOnlyList<TransactionTargetInfo> BuildConservativeRollbackTargets(
        IReadOnlyList<SessionChange> writableChanges,
        string? configPath,
        string? globalStatePath,
        string? globalStateBackupPath,
        string? sqlitePath)
    {
        List<TransactionTargetInfo> targets = writableChanges
            .Select(static change => new TransactionTargetInfo("rollout", Path.GetFullPath(change.Path), "applying"))
            .ToList();
        if (globalStatePath is not null)
        {
            targets.Add(new TransactionTargetInfo("globalState", Path.GetFullPath(globalStatePath), "applying"));
        }
        if (globalStateBackupPath is not null)
        {
            targets.Add(new TransactionTargetInfo("globalState", Path.GetFullPath(globalStateBackupPath), "applying"));
        }
        if (sqlitePath is not null)
        {
            targets.Add(new TransactionTargetInfo("sqlite", Path.GetFullPath(sqlitePath), "applying"));
        }
        if (configPath is not null)
        {
            targets.Add(new TransactionTargetInfo("config", Path.GetFullPath(configPath), "applying"));
        }
        return targets;
    }

    private static IReadOnlyList<string> BuildReportedCompletedTargets(
        IEnumerable<string> journaledCompletedTargets,
        IEnumerable<string> observedMutatedTargets)
    {
        List<string> result = [];
        foreach (string target in journaledCompletedTargets.Concat(observedMutatedTargets))
        {
            string fullPath = Path.GetFullPath(target);
            if (!result.Contains(fullPath, PathComparer))
            {
                result.Add(fullPath);
            }
        }
        return result;
    }

    public async Task<SyncResult> RunSwitchAsync(
        string? explicitCodexHome,
        string provider,
        int keepCount = AppConstants.DefaultBackupRetentionCount,
        string? model = null,
        bool keepRootModel = false,
        string? explicitSqliteHome = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(provider))
        {
            throw new InvalidOperationException("Missing provider id. Usage: codex-provider switch <provider-id>");
        }

        return await RunSyncCoreAsync(
            explicitCodexHome,
            provider: null,
            configBackupText: null,
            keepCount,
            sqliteBusyTimeoutMs: null,
            model: null,
            explicitSqliteHome,
            switchPreparationFactory: originalConfigText => CreateSwitchPreparation(
                originalConfigText,
                provider,
                model,
                keepRootModel),
            expectedSnapshot: null,
            snapshotExpiresAtUtc: null,
            cancellationToken);
    }

    public async Task<CoreWritePlanSnapshot> CreateSwitchPlanSnapshotAsync(
        string? explicitCodexHome,
        string provider,
        int keepCount = AppConstants.DefaultBackupRetentionCount,
        string? model = null,
        bool keepRootModel = false,
        string? explicitSqliteHome = null,
        CancellationToken cancellationToken = default)
    {
        ValidateSwitchProvider(provider);
        ValidateAutomaticRetention(keepCount);
        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);
        await FileTransactionJournal.AssertNoPendingAsync(codexHome);
        SyncPreparation preparation = await PrepareSyncAsync(
            codexHome,
            provider: null,
            sqliteBusyTimeoutMs: null,
            model: null,
            explicitSqliteHome,
            originalConfigText => CreateSwitchPreparation(
                originalConfigText,
                provider,
                model,
                keepRootModel),
            cancellationToken);
        return await BuildSyncPlanSnapshotAsync(preparation, keepCount, cancellationToken);
    }

    public Task<SyncResult> RunSwitchCheckedAsync(
        CoreWritePlanSnapshot expectedSnapshot,
        string? explicitCodexHome,
        string provider,
        int keepCount = AppConstants.DefaultBackupRetentionCount,
        string? model = null,
        bool keepRootModel = false,
        string? explicitSqliteHome = null,
        DateTimeOffset? snapshotExpiresAtUtc = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(expectedSnapshot);
        ValidateSwitchProvider(provider);
        return RunSyncCoreAsync(
            explicitCodexHome,
            provider: null,
            configBackupText: null,
            keepCount,
            sqliteBusyTimeoutMs: null,
            model: null,
            explicitSqliteHome,
            originalConfigText => CreateSwitchPreparation(
                originalConfigText,
                provider,
                model,
                keepRootModel),
            expectedSnapshot,
            snapshotExpiresAtUtc,
            cancellationToken);
    }

    private SwitchPreparation CreateSwitchPreparation(
        string originalConfigText,
        string provider,
        string? model,
        bool keepRootModel)
    {
        if (!_configFileService.ConfigDeclaresProvider(originalConfigText, provider))
        {
            string configuredProviders = string.Join(", ", _configFileService.ListConfiguredProviderIds(originalConfigText));
            throw new InvalidOperationException(
                $"Provider \"{provider}\" is not available in config.toml. Configure it first or use one of: {configuredProviders}");
        }

        string nextConfigText = _configFileService.SetRootProviderInConfigText(originalConfigText, provider);
        ModelSyncOutcome modelSync = ResolveModelSyncOutcome(
            originalConfigText,
            provider,
            model,
            keepRootModel);
        if (modelSync.Applied)
        {
            nextConfigText = _configFileService.SetRootModelInConfigText(nextConfigText, modelSync.Model!);
        }

        // Even when the switch keeps the existing root model, keep SQLite and
        // rollout turn_context fields aligned with it.
        string? modelForThreads = modelSync.Applied
            ? modelSync.Model
            : _configFileService.ReadRootModelFromConfigText(nextConfigText);
        return new SwitchPreparation(provider, nextConfigText, modelForThreads, modelSync);
    }

    private static void ValidateSwitchProvider(string provider)
    {
        if (string.IsNullOrWhiteSpace(provider))
        {
            throw new InvalidOperationException("Missing provider id. Usage: codex-provider switch <provider-id>");
        }
    }

    private ModelSyncOutcome ResolveModelSyncOutcome(
        string originalConfigText,
        string provider,
        string? model,
        bool keepRootModel)
    {
        if (model is not null)
        {
            if (model.Length == 0)
            {
                throw new ArgumentException(
                    "Invalid --model value. Expected a non-empty string.",
                    nameof(model));
            }
            return ModelSyncOutcome.CreateApplied("explicit", model);
        }

        if (keepRootModel)
        {
            return ModelSyncOutcome.CreateSkipped("keep-root-model", warning: null);
        }

        string? providerModel = _configFileService.ReadProviderModel(originalConfigText, provider);
        if (providerModel is not null)
        {
            return ModelSyncOutcome.CreateApplied("provider-section", providerModel);
        }

        if (!string.Equals(provider, AppConstants.DefaultProvider, StringComparison.Ordinal))
        {
            return ModelSyncOutcome.CreateSkipped(
                "none",
                warning: $"Provider \"{provider}\" has no model field in [model_providers.{provider}]; root-level model left unchanged. Use --model <name> to set it explicitly, or pass keepRootModel to suppress this warning.");
        }

        return ModelSyncOutcome.CreateSkipped("none", warning: null);
    }

    public async Task<RestoreResult> RunRestoreAsync(
        string? explicitCodexHome,
        string backupDir,
        string? explicitSqliteHome = null)
    {
        return await RunRestoreAsync(explicitCodexHome, backupDir, new RestoreBackupOptions(), explicitSqliteHome);
    }

    public async Task<RestoreResult> RunRestoreAsync(
        string? explicitCodexHome,
        string backupDir,
        RestoreBackupOptions options,
        string? explicitSqliteHome = null)
    {
        return await RunRestoreCoreAsync(
            expectedSnapshot: null,
            explicitCodexHome,
            backupDir,
            options,
            explicitSqliteHome,
            snapshotExpiresAtUtc: null,
            CancellationToken.None);
    }

    public async Task<CoreWritePlanSnapshot> CreateRestorePlanSnapshotAsync(
        string? explicitCodexHome,
        string backupDir,
        RestoreBackupOptions options,
        string? explicitSqliteHome = null,
        CancellationToken cancellationToken = default)
    {
        ValidateRestoreRequest(backupDir, options);
        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);
        RestorePreparation preparation = await PrepareRestoreAsync(
            codexHome,
            backupDir,
            options,
            explicitSqliteHome,
            cancellationToken);
        return await BuildRestorePlanSnapshotAsync(preparation, cancellationToken);
    }

    public Task<RestoreResult> RunRestoreCheckedAsync(
        CoreWritePlanSnapshot expectedSnapshot,
        string? explicitCodexHome,
        string backupDir,
        RestoreBackupOptions options,
        string? explicitSqliteHome = null,
        DateTimeOffset? snapshotExpiresAtUtc = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(expectedSnapshot);
        return RunRestoreCoreAsync(
            expectedSnapshot,
            explicitCodexHome,
            backupDir,
            options,
            explicitSqliteHome,
            snapshotExpiresAtUtc,
            cancellationToken);
    }

    private async Task<RestoreResult> RunRestoreCoreAsync(
        CoreWritePlanSnapshot? expectedSnapshot,
        string? explicitCodexHome,
        string backupDir,
        RestoreBackupOptions options,
        string? explicitSqliteHome,
        DateTimeOffset? snapshotExpiresAtUtc,
        CancellationToken cancellationToken)
    {
        ValidateRestoreRequest(backupDir, options);
        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);

        await using LockHandle homeLock = await _lockService.AcquireLockAsync(codexHome, "restore");
        StateDbLockResource? lockedStateDb = null;
        LockHandle? stateDbHandle = null;
        if (options.RestoreDatabase)
        {
            string lockConfigText = await _configFileService.ReadConfigTextAsync(
                _codexHomeService.ConfigPath(codexHome));
            CodexStorageLayout lockStorage = await PrepareStorageAsync(
                codexHome,
                explicitSqliteHome,
                lockConfigText);
            lockStorage.EnsureSqliteAccessSupported("restore");
            string stateDbTargetPath = await _backupService.ResolveRestoreStateDbTargetPathAsync(
                backupDir,
                lockStorage,
                options);
            lockedStateDb = await StateDbLockResource.ResolveAsync(
                stateDbTargetPath,
                cancellationToken);
            stateDbHandle = await _lockService.AcquireStateDbLockAsync(lockedStateDb, "restore");
        }
        await using LockHandle? stateDbLock = stateDbHandle;
        RestorePreparation preparation = await PrepareRestoreAsync(
            codexHome,
            backupDir,
            options,
            explicitSqliteHome,
            cancellationToken);
        await AssertStateDbTargetLockMatchesAsync(
            preparation.StateDbTargetPath,
            lockedStateDb,
            cancellationToken);
        if (expectedSnapshot is not null)
        {
            AssertSnapshotFresh(snapshotExpiresAtUtc);
            CoreWritePlanSnapshot actualSnapshot = await BuildRestorePlanSnapshotAsync(
                preparation,
                cancellationToken);
            CoreWriteSnapshotBuilder.AssertExactMatch(expectedSnapshot, actualSnapshot);
            AssertSnapshotFresh(snapshotExpiresAtUtc);
        }
        // BackupService currently exposes an atomic restore operation rather
        // than per-file cancellation. Honor cancellation until the mutation
        // boundary, then let that authoritative operation finish and report
        // its real result instead of returning a false cancelled outcome.
        cancellationToken.ThrowIfCancellationRequested();
        RestoreResult result = await _backupService.RestoreBackupAsync(
            preparation.BackupDirectory,
            preparation.Storage,
            preparation.Options,
            expectedStateDbTargetPath: preparation.StateDbTargetPath);
        await FileTransactionJournal.MarkBackupRolledBackAsync(
            preparation.BackupDirectory,
            codexHome,
            result.TargetProvider);
        // The restore and its journal marker are already durable. Refreshing the
        // inventory only corrects metadata.json bookkeeping, so surface a
        // failure as a warning instead of reporting a completed restore as
        // failed.
        try
        {
            await _backupService.RefreshMetadataInventoryAsync(preparation.BackupDirectory);
            return result;
        }
        catch (Exception error)
        {
            return new RestoreResult
            {
                CodexHome = result.CodexHome,
                BackupDir = result.BackupDir,
                TargetProvider = result.TargetProvider,
                CreatedAt = result.CreatedAt,
                ChangedSessionFiles = result.ChangedSessionFiles,
                BackupInventoryWarning = $"Backup inventory refresh failed: {error.Message}"
            };
        }
    }

    private async Task<RestorePreparation> PrepareRestoreAsync(
        string codexHome,
        string backupDir,
        RestoreBackupOptions options,
        string? explicitSqliteHome,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string configPath = _codexHomeService.ConfigPath(codexHome);
        string configText = await _configFileService.ReadConfigTextAsync(configPath);
        CodexStorageLayout storage = await PrepareStorageAsync(codexHome, explicitSqliteHome, configText);
        storage.EnsureSqliteAccessSupported("restore");
        string normalizedBackupDir = Path.GetFullPath(backupDir);
        string? stateDbTargetPath = options.RestoreDatabase
            ? await _backupService.ResolveRestoreStateDbTargetPathAsync(
                normalizedBackupDir,
                storage,
                options)
            : null;
        IReadOnlyList<PendingTransactionInfo> pending = await FileTransactionJournal.FindPendingAsync(codexHome);
        PendingTransactionInfo[] foreignPending = pending
            .Where(transaction => !PathComparer.Equals(
                Path.GetFullPath(transaction.BackupDir),
                normalizedBackupDir))
            .ToArray();
        if (foreignPending.Length > 0)
        {
            string backups = string.Join(", ", foreignPending.Select(static item => item.BackupDir));
            throw new RecoveryRequiredException(
                "A different unfinished provider-sync transaction must be restored before this backup can be used. "
                + $"Restore the bound backup first: {backups}",
                foreignPending);
        }
        await EnsurePendingRecoveryCoverageAsync(normalizedBackupDir, codexHome, options);
        cancellationToken.ThrowIfCancellationRequested();
        return new RestorePreparation(
            codexHome,
            configPath,
            normalizedBackupDir,
            options,
            storage,
            stateDbTargetPath);
    }

    private async Task<CoreWritePlanSnapshot> BuildRestorePlanSnapshotAsync(
        RestorePreparation preparation,
        CancellationToken cancellationToken)
    {
        List<CoreWriteTargetSpec> targets =
        [
            new(preparation.BackupDirectory, "read-restore-source"),
            new(preparation.ConfigPath, preparation.Options.RestoreConfig ? "restore" : "read")
        ];
        if (preparation.Options.RestoreConfig)
        {
            targets.Add(new CoreWriteTargetSpec(
                _globalStateService.StatePath(preparation.CodexHome),
                "restore"));
            targets.Add(new CoreWriteTargetSpec(
                _globalStateService.BackupPath(preparation.CodexHome),
                "restore"));
        }
        if (preparation.Options.RestoreSessions)
        {
            targets.Add(new CoreWriteTargetSpec(
                Path.Combine(preparation.CodexHome, "sessions"),
                "restore",
                CoreWriteFingerprintMode.RecursiveInventory));
            targets.Add(new CoreWriteTargetSpec(
                Path.Combine(preparation.CodexHome, "archived_sessions"),
                "restore",
                CoreWriteFingerprintMode.RecursiveInventory));
        }
        if (preparation.Options.RestoreDatabase)
        {
            string databasePath = preparation.StateDbTargetPath
                ?? throw new InvalidOperationException("Restore preparation did not resolve a State DB target.");
            targets.Add(new CoreWriteTargetSpec(
                databasePath,
                "restore",
                CoreWriteFingerprintMode.SqliteMainContent));
            targets.Add(new CoreWriteTargetSpec(
                databasePath + "-wal",
                "restore-if-present",
                CoreWriteFingerprintMode.SqliteWalContent));
        }

        string binding = System.Text.Json.JsonSerializer.Serialize(new
        {
            operation = "restore",
            preparation.CodexHome,
            preparation.BackupDirectory,
            preparation.Storage.SqliteHome,
            preparation.Storage.SqliteHomeSource,
            preparation.Options.RestoreConfig,
            preparation.Options.RestoreDatabase,
            preparation.Options.RestoreSessions,
            preparation.Options.AllowSqliteHomeRelocation
        });
        return await CoreWriteSnapshotBuilder.BuildAsync(
            "restore",
            binding,
            targets,
            cancellationToken: cancellationToken);
    }

    private static void ValidateRestoreRequest(string backupDir, RestoreBackupOptions options)
    {
        if (string.IsNullOrWhiteSpace(backupDir))
        {
            throw new InvalidOperationException("Missing backup path. Usage: codex-provider restore <backup-dir>");
        }
        ArgumentNullException.ThrowIfNull(options);
    }

    private async Task EnsurePendingRecoveryCoverageAsync(
        string backupDir,
        string codexHome,
        RestoreBackupOptions options)
    {
        string journalPath = Path.Combine(backupDir, FileTransactionJournal.FileName);
        if (!File.Exists(journalPath))
        {
            return;
        }

        PendingTransactionInfo journal = await FileTransactionJournal.ReadInfoAsync(journalPath);
        if (journal.Terminal)
        {
            return;
        }

        bool requireConfig;
        bool requireDatabase;
        bool requireSessions;
        if (journal.InvalidTail || string.IsNullOrWhiteSpace(journal.OperationId))
        {
            BackupRecoveryCoverage coverage = await _backupService.GetRecoveryCoverageAsync(backupDir, codexHome);
            requireConfig = coverage.Config;
            requireDatabase = coverage.Database;
            requireSessions = coverage.Sessions;
        }
        else
        {
            requireConfig = journal.AffectedTargets.Any(
                static target => target.Kind is "config" or "globalState");
            requireDatabase = journal.AffectedTargets.Any(static target => target.Kind == "sqlite");
            requireSessions = journal.AffectedTargets.Any(static target => target.Kind == "rollout");
        }

        List<string> missing = [];
        if (requireConfig && !options.RestoreConfig)
        {
            missing.Add("config/global state");
        }
        if (requireDatabase && !options.RestoreDatabase)
        {
            missing.Add("SQLite");
        }
        if (requireSessions && !options.RestoreSessions)
        {
            missing.Add("rollout sessions");
        }
        if (missing.Count > 0)
        {
            throw new InvalidOperationException(
                "Cannot resolve the pending transaction with a partial restore. "
                + $"Enable restore for: {string.Join(", ", missing)}. The recovery journal remains pending.");
        }
    }

    public Task<BackupPruneResult> RunPruneBackupsAsync(
        string? explicitCodexHome = null,
        int keepCount = AppConstants.DefaultBackupRetentionCount)
    {
        return RunPruneBackupsCoreAsync(
            expectedSnapshot: null,
            explicitCodexHome,
            keepCount,
            snapshotExpiresAtUtc: null,
            CancellationToken.None);
    }

    public async Task<CoreWritePlanSnapshot> CreatePrunePlanSnapshotAsync(
        string? explicitCodexHome = null,
        int keepCount = AppConstants.DefaultBackupRetentionCount,
        CancellationToken cancellationToken = default)
    {
        ValidatePruneRetention(keepCount);
        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);
        await using LockHandle _ = await _lockService.AcquireLockAsync(codexHome, "plan-prune-backups");
        return await BuildPrunePlanSnapshotAsync(codexHome, keepCount, cancellationToken);
    }

    public Task<BackupPruneResult> RunPruneBackupsCheckedAsync(
        CoreWritePlanSnapshot expectedSnapshot,
        string? explicitCodexHome = null,
        int keepCount = AppConstants.DefaultBackupRetentionCount,
        DateTimeOffset? snapshotExpiresAtUtc = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(expectedSnapshot);
        return RunPruneBackupsCoreAsync(
            expectedSnapshot,
            explicitCodexHome,
            keepCount,
            snapshotExpiresAtUtc,
            cancellationToken);
    }

    private async Task<BackupPruneResult> RunPruneBackupsCoreAsync(
        CoreWritePlanSnapshot? expectedSnapshot,
        string? explicitCodexHome,
        int keepCount,
        DateTimeOffset? snapshotExpiresAtUtc,
        CancellationToken cancellationToken)
    {
        ValidatePruneRetention(keepCount);
        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);

        await using LockHandle _ = await _lockService.AcquireLockAsync(codexHome, "prune-backups");
        if (expectedSnapshot is not null)
        {
            AssertSnapshotFresh(snapshotExpiresAtUtc);
            CoreWritePlanSnapshot actualSnapshot = await BuildPrunePlanSnapshotAsync(
                codexHome,
                keepCount,
                cancellationToken);
            CoreWriteSnapshotBuilder.AssertExactMatch(expectedSnapshot, actualSnapshot);
            AssertSnapshotFresh(snapshotExpiresAtUtc);
        }
        cancellationToken.ThrowIfCancellationRequested();
        return await _backupService.PruneBackupsAsync(codexHome, keepCount);
    }

    private Task<CoreWritePlanSnapshot> BuildPrunePlanSnapshotAsync(
        string codexHome,
        int keepCount,
        CancellationToken cancellationToken)
    {
        string backupRoot = _codexHomeService.BackupRoot(codexHome);
        string binding = System.Text.Json.JsonSerializer.Serialize(new
        {
            operation = "prune",
            codexHome,
            keepCount
        });
        return CoreWriteSnapshotBuilder.BuildAsync(
            "prune",
            binding,
            [new CoreWriteTargetSpec(
                backupRoot,
                $"prune-to-{keepCount}",
                CoreWriteFingerprintMode.RecursiveInventory)],
            cancellationToken: cancellationToken);
    }

    private static void ValidatePruneRetention(int keepCount)
    {
        if (keepCount < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(keepCount),
                keepCount,
                "keepCount must be 0 or greater.");
        }
    }

    public Task<BackupStorageInfo> GetBackupStorageInfoAsync(string backupDir)
    {
        return _backupService.GetBackupStorageInfoAsync(backupDir);
    }

    private static string? BuildEncryptedContentWarning(ProviderCounts encryptedContentCounts, string targetProvider)
    {
        int total = encryptedContentCounts.Sessions.Values.Sum() + encryptedContentCounts.ArchivedSessions.Values.Sum();
        List<string> riskyProviders = encryptedContentCounts.Sessions
            .Concat(encryptedContentCounts.ArchivedSessions)
            .Where(pair => pair.Value > 0 && !string.Equals(pair.Key, targetProvider, StringComparison.Ordinal))
            .Select(static pair => pair.Key)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToList();

        if (riskyProviders.Count == 0)
        {
            return null;
        }

        return $"Encrypted content warning: {total} rollout file(s) contain encrypted_content from provider(s) {string.Join(", ", riskyProviders)}. Visibility metadata can be synchronized to {targetProvider}, but continuing or compacting those histories may fail with invalid_encrypted_content. Return to the original provider/account or start a new session if you need reliable continuation.";
    }

    private static long ElapsedMilliseconds(long started) =>
        (long)Math.Round(Stopwatch.GetElapsedTime(started).TotalMilliseconds);

    /// <summary>
    /// Rewrites the retained backup's recorded size and file count after the
    /// journal reached a terminal state, so status and pruning do not trust an
    /// inventory captured before those journal records existed. Used on the
    /// rollback paths, where the caller is already reporting a failure: a
    /// bookkeeping problem here must never replace the original error.
    /// </summary>
    private async Task TryRefreshBackupInventoryAsync(string backupDir)
    {
        try
        {
            await _backupService.RefreshMetadataInventoryAsync(backupDir);
        }
        catch
        {
            // The original sync failure and its rollback details are the
            // authoritative diagnosis and must reach the caller unchanged.
        }
    }

    private static string? JoinBackupWarnings(string? first, string? second)
    {
        string[] parts = new[] { first, second }
            .Where(static part => !string.IsNullOrWhiteSpace(part))
            .Select(static part => part!.Trim())
            .ToArray();
        return parts.Length == 0 ? null : string.Join(" | ", parts);
    }

    private async Task<CodexStorageLayout> PrepareStorageAsync(
        string codexHome,
        string? explicitSqliteHome,
        string configText)
    {
        CodexStorageLayout storage = _storageLayoutService.Resolve(
            codexHome,
            explicitSqliteHome,
            configText,
            explicitSource: "gui");
        StateDbLocation? stateDb = storage.SqliteAccess.Supported
            ? _sqliteStateService.DetectStateDb(storage)
            : null;
        return storage with { StateDbLocation = stateDb };
    }

    private async Task<(StateDbLockResource? Resource, LockHandle? Handle)> AcquireCurrentStateDbLockAsync(
        string codexHome,
        string? explicitSqliteHome,
        string label,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string configText = await _configFileService.ReadConfigTextAsync(
            _codexHomeService.ConfigPath(codexHome));
        CodexStorageLayout storage = await PrepareStorageAsync(codexHome, explicitSqliteHome, configText);
        storage.EnsureSqliteAccessSupported(label);
        EnsureWritableStorage(storage);
        if (storage.StateDbLocation is null)
        {
            return (null, null);
        }
        StateDbLockResource resource = await StateDbLockResource.ResolveAsync(
            storage.StateDbLocation.Path,
            cancellationToken);
        LockHandle handle = await _lockService.AcquireStateDbLockAsync(resource, label);
        return (resource, handle);
    }

    private static async Task AssertStateDbLockMatchesAsync(
        CodexStorageLayout storage,
        StateDbLockResource? lockedResource,
        CancellationToken cancellationToken)
    {
        if (storage.StateDbLocation is null)
        {
            if (lockedResource is null) return;
            throw StateDbLockChanged();
        }
        StateDbLockResource current = await StateDbLockResource.ResolveAsync(
            storage.StateDbLocation.Path,
            cancellationToken);
        if (lockedResource is null
            || !string.Equals(current.ResourceKey, lockedResource.ResourceKey, StringComparison.Ordinal))
        {
            throw StateDbLockChanged();
        }
    }

    private static async Task AssertStateDbTargetLockMatchesAsync(
        string? stateDbTargetPath,
        StateDbLockResource? lockedResource,
        CancellationToken cancellationToken)
    {
        if (stateDbTargetPath is null)
        {
            if (lockedResource is null) return;
            throw StateDbLockChanged();
        }
        StateDbLockResource current = await StateDbLockResource.ResolveAsync(
            stateDbTargetPath,
            cancellationToken);
        if (lockedResource is null
            || !string.Equals(current.ResourceKey, lockedResource.ResourceKey, StringComparison.Ordinal))
        {
            throw StateDbLockChanged();
        }
    }

    private static InvalidOperationException StateDbLockChanged()
    {
        InvalidOperationException error = new(
            "The resolved State DB resource changed while the write operation was acquiring its locks.");
        error.Data["codex-provider-sync/error-code"] = LockService.LockUnverifiableErrorCode;
        error.Data["codex-provider-sync/lock-scope"] = "state-db";
        return error;
    }

    private static void EnsureWritableStorage(CodexStorageLayout storage)
    {
        if (storage.StateDbLocation is null && storage.HasConfiguredSqliteHome)
        {
            throw new InvalidOperationException(
                $"state_5.sqlite not found in configured SQLite home {storage.SqliteHome} "
                + $"(source: {storage.SqliteHomeSource}).");
        }
    }

    private static void AssertSnapshotFresh(DateTimeOffset? expiresAtUtc)
    {
        if (expiresAtUtc is not null && DateTimeOffset.UtcNow >= expiresAtUtc.Value)
        {
            throw new CoreWritePlanExpiredException();
        }
    }

    private static StringComparer PathComparer => OperatingSystem.IsWindows()
        ? StringComparer.OrdinalIgnoreCase
        : StringComparer.Ordinal;

    private sealed record SwitchPreparation(
        string Provider,
        string NextConfigText,
        string? ThreadModel,
        ModelSyncOutcome ModelSync);

    private sealed record SyncPreparation(
        string CodexHome,
        string ConfigPath,
        string ConfigText,
        SwitchPreparation? SwitchPreparation,
        CodexStorageLayout Storage,
        CurrentProviderInfo Current,
        string TargetProvider,
        string? TargetModel,
        SessionChangeCollection SessionInfo,
        IReadOnlyList<ThreadCwdStat> WorkspaceCwdStats,
        string? EncryptedContentWarning,
        IReadOnlyList<SessionChange> WritableChanges,
        IReadOnlyList<string> SkippedRolloutFiles,
        IReadOnlyList<string> SkippedUnreadableRolloutFiles);

    private sealed record RestorePreparation(
        string CodexHome,
        string ConfigPath,
        string BackupDirectory,
        RestoreBackupOptions Options,
        CodexStorageLayout Storage,
        string? StateDbTargetPath);
}
