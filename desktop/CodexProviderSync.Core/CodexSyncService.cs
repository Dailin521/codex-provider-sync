namespace CodexProviderSync.Core;

public sealed class CodexSyncService
{
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
        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);
        string configText = await _configFileService.ReadConfigTextAsync(_codexHomeService.ConfigPath(codexHome));
        CodexStorageLayout storage = await PrepareStorageAsync(codexHome, explicitSqliteHome, configText);
        CurrentProviderInfo currentProvider = _configFileService.ReadCurrentProviderFromConfigText(configText);
        IReadOnlyList<string> configuredProviders = _configFileService.ListConfiguredProviderIds(configText);
        SessionChangeCollection rolloutInfo = await _sessionRolloutService.CollectSessionChangesAsync(codexHome, "__status_only__", skipLockedReads: true);
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
        BackupSummary backupSummary = await _backupService.GetBackupSummaryAsync(codexHome);
        IReadOnlyList<PendingTransactionInfo> pendingTransactions = await FileTransactionJournal.FindPendingAsync(codexHome);

        return new StatusSnapshot
        {
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
                .ToArray()
        };
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
            cancellationToken);
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
        CancellationToken cancellationToken = default)
    {
        if (keepCount < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(keepCount), keepCount, "keepCount must be 1 or greater for automatic cleanup.");
        }

        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);
        await using LockHandle _ = await _lockService.AcquireLockAsync(codexHome, "sync");
        await FileTransactionJournal.AssertNoPendingAsync(codexHome);
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
        if (FaultInjector is not null)
        {
            await FaultInjector("before_backup", null, 0);
        }
        string? effectiveConfigBackupText = switchPreparation is null ? configBackupText : configText;
        string backupDir = await _backupService.CreateBackupAsync(
            storage,
            targetProvider,
            writableChanges,
            configPath,
            effectiveConfigBackupText);
        List<SessionChange> appliedSessionChanges = [];
        bool sqliteMutationCommitted = false;
        string globalStatePath = _globalStateService.StatePath(codexHome);
        string globalStateBackupPath = _globalStateService.BackupPath(codexHome);
        string[] potentialTargets = writableChanges.Select(static change => Path.GetFullPath(change.Path))
            .Concat(File.Exists(globalStatePath)
                ? [Path.GetFullPath(globalStatePath), Path.GetFullPath(globalStateBackupPath)]
                : [])
            .Concat(switchPreparation is null ? [] : [Path.GetFullPath(configPath)])
            .Concat(storage.StateDbLocation is null ? [] : [Path.GetFullPath(storage.StateDbLocation.Path)])
            .ToArray();
        FileTransactionJournal journal = await FileTransactionJournal.CreateAsync(
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
                sessionInfo.ThreadCwdsById);
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
            if (FaultInjector is not null)
            {
                await FaultInjector("after_transaction_commit", null, completedTargets.Count);
            }

            BackupPruneResult? autoPruneResult = null;
            string? autoPruneWarning = null;
            try
            {
                autoPruneResult = await _backupService.PruneBackupsAsync(codexHome, keepCount);
            }
            catch (Exception error)
            {
                autoPruneWarning = $"Automatic backup cleanup failed: {error.Message}";
            }

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
                ModelSync = switchPreparation?.ModelSync ?? ModelSyncOutcome.NotApplicable()
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
                    sqliteMutationCommitted ? storage.StateDbLocation?.Path : null);
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
                    .Where(target => target.Kind != "sqlite" || sqliteMutationCommitted)
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
            switchPreparationFactory: originalConfigText =>
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

                // Even when the switch keeps the existing root model, keep
                // SQLite and rollout turn_context fields aligned with it.
                string? modelForThreads = modelSync.Applied
                    ? modelSync.Model
                    : _configFileService.ReadRootModelFromConfigText(nextConfigText);
                return new SwitchPreparation(provider, nextConfigText, modelForThreads, modelSync);
            },
            cancellationToken);
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
        if (string.IsNullOrWhiteSpace(backupDir))
        {
            throw new InvalidOperationException("Missing backup path. Usage: codex-provider restore <backup-dir>");
        }

        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);
        string configText = await _configFileService.ReadConfigTextAsync(_codexHomeService.ConfigPath(codexHome));
        CodexStorageLayout storage = await PrepareStorageAsync(codexHome, explicitSqliteHome, configText);
        storage.EnsureSqliteAccessSupported("restore");

        await using LockHandle _ = await _lockService.AcquireLockAsync(codexHome, "restore");
        string normalizedBackupDir = Path.GetFullPath(backupDir);
        await EnsurePendingRecoveryCoverageAsync(normalizedBackupDir, codexHome, options);
        RestoreResult result = await _backupService.RestoreBackupAsync(normalizedBackupDir, storage, options);
        await FileTransactionJournal.MarkBackupRolledBackAsync(
            normalizedBackupDir,
            codexHome,
            result.TargetProvider);
        return result;
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

    public async Task<BackupPruneResult> RunPruneBackupsAsync(
        string? explicitCodexHome = null,
        int keepCount = AppConstants.DefaultBackupRetentionCount)
    {
        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);

        await using LockHandle _ = await _lockService.AcquireLockAsync(codexHome, "prune-backups");
        return await _backupService.PruneBackupsAsync(codexHome, keepCount);
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

    private static void EnsureWritableStorage(CodexStorageLayout storage)
    {
        if (storage.StateDbLocation is null && storage.HasConfiguredSqliteHome)
        {
            throw new InvalidOperationException(
                $"state_5.sqlite not found in configured SQLite home {storage.SqliteHome} "
                + $"(source: {storage.SqliteHomeSource}).");
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
}
