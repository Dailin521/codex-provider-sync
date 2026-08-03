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
            afterBackup: null,
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
        Func<string, Task>? afterBackup,
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
        CodexStorageLayout storage = await PrepareStorageAsync(codexHome, explicitSqliteHome, configText);
        storage.EnsureSqliteAccessSupported("sync");
        EnsureWritableStorage(storage);
        CurrentProviderInfo current = _configFileService.ReadCurrentProviderFromConfigText(configText);
        string targetProvider = provider ?? current.Provider ?? AppConstants.DefaultProvider;

        // When the caller did not pin a model, mirror the active root-level
        // `model = "..."` field from config.toml into the per-thread SQLite
        // `model` column. Without this, old sessions keep showing the model
        // they were created with in Codex's bottom-right UI label, even after
        // the root-level `model` changes.
        string? targetModel = model;
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
        string backupDir = await _backupService.CreateBackupAsync(storage, targetProvider, writableChanges, configPath, configBackupText);
        bool sessionRestoreNeeded = false;
        List<SessionChange> appliedSessionChanges = [];
        bool globalStateRestoreNeeded = false;
        string globalStatePath = _globalStateService.StatePath(codexHome);
        string globalStateBackupPath = _globalStateService.BackupPath(codexHome);
        string[] potentialTargets = writableChanges.Select(static change => Path.GetFullPath(change.Path))
            .Append(Path.GetFullPath(globalStatePath))
            .Append(Path.GetFullPath(globalStateBackupPath))
            .Concat(configBackupText is null ? [] : [Path.GetFullPath(configPath)])
            .Concat(storage.StateDbCandidates.Select(static candidate => Path.GetFullPath(candidate.Path)))
            .ToArray();
        FileTransactionJournal journal = await FileTransactionJournal.CreateAsync(
            backupDir,
            codexHome,
            targetProvider,
            potentialTargets);
        List<string> completedTargets = [];
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
            if (afterBackup is not null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await journal.ApplyingAsync("config", configPath);
                await afterBackup(backupDir);
                await journal.AppliedAsync("config", configPath);
                RecordCompletedTarget(configPath);
                if (FaultInjector is not null)
                {
                    await FaultInjector("after_config_apply", configPath, 1);
                }
            }

            SessionApplyResult? applyResult = null;
            await journal.ApplyingAsync("sqlite", storage.StateDbLocation?.Path ?? storage.SqliteHome);
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
                                appliedSessionChanges.Add(change);
                                sessionRestoreNeeded = true;
                                await journal.AppliedAsync("rollout", change.Path);
                                RecordCompletedTarget(change.Path);
                                await _backupService.UpdateSessionBackupManifestAsync(backupDir, appliedSessionChanges);
                                if (FaultInjector is not null)
                                {
                                    await FaultInjector("after_rollout_apply", change.Path, appliedSessionChanges.Count);
                                }
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
                            globalStateRestoreNeeded = true;
                            await journal.AppliedAsync("globalState", targetPath);
                            RecordCompletedTarget(targetPath);
                            if (FaultInjector is not null)
                            {
                                await FaultInjector("after_global_state_apply", targetPath, 1);
                            }
                        });
                },
                sqliteBusyTimeoutMs,
                sessionInfo.UserEventThreadIds,
                sessionInfo.ThreadCwdsById);
            await journal.AppliedAsync("sqlite", storage.StateDbLocation?.Path ?? storage.SqliteHome);
            RecordCompletedTarget(storage.StateDbLocation?.Path ?? storage.SqliteHome);

            skippedRolloutFiles.AddRange(applyResult?.SkippedPaths ?? []);
            skippedRolloutFiles = skippedRolloutFiles.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();

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
                AutoPruneWarning = autoPruneWarning
            };
            await journal.CommittedAsync();
            return result;
        }
        catch (Exception error)
        {
            List<string> restoreFailures = [];
            try
            {
                await journal.RollingBackAsync(error);
            }
            catch (Exception journalError)
            {
                restoreFailures.Add($"transaction journal: {journalError.Message}");
            }
            if (sessionRestoreNeeded)
            {
                try
                {
                    if (FaultInjector is not null)
                    {
                        await FaultInjector("before_rollout_rollback", null, appliedSessionChanges.Count);
                    }
                    await _sessionRolloutService.RestoreSessionChangesAsync(appliedSessionChanges);
                }
                catch (Exception restoreError)
                {
                    restoreFailures.Add($"rollout files: {restoreError.Message}");
                }
            }
            if (globalStateRestoreNeeded)
            {
                try
                {
                    if (FaultInjector is not null)
                    {
                        await FaultInjector("before_global_state_rollback", null, 1);
                    }
                    await _backupService.RestoreGlobalStateFilesAsync(backupDir, codexHome);
                }
                catch (Exception restoreError)
                {
                    restoreFailures.Add($"global state: {restoreError.Message}");
                }
            }

            if (configBackupText is not null)
            {
                try
                {
                    await _configFileService.WriteConfigTextAsync(configPath, configBackupText);
                }
                catch (Exception restoreError)
                {
                    restoreFailures.Add($"config: {restoreError.Message}");
                }
            }

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
                HashSet<string> completedTargetSet = new(completedTargets, StringComparer.Ordinal);
                IReadOnlyList<string> uncompletedTargets = potentialTargets
                    .Where(targetPath => !completedTargetSet.Contains(targetPath))
                    .ToArray();
                throw new SyncTransactionException(
                    error,
                    restoreFailures,
                    backupDir,
                    completedTargets,
                    uncompletedTargets,
                    rollbackStatus: "incomplete",
                    recoveryRequired: true);
            }

            HashSet<string> completedSet = new(completedTargets, StringComparer.Ordinal);
            throw new SyncTransactionException(
                error,
                [],
                backupDir,
                completedTargets,
                potentialTargets.Where(targetPath => !completedSet.Contains(targetPath)).ToArray(),
                rollbackStatus: "complete",
                recoveryRequired: false);
        }
    }

    public async Task<SyncResult> RunSwitchAsync(
        string? explicitCodexHome,
        string provider,
        int keepCount = AppConstants.DefaultBackupRetentionCount,
        string? model = null,
        bool keepRootModel = false,
        string? explicitSqliteHome = null)
    {
        if (string.IsNullOrWhiteSpace(provider))
        {
            throw new InvalidOperationException("Missing provider id. Usage: codex-provider switch <provider-id>");
        }

        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);
        string configPath = _codexHomeService.ConfigPath(codexHome);
        string originalConfigText = await _configFileService.ReadConfigTextAsync(configPath);
        CodexStorageLayout storage = await PrepareStorageAsync(codexHome, explicitSqliteHome, originalConfigText);
        storage.EnsureSqliteAccessSupported("switch");
        EnsureWritableStorage(storage);
        if (!_configFileService.ConfigDeclaresProvider(originalConfigText, provider))
        {
            string configuredProviders = string.Join(", ", _configFileService.ListConfiguredProviderIds(originalConfigText));
            throw new InvalidOperationException(
                $"Provider \"{provider}\" is not available in config.toml. Configure it first or use one of: {configuredProviders}");
        }

        string nextConfigText = _configFileService.SetRootProviderInConfigText(originalConfigText, provider);
        ModelSyncOutcome modelSync = ResolveModelSyncOutcome(originalConfigText, provider, model, keepRootModel);
        if (modelSync.Applied)
        {
            nextConfigText = _configFileService.SetRootModelInConfigText(nextConfigText, modelSync.Model!);
        }

        bool configMutationAttempted = false;
        try
        {
            // Even when the switch keeps the existing root model, keep
            // SQLite and rollout turn_context fields aligned with it.
            string? modelForThreads = modelSync.Applied
                ? modelSync.Model
                : _configFileService.ReadRootModelFromConfigText(nextConfigText);
            SyncResult result = await RunSyncCoreAsync(
                codexHome,
                provider,
                originalConfigText,
                keepCount,
                sqliteBusyTimeoutMs: null,
                model: modelForThreads,
                explicitSqliteHome: explicitSqliteHome,
                afterBackup: async _ =>
                {
                    configMutationAttempted = true;
                    await _configFileService.WriteConfigTextAsync(configPath, nextConfigText);
                });
            return new SyncResult
            {
                CodexHome = result.CodexHome,
                SqliteHome = result.SqliteHome,
                SqliteHomeSource = result.SqliteHomeSource,
                TargetProvider = result.TargetProvider,
                PreviousProvider = result.PreviousProvider,
                BackupDir = result.BackupDir,
                ChangedSessionFiles = result.ChangedSessionFiles,
                SkippedLockedRolloutFiles = result.SkippedLockedRolloutFiles,
                SkippedUnreadableRolloutFiles = result.SkippedUnreadableRolloutFiles,
                SqliteRowsUpdated = result.SqliteRowsUpdated,
                SqliteProviderRowsUpdated = result.SqliteProviderRowsUpdated,
                SqliteModelRowsUpdated = result.SqliteModelRowsUpdated,
                SqliteUserEventRowsUpdated = result.SqliteUserEventRowsUpdated,
                SqliteCwdRowsUpdated = result.SqliteCwdRowsUpdated,
                UpdatedWorkspaceRoots = result.UpdatedWorkspaceRoots,
                SavedWorkspaceRootCount = result.SavedWorkspaceRootCount,
                SqlitePresent = result.SqlitePresent,
                RolloutCountsBefore = result.RolloutCountsBefore,
                EncryptedContentCounts = result.EncryptedContentCounts,
                EncryptedContentWarning = result.EncryptedContentWarning,
                ConfigUpdated = true,
                ModelSync = modelSync,
                AutoPruneResult = result.AutoPruneResult,
                AutoPruneWarning = result.AutoPruneWarning
            };
        }
        catch
        {
            if (configMutationAttempted)
            {
                await _configFileService.WriteConfigTextAsync(configPath, originalConfigText);
            }
            throw;
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
        RestoreResult result = await _backupService.RestoreBackupAsync(normalizedBackupDir, storage, options);
        await FileTransactionJournal.MarkBackupRolledBackAsync(normalizedBackupDir);
        return result;
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
}
