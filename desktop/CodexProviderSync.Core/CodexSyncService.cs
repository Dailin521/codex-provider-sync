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
        _backupService = new BackupService(sessionRolloutService, sqliteStateService);
    }

    public async Task<StatusSnapshot> GetStatusAsync(string? explicitCodexHome = null)
    {
        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);
        string configText = await _configFileService.ReadConfigTextAsync(_codexHomeService.ConfigPath(codexHome));
        CurrentProviderInfo currentProvider = _configFileService.ReadCurrentProviderFromConfigText(configText);
        IReadOnlyList<string> configuredProviders = _configFileService.ListConfiguredProviderIds(configText);
        SessionChangeCollection rolloutInfo = await _sessionRolloutService.CollectSessionChangesAsync(codexHome, "__status_only__", skipLockedReads: true);
        StateDbLocation? stateDbLocation = _sqliteStateService.DetectStateDb(codexHome);
        ProviderCounts? sqliteCounts = await _sqliteStateService.ReadSqliteProviderCountsAsync(codexHome);
        SqliteRepairStats? sqliteRepairStats = sqliteCounts is not null && !sqliteCounts.Unreadable
            ? await _sqliteStateService.ReadSqliteRepairStatsAsync(
                codexHome,
                rolloutInfo.UserEventThreadIds,
                rolloutInfo.ThreadCwdsById)
            : null;
        IReadOnlyList<ProjectThreadVisibility> projectThreadVisibility = sqliteCounts?.Unreadable == true
            ? []
            : await _globalStateService.ReadProjectThreadVisibilityAsync(codexHome);
        BackupSummary backupSummary = await _backupService.GetBackupSummaryAsync(codexHome);

        return new StatusSnapshot
        {
            CodexHome = codexHome,
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
            BackupSummary = backupSummary
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

    public async Task<SyncResult> RunSyncAsync(
        string? explicitCodexHome = null,
        string? provider = null,
        string? configBackupText = null,
        int keepCount = AppConstants.DefaultBackupRetentionCount,
        int? sqliteBusyTimeoutMs = null,
        string? model = null)
    {
        if (keepCount < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(keepCount), keepCount, "keepCount must be 1 or greater for automatic cleanup.");
        }

        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);
        string configPath = _codexHomeService.ConfigPath(codexHome);
        string configText = await _configFileService.ReadConfigTextAsync(configPath);
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

        await using LockHandle _ = await _lockService.AcquireLockAsync(codexHome, "sync");

        SessionChangeCollection sessionInfo = await _sessionRolloutService.CollectSessionChangesAsync(
            codexHome,
            targetProvider,
            skipLockedReads: true,
            // Plumb the resolved root-level model down to the rollout
            // collector so it can skip rollouts whose `turn_context.model`
            // already matches the active target — no rewrite, no
            // backup, no `changed files` entry.
            targetModel: targetModel);
        IReadOnlyList<ThreadCwdStat> workspaceCwdStats = await _globalStateService.ReadThreadCwdStatsAsync(codexHome);
        string? encryptedContentWarning = BuildEncryptedContentWarning(sessionInfo.EncryptedContentCounts, targetProvider);
        (IReadOnlyList<SessionChange> writableChanges, IReadOnlyList<SessionChange> lockedChanges) =
            await _sessionRolloutService.SplitLockedSessionChangesAsync(sessionInfo.Changes);

        List<string> skippedRolloutFiles = [.. sessionInfo.LockedPaths, .. lockedChanges.Select(static change => change.Path)];
        IReadOnlyList<string> skippedUnreadableRolloutFiles = sessionInfo.UnreadablePaths
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToList();

        await _sqliteStateService.AssertSqliteWritableAsync(codexHome, sqliteBusyTimeoutMs);
        string backupDir = await _backupService.CreateBackupAsync(codexHome, targetProvider, writableChanges, configPath, configBackupText);

        bool sessionRestoreNeeded = false;
        List<SessionChange> appliedSessionChanges = [];
        bool globalStateRestoreNeeded = false;
        WorkspaceRootSyncResult workspaceRootResult = new()
        {
            Present = false,
            Updated = false,
            UpdatedWorkspaceRoots = 0,
            SavedWorkspaceRootCount = 0
        };
        try
        {
            SessionApplyResult? applyResult = null;
            (int updatedRows, int providerRowsUpdated, int modelRowsUpdated, int userEventRowsUpdated, int cwdRowsUpdated, bool databasePresent) = await _sqliteStateService.UpdateSqliteProviderAsync(
                codexHome,
                targetProvider,
                targetModel,
                async _ =>
                {
                    if (writableChanges.Count > 0)
                    {
                        applyResult = await _sessionRolloutService.ApplySessionChangesAsync(writableChanges, targetModel);
                        HashSet<string> appliedPathSet = new(applyResult.AppliedPaths, StringComparer.Ordinal);
                        appliedSessionChanges = writableChanges.Where(change => appliedPathSet.Contains(change.Path)).ToList();
                        sessionRestoreNeeded = appliedSessionChanges.Count > 0;
                        await _backupService.UpdateSessionBackupManifestAsync(backupDir, appliedSessionChanges);
                    }
                    workspaceRootResult = await _globalStateService.SyncWorkspaceRootsAsync(codexHome, workspaceCwdStats);
                    globalStateRestoreNeeded = workspaceRootResult.Updated;
                },
                sqliteBusyTimeoutMs,
                sessionInfo.UserEventThreadIds,
                sessionInfo.ThreadCwdsById);

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

            return new SyncResult
            {
                CodexHome = codexHome,
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
        }
        catch (Exception error)
        {
            List<string> restoreFailures = [];
            if (sessionRestoreNeeded)
            {
                try
                {
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
                    await _backupService.RestoreGlobalStateFilesAsync(backupDir, codexHome);
                }
                catch (Exception restoreError)
                {
                    restoreFailures.Add($"global state: {restoreError.Message}");
                }
            }

            if (restoreFailures.Count > 0)
            {
                throw new InvalidOperationException(
                    $"Failed to restore state after sync error. Original error: {error.Message}. Restore error: {string.Join("; ", restoreFailures)}",
                    error);
            }

            throw;
        }
    }

    public async Task<SyncResult> RunSwitchAsync(
        string? explicitCodexHome,
        string provider,
        int keepCount = AppConstants.DefaultBackupRetentionCount,
        string? model = null,
        bool keepRootModel = false)
    {
        if (string.IsNullOrWhiteSpace(provider))
        {
            throw new InvalidOperationException("Missing provider id. Usage: codex-provider switch <provider-id>");
        }

        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);
        string configPath = _codexHomeService.ConfigPath(codexHome);
        string originalConfigText = await _configFileService.ReadConfigTextAsync(configPath);
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

        await _configFileService.WriteConfigTextAsync(configPath, nextConfigText);

        try
        {
            // Forward the resolved model to RunSyncAsync so the per-thread
            // SQLite `model` column also gets aligned with the new value. If
            // the switch did not apply a model (keepRootModel, no model
            // found, etc.), pass null so the legacy behaviour is preserved.
            string? modelForThreads = modelSync.Applied ? modelSync.Model : null;
            SyncResult result = await RunSyncAsync(codexHome, provider, originalConfigText, keepCount, model: modelForThreads);
            return new SyncResult
            {
                CodexHome = result.CodexHome,
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
            await _configFileService.WriteConfigTextAsync(configPath, originalConfigText);
            throw;
        }
    }

    private static ModelSyncOutcome ResolveModelSyncOutcome(
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

        string? providerModel = new ConfigFileService().ReadProviderModel(originalConfigText, provider);
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

    public async Task<RestoreResult> RunRestoreAsync(string? explicitCodexHome, string backupDir)
    {
        return await RunRestoreAsync(explicitCodexHome, backupDir, new RestoreBackupOptions());
    }

    public async Task<RestoreResult> RunRestoreAsync(string? explicitCodexHome, string backupDir, RestoreBackupOptions options)
    {
        if (string.IsNullOrWhiteSpace(backupDir))
        {
            throw new InvalidOperationException("Missing backup path. Usage: codex-provider restore <backup-dir>");
        }

        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);

        await using LockHandle _ = await _lockService.AcquireLockAsync(codexHome, "restore");
        return await _backupService.RestoreBackupAsync(Path.GetFullPath(backupDir), codexHome, options);
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
}
