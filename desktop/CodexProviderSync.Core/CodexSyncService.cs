namespace CodexProviderSync.Core;

public sealed class CodexSyncService
{
    private readonly CodexHomeService _codexHomeService;
    private readonly ConfigFileService _configFileService;
    private readonly SessionRolloutService _sessionRolloutService;
    private readonly SqliteStateService _sqliteStateService;
    private readonly BackupService _backupService;
    private readonly LockService _lockService;
    private readonly ProviderDiscoveryService _providerDiscoveryService;

    public CodexSyncService()
        : this(
            new CodexHomeService(),
            new ConfigFileService(),
            new SessionRolloutService(),
            new SqliteStateService(),
            new LockService(),
            new ProviderDiscoveryService())
    {
    }

    public CodexSyncService(
        CodexHomeService codexHomeService,
        ConfigFileService configFileService,
        SessionRolloutService sessionRolloutService,
        SqliteStateService sqliteStateService,
        LockService lockService,
        ProviderDiscoveryService providerDiscoveryService)
    {
        _codexHomeService = codexHomeService;
        _configFileService = configFileService;
        _sessionRolloutService = sessionRolloutService;
        _sqliteStateService = sqliteStateService;
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
        SessionChangeCollection rolloutInfo = await _sessionRolloutService.CollectSessionChangesAsync(codexHome, "__status_only__");
        ProviderCounts? sqliteCounts = await _sqliteStateService.ReadSqliteProviderCountsAsync(codexHome);
        BackupSummary backupSummary = await _backupService.GetBackupSummaryAsync(codexHome);

        return new StatusSnapshot
        {
            CodexHome = codexHome,
            CurrentProvider = currentProvider,
            ConfiguredProviders = configuredProviders,
            RolloutCounts = rolloutInfo.ProviderCounts,
            SqliteCounts = sqliteCounts,
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
        int? sqliteBusyTimeoutMs = null)
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

        await using LockHandle _ = await _lockService.AcquireLockAsync(codexHome, "sync");

        SessionChangeCollection sessionInfo = await _sessionRolloutService.CollectSessionChangesAsync(codexHome, targetProvider, skipLockedReads: true);
        IReadOnlyDictionary<string, long> threadActivityUtcTicks = await _sqliteStateService.ReadThreadActivityUtcTicksByIdAsync(
            codexHome,
            sessionInfo.Changes.Select(static change => change.ThreadId));
        foreach (SessionChange change in sessionInfo.Changes)
        {
            if (change.ThreadId is not null
                && threadActivityUtcTicks.TryGetValue(change.ThreadId, out long normalizedTicks))
            {
                change.NormalizedLastWriteTimeUtcTicks = normalizedTicks;
            }
        }
        (IReadOnlyList<SessionChange> writableChanges, IReadOnlyList<SessionChange> lockedChanges) =
            await _sessionRolloutService.SplitLockedSessionChangesAsync(sessionInfo.Changes);

        List<string> skippedRolloutFiles = [.. sessionInfo.LockedPaths, .. lockedChanges.Select(static change => change.Path)];

        await _sqliteStateService.AssertSqliteWritableAsync(codexHome, sqliteBusyTimeoutMs);
        string backupDir = await _backupService.CreateBackupAsync(codexHome, targetProvider, writableChanges, configPath, configBackupText);

        bool sessionRestoreNeeded = false;
        List<SessionChange> appliedSessionChanges = [];
        try
        {
            SessionApplyResult? applyResult = null;
            (int updatedRows, bool databasePresent) = await _sqliteStateService.UpdateSqliteProviderAsync(
                codexHome,
                targetProvider,
                async _ =>
                {
                    if (writableChanges.Count == 0)
                    {
                        return;
                    }

                    applyResult = await _sessionRolloutService.ApplySessionChangesAsync(writableChanges);
                    HashSet<string> appliedPathSet = new(applyResult.AppliedPaths, StringComparer.Ordinal);
                    appliedSessionChanges = writableChanges.Where(change => appliedPathSet.Contains(change.Path)).ToList();
                    sessionRestoreNeeded = appliedSessionChanges.Count > 0;
                    await _backupService.UpdateSessionBackupManifestAsync(backupDir, appliedSessionChanges);
                },
                sqliteBusyTimeoutMs);

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
                SqliteRowsUpdated = updatedRows,
                SqlitePresent = databasePresent,
                RolloutCountsBefore = sessionInfo.ProviderCounts,
                AutoPruneResult = autoPruneResult,
                AutoPruneWarning = autoPruneWarning
            };
        }
        catch
        {
            if (sessionRestoreNeeded)
            {
                await _sessionRolloutService.RestoreSessionChangesAsync(appliedSessionChanges);
            }

            throw;
        }
    }

    public async Task<SyncResult> RunSwitchAsync(
        string? explicitCodexHome,
        string provider,
        int keepCount = AppConstants.DefaultBackupRetentionCount)
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
        await _configFileService.WriteConfigTextAsync(configPath, nextConfigText);

        try
        {
            SyncResult result = await RunSyncAsync(codexHome, provider, originalConfigText, keepCount);
            return new SyncResult
            {
                CodexHome = result.CodexHome,
                TargetProvider = result.TargetProvider,
                PreviousProvider = result.PreviousProvider,
                BackupDir = result.BackupDir,
                ChangedSessionFiles = result.ChangedSessionFiles,
                SkippedLockedRolloutFiles = result.SkippedLockedRolloutFiles,
                SqliteRowsUpdated = result.SqliteRowsUpdated,
                SqlitePresent = result.SqlitePresent,
                RolloutCountsBefore = result.RolloutCountsBefore,
                ConfigUpdated = true,
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

    public async Task<RestoreResult> RunRestoreAsync(string? explicitCodexHome, string backupDir)
    {
        if (string.IsNullOrWhiteSpace(backupDir))
        {
            throw new InvalidOperationException("Missing backup path. Usage: codex-provider restore <backup-dir>");
        }

        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        await _codexHomeService.EnsureCodexHomeAsync(codexHome);

        await using LockHandle _ = await _lockService.AcquireLockAsync(codexHome, "restore");
        return await _backupService.RestoreBackupAsync(Path.GetFullPath(backupDir), codexHome);
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
}
