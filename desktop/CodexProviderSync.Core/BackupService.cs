using System.Text;
using System.Text.Json;

namespace CodexProviderSync.Core;

public sealed class BackupService
{
    private readonly SessionRolloutService _sessionRolloutService;
    private readonly SqliteStateService _sqliteStateService;

    internal Func<string, string, string, Task>? AtomicWriteFaultInjector { get; set; }

    internal Action<string>? DirectoryInventoryFallbackObserver { get; set; }

    public BackupService(SessionRolloutService sessionRolloutService, SqliteStateService sqliteStateService)
    {
        _sessionRolloutService = sessionRolloutService;
        _sqliteStateService = sqliteStateService;
    }

    public async Task<string> CreateBackupAsync(
        string codexHome,
        string targetProvider,
        IReadOnlyList<SessionChange> sessionChanges,
        string configPath,
        string? configBackupText = null)
    {
        return await CreateBackupAsync(
            new CodexStorageLayoutService().CreateDefault(codexHome),
            targetProvider,
            sessionChanges,
            configPath,
            configBackupText);
    }

    public async Task<string> CreateBackupAsync(
        CodexStorageLayout storage,
        string targetProvider,
        IReadOnlyList<SessionChange> sessionChanges,
        string configPath,
        string? configBackupText = null)
    {
        storage.EnsureSqliteAccessSupported("create a backup");
        string codexHome = storage.CodexHome;
        string backupRoot = AppConstants.DefaultBackupRoot(codexHome);
        string backupDir = Path.Combine(backupRoot, DateTimeOffset.UtcNow.ToString("yyyyMMdd'T'HHmmssfff'Z'"));
        string dbDir = Path.Combine(backupDir, "db");
        Directory.CreateDirectory(dbDir);

        List<string> copiedDbFiles = [];
        List<string> copiedSqliteDbFiles = [];
        StateDbLocation? stateDb = storage.StateDbLocation ?? _sqliteStateService.DetectStateDb(storage);
        string actualSqliteHome = stateDb is null ? storage.SqliteHome : Path.GetDirectoryName(stateDb.Path)!;
        if (stateDb is not null)
        {
            string sqliteRelativePath = AppConstants.DbFileBasename;
            string sqliteBackupPath = Path.Combine(dbDir, "sqlite-home", sqliteRelativePath);
            CodexStorageLayout detectedStorage = storage with { StateDbLocation = stateDb };
            SqliteOnlineBackupResult sqliteBackup = await _sqliteStateService.CreateSqliteOnlineBackupAsync(
                detectedStorage,
                sqliteBackupPath);
            if (!sqliteBackup.DatabasePresent)
            {
                throw new InvalidOperationException(
                    $"state_5.sqlite disappeared while creating a backup: {stateDb.Path}");
            }
            copiedSqliteDbFiles.Add(sqliteRelativePath);

            // Keep the v2 legacy mirror for readers that still consult DbFiles,
            // but derive it from the consistent standalone snapshot. Never
            // copy live WAL/SHM sidecars independently into a managed backup.
            string? legacyRelativePath = SafeRelativePath(codexHome, stateDb.Path);
            if (legacyRelativePath is not null)
            {
                await AtomicFile.CopyAsync(
                    sqliteBackupPath,
                    Path.Combine(dbDir, legacyRelativePath),
                    overwrite: false);
                copiedDbFiles.Add(legacyRelativePath);
            }
        }

        string configBackupPath = Path.Combine(backupDir, "config.toml");
        if (configBackupText is not null)
        {
            await AtomicFile.WriteAllTextAsync(
                configBackupPath,
                configBackupText,
                faultInjector: AtomicWriteFaultInjector);
        }
        else
        {
            await CopyIfPresentAsync(configPath, configBackupPath, overwrite: false);
        }
        await CopyIfPresentAsync(
            Path.Combine(codexHome, AppConstants.GlobalStateFileBasename),
            Path.Combine(backupDir, AppConstants.GlobalStateFileBasename),
            overwrite: false);
        await CopyIfPresentAsync(
            Path.Combine(codexHome, AppConstants.GlobalStateBackupFileBasename),
            Path.Combine(backupDir, AppConstants.GlobalStateBackupFileBasename),
            overwrite: false);

        DateTimeOffset createdAt = DateTimeOffset.UtcNow;
        SessionBackupManifest sessionManifest = new()
        {
            Version = 2,
            Namespace = AppConstants.BackupNamespace,
            CodexHome = codexHome,
            TargetProvider = targetProvider,
            CreatedAt = createdAt,
            Files = sessionChanges.Select(SessionBackupManifestEntry.FromChange).ToList()
        };
        await AtomicFile.WriteAllTextAsync(
            Path.Combine(backupDir, "session-meta-backup.json"),
            JsonSerializer.Serialize(sessionManifest, JsonOptions()),
            faultInjector: AtomicWriteFaultInjector);

        bool globalStateFilePresent = File.Exists(
            Path.Combine(codexHome, AppConstants.GlobalStateFileBasename));
        bool globalStateBackupFilePresent = File.Exists(
            Path.Combine(codexHome, AppConstants.GlobalStateBackupFileBasename));
        BackupMetadataFile metadata = new()
        {
            Version = 2,
            Namespace = AppConstants.BackupNamespace,
            CodexHome = codexHome,
            SqliteHome = actualSqliteHome,
            TargetProvider = targetProvider,
            CreatedAt = createdAt,
            DbFiles = copiedDbFiles,
            SqliteDbFiles = copiedSqliteDbFiles,
            ChangedSessionFiles = sessionChanges.Count,
            GlobalStateFiles = new Dictionary<string, bool>(StringComparer.Ordinal)
            {
                [AppConstants.GlobalStateFileBasename] = globalStateFilePresent,
                [AppConstants.GlobalStateBackupFileBasename] = globalStateBackupFilePresent
            },
            GlobalStateFilePresent = globalStateFilePresent,
            GlobalStateBackupFilePresent = globalStateBackupFilePresent
        };
        await WriteMetadataWithInventoryAsync(backupDir, metadata);

        return backupDir;
    }

    public async Task<RestoreResult> RestoreBackupAsync(
        string backupDir,
        string codexHome,
        RestoreBackupOptions? options = null)
    {
        return await RestoreBackupAsync(
            backupDir,
            new CodexStorageLayoutService().CreateDefault(codexHome),
            options);
    }

    public async Task<RestoreResult> RestoreBackupAsync(
        string backupDir,
        CodexStorageLayout storage,
        RestoreBackupOptions? options = null)
    {
        storage.EnsureSqliteAccessSupported("restore");
        options ??= new RestoreBackupOptions();
        string codexHome = storage.CodexHome;
        string normalizedBackupDir = Path.GetFullPath(backupDir);
        string metadataPath = Path.Combine(normalizedBackupDir, "metadata.json");
        BackupMetadataFile metadata = JsonSerializer.Deserialize<BackupMetadataFile>(
            await File.ReadAllTextAsync(metadataPath),
            JsonOptions()) ?? throw new InvalidOperationException($"Backup metadata is invalid: {backupDir}");

        if (!string.Equals(metadata.Namespace, AppConstants.BackupNamespace, StringComparison.Ordinal)
            || metadata.Version is not (1 or 2))
        {
            throw new InvalidOperationException($"Unsupported backup metadata in {metadataPath}.");
        }

        if (!PathsEqual(metadata.CodexHome, codexHome))
        {
            throw new InvalidOperationException($"Backup was created for {metadata.CodexHome}, not {codexHome}.");
        }

        if (options.RestoreConfig)
        {
            ValidateGlobalStatePresenceMetadata(metadata);
        }

        SessionBackupManifest? sessionManifest = null;
        if (options.RestoreSessions)
        {
            sessionManifest = JsonSerializer.Deserialize<SessionBackupManifest>(
                await File.ReadAllTextAsync(Path.Combine(normalizedBackupDir, "session-meta-backup.json")),
                JsonOptions()) ?? throw new InvalidOperationException($"Session backup manifest is invalid: {backupDir}");

            ValidateSessionManifest(sessionManifest, codexHome, normalizedBackupDir);
            sessionManifest = await SelectSessionEntriesForRestoreAsync(normalizedBackupDir, sessionManifest);

            await _sessionRolloutService.AssertSessionFilesWritableAsync(
                sessionManifest.Files.Select(static entry => entry.Path));
        }

        (string SourcePath, string TargetPath)? databaseRestorePlan = null;
        if (options.RestoreDatabase)
        {
            StateDbLocation? stateDb = storage.StateDbLocation ?? _sqliteStateService.DetectStateDb(storage);
            if (stateDb is null && storage.HasConfiguredSqliteHome)
            {
                throw new InvalidOperationException(
                    $"state_5.sqlite not found in SQLite home {storage.SqliteHome}.");
            }

            string targetSqliteHome = ResolveRestoreSqliteHome(storage, metadata, stateDb);
            bool sqliteHomeRelocation = metadata.Version >= 2
                && !string.IsNullOrWhiteSpace(metadata.SqliteHome)
                && !PathsEqual(metadata.SqliteHome, targetSqliteHome);
            if (sqliteHomeRelocation && !options.AllowSqliteHomeRelocation)
            {
                throw new InvalidOperationException(
                    $"Backup SQLite home is {metadata.SqliteHome}, but the current target is {targetSqliteHome}. "
                    + "Confirm SQLite Home relocation before restoring to a different location.");
            }
            if (sqliteHomeRelocation && options.RestoreConfig)
            {
                throw new InvalidOperationException(
                    "Cannot restore config.toml while relocating SQLite home. "
                    + "Disable config restore to preserve the current target configuration.");
            }

            if (stateDb is not null)
            {
                CodexStorageLayout detectedStorage = storage with { StateDbLocation = stateDb };
                await _sqliteStateService.AssertSqliteWritableAsync(detectedStorage);
            }

            IReadOnlyList<string> databaseFiles = metadata.Version >= 2
                ? metadata.SqliteDbFiles ?? []
                : metadata.DbFiles ?? [];
            if (!databaseFiles.Any(static fileName => Path.GetFileName(fileName) == AppConstants.DbFileBasename))
            {
                throw new InvalidOperationException(
                    "Backup does not contain state_5.sqlite. Disable database restore to restore the remaining data.");
            }
            string databaseBackupRoot = metadata.Version >= 2
                ? Path.Combine(normalizedBackupDir, "db", "sqlite-home")
                : Path.Combine(normalizedBackupDir, "db");
            string restoreRoot = metadata.Version >= 2 ? targetSqliteHome : codexHome;
            foreach (string fileName in databaseFiles)
            {
                string targetPath = metadata.Version >= 2
                    ? RestoreSqliteTargetPath(restoreRoot, fileName)
                    : RestoreDbTargetPath(restoreRoot, fileName);
                string sourcePath = Path.Combine(databaseBackupRoot, fileName);
                if (!File.Exists(sourcePath))
                {
                    throw new InvalidOperationException($"Backup declares a missing SQLite file: {sourcePath}");
                }
                if (Path.GetFileName(fileName) == AppConstants.DbFileBasename)
                {
                    if (databaseRestorePlan is not null)
                    {
                        throw new InvalidOperationException(
                            "Backup must contain exactly one state_5.sqlite restore source.");
                    }
                    databaseRestorePlan = (sourcePath, targetPath);
                }
            }
        }

        if (options.RestoreConfig)
        {
            await CopyIfPresentAsync(
                Path.Combine(normalizedBackupDir, "config.toml"),
                Path.Combine(codexHome, "config.toml"),
                overwrite: true);
            await RestoreGlobalStateFilesAsync(normalizedBackupDir, codexHome);
        }

        if (options.RestoreDatabase && databaseRestorePlan is { } restorePlan)
        {
            await _sqliteStateService.RestoreSqliteOnlineBackupAsync(
                restorePlan.SourcePath,
                restorePlan.TargetPath);
        }

        if (options.RestoreSessions && sessionManifest is not null)
        {
            await _sessionRolloutService.RestoreSessionChangesAsync(sessionManifest.Files);
        }

        return new RestoreResult
        {
            CodexHome = codexHome,
            BackupDir = normalizedBackupDir,
            TargetProvider = metadata.TargetProvider,
            CreatedAt = metadata.CreatedAt,
            ChangedSessionFiles = metadata.ChangedSessionFiles
        };
    }

    public async Task UpdateSessionBackupManifestAsync(string backupDir, IReadOnlyList<SessionChange> sessionChanges)
    {
        string normalizedBackupDir = Path.GetFullPath(backupDir);
        string manifestPath = Path.Combine(normalizedBackupDir, "session-meta-backup.json");
        string metadataPath = Path.Combine(normalizedBackupDir, "metadata.json");

        SessionBackupManifest sessionManifest = JsonSerializer.Deserialize<SessionBackupManifest>(
            await File.ReadAllTextAsync(manifestPath),
            JsonOptions()) ?? throw new InvalidOperationException($"Session backup manifest is invalid: {backupDir}");
        BackupMetadataFile metadata = JsonSerializer.Deserialize<BackupMetadataFile>(
            await File.ReadAllTextAsync(metadataPath),
            JsonOptions()) ?? throw new InvalidOperationException($"Backup metadata is invalid: {backupDir}");

        sessionManifest = new SessionBackupManifest
        {
            Version = 2,
            Namespace = sessionManifest.Namespace,
            CodexHome = sessionManifest.CodexHome,
            TargetProvider = sessionManifest.TargetProvider,
            CreatedAt = sessionManifest.CreatedAt,
            Files = sessionChanges.Select(SessionBackupManifestEntry.FromChange).ToList()
        };
        metadata = new BackupMetadataFile
        {
            Version = metadata.Version,
            Namespace = metadata.Namespace,
            CodexHome = metadata.CodexHome,
            SqliteHome = metadata.SqliteHome,
            TargetProvider = metadata.TargetProvider,
            CreatedAt = metadata.CreatedAt,
            DbFiles = metadata.DbFiles,
            SqliteDbFiles = metadata.SqliteDbFiles,
            ChangedSessionFiles = sessionChanges.Count,
            GlobalStateFiles = metadata.GlobalStateFiles,
            GlobalStateFilePresent = metadata.GlobalStateFilePresent,
            GlobalStateBackupFilePresent = metadata.GlobalStateBackupFilePresent,
            SizeBytes = metadata.SizeBytes,
            FileCount = metadata.FileCount
        };

        await AtomicFile.WriteAllTextAsync(
            manifestPath,
            JsonSerializer.Serialize(sessionManifest, JsonOptions()),
            faultInjector: AtomicWriteFaultInjector);
        await WriteMetadataWithInventoryAsync(normalizedBackupDir, metadata);
    }

    internal async Task<IReadOnlyList<SessionBackupManifestEntry>> ReadSessionBackupEntriesAsync(
        string backupDir,
        string codexHome)
    {
        string normalizedBackupDir = Path.GetFullPath(backupDir);
        SessionBackupManifest manifest = JsonSerializer.Deserialize<SessionBackupManifest>(
            await File.ReadAllTextAsync(Path.Combine(normalizedBackupDir, "session-meta-backup.json")),
            JsonOptions()) ?? throw new InvalidOperationException($"Session backup manifest is invalid: {backupDir}");
        ValidateSessionManifest(manifest, codexHome, normalizedBackupDir);
        return manifest.Files;
    }

    internal async Task<BackupRecoveryCoverage> GetRecoveryCoverageAsync(
        string backupDir,
        string codexHome)
    {
        string normalizedBackupDir = Path.GetFullPath(backupDir);
        BackupMetadataFile metadata = JsonSerializer.Deserialize<BackupMetadataFile>(
            await File.ReadAllTextAsync(Path.Combine(normalizedBackupDir, "metadata.json")),
            JsonOptions()) ?? throw new InvalidOperationException($"Backup metadata is invalid: {backupDir}");
        if (!string.Equals(metadata.Namespace, AppConstants.BackupNamespace, StringComparison.Ordinal)
            || metadata.Version is not (1 or 2)
            || !PathsEqual(metadata.CodexHome, codexHome))
        {
            throw new InvalidOperationException($"Backup metadata is not valid for recovery: {backupDir}");
        }

        bool database = (metadata.Version >= 2 ? metadata.SqliteDbFiles : metadata.DbFiles)
            .Any(static fileName => Path.GetFileName(fileName) == AppConstants.DbFileBasename);
        bool sessions = false;
        string sessionManifestPath = Path.Combine(normalizedBackupDir, "session-meta-backup.json");
        if (File.Exists(sessionManifestPath))
        {
            sessions = (await ReadSessionBackupEntriesAsync(normalizedBackupDir, codexHome)).Count > 0;
        }
        bool? globalStatePresent = ResolveGlobalStatePresence(
            metadata,
            AppConstants.GlobalStateFileBasename);
        bool? globalStateBackupPresent = ResolveGlobalStatePresence(
            metadata,
            AppConstants.GlobalStateBackupFileBasename);
        bool config = File.Exists(Path.Combine(normalizedBackupDir, "config.toml"))
            || globalStatePresent == true
            || globalStateBackupPresent == true;
        return new BackupRecoveryCoverage(config, database, sessions);
    }

    internal async Task RestoreConfigFileAsync(string backupDir, string codexHome)
    {
        string sourcePath = Path.Combine(Path.GetFullPath(backupDir), "config.toml");
        if (!await CopyIfPresentAsync(sourcePath, Path.Combine(codexHome, "config.toml"), overwrite: true))
        {
            throw new InvalidOperationException($"Backup config is missing: {sourcePath}");
        }
    }

    internal async Task RestoreGlobalStateTargetAsync(string backupDir, string codexHome, string targetPath)
    {
        string statePath = Path.GetFullPath(Path.Combine(codexHome, AppConstants.GlobalStateFileBasename));
        string backupPath = Path.GetFullPath(Path.Combine(codexHome, AppConstants.GlobalStateBackupFileBasename));
        string normalizedTarget = Path.GetFullPath(targetPath);
        string fileName;
        if (PathsEqual(normalizedTarget, statePath))
        {
            fileName = AppConstants.GlobalStateFileBasename;
        }
        else if (PathsEqual(normalizedTarget, backupPath))
        {
            fileName = AppConstants.GlobalStateBackupFileBasename;
        }
        else
        {
            throw new InvalidOperationException($"Unexpected global-state rollback target: {targetPath}");
        }

        string normalizedBackupDir = Path.GetFullPath(backupDir);
        BackupMetadataFile metadata = JsonSerializer.Deserialize<BackupMetadataFile>(
            await File.ReadAllTextAsync(Path.Combine(normalizedBackupDir, "metadata.json")),
            JsonOptions()) ?? throw new InvalidOperationException($"Backup metadata is invalid: {backupDir}");
        ValidateGlobalStatePresenceMetadata(metadata);
        await RestoreOptionalFileAsync(
            Path.Combine(normalizedBackupDir, fileName),
            normalizedTarget,
            ResolveGlobalStatePresence(metadata, fileName));
    }

    public async Task RestoreGlobalStateFilesAsync(string backupDir, string codexHome)
    {
        string normalizedBackupDir = Path.GetFullPath(backupDir);
        string metadataPath = Path.Combine(normalizedBackupDir, "metadata.json");
        BackupMetadataFile metadata = JsonSerializer.Deserialize<BackupMetadataFile>(
            await File.ReadAllTextAsync(metadataPath),
            JsonOptions()) ?? throw new InvalidOperationException($"Backup metadata is invalid: {backupDir}");
        bool? globalStatePresent = ResolveGlobalStatePresence(
            metadata,
            AppConstants.GlobalStateFileBasename);
        bool? globalStateBackupPresent = ResolveGlobalStatePresence(
            metadata,
            AppConstants.GlobalStateBackupFileBasename);
        await RestoreOptionalFileAsync(
            Path.Combine(normalizedBackupDir, AppConstants.GlobalStateFileBasename),
            Path.Combine(codexHome, AppConstants.GlobalStateFileBasename),
            globalStatePresent);
        await RestoreOptionalFileAsync(
            Path.Combine(normalizedBackupDir, AppConstants.GlobalStateBackupFileBasename),
            Path.Combine(codexHome, AppConstants.GlobalStateBackupFileBasename),
            globalStateBackupPresent);
    }

    public async Task<BackupStorageInfo> GetBackupStorageInfoAsync(string backupDir)
    {
        string metadataPath = Path.Combine(Path.GetFullPath(backupDir), "metadata.json");
        BackupMetadataFile metadata = JsonSerializer.Deserialize<BackupMetadataFile>(
            await File.ReadAllTextAsync(metadataPath),
            JsonOptions()) ?? throw new InvalidOperationException($"Backup metadata is invalid: {backupDir}");
        if (!string.Equals(metadata.Namespace, AppConstants.BackupNamespace, StringComparison.Ordinal)
            || metadata.Version is not (1 or 2))
        {
            throw new InvalidOperationException($"Unsupported backup metadata in {metadataPath}.");
        }
        return new BackupStorageInfo
        {
            Version = metadata.Version,
            SqliteHome = metadata.SqliteHome
        };
    }

    internal async Task RefreshMetadataInventoryAsync(string backupDir)
    {
        string normalizedBackupDir = Path.GetFullPath(backupDir);
        string metadataPath = Path.Combine(normalizedBackupDir, "metadata.json");
        BackupMetadataFile metadata = JsonSerializer.Deserialize<BackupMetadataFile>(
            await File.ReadAllTextAsync(metadataPath),
            JsonOptions()) ?? throw new InvalidOperationException($"Backup metadata is invalid: {backupDir}");
        if (!string.Equals(metadata.Namespace, AppConstants.BackupNamespace, StringComparison.Ordinal)
            || metadata.Version is not (1 or 2))
        {
            throw new InvalidOperationException($"Unsupported backup metadata in {metadataPath}.");
        }

        await WriteMetadataWithInventoryAsync(normalizedBackupDir, metadata);
    }

    public Task<BackupSummary> GetBackupSummaryAsync(string codexHome)
    {
        string backupRoot = AppConstants.DefaultBackupRoot(codexHome);
        return Task.Run(() =>
        {
            if (!Directory.Exists(backupRoot))
            {
                return new BackupSummary
                {
                    Count = 0,
                    TotalBytes = 0
                };
            }

            List<DirectoryInfo> entries = GetManagedBackupDirectories(backupRoot);
            long totalBytes = entries.Sum(entry => GetBackupDirectorySize(entry.FullName));

            return new BackupSummary
            {
                Count = entries.Count,
                TotalBytes = totalBytes
            };
        });
    }

    public async Task<BackupPruneResult> PruneBackupsAsync(string codexHome, int keepCount = AppConstants.DefaultBackupRetentionCount)
    {
        if (keepCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(keepCount), keepCount, "keepCount must be 0 or greater.");
        }

        return await PruneBackupsCoreAsync(codexHome, keepCount, reserveNewBackupSlot: false, null, null);
    }

    /// <summary>
    /// Lists the managed backups that automatic sync/switch cleanup will delete.
    /// The new backup has not been created while planning, so reserve its keep
    /// slot explicitly instead of deriving it from timestamp-like directory names.
    /// </summary>
    public Task<IReadOnlyList<string>> GetAutomaticPruneDeletionCandidatesAsync(
        string codexHome,
        int keepCount)
    {
        if (keepCount < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(keepCount), keepCount, "keepCount must be 1 or greater for automatic cleanup.");
        }

        return GetPruneDeletionCandidatesAsync(codexHome, keepCount, reserveNewBackupSlot: true, null);
    }

    public Task<BackupPruneResult> PruneAutomaticBackupsAsync(
        string codexHome,
        int keepCount,
        string backupDirectory,
        IReadOnlyList<CoreWritePlanTarget>? allowedDeletionTargets = null)
    {
        if (keepCount < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(keepCount), keepCount, "keepCount must be 1 or greater for automatic cleanup.");
        }
        ArgumentException.ThrowIfNullOrWhiteSpace(backupDirectory);

        return PruneBackupsCoreAsync(
            codexHome,
            keepCount,
            reserveNewBackupSlot: true,
            backupDirectory,
            allowedDeletionTargets);
    }

    private async Task<BackupPruneResult> PruneBackupsCoreAsync(
        string codexHome,
        int keepCount,
        bool reserveNewBackupSlot,
        string? preservedBackupDirectory,
        IReadOnlyList<CoreWritePlanTarget>? allowedDeletionTargets)
    {
        string backupRoot = AppConstants.DefaultBackupRoot(codexHome);
        IReadOnlyList<string> candidates = await GetPruneDeletionCandidatesAsync(
            codexHome,
            keepCount,
            reserveNewBackupSlot,
            preservedBackupDirectory);
        if (allowedDeletionTargets is not null)
        {
            await AssertAutomaticPrunePlanMatchesAsync(candidates, allowedDeletionTargets);
        }
        return await Task.Run(() =>
        {
            if (!Directory.Exists(backupRoot))
            {
                return new BackupPruneResult
                {
                    BackupRoot = backupRoot,
                    DeletedCount = 0,
                    RemainingCount = 0,
                    FreedBytes = 0
                };
            }

            List<DirectoryInfo> entries = GetManagedBackupDirectories(backupRoot);
            List<DirectoryInfo> toDelete = entries
                .Where(entry => candidates.Contains(Path.GetFullPath(entry.FullName), PathComparer))
                .ToList();
            long freedBytes = 0;
            foreach (DirectoryInfo entry in toDelete)
            {
                freedBytes += GetBackupDirectorySize(entry.FullName);
                entry.Delete(recursive: true);
            }

            return new BackupPruneResult
            {
                BackupRoot = backupRoot,
                DeletedCount = toDelete.Count,
                RemainingCount = entries.Count - toDelete.Count,
                FreedBytes = freedBytes
            };
        });
    }

    private static async Task AssertAutomaticPrunePlanMatchesAsync(
        IReadOnlyList<string> candidates,
        IReadOnlyList<CoreWritePlanTarget> allowedDeletionTargets)
    {
        HashSet<string> candidatePaths = new(candidates.Select(Path.GetFullPath), PathComparer);
        HashSet<string> allowedPaths = new(
            allowedDeletionTargets.Select(static target => Path.GetFullPath(target.Path)),
            PathComparer);
        if (!candidatePaths.SetEquals(allowedPaths))
        {
            throw new AutomaticPrunePlanStaleException(
                "Automatic backup cleanup was skipped because its deletion candidates no longer match the checked plan.");
        }

        CoreWritePlanSnapshot actual = await CoreWriteSnapshotBuilder.BuildAsync(
            "automatic-prune",
            "checked-cleanup",
            candidates.Select(static path => new CoreWriteTargetSpec(
                path,
                "delete",
                CoreWriteFingerprintMode.RecursiveInventory)));
        CoreWritePlanTarget[] expected = allowedDeletionTargets
            .Select(static target => target with { Path = Path.GetFullPath(target.Path) })
            .OrderBy(static target => target.Path, StringComparer.Ordinal)
            .ThenBy(static target => target.Action, StringComparer.Ordinal)
            .ThenBy(static target => target.Fingerprint, StringComparer.Ordinal)
            .ToArray();
        if (!actual.Targets.SequenceEqual(expected))
        {
            throw new AutomaticPrunePlanStaleException(
                "Automatic backup cleanup was skipped because a planned backup changed after the checked plan was created.");
        }
    }

    private static async Task<IReadOnlyList<string>> GetPruneDeletionCandidatesAsync(
        string codexHome,
        int keepCount,
        bool reserveNewBackupSlot,
        string? preservedBackupDirectory)
    {
        string backupRoot = AppConstants.DefaultBackupRoot(codexHome);
        IReadOnlyList<PendingTransactionInfo> pending = await FileTransactionJournal.FindPendingAsync(codexHome);
        HashSet<string> protectedBackups = new(
            pending.Select(static transaction => Path.GetFullPath(transaction.BackupDir)),
            PathComparer);
        string? preserved = string.IsNullOrWhiteSpace(preservedBackupDirectory)
            ? null
            : Path.GetFullPath(preservedBackupDirectory);

        return await Task.Run<IReadOnlyList<string>>(() =>
        {
            if (!Directory.Exists(backupRoot))
            {
                return [];
            }

            int existingKeepSlots = Math.Max(0, keepCount - (reserveNewBackupSlot ? 1 : 0));
            return GetManagedBackupDirectories(backupRoot)
                .Where(entry => preserved is null || !PathComparer.Equals(Path.GetFullPath(entry.FullName), preserved))
                .Skip(existingKeepSlots)
                .Where(entry => !protectedBackups.Contains(Path.GetFullPath(entry.FullName)))
                .Select(static entry => Path.GetFullPath(entry.FullName))
                .ToArray();
        });
    }

    private static async Task<SessionBackupManifest> SelectSessionEntriesForRestoreAsync(
        string backupDir,
        SessionBackupManifest manifest)
    {
        string journalPath = Path.Combine(backupDir, FileTransactionJournal.FileName);
        if (!File.Exists(journalPath))
        {
            return manifest;
        }

        PendingTransactionInfo journal = await FileTransactionJournal.ReadInfoAsync(journalPath);
        if (string.IsNullOrWhiteSpace(journal.OperationId))
        {
            // A legacy or externally damaged journal cannot authoritatively
            // narrow the immutable backup manifest. An explicit restore is
            // safest when it restores the whole validated manifest.
            return manifest;
        }

        HashSet<string> affectedRollouts = new(
            journal.AffectedTargets
                .Where(static target => target.Kind == "rollout")
                .Select(static target => Path.GetFullPath(target.TargetPath)),
            PathComparer);
        return new SessionBackupManifest
        {
            Version = manifest.Version,
            Namespace = manifest.Namespace,
            CodexHome = manifest.CodexHome,
            TargetProvider = manifest.TargetProvider,
            CreatedAt = manifest.CreatedAt,
            Files = manifest.Files
                .Where(entry => affectedRollouts.Contains(Path.GetFullPath(entry.Path)))
                .ToList()
        };
    }

    private static void ValidateSessionManifest(
        SessionBackupManifest manifest,
        string codexHome,
        string backupDir)
    {
        if (!string.Equals(manifest.Namespace, AppConstants.BackupNamespace, StringComparison.Ordinal)
            || manifest.Version is not (1 or 2))
        {
            throw new InvalidOperationException(
                $"Unsupported session backup manifest in {Path.Combine(backupDir, "session-meta-backup.json")}.");
        }
        if (!PathsEqual(manifest.CodexHome, codexHome))
        {
            throw new InvalidOperationException(
                $"Session backup was created for {manifest.CodexHome}, not {codexHome}.");
        }

        HashSet<string> seen = new(PathComparer);
        foreach (SessionBackupManifestEntry entry in manifest.Files)
        {
            string fullPath = ValidateSessionRestorePath(codexHome, entry.Path);
            _ = entry.ResolveOriginalLastWriteTimeUtcTicks();
            if (!seen.Add(fullPath))
            {
                throw new InvalidOperationException(
                    $"Session backup manifest contains a duplicate rollout path: {entry.Path}");
            }
        }
    }

    private static string ValidateSessionRestorePath(string codexHome, string candidatePath)
    {
        if (string.IsNullOrWhiteSpace(candidatePath))
        {
            throw new InvalidOperationException("Session backup manifest contains an empty rollout path.");
        }

        string fullHome = Path.GetFullPath(codexHome);
        string fullPath = Path.GetFullPath(candidatePath);
        string relativePath = Path.GetRelativePath(fullHome, fullPath);
        string[] segments = relativePath.Split(
            [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
            StringSplitOptions.RemoveEmptyEntries);
        bool validRoot = segments.Length >= 2
            && AppConstants.SessionDirectories.Contains(segments[0], PathComparer);
        if (Path.IsPathRooted(relativePath)
            || relativePath == ".."
            || relativePath.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal)
            || relativePath.StartsWith(".." + Path.AltDirectorySeparatorChar, StringComparison.Ordinal)
            || !validRoot
            || !Path.GetFileName(fullPath).StartsWith("rollout-", StringComparison.Ordinal)
            || !string.Equals(Path.GetExtension(fullPath), ".jsonl", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Session backup path escapes the Codex rollout directories: {candidatePath}");
        }

        string currentPath = fullHome;
        foreach (string segment in segments)
        {
            currentPath = Path.Combine(currentPath, segment);
            if (!File.Exists(currentPath) && !Directory.Exists(currentPath))
            {
                continue;
            }
            if ((File.GetAttributes(currentPath) & FileAttributes.ReparsePoint) != 0)
            {
                throw new InvalidOperationException(
                    $"Session backup path crosses a symbolic link or reparse point: {candidatePath}");
            }
        }
        return fullPath;
    }

    private static StringComparer PathComparer => OperatingSystem.IsWindows()
        ? StringComparer.OrdinalIgnoreCase
        : StringComparer.Ordinal;

    private static async Task<bool> CopyIfPresentAsync(string sourcePath, string destinationPath, bool overwrite)
    {
        return await AtomicFile.CopyAsync(sourcePath, destinationPath, overwrite);
    }

    private static async Task RestoreOptionalFileAsync(
        string sourcePath,
        string destinationPath,
        bool? originallyPresent)
    {
        if (await CopyIfPresentAsync(sourcePath, destinationPath, overwrite: true))
        {
            return;
        }

        if (originallyPresent == true)
        {
            throw new InvalidOperationException(
                $"Backup declares an original file but the backup copy is missing: {sourcePath}");
        }

        // Nullable markers keep metadata v1 and early-v2 backups backward
        // compatible. A new backup explicitly records absence so rollback can
        // remove a .bak file created by the interrupted operation.
        if (originallyPresent == false && File.Exists(destinationPath))
        {
            File.Delete(destinationPath);
        }
    }

    private static bool? ResolveGlobalStatePresence(
        BackupMetadataFile metadata,
        string fileName)
    {
        bool? legacy = fileName switch
        {
            AppConstants.GlobalStateFileBasename => metadata.GlobalStateFilePresent,
            AppConstants.GlobalStateBackupFileBasename => metadata.GlobalStateBackupFilePresent,
            _ => throw new InvalidOperationException(
                $"Unsupported global-state metadata key: {fileName}")
        };
        if (metadata.GlobalStateFiles is null)
        {
            return legacy;
        }
        if (!metadata.GlobalStateFiles.TryGetValue(fileName, out bool canonical))
        {
            throw new InvalidOperationException(
                $"Backup globalStateFiles is missing required key {fileName}.");
        }
        if (legacy is not null && legacy.Value != canonical)
        {
            throw new InvalidOperationException(
                $"Backup global-state metadata disagrees for {fileName}.");
        }
        return canonical;
    }

    private static void ValidateGlobalStatePresenceMetadata(BackupMetadataFile metadata)
    {
        _ = ResolveGlobalStatePresence(metadata, AppConstants.GlobalStateFileBasename);
        _ = ResolveGlobalStatePresence(metadata, AppConstants.GlobalStateBackupFileBasename);
    }

    private static string? SafeRelativePath(string root, string target)
    {
        string relativePath = Path.GetRelativePath(root, target);
        return !string.IsNullOrEmpty(relativePath)
            && !relativePath.StartsWith("..", StringComparison.Ordinal)
            && !Path.IsPathRooted(relativePath)
            ? relativePath
            : null;
    }

    private static string RestoreDbTargetPath(string codexHome, string relativePath)
    {
        if (Path.IsPathRooted(relativePath)
            || relativePath.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar).Contains("..", StringComparer.Ordinal))
        {
            throw new InvalidOperationException($"Invalid database backup path: {relativePath}");
        }

        return Path.Combine(codexHome, relativePath);
    }

    private static string RestoreSqliteTargetPath(string sqliteHome, string relativePath)
    {
        if (Path.IsPathRooted(relativePath)
            || relativePath.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar).Contains("..", StringComparer.Ordinal))
        {
            throw new InvalidOperationException($"Invalid SQLite backup path: {relativePath}");
        }

        return Path.Combine(sqliteHome, relativePath);
    }

    private static string ResolveRestoreSqliteHome(
        CodexStorageLayout storage,
        BackupMetadataFile metadata,
        StateDbLocation? stateDb)
    {
        if (stateDb is not null)
        {
            return Path.GetDirectoryName(stateDb.Path)!;
        }
        if (metadata.Version >= 2
            && !string.IsNullOrWhiteSpace(metadata.SqliteHome)
            && !storage.HasConfiguredSqliteHome)
        {
            StateDbLocation? matchingCandidate = storage.StateDbCandidates.FirstOrDefault(
                candidate => PathsEqual(Path.GetDirectoryName(candidate.Path)!, metadata.SqliteHome));
            if (matchingCandidate is not null)
            {
                return Path.GetDirectoryName(matchingCandidate.Path)!;
            }
        }
        return storage.SqliteHome;
    }

    private static bool PathsEqual(string left, string right)
    {
        StringComparison comparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
        return string.Equals(Path.GetFullPath(left), Path.GetFullPath(right), comparison);
    }

    private static JsonSerializerOptions JsonOptions()
    {
        return new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true
        };
    }

    private async Task WriteMetadataWithInventoryAsync(string backupDir, BackupMetadataFile metadata)
    {
        string metadataPath = Path.Combine(backupDir, "metadata.json");
        (long payloadBytes, int payloadFileCount) = GetDirectoryInventory(
            backupDir,
            metadataPath);
        int fileCount = checked(payloadFileCount + 1);
        long sizeBytes = 0;
        string serialized = string.Empty;
        for (int attempt = 0; attempt < 8; attempt += 1)
        {
            BackupMetadataFile withInventory = CopyMetadataWithInventory(metadata, sizeBytes, fileCount);
            serialized = JsonSerializer.Serialize(withInventory, JsonOptions());
            long nextSizeBytes = checked(payloadBytes + Encoding.UTF8.GetByteCount(serialized));
            if (nextSizeBytes == sizeBytes)
            {
                break;
            }
            sizeBytes = nextSizeBytes;
        }

        BackupMetadataFile finalMetadata = CopyMetadataWithInventory(metadata, sizeBytes, fileCount);
        serialized = JsonSerializer.Serialize(finalMetadata, JsonOptions());
        long verifiedSizeBytes = checked(payloadBytes + Encoding.UTF8.GetByteCount(serialized));
        if (verifiedSizeBytes != sizeBytes)
        {
            finalMetadata = CopyMetadataWithInventory(metadata, verifiedSizeBytes, fileCount);
            serialized = JsonSerializer.Serialize(finalMetadata, JsonOptions());
        }
        await AtomicFile.WriteAllTextAsync(
            metadataPath,
            serialized,
            faultInjector: AtomicWriteFaultInjector);
    }

    private static BackupMetadataFile CopyMetadataWithInventory(
        BackupMetadataFile metadata,
        long sizeBytes,
        int fileCount)
    {
        return new BackupMetadataFile
        {
            Version = metadata.Version,
            Namespace = metadata.Namespace,
            CodexHome = metadata.CodexHome,
            SqliteHome = metadata.SqliteHome,
            TargetProvider = metadata.TargetProvider,
            CreatedAt = metadata.CreatedAt,
            DbFiles = metadata.DbFiles,
            SqliteDbFiles = metadata.SqliteDbFiles,
            ChangedSessionFiles = metadata.ChangedSessionFiles,
            GlobalStateFiles = metadata.GlobalStateFiles,
            GlobalStateFilePresent = metadata.GlobalStateFilePresent,
            GlobalStateBackupFilePresent = metadata.GlobalStateBackupFilePresent,
            SizeBytes = sizeBytes,
            FileCount = fileCount
        };
    }

    private long GetBackupDirectorySize(string directoryPath)
    {
        if (TryReadCachedDirectoryInventory(directoryPath, out long sizeBytes))
        {
            return sizeBytes;
        }
        DirectoryInventoryFallbackObserver?.Invoke(directoryPath);
        return GetDirectoryInventory(directoryPath).SizeBytes;
    }

    private static bool TryReadCachedDirectoryInventory(string directoryPath, out long sizeBytes)
    {
        sizeBytes = 0;
        string metadataPath = Path.Combine(directoryPath, "metadata.json");
        try
        {
            using JsonDocument document = JsonDocument.Parse(File.ReadAllText(metadataPath));
            JsonElement root = document.RootElement;
            if (!root.TryGetProperty("namespace", out JsonElement namespaceValue)
                || !string.Equals(namespaceValue.GetString(), AppConstants.BackupNamespace, StringComparison.Ordinal)
                || !root.TryGetProperty("sizeBytes", out JsonElement sizeValue)
                || !sizeValue.TryGetInt64(out long cachedSize)
                || cachedSize < 0
                || !root.TryGetProperty("fileCount", out JsonElement countValue)
                || !countValue.TryGetInt32(out int cachedFileCount)
                || cachedFileCount < 1)
            {
                return false;
            }
            sizeBytes = cachedSize;
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static (long SizeBytes, int FileCount) GetDirectoryInventory(
        string directoryPath,
        string? excludedFilePath = null)
    {
        if (!Directory.Exists(directoryPath))
        {
            return (0, 0);
        }

        string? excluded = string.IsNullOrWhiteSpace(excludedFilePath)
            ? null
            : Path.GetFullPath(excludedFilePath);
        long sizeBytes = 0;
        int fileCount = 0;
        foreach (string filePath in Directory.EnumerateFiles(directoryPath, "*", SearchOption.AllDirectories))
        {
            if (excluded is not null && PathsEqual(filePath, excluded))
            {
                continue;
            }
            sizeBytes = checked(sizeBytes + new FileInfo(filePath).Length);
            fileCount = checked(fileCount + 1);
        }
        return (sizeBytes, fileCount);
    }

    private static List<DirectoryInfo> GetManagedBackupDirectories(string backupRoot)
    {
        return new DirectoryInfo(backupRoot)
            .EnumerateDirectories()
            .Where(static entry => IsManagedBackupDirectory(entry.FullName))
            .OrderByDescending(static entry => entry.Name, StringComparer.Ordinal)
            .ToList();
    }

    private sealed class AutomaticPrunePlanStaleException(string message) : InvalidOperationException(message);

    private static bool IsManagedBackupDirectory(string backupDirectoryPath)
    {
        string metadataPath = Path.Combine(backupDirectoryPath, "metadata.json");
        if (!File.Exists(metadataPath))
        {
            return false;
        }

        try
        {
            BackupMetadataValidationFile? metadata = JsonSerializer.Deserialize<BackupMetadataValidationFile>(
                File.ReadAllText(metadataPath),
                JsonOptions());
            return string.Equals(metadata?.Namespace, AppConstants.BackupNamespace, StringComparison.Ordinal);
        }
        catch
        {
            return false;
        }
    }

    private sealed class BackupMetadataValidationFile
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
    }
}

internal sealed record BackupRecoveryCoverage(bool Config, bool Database, bool Sessions);
