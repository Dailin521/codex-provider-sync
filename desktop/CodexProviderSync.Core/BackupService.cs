using System.Text.Json;

namespace CodexProviderSync.Core;

public sealed class BackupService
{
    private readonly SessionRolloutService _sessionRolloutService;
    private readonly SqliteStateService _sqliteStateService;

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
            foreach (string suffix in new[] { string.Empty, "-shm", "-wal" })
            {
                string sourcePath = stateDb.Path + suffix;
                string sqliteRelativePath = AppConstants.DbFileBasename + suffix;
                if (!await CopyIfPresentAsync(
                    sourcePath,
                    Path.Combine(dbDir, "sqlite-home", sqliteRelativePath),
                    overwrite: false))
                {
                    continue;
                }

                copiedSqliteDbFiles.Add(sqliteRelativePath);
                string? legacyRelativePath = SafeRelativePath(codexHome, sourcePath);
                if (legacyRelativePath is not null)
                {
                    await CopyIfPresentAsync(sourcePath, Path.Combine(dbDir, legacyRelativePath), overwrite: false);
                    copiedDbFiles.Add(legacyRelativePath);
                }
            }
        }

        string configBackupPath = Path.Combine(backupDir, "config.toml");
        if (configBackupText is not null)
        {
            await File.WriteAllTextAsync(configBackupPath, configBackupText);
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
            Files = sessionChanges.Select(static change => new SessionBackupManifestEntry
            {
                Path = change.Path,
                OriginalFirstLine = change.OriginalFirstLine,
                OriginalSeparator = change.OriginalSeparator,
                OriginalLastWriteTimeUtcTicks = change.OriginalLastWriteTimeUtcTicks,
                ModelOnlyChange = change.ModelOnlyChange,
                OriginalTurnContextModels = [.. change.OriginalTurnContextModels]
            }).ToList()
        };
        await File.WriteAllTextAsync(
            Path.Combine(backupDir, "session-meta-backup.json"),
            JsonSerializer.Serialize(sessionManifest, JsonOptions()));

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
            ChangedSessionFiles = sessionChanges.Count
        };
        await File.WriteAllTextAsync(
            Path.Combine(backupDir, "metadata.json"),
            JsonSerializer.Serialize(metadata, JsonOptions()));

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

        SessionBackupManifest? sessionManifest = null;
        if (options.RestoreSessions)
        {
            sessionManifest = JsonSerializer.Deserialize<SessionBackupManifest>(
                await File.ReadAllTextAsync(Path.Combine(normalizedBackupDir, "session-meta-backup.json")),
                JsonOptions()) ?? throw new InvalidOperationException($"Session backup manifest is invalid: {backupDir}");

            await _sessionRolloutService.AssertSessionFilesWritableAsync(
                sessionManifest.Files.Select(static entry => entry.Path));
        }

        List<(string SourcePath, string TargetPath)> databaseEntries = [];
        List<string> sidecarsToRemove = [];
        if (options.RestoreDatabase)
        {
            StateDbLocation? stateDb = storage.StateDbLocation ?? _sqliteStateService.DetectStateDb(storage);
            if (stateDb is null && storage.HasConfiguredSqliteHome)
            {
                throw new InvalidOperationException(
                    $"state_5.sqlite not found in SQLite home {storage.SqliteHome}.");
            }

            string targetSqliteHome = stateDb is null ? storage.SqliteHome : Path.GetDirectoryName(stateDb.Path)!;
            if (stateDb is not null
                && metadata.Version >= 2
                && !string.IsNullOrWhiteSpace(metadata.SqliteHome)
                && !PathsEqual(metadata.SqliteHome, targetSqliteHome)
                && !options.AllowSqliteHomeRelocation)
            {
                throw new InvalidOperationException(
                    $"Backup SQLite home is {metadata.SqliteHome}, but the current target is {targetSqliteHome}. "
                    + "Confirm SQLite Home relocation before restoring to a different location.");
            }

            if (stateDb is not null)
            {
                CodexStorageLayout detectedStorage = storage with { StateDbLocation = stateDb };
                await _sqliteStateService.AssertSqliteWritableAsync(detectedStorage);

                IReadOnlyList<string> databaseFiles = metadata.Version >= 2
                    ? metadata.SqliteDbFiles ?? []
                    : metadata.DbFiles ?? [];
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
                    databaseEntries.Add((sourcePath, targetPath));
                }

                HashSet<string> backedUpFiles = new(databaseFiles, StringComparer.Ordinal);
                foreach (string baseFile in databaseFiles.Where(
                    static fileName => Path.GetFileName(fileName) == AppConstants.DbFileBasename))
                {
                    string basePath = metadata.Version >= 2
                        ? RestoreSqliteTargetPath(restoreRoot, baseFile)
                        : RestoreDbTargetPath(restoreRoot, baseFile);
                    foreach (string suffix in new[] { "-shm", "-wal" })
                    {
                        if (!backedUpFiles.Contains(baseFile + suffix))
                        {
                            sidecarsToRemove.Add(basePath + suffix);
                        }
                    }
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

        if (options.RestoreDatabase)
        {
            foreach (string sidecarPath in sidecarsToRemove)
            {
                if (File.Exists(sidecarPath))
                {
                    File.Delete(sidecarPath);
                }
            }

            foreach ((string sourcePath, string targetPath) in databaseEntries)
            {
                Directory.CreateDirectory(Path.GetDirectoryName(targetPath)!);
                File.Copy(sourcePath, targetPath, overwrite: true);
            }
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
            Files = sessionChanges.Select(static change => new SessionBackupManifestEntry
            {
                Path = change.Path,
                OriginalFirstLine = change.OriginalFirstLine,
                OriginalSeparator = change.OriginalSeparator,
                OriginalLastWriteTimeUtcTicks = change.OriginalLastWriteTimeUtcTicks,
                ModelOnlyChange = change.ModelOnlyChange,
                OriginalTurnContextModels = [.. change.OriginalTurnContextModels]
            }).ToList()
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
            ChangedSessionFiles = sessionChanges.Count
        };

        await File.WriteAllTextAsync(manifestPath, JsonSerializer.Serialize(sessionManifest, JsonOptions()));
        await File.WriteAllTextAsync(metadataPath, JsonSerializer.Serialize(metadata, JsonOptions()));
    }

    public async Task RestoreGlobalStateFilesAsync(string backupDir, string codexHome)
    {
        string normalizedBackupDir = Path.GetFullPath(backupDir);
        await CopyIfPresentAsync(
            Path.Combine(normalizedBackupDir, AppConstants.GlobalStateFileBasename),
            Path.Combine(codexHome, AppConstants.GlobalStateFileBasename),
            overwrite: true);
        await CopyIfPresentAsync(
            Path.Combine(normalizedBackupDir, AppConstants.GlobalStateBackupFileBasename),
            Path.Combine(codexHome, AppConstants.GlobalStateBackupFileBasename),
            overwrite: true);
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
            long totalBytes = entries.Sum(static entry => GetDirectorySize(entry.FullName));

            return new BackupSummary
            {
                Count = entries.Count,
                TotalBytes = totalBytes
            };
        });
    }

    public Task<BackupPruneResult> PruneBackupsAsync(string codexHome, int keepCount = AppConstants.DefaultBackupRetentionCount)
    {
        if (keepCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(keepCount), keepCount, "keepCount must be 0 or greater.");
        }

        string backupRoot = AppConstants.DefaultBackupRoot(codexHome);
        return Task.Run(() =>
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

            List<DirectoryInfo> toDelete = entries.Skip(keepCount).ToList();
            long freedBytes = 0;
            foreach (DirectoryInfo entry in toDelete)
            {
                freedBytes += GetDirectorySize(entry.FullName);
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

    private static async Task<bool> CopyIfPresentAsync(string sourcePath, string destinationPath, bool overwrite)
    {
        if (!File.Exists(sourcePath))
        {
            return false;
        }

        Directory.CreateDirectory(Path.GetDirectoryName(destinationPath)!);
        File.Copy(sourcePath, destinationPath, overwrite);
        await Task.CompletedTask;
        return true;
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

    private static long GetDirectorySize(string directoryPath)
    {
        if (!Directory.Exists(directoryPath))
        {
            return 0;
        }

        return Directory
            .EnumerateFiles(directoryPath, "*", SearchOption.AllDirectories)
            .Sum(static filePath => new FileInfo(filePath).Length);
    }

    private static List<DirectoryInfo> GetManagedBackupDirectories(string backupRoot)
    {
        return new DirectoryInfo(backupRoot)
            .EnumerateDirectories()
            .Where(static entry => IsManagedBackupDirectory(entry.FullName))
            .OrderByDescending(static entry => entry.Name, StringComparer.Ordinal)
            .ToList();
    }

    private static bool IsManagedBackupDirectory(string backupDirectoryPath)
    {
        string metadataPath = Path.Combine(backupDirectoryPath, "metadata.json");
        if (!File.Exists(metadataPath))
        {
            return false;
        }

        try
        {
            BackupMetadataFile? metadata = JsonSerializer.Deserialize<BackupMetadataFile>(
                File.ReadAllText(metadataPath),
                JsonOptions());
            return string.Equals(metadata?.Namespace, AppConstants.BackupNamespace, StringComparison.Ordinal);
        }
        catch
        {
            return false;
        }
    }
}
