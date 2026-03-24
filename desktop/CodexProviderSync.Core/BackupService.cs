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
        string backupRoot = AppConstants.DefaultBackupRoot(codexHome);
        string backupDir = Path.Combine(backupRoot, DateTimeOffset.UtcNow.ToString("yyyyMMdd'T'HHmmssfff'Z'"));
        string dbDir = Path.Combine(backupDir, "db");
        Directory.CreateDirectory(dbDir);

        List<string> copiedDbFiles = [];
        foreach (string suffix in new[] { string.Empty, "-shm", "-wal" })
        {
            string fileName = $"{AppConstants.DbFileBasename}{suffix}";
            if (await CopyIfPresentAsync(Path.Combine(codexHome, fileName), Path.Combine(dbDir, fileName), overwrite: false))
            {
                copiedDbFiles.Add(fileName);
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

        DateTimeOffset createdAt = DateTimeOffset.UtcNow;
        SessionBackupManifest sessionManifest = new()
        {
            Version = 1,
            Namespace = AppConstants.BackupNamespace,
            CodexHome = codexHome,
            TargetProvider = targetProvider,
            CreatedAt = createdAt,
            Files = sessionChanges.Select(static change => new SessionBackupManifestEntry
            {
                Path = change.Path,
                OriginalFirstLine = change.OriginalFirstLine,
                OriginalSeparator = change.OriginalSeparator
            }).ToList()
        };
        await File.WriteAllTextAsync(
            Path.Combine(backupDir, "session-meta-backup.json"),
            JsonSerializer.Serialize(sessionManifest, JsonOptions()));

        BackupMetadataFile metadata = new()
        {
            Version = 1,
            Namespace = AppConstants.BackupNamespace,
            CodexHome = codexHome,
            TargetProvider = targetProvider,
            CreatedAt = createdAt,
            DbFiles = copiedDbFiles,
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
        options ??= new RestoreBackupOptions();
        string normalizedBackupDir = Path.GetFullPath(backupDir);
        BackupMetadataFile metadata = JsonSerializer.Deserialize<BackupMetadataFile>(
            await File.ReadAllTextAsync(Path.Combine(normalizedBackupDir, "metadata.json")),
            JsonOptions()) ?? throw new InvalidOperationException($"Backup metadata is invalid: {backupDir}");

        if (!string.Equals(metadata.CodexHome, codexHome, StringComparison.Ordinal))
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

        if (options.RestoreConfig)
        {
            await CopyIfPresentAsync(
                Path.Combine(normalizedBackupDir, "config.toml"),
                Path.Combine(codexHome, "config.toml"),
                overwrite: true);
        }

        if (options.RestoreDatabase)
        {
            await _sqliteStateService.AssertSqliteWritableAsync(codexHome);
            string dbDir = Path.Combine(normalizedBackupDir, "db");
            HashSet<string> backedUpFiles = new(metadata.DbFiles, StringComparer.Ordinal);

            foreach (string suffix in new[] { string.Empty, "-shm", "-wal" })
            {
                string fileName = $"{AppConstants.DbFileBasename}{suffix}";
                string targetPath = Path.Combine(codexHome, fileName);
                if (!backedUpFiles.Contains(fileName) && File.Exists(targetPath))
                {
                    File.Delete(targetPath);
                }
            }

            foreach (string fileName in metadata.DbFiles)
            {
                await CopyIfPresentAsync(Path.Combine(dbDir, fileName), Path.Combine(codexHome, fileName), overwrite: true);
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
            Version = sessionManifest.Version,
            Namespace = sessionManifest.Namespace,
            CodexHome = sessionManifest.CodexHome,
            TargetProvider = sessionManifest.TargetProvider,
            CreatedAt = sessionManifest.CreatedAt,
            Files = sessionChanges.Select(static change => new SessionBackupManifestEntry
            {
                Path = change.Path,
                OriginalFirstLine = change.OriginalFirstLine,
                OriginalSeparator = change.OriginalSeparator
            }).ToList()
        };
        metadata = new BackupMetadataFile
        {
            Version = metadata.Version,
            Namespace = metadata.Namespace,
            CodexHome = metadata.CodexHome,
            TargetProvider = metadata.TargetProvider,
            CreatedAt = metadata.CreatedAt,
            DbFiles = metadata.DbFiles,
            ChangedSessionFiles = sessionChanges.Count
        };

        await File.WriteAllTextAsync(manifestPath, JsonSerializer.Serialize(sessionManifest, JsonOptions()));
        await File.WriteAllTextAsync(metadataPath, JsonSerializer.Serialize(metadata, JsonOptions()));
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
