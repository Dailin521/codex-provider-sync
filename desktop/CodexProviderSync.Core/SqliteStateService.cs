using System.Buffers.Binary;
using Microsoft.Data.Sqlite;

namespace CodexProviderSync.Core;

public sealed record SqliteFileMetadata(
    string JournalMode,
    long PageSize,
    long UserVersion,
    long ApplicationId);

public sealed record SqliteOnlineBackupPreservation(
    bool JournalMode,
    bool PageSize,
    bool UserVersion,
    bool ApplicationId);

public sealed record SqliteOnlineBackupMetadata(
    SqliteFileMetadata Source,
    SqliteFileMetadata Backup,
    SqliteOnlineBackupPreservation Preserved);

public sealed record SqliteOnlineBackupResult(
    bool DatabasePresent,
    string? BackupPath,
    SqliteOnlineBackupMetadata? Metadata);

public sealed class SqliteStateService
{
    private const int DefaultBusyTimeoutMs = 5000;

    private sealed record StateDbCandidateStats(
        StateDbLocation Location,
        int Priority,
        long ThreadCount,
        long MaxThreadTimestampMs,
        long LastWriteTimeUtcTicks,
        long RolloutDistance);

    static SqliteStateService()
    {
        SQLitePCL.Batteries_V2.Init();
    }

    public string StateDbPath(string codexHome)
    {
        return Path.Combine(codexHome, AppConstants.SqliteDirBasename, AppConstants.DbFileBasename);
    }

    public string LegacyStateDbPath(string codexHome)
    {
        return Path.Combine(codexHome, AppConstants.DbFileBasename);
    }

    public IReadOnlyList<StateDbLocation> StateDbCandidates(string codexHome)
    {
        return new CodexStorageLayoutService().CreateDefault(codexHome).StateDbCandidates;
    }

    public StateDbLocation? DetectStateDb(string codexHome)
    {
        return DetectStateDb(new CodexStorageLayoutService().CreateDefault(codexHome));
    }

    public StateDbLocation? DetectStateDb(CodexStorageLayout storage)
    {
        List<(StateDbLocation Location, int Priority)> existingCandidates = [];
        IReadOnlyList<StateDbLocation> candidates = storage.StateDbCandidates;
        for (int index = 0; index < candidates.Count; index += 1)
        {
            StateDbLocation candidate = candidates[index];
            if (File.Exists(candidate.Path))
            {
                existingCandidates.Add((candidate, index));
            }
        }

        if (existingCandidates.Count == 0)
        {
            return null;
        }

        long rolloutCount = CountRolloutFiles(storage.CodexHome);
        List<StateDbCandidateStats> readableCandidates = [];
        foreach ((StateDbLocation candidate, int priority) in existingCandidates)
        {
            try
            {
                StateDbCandidateStats stats = ReadStateDbCandidateStats(candidate, priority, rolloutCount);
                readableCandidates.Add(stats);
            }
            catch
            {
                // Keep unreadable candidates as a fallback so existing status/error
                // handling still points at state_5.sqlite when no usable DB exists.
            }
        }

        if (readableCandidates.Count == 0)
        {
            return existingCandidates[0].Location;
        }

        return readableCandidates
            .OrderBy(static candidate => candidate.RolloutDistance)
            .ThenByDescending(static candidate => candidate.ThreadCount)
            .ThenByDescending(static candidate => candidate.MaxThreadTimestampMs)
            .ThenByDescending(static candidate => candidate.LastWriteTimeUtcTicks)
            .ThenBy(static candidate => candidate.Priority)
            .First()
            .Location;
    }

    public string? ExistingStateDbPath(string codexHome)
    {
        return DetectStateDb(codexHome)?.Path;
    }

    public string? ExistingStateDbPath(CodexStorageLayout storage)
    {
        return storage.StateDbLocation?.Path ?? DetectStateDb(storage)?.Path;
    }

    public async Task<ProviderCounts?> ReadSqliteProviderCountsAsync(string codexHome)
    {
        return await ReadSqliteProviderCountsAsync(new CodexStorageLayoutService().CreateDefault(codexHome));
    }

    public async Task<ProviderCounts?> ReadSqliteProviderCountsAsync(CodexStorageLayout storage)
    {
        string? dbPath = ExistingStateDbPath(storage);
        if (dbPath is null)
        {
            return null;
        }

        try
        {
            await using SqliteConnection connection = OpenConnection(dbPath, SqliteOpenMode.ReadOnly);
            await connection.OpenAsync();
            await using SqliteCommand command = connection.CreateCommand();
            command.CommandText = """
                SELECT
                  CASE
                    WHEN model_provider IS NULL OR model_provider = '' THEN '(missing)'
                    ELSE model_provider
                  END AS model_provider,
                  archived,
                  COUNT(*) AS count
                FROM threads
                GROUP BY model_provider, archived
                ORDER BY archived, model_provider
                """;

            Dictionary<string, int> sessions = new(StringComparer.Ordinal);
            Dictionary<string, int> archivedSessions = new(StringComparer.Ordinal);
            await using SqliteDataReader reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                string provider = reader.GetString(0);
                bool archived = reader.GetInt64(1) != 0;
                int count = reader.GetInt32(2);
                Dictionary<string, int> bucket = archived ? archivedSessions : sessions;
                bucket[provider] = count;
            }

            return new ProviderCounts
            {
                Sessions = sessions,
                ArchivedSessions = archivedSessions
            };
        }
        catch (Exception error) when (IsSqliteMalformedError(error))
        {
            return new ProviderCounts
            {
                Unreadable = true,
                Error = "state_5.sqlite is malformed or unreadable"
            };
        }
        catch (Exception error) when (IsSqliteBusyError(error))
        {
            return new ProviderCounts
            {
                Unreadable = true,
                Error = "state_5.sqlite is currently in use"
            };
        }
    }

    public async Task<SqliteRepairStats?> ReadSqliteRepairStatsAsync(
        string codexHome,
        IReadOnlyCollection<string>? userEventThreadIds = null,
        IReadOnlyDictionary<string, string>? threadCwdsById = null)
    {
        return await ReadSqliteRepairStatsAsync(
            new CodexStorageLayoutService().CreateDefault(codexHome),
            userEventThreadIds,
            threadCwdsById);
    }

    public async Task<SqliteRepairStats?> ReadSqliteRepairStatsAsync(
        CodexStorageLayout storage,
        IReadOnlyCollection<string>? userEventThreadIds = null,
        IReadOnlyDictionary<string, string>? threadCwdsById = null)
    {
        string? dbPath = ExistingStateDbPath(storage);
        if (dbPath is null)
        {
            return null;
        }

        try
        {
            await using SqliteConnection connection = OpenConnection(dbPath, SqliteOpenMode.ReadOnly);
            await connection.OpenAsync();

            int userEventRowsNeedingRepair = 0;
            if (userEventThreadIds?.Count > 0 && await TableHasColumnAsync(connection, "threads", "has_user_event"))
            {
                await using SqliteCommand userEventCommand = connection.CreateCommand();
                userEventCommand.CommandText = "SELECT has_user_event FROM threads WHERE id = $id";
                SqliteParameter idParameter = userEventCommand.Parameters.Add("$id", SqliteType.Text);
                foreach (string threadId in userEventThreadIds)
                {
                    idParameter.Value = threadId;
                    object? value = await userEventCommand.ExecuteScalarAsync();
                    if (value is not null && value is not DBNull && Convert.ToInt64(value) != 1)
                    {
                        userEventRowsNeedingRepair += 1;
                    }
                }
            }

            int cwdRowsNeedingRepair = 0;
            if (threadCwdsById?.Count > 0 && await TableHasColumnAsync(connection, "threads", "cwd"))
            {
                await using SqliteCommand cwdCommand = connection.CreateCommand();
                cwdCommand.CommandText = "SELECT cwd FROM threads WHERE id = $id";
                SqliteParameter idParameter = cwdCommand.Parameters.Add("$id", SqliteType.Text);
                foreach ((string threadId, string expectedCwd) in threadCwdsById)
                {
                    if (string.IsNullOrWhiteSpace(threadId) || string.IsNullOrWhiteSpace(expectedCwd))
                    {
                        continue;
                    }

                    idParameter.Value = threadId;
                    object? value = await cwdCommand.ExecuteScalarAsync();
                    if (value is not null
                        && value is not DBNull
                        && !string.Equals(Convert.ToString(value), expectedCwd, StringComparison.Ordinal))
                    {
                        cwdRowsNeedingRepair += 1;
                    }
                }
            }

            return new SqliteRepairStats
            {
                UserEventRowsNeedingRepair = userEventRowsNeedingRepair,
                CwdRowsNeedingRepair = cwdRowsNeedingRepair
            };
        }
        catch (Exception error)
        {
            throw WrapSqliteMalformedError(
                WrapSqliteBusyError(error, "read SQLite repair diagnostics"),
                "read SQLite repair diagnostics");
        }
    }

    public async Task<bool> AssertSqliteWritableAsync(string codexHome, int? busyTimeoutMs = null)
    {
        return await AssertSqliteWritableAsync(
            new CodexStorageLayoutService().CreateDefault(codexHome),
            busyTimeoutMs);
    }

    public async Task<bool> AssertSqliteWritableAsync(CodexStorageLayout storage, int? busyTimeoutMs = null)
    {
        string? dbPath = ExistingStateDbPath(storage);
        if (dbPath is null)
        {
            return false;
        }

        await using SqliteConnection connection = OpenConnection(dbPath, SqliteOpenMode.ReadWriteCreate);
        try
        {
            await connection.OpenAsync();
            await SetBusyTimeoutAsync(connection, busyTimeoutMs);
            await ConfigureSqliteWriteDurabilityAsync(connection);
            await ExecuteNonQueryAsync(connection, "BEGIN IMMEDIATE");
            await ExecuteNonQueryAsync(connection, "ROLLBACK");
            return true;
        }
        catch (Exception error)
        {
            throw WrapSqliteMalformedError(
                WrapSqliteBusyError(error, "update session provider metadata"),
                "update session provider metadata");
        }
    }

    public async Task<(int UpdatedRows, int ProviderRowsUpdated, int ModelRowsUpdated, int UserEventRowsUpdated, int CwdRowsUpdated, bool DatabasePresent)> UpdateSqliteProviderAsync(
        string codexHome,
        string targetProvider,
        string? targetModel = null,
        Func<(int UpdatedRows, int ProviderRowsUpdated, int ModelRowsUpdated, int UserEventRowsUpdated, int CwdRowsUpdated, bool DatabasePresent), Task>? afterUpdate = null,
        int? busyTimeoutMs = null,
        IReadOnlyCollection<string>? userEventThreadIds = null,
        IReadOnlyDictionary<string, string>? threadCwdsById = null,
        Action<(int UpdatedRows, int ProviderRowsUpdated, int ModelRowsUpdated, int UserEventRowsUpdated, int CwdRowsUpdated, bool DatabasePresent)>? onCommitAttempt = null,
        Func<Task>? afterCommitBeforeAcknowledgement = null)
    {
        return await UpdateSqliteProviderAsync(
            new CodexStorageLayoutService().CreateDefault(codexHome),
            targetProvider,
            targetModel,
            afterUpdate,
            busyTimeoutMs,
            userEventThreadIds,
            threadCwdsById,
            onCommitAttempt,
            afterCommitBeforeAcknowledgement);
    }

    public async Task<(int UpdatedRows, int ProviderRowsUpdated, int ModelRowsUpdated, int UserEventRowsUpdated, int CwdRowsUpdated, bool DatabasePresent)> UpdateSqliteProviderAsync(
        CodexStorageLayout storage,
        string targetProvider,
        string? targetModel = null,
        Func<(int UpdatedRows, int ProviderRowsUpdated, int ModelRowsUpdated, int UserEventRowsUpdated, int CwdRowsUpdated, bool DatabasePresent), Task>? afterUpdate = null,
        int? busyTimeoutMs = null,
        IReadOnlyCollection<string>? userEventThreadIds = null,
        IReadOnlyDictionary<string, string>? threadCwdsById = null,
        Action<(int UpdatedRows, int ProviderRowsUpdated, int ModelRowsUpdated, int UserEventRowsUpdated, int CwdRowsUpdated, bool DatabasePresent)>? onCommitAttempt = null,
        Func<Task>? afterCommitBeforeAcknowledgement = null)
    {
        string? dbPath = ExistingStateDbPath(storage);
        if (dbPath is null)
        {
            if (afterUpdate is not null)
            {
                await afterUpdate((0, 0, 0, 0, 0, false));
            }

            return (0, 0, 0, 0, 0, false);
        }

        await using SqliteConnection connection = OpenConnection(dbPath, SqliteOpenMode.ReadWriteCreate);
        bool transactionOpen = false;
        try
        {
            await connection.OpenAsync();
            await SetBusyTimeoutAsync(connection, busyTimeoutMs);
            await ConfigureSqliteWriteDurabilityAsync(connection);
            await ExecuteNonQueryAsync(connection, "BEGIN IMMEDIATE");
            transactionOpen = true;

            await using SqliteCommand command = connection.CreateCommand();
            command.CommandText = """
                UPDATE threads
                SET model_provider = $provider
                WHERE COALESCE(model_provider, '') <> $provider
                """;
            command.Parameters.AddWithValue("$provider", targetProvider);
            int providerRowsUpdated = await command.ExecuteNonQueryAsync();

            // When a target model is provided, align every thread's `model`
            // column with it alongside `model_provider`. This is what makes
            // the bottom-right of the Codex UI show the active model for old
            // sessions, instead of the name that was in effect when each
            // thread was originally created. The `model` column is only
            // present in newer Codex schemas, so guard with TableHasColumn
            // to keep legacy layouts working.
            int modelRowsUpdated = 0;
            if (!string.IsNullOrEmpty(targetModel) && await TableHasColumnAsync(connection, "threads", "model"))
            {
                await using SqliteCommand modelCommand = connection.CreateCommand();
                modelCommand.CommandText = """
                    UPDATE threads
                    SET model = $model
                    WHERE COALESCE(model, '') <> $model
                    """;
                modelCommand.Parameters.AddWithValue("$model", targetModel);
                modelRowsUpdated = await modelCommand.ExecuteNonQueryAsync();
            }
            int userEventRowsUpdated = 0;
            if (userEventThreadIds?.Count > 0 && await TableHasColumnAsync(connection, "threads", "has_user_event"))
            {
                await using SqliteCommand userEventCommand = connection.CreateCommand();
                userEventCommand.CommandText = """
                    UPDATE threads
                    SET has_user_event = 1
                    WHERE id = $id AND COALESCE(has_user_event, 0) <> 1
                    """;
                SqliteParameter idParameter = userEventCommand.Parameters.Add("$id", SqliteType.Text);
                foreach (string threadId in userEventThreadIds)
                {
                    idParameter.Value = threadId;
                    userEventRowsUpdated += await userEventCommand.ExecuteNonQueryAsync();
                }
            }

            int cwdRowsUpdated = 0;
            if (threadCwdsById?.Count > 0 && await TableHasColumnAsync(connection, "threads", "cwd"))
            {
                await using SqliteCommand cwdCommand = connection.CreateCommand();
                cwdCommand.CommandText = """
                    UPDATE threads
                    SET cwd = $cwd
                    WHERE id = $id AND COALESCE(cwd, '') <> $cwd
                    """;
                SqliteParameter cwdIdParameter = cwdCommand.Parameters.Add("$id", SqliteType.Text);
                SqliteParameter cwdParameter = cwdCommand.Parameters.Add("$cwd", SqliteType.Text);
                foreach ((string threadId, string cwd) in threadCwdsById)
                {
                    if (string.IsNullOrWhiteSpace(threadId) || string.IsNullOrWhiteSpace(cwd))
                    {
                        continue;
                    }

                    cwdIdParameter.Value = threadId;
                    cwdParameter.Value = cwd;
                    cwdRowsUpdated += await cwdCommand.ExecuteNonQueryAsync();
                }
            }

            int updatedRows = providerRowsUpdated + modelRowsUpdated + userEventRowsUpdated + cwdRowsUpdated;

            if (afterUpdate is not null)
            {
                await afterUpdate((updatedRows, providerRowsUpdated, modelRowsUpdated, userEventRowsUpdated, cwdRowsUpdated, true));
            }

            (int UpdatedRows, int ProviderRowsUpdated, int ModelRowsUpdated, int UserEventRowsUpdated, int CwdRowsUpdated, bool DatabasePresent) result =
                (updatedRows, providerRowsUpdated, modelRowsUpdated, userEventRowsUpdated, cwdRowsUpdated, true);

            // Once COMMIT is attempted, an exception no longer proves that
            // SQLite stayed unchanged: the database may be durable even when
            // the caller never receives confirmation. Tell the coordinator
            // immediately before the attempt so it can conservatively restore
            // the bound backup on any acknowledgement failure.
            onCommitAttempt?.Invoke(result);
            await ExecuteNonQueryAsync(connection, "COMMIT");
            transactionOpen = false;
            if (afterCommitBeforeAcknowledgement is not null)
            {
                await afterCommitBeforeAcknowledgement();
            }
            return result;
        }
        catch (Exception error)
        {
            if (transactionOpen)
            {
                try
                {
                    await ExecuteNonQueryAsync(connection, "ROLLBACK");
                }
                catch
                {
                    // Ignore rollback failures and surface the original error.
                }
            }

            throw WrapSqliteMalformedError(
                WrapSqliteBusyError(error, "update session provider metadata"),
                "update session provider metadata");
        }
    }

    /// <summary>
    /// Creates one consistent SQLite main database via SQLite's online-backup
    /// API. WAL/SHM sidecars are neither copied nor emitted.
    /// </summary>
    public async Task<SqliteOnlineBackupResult> CreateSqliteOnlineBackupAsync(
        CodexStorageLayout storage,
        string destinationPath,
        int? busyTimeoutMs = null)
    {
        string? dbPath = ExistingStateDbPath(storage);
        if (dbPath is null)
        {
            return new SqliteOnlineBackupResult(false, null, null);
        }

        string fullSourcePath = Path.GetFullPath(dbPath);
        string fullDestinationPath = Path.GetFullPath(destinationPath);
        StringComparison pathComparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
        if (string.Equals(fullSourcePath, fullDestinationPath, pathComparison))
        {
            throw new InvalidOperationException(
                "SQLite online backup destination must differ from the source database.");
        }
        if (File.Exists(fullDestinationPath))
        {
            throw new IOException("SQLite online backup destination already exists.");
        }
        string? destinationDirectory = Path.GetDirectoryName(fullDestinationPath);
        if (string.IsNullOrEmpty(destinationDirectory))
        {
            throw new InvalidOperationException("Cannot resolve SQLite online backup directory.");
        }
        Directory.CreateDirectory(destinationDirectory);

        try
        {
            SqliteFileMetadata sourceMetadata;
            await using (SqliteConnection source = OpenConnection(
                fullSourcePath,
                SqliteOpenMode.ReadOnly))
            {
                await source.OpenAsync();
                await SetBusyTimeoutAsync(source, busyTimeoutMs);
                sourceMetadata = await ReadSqliteConnectionMetadataAsync(source);
                await using SqliteConnection destination = OpenConnection(
                    fullDestinationPath,
                    SqliteOpenMode.ReadWriteCreate);
                await destination.OpenAsync();
                source.BackupDatabase(destination);
            }

            await using (FileStream stream = new(
                fullDestinationPath,
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.Read,
                4096,
                FileOptions.WriteThrough))
            {
                await stream.FlushAsync();
                stream.Flush(flushToDisk: true);
            }

            if (File.Exists(fullDestinationPath + "-wal")
                || File.Exists(fullDestinationPath + "-shm"))
            {
                throw new InvalidOperationException(
                    "SQLite online backup unexpectedly emitted a WAL/SHM sidecar.");
            }

            SqliteFileMetadata backupMetadata = await ReadStandaloneSqliteHeaderMetadataAsync(
                fullDestinationPath);
            SqliteOnlineBackupPreservation preserved = new(
                sourceMetadata.JournalMode == backupMetadata.JournalMode,
                sourceMetadata.PageSize == backupMetadata.PageSize,
                sourceMetadata.UserVersion == backupMetadata.UserVersion,
                sourceMetadata.ApplicationId == backupMetadata.ApplicationId);
            return new SqliteOnlineBackupResult(
                true,
                fullDestinationPath,
                new SqliteOnlineBackupMetadata(sourceMetadata, backupMetadata, preserved));
        }
        catch (Exception error)
        {
            TryDeleteSqliteBackupArtifact(fullDestinationPath);
            TryDeleteSqliteBackupArtifact(fullDestinationPath + "-wal");
            TryDeleteSqliteBackupArtifact(fullDestinationPath + "-shm");
            throw WrapSqliteMalformedError(
                WrapSqliteBusyError(error, "create a consistent SQLite online backup"),
                "create a consistent SQLite online backup");
        }
    }

    public async Task<SqliteOnlineBackupResult> CreateSqliteOnlineBackupAsync(
        string codexHome,
        string destinationPath,
        int? busyTimeoutMs = null)
    {
        return await CreateSqliteOnlineBackupAsync(
            new CodexStorageLayoutService().CreateDefault(codexHome),
            destinationPath,
            busyTimeoutMs);
    }

    /// <summary>
    /// Restores a SQLite snapshot into the live database via SQLite's online
    /// backup API. SQLite owns the destination write transaction, so an
    /// unfinished restore rolls back without unlinking live WAL/SHM files.
    /// </summary>
    public async Task RestoreSqliteOnlineBackupAsync(
        string sourcePath,
        string destinationPath,
        int? busyTimeoutMs = null)
    {
        string fullSourcePath = Path.GetFullPath(sourcePath);
        string fullDestinationPath = Path.GetFullPath(destinationPath);
        StringComparison pathComparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
        if (string.Equals(fullSourcePath, fullDestinationPath, pathComparison))
        {
            throw new InvalidOperationException(
                "SQLite online restore source must differ from the destination database.");
        }
        if (!File.Exists(fullSourcePath))
        {
            throw new FileNotFoundException(
                "SQLite restore source does not exist.",
                fullSourcePath);
        }

        string? destinationDirectory = Path.GetDirectoryName(fullDestinationPath);
        if (string.IsNullOrEmpty(destinationDirectory))
        {
            throw new InvalidOperationException("Cannot resolve SQLite online restore directory.");
        }

        try
        {
            // Open and inspect the source before allowing SQLite to touch the
            // live destination. ReadOnly closes the disappearance race without
            // ever creating an empty source database.
            await using SqliteConnection source = OpenConnection(
                fullSourcePath,
                SqliteOpenMode.ReadOnly);
            await source.OpenAsync();
            await SetBusyTimeoutAsync(source, busyTimeoutMs);
            _ = await ReadSqliteConnectionMetadataAsync(source);

            Directory.CreateDirectory(destinationDirectory);
            await using SqliteConnection destination = OpenConnection(
                fullDestinationPath,
                SqliteOpenMode.ReadWriteCreate);
            await destination.OpenAsync();
            await SetBusyTimeoutAsync(destination, busyTimeoutMs);
            await ConfigureSqliteWriteDurabilityAsync(destination);
            source.BackupDatabase(destination);
        }
        catch (Exception error)
        {
            // Never delete or replace destination artifacts here. SQLite owns
            // the destination transaction and rolls it back on failure.
            throw WrapSqliteMalformedError(
                WrapSqliteBusyError(error, "restore a consistent SQLite online backup"),
                "restore a consistent SQLite online backup");
        }
    }

    internal static async Task<int> ConfigureSqliteWriteDurabilityAsync(
        SqliteConnection connection)
    {
        await ExecuteNonQueryAsync(connection, "PRAGMA synchronous = FULL");
        object? rawValue = await ExecuteScalarAsync(connection, "PRAGMA synchronous");
        int synchronous = Convert.ToInt32(rawValue);
        if (synchronous != 2)
        {
            throw new InvalidOperationException(
                $"Unable to configure SQLite synchronous=FULL (reported {synchronous}).");
        }
        return synchronous;
    }

    private static async Task<SqliteFileMetadata> ReadSqliteConnectionMetadataAsync(
        SqliteConnection connection)
    {
        string journalMode = Convert.ToString(
            await ExecuteScalarAsync(connection, "PRAGMA journal_mode"))?.ToLowerInvariant() ?? "";
        long pageSize = Convert.ToInt64(await ExecuteScalarAsync(connection, "PRAGMA page_size"));
        long userVersion = Convert.ToInt64(await ExecuteScalarAsync(connection, "PRAGMA user_version"));
        long applicationId = Convert.ToInt64(await ExecuteScalarAsync(connection, "PRAGMA application_id"));
        return new SqliteFileMetadata(journalMode, pageSize, userVersion, applicationId);
    }

    private static async Task<SqliteFileMetadata> ReadStandaloneSqliteHeaderMetadataAsync(
        string dbPath)
    {
        byte[] header = new byte[100];
        await using FileStream stream = new(
            dbPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            4096,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        await stream.ReadExactlyAsync(header);
        ReadOnlySpan<byte> magic = "SQLite format 3\0"u8;
        if (!header.AsSpan(0, magic.Length).SequenceEqual(magic))
        {
            throw new InvalidOperationException(
                "SQLite online backup did not produce a valid standalone database header.");
        }
        int rawPageSize = BinaryPrimitives.ReadUInt16BigEndian(header.AsSpan(16, 2));
        string journalMode = header[18] == 2 && header[19] == 2 ? "wal" : "delete";
        return new SqliteFileMetadata(
            journalMode,
            rawPageSize == 1 ? 65536 : rawPageSize,
            BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(60, 4)),
            BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(68, 4)));
    }

    private static async Task<object?> ExecuteScalarAsync(
        SqliteConnection connection,
        string commandText)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = commandText;
        return await command.ExecuteScalarAsync();
    }

    private static void TryDeleteSqliteBackupArtifact(string filePath)
    {
        try
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }
        }
        catch
        {
            // Cleanup must not hide the original backup failure.
        }
    }

    private static SqliteConnection OpenConnection(string dbPath, SqliteOpenMode mode)
    {
        SqliteConnectionStringBuilder builder = new()
        {
            DataSource = dbPath,
            Mode = mode,
            Pooling = false
        };
        return new SqliteConnection(builder.ConnectionString);
    }

    private static long CountRolloutFiles(string codexHome)
    {
        long count = 0;
        foreach (string directory in AppConstants.SessionDirectories)
        {
            count += CountRolloutFilesInDirectory(Path.Combine(codexHome, directory));
        }

        return count;
    }

    private static long CountRolloutFilesInDirectory(string rootDir)
    {
        if (!Directory.Exists(rootDir))
        {
            return 0;
        }

        try
        {
            return Directory
                .EnumerateFiles(rootDir, "rollout-*.jsonl", SearchOption.AllDirectories)
                .LongCount();
        }
        catch
        {
            return 0;
        }
    }

    private static StateDbCandidateStats ReadStateDbCandidateStats(
        StateDbLocation candidate,
        int priority,
        long rolloutCount)
    {
        using SqliteConnection connection = OpenConnection(candidate.Path, SqliteOpenMode.ReadOnly);
        connection.Open();
        if (!TableExists(connection, "threads"))
        {
            throw new InvalidOperationException("threads table not found");
        }

        long threadCount = ExecuteScalarLong(connection, "SELECT COUNT(*) FROM threads");
        long rolloutDistance = rolloutCount > 0 ? Math.Abs(threadCount - rolloutCount) : 0;
        return new StateDbCandidateStats(
            candidate,
            priority,
            threadCount,
            MaxThreadTimestampMs(connection),
            File.GetLastWriteTimeUtc(candidate.Path).Ticks,
            rolloutDistance);
    }

    private static bool TableExists(SqliteConnection connection, string tableName)
    {
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM sqlite_master WHERE type = 'table' AND name = $name";
        command.Parameters.AddWithValue("$name", tableName);
        object? value = command.ExecuteScalar();
        return value is not null && value is not DBNull;
    }

    private static long MaxThreadTimestampMs(SqliteConnection connection)
    {
        if (TableHasColumn(connection, "threads", "updated_at_ms"))
        {
            return ExecuteScalarLong(connection, "SELECT COALESCE(MAX(updated_at_ms), 0) FROM threads");
        }
        if (TableHasColumn(connection, "threads", "updated_at"))
        {
            return ExecuteScalarLong(connection, "SELECT COALESCE(MAX(updated_at), 0) FROM threads") * 1000;
        }
        if (TableHasColumn(connection, "threads", "created_at_ms"))
        {
            return ExecuteScalarLong(connection, "SELECT COALESCE(MAX(created_at_ms), 0) FROM threads");
        }
        if (TableHasColumn(connection, "threads", "created_at"))
        {
            return ExecuteScalarLong(connection, "SELECT COALESCE(MAX(created_at), 0) FROM threads") * 1000;
        }

        return 0;
    }

    private static long ExecuteScalarLong(SqliteConnection connection, string commandText)
    {
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = commandText;
        object? value = command.ExecuteScalar();
        return value is null || value is DBNull ? 0 : Convert.ToInt64(value);
    }

    private static async Task SetBusyTimeoutAsync(SqliteConnection connection, int? busyTimeoutMs)
    {
        int timeout = busyTimeoutMs is >= 0 ? busyTimeoutMs.Value : DefaultBusyTimeoutMs;
        await ExecuteNonQueryAsync(connection, $"PRAGMA busy_timeout = {timeout}");
    }

    private static async Task ExecuteNonQueryAsync(SqliteConnection connection, string commandText)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = commandText;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<bool> TableHasColumnAsync(SqliteConnection connection, string tableName, string columnName)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = $"PRAGMA table_info({QuoteIdentifier(tableName)})";
        await using SqliteDataReader reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (string.Equals(reader.GetString(1), columnName, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static bool TableHasColumn(SqliteConnection connection, string tableName, string columnName)
    {
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = $"PRAGMA table_info({QuoteIdentifier(tableName)})";
        using SqliteDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            if (string.Equals(reader.GetString(1), columnName, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static string QuoteIdentifier(string value)
    {
        return "\"" + value.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }

    internal static Exception WrapSqliteBusyError(Exception error, string action)
    {
        if (error is not SqliteException sqliteError
            || (sqliteError.SqliteErrorCode != 5 && sqliteError.SqliteErrorCode != 6))
        {
            return error;
        }

        return new InvalidOperationException(
            $"Unable to {action} because state_5.sqlite is currently in use. Close Codex and the Codex app, then retry. Original error: {sqliteError.Message}",
            sqliteError);
    }

    private static bool IsSqliteBusyError(Exception error)
    {
        if (error.InnerException is not null && IsSqliteBusyError(error.InnerException))
        {
            return true;
        }

        return error is SqliteException sqliteError
            && (sqliteError.SqliteErrorCode == 5 || sqliteError.SqliteErrorCode == 6);
    }

    private static bool IsSqliteMalformedError(Exception error)
    {
        if (error.InnerException is not null && IsSqliteMalformedError(error.InnerException))
        {
            return true;
        }

        return error is SqliteException sqliteError
            && (sqliteError.SqliteErrorCode == 11
                || sqliteError.Message.Contains("malformed", StringComparison.OrdinalIgnoreCase)
                || sqliteError.Message.Contains("not a database", StringComparison.OrdinalIgnoreCase));
    }

    internal static Exception WrapSqliteMalformedError(Exception error, string action)
    {
        if (!IsSqliteMalformedError(error))
        {
            return error;
        }

        return new InvalidOperationException(
            $"Unable to {action} because state_5.sqlite is malformed or unreadable. Close Codex, back up or repair the database, then retry. Original error: {error.Message}",
            error);
    }
}
