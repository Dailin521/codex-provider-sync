using Microsoft.Data.Sqlite;

namespace CodexProviderSync.Core;

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
        return
        [
            new StateDbLocation(
                StateDbPath(codexHome),
                Path.Combine(AppConstants.SqliteDirBasename, AppConstants.DbFileBasename),
                "sqlite-dir"),
            new StateDbLocation(
                LegacyStateDbPath(codexHome),
                AppConstants.DbFileBasename,
                "legacy-root")
        ];
    }

    public StateDbLocation? DetectStateDb(string codexHome)
    {
        List<(StateDbLocation Location, int Priority)> existingCandidates = [];
        IReadOnlyList<StateDbLocation> candidates = StateDbCandidates(codexHome);
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

        long rolloutCount = CountRolloutFiles(codexHome);
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

    public IReadOnlyList<StateDbLocation> ExistingStateDbs(string codexHome)
    {
        return StateDbCandidates(codexHome)
            .Where(static candidate => File.Exists(candidate.Path))
            .ToList();
    }

    public async Task<ProviderCounts?> ReadSqliteProviderCountsAsync(string codexHome)
    {
        string? dbPath = ExistingStateDbPath(codexHome);
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
        string? dbPath = ExistingStateDbPath(codexHome);
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
        IReadOnlyList<StateDbLocation> databases = ExistingStateDbs(codexHome);
        if (databases.Count == 0)
        {
            return false;
        }

        foreach (StateDbLocation database in databases)
        {
            await using SqliteConnection connection = OpenConnection(database.Path, SqliteOpenMode.ReadWriteCreate);
            try
            {
                await connection.OpenAsync();
                await SetBusyTimeoutAsync(connection, busyTimeoutMs);
                await ExecuteNonQueryAsync(connection, "BEGIN IMMEDIATE");
                await ExecuteNonQueryAsync(connection, "ROLLBACK");
            }
            catch (Exception error)
            {
                string action = $"update session provider metadata in {database.RelativePath}";
                throw WrapSqliteMalformedError(WrapSqliteBusyError(error, action), action);
            }
        }

        return true;
    }

    public async Task<(int UpdatedRows, int ProviderRowsUpdated, int UserEventRowsUpdated, int CwdRowsUpdated, bool DatabasePresent)> UpdateSqliteProviderAsync(
        string codexHome,
        string targetProvider,
        Func<(int UpdatedRows, int ProviderRowsUpdated, int UserEventRowsUpdated, int CwdRowsUpdated, bool DatabasePresent), Task>? afterUpdate = null,
        int? busyTimeoutMs = null,
        IReadOnlyCollection<string>? userEventThreadIds = null,
        IReadOnlyDictionary<string, string>? threadCwdsById = null)
    {
        IReadOnlyList<StateDbLocation> databases = ExistingStateDbs(codexHome);
        if (databases.Count == 0)
        {
            if (afterUpdate is not null)
            {
                await afterUpdate((0, 0, 0, 0, false));
            }

            return (0, 0, 0, 0, false);
        }

        List<(SqliteConnection Connection, bool TransactionOpen)> openDatabases = [];
        try
        {
            foreach (StateDbLocation database in databases)
            {
                SqliteConnection connection = OpenConnection(database.Path, SqliteOpenMode.ReadWriteCreate);
                openDatabases.Add((connection, false));
                await connection.OpenAsync();
                await SetBusyTimeoutAsync(connection, busyTimeoutMs);
                await ExecuteNonQueryAsync(connection, "BEGIN IMMEDIATE");
                openDatabases[^1] = (connection, true);
            }

            int providerRowsUpdated = 0;
            int userEventRowsUpdated = 0;
            int cwdRowsUpdated = 0;
            foreach ((SqliteConnection connection, _) in openDatabases)
            {
                await using SqliteCommand command = connection.CreateCommand();
                command.CommandText = """
                    UPDATE threads
                    SET model_provider = $provider
                    WHERE COALESCE(model_provider, '') <> $provider
                    """;
                command.Parameters.AddWithValue("$provider", targetProvider);
                providerRowsUpdated += await command.ExecuteNonQueryAsync();

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
            }

            int updatedRows = providerRowsUpdated + userEventRowsUpdated + cwdRowsUpdated;

            if (afterUpdate is not null)
            {
                await afterUpdate((updatedRows, providerRowsUpdated, userEventRowsUpdated, cwdRowsUpdated, true));
            }

            for (int index = 0; index < openDatabases.Count; index += 1)
            {
                (SqliteConnection connection, _) = openDatabases[index];
                await ExecuteNonQueryAsync(connection, "COMMIT");
                openDatabases[index] = (connection, false);
            }
            return (updatedRows, providerRowsUpdated, userEventRowsUpdated, cwdRowsUpdated, true);
        }
        catch (Exception error)
        {
            foreach ((SqliteConnection connection, bool transactionOpen) in openDatabases)
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
            }

            throw WrapSqliteMalformedError(
                WrapSqliteBusyError(error, "update session provider metadata"),
                "update session provider metadata");
        }
        finally
        {
            foreach ((SqliteConnection connection, _) in openDatabases)
            {
                await connection.DisposeAsync();
            }
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
