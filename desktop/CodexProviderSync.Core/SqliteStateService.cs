using Microsoft.Data.Sqlite;

namespace CodexProviderSync.Core;

public sealed class SqliteStateService
{
    private const int DefaultBusyTimeoutMs = 5000;
    private static readonly string[] StateDbRelativePaths =
    [
        AppConstants.DbFileBasename,
        Path.Combine("sqlite", AppConstants.DbFileBasename)
    ];

    static SqliteStateService()
    {
        SQLitePCL.Batteries_V2.Init();
    }

    public string StateDbPath(string codexHome)
    {
        string nestedPath = StateDbPathFromRelative(codexHome, StateDbRelativePaths[1]);
        if (File.Exists(nestedPath))
        {
            return nestedPath;
        }

        return Path.Combine(codexHome, AppConstants.DbFileBasename);
    }

    public IReadOnlyList<string> StateDbRelativePathsSnapshot()
    {
        return StateDbRelativePaths.ToArray();
    }

    public string StateDbPathFromRelative(string codexHome, string relativePath)
    {
        return Path.Combine(
            new[] { codexHome }
                .Concat(relativePath.Split(new[] { '/', '\\' }, StringSplitOptions.RemoveEmptyEntries))
                .ToArray());
    }

    private List<string> ExistingStateDbPaths(string codexHome)
    {
        return StateDbRelativePaths
            .Select(relativePath => StateDbPathFromRelative(codexHome, relativePath))
            .Where(File.Exists)
            .ToList();
    }

    public async Task<ProviderCounts?> ReadSqliteProviderCountsAsync(string codexHome)
    {
        string dbPath = StateDbPath(codexHome);
        if (!File.Exists(dbPath))
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
        string dbPath = StateDbPath(codexHome);
        if (!File.Exists(dbPath))
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
        List<string> dbPaths = ExistingStateDbPaths(codexHome);
        if (dbPaths.Count == 0)
        {
            return false;
        }

        try
        {
            foreach (string dbPath in dbPaths)
            {
                await using SqliteConnection connection = OpenConnection(dbPath, SqliteOpenMode.ReadWriteCreate);
                await connection.OpenAsync();
                await SetBusyTimeoutAsync(connection, busyTimeoutMs);
                await ExecuteNonQueryAsync(connection, "BEGIN IMMEDIATE");
                await ExecuteNonQueryAsync(connection, "ROLLBACK");
            }

            return true;
        }
        catch (Exception error)
        {
            throw WrapSqliteMalformedError(
                WrapSqliteBusyError(error, "update session provider metadata"),
                "update session provider metadata");
        }
    }

    public async Task<(int UpdatedRows, int ProviderRowsUpdated, int UserEventRowsUpdated, int CwdRowsUpdated, bool DatabasePresent)> UpdateSqliteProviderAsync(
        string codexHome,
        string targetProvider,
        Func<(int UpdatedRows, int ProviderRowsUpdated, int UserEventRowsUpdated, int CwdRowsUpdated, bool DatabasePresent), Task>? afterUpdate = null,
        int? busyTimeoutMs = null,
        IReadOnlyCollection<string>? userEventThreadIds = null,
        IReadOnlyDictionary<string, string>? threadCwdsById = null)
    {
        List<string> dbPaths = ExistingStateDbPaths(codexHome);
        if (dbPaths.Count == 0)
        {
            if (afterUpdate is not null)
            {
                await afterUpdate((0, 0, 0, 0, false));
            }

            return (0, 0, 0, 0, false);
        }

        List<(SqliteConnection Connection, bool TransactionOpen)> openedConnections = [];
        try
        {
            int providerRowsUpdated = 0;
            int userEventRowsUpdated = 0;
            int cwdRowsUpdated = 0;
            foreach (string dbPath in dbPaths)
            {
                SqliteConnection connection = OpenConnection(dbPath, SqliteOpenMode.ReadWriteCreate);
                openedConnections.Add((connection, false));
                await connection.OpenAsync();
                await SetBusyTimeoutAsync(connection, busyTimeoutMs);
                await ExecuteNonQueryAsync(connection, "BEGIN IMMEDIATE");
                openedConnections[^1] = (connection, true);

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

            for (int index = 0; index < openedConnections.Count; index += 1)
            {
                await ExecuteNonQueryAsync(openedConnections[index].Connection, "COMMIT");
                openedConnections[index] = (openedConnections[index].Connection, false);
            }

            return (updatedRows, providerRowsUpdated, userEventRowsUpdated, cwdRowsUpdated, true);
        }
        catch (Exception error)
        {
            foreach ((SqliteConnection connection, bool transactionOpen) in openedConnections)
            {
                if (!transactionOpen)
                {
                    continue;
                }

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
        finally
        {
            foreach ((SqliteConnection connection, _) in openedConnections)
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
