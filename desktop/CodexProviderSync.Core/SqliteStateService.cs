using Microsoft.Data.Sqlite;

namespace CodexProviderSync.Core;

public sealed class SqliteStateService
{
    private const int DefaultBusyTimeoutMs = 5000;

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
        foreach (StateDbLocation candidate in StateDbCandidates(codexHome))
        {
            if (File.Exists(candidate.Path))
            {
                return candidate;
            }
        }

        return null;
    }

    public string? ExistingStateDbPath(string codexHome)
    {
        return DetectStateDb(codexHome)?.Path;
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
        string? dbPath = ExistingStateDbPath(codexHome);
        if (dbPath is null)
        {
            return false;
        }

        await using SqliteConnection connection = OpenConnection(dbPath, SqliteOpenMode.ReadWriteCreate);
        try
        {
            await connection.OpenAsync();
            await SetBusyTimeoutAsync(connection, busyTimeoutMs);
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
        IReadOnlyDictionary<string, string>? threadCwdsById = null)
    {
        // Walk every candidate database, not just the first one. Codex
        // stores its state database in two locations (`~/.codex/sqlite/
        // state_5.sqlite` for newer installs and `~/.codex/state_5.sqlite`
        // for older installs), and the Codex App GUI keeps the legacy
        // root database alive as long as it is on disk — including
        // for the older project sessions it created before the new
        // location was introduced. If we only update the first hit,
        // the GUI keeps reading stale `model_provider` / `model`
        // values for those older sessions and either shows the old
        // provider label or sends requests with the wrong model name.
        List<StateDbLocation> candidates = StateDbCandidates(codexHome)
            .Where(static c => File.Exists(c.Path))
            .ToList();

        if (candidates.Count == 0)
        {
            if (afterUpdate is not null)
            {
                await afterUpdate((0, 0, 0, 0, 0, false));
            }

            return (0, 0, 0, 0, 0, false);
        }

        int totalUpdatedRows = 0;
        int totalProviderRowsUpdated = 0;
        int totalModelRowsUpdated = 0;
        int totalUserEventRowsUpdated = 0;
        int totalCwdRowsUpdated = 0;

        foreach (StateDbLocation candidate in candidates)
        {
            (int updatedRows, int providerRowsUpdated, int modelRowsUpdated, int userEventRowsUpdated, int cwdRowsUpdated, bool databasePresent) = await UpdateSingleSqliteDatabaseAsync(
                candidate.Path,
                targetProvider,
                targetModel,
                busyTimeoutMs,
                userEventThreadIds,
                threadCwdsById);

            totalUpdatedRows += updatedRows;
            totalProviderRowsUpdated += providerRowsUpdated;
            totalModelRowsUpdated += modelRowsUpdated;
            totalUserEventRowsUpdated += userEventRowsUpdated;
            totalCwdRowsUpdated += cwdRowsUpdated;
        }

        if (afterUpdate is not null)
        {
            // Run the rollout rewrite once, after both candidate
            // databases have been updated. The rewrite is keyed off
            // the session change collection, not the SQLite state,
            // so it does not matter whether the data lives in the
            // new or the legacy database — we only want to do it
            // once, after the SQLite phase is fully done.
            await afterUpdate((totalUpdatedRows, totalProviderRowsUpdated, totalModelRowsUpdated, totalUserEventRowsUpdated, totalCwdRowsUpdated, candidates.Count > 0));
        }

        return (totalUpdatedRows, totalProviderRowsUpdated, totalModelRowsUpdated, totalUserEventRowsUpdated, totalCwdRowsUpdated, candidates.Count > 0);
    }

    private async Task<(int UpdatedRows, int ProviderRowsUpdated, int ModelRowsUpdated, int UserEventRowsUpdated, int CwdRowsUpdated, bool DatabasePresent)> UpdateSingleSqliteDatabaseAsync(
        string dbPath,
        string targetProvider,
        string? targetModel,
        int? busyTimeoutMs,
        IReadOnlyCollection<string>? userEventThreadIds,
        IReadOnlyDictionary<string, string>? threadCwdsById)
    {
        await using SqliteConnection connection = OpenConnection(dbPath, SqliteOpenMode.ReadWriteCreate);
        bool transactionOpen = false;
        try
        {
            await connection.OpenAsync();
            await SetBusyTimeoutAsync(connection, busyTimeoutMs);
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

            await ExecuteNonQueryAsync(connection, "COMMIT");
            transactionOpen = false;
            return (updatedRows, providerRowsUpdated, modelRowsUpdated, userEventRowsUpdated, cwdRowsUpdated, true);
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
