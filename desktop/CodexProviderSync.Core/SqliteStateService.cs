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
        return Path.Combine(codexHome, AppConstants.DbFileBasename);
    }

    public async Task<ProviderCounts?> ReadSqliteProviderCountsAsync(string codexHome)
    {
        string dbPath = StateDbPath(codexHome);
        if (!File.Exists(dbPath))
        {
            return null;
        }

        await using SqliteConnection connection = OpenConnection(dbPath);
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

    public async Task<bool> AssertSqliteWritableAsync(string codexHome, int? busyTimeoutMs = null)
    {
        string dbPath = StateDbPath(codexHome);
        if (!File.Exists(dbPath))
        {
            return false;
        }

        await using SqliteConnection connection = OpenConnection(dbPath);
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
            throw WrapSqliteBusyError(error, "update session provider metadata");
        }
    }

    public async Task<(int UpdatedRows, bool DatabasePresent)> UpdateSqliteProviderAsync(
        string codexHome,
        string targetProvider,
        Func<(int UpdatedRows, bool DatabasePresent), Task>? afterUpdate = null,
        int? busyTimeoutMs = null)
    {
        string dbPath = StateDbPath(codexHome);
        if (!File.Exists(dbPath))
        {
            if (afterUpdate is not null)
            {
                await afterUpdate((0, false));
            }

            return (0, false);
        }

        await using SqliteConnection connection = OpenConnection(dbPath);
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
            int updatedRows = await command.ExecuteNonQueryAsync();

            if (afterUpdate is not null)
            {
                await afterUpdate((updatedRows, true));
            }

            await ExecuteNonQueryAsync(connection, "COMMIT");
            transactionOpen = false;
            return (updatedRows, true);
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

            throw WrapSqliteBusyError(error, "update session provider metadata");
        }
    }

    private static SqliteConnection OpenConnection(string dbPath)
    {
        SqliteConnectionStringBuilder builder = new()
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
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

    private static Exception WrapSqliteBusyError(Exception error, string action)
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
}
