using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace CodexProviderSync.Core.Tests;

internal sealed class TestCodexHomeFixture
{
    private TestCodexHomeFixture(string root, string codexHome)
    {
        Root = root;
        CodexHome = codexHome;
    }

    public string Root { get; }

    public string CodexHome { get; }

    public static async Task<TestCodexHomeFixture> CreateAsync()
    {
        string root = Path.Combine(Path.GetTempPath(), $"codex-provider-sync-{Guid.NewGuid():N}");
        string codexHome = Path.Combine(root, ".codex");
        Directory.CreateDirectory(Path.Combine(codexHome, "sessions", "2026", "03", "19"));
        Directory.CreateDirectory(Path.Combine(codexHome, "archived_sessions", "2026", "03", "18"));
        return await Task.FromResult(new TestCodexHomeFixture(root, codexHome));
    }

    public string RolloutPath(string directory, string fileName)
    {
        return Path.Combine(CodexHome, directory, "2026", "03", directory == "sessions" ? "19" : "18", fileName);
    }

    public string BackupRoot()
    {
        return Path.Combine(CodexHome, "backups_state", AppConstants.BackupNamespace);
    }

    public string BackupPath(string directoryName)
    {
        return Path.Combine(BackupRoot(), directoryName);
    }

    public string StateDbPath()
    {
        return Path.Combine(CodexHome, AppConstants.SqliteDirBasename, AppConstants.DbFileBasename);
    }

    public string LegacyStateDbPath()
    {
        return Path.Combine(CodexHome, AppConstants.DbFileBasename);
    }

    public async Task WriteConfigAsync(string modelProviderLine)
    {
        string prefix = string.IsNullOrWhiteSpace(modelProviderLine) ? string.Empty : modelProviderLine + "\n";
        string configText = $"{prefix}sandbox_mode = \"danger-full-access\"\n\n[model_providers.apigather]\nbase_url = \"https://example.com\"\n";
        await File.WriteAllTextAsync(Path.Combine(CodexHome, "config.toml"), configText);
    }

    public async Task WriteGlobalStateAsync(object state)
    {
        string json = JsonSerializer.Serialize(state, new JsonSerializerOptions { WriteIndented = true }) + "\n";
        await File.WriteAllTextAsync(Path.Combine(CodexHome, AppConstants.GlobalStateFileBasename), json);
        await File.WriteAllTextAsync(Path.Combine(CodexHome, AppConstants.GlobalStateBackupFileBasename), json);
    }

    public async Task WriteRolloutAsync(string filePath, string id, string provider, string cwd = "C:\\AITemp")
    {
        object payload = new
        {
            id,
            timestamp = "2026-03-19T00:00:00.000Z",
            cwd,
            source = "cli",
            cli_version = "0.115.0",
            model_provider = provider
        };
        string first = JsonSerializer.Serialize(new
        {
            timestamp = "2026-03-19T00:00:00.000Z",
            type = "session_meta",
            payload
        });
        string second = JsonSerializer.Serialize(new
        {
            timestamp = "2026-03-19T00:00:00.000Z",
            type = "event_msg",
            payload = new
            {
                type = "user_message",
                message = "hi"
            }
        });

        await File.WriteAllTextAsync(filePath, $"{first}\n{second}\n");
    }

    public async Task WriteRolloutWithTurnContextAsync(string filePath, string id, string provider, string model)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(filePath)!);
        object payload = new
        {
            id,
            timestamp = "2026-06-09T09:16:03.878Z",
            cwd = "C:\\AITemp",
            source = "cli",
            cli_version = "0.115.0",
            model_provider = provider
        };
        string first = JsonSerializer.Serialize(new
        {
            timestamp = "2026-06-09T09:16:03.878Z",
            type = "session_meta",
            payload
        });
        string turnContext = JsonSerializer.Serialize(new
        {
            timestamp = "2026-06-09T09:16:03.880Z",
            type = "turn_context",
            payload = new
            {
                turn_id = "019eabaa-e391-7e21-89cd-e761b5dee114",
                cwd = "C:\\AITemp",
                current_date = "2026-06-09",
                model,
                collaboration_mode = new
                {
                    mode = "default",
                    settings = new
                    {
                        model,
                        reasoning_effort = "xhigh"
                    }
                }
            }
        });
        string heartbeat = JsonSerializer.Serialize(new
        {
            timestamp = "2026-06-09T10:16:03.880Z",
            type = "turn_context",
            payload = new
            {
                turn_id = "019eabaa-e391-7e21-89cd-e761b5dee115",
                cwd = "C:\\AITemp",
                current_date = "2026-06-09",
                model,
                collaboration_mode = new
                {
                    mode = "default",
                    settings = new
                    {
                        model,
                        reasoning_effort = "xhigh"
                    }
                }
            }
        });

        await File.WriteAllTextAsync(filePath, $"{first}\n{turnContext}\n{heartbeat}\n");
    }

    // Write a rollout whose first `turn_context` payload carries a
    // `developer_instructions` blob large enough that the line
    // blows past 64 KB on its own. This is the regression case
    // behind the `ReadFirstTurnContextModelAsync` fix that
    // switched from a fixed-window read to a streaming line reader.
    public async Task WriteLongTurnContextRolloutAsync(string filePath, string id, string provider, string model, int paddingBytes)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(filePath)!);
        string padding = new('x', Math.Max(0, paddingBytes));
        object metaPayload = new
        {
            id,
            timestamp = "2026-06-09T09:16:03.878Z",
            cwd = "C:\\AITemp",
            source = "cli",
            cli_version = "0.115.0",
            model_provider = provider
        };
        string meta = JsonSerializer.Serialize(new
        {
            timestamp = "2026-06-09T09:16:03.878Z",
            type = "session_meta",
            payload = metaPayload
        });
        string turnContext = JsonSerializer.Serialize(new
        {
            timestamp = "2026-06-09T09:16:03.880Z",
            type = "turn_context",
            payload = new
            {
                turn_id = "019eabaa-e391-7e21-89cd-e761b5dee114",
                cwd = "C:\\AITemp",
                current_date = "2026-06-09",
                model,
                developer_instructions = padding,
                collaboration_mode = new
                {
                    mode = "default",
                    settings = new
                    {
                        model,
                        reasoning_effort = "xhigh"
                    }
                }
            }
        });
        if (turnContext.Length < 64 * 1024 + 1)
        {
            throw new InvalidOperationException($"Test setup error: long turn_context line is only {turnContext.Length} bytes; bump paddingBytes");
        }
        await File.WriteAllTextAsync(filePath, $"{meta}\n{turnContext}\n");
    }

    public async Task AppendEncryptedContentAsync(string filePath)
    {
        await File.AppendAllTextAsync(filePath, "{\"type\":\"event_msg\",\"payload\":{\"encrypted_content\":\"gAAA\"}}\n");
    }

    public async Task<long> WriteBackupAsync(string directoryName, params (string RelativePath, string Content)[] files)
    {
        string backupDir = BackupPath(directoryName);
        Directory.CreateDirectory(backupDir);
        long totalBytes = 0;

        if (!files.Any(file => string.Equals(file.RelativePath, "metadata.json", StringComparison.Ordinal)))
        {
            string metadataContent = $$"""
                {
                  "version": 1,
                  "namespace": "provider-sync",
                  "codexHome": "{{CodexHome.Replace("\\", "\\\\")}}",
                  "targetProvider": "openai",
                  "createdAt": "2026-03-24T00:00:00.0000000+00:00",
                  "dbFiles": [],
                  "changedSessionFiles": 0
                }
                """;
            string metadataPath = Path.Combine(backupDir, "metadata.json");
            await File.WriteAllTextAsync(metadataPath, metadataContent);
            totalBytes += new FileInfo(metadataPath).Length;
        }

        foreach ((string relativePath, string content) in files)
        {
            string fullPath = Path.Combine(backupDir, relativePath);
            Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
            await File.WriteAllTextAsync(fullPath, content);
            totalBytes += new FileInfo(fullPath).Length;
        }

        return totalBytes;
    }

    public async Task WriteStateDbAsync(IEnumerable<(string Id, string ModelProvider, bool Archived)> rows)
    {
        await WriteStateDbAsync(rows, model: null);
    }

    public async Task WriteStateDbAsync(IEnumerable<(string Id, string ModelProvider, bool Archived)> rows, string? model)
    {
        await using SqliteConnection connection = OpenSqliteConnection();
        await connection.OpenAsync();
        SqliteCommand create = connection.CreateCommand();
        create.CommandText = """
            CREATE TABLE threads (
              id TEXT PRIMARY KEY,
              model_provider TEXT,
              cwd TEXT NOT NULL DEFAULT '',
              archived INTEGER NOT NULL DEFAULT 0,
              first_user_message TEXT NOT NULL DEFAULT '',
              model TEXT
            )
            """;
        await create.ExecuteNonQueryAsync();

        foreach ((string id, string modelProvider, bool archived) in rows)
        {
            SqliteCommand insert = connection.CreateCommand();
            insert.CommandText = """
                INSERT INTO threads (id, model_provider, cwd, archived, first_user_message, model)
                VALUES ($id, $provider, 'C:\AITemp', $archived, 'hello', $model)
                """;
            insert.Parameters.AddWithValue("$id", id);
            insert.Parameters.AddWithValue("$provider", modelProvider);
            insert.Parameters.AddWithValue("$archived", archived ? 1 : 0);
            insert.Parameters.AddWithValue("$model", (object?)model ?? DBNull.Value);
            await insert.ExecuteNonQueryAsync();
        }
    }

    public async Task WriteStateDbWithUserEventColumnAsync(IEnumerable<(string Id, string ModelProvider, bool Archived, bool HasUserEvent)> rows)
    {
        await using SqliteConnection connection = OpenSqliteConnection();
        await connection.OpenAsync();
        SqliteCommand create = connection.CreateCommand();
        create.CommandText = """
            CREATE TABLE threads (
              id TEXT PRIMARY KEY,
              model_provider TEXT,
              cwd TEXT NOT NULL DEFAULT '',
              archived INTEGER NOT NULL DEFAULT 0,
              has_user_event INTEGER NOT NULL DEFAULT 0,
              first_user_message TEXT NOT NULL DEFAULT ''
            )
            """;
        await create.ExecuteNonQueryAsync();

        foreach ((string id, string modelProvider, bool archived, bool hasUserEvent) in rows)
        {
            SqliteCommand insert = connection.CreateCommand();
            insert.CommandText = """
                INSERT INTO threads (id, model_provider, cwd, archived, has_user_event, first_user_message)
                VALUES ($id, $provider, 'C:\AITemp', $archived, $hasUserEvent, 'hello')
                """;
            insert.Parameters.AddWithValue("$id", id);
            insert.Parameters.AddWithValue("$provider", modelProvider);
            insert.Parameters.AddWithValue("$archived", archived ? 1 : 0);
            insert.Parameters.AddWithValue("$hasUserEvent", hasUserEvent ? 1 : 0);
            await insert.ExecuteNonQueryAsync();
        }
    }

    public async Task WriteStateDbWithUserEventAndCwdAsync(IEnumerable<(string Id, string ModelProvider, bool Archived, bool HasUserEvent, string Cwd)> rows)
    {
        await using SqliteConnection connection = OpenSqliteConnection();
        await connection.OpenAsync();
        SqliteCommand create = connection.CreateCommand();
        create.CommandText = """
            CREATE TABLE threads (
              id TEXT PRIMARY KEY,
              model_provider TEXT,
              cwd TEXT NOT NULL DEFAULT '',
              archived INTEGER NOT NULL DEFAULT 0,
              has_user_event INTEGER NOT NULL DEFAULT 0,
              first_user_message TEXT NOT NULL DEFAULT ''
            )
            """;
        await create.ExecuteNonQueryAsync();

        foreach ((string id, string modelProvider, bool archived, bool hasUserEvent, string cwd) in rows)
        {
            SqliteCommand insert = connection.CreateCommand();
            insert.CommandText = """
                INSERT INTO threads (id, model_provider, cwd, archived, has_user_event, first_user_message)
                VALUES ($id, $provider, $cwd, $archived, $hasUserEvent, 'hello')
                """;
            insert.Parameters.AddWithValue("$id", id);
            insert.Parameters.AddWithValue("$provider", modelProvider);
            insert.Parameters.AddWithValue("$cwd", cwd);
            insert.Parameters.AddWithValue("$archived", archived ? 1 : 0);
            insert.Parameters.AddWithValue("$hasUserEvent", hasUserEvent ? 1 : 0);
            await insert.ExecuteNonQueryAsync();
        }
    }

    public async Task WriteStateDbWithCwdAsync(IEnumerable<(string Id, string ModelProvider, bool Archived, string Cwd)> rows)
    {
        await using SqliteConnection connection = OpenSqliteConnection();
        await connection.OpenAsync();
        SqliteCommand create = connection.CreateCommand();
        create.CommandText = """
            CREATE TABLE threads (
              id TEXT PRIMARY KEY,
              model_provider TEXT,
              cwd TEXT NOT NULL DEFAULT '',
              archived INTEGER NOT NULL DEFAULT 0,
              first_user_message TEXT NOT NULL DEFAULT ''
            )
            """;
        await create.ExecuteNonQueryAsync();

        foreach ((string id, string modelProvider, bool archived, string cwd) in rows)
        {
            SqliteCommand insert = connection.CreateCommand();
            insert.CommandText = """
                INSERT INTO threads (id, model_provider, cwd, archived, first_user_message)
                VALUES ($id, $provider, $cwd, $archived, 'hello')
                """;
            insert.Parameters.AddWithValue("$id", id);
            insert.Parameters.AddWithValue("$provider", modelProvider);
            insert.Parameters.AddWithValue("$cwd", cwd);
            insert.Parameters.AddWithValue("$archived", archived ? 1 : 0);
            await insert.ExecuteNonQueryAsync();
        }
    }

    public async Task WriteStateDbForProjectVisibilityAsync(IEnumerable<(string Id, string ModelProvider, string Cwd, string Source, bool Archived, string FirstUserMessage, long UpdatedAtMs)> rows)
    {
        await using SqliteConnection connection = OpenSqliteConnection();
        await connection.OpenAsync();
        SqliteCommand create = connection.CreateCommand();
        create.CommandText = """
            CREATE TABLE threads (
              id TEXT PRIMARY KEY,
              model_provider TEXT,
              cwd TEXT NOT NULL DEFAULT '',
              source TEXT NOT NULL DEFAULT 'cli',
              archived INTEGER NOT NULL DEFAULT 0,
              first_user_message TEXT NOT NULL DEFAULT '',
              updated_at_ms INTEGER NOT NULL DEFAULT 0
            )
            """;
        await create.ExecuteNonQueryAsync();

        foreach ((string id, string modelProvider, string cwd, string source, bool archived, string firstUserMessage, long updatedAtMs) in rows)
        {
            SqliteCommand insert = connection.CreateCommand();
            insert.CommandText = """
                INSERT INTO threads (id, model_provider, cwd, source, archived, first_user_message, updated_at_ms)
                VALUES ($id, $provider, $cwd, $source, $archived, $firstUserMessage, $updatedAtMs)
                """;
            insert.Parameters.AddWithValue("$id", id);
            insert.Parameters.AddWithValue("$provider", modelProvider);
            insert.Parameters.AddWithValue("$cwd", cwd);
            insert.Parameters.AddWithValue("$source", source);
            insert.Parameters.AddWithValue("$archived", archived ? 1 : 0);
            insert.Parameters.AddWithValue("$firstUserMessage", firstUserMessage);
            insert.Parameters.AddWithValue("$updatedAtMs", updatedAtMs);
            await insert.ExecuteNonQueryAsync();
        }
    }

    public SqliteConnection OpenSqliteConnection()
    {
        return OpenSqliteConnection(StateDbPath());
    }

    public SqliteConnection OpenLegacySqliteConnection()
    {
        return OpenSqliteConnection(LegacyStateDbPath());
    }

    private static SqliteConnection OpenSqliteConnection(string dbPath)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(dbPath)!);
        return new SqliteConnection($"Data Source={dbPath};Mode=ReadWriteCreate;Pooling=False");
    }
}
