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

    public async Task WriteConfigAsync(string modelProviderLine)
    {
        string prefix = string.IsNullOrWhiteSpace(modelProviderLine) ? string.Empty : modelProviderLine + "\n";
        string configText = $"{prefix}sandbox_mode = \"danger-full-access\"\n\n[model_providers.apigather]\nbase_url = \"https://example.com\"\n";
        await File.WriteAllTextAsync(Path.Combine(CodexHome, "config.toml"), configText);
    }

    public async Task WriteRolloutAsync(string filePath, string id, string provider)
    {
        object payload = new
        {
            id,
            timestamp = "2026-03-19T00:00:00.000Z",
            cwd = "C:\\AITemp",
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
        string dbPath = Path.Combine(CodexHome, "state_5.sqlite");
        await using SqliteConnection connection = OpenSqliteConnection();
        await connection.OpenAsync();
        SqliteCommand create = connection.CreateCommand();
        create.CommandText = """
            CREATE TABLE threads (
              id TEXT PRIMARY KEY,
              model_provider TEXT,
              archived INTEGER NOT NULL DEFAULT 0,
              first_user_message TEXT NOT NULL DEFAULT ''
            )
            """;
        await create.ExecuteNonQueryAsync();

        foreach ((string id, string modelProvider, bool archived) in rows)
        {
            SqliteCommand insert = connection.CreateCommand();
            insert.CommandText = """
                INSERT INTO threads (id, model_provider, archived, first_user_message)
                VALUES ($id, $provider, $archived, 'hello')
                """;
            insert.Parameters.AddWithValue("$id", id);
            insert.Parameters.AddWithValue("$provider", modelProvider);
            insert.Parameters.AddWithValue("$archived", archived ? 1 : 0);
            await insert.ExecuteNonQueryAsync();
        }
    }

    public SqliteConnection OpenSqliteConnection()
    {
        return new SqliteConnection($"Data Source={Path.Combine(CodexHome, "state_5.sqlite")};Mode=ReadWriteCreate;Pooling=False");
    }
}
