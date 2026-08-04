using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace CodexProviderSync.GuiE2E;

internal sealed record AutomationLaunchDescriptor(
    int SchemaVersion,
    string IsolationRoot,
    string PipeName,
    string Token);

internal sealed record FixtureSnapshot(
    string ConfigSha256,
    string RolloutSha256,
    string DatabaseStorageSha256,
    IReadOnlyList<DatabaseFileEvidence> DatabaseFiles,
    string ConfigProvider,
    string RolloutProvider,
    string DatabaseProvider,
    string? DatabaseModel,
    int ManagedBackupCount);

internal sealed record DatabaseFileEvidence(string Name, long Length, string Sha256);

internal sealed class IsolatedFixture
{
    internal const string SentinelFileName = ".codex-provider-sync-test-root";
    internal const string SentinelContent = "codex-provider-sync isolated GUI automation root v1";

    internal IsolatedFixture(string root)
    {
        Root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(root));
        CodexHome = Path.Combine(Root, "codex-home");
        SqliteHome = Path.Combine(Root, "sqlite-home");
        RolloutPath = Path.Combine(CodexHome, "sessions", "2026", "08", "04", "rollout-e2e.jsonl");
        DatabasePath = Path.Combine(SqliteHome, "state_5.sqlite");
        ConfigPath = Path.Combine(CodexHome, "config.toml");
        TracePath = Path.Combine(Root, "automation", "gui-trace.jsonl");
        SettingsPath = Path.Combine(Root, "appdata", "settings.json");
        PickerCodexHome = Path.Combine(Root, "picker-probes", "codex-home-alternate");
        PickerSqliteHome = Path.Combine(Root, "picker-probes", "sqlite-home-alternate");
    }

    internal string Root { get; }
    internal string CodexHome { get; }
    internal string SqliteHome { get; }
    internal string RolloutPath { get; }
    internal string DatabasePath { get; }
    internal string ConfigPath { get; }
    internal string TracePath { get; }
    internal string SettingsPath { get; }
    internal string PickerCodexHome { get; }
    internal string PickerSqliteHome { get; }

    internal IReadOnlyList<string> ManagedBackups()
    {
        string root = Path.Combine(CodexHome, "backups_state", "provider-sync");
        return Directory.Exists(root)
            ? Directory.EnumerateDirectories(root)
                .Where(IsManagedBackup)
                .Order(StringComparer.Ordinal)
                .ToArray()
            : [];
    }

    internal async Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (Directory.Exists(Root) && Directory.EnumerateFileSystemEntries(Root).Any())
        {
            throw new InvalidOperationException("GUI E2E isolation root must be new and empty.");
        }
        Directory.CreateDirectory(Root);
        if ((File.GetAttributes(Root) & FileAttributes.ReparsePoint) != 0)
        {
            throw new InvalidOperationException("GUI E2E isolation root cannot be a reparse point.");
        }
        await File.WriteAllTextAsync(
            Path.Combine(Root, SentinelFileName),
            SentinelContent + Environment.NewLine,
            new UTF8Encoding(false),
            cancellationToken);
        Directory.CreateDirectory(CodexHome);
        Directory.CreateDirectory(SqliteHome);
        Directory.CreateDirectory(Path.GetDirectoryName(RolloutPath)!);

        string config = """
            model_provider = "openai"
            model = "gpt-e2e-root"

            [model_providers.apigather]
            name = "Isolated E2E Provider"
            base_url = "http://127.0.0.1:9/v1"
            model = "gpt-e2e-target"
            """ + Environment.NewLine;
        await File.WriteAllTextAsync(ConfigPath, config, new UTF8Encoding(false), cancellationToken);

        string rollout = JsonSerializer.Serialize(new
        {
            timestamp = "2026-08-04T00:00:00.000Z",
            type = "session_meta",
            payload = new
            {
                id = "gui-e2e-thread",
                model_provider = "legacy-e2e",
                cwd = Path.Combine(Root, "workspace")
            }
        }) + Environment.NewLine + JsonSerializer.Serialize(new
        {
            timestamp = "2026-08-04T00:00:01.000Z",
            type = "event_msg",
            payload = new { type = "user_message", message = "isolated GUI E2E fixture" }
        }) + Environment.NewLine;
        await File.WriteAllTextAsync(RolloutPath, rollout, new UTF8Encoding(false), cancellationToken);

        await using SqliteConnection connection = new(SqliteConnectionString(DatabasePath, SqliteOpenMode.ReadWriteCreate));
        await connection.OpenAsync(cancellationToken);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE threads (
              id TEXT PRIMARY KEY,
              model_provider TEXT,
              cwd TEXT NOT NULL DEFAULT '',
              archived INTEGER NOT NULL DEFAULT 0,
              first_user_message TEXT NOT NULL DEFAULT '',
              model TEXT
            );
            INSERT INTO threads (id, model_provider, cwd, archived, first_user_message, model)
            VALUES ('gui-e2e-thread', 'legacy-e2e', $cwd, 0, 'isolated GUI E2E fixture', 'gpt-e2e-old');
            PRAGMA wal_checkpoint(TRUNCATE);
            """;
        command.Parameters.AddWithValue("$cwd", Path.Combine(Root, "workspace"));
        await command.ExecuteNonQueryAsync(cancellationToken);

        string alternateConfig = Path.Combine(PickerCodexHome, "config.toml");
        string alternateRollout = Path.Combine(
            PickerCodexHome, "sessions", "2026", "08", "04", "rollout-picker-e2e.jsonl");
        Directory.CreateDirectory(Path.GetDirectoryName(alternateRollout)!);
        Directory.CreateDirectory(PickerSqliteHome);
        await File.WriteAllTextAsync(alternateConfig, config, new UTF8Encoding(false), cancellationToken);
        await File.WriteAllTextAsync(alternateRollout, rollout, new UTF8Encoding(false), cancellationToken);
        await using SqliteConnection alternate = new(
            SqliteConnectionString(Path.Combine(PickerSqliteHome, "state_5.sqlite"), SqliteOpenMode.ReadWriteCreate));
        await alternate.OpenAsync(cancellationToken);
        connection.BackupDatabase(alternate);
    }

    internal AutomationLaunchDescriptor CreateDescriptor(int generation)
    {
        string pipe = $"CodexProviderSync.Automation.{Guid.NewGuid():N}";
        string token = Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(32));
        AutomationLaunchDescriptor descriptor = new(1, Root, pipe, token);
        string path = DescriptorPath(generation);
        File.WriteAllText(
            path,
            JsonSerializer.Serialize(descriptor, new JsonSerializerOptions(JsonSerializerDefaults.Web)),
            new UTF8Encoding(false));
        return descriptor;
    }

    internal string DescriptorPath(int generation) => Path.Combine(Root, $"gui-automation-{generation:D2}.json");

    internal async Task<FixtureSnapshot> SnapshotAsync(CancellationToken cancellationToken)
    {
        string config = await File.ReadAllTextAsync(ConfigPath, cancellationToken);
        string rollout = await File.ReadAllTextAsync(RolloutPath, cancellationToken);
        string databaseProvider;
        string? databaseModel;
        await using (SqliteConnection connection = new(SqliteConnectionString(DatabasePath, SqliteOpenMode.ReadOnly)))
        {
            await connection.OpenAsync(cancellationToken);
            await using SqliteCommand command = connection.CreateCommand();
            command.CommandText = "SELECT model_provider, model FROM threads WHERE id = 'gui-e2e-thread'";
            await using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                throw new InvalidDataException("Isolated fixture thread is missing from SQLite.");
            }
            databaseProvider = reader.GetString(0);
            databaseModel = reader.IsDBNull(1) ? null : reader.GetString(1);
        }

        List<DatabaseFileEvidence> databaseFiles = [];
        foreach (string path in new[] { DatabasePath, DatabasePath + "-wal", DatabasePath + "-shm" })
        {
            if (File.Exists(path))
            {
                FileInfo info = new(path);
                databaseFiles.Add(new(
                    Path.GetFileName(path),
                    info.Length,
                    await Hashing.Sha256FileAsync(path, cancellationToken)));
            }
        }
        string databaseStorageDigest = Hashing.Sha256Text(string.Join(
            "\n",
            databaseFiles.Select(file => $"{file.Name}|{file.Length}|{file.Sha256}")));

        string rolloutProvider = ReadRolloutProvider(rollout);
        string configProvider = ReadConfigProvider(config);
        int backups = ManagedBackups().Count;
        return new(
            Hashing.Sha256Text(config),
            Hashing.Sha256Text(rollout),
            databaseStorageDigest,
            databaseFiles,
            configProvider,
            rolloutProvider,
            databaseProvider,
            databaseModel,
            backups);
    }

    private static string ReadConfigProvider(string config)
    {
        string line = config.Split('\n')
            .First(value => value.TrimStart().StartsWith("model_provider", StringComparison.Ordinal));
        return line[(line.IndexOf('=') + 1)..].Trim().Trim('"');
    }

    private static string ReadRolloutProvider(string rollout)
    {
        using JsonDocument document = JsonDocument.Parse(rollout.Split('\n', StringSplitOptions.RemoveEmptyEntries)[0]);
        JsonElement payload = document.RootElement.GetProperty("payload");
        if (payload.TryGetProperty("model_provider", out JsonElement snake))
        {
            return snake.GetString()!;
        }
        return payload.GetProperty("modelProvider").GetString()!;
    }

    private static bool IsManagedBackup(string directory)
    {
        string metadataPath = Path.Combine(directory, "metadata.json");
        if (!File.Exists(metadataPath))
        {
            return false;
        }
        try
        {
            using JsonDocument metadata = JsonDocument.Parse(File.ReadAllText(metadataPath));
            return metadata.RootElement.TryGetProperty("namespace", out JsonElement backupNamespace)
                && string.Equals(backupNamespace.GetString(), "provider-sync", StringComparison.Ordinal);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static string SqliteConnectionString(string path, SqliteOpenMode mode) =>
        new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = mode,
            Cache = SqliteCacheMode.Private,
            Pooling = false
        }.ToString();
}
