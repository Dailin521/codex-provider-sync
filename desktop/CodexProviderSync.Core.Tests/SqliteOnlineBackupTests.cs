using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace CodexProviderSync.Core.Tests;

public sealed class SqliteOnlineBackupTests
{
    [Fact]
    public async Task WritesConfigureSynchronousFull_AndNodeDotNetCountersAgree()
    {
        await using Fixture fixture = Fixture.Create();
        await using (SqliteConnection setup = Open(fixture.DbPath))
        {
            await setup.OpenAsync();
            await ExecuteAsync(setup, """
                PRAGMA synchronous = OFF;
                CREATE TABLE threads (
                  id TEXT PRIMARY KEY,
                  model_provider TEXT,
                  model TEXT
                );
                INSERT INTO threads VALUES ('a', 'legacy', 'old');
                INSERT INTO threads VALUES ('b', 'openai', 'old');
                """);
            Assert.Equal(0L, await ScalarAsync(setup, "PRAGMA synchronous"));
            Assert.Equal(2, await SqliteStateService.ConfigureSqliteWriteDurabilityAsync(setup));
            Assert.Equal(2L, await ScalarAsync(setup, "PRAGMA synchronous"));
        }

        var update = await fixture.Service.UpdateSqliteProviderAsync(
            fixture.Storage,
            "openai",
            targetModel: "new");
        Assert.True(update.DatabasePresent);
        Assert.Equal(
            (3, 1, 2),
            (update.UpdatedRows, update.ProviderRowsUpdated, update.ModelRowsUpdated));

        await using SqliteConnection verified = Open(fixture.DbPath, SqliteOpenMode.ReadOnly);
        await verified.OpenAsync();
        await using SqliteCommand command = verified.CreateCommand();
        command.CommandText = "SELECT model_provider, model FROM threads ORDER BY id";
        await using SqliteDataReader reader = await command.ExecuteReaderAsync();
        int rowCount = 0;
        while (await reader.ReadAsync())
        {
            Assert.Equal("openai", reader.GetString(0));
            Assert.Equal("new", reader.GetString(1));
            rowCount += 1;
        }
        Assert.Equal(2, rowCount);
    }

    [Fact]
    public async Task OfficialOnlineBackup_CapturesLiveWalIntoOneStandaloneMainFile()
    {
        await using Fixture fixture = Fixture.Create();
        string backupPath = Path.Combine(fixture.Root, "backup", "state_5.sqlite");
        await using SqliteConnection source = Open(fixture.DbPath);
        await source.OpenAsync();
        await ExecuteAsync(source, "PRAGMA page_size = 8192; VACUUM;");
        Assert.Equal("wal", Convert.ToString(await ScalarObjectAsync(source, "PRAGMA journal_mode = WAL")));
        await ExecuteAsync(source, """
            PRAGMA user_version = 73;
            PRAGMA application_id = 1129333840;
            CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT);
            INSERT INTO threads VALUES ('wal-row', 'openai');
            """);
        Assert.True(new FileInfo(fixture.DbPath + "-wal").Length > 0);

        SqliteOnlineBackupResult result = await fixture.Service.CreateSqliteOnlineBackupAsync(
            fixture.Storage,
            backupPath);
        Assert.True(result.DatabasePresent);
        Assert.Equal(Path.GetFullPath(backupPath), result.BackupPath);
        Assert.NotNull(result.Metadata);
        Assert.Equal(new SqliteFileMetadata("wal", 8192, 73, 1129333840), result.Metadata.Source);
        Assert.Equal(result.Metadata.Source, result.Metadata.Backup);
        Assert.Equal(
            new SqliteOnlineBackupPreservation(true, true, true, true),
            result.Metadata.Preserved);
        Assert.True(File.Exists(backupPath));
        Assert.False(File.Exists(backupPath + "-wal"));
        Assert.False(File.Exists(backupPath + "-shm"));

        await source.CloseAsync();
        await using SqliteConnection backup = Open(backupPath, SqliteOpenMode.ReadOnly);
        await backup.OpenAsync();
        Assert.Equal(
            "openai",
            Convert.ToString(await ScalarObjectAsync(
                backup,
                "SELECT model_provider FROM threads WHERE id = 'wal-row'")));
    }

    [Fact]
    public async Task OnlineBackup_DoesNotRecreateSourceDatabaseThatDisappeared()
    {
        await using Fixture fixture = Fixture.Create();
        string backupPath = Path.Combine(fixture.Root, "backup", "state_5.sqlite");
        await using (SqliteConnection source = Open(fixture.DbPath))
        {
            await source.OpenAsync();
            await ExecuteAsync(source, "CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT)");
        }
        File.Delete(fixture.DbPath);
        CodexStorageLayout boundStorage = fixture.Storage with
        {
            StateDbLocation = new StateDbLocation(
                fixture.DbPath,
                AppConstants.DbFileBasename,
                "explicit")
        };

        await Assert.ThrowsAnyAsync<Exception>(
            () => fixture.Service.CreateSqliteOnlineBackupAsync(boundStorage, backupPath));

        Assert.False(File.Exists(fixture.DbPath));
        Assert.False(File.Exists(backupPath));
    }

    [Fact]
    public async Task ManagedBackup_SnapshotsLiveWal_AndManifestsOnlyStandaloneMainDatabases()
    {
        await using Fixture fixture = Fixture.Create();
        string configPath = Path.Combine(fixture.CodexHome, "config.toml");
        await File.WriteAllTextAsync(configPath, "model_provider = \"openai\"\n");

        await using SqliteConnection source = Open(fixture.DbPath);
        await source.OpenAsync();
        Assert.Equal("wal", Convert.ToString(await ScalarObjectAsync(source, "PRAGMA journal_mode = WAL")));
        await ExecuteAsync(source, """
            CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT);
            INSERT INTO threads VALUES ('live-wal-row', 'apigather');
            """);
        Assert.True(new FileInfo(fixture.DbPath + "-wal").Length > 0);

        BackupService backups = new(new SessionRolloutService(), fixture.Service);
        string backupDir = await backups.CreateBackupAsync(
            fixture.Storage,
            "openai",
            [],
            configPath);

        using JsonDocument metadata = JsonDocument.Parse(
            await File.ReadAllTextAsync(Path.Combine(backupDir, "metadata.json")));
        Assert.Equal(
            [AppConstants.DbFileBasename],
            metadata.RootElement.GetProperty("sqliteDbFiles")
                .EnumerateArray()
                .Select(static value => value.GetString()!)
                .ToArray());
        Assert.Equal(
            [Path.Combine(AppConstants.SqliteDirBasename, AppConstants.DbFileBasename)],
            metadata.RootElement.GetProperty("dbFiles")
                .EnumerateArray()
                .Select(static value => value.GetString()!)
                .ToArray());

        string canonicalBackupPath = Path.Combine(
            backupDir,
            "db",
            "sqlite-home",
            AppConstants.DbFileBasename);
        string legacyMirrorPath = Path.Combine(
            backupDir,
            "db",
            AppConstants.SqliteDirBasename,
            AppConstants.DbFileBasename);
        foreach (string backupPath in new[] { canonicalBackupPath, legacyMirrorPath })
        {
            Assert.True(File.Exists(backupPath));
            Assert.False(File.Exists(backupPath + "-wal"));
            Assert.False(File.Exists(backupPath + "-shm"));
        }

        await using (SqliteConnection backup = Open(canonicalBackupPath, SqliteOpenMode.ReadOnly))
        {
            await backup.OpenAsync();
            Assert.Equal(
                "apigather",
                Convert.ToString(await ScalarObjectAsync(
                    backup,
                    "SELECT model_provider FROM threads WHERE id = 'live-wal-row'")));
        }

        await ExecuteAsync(source, """
            UPDATE threads
            SET model_provider = 'live-after-backup'
            WHERE id = 'live-wal-row';
            INSERT INTO threads VALUES ('live-only-row', 'live-after-backup');
            """);
        Assert.True(new FileInfo(fixture.DbPath + "-wal").Length > 0);

        await backups.RestoreBackupAsync(
            backupDir,
            fixture.Storage,
            new RestoreBackupOptions
            {
                RestoreConfig = false,
                RestoreDatabase = true,
                RestoreSessions = false
            });
        Assert.Equal(
            "apigather",
            Convert.ToString(await ScalarObjectAsync(
                source,
                "SELECT model_provider FROM threads WHERE id = 'live-wal-row'")));
        Assert.Null(await ScalarObjectAsync(
            source,
            "SELECT model_provider FROM threads WHERE id = 'live-only-row'"));

        await ExecuteAsync(source, """
            UPDATE threads
            SET model_provider = 'live-before-failed-restore'
            WHERE id = 'live-wal-row';
            INSERT INTO threads VALUES ('failed-restore-must-preserve', 'live-before-failed-restore');
            """);
        byte[] walBeforeFailure = await ReadSharedFileAsync(fixture.DbPath + "-wal");
        await File.WriteAllTextAsync(canonicalBackupPath, "not a sqlite database");

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => backups.RestoreBackupAsync(
                backupDir,
                fixture.Storage,
                new RestoreBackupOptions
                {
                    RestoreConfig = false,
                    RestoreDatabase = true,
                    RestoreSessions = false
                }));
        Assert.Contains("malformed", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(walBeforeFailure, await ReadSharedFileAsync(fixture.DbPath + "-wal"));
        Assert.Equal(
            "live-before-failed-restore",
            Convert.ToString(await ScalarObjectAsync(
                source,
                "SELECT model_provider FROM threads WHERE id = 'live-wal-row'")));
        Assert.Equal(
            "live-before-failed-restore",
            Convert.ToString(await ScalarObjectAsync(
                source,
                "SELECT model_provider FROM threads WHERE id = 'failed-restore-must-preserve'")));
    }

    private sealed class Fixture : IAsyncDisposable
    {
        private Fixture(string root)
        {
            Root = root;
            CodexHome = Path.Combine(root, "codex-home");
            DbPath = Path.Combine(CodexHome, "sqlite", "state_5.sqlite");
            Directory.CreateDirectory(Path.GetDirectoryName(DbPath)!);
            Service = new SqliteStateService();
            Storage = new CodexStorageLayoutService().CreateDefault(CodexHome);
        }

        public string Root { get; }
        public string CodexHome { get; }
        public string DbPath { get; }
        public SqliteStateService Service { get; }
        public CodexStorageLayout Storage { get; }

        public static Fixture Create()
        {
            string root = Path.Combine(
                Path.GetTempPath(),
                $"provider-sync-sqlite-online-{Guid.NewGuid():N}");
            Directory.CreateDirectory(root);
            return new Fixture(root);
        }

        public ValueTask DisposeAsync()
        {
            try
            {
                Directory.Delete(Root, recursive: true);
            }
            catch
            {
                // SQLite teardown can briefly retain handles on Windows.
            }
            return ValueTask.CompletedTask;
        }
    }

    private static SqliteConnection Open(
        string dbPath,
        SqliteOpenMode mode = SqliteOpenMode.ReadWriteCreate)
    {
        return new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = mode,
            Pooling = false
        }.ConnectionString);
    }

    private static async Task ExecuteAsync(SqliteConnection connection, string sql)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<long> ScalarAsync(SqliteConnection connection, string sql)
    {
        return Convert.ToInt64(await ScalarObjectAsync(connection, sql));
    }

    private static async Task<object?> ScalarObjectAsync(SqliteConnection connection, string sql)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync();
    }

    private static async Task<byte[]> ReadSharedFileAsync(string filePath)
    {
        await using FileStream stream = new(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            4096,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        using MemoryStream copy = new();
        await stream.CopyToAsync(copy);
        return copy.ToArray();
    }
}
