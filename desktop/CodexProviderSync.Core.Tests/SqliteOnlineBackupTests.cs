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
}
