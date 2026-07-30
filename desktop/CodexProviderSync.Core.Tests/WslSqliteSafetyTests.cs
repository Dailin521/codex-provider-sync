using System.Diagnostics;
using System.Security.Cryptography;

namespace CodexProviderSync.Core.Tests;

public sealed class WslSqliteSafetyTests
{
    [Fact]
    [Trait("Category", "WindowsWslIntegration")]
    public async Task WindowsCore_DoesNotTouchRealWslSqliteHome()
    {
        string? sqliteHome = Environment.GetEnvironmentVariable("CODEX_PROVIDER_SYNC_WSL_SQLITE_HOME");
        if (string.IsNullOrWhiteSpace(sqliteHome))
        {
            return;
        }

        Assert.True(OperatingSystem.IsWindows(), "This integration test must run in a Windows process.");
        string stateDbPath = Path.Combine(sqliteHome, AppConstants.DbFileBasename);
        Assert.True(File.Exists(stateDbPath), $"Expected a real WSL SQLite database at {stateDbPath}.");
        string originalDatabaseHash = await Sha256Async(stateDbPath);

        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-wsl-safety.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-wsl-safety", "apigather");
        string configPath = Path.Combine(fixture.CodexHome, "config.toml");
        string originalConfig = await File.ReadAllTextAsync(configPath);
        string originalRollout = await File.ReadAllTextAsync(rolloutPath);
        CodexSyncService syncService = new();

        Stopwatch timer = Stopwatch.StartNew();
        StatusSnapshot status = await syncService.GetStatusAsync(fixture.CodexHome, sqliteHome);
        timer.Stop();

        Assert.False(status.SqliteAccess.Supported);
        Assert.Equal("windows-wsl-unc", status.SqliteAccess.Reason);
        Assert.Null(status.SqliteCounts);
        Assert.True(timer.Elapsed < TimeSpan.FromSeconds(5), $"Status took {timer.Elapsed}.");

        InvalidOperationException syncError = await Assert.ThrowsAsync<InvalidOperationException>(
            () => syncService.RunSyncAsync(fixture.CodexHome, explicitSqliteHome: sqliteHome));
        InvalidOperationException switchError = await Assert.ThrowsAsync<InvalidOperationException>(
            () => syncService.RunSwitchAsync(fixture.CodexHome, "apigather", explicitSqliteHome: sqliteHome));
        InvalidOperationException restoreError = await Assert.ThrowsAsync<InvalidOperationException>(
            () => syncService.RunRestoreAsync(
                fixture.CodexHome,
                Path.Combine(fixture.Root, "missing-backup"),
                sqliteHome));
        CodexStorageLayout storage = new CodexStorageLayoutService().Resolve(
            fixture.CodexHome,
            sqliteHome,
            originalConfig,
            new Dictionary<string, string?>());
        BackupService backupService = new(new SessionRolloutService(), new SqliteStateService());
        InvalidOperationException backupError = await Assert.ThrowsAsync<InvalidOperationException>(
            () => backupService.CreateBackupAsync(
                storage,
                "openai",
                [],
                configPath));

        Assert.Contains("Cannot sync", syncError.Message);
        Assert.Contains("Cannot switch", switchError.Message);
        Assert.Contains("Cannot restore", restoreError.Message);
        Assert.Contains("Cannot create a backup", backupError.Message);
        Assert.Equal(originalConfig, await File.ReadAllTextAsync(configPath));
        Assert.Equal(originalRollout, await File.ReadAllTextAsync(rolloutPath));
        Assert.False(Directory.Exists(fixture.BackupRoot()));
        Assert.Equal(originalDatabaseHash, await Sha256Async(stateDbPath));
    }

    private static async Task<string> Sha256Async(string path)
    {
        await using FileStream stream = File.OpenRead(path);
        byte[] hash = await SHA256.HashDataAsync(stream);
        return Convert.ToHexStringLower(hash);
    }
}
