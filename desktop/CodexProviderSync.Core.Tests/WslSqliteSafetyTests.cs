using System.Diagnostics;

namespace CodexProviderSync.Core.Tests;

public sealed class WslSqliteSafetyTests
{
    [WindowsWslIntegrationFact]
    [Trait("Category", "WindowsWslIntegration")]
    public async Task WindowsCore_DoesNotTouchRealWslSqliteHome()
    {
        string sqliteHome = Environment.GetEnvironmentVariable("CODEX_PROVIDER_SYNC_WSL_SQLITE_HOME")!;

        Assert.True(OperatingSystem.IsWindows(), "This integration test must run in a Windows process.");
        Assert.False(
            string.IsNullOrWhiteSpace(sqliteHome),
            "CPS_REQUIRE_REAL_WSL=1 requires CODEX_PROVIDER_SYNC_WSL_SQLITE_HOME from a real WSL filesystem.");
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
    }
}

public sealed class WindowsWslIntegrationFactAttribute : FactAttribute
{
    public WindowsWslIntegrationFactAttribute()
    {
        bool required = string.Equals(
            Environment.GetEnvironmentVariable("CPS_REQUIRE_REAL_WSL"),
            "1",
            StringComparison.Ordinal);
        if (!required
            && string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("CODEX_PROVIDER_SYNC_WSL_SQLITE_HOME")))
        {
            Skip = "Run scripts/test-wsl-unc-safety.sh from WSL to provide a real ext4 SQLite Home.";
        }
    }
}
