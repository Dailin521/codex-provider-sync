namespace CodexProviderSync.Core.Tests;

public sealed class SettingsAndDiscoveryTests
{
    [Fact]
    public async Task SettingsService_PersistsRecentPathsAndProviders()
    {
        string uniqueSettingsRoot = Path.Combine(Path.GetTempPath(), $"codex-provider-settings-{Guid.NewGuid():N}");
        SettingsService service = new(Path.Combine(uniqueSettingsRoot, "settings.json"));
        AppSettings settings = new()
        {
            RecentCodexHomes = ["C:\\Users\\Administrator\\.codex"],
            SavedProviders = ["apigather"],
            ManualProviders = ["custom-a"],
            LastSelectedProvider = "apigather",
            BackupRetentionCount = 7
        };

        await service.SaveAsync(settings);
        AppSettings loaded = await service.LoadAsync();

        Assert.Contains("apigather", loaded.SavedProviders);
        Assert.Contains("custom-a", loaded.ManualProviders);
        Assert.Equal("apigather", loaded.LastSelectedProvider);
        Assert.Equal(7, loaded.BackupRetentionCount);
    }

    [Fact]
    public void ProviderDiscovery_MergesDetectedAndManualProviders()
    {
        ProviderDiscoveryService service = new();
        StatusSnapshot status = new()
        {
            CodexHome = "C:\\Users\\Administrator\\.codex",
            CurrentProvider = new CurrentProviderInfo("openai", false),
            ConfiguredProviders = ["apigather", "openai"],
            RolloutCounts = new ProviderCounts
            {
                Sessions = new Dictionary<string, int>(StringComparer.Ordinal)
                {
                    ["newapi"] = 2
                },
                ArchivedSessions = new Dictionary<string, int>(StringComparer.Ordinal)
            },
            SqliteCounts = new ProviderCounts
            {
                Sessions = new Dictionary<string, int>(StringComparer.Ordinal),
                ArchivedSessions = new Dictionary<string, int>(StringComparer.Ordinal)
                {
                    ["azure"] = 1
                }
            },
            BackupRoot = "C:\\Users\\Administrator\\.codex\\backups_state\\provider-sync",
            BackupSummary = new BackupSummary
            {
                Count = 2,
                TotalBytes = 1024
            }
        };
        AppSettings settings = new()
        {
            SavedProviders = ["saved-only"],
            ManualProviders = ["manual-only"]
        };

        IReadOnlyList<ProviderOption> options = service.BuildProviderOptions(status, settings);

        Assert.Contains(options, option => option.Id == "openai" && option.IsCurrentProvider);
        Assert.Contains(options, option => option.Id == "apigather" && option.Sources.Contains(ProviderSource.Config));
        Assert.Contains(options, option => option.Id == "newapi" && option.Sources.Contains(ProviderSource.Rollout));
        Assert.Contains(options, option => option.Id == "azure" && option.Sources.Contains(ProviderSource.Sqlite));
        Assert.Contains(options, option => option.Id == "manual-only" && option.IsManual);
    }
}
