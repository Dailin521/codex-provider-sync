namespace CodexProviderSync.Core.Tests;

public sealed class SettingsAndDiscoveryTests
{
    [Fact]
    public void ConfigFileService_ReadsBasicAndLiteralSqliteHome()
    {
        ConfigFileService service = new();

        Assert.Equal(
            @"C:\Users\cheng\.codex\sqlite",
            service.ReadSqliteHomeFromConfigText("sqlite_home = 'C:\\Users\\cheng\\.codex\\sqlite'\n[model_providers.custom]\n"));
        Assert.Equal(
            "C:\\Users\\cheng\\.codex\\sqlite",
            service.ReadSqliteHomeFromConfigText("sqlite_home = \"C:\\\\Users\\\\cheng\\\\.codex\\\\sqlite\" # comment\n"));
    }

    [Fact]
    public void StorageLayout_UsesExplicitConfigEnvironmentAndDefaultPrecedence()
    {
        string root = Path.Combine(Path.GetTempPath(), $"storage-layout-{Guid.NewGuid():N}");
        string codexHome = Path.Combine(root, ".codex");
        CodexStorageLayoutService service = new();
        Dictionary<string, string?> environment = new()
        {
            ["CODEX_SQLITE_HOME"] = Path.Combine(root, "env")
        };

        CodexStorageLayout explicitLayout = service.Resolve(
            codexHome,
            Path.Combine(root, "explicit"),
            $"sqlite_home = '{Path.Combine(root, "config")}'\n",
            environment);
        CodexStorageLayout configLayout = service.Resolve(
            codexHome,
            null,
            $"sqlite_home = '{Path.Combine(root, "config")}'\n",
            environment);
        CodexStorageLayout environmentLayout = service.Resolve(codexHome, null, string.Empty, environment);
        CodexStorageLayout defaultLayout = service.Resolve(
            codexHome,
            null,
            string.Empty,
            new Dictionary<string, string?>());

        Assert.Equal("gui", explicitLayout.SqliteHomeSource);
        Assert.Equal("config", configLayout.SqliteHomeSource);
        Assert.Equal("env", environmentLayout.SqliteHomeSource);
        Assert.Equal("default", defaultLayout.SqliteHomeSource);
        Assert.Single(explicitLayout.StateDbCandidates);
        Assert.Equal(2, defaultLayout.StateDbCandidates.Count);
    }

    [Fact]
    public void ConfigFileService_UpdatesCompactRootModelAssignment()
    {
        ConfigFileService service = new();

        string updated = service.SetRootModelInConfigText(
            "model_provider=\"openai\"\nmodel=\"old\"\n\n[model_providers.apigather]\nmodel=\"section\"\n",
            "new");

        Assert.Contains("model = \"new\"", updated);
        Assert.Contains("model=\"section\"", updated);
        Assert.Equal(1, updated.Split("model = \"new\"", StringSplitOptions.None).Length - 1);
    }

    [Fact]
    public void ConfigFileService_ReadsProviderModel_WhenProviderIdContainsRegexCharacters()
    {
        ConfigFileService service = new();
        string config = "[model_providers.foo.bar+v2]\nmodel = \"special\"\n";

        Assert.Equal("special", service.ReadProviderModel(config, "foo.bar+v2"));
    }

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
            BackupRetentionCount = 7,
            UiLanguage = "zh-Hans",
            LastAutomaticUpdateCheckDate = new DateOnly(2026, 7, 23)
        };

        await service.SaveAsync(settings);
        AppSettings loaded = await service.LoadAsync();

        Assert.Contains("apigather", loaded.SavedProviders);
        Assert.Contains("custom-a", loaded.ManualProviders);
        Assert.Equal("apigather", loaded.LastSelectedProvider);
        Assert.Equal(7, loaded.BackupRetentionCount);
        Assert.Equal("zh-Hans", loaded.UiLanguage);
        Assert.Equal(new DateOnly(2026, 7, 23), loaded.LastAutomaticUpdateCheckDate);
    }

    [Fact]
    public async Task SettingsService_PersistsSqliteHomeOverridePerCodexHome()
    {
        string root = Path.Combine(Path.GetTempPath(), $"codex-provider-settings-{Guid.NewGuid():N}");
        SettingsService service = new(Path.Combine(root, "settings.json"));
        string codexHomeA = Path.Combine(root, "codex-a");
        string codexHomeB = Path.Combine(root, "codex-b");
        string sqliteHomeA = Path.Combine(root, "sqlite-a");

        AppSettings settings = service.RecordSqliteHomeOverride(new AppSettings(), codexHomeA, sqliteHomeA);
        settings = service.RecordCodexHome(settings, codexHomeB);
        await service.SaveAsync(settings);
        AppSettings loaded = await service.LoadAsync();

        Assert.Equal(Path.GetFullPath(sqliteHomeA), service.GetSqliteHomeOverride(loaded, codexHomeA));
        Assert.Null(service.GetSqliteHomeOverride(loaded, codexHomeB));
    }

    [Fact]
    public async Task SettingsService_OldSettingsDefaultToNoAutomaticUpdateCheck()
    {
        string uniqueSettingsRoot = Path.Combine(Path.GetTempPath(), $"codex-provider-settings-{Guid.NewGuid():N}");
        string settingsPath = Path.Combine(uniqueSettingsRoot, "settings.json");
        Directory.CreateDirectory(uniqueSettingsRoot);
        await File.WriteAllTextAsync(settingsPath, """{"backupRetentionCount":5,"uiLanguage":"zh-Hans"}""");

        try
        {
            SettingsService service = new(settingsPath);

            AppSettings loaded = await service.LoadAsync();

            Assert.Null(loaded.LastAutomaticUpdateCheckDate);
        }
        finally
        {
            Directory.Delete(uniqueSettingsRoot, recursive: true);
        }
    }

    [Fact]
    public void SettingsService_RecordAutomaticUpdateCheckPreservesOtherState()
    {
        SettingsService service = new(Path.Combine(Path.GetTempPath(), $"unused-{Guid.NewGuid():N}.json"));
        AppSettings settings = new()
        {
            LastCodexHome = @"C:\Users\Administrator\.codex",
            SavedProviders = ["openai"],
            LastSelectedProvider = "openai",
            BackupRetentionCount = 7
        };

        AppSettings updated = service.RecordAutomaticUpdateCheck(settings, new DateOnly(2026, 7, 23));

        Assert.Equal(new DateOnly(2026, 7, 23), updated.LastAutomaticUpdateCheckDate);
        Assert.Equal(settings.LastCodexHome, updated.LastCodexHome);
        Assert.Equal(settings.LastSelectedProvider, updated.LastSelectedProvider);
        Assert.Equal(settings.BackupRetentionCount, updated.BackupRetentionCount);
        Assert.Contains("openai", updated.SavedProviders);
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
            LockedRolloutFiles = [],
            UnreadableRolloutFiles = [],
            EncryptedContentCounts = new ProviderCounts(),
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
