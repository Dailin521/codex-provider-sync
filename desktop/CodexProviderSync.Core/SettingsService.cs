using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace CodexProviderSync.Core;

public sealed class SettingsService
{
    public SettingsService(string? settingsPath = null)
    {
        SettingsPath = string.IsNullOrWhiteSpace(settingsPath)
            ? AppConstants.SettingsPath()
            : Path.GetFullPath(settingsPath);
    }

    public string SettingsPath { get; }

    public async Task<AppSettings> LoadAsync()
    {
        if (!File.Exists(SettingsPath))
        {
            return new AppSettings();
        }

        try
        {
            AppSettings? settings = JsonSerializer.Deserialize<AppSettings>(
                await File.ReadAllTextAsync(SettingsPath),
                JsonSerializerOptions());
            return Normalize(settings ?? new AppSettings());
        }
        catch
        {
            return new AppSettings();
        }
    }

    public async Task SaveAsync(AppSettings settings)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(SettingsPath)!);
        string json = JsonSerializer.Serialize(Normalize(settings), JsonSerializerOptions());
        await File.WriteAllTextAsync(SettingsPath, json);
    }

    public void Save(AppSettings settings)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(SettingsPath)!);
        string json = JsonSerializer.Serialize(Normalize(settings), JsonSerializerOptions());
        File.WriteAllText(SettingsPath, json);
    }

    public AppSettings RecordCodexHome(AppSettings settings, string codexHome)
    {
        List<string> recents = Deduplicate([codexHome, .. settings.RecentCodexHomes.Select(Path.GetFullPath)])
            .Take(10)
            .ToList();

        return new AppSettings
        {
            RecentCodexHomes = recents,
            LastCodexHome = Path.GetFullPath(codexHome),
            SavedProviders = Deduplicate(settings.SavedProviders).ToList(),
            ManualProviders = Deduplicate(settings.ManualProviders).ToList(),
            LastSelectedProvider = settings.LastSelectedProvider,
            LastBackupDirectory = settings.LastBackupDirectory,
            BackupRetentionCount = NormalizeBackupRetentionCount(settings.BackupRetentionCount),
            WindowBounds = settings.WindowBounds
        };
    }

    public AppSettings MergeDetectedProviders(AppSettings settings, IEnumerable<string> providerIds)
    {
        return new AppSettings
        {
            RecentCodexHomes = Deduplicate(settings.RecentCodexHomes).ToList(),
            LastCodexHome = settings.LastCodexHome,
            SavedProviders = Deduplicate([.. settings.SavedProviders, .. providerIds]).ToList(),
            ManualProviders = Deduplicate(settings.ManualProviders).ToList(),
            LastSelectedProvider = settings.LastSelectedProvider,
            LastBackupDirectory = settings.LastBackupDirectory,
            BackupRetentionCount = NormalizeBackupRetentionCount(settings.BackupRetentionCount),
            WindowBounds = settings.WindowBounds
        };
    }

    public AppSettings AddManualProvider(AppSettings settings, string providerId)
    {
        return new AppSettings
        {
            RecentCodexHomes = Deduplicate(settings.RecentCodexHomes).ToList(),
            LastCodexHome = settings.LastCodexHome,
            SavedProviders = Deduplicate([.. settings.SavedProviders, providerId]).ToList(),
            ManualProviders = Deduplicate([.. settings.ManualProviders, providerId]).ToList(),
            LastSelectedProvider = providerId,
            LastBackupDirectory = settings.LastBackupDirectory,
            BackupRetentionCount = NormalizeBackupRetentionCount(settings.BackupRetentionCount),
            WindowBounds = settings.WindowBounds
        };
    }

    public AppSettings RemoveManualProvider(AppSettings settings, string providerId)
    {
        return new AppSettings
        {
            RecentCodexHomes = Deduplicate(settings.RecentCodexHomes).ToList(),
            LastCodexHome = settings.LastCodexHome,
            SavedProviders = settings.SavedProviders.Where(provider => !string.Equals(provider, providerId, StringComparison.Ordinal)).Order(StringComparer.Ordinal).ToList(),
            ManualProviders = settings.ManualProviders.Where(provider => !string.Equals(provider, providerId, StringComparison.Ordinal)).Order(StringComparer.Ordinal).ToList(),
            LastSelectedProvider = string.Equals(settings.LastSelectedProvider, providerId, StringComparison.Ordinal) ? null : settings.LastSelectedProvider,
            LastBackupDirectory = settings.LastBackupDirectory,
            BackupRetentionCount = NormalizeBackupRetentionCount(settings.BackupRetentionCount),
            WindowBounds = settings.WindowBounds
        };
    }

    public AppSettings UpdateState(
        AppSettings settings,
        string? selectedProvider,
        string? backupDirectory,
        WindowBoundsState? bounds = null,
        int? backupRetentionCount = null)
    {
        return new AppSettings
        {
            RecentCodexHomes = Deduplicate(settings.RecentCodexHomes).ToList(),
            LastCodexHome = settings.LastCodexHome,
            SavedProviders = Deduplicate(settings.SavedProviders).ToList(),
            ManualProviders = Deduplicate(settings.ManualProviders).ToList(),
            LastSelectedProvider = string.IsNullOrWhiteSpace(selectedProvider) ? settings.LastSelectedProvider : selectedProvider.Trim(),
            LastBackupDirectory = string.IsNullOrWhiteSpace(backupDirectory) ? settings.LastBackupDirectory : Path.GetFullPath(backupDirectory),
            BackupRetentionCount = NormalizeBackupRetentionCount(backupRetentionCount ?? settings.BackupRetentionCount),
            WindowBounds = bounds ?? settings.WindowBounds
        };
    }

    private static AppSettings Normalize(AppSettings settings)
    {
        return new AppSettings
        {
            RecentCodexHomes = Deduplicate(settings.RecentCodexHomes.Select(Path.GetFullPath)).Take(10).ToList(),
            LastCodexHome = string.IsNullOrWhiteSpace(settings.LastCodexHome) ? null : Path.GetFullPath(settings.LastCodexHome),
            SavedProviders = Deduplicate(settings.SavedProviders).ToList(),
            ManualProviders = Deduplicate(settings.ManualProviders).ToList(),
            LastSelectedProvider = string.IsNullOrWhiteSpace(settings.LastSelectedProvider) ? null : settings.LastSelectedProvider.Trim(),
            LastBackupDirectory = string.IsNullOrWhiteSpace(settings.LastBackupDirectory) ? null : Path.GetFullPath(settings.LastBackupDirectory),
            BackupRetentionCount = NormalizeBackupRetentionCount(settings.BackupRetentionCount),
            WindowBounds = settings.WindowBounds
        };
    }

    private static IEnumerable<string> Deduplicate(IEnumerable<string> values)
    {
        return values
            .Where(static value => !string.IsNullOrWhiteSpace(value))
            .Select(static value => value.Trim())
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal);
    }

    private static JsonSerializerOptions JsonSerializerOptions()
    {
        return new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true
        };
    }

    private static int NormalizeBackupRetentionCount(int value)
    {
        return value < 1 ? AppConstants.DefaultBackupRetentionCount : value;
    }
}
