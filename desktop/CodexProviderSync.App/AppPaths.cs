using System.Diagnostics;
using CodexProviderSync.Core;

namespace CodexProviderSync.App;

internal interface IAppPathProvider
{
    bool IsAutomation { get; }
    string? IsolationRoot { get; }
    string SettingsPath { get; }
    string LogDirectory { get; }
    string SingletonDirectory { get; }
    string DefaultCodexHome { get; }
    string? RequiredSqliteHomeOverride { get; }
    string UpdateDownloadDirectory { get; }
    string UpdaterRoot { get; }
    string StartupErrorPath { get; }
    string? AutomationTracePath { get; }
    bool Contains(string path);
}

internal sealed class SystemAppPathProvider : IAppPathProvider
{
    private readonly string _settingsDirectory = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "codex-provider-sync");

    public bool IsAutomation => false;
    public string? IsolationRoot => null;
    public string SettingsPath => Path.Combine(_settingsDirectory, "settings.json");
    public string LogDirectory => Path.Combine(_settingsDirectory, "logs");
    public string SingletonDirectory => Path.Combine(_settingsDirectory, "singleton");
    public string DefaultCodexHome => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
        ".codex");
    public string? RequiredSqliteHomeOverride => null;
    public string UpdateDownloadDirectory => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "codex-provider-sync",
        "updates");
    public string UpdaterRoot => Path.Combine(Path.GetTempPath(), "codex-provider-sync-updater");
    public string StartupErrorPath => Path.Combine(_settingsDirectory, "startup-error.log");
    public string? AutomationTracePath => null;
    public bool Contains(string path) => true;
}

internal sealed class IsolatedAppPathProvider : IAppPathProvider
{
    private readonly StringComparison _comparison = OperatingSystem.IsWindows()
        ? StringComparison.OrdinalIgnoreCase
        : StringComparison.Ordinal;

    internal IsolatedAppPathProvider(string isolationRoot)
    {
        IsolationRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(isolationRoot));
    }

    public bool IsAutomation => true;
    public string IsolationRoot { get; }
    string? IAppPathProvider.IsolationRoot => IsolationRoot;
    public string SettingsPath => UnderRoot("appdata", "settings.json");
    public string LogDirectory => UnderRoot("appdata", "logs");
    public string SingletonDirectory => UnderRoot("appdata", "singleton");
    public string DefaultCodexHome => UnderRoot("codex-home");
    public string? RequiredSqliteHomeOverride => UnderRoot("sqlite-home");
    public string UpdateDownloadDirectory => UnderRoot("updates", "downloads");
    public string UpdaterRoot => UnderRoot("updates", "updater");
    public string StartupErrorPath => UnderRoot("appdata", "startup-error.log");
    public string? AutomationTracePath => UnderRoot("automation", "gui-trace.jsonl");

    public bool Contains(string path)
    {
        string candidate = Path.GetFullPath(path);
        string prefix = IsolationRoot + Path.DirectorySeparatorChar;
        bool lexicallyContained = string.Equals(candidate, IsolationRoot, _comparison)
            || candidate.StartsWith(prefix, _comparison);
        if (!lexicallyContained)
        {
            return false;
        }

        string relative = Path.GetRelativePath(IsolationRoot, candidate);
        string current = IsolationRoot;
        foreach (string segment in relative.Split(
            [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
            StringSplitOptions.RemoveEmptyEntries))
        {
            current = Path.Combine(current, segment);
            if ((File.Exists(current) || Directory.Exists(current))
                && (File.GetAttributes(current) & FileAttributes.ReparsePoint) != 0)
            {
                return false;
            }
        }
        return true;
    }

    private string UnderRoot(params string[] segments)
    {
        string path = segments.Aggregate(IsolationRoot, Path.Combine);
        if (!Contains(path))
        {
            throw new InvalidOperationException($"Automation path escaped the isolation root: {path}");
        }
        return path;
    }
}

internal static class AutomationIsolation
{
    internal static void PrepareSettings(SettingsService settingsService, IAppPathProvider paths)
    {
        if (!paths.IsAutomation)
        {
            return;
        }
        AppSettings current = settingsService.LoadAsync().GetAwaiter().GetResult();
        string lastHome = SafeContains(paths, current.LastCodexHome)
            ? Path.GetFullPath(current.LastCodexHome!)
            : paths.DefaultCodexHome;
        List<string> recents = current.RecentCodexHomes
            .Where(path => SafeContains(paths, path))
            .Select(Path.GetFullPath)
            .ToList();
        if (!recents.Contains(lastHome, OperatingSystem.IsWindows()
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal))
        {
            recents.Insert(0, lastHome);
        }
        Dictionary<string, string> overrides = current.SqliteHomeOverrides
            .Where(pair => SafeContains(paths, pair.Key) && SafeContains(paths, pair.Value))
            .ToDictionary(
                pair => Path.GetFullPath(pair.Key),
                pair => Path.GetFullPath(pair.Value),
                OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);

        if (paths.RequiredSqliteHomeOverride is { } requiredSqliteHome)
        {
            Directory.CreateDirectory(requiredSqliteHome);
            overrides[lastHome] = requiredSqliteHome;
        }

        Directory.CreateDirectory(lastHome);
        settingsService.Save(new AppSettings
        {
            RecentCodexHomes = recents,
            LastCodexHome = lastHome,
            SqliteHomeOverrides = overrides,
            SavedProviders = current.SavedProviders,
            ManualProviders = current.ManualProviders,
            LastSelectedProvider = current.LastSelectedProvider,
            LastBackupDirectory = SafeContains(paths, current.LastBackupDirectory)
                ? Path.GetFullPath(current.LastBackupDirectory!)
                : null,
            BackupRetentionCount = current.BackupRetentionCount,
            UiLanguage = current.UiLanguage,
            LastAutomaticUpdateCheckDate = current.LastAutomaticUpdateCheckDate,
            WindowBounds = current.WindowBounds
        });
    }

    private static bool SafeContains(IAppPathProvider paths, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }
        try
        {
            return paths.Contains(value);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException)
        {
            return false;
        }
    }
}

internal interface IAppPlatformBoundary
{
    bool UpdatesEnabled { get; }
    string? PickFolder(IWin32Window owner, FolderPickerRequest request);
    Task<UpdateCheckResult> CheckForUpdateAsync(
        UpdateService updateService,
        Version currentVersion,
        CancellationToken cancellationToken = default);
    void OpenPath(string path);
    void StartUpdate(string downloadedExePath, string targetExePath, string expectedSha256);
}

internal sealed record FolderPickerRequest(
    string Description,
    string InitialDirectory,
    bool AllowCreate = false);

internal static class AppFolderPickerDialog
{
    internal static FolderBrowserDialog Create(FolderPickerRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        return new FolderBrowserDialog
        {
            Description = request.Description,
            UseDescriptionForTitle = true,
            InitialDirectory = request.InitialDirectory,
            ShowNewFolderButton = request.AllowCreate
        };
    }
}

internal sealed class SystemAppPlatformBoundary(IAppPathProvider paths) : IAppPlatformBoundary
{
    public bool UpdatesEnabled => true;

    public string? PickFolder(IWin32Window owner, FolderPickerRequest request)
    {
        using FolderBrowserDialog dialog = AppFolderPickerDialog.Create(request);
        return dialog.ShowDialog(owner) == DialogResult.OK ? dialog.SelectedPath : null;
    }

    public Task<UpdateCheckResult> CheckForUpdateAsync(
        UpdateService updateService,
        Version currentVersion,
        CancellationToken cancellationToken = default) =>
        updateService.CheckForUpdateAsync(currentVersion, cancellationToken);

    public void OpenPath(string path)
    {
        // With UseShellExecute the shell may satisfy the request through a
        // process it already owns - Explorer reusing an open window is the
        // common case - and then returns no Process handle even though the
        // path opened. A null result therefore carries no failure information;
        // a genuine failure such as a missing path surfaces as Win32Exception.
        _ = Process.Start(new ProcessStartInfo
        {
            FileName = path,
            UseShellExecute = true
        });
    }

    public void StartUpdate(string downloadedExePath, string targetExePath, string expectedSha256) =>
        UpdateApplier.Start(downloadedExePath, targetExePath, expectedSha256, paths.UpdaterRoot);
}

internal sealed class IsolatedAppPlatformBoundary(IAppPathProvider paths) : IAppPlatformBoundary
{
    public bool UpdatesEnabled => true;

    public string? PickFolder(IWin32Window owner, FolderPickerRequest request)
    {
        EnsureContained(request.InitialDirectory);
        using FolderBrowserDialog dialog = AppFolderPickerDialog.Create(request);
        if (dialog.ShowDialog(owner) != DialogResult.OK)
        {
            return null;
        }

        EnsureContained(dialog.SelectedPath);
        return Path.GetFullPath(dialog.SelectedPath);
    }

    public Task<UpdateCheckResult> CheckForUpdateAsync(
        UpdateService updateService,
        Version currentVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(updateService);
        ArgumentNullException.ThrowIfNull(currentVersion);
        cancellationToken.ThrowIfCancellationRequested();
        Version normalized = UpdateService.NormalizeVersion(currentVersion);
        ReleaseInfo release = new($"v{normalized}", normalized, []);
        return Task.FromResult(new UpdateCheckResult(
            normalized,
            release,
            IsUpdateAvailable: false));
    }

    public void OpenPath(string path)
    {
        EnsureContained(path);
        // Deliberately no shell launch in automation mode. The real button event
        // still runs and reaches this boundary, which is recorded by the GUI trace.
    }

    public void StartUpdate(string downloadedExePath, string targetExePath, string expectedSha256) =>
        throw new InvalidOperationException("Self-update is disabled in GUI automation mode.");

    private void EnsureContained(string path)
    {
        if (!paths.Contains(path))
        {
            throw new InvalidOperationException($"Automation attempted to access a path outside its isolation root: {path}");
        }
    }
}
