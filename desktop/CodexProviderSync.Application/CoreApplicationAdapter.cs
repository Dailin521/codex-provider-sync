using CodexProviderSync.Core;

namespace CodexProviderSync.Application;

public sealed class CoreApplicationAdapter : ICoreApplicationAdapter
{
    private readonly CodexSyncService _syncService;
    private readonly SettingsService _settingsService;
    private readonly CodexHomeService _codexHomeService;

    public CoreApplicationAdapter()
        : this(new CodexSyncService(), new SettingsService(), new CodexHomeService())
    {
    }

    public CoreApplicationAdapter(
        CodexSyncService syncService,
        SettingsService settingsService,
        CodexHomeService codexHomeService)
    {
        _syncService = syncService ?? throw new ArgumentNullException(nameof(syncService));
        _settingsService = settingsService ?? throw new ArgumentNullException(nameof(settingsService));
        _codexHomeService = codexHomeService ?? throw new ArgumentNullException(nameof(codexHomeService));
    }

    public async Task<CoreInitializationState> InitializeAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        AppSettings settings = await _settingsService.LoadAsync();
        cancellationToken.ThrowIfCancellationRequested();

        string codexHome = _codexHomeService.NormalizeCodexHome(settings.LastCodexHome);
        string? sqliteHomeOverride = _settingsService.GetSqliteHomeOverride(settings, codexHome);
        return new CoreInitializationState(
            codexHome,
            sqliteHomeOverride,
            NormalizeOptional(settings.LastSelectedProvider));
    }

    public async Task<CoreRefreshState> RefreshAsync(
        CoreRefreshRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        // Other GUI slices still write through SettingsService during the
        // incremental migration. Reload here so a refresh never overwrites
        // those newer settings with an adapter-local stale copy.
        AppSettings settings = await _settingsService.LoadAsync();
        cancellationToken.ThrowIfCancellationRequested();
        string codexHome = _codexHomeService.NormalizeCodexHome(request.CodexHome);
        string? sqliteHomeOverride = NormalizeOptional(request.SqliteHomeOverride);
        StatusSnapshot status = await _syncService.GetStatusAsync(codexHome, sqliteHomeOverride);
        cancellationToken.ThrowIfCancellationRequested();

        AppSettings nextSettings = _settingsService.RecordCodexHome(settings, status.CodexHome);
        nextSettings = _settingsService.RecordSqliteHomeOverride(
            nextSettings,
            status.CodexHome,
            sqliteHomeOverride);
        nextSettings = _settingsService.MergeDetectedProviders(
            nextSettings,
            _syncService.ExtractDetectedProviderIds(status));
        nextSettings = _settingsService.UpdateState(
            nextSettings,
            NormalizeOptional(request.SelectedProviderId),
            nextSettings.LastBackupDirectory);

        IReadOnlyList<ProviderOption> providers = _syncService.BuildProviderOptions(status, nextSettings);
        await _settingsService.SaveAsync(nextSettings);
        cancellationToken.ThrowIfCancellationRequested();

        return new CoreRefreshState(status, providers);
    }

    private static string? NormalizeOptional(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }
}
