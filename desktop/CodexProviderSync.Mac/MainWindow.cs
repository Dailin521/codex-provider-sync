using System.Diagnostics;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Layout;
using Avalonia.Media;
using Avalonia.Platform.Storage;
using CodexProviderSync.Core;

namespace CodexProviderSync.Mac;

public sealed class MainWindow : Window
{
    private readonly CodexSyncService _syncService = new();
    private readonly SettingsService _settingsService = new();

    private readonly TextBox _codexHomeText = new();
    private readonly TextBlock _sqliteHomeLabel = new();
    private readonly TextBox _sqliteHomeText = new();
    private readonly Button _browseSqliteHomeButton = new();
    private readonly ComboBox _recentHomes = new();
    private readonly TextBlock _languageLabel = new();
    private readonly ComboBox _languageCombo = new();
    private readonly Button _browseButton = new();
    private readonly Button _refreshButton = new();
    private readonly TextBlock _busyText = new();
    private readonly TextBox _statusText = new();
    private readonly ListBox _providerList = new();
    private readonly TextBox _manualProviderText = new();
    private readonly Button _addProviderButton = new();
    private readonly Button _removeProviderButton = new();
    private readonly TextBlock _selectedProviderText = new();
    private readonly CheckBox _switchConfigCheck = new();
    private readonly NumericUpDown _backupRetentionInput = new();
    private readonly Button _executeButton = new();
    private readonly CheckBox _restoreConfigCheck = new();
    private readonly CheckBox _restoreDatabaseCheck = new();
    private readonly CheckBox _restoreSessionsCheck = new();
    private readonly Button _restoreButton = new();
    private readonly Button _openBackupButton = new();
    private readonly Button _pruneBackupsButton = new();
    private readonly TextBox _logText = new();
    private readonly TextBlock _codexHomeLabel = new();
    private readonly TextBlock _statusTitle = new();
    private readonly TextBlock _providersTitle = new();
    private readonly TextBlock _actionsTitle = new();
    private readonly TextBlock _logTitle = new();
    private readonly TextBlock _targetProviderLabel = new();
    private readonly TextBlock _restoreContentsLabel = new();
    private readonly TextBlock _keepBackupsLabel = new();
    private readonly TextBlock _writeWarningText = new();

    private AppSettings _settings = new();
    private StatusSnapshot? _currentStatus;
    private bool _loadingSettings;
    private string _language = MacUiText.English;
    private string? _sqliteOverrideCodexHome;

    public MainWindow()
    {
        Title = "Codex Provider Sync";
        MinWidth = 1120;
        MinHeight = 760;
        Width = 1240;
        Height = 820;
        Background = Brush.Parse("#f5f5f7");

        Content = BuildLayout();
        WireEvents();
    }

    protected override async void OnOpened(EventArgs e)
    {
        base.OnOpened(e);
        await LoadStateAsync();
    }

    protected override void OnClosing(WindowClosingEventArgs e)
    {
        PersistUiState();
        base.OnClosing(e);
    }

    private Control BuildLayout()
    {
        Grid root = new()
        {
            RowDefinitions = new RowDefinitions("Auto,*,220"),
            Margin = new Thickness(18),
            RowSpacing = 14
        };

        root.Children.Add(BuildCodexHomePanel());
        root.Children.Add(BuildMainPanel());
        root.Children.Add(BuildLogPanel());
        Grid.SetRow(root.Children[1], 1);
        Grid.SetRow(root.Children[2], 2);
        return root;
    }

    private Control BuildCodexHomePanel()
    {
        Grid panel = new()
        {
            ColumnDefinitions = new ColumnDefinitions("Auto,*,170,Auto,Auto,Auto,132"),
            RowDefinitions = new RowDefinitions("Auto,Auto"),
            RowSpacing = 8,
            ColumnSpacing = 10
        };

        ConfigureLabel(_codexHomeLabel, "Codex Home");
        ConfigureLabel(_sqliteHomeLabel, "SQLite Home");
        ConfigureLabel(_languageLabel, "Language");
        _codexHomeText.PlaceholderText = AppConstants.DefaultCodexHome();
        _codexHomeText.MinHeight = 34;
        _recentHomes.MinHeight = 34;
        _recentHomes.PlaceholderText = "Recent";
        _languageCombo.MinHeight = 34;
        _languageCombo.Items.Add("English");
        _languageCombo.Items.Add("中文");
        _languageCombo.SelectedIndex = 0;
        _browseButton.Content = "Browse";
        _browseButton.MinHeight = 34;
        _refreshButton.Content = "Refresh";
        _refreshButton.MinHeight = 34;
        _sqliteHomeText.PlaceholderText = "Resolved automatically when empty";
        _sqliteHomeText.MinHeight = 34;
        _browseSqliteHomeButton.Content = "Browse";
        _browseSqliteHomeButton.MinHeight = 34;

        panel.Children.Add(_codexHomeLabel);
        panel.Children.Add(_codexHomeText);
        panel.Children.Add(_recentHomes);
        panel.Children.Add(_browseButton);
        panel.Children.Add(_refreshButton);
        panel.Children.Add(_languageLabel);
        panel.Children.Add(_languageCombo);
        panel.Children.Add(_sqliteHomeLabel);
        panel.Children.Add(_sqliteHomeText);
        panel.Children.Add(_browseSqliteHomeButton);
        Grid.SetColumn(_codexHomeText, 1);
        Grid.SetColumn(_recentHomes, 2);
        Grid.SetColumn(_browseButton, 3);
        Grid.SetColumn(_refreshButton, 4);
        Grid.SetColumn(_languageLabel, 5);
        Grid.SetColumn(_languageCombo, 6);
        Grid.SetRow(_sqliteHomeLabel, 1);
        Grid.SetRow(_sqliteHomeText, 1);
        Grid.SetColumn(_sqliteHomeText, 1);
        Grid.SetColumnSpan(_sqliteHomeText, 2);
        Grid.SetRow(_browseSqliteHomeButton, 1);
        Grid.SetColumn(_browseSqliteHomeButton, 3);
        return Card(panel);
    }

    private Control BuildMainPanel()
    {
        Grid main = new()
        {
            ColumnDefinitions = new ColumnDefinitions("1.15*,1*,340"),
            ColumnSpacing = 14
        };

        main.Children.Add(BuildStatusPanel());
        main.Children.Add(BuildProviderPanel());
        main.Children.Add(BuildActionsPanel());
        Grid.SetColumn(main.Children[1], 1);
        Grid.SetColumn(main.Children[2], 2);
        return main;
    }

    private Control BuildStatusPanel()
    {
        _statusText.IsReadOnly = true;
        _statusText.AcceptsReturn = true;
        _statusText.TextWrapping = TextWrapping.NoWrap;
        _statusText.FontFamily = FontFamily.Parse("Menlo, Consolas, monospace");
        _statusText.FontSize = 12;
        _statusText.Background = Brushes.White;

        return Section(_statusTitle, _statusText);
    }

    private Control BuildProviderPanel()
    {
        Grid panel = new()
        {
            RowDefinitions = new RowDefinitions("*,Auto,Auto"),
            RowSpacing = 10
        };

        _providerList.SelectionMode = SelectionMode.Single;
        _providerList.Background = Brushes.White;

        Grid addPanel = new()
        {
            ColumnDefinitions = new ColumnDefinitions("*,Auto"),
            ColumnSpacing = 8
        };
        _manualProviderText.PlaceholderText = "Add provider id";
        _manualProviderText.MinHeight = 34;
        _addProviderButton.Content = "Add";
        _addProviderButton.MinHeight = 34;
        addPanel.Children.Add(_manualProviderText);
        addPanel.Children.Add(_addProviderButton);
        Grid.SetColumn(_addProviderButton, 1);

        _removeProviderButton.Content = "Remove Manual";
        _removeProviderButton.HorizontalAlignment = HorizontalAlignment.Left;

        panel.Children.Add(_providerList);
        panel.Children.Add(addPanel);
        panel.Children.Add(_removeProviderButton);
        Grid.SetRow(addPanel, 1);
        Grid.SetRow(_removeProviderButton, 2);

        return Section(_providersTitle, panel);
    }

    private Control BuildActionsPanel()
    {
        StackPanel panel = new()
        {
            Spacing = 12
        };

        _selectedProviderText.Text = "No provider selected";
        _selectedProviderText.FontSize = 18;
        _selectedProviderText.FontWeight = FontWeight.SemiBold;
        _selectedProviderText.TextWrapping = TextWrapping.Wrap;

        _switchConfigCheck.Content = "Switch config.toml and sync";

        _backupRetentionInput.Minimum = 1;
        _backupRetentionInput.Maximum = 100000;
        _backupRetentionInput.Value = AppConstants.DefaultBackupRetentionCount;
        _backupRetentionInput.Width = 112;

        _executeButton.Content = "Sync Metadata Only";
        _executeButton.MinHeight = 40;
        _executeButton.HorizontalAlignment = HorizontalAlignment.Stretch;

        _restoreConfigCheck.Content = "Restore config.toml";
        _restoreDatabaseCheck.Content = "Restore SQLite";
        _restoreSessionsCheck.Content = "Restore rollout metadata";
        _restoreConfigCheck.IsChecked = false;
        _restoreDatabaseCheck.IsChecked = true;
        _restoreSessionsCheck.IsChecked = true;

        _restoreButton.Content = "Restore Backup";
        _restoreButton.MinHeight = 38;
        _openBackupButton.Content = "Open Backup Folder";
        _openBackupButton.MinHeight = 38;
        _pruneBackupsButton.Content = "Clean Old Backups";
        _pruneBackupsButton.MinHeight = 38;

        _busyText.Text = "Ready";
        _busyText.Foreground = Brush.Parse("#166534");

        ConfigureMutedLabel(_targetProviderLabel, "Target provider");
        ConfigureMutedLabel(_restoreContentsLabel, "Restore contents");

        panel.Children.Add(_targetProviderLabel);
        panel.Children.Add(_selectedProviderText);
        panel.Children.Add(_switchConfigCheck);
        panel.Children.Add(BuildRetentionPanel());
        panel.Children.Add(WarningBlock(_writeWarningText));
        panel.Children.Add(_executeButton);
        panel.Children.Add(Separator());
        panel.Children.Add(_restoreContentsLabel);
        panel.Children.Add(_restoreConfigCheck);
        panel.Children.Add(_restoreDatabaseCheck);
        panel.Children.Add(_restoreSessionsCheck);
        panel.Children.Add(_restoreButton);
        panel.Children.Add(_openBackupButton);
        panel.Children.Add(_pruneBackupsButton);
        panel.Children.Add(_busyText);

        return Section(_actionsTitle, panel);
    }

    private Control BuildRetentionPanel()
    {
        StackPanel panel = new()
        {
            Orientation = Orientation.Horizontal,
            Spacing = 8,
            VerticalAlignment = VerticalAlignment.Center
        };
        ConfigureMutedLabel(_keepBackupsLabel, "Keep backups");
        panel.Children.Add(_keepBackupsLabel);
        panel.Children.Add(_backupRetentionInput);
        return panel;
    }

    private Control BuildLogPanel()
    {
        _logText.IsReadOnly = true;
        _logText.AcceptsReturn = true;
        _logText.FontFamily = FontFamily.Parse("Menlo, Consolas, monospace");
        _logText.FontSize = 12;
        _logText.Background = Brushes.White;
        return Section(_logTitle, _logText);
    }

    private void WireEvents()
    {
        _browseButton.Click += async (_, _) => await BrowseCodexHomeAsync();
        _browseSqliteHomeButton.Click += async (_, _) => await BrowseSqliteHomeAsync();
        _refreshButton.Click += async (_, _) => await RefreshStatusAsync();
        _languageCombo.SelectionChanged += async (_, _) => await ChangeLanguageAsync();
        _recentHomes.SelectionChanged += (_, _) =>
        {
            if (_recentHomes.SelectedItem is string home)
            {
                _codexHomeText.Text = home;
                LoadSqliteHomeOverride(home);
            }
        };
        _addProviderButton.Click += async (_, _) => await AddManualProviderAsync();
        _removeProviderButton.Click += async (_, _) => await RemoveManualProviderAsync();
        _providerList.SelectionChanged += (_, _) => UpdateSelectionLabel();
        _switchConfigCheck.PropertyChanged += (_, args) =>
        {
            if (args.Property.Name == nameof(CheckBox.IsChecked))
            {
                UpdateExecuteButtonText();
            }
        };
        _backupRetentionInput.ValueChanged += async (_, _) => await PersistBackupRetentionAsync();
        _executeButton.Click += async (_, _) => await ExecuteSyncOrSwitchAsync();
        _restoreButton.Click += async (_, _) => await RestoreBackupAsync();
        _openBackupButton.Click += async (_, _) => await OpenBackupFolderAsync();
        _pruneBackupsButton.Click += async (_, _) => await PruneBackupsAsync();
        _codexHomeText.LostFocus += async (_, _) => await PersistHomeSelectionAsync();
        _sqliteHomeText.LostFocus += async (_, _) => await PersistSqliteHomeOverrideAsync();
        _manualProviderText.KeyDown += async (_, args) =>
        {
            if (args.Key == Key.Enter)
            {
                args.Handled = true;
                await AddManualProviderAsync();
            }
        };
    }

    private async Task LoadStateAsync()
    {
        _loadingSettings = true;
        _settings = await _settingsService.LoadAsync();
        _language = MacUiText.NormalizeLanguage(_settings.UiLanguage);
        _languageCombo.SelectedIndex = MacUiText.IsChinese(_language) ? 1 : 0;
        ApplyLanguage();
        ApplyWindowBounds(_settings.WindowBounds);
        ReloadRecentHomes();
        _codexHomeText.Text = _settings.LastCodexHome ?? AppConstants.DefaultCodexHome();
        LoadSqliteHomeOverride(CurrentCodexHome());
        _backupRetentionInput.Value = Math.Max(1, _settings.BackupRetentionCount);
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {T("loadedSettings")}: {_settingsService.SettingsPath}");
        _loadingSettings = false;
        await RefreshStatusAsync();
    }

    private async Task RefreshStatusAsync()
    {
        string codexHome = CurrentCodexHome();
        await PersistSqliteHomeOverrideAsync();
        await RunBusyAsync(T("refreshing"), () => RefreshStatusCoreAsync(codexHome));
    }

    private async Task ChangeLanguageAsync()
    {
        if (_loadingSettings)
        {
            return;
        }

        _language = _languageCombo.SelectedIndex == 1 ? MacUiText.Chinese : MacUiText.English;
        _settings = _settingsService.UpdateUiLanguage(_settings, _language);
        await _settingsService.SaveAsync(_settings);
        ApplyLanguage();
        if (_currentStatus is not null)
        {
            _statusText.Text = MacDisplayFormatter.FormatStatus(_currentStatus, _language);
        }
        ReloadProviderList();
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {T("languageChanged")}: {(_language == MacUiText.Chinese ? "中文" : "English")}");
    }

    private async Task BrowseCodexHomeAsync()
    {
        IStorageFolder? start = null;
        string current = CurrentCodexHome();
        if (Directory.Exists(current))
        {
            start = await StorageProvider.TryGetFolderFromPathAsync(current);
        }

        IReadOnlyList<IStorageFolder> folders = await StorageProvider.OpenFolderPickerAsync(new FolderPickerOpenOptions
        {
            Title = T("chooseCodexFolder"),
            AllowMultiple = false,
            SuggestedStartLocation = start
        });

        if (folders.Count == 0 || folders[0].Path.LocalPath is not { Length: > 0 } selected)
        {
            return;
        }

        _codexHomeText.Text = selected;
        await PersistHomeSelectionAsync();
        await RefreshStatusAsync();
    }

    private async Task PersistHomeSelectionAsync()
    {
        string codexHome = CurrentCodexHome();
        if (string.IsNullOrWhiteSpace(codexHome))
        {
            return;
        }

        _settings = _settingsService.RecordCodexHome(_settings, codexHome);
        _settings = _settingsService.UpdateState(_settings, SelectedProvider(), _settings.LastBackupDirectory, CaptureWindowBounds(), CurrentBackupRetentionCount());
        await _settingsService.SaveAsync(_settings);
        ReloadRecentHomes();
        if (_sqliteOverrideCodexHome is null || !PathsEqual(_sqliteOverrideCodexHome, codexHome))
        {
            LoadSqliteHomeOverride(codexHome);
        }
    }

    private async Task BrowseSqliteHomeAsync()
    {
        string startPath = CurrentSqliteHomeOverride() ?? CurrentCodexHome();
        IStorageFolder? start = Directory.Exists(startPath)
            ? await StorageProvider.TryGetFolderFromPathAsync(startPath)
            : null;
        IReadOnlyList<IStorageFolder> folders = await StorageProvider.OpenFolderPickerAsync(
            new FolderPickerOpenOptions
            {
                Title = T("chooseSqliteFolder"),
                AllowMultiple = false,
                SuggestedStartLocation = start
            });
        if (folders.Count == 0 || folders[0].Path.LocalPath is not { Length: > 0 } selected)
        {
            return;
        }

        _sqliteHomeText.Text = selected;
        await PersistSqliteHomeOverrideAsync();
        await RefreshStatusAsync();
    }

    private async Task PersistSqliteHomeOverrideAsync()
    {
        if (_loadingSettings)
        {
            return;
        }
        _settings = _settingsService.RecordSqliteHomeOverride(
            _settings,
            CurrentCodexHome(),
            CurrentSqliteHomeOverride());
        await _settingsService.SaveAsync(_settings);
    }

    private void LoadSqliteHomeOverride(string codexHome)
    {
        _sqliteOverrideCodexHome = Path.GetFullPath(codexHome);
        _sqliteHomeText.Text = _settingsService.GetSqliteHomeOverride(_settings, codexHome) ?? string.Empty;
    }

    private async Task PersistBackupRetentionAsync()
    {
        if (_loadingSettings)
        {
            return;
        }

        _settings = _settingsService.UpdateState(
            _settings,
            SelectedProvider(),
            _settings.LastBackupDirectory,
            CaptureWindowBounds(),
            CurrentBackupRetentionCount());
        await _settingsService.SaveAsync(_settings);
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Backup retention set to {CurrentBackupRetentionCount()}");
    }

    private async Task AddManualProviderAsync()
    {
        string provider = (_manualProviderText.Text ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(provider))
        {
            await ShowInfoAsync(T("enterProvider"));
            return;
        }

        _settings = _settingsService.AddManualProvider(_settings, provider);
        await _settingsService.SaveAsync(_settings);
        _manualProviderText.Clear();
        ReloadProviderList();
        SelectProvider(provider);
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {T("addedManualProvider")}: {provider}");
    }

    private async Task RemoveManualProviderAsync()
    {
        string? provider = SelectedProvider();
        if (string.IsNullOrWhiteSpace(provider))
        {
            await ShowInfoAsync(T("selectProvider"));
            return;
        }

        _settings = _settingsService.RemoveManualProvider(_settings, provider);
        await _settingsService.SaveAsync(_settings);
        ReloadProviderList();
        SelectProvider(_currentStatus?.CurrentProvider.Provider);
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {T("removedManualProvider")}: {provider}");
    }

    private async Task ExecuteSyncOrSwitchAsync()
    {
        string? provider = SelectedProvider();
        if (string.IsNullOrWhiteSpace(provider))
        {
            await ShowInfoAsync(T("selectTargetProvider"));
            return;
        }

        string mode = IsSwitchMode()
            ? T("confirmSwitchMode")
            : T("confirmSyncMode");

        if (!await ConfirmAsync(
            T("confirmWriteTitle"),
            string.Format(T("confirmWriteMessage"), mode, provider)))
        {
            return;
        }

        await RunBusyAsync(T("executing"), async () =>
        {
            string codexHome = CurrentCodexHome();
            string? sqliteHome = CurrentSqliteHomeOverride();
            await PersistSqliteHomeOverrideAsync();
            int backupRetentionCount = CurrentBackupRetentionCount();
            SyncResult result = IsSwitchMode()
                ? await Task.Run(async () => await _syncService.RunSwitchAsync(
                    codexHome,
                    provider,
                    backupRetentionCount,
                    explicitSqliteHome: sqliteHome))
                : await Task.Run(async () => await _syncService.RunSyncAsync(
                    codexHome,
                    provider: provider,
                    keepCount: backupRetentionCount,
                    explicitSqliteHome: sqliteHome));

            _settings = _settingsService.UpdateState(_settings, provider, result.BackupDir, CaptureWindowBounds(), backupRetentionCount);
            await _settingsService.SaveAsync(_settings);
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {T("executionFinished")}");
            AppendLog(MacDisplayFormatter.FormatSyncResult(result, IsSwitchMode() ? T("switchedAndSynced") : T("synced"), _language));
            AppendLog(string.Empty);
            await RefreshStatusCoreAsync(codexHome);
            SelectProvider(provider);
        });
    }

    private async Task RestoreBackupAsync()
    {
        string backupRoot = _currentStatus?.BackupRoot ?? AppConstants.DefaultBackupRoot(CurrentCodexHome());
        string initialBackupDir = Directory.Exists(_settings.LastBackupDirectory)
            ? _settings.LastBackupDirectory!
            : backupRoot;

        IStorageFolder? start = Directory.Exists(initialBackupDir)
            ? await StorageProvider.TryGetFolderFromPathAsync(initialBackupDir)
            : null;

        IReadOnlyList<IStorageFolder> folders = await StorageProvider.OpenFolderPickerAsync(new FolderPickerOpenOptions
        {
            Title = T("chooseBackupFolder"),
            AllowMultiple = false,
            SuggestedStartLocation = start
        });

        if (folders.Count == 0 || folders[0].Path.LocalPath is not { Length: > 0 } backupDir)
        {
            return;
        }

        bool restoreConfig = _restoreConfigCheck.IsChecked == true;
        bool restoreDatabase = _restoreDatabaseCheck.IsChecked == true;
        bool restoreSessions = _restoreSessionsCheck.IsChecked == true;
        if (!restoreConfig && !restoreDatabase && !restoreSessions)
        {
            await ShowInfoAsync(T("chooseRestoreTarget"));
            return;
        }

        string restoreTargets = string.Join(", ", new[]
        {
            restoreConfig ? "config.toml" : null,
            restoreDatabase ? "SQLite" : null,
            restoreSessions ? "rollout metadata" : null
        }.Where(static value => value is not null));

        if (!await ConfirmAsync(
            T("restoreTitle"),
            string.Format(T("restoreMessage"), backupDir, restoreTargets)))
        {
            return;
        }

        bool allowSqliteHomeRelocation = false;
        string? sqliteHome = CurrentSqliteHomeOverride();
        try
        {
            if (restoreDatabase)
            {
                BackupStorageInfo backupStorage = await _syncService.GetBackupStorageInfoAsync(backupDir);
                StatusSnapshot targetStatus = await _syncService.GetStatusAsync(CurrentCodexHome(), sqliteHome);
                string targetSqliteHome = targetStatus.StateDbLocation is null
                    ? targetStatus.SqliteHome
                    : Path.GetDirectoryName(targetStatus.StateDbLocation.Path)!;
                if (backupStorage.Version >= 2
                    && !string.IsNullOrWhiteSpace(backupStorage.SqliteHome)
                    && !PathsEqual(backupStorage.SqliteHome, targetSqliteHome))
                {
                    if (!await ConfirmAsync(
                        T("relocationTitle"),
                        string.Format(T("relocationMessage"), backupStorage.SqliteHome, targetSqliteHome)))
                    {
                        return;
                    }
                    allowSqliteHomeRelocation = true;
                }
            }
        }
        catch (Exception error)
        {
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {T("error")}: {error}");
            await ShowErrorAsync(error);
            return;
        }

        await RunBusyAsync(T("restoring"), async () =>
        {
            string codexHome = CurrentCodexHome();
            RestoreResult result = await Task.Run(async () => await _syncService.RunRestoreAsync(
                codexHome,
                backupDir,
                new RestoreBackupOptions
                {
                    RestoreConfig = restoreConfig,
                    RestoreDatabase = restoreDatabase,
                    RestoreSessions = restoreSessions,
                    AllowSqliteHomeRelocation = allowSqliteHomeRelocation
                },
                sqliteHome));
            _settings = _settingsService.UpdateState(_settings, SelectedProvider(), backupDir, CaptureWindowBounds(), CurrentBackupRetentionCount());
            await _settingsService.SaveAsync(_settings);
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {T("restoreFinished")}");
            AppendLog(MacDisplayFormatter.FormatRestoreResult(result, _language));
            AppendLog(string.Empty);
            await RefreshStatusCoreAsync(codexHome);
        });
    }

    private async Task OpenBackupFolderAsync()
    {
        string path = _currentStatus?.BackupRoot ?? AppConstants.DefaultBackupRoot(CurrentCodexHome());
        Directory.CreateDirectory(path);
        try
        {
            Process.Start(new ProcessStartInfo
            {
                FileName = OperatingSystem.IsMacOS() ? "open" : path,
                Arguments = OperatingSystem.IsMacOS() ? QuoteShellArgument(path) : string.Empty,
                UseShellExecute = !OperatingSystem.IsMacOS()
            });
        }
        catch (Exception error)
        {
            await ShowErrorAsync(error);
        }
    }

    private async Task PruneBackupsAsync()
    {
        if (!await ConfirmAsync(
            T("cleanTitle"),
            string.Format(T("cleanMessage"), CurrentBackupRetentionCount())))
        {
            return;
        }

        await RunBusyAsync(T("cleaning"), async () =>
        {
            string codexHome = CurrentCodexHome();
            BackupPruneResult result = await Task.Run(async () => await _syncService.RunPruneBackupsAsync(codexHome, CurrentBackupRetentionCount()));
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {T("backupCleanupFinished")}");
            AppendLog(MacDisplayFormatter.FormatBackupPruneResult(result, _language));
            AppendLog(string.Empty);
            await RefreshStatusCoreAsync(codexHome);
        });
    }

    private async Task RefreshStatusCoreAsync(string codexHome)
    {
        _currentStatus = await Task.Run(async () => await _syncService.GetStatusAsync(
            codexHome,
            CurrentSqliteHomeOverride()));
        _settings = _settingsService.RecordCodexHome(_settings, _currentStatus.CodexHome);
        _settings = _settingsService.MergeDetectedProviders(_settings, _syncService.ExtractDetectedProviderIds(_currentStatus));
        _settings = _settingsService.UpdateState(_settings, SelectedProvider(), _settings.LastBackupDirectory, CaptureWindowBounds(), CurrentBackupRetentionCount());
        await _settingsService.SaveAsync(_settings);

        _statusText.Text = MacDisplayFormatter.FormatStatus(_currentStatus, _language);
        ReloadRecentHomes();
        ReloadProviderList();
        _codexHomeText.Text = _currentStatus.CodexHome;
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {T("refreshed")}: {_currentStatus.CodexHome}");
    }

    private void ReloadRecentHomes()
    {
        string? selected = _codexHomeText.Text;
        _recentHomes.Items.Clear();
        foreach (string home in _settings.RecentCodexHomes)
        {
            _recentHomes.Items.Add(home);
        }

        _codexHomeText.Text = selected;
    }

    private void ReloadProviderList()
    {
        _providerList.Items.Clear();
        if (_currentStatus is not null)
        {
            foreach (ProviderOption option in _syncService.BuildProviderOptions(_currentStatus, _settings))
            {
                ProviderListItem item = new()
                {
                    Option = option,
                    SourcesText = MacDisplayFormatter.FormatProviderSources(option, _language),
                    DetailText = ProviderDetailText(option)
                };
                _providerList.Items.Add(BuildProviderRow(item));
            }
        }

        SelectProvider(_settings.LastSelectedProvider ?? _currentStatus?.CurrentProvider.Provider);
        UpdateSelectionLabel();
    }

    private ListBoxItem BuildProviderRow(ProviderListItem provider)
    {
        Grid row = new()
        {
            ColumnDefinitions = new ColumnDefinitions("*,Auto"),
            RowDefinitions = new RowDefinitions("Auto,Auto"),
            Margin = new Thickness(8, 7)
        };

        TextBlock title = new()
        {
            Text = provider.Option.Id,
            FontWeight = provider.Option.IsCurrentProvider ? FontWeight.SemiBold : FontWeight.Normal,
            TextTrimming = TextTrimming.CharacterEllipsis
        };
        TextBlock sources = new()
        {
            Text = provider.SourcesText,
            Foreground = Brush.Parse("#52525b"),
            FontSize = 12,
            TextWrapping = TextWrapping.Wrap
        };
        TextBlock detail = new()
        {
            Text = provider.DetailText,
            Foreground = Brush.Parse("#71717a"),
            FontSize = 12
        };

        row.Children.Add(title);
        row.Children.Add(detail);
        row.Children.Add(sources);
        Grid.SetColumn(detail, 1);
        Grid.SetRow(sources, 1);
        Grid.SetColumnSpan(sources, 2);

        return new ListBoxItem
        {
            Tag = provider.Option.Id,
            Content = row
        };
    }

    private string ProviderDetailText(ProviderOption option)
    {
        List<string> details = [];
        if (option.IsCurrentProvider)
        {
            details.Add(T("current"));
        }
        if (option.IsManual)
        {
            details.Add(T("manual"));
        }
        if (option.IsSaved)
        {
            details.Add(T("saved"));
        }

        return details.Count == 0 ? string.Empty : string.Join(" · ", details);
    }

    private void SelectProvider(string? provider)
    {
        if (string.IsNullOrWhiteSpace(provider))
        {
            return;
        }

        foreach (object? item in _providerList.Items)
        {
            if (item is ListBoxItem row && string.Equals(row.Tag as string, provider, StringComparison.Ordinal))
            {
                _providerList.SelectedItem = row;
                break;
            }
        }
    }

    private void UpdateSelectionLabel()
    {
        string? provider = SelectedProvider();
        _selectedProviderText.Text = string.IsNullOrWhiteSpace(provider) ? T("noProvider") : provider;
    }

    private void UpdateExecuteButtonText()
    {
        _executeButton.Content = IsSwitchMode()
            ? T("switchSync")
            : T("syncMetadata");
    }

    private string? SelectedProvider()
    {
        return _providerList.SelectedItem is ListBoxItem row ? row.Tag as string : null;
    }

    private string CurrentCodexHome()
    {
        string text = (_codexHomeText.Text ?? string.Empty).Trim();
        return string.IsNullOrWhiteSpace(text) ? AppConstants.DefaultCodexHome() : text;
    }

    private string? CurrentSqliteHomeOverride()
    {
        string text = (_sqliteHomeText.Text ?? string.Empty).Trim();
        return string.IsNullOrWhiteSpace(text) ? null : text;
    }

    private int CurrentBackupRetentionCount()
    {
        decimal value = _backupRetentionInput.Value ?? AppConstants.DefaultBackupRetentionCount;
        return Decimal.ToInt32(Math.Max(1, value));
    }

    private bool IsSwitchMode()
    {
        return _switchConfigCheck.IsChecked == true;
    }

    private void PersistUiState()
    {
        try
        {
            _settings = _settingsService.RecordCodexHome(_settings, CurrentCodexHome());
            _settings = _settingsService.RecordSqliteHomeOverride(
                _settings,
                CurrentCodexHome(),
                CurrentSqliteHomeOverride());
            _settings = _settingsService.UpdateUiLanguage(_settings, _language);
            _settings = _settingsService.UpdateState(_settings, SelectedProvider(), _settings.LastBackupDirectory, CaptureWindowBounds(), CurrentBackupRetentionCount());
            _settingsService.Save(_settings);
        }
        catch
        {
            // Ignore shutdown persistence failures.
        }
    }

    private WindowBoundsState CaptureWindowBounds()
    {
        return new WindowBoundsState
        {
            X = Position.X,
            Y = Position.Y,
            Width = Decimal.ToInt32((decimal)Math.Max(Width, MinWidth)),
            Height = Decimal.ToInt32((decimal)Math.Max(Height, MinHeight)),
            Maximized = WindowState == WindowState.Maximized
        };
    }

    private void ApplyWindowBounds(WindowBoundsState? bounds)
    {
        if (bounds is null || bounds.Width < 800 || bounds.Height < 600)
        {
            return;
        }

        Position = new PixelPoint(bounds.X, bounds.Y);
        Width = bounds.Width;
        Height = bounds.Height;
        if (bounds.Maximized)
        {
            WindowState = WindowState.Maximized;
        }
    }

    private async Task RunBusyAsync(string stateText, Func<Task> action)
    {
        SetBusy(true, stateText);
        try
        {
            await action();
        }
        catch (Exception error)
        {
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {T("error")}: {error}");
            await ShowErrorAsync(error);
        }
        finally
        {
            SetBusy(false, T("ready"));
        }
    }

    private void SetBusy(bool busy, string stateText)
    {
        Cursor = busy ? new Cursor(StandardCursorType.Wait) : Cursor.Default;
        _busyText.Text = stateText;
        _busyText.Foreground = busy ? Brush.Parse("#b45309") : Brush.Parse("#166534");

        _browseButton.IsEnabled = !busy;
        _browseSqliteHomeButton.IsEnabled = !busy;
        _refreshButton.IsEnabled = !busy;
        _addProviderButton.IsEnabled = !busy;
        _removeProviderButton.IsEnabled = !busy;
        _switchConfigCheck.IsEnabled = !busy;
        _backupRetentionInput.IsEnabled = !busy;
        _restoreConfigCheck.IsEnabled = !busy;
        _restoreDatabaseCheck.IsEnabled = !busy;
        _restoreSessionsCheck.IsEnabled = !busy;
        _executeButton.IsEnabled = !busy;
        _restoreButton.IsEnabled = !busy;
        _openBackupButton.IsEnabled = !busy;
        _pruneBackupsButton.IsEnabled = !busy;
        _providerList.IsEnabled = !busy;
        _manualProviderText.IsEnabled = !busy;
        _codexHomeText.IsEnabled = !busy;
        _sqliteHomeText.IsEnabled = !busy;
        _recentHomes.IsEnabled = !busy;
        _languageCombo.IsEnabled = !busy;
    }

    private void AppendLog(string message)
    {
        _logText.Text = string.IsNullOrEmpty(_logText.Text)
            ? message
            : $"{_logText.Text}{Environment.NewLine}{message}";
        _logText.CaretIndex = _logText.Text?.Length ?? 0;
    }

    private async Task<bool> ConfirmAsync(string title, string message)
    {
        ConfirmationDialog dialog = new(title, message, T("continue"), T("cancel"));
        return await dialog.ShowDialog<bool>(this);
    }

    private async Task ShowInfoAsync(string message)
    {
        ConfirmationDialog dialog = new("Codex Provider Sync", message, T("ok"), null);
        await dialog.ShowDialog<bool>(this);
    }

    private async Task ShowErrorAsync(Exception error)
    {
        ConfirmationDialog dialog = new(T("error"), error.Message, T("ok"), null);
        await dialog.ShowDialog<bool>(this);
    }

    private void ApplyLanguage()
    {
        _languageLabel.Text = T("language");
        _sqliteHomeLabel.Text = T("sqliteHome");
        _sqliteHomeText.PlaceholderText = T("sqliteHomePlaceholder");
        _recentHomes.PlaceholderText = T("recent");
        _browseButton.Content = T("browse");
        _browseSqliteHomeButton.Content = T("browse");
        _refreshButton.Content = T("refresh");
        _statusTitle.Text = T("status");
        _providersTitle.Text = T("providers");
        _actionsTitle.Text = T("actions");
        _logTitle.Text = T("executionLog");
        _manualProviderText.PlaceholderText = T("addProvider");
        _addProviderButton.Content = T("add");
        _removeProviderButton.Content = T("removeManual");
        _switchConfigCheck.Content = T("switchConfig");
        _restoreConfigCheck.Content = T("restoreConfig");
        _restoreDatabaseCheck.Content = T("restoreSqlite");
        _restoreSessionsCheck.Content = T("restoreRollout");
        _restoreButton.Content = T("restoreBackup");
        _openBackupButton.Content = T("openBackupFolder");
        _pruneBackupsButton.Content = T("cleanBackups");
        _targetProviderLabel.Text = T("targetProvider");
        _restoreContentsLabel.Text = T("restoreContents");
        _keepBackupsLabel.Text = T("keepBackups");
        _writeWarningText.Text = T("writeWarning");
        _busyText.Text = T("ready");
        UpdateExecuteButtonText();
        UpdateSelectionLabel();
    }

    private string T(string key)
    {
        return MacUiText.Get(_language, key);
    }

    private static bool PathsEqual(string left, string right)
    {
        return string.Equals(
            Path.GetFullPath(left),
            Path.GetFullPath(right),
            OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
    }

    private static Border Card(Control content)
    {
        return new Border
        {
            Background = Brushes.White,
            BorderBrush = Brush.Parse("#d4d4d8"),
            BorderThickness = new Thickness(1),
            CornerRadius = new CornerRadius(8),
            Padding = new Thickness(12),
            Child = content
        };
    }

    private static Control Section(TextBlock title, Control content)
    {
        Grid panel = new()
        {
            RowDefinitions = new RowDefinitions("Auto,*"),
            RowSpacing = 8
        };
        ConfigureSectionTitle(title);
        panel.Children.Add(title);
        panel.Children.Add(content);
        Grid.SetRow(content, 1);
        return Card(panel);
    }

    private static void ConfigureLabel(TextBlock label, string text)
    {
        label.Text = text;
        label.VerticalAlignment = VerticalAlignment.Center;
        label.FontWeight = FontWeight.SemiBold;
        label.Foreground = Brush.Parse("#18181b");
    }

    private static void ConfigureSectionTitle(TextBlock title)
    {
        title.FontSize = 13;
        title.FontWeight = FontWeight.SemiBold;
        title.Foreground = Brush.Parse("#18181b");
    }

    private static void ConfigureMutedLabel(TextBlock label, string text)
    {
        label.Text = text;
        label.Foreground = Brush.Parse("#52525b");
        label.FontSize = 12;
    }

    private static Border WarningBlock(TextBlock text)
    {
        text.Foreground = Brush.Parse("#9a3412");
        text.TextWrapping = TextWrapping.Wrap;
        return new Border
        {
            Background = Brush.Parse("#fff7ed"),
            BorderBrush = Brush.Parse("#fed7aa"),
            BorderThickness = new Thickness(1),
            CornerRadius = new CornerRadius(8),
            Padding = new Thickness(10),
            Child = text
        };
    }

    private static Separator Separator()
    {
        return new Separator
        {
            Margin = new Thickness(0, 2)
        };
    }

    private static string QuoteShellArgument(string value)
    {
        return "\"" + value.Replace("\"", "\\\"", StringComparison.Ordinal) + "\"";
    }
}
