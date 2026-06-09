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
    private readonly ComboBox _recentHomes = new();
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

    private AppSettings _settings = new();
    private StatusSnapshot? _currentStatus;
    private bool _loadingSettings;

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
            ColumnDefinitions = new ColumnDefinitions("Auto,*,180,Auto,Auto"),
            ColumnSpacing = 10
        };

        TextBlock label = Label("Codex Home");
        _codexHomeText.Watermark = AppConstants.DefaultCodexHome();
        _codexHomeText.MinHeight = 34;
        _recentHomes.MinHeight = 34;
        _recentHomes.PlaceholderText = "Recent";
        _browseButton.Content = "Browse";
        _browseButton.MinHeight = 34;
        _refreshButton.Content = "Refresh";
        _refreshButton.MinHeight = 34;

        panel.Children.Add(label);
        panel.Children.Add(_codexHomeText);
        panel.Children.Add(_recentHomes);
        panel.Children.Add(_browseButton);
        panel.Children.Add(_refreshButton);
        Grid.SetColumn(_codexHomeText, 1);
        Grid.SetColumn(_recentHomes, 2);
        Grid.SetColumn(_browseButton, 3);
        Grid.SetColumn(_refreshButton, 4);
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

        return Section("Status", _statusText);
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
        _manualProviderText.Watermark = "Add provider id";
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

        return Section("Providers", panel);
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

        panel.Children.Add(MutedLabel("Target provider"));
        panel.Children.Add(_selectedProviderText);
        panel.Children.Add(_switchConfigCheck);
        panel.Children.Add(BuildRetentionPanel());
        panel.Children.Add(WarningBlock("Close Codex CLI, Codex App, app-server, and related terminals before executing write actions."));
        panel.Children.Add(_executeButton);
        panel.Children.Add(Separator());
        panel.Children.Add(MutedLabel("Restore contents"));
        panel.Children.Add(_restoreConfigCheck);
        panel.Children.Add(_restoreDatabaseCheck);
        panel.Children.Add(_restoreSessionsCheck);
        panel.Children.Add(_restoreButton);
        panel.Children.Add(_openBackupButton);
        panel.Children.Add(_pruneBackupsButton);
        panel.Children.Add(_busyText);

        return Section("Actions", panel);
    }

    private Control BuildRetentionPanel()
    {
        StackPanel panel = new()
        {
            Orientation = Orientation.Horizontal,
            Spacing = 8,
            VerticalAlignment = VerticalAlignment.Center
        };
        panel.Children.Add(MutedLabel("Keep backups"));
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
        return Section("Execution Log", _logText);
    }

    private void WireEvents()
    {
        _browseButton.Click += async (_, _) => await BrowseCodexHomeAsync();
        _refreshButton.Click += async (_, _) => await RefreshStatusAsync();
        _recentHomes.SelectionChanged += (_, _) =>
        {
            if (_recentHomes.SelectedItem is string home)
            {
                _codexHomeText.Text = home;
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
        ApplyWindowBounds(_settings.WindowBounds);
        ReloadRecentHomes();
        _codexHomeText.Text = _settings.LastCodexHome ?? AppConstants.DefaultCodexHome();
        _backupRetentionInput.Value = Math.Max(1, _settings.BackupRetentionCount);
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Loaded settings: {_settingsService.SettingsPath}");
        _loadingSettings = false;
        await RefreshStatusAsync();
    }

    private async Task RefreshStatusAsync()
    {
        string codexHome = CurrentCodexHome();
        await RunBusyAsync("Refreshing...", () => RefreshStatusCoreAsync(codexHome));
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
            Title = "Choose .codex folder",
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
            await ShowInfoAsync("Enter a provider id before adding it.");
            return;
        }

        _settings = _settingsService.AddManualProvider(_settings, provider);
        await _settingsService.SaveAsync(_settings);
        _manualProviderText.Clear();
        ReloadProviderList();
        SelectProvider(provider);
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Added manual provider: {provider}");
    }

    private async Task RemoveManualProviderAsync()
    {
        string? provider = SelectedProvider();
        if (string.IsNullOrWhiteSpace(provider))
        {
            await ShowInfoAsync("Select a provider first.");
            return;
        }

        _settings = _settingsService.RemoveManualProvider(_settings, provider);
        await _settingsService.SaveAsync(_settings);
        ReloadProviderList();
        SelectProvider(_currentStatus?.CurrentProvider.Provider);
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Removed manual provider: {provider}");
    }

    private async Task ExecuteSyncOrSwitchAsync()
    {
        string? provider = SelectedProvider();
        if (string.IsNullOrWhiteSpace(provider))
        {
            await ShowInfoAsync("Select a target provider first.");
            return;
        }

        string mode = IsSwitchMode()
            ? "switch config.toml and sync metadata"
            : "sync metadata only";

        if (!await ConfirmAsync(
            "Confirm Write Action",
            $"This will {mode} for provider \"{provider}\".\n\nClose Codex CLI, Codex App, app-server, and related terminals first.\n\nA backup will be created before metadata is changed. Continue?"))
        {
            return;
        }

        await RunBusyAsync("Executing...", async () =>
        {
            string codexHome = CurrentCodexHome();
            int backupRetentionCount = CurrentBackupRetentionCount();
            SyncResult result = IsSwitchMode()
                ? await Task.Run(async () => await _syncService.RunSwitchAsync(codexHome, provider, backupRetentionCount))
                : await Task.Run(async () => await _syncService.RunSyncAsync(codexHome, provider: provider, keepCount: backupRetentionCount));

            _settings = _settingsService.UpdateState(_settings, provider, result.BackupDir, CaptureWindowBounds(), backupRetentionCount);
            await _settingsService.SaveAsync(_settings);
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Execution finished");
            AppendLog(TextFormatter.FormatSyncResult(result, IsSwitchMode() ? "Switched and synced" : "Synced"));
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
            Title = "Choose backup folder",
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
            await ShowInfoAsync("Choose at least one restore target.");
            return;
        }

        string restoreTargets = string.Join(", ", new[]
        {
            restoreConfig ? "config.toml" : null,
            restoreDatabase ? "SQLite" : null,
            restoreSessions ? "rollout metadata" : null
        }.Where(static value => value is not null));

        if (!await ConfirmAsync(
            "Restore Backup",
            $"Restore this backup?\n\n{backupDir}\n\nThis will overwrite: {restoreTargets}.\nClose Codex first. Continue?"))
        {
            return;
        }

        await RunBusyAsync("Restoring...", async () =>
        {
            string codexHome = CurrentCodexHome();
            RestoreResult result = await Task.Run(async () => await _syncService.RunRestoreAsync(
                codexHome,
                backupDir,
                new RestoreBackupOptions
                {
                    RestoreConfig = restoreConfig,
                    RestoreDatabase = restoreDatabase,
                    RestoreSessions = restoreSessions
                }));
            _settings = _settingsService.UpdateState(_settings, SelectedProvider(), backupDir, CaptureWindowBounds(), CurrentBackupRetentionCount());
            await _settingsService.SaveAsync(_settings);
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Restore finished");
            AppendLog(TextFormatter.FormatRestoreResult(result));
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
            "Clean Old Backups",
            $"Keep only the newest {CurrentBackupRetentionCount()} managed backup(s).\n\nDeleted backup folders cannot be restored from this app. Continue?"))
        {
            return;
        }

        await RunBusyAsync("Cleaning backups...", async () =>
        {
            string codexHome = CurrentCodexHome();
            BackupPruneResult result = await Task.Run(async () => await _syncService.RunPruneBackupsAsync(codexHome, CurrentBackupRetentionCount()));
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Backup cleanup finished");
            AppendLog(TextFormatter.FormatBackupPruneResult(result));
            AppendLog(string.Empty);
            await RefreshStatusCoreAsync(codexHome);
        });
    }

    private async Task RefreshStatusCoreAsync(string codexHome)
    {
        _currentStatus = await Task.Run(async () => await _syncService.GetStatusAsync(codexHome));
        _settings = _settingsService.RecordCodexHome(_settings, _currentStatus.CodexHome);
        _settings = _settingsService.MergeDetectedProviders(_settings, _syncService.ExtractDetectedProviderIds(_currentStatus));
        _settings = _settingsService.UpdateState(_settings, SelectedProvider(), _settings.LastBackupDirectory, CaptureWindowBounds(), CurrentBackupRetentionCount());
        await _settingsService.SaveAsync(_settings);

        _statusText.Text = TextFormatter.FormatStatus(_currentStatus);
        ReloadRecentHomes();
        ReloadProviderList();
        _codexHomeText.Text = _currentStatus.CodexHome;
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Refreshed: {_currentStatus.CodexHome}");
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
                    SourcesText = TextFormatter.FormatProviderSources(option),
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

    private static string ProviderDetailText(ProviderOption option)
    {
        List<string> details = [];
        if (option.IsCurrentProvider)
        {
            details.Add("current");
        }
        if (option.IsManual)
        {
            details.Add("manual");
        }
        if (option.IsSaved)
        {
            details.Add("saved");
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
        _selectedProviderText.Text = string.IsNullOrWhiteSpace(provider) ? "No provider selected" : provider;
    }

    private void UpdateExecuteButtonText()
    {
        _executeButton.Content = IsSwitchMode()
            ? "Switch Config + Sync"
            : "Sync Metadata Only";
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
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Error: {error}");
            await ShowErrorAsync(error);
        }
        finally
        {
            SetBusy(false, "Ready");
        }
    }

    private void SetBusy(bool busy, string stateText)
    {
        Cursor = busy ? new Cursor(StandardCursorType.Wait) : Cursor.Default;
        _busyText.Text = stateText;
        _busyText.Foreground = busy ? Brush.Parse("#b45309") : Brush.Parse("#166534");

        _browseButton.IsEnabled = !busy;
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
        _recentHomes.IsEnabled = !busy;
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
        ConfirmationDialog dialog = new(title, message, "Continue", "Cancel");
        return await dialog.ShowDialog<bool>(this);
    }

    private async Task ShowInfoAsync(string message)
    {
        ConfirmationDialog dialog = new("Codex Provider Sync", message, "OK", null);
        await dialog.ShowDialog<bool>(this);
    }

    private async Task ShowErrorAsync(Exception error)
    {
        ConfirmationDialog dialog = new("Error", error.Message, "OK", null);
        await dialog.ShowDialog<bool>(this);
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

    private static Control Section(string title, Control content)
    {
        Grid panel = new()
        {
            RowDefinitions = new RowDefinitions("Auto,*"),
            RowSpacing = 8
        };
        panel.Children.Add(new TextBlock
        {
            Text = title,
            FontSize = 13,
            FontWeight = FontWeight.SemiBold,
            Foreground = Brush.Parse("#18181b")
        });
        panel.Children.Add(content);
        Grid.SetRow(content, 1);
        return Card(panel);
    }

    private static TextBlock Label(string text)
    {
        return new TextBlock
        {
            Text = text,
            VerticalAlignment = VerticalAlignment.Center,
            FontWeight = FontWeight.SemiBold,
            Foreground = Brush.Parse("#18181b")
        };
    }

    private static TextBlock MutedLabel(string text)
    {
        return new TextBlock
        {
            Text = text,
            Foreground = Brush.Parse("#52525b"),
            FontSize = 12
        };
    }

    private static Border WarningBlock(string text)
    {
        return new Border
        {
            Background = Brush.Parse("#fff7ed"),
            BorderBrush = Brush.Parse("#fed7aa"),
            BorderThickness = new Thickness(1),
            CornerRadius = new CornerRadius(8),
            Padding = new Thickness(10),
            Child = new TextBlock
            {
                Text = text,
                Foreground = Brush.Parse("#9a3412"),
                TextWrapping = TextWrapping.Wrap
            }
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
