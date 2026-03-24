using System.Diagnostics;
using CodexProviderSync.Core;

namespace CodexProviderSync.App;

public sealed class MainForm : Form
{
    private readonly CodexSyncService _syncService = new();
    private readonly SettingsService _settingsService = new();

    private readonly ComboBox _codexHomeCombo = new() { Dock = DockStyle.Fill, DropDownStyle = ComboBoxStyle.DropDown };
    private readonly Button _browseButton = new() { Text = "Browse...", AutoSize = true };
    private readonly Button _refreshButton = new() { Text = "Refresh", AutoSize = true };
    private readonly RichTextBox _statusBox = new()
    {
        Dock = DockStyle.Fill,
        ReadOnly = true,
        WordWrap = false,
        Font = new Font("Consolas", 10F),
        BackColor = SystemColors.Window
    };
    private readonly ListView _providerList = new()
    {
        Dock = DockStyle.Fill,
        View = View.Details,
        FullRowSelect = true,
        HideSelection = false,
        MultiSelect = false
    };
    private readonly TextBox _manualProviderText = new() { Dock = DockStyle.Fill };
    private readonly Button _addProviderButton = new() { Text = "添加", AutoSize = true };
    private readonly Button _removeProviderButton = new() { Text = "删除手动项", AutoSize = true };
    private readonly Label _selectedProviderValue = new()
    {
        AutoSize = false,
        Dock = DockStyle.Fill,
        Height = 32,
        Font = new Font(SystemFonts.DefaultFont, FontStyle.Bold),
        TextAlign = ContentAlignment.MiddleLeft
    };
    private readonly CheckBox _updateConfigCheck = new()
    {
        AutoSize = true,
        Margin = new Padding(0, 4, 8, 0)
    };
    private readonly Label _updateConfigLabel = new()
    {
        AutoSize = false,
        Dock = DockStyle.Fill,
        TextAlign = ContentAlignment.MiddleLeft,
        Text = "同步时改写 config.toml"
    };
    private readonly NumericUpDown _backupRetentionInput = new()
    {
        Minimum = 1,
        Maximum = 100000,
        Value = AppConstants.DefaultBackupRetentionCount,
        Width = 72
    };
    private readonly Button _executeButton = new() { Text = "立即同步", Dock = DockStyle.Fill, Height = 40 };
    private readonly Button _restoreButton = new() { Text = "恢复备份", Dock = DockStyle.Fill, Height = 40 };
    private readonly Button _openBackupButton = new() { Text = "打开备份目录", Dock = DockStyle.Fill, Height = 40 };
    private readonly Button _pruneBackupsButton = new() { Text = "清理旧备份", Dock = DockStyle.Fill, Height = 40 };
    private readonly Label _busyLabel = new() { AutoSize = true, ForeColor = Color.DarkGreen, Text = "Ready" };
    private readonly Label _warningLine1 = new()
    {
        AutoSize = false,
        Dock = DockStyle.Fill,
        BackColor = Color.FromArgb(255, 244, 214),
        ForeColor = Color.FromArgb(120, 53, 15),
        TextAlign = ContentAlignment.MiddleLeft,
        Text = "执行前先关闭 Codex CLI / App"
    };
    private readonly Label _warningLine2 = new()
    {
        AutoSize = false,
        Dock = DockStyle.Fill,
        BackColor = Color.FromArgb(255, 244, 214),
        ForeColor = Color.FromArgb(120, 53, 15),
        TextAlign = ContentAlignment.MiddleLeft,
        Text = "以及 app-server / 相关终端"
    };
    private readonly TextBox _logBox = new()
    {
        Dock = DockStyle.Fill,
        ReadOnly = true,
        Multiline = true,
        ScrollBars = ScrollBars.Both,
        Font = new Font("Consolas", 10F),
        BackColor = SystemColors.Window
    };

    private AppSettings _settings = new();
    private StatusSnapshot? _currentStatus;
    private bool _loadingSettings;

    public MainForm()
    {
        Text = "Codex Provider Sync";
        MinimumSize = new Size(1180, 760);
        StartPosition = FormStartPosition.CenterScreen;

        _providerList.Columns.Add("Provider", 180);
        _providerList.Columns.Add("来源", 180);
        _providerList.Columns.Add("当前", 70);
        _providerList.Columns.Add("手动", 70);
        _providerList.Columns.Add("已保存", 70);

        BuildLayout();
        WireEvents();
    }

    protected override async void OnLoad(EventArgs e)
    {
        base.OnLoad(e);
        await LoadStateAsync();
    }

    protected override void OnFormClosing(FormClosingEventArgs e)
    {
        PersistUiState();
        base.OnFormClosing(e);
    }

    private void BuildLayout()
    {
        TableLayoutPanel root = new()
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = 3,
            Padding = new Padding(12)
        };
        root.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        root.RowStyles.Add(new RowStyle(SizeType.Percent, 68));
        root.RowStyles.Add(new RowStyle(SizeType.Percent, 32));

        root.Controls.Add(BuildTopPanel(), 0, 0);
        root.Controls.Add(BuildMainPanel(), 0, 1);
        root.Controls.Add(BuildLogPanel(), 0, 2);

        Controls.Add(root);
    }

    private Control BuildTopPanel()
    {
        GroupBox group = new()
        {
            Text = "Codex Home",
            Dock = DockStyle.Top,
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink
        };

        TableLayoutPanel panel = new()
        {
            Dock = DockStyle.Top,
            ColumnCount = 4,
            AutoSize = true,
            Padding = new Padding(10)
        };
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));

        Label recentLabel = new()
        {
            Text = "最近使用会自动保留在下拉列表中",
            AutoSize = true,
            ForeColor = SystemColors.GrayText,
            Anchor = AnchorStyles.Left
        };

        panel.Controls.Add(_codexHomeCombo, 0, 0);
        panel.Controls.Add(_browseButton, 1, 0);
        panel.Controls.Add(_refreshButton, 2, 0);
        panel.Controls.Add(recentLabel, 3, 0);
        group.Controls.Add(panel);
        return group;
    }

    private Control BuildMainPanel()
    {
        TableLayoutPanel main = new()
        {
            Dock = DockStyle.Fill,
            ColumnCount = 3,
            RowCount = 1,
            Margin = new Padding(0, 12, 0, 12)
        };
        main.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 50));
        main.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 50));
        main.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 380));

        main.Controls.Add(BuildStatusGroup(), 0, 0);
        main.Controls.Add(BuildProviderGroup(), 1, 0);
        main.Controls.Add(BuildActionGroup(), 2, 0);
        return main;
    }

    private Control BuildStatusGroup()
    {
        GroupBox group = new() { Text = "当前状态", Dock = DockStyle.Fill };
        group.Controls.Add(_statusBox);
        return group;
    }

    private Control BuildProviderGroup()
    {
        GroupBox group = new() { Text = "Provider 列表", Dock = DockStyle.Fill };
        TableLayoutPanel panel = new()
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = 3,
            Padding = new Padding(8)
        };
        panel.RowStyles.Add(new RowStyle(SizeType.Percent, 100));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));

        FlowLayoutPanel addPanel = new()
        {
            Dock = DockStyle.Fill,
            FlowDirection = FlowDirection.LeftToRight,
            AutoSize = true,
            WrapContents = false
        };
        addPanel.Controls.Add(new Label { Text = "手动添加:", AutoSize = true, Margin = new Padding(0, 8, 8, 0) });
        _manualProviderText.Width = 220;
        addPanel.Controls.Add(_manualProviderText);
        addPanel.Controls.Add(_addProviderButton);

        FlowLayoutPanel hintPanel = new()
        {
            Dock = DockStyle.Fill,
            FlowDirection = FlowDirection.LeftToRight,
            AutoSize = true,
            WrapContents = false
        };
        hintPanel.Controls.Add(_removeProviderButton);
        hintPanel.Controls.Add(new Label
        {
            Text = "Refresh 时会把扫描到的新 Provider 自动并入持久化列表",
            AutoSize = true,
            Margin = new Padding(12, 8, 0, 0),
            ForeColor = SystemColors.GrayText
        });

        panel.Controls.Add(_providerList, 0, 0);
        panel.Controls.Add(addPanel, 0, 1);
        panel.Controls.Add(hintPanel, 0, 2);
        group.Controls.Add(panel);
        return group;
    }

    private Control BuildActionGroup()
    {
        GroupBox group = new() { Text = "执行", Dock = DockStyle.Fill, MinimumSize = new Size(380, 0) };
        TableLayoutPanel panel = new()
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = 10,
            Padding = new Padding(12)
        };
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 32));
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 52));
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 92));
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 54));
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 44));
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 44));
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 44));
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 44));
        panel.RowStyles.Add(new RowStyle(SizeType.Percent, 100));

        panel.Controls.Add(new Label { Text = "目标 Provider", AutoSize = true }, 0, 0);
        panel.Controls.Add(_selectedProviderValue, 0, 1);
        panel.Controls.Add(BuildUpdateConfigPanel(), 0, 2);
        panel.Controls.Add(BuildWarningPanel(), 0, 3);
        panel.Controls.Add(BuildBackupRetentionPanel(), 0, 4);
        panel.Controls.Add(_executeButton, 0, 5);
        panel.Controls.Add(_restoreButton, 0, 6);
        panel.Controls.Add(_openBackupButton, 0, 7);
        panel.Controls.Add(_pruneBackupsButton, 0, 8);
        panel.Controls.Add(_busyLabel, 0, 9);

        group.Controls.Add(panel);
        return group;
    }

    private Control BuildUpdateConfigPanel()
    {
        TableLayoutPanel panel = new()
        {
            Dock = DockStyle.Fill,
            ColumnCount = 2,
            Margin = new Padding(0, 2, 0, 8)
        };
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        panel.Controls.Add(_updateConfigCheck, 0, 0);
        panel.Controls.Add(_updateConfigLabel, 1, 0);
        return panel;
    }

    private Control BuildWarningPanel()
    {
        Panel panel = new()
        {
            Dock = DockStyle.Fill,
            Height = 86,
            Padding = new Padding(10, 8, 10, 8),
            Margin = new Padding(0, 4, 0, 8),
            BackColor = Color.FromArgb(255, 244, 214)
        };
        TableLayoutPanel textLayout = new()
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = 2,
            BackColor = Color.FromArgb(255, 244, 214),
            Margin = new Padding(0),
            Padding = new Padding(0)
        };
        textLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 50));
        textLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 50));
        textLayout.Controls.Add(_warningLine1, 0, 0);
        textLayout.Controls.Add(_warningLine2, 0, 1);
        panel.Controls.Add(textLayout);
        return panel;
    }

    private Control BuildBackupRetentionPanel()
    {
        TableLayoutPanel panel = new()
        {
            Dock = DockStyle.Fill,
            ColumnCount = 3,
            Margin = new Padding(0, 2, 0, 8)
        };
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));

        panel.Controls.Add(new Label
        {
            Text = "自动保留最近",
            AutoSize = true,
            Anchor = AnchorStyles.Left,
            Margin = new Padding(0, 8, 8, 0)
        }, 0, 0);
        panel.Controls.Add(_backupRetentionInput, 1, 0);
        panel.Controls.Add(new Label
        {
            Text = "份备份",
            AutoSize = true,
            Anchor = AnchorStyles.Left,
            Margin = new Padding(8, 8, 0, 0)
        }, 2, 0);

        return panel;
    }

    private Control BuildLogPanel()
    {
        GroupBox group = new() { Text = "执行日志", Dock = DockStyle.Fill };
        group.Controls.Add(_logBox);
        return group;
    }

    private void WireEvents()
    {
        _browseButton.Click += async (_, _) => await BrowseCodexHomeAsync();
        _refreshButton.Click += async (_, _) => await RefreshStatusAsync();
        _addProviderButton.Click += async (_, _) => await AddManualProviderAsync();
        _removeProviderButton.Click += async (_, _) => await RemoveManualProviderAsync();
        _backupRetentionInput.ValueChanged += async (_, _) => await PersistBackupRetentionAsync();
        _executeButton.Click += async (_, _) => await ExecuteSyncOrSwitchAsync();
        _restoreButton.Click += async (_, _) => await RestoreBackupAsync();
        _openBackupButton.Click += (_, _) => OpenBackupFolder();
        _pruneBackupsButton.Click += async (_, _) => await PruneBackupsAsync();
        _providerList.SelectedIndexChanged += (_, _) => UpdateSelectionLabel();
        _codexHomeCombo.Leave += async (_, _) => await PersistHomeSelectionAsync();
    }

    private async Task LoadStateAsync()
    {
        _loadingSettings = true;
        _settings = await _settingsService.LoadAsync();
        ApplyWindowBounds(_settings.WindowBounds);
        ReloadRecentHomes();
        _codexHomeCombo.Text = _settings.LastCodexHome ?? AppConstants.DefaultCodexHome();
        _backupRetentionInput.Value = Math.Max(_backupRetentionInput.Minimum, Math.Min(_backupRetentionInput.Maximum, _settings.BackupRetentionCount));
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 已加载设置: {_settingsService.SettingsPath}");
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
        using FolderBrowserDialog dialog = new()
        {
            Description = "选择 .codex 目录",
            UseDescriptionForTitle = true,
            InitialDirectory = Directory.Exists(CurrentCodexHome()) ? CurrentCodexHome() : AppConstants.DefaultCodexHome(),
            ShowNewFolderButton = false
        };

        if (dialog.ShowDialog(this) != DialogResult.OK)
        {
            return;
        }

        _codexHomeCombo.Text = dialog.SelectedPath;
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
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 已更新自动保留数: {CurrentBackupRetentionCount()}");
    }

    private async Task AddManualProviderAsync()
    {
        string provider = _manualProviderText.Text.Trim();
        if (string.IsNullOrWhiteSpace(provider))
        {
            MessageBox.Show(this, "请输入要添加的 Provider ID。", Text, MessageBoxButtons.OK, MessageBoxIcon.Information);
            return;
        }

        _settings = _settingsService.AddManualProvider(_settings, provider);
        await _settingsService.SaveAsync(_settings);
        _manualProviderText.Clear();
        ReloadProviderList();
        SelectProvider(provider);
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 已添加手动 Provider: {provider}");
    }

    private async Task RemoveManualProviderAsync()
    {
        string? provider = SelectedProvider();
        if (string.IsNullOrWhiteSpace(provider))
        {
            MessageBox.Show(this, "请先选择要删除的 Provider。", Text, MessageBoxButtons.OK, MessageBoxIcon.Information);
            return;
        }

        _settings = _settingsService.RemoveManualProvider(_settings, provider);
        await _settingsService.SaveAsync(_settings);
        ReloadProviderList();
        SelectProvider(_currentStatus?.CurrentProvider.Provider);
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 已删除手动 Provider: {provider}");
    }

    private async Task ExecuteSyncOrSwitchAsync()
    {
        string? provider = SelectedProvider();
        if (string.IsNullOrWhiteSpace(provider))
        {
            MessageBox.Show(this, "请先选择目标 Provider。", Text, MessageBoxButtons.OK, MessageBoxIcon.Information);
            return;
        }

        if (!ConfirmCodexClosed("执行同步前，请先关闭已打开的 Codex CLI、Codex App、app-server 和相关终端。是否继续？"))
        {
            return;
        }

        await RunBusyAsync("Executing...", async () =>
        {
            string codexHome = CurrentCodexHome();
            int backupRetentionCount = CurrentBackupRetentionCount();
            SyncResult result = _updateConfigCheck.Checked
                ? await Task.Run(async () => await _syncService.RunSwitchAsync(codexHome, provider, backupRetentionCount))
                : await Task.Run(async () => await _syncService.RunSyncAsync(codexHome, provider: provider, keepCount: backupRetentionCount));

            _settings = _settingsService.UpdateState(_settings, provider, result.BackupDir, CaptureWindowBounds(), backupRetentionCount);
            await _settingsService.SaveAsync(_settings);
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 执行完成");
            AppendLog(TextFormatter.FormatSyncResult(result, _updateConfigCheck.Checked ? "已切换并同步" : "已同步"));
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

        using FolderBrowserDialog dialog = new()
        {
            Description = "选择要恢复的 backup 目录",
            UseDescriptionForTitle = true,
            InitialDirectory = initialBackupDir,
            ShowNewFolderButton = false
        };

        if (dialog.ShowDialog(this) != DialogResult.OK)
        {
            return;
        }

        DialogResult confirm = MessageBox.Show(
            this,
            $"确认恢复以下备份？{Environment.NewLine}{Environment.NewLine}{dialog.SelectedPath}{Environment.NewLine}{Environment.NewLine}这会覆盖当前的 config、SQLite 和 rollout session_meta。",
            Text,
            MessageBoxButtons.OKCancel,
            MessageBoxIcon.Warning);
        if (confirm != DialogResult.OK)
        {
            return;
        }

        if (!ConfirmCodexClosed("恢复 backup 前，请先关闭已打开的 Codex CLI、Codex App、app-server 和相关终端。是否继续？"))
        {
            return;
        }

        await RunBusyAsync("Restoring...", async () =>
        {
            string codexHome = CurrentCodexHome();
            RestoreResult result = await Task.Run(async () => await _syncService.RunRestoreAsync(codexHome, dialog.SelectedPath));
            _settings = _settingsService.UpdateState(_settings, SelectedProvider(), dialog.SelectedPath, CaptureWindowBounds(), CurrentBackupRetentionCount());
            await _settingsService.SaveAsync(_settings);
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 恢复完成");
            AppendLog(TextFormatter.FormatRestoreResult(result));
            AppendLog(string.Empty);
            await RefreshStatusCoreAsync(codexHome);
        });
    }

    private void OpenBackupFolder()
    {
        string path = _currentStatus?.BackupRoot ?? AppConstants.DefaultBackupRoot(CurrentCodexHome());
        Directory.CreateDirectory(path);
        Process.Start(new ProcessStartInfo
        {
            FileName = path,
            UseShellExecute = true
        });
    }

    private async Task PruneBackupsAsync()
    {
        if (!ConfirmBackupPrune())
        {
            return;
        }

        await RunBusyAsync("Cleaning backups...", async () =>
        {
            string codexHome = CurrentCodexHome();
            BackupPruneResult result = await Task.Run(async () => await _syncService.RunPruneBackupsAsync(codexHome, CurrentBackupRetentionCount()));
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 旧备份清理完成");
            AppendLog(TextFormatter.FormatBackupPruneResult(result));
            AppendLog(string.Empty);
            await RefreshStatusCoreAsync(codexHome);
        });
    }

    private void ReloadRecentHomes()
    {
        string currentText = _codexHomeCombo.Text;
        _codexHomeCombo.BeginUpdate();
        _codexHomeCombo.Items.Clear();
        foreach (string home in _settings.RecentCodexHomes)
        {
            _codexHomeCombo.Items.Add(home);
        }
        _codexHomeCombo.EndUpdate();
        if (!string.IsNullOrWhiteSpace(currentText))
        {
            _codexHomeCombo.Text = currentText;
        }
    }

    private void ReloadProviderList()
    {
        _providerList.BeginUpdate();
        _providerList.Items.Clear();

        if (_currentStatus is not null)
        {
            foreach (ProviderOption option in _syncService.BuildProviderOptions(_currentStatus, _settings))
            {
                ListViewItem item = new(option.Id)
                {
                    Tag = option.Id
                };
                item.SubItems.Add(TextFormatter.FormatProviderSources(option));
                item.SubItems.Add(option.IsCurrentProvider ? "Yes" : string.Empty);
                item.SubItems.Add(option.IsManual ? "Yes" : string.Empty);
                item.SubItems.Add(option.IsSaved ? "Yes" : string.Empty);
                _providerList.Items.Add(item);
            }
        }

        _providerList.EndUpdate();
        SelectProvider(_settings.LastSelectedProvider ?? _currentStatus?.CurrentProvider.Provider);
        UpdateSelectionLabel();
    }

    private void SelectProvider(string? provider)
    {
        if (string.IsNullOrWhiteSpace(provider))
        {
            return;
        }

        _providerList.SelectedItems.Clear();
        foreach (ListViewItem item in _providerList.Items)
        {
            if (string.Equals(item.Tag as string, provider, StringComparison.Ordinal))
            {
                item.Selected = true;
                item.Focused = true;
                item.EnsureVisible();
                break;
            }
        }
    }

    private void UpdateSelectionLabel()
    {
        string? provider = SelectedProvider();
        _selectedProviderValue.Text = string.IsNullOrWhiteSpace(provider) ? "未选择" : provider;
    }

    private string? SelectedProvider()
    {
        return _providerList.SelectedItems.Count == 0 ? null : _providerList.SelectedItems[0].Tag as string;
    }

    private string CurrentCodexHome()
    {
        string text = _codexHomeCombo.Text.Trim();
        return string.IsNullOrWhiteSpace(text) ? AppConstants.DefaultCodexHome() : text;
    }

    private int CurrentBackupRetentionCount()
    {
        return Decimal.ToInt32(_backupRetentionInput.Value);
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

    private async Task RefreshStatusCoreAsync(string codexHome)
    {
        _currentStatus = await Task.Run(async () => await _syncService.GetStatusAsync(codexHome));
        _settings = _settingsService.RecordCodexHome(_settings, _currentStatus.CodexHome);
        _settings = _settingsService.MergeDetectedProviders(_settings, _syncService.ExtractDetectedProviderIds(_currentStatus));
        _settings = _settingsService.UpdateState(_settings, SelectedProvider(), _settings.LastBackupDirectory, CaptureWindowBounds(), CurrentBackupRetentionCount());
        await _settingsService.SaveAsync(_settings);

        _statusBox.Text = TextFormatter.FormatStatus(_currentStatus);
        ReloadRecentHomes();
        ReloadProviderList();
        _codexHomeCombo.Text = _currentStatus.CodexHome;
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 已刷新: {_currentStatus.CodexHome}");
    }

    private WindowBoundsState CaptureWindowBounds()
    {
        Rectangle bounds = WindowState == FormWindowState.Normal ? Bounds : RestoreBounds;
        return new WindowBoundsState
        {
            X = bounds.X,
            Y = bounds.Y,
            Width = bounds.Width,
            Height = bounds.Height,
            Maximized = WindowState == FormWindowState.Maximized
        };
    }

    private void ApplyWindowBounds(WindowBoundsState? bounds)
    {
        if (bounds is null || bounds.Width < 800 || bounds.Height < 600)
        {
            Size = new Size(1280, 820);
            return;
        }

        StartPosition = FormStartPosition.Manual;
        Bounds = new Rectangle(bounds.X, bounds.Y, bounds.Width, bounds.Height);
        if (bounds.Maximized)
        {
            WindowState = FormWindowState.Maximized;
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
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 错误: {error}");
            MessageBox.Show(this, error.Message, Text, MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
        finally
        {
            SetBusy(false, "Ready");
        }
    }

    private void SetBusy(bool busy, string stateText)
    {
        UseWaitCursor = busy;
        _busyLabel.Text = stateText;
        _busyLabel.ForeColor = busy ? Color.DarkOrange : Color.DarkGreen;
        _browseButton.Enabled = !busy;
        _refreshButton.Enabled = !busy;
        _addProviderButton.Enabled = !busy;
        _removeProviderButton.Enabled = !busy;
        _updateConfigCheck.Enabled = !busy;
        _backupRetentionInput.Enabled = !busy;
        _executeButton.Enabled = !busy;
        _restoreButton.Enabled = !busy;
        _openBackupButton.Enabled = !busy;
        _pruneBackupsButton.Enabled = !busy;
        _providerList.Enabled = !busy;
        _manualProviderText.Enabled = !busy;
        _codexHomeCombo.Enabled = !busy;
    }

    private void AppendLog(string message)
    {
        if (_logBox.TextLength > 0)
        {
            _logBox.AppendText(Environment.NewLine);
        }

        _logBox.AppendText(message);
        _logBox.SelectionStart = _logBox.TextLength;
        _logBox.ScrollToCaret();
    }

    private bool ConfirmCodexClosed(string message)
    {
        return MessageBox.Show(
            this,
            message,
            Text,
            MessageBoxButtons.OKCancel,
            MessageBoxIcon.Warning) == DialogResult.OK;
    }

    private bool ConfirmBackupPrune()
    {
        string message =
            $"确认清理旧备份？{Environment.NewLine}{Environment.NewLine}" +
            $"将只保留最近 {CurrentBackupRetentionCount()} 份受本工具管理的备份。{Environment.NewLine}" +
            "被删除的旧备份无法直接恢复。";

        return MessageBox.Show(
            this,
            message,
            Text,
            MessageBoxButtons.OKCancel,
            MessageBoxIcon.Warning) == DialogResult.OK;
    }
}
