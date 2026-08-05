using CodexProviderSync.App.Automation;
using CodexProviderSync.Application;
using CodexProviderSync.Core;
using System.Text.Json;

namespace CodexProviderSync.App;

public sealed class MainForm : Form
{
    private const int ActionGroupMinimumWidth = 460;

    private readonly CodexSyncService _syncService;
    private readonly IApplicationService _applicationService;
    private readonly ApplicationOperationTraceHub _applicationTraceHub;
    private readonly AppController _appController;
    private readonly SettingsService _settingsService;
    private readonly UpdateService _updateService;
    private readonly ExecutionLogService _executionLogService;
    private readonly IAppPathProvider _paths;
    private readonly IAppPlatformBoundary _platformBoundary;
    private readonly Func<DateOnly> _localDate;

    private readonly ComboBox _codexHomeCombo = new() { Dock = DockStyle.Fill, DropDownStyle = ComboBoxStyle.DropDown };
    private readonly Button _browseButton = new() { Text = "浏览...", AutoSize = true };
    private readonly Button _refreshButton = new() { Text = "刷新", AutoSize = true };
    private readonly TextBox _sqliteHomeText = new() { Dock = DockStyle.Fill };
    private readonly Button _browseSqliteHomeButton = new() { Text = "浏览...", AutoSize = true };
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
        Text = "同时更新 config.toml\r\n（切换 Provider）",
        AutoSize = true,
        Margin = new Padding(0, 4, 0, 0)
    };
    private readonly RadioButton _modelAutoRadio = new()
    {
        Text = "跟随 provider 里的 model (默认)",
        AutoSize = true,
        Checked = true,
        Margin = new Padding(0, 2, 12, 2)
    };
    private readonly RadioButton _modelKeepRadio = new()
    {
        Text = "保留当前顶层 model",
        AutoSize = true,
        Margin = new Padding(0, 2, 12, 2)
    };
    private readonly RadioButton _modelCustomRadio = new()
    {
        Text = "自定义:",
        AutoSize = true,
        Margin = new Padding(0, 2, 6, 2)
    };
    private readonly TextBox _modelCustomText = new()
    {
        Width = 140,
        Margin = new Padding(0, 0, 0, 2)
    };
    private readonly CheckBox _restoreConfigCheck = new() { Text = "恢复配置文件（config.toml）", Checked = false, AutoSize = true };
    private readonly CheckBox _restoreDatabaseCheck = new() { Text = "恢复线程数据库（SQLite）", Checked = true, AutoSize = true };
    private readonly CheckBox _restoreSessionsCheck = new() { Text = "恢复会话文件元数据（rollout）", Checked = true, AutoSize = true };
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
    private readonly Button _checkUpdateButton = new() { Text = "检查更新", Dock = DockStyle.Fill, Height = 40 };
    private readonly Button _openLogButton = new() { Text = "打开日志目录", AutoSize = true, Height = 32 };
    private readonly Label _busyLabel = new() { AutoSize = true, ForeColor = Color.DarkGreen, Text = "就绪" };
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
    private bool _logFailureReported;
    private bool _busy;
    private bool _updateCheckInProgress;
    private bool _renderingControllerState;
    private string? _sqliteOverrideCodexHome;

    public MainForm() : this(new SystemAppPathProvider())
    {
    }

    private MainForm(IAppPathProvider paths) : this(
        new ExecutionLogService(paths.LogDirectory),
        new SettingsService(paths.SettingsPath),
        paths: paths,
        platformBoundary: new SystemAppPlatformBoundary(paths))
    {
    }

    internal MainForm(
        ExecutionLogService executionLogService,
        SettingsService? settingsService = null,
        UpdateService? updateService = null,
        Func<DateOnly>? localDate = null,
        IAppPathProvider? paths = null,
        IAppPlatformBoundary? platformBoundary = null,
        CodexSyncService? syncService = null,
        IApplicationService? applicationService = null,
        ApplicationOperationTraceHub? applicationTraceHub = null)
    {
        _paths = paths ?? new SystemAppPathProvider();
        _platformBoundary = platformBoundary ?? new SystemAppPlatformBoundary(_paths);
        _executionLogService = executionLogService;
        _settingsService = settingsService ?? new SettingsService(_paths.SettingsPath);
        _updateService = updateService ?? new UpdateService();
        _localDate = localDate ?? (() => DateOnly.FromDateTime(DateTime.Now));
        _syncService = syncService ?? new CodexSyncService();
        _applicationTraceHub = applicationTraceHub ?? new ApplicationOperationTraceHub();
        IApplicationService sharedApplicationService = applicationService ?? new ApplicationService(
            new CoreApplicationStatusPort(_syncService),
            new CoreApplicationWritePort(_syncService, new CodexHomeService()),
            new InMemoryApplicationPlanLedger());
        _applicationService = new TrackedApplicationService(
            sharedApplicationService,
            _applicationTraceHub);
        _appController = new AppController(new CoreApplicationAdapter(
            _syncService,
            _settingsService,
            new CodexHomeService(),
            _applicationService));
        Text = "Codex Provider Sync";
        ConfigureAutomationIdentities();
        MinimumSize = new Size(1180, 760);
        StartPosition = FormStartPosition.CenterScreen;

        _executeButton.BackColor = Color.FromArgb(220, 252, 231);
        _executeButton.ForeColor = Color.FromArgb(22, 101, 52);
        _executeButton.FlatStyle = FlatStyle.Flat;
        _executeButton.FlatAppearance.BorderColor = Color.FromArgb(134, 239, 172);
        _executeButton.FlatAppearance.BorderSize = 1;
        _executeButton.FlatAppearance.MouseOverBackColor = Color.FromArgb(187, 247, 208);
        _executeButton.UseVisualStyleBackColor = false;

        _providerList.Columns.Add("Provider", 180);
        _providerList.Columns.Add("来源", 180);
        _providerList.Columns.Add("当前", 70);
        _providerList.Columns.Add("手动", 70);
        _providerList.Columns.Add("已保存", 70);

        BuildLayout();
        WireEvents();
    }

    private void ConfigureAutomationIdentities()
    {
        GuiAutomationCatalog.Register(this, GuiAutomationCatalog.Ids.MainWindow);
        GuiAutomationCatalog.Register(_codexHomeCombo, GuiAutomationCatalog.Ids.CodexHome);
        GuiAutomationCatalog.Register(_browseButton, GuiAutomationCatalog.Ids.BrowseCodexHome);
        GuiAutomationCatalog.Register(_refreshButton, GuiAutomationCatalog.Ids.RefreshStatus);
        GuiAutomationCatalog.Register(_sqliteHomeText, GuiAutomationCatalog.Ids.SqliteHome);
        GuiAutomationCatalog.Register(_browseSqliteHomeButton, GuiAutomationCatalog.Ids.BrowseSqliteHome);
        GuiAutomationCatalog.Register(_statusBox, GuiAutomationCatalog.Ids.StatusOutput);
        GuiAutomationCatalog.Register(_providerList, GuiAutomationCatalog.Ids.ProviderList);
        GuiAutomationCatalog.Register(_manualProviderText, GuiAutomationCatalog.Ids.ManualProviderId);
        GuiAutomationCatalog.Register(_addProviderButton, GuiAutomationCatalog.Ids.AddManualProvider);
        GuiAutomationCatalog.Register(_removeProviderButton, GuiAutomationCatalog.Ids.RemoveManualProvider);
        GuiAutomationCatalog.Register(_selectedProviderValue, GuiAutomationCatalog.Ids.SelectedProvider);
        GuiAutomationCatalog.Register(_updateConfigCheck, GuiAutomationCatalog.Ids.UpdateConfig);
        GuiAutomationCatalog.Register(_modelAutoRadio, GuiAutomationCatalog.Ids.FollowProviderModel);
        GuiAutomationCatalog.Register(_modelKeepRadio, GuiAutomationCatalog.Ids.KeepCurrentModel);
        GuiAutomationCatalog.Register(_modelCustomRadio, GuiAutomationCatalog.Ids.CustomModelMode);
        GuiAutomationCatalog.Register(_modelCustomText, GuiAutomationCatalog.Ids.CustomModel);
        GuiAutomationCatalog.Register(_restoreConfigCheck, GuiAutomationCatalog.Ids.RestoreConfig);
        GuiAutomationCatalog.Register(_restoreDatabaseCheck, GuiAutomationCatalog.Ids.RestoreDatabase);
        GuiAutomationCatalog.Register(_restoreSessionsCheck, GuiAutomationCatalog.Ids.RestoreSessions);
        GuiAutomationCatalog.Register(_backupRetentionInput, GuiAutomationCatalog.Ids.BackupRetention);
        GuiAutomationCatalog.Register(_executeButton, GuiAutomationCatalog.Ids.Execute);
        GuiAutomationCatalog.Register(_restoreButton, GuiAutomationCatalog.Ids.Restore);
        GuiAutomationCatalog.Register(_openBackupButton, GuiAutomationCatalog.Ids.OpenBackupDirectory);
        GuiAutomationCatalog.Register(_pruneBackupsButton, GuiAutomationCatalog.Ids.PruneBackups);
        GuiAutomationCatalog.Register(_checkUpdateButton, GuiAutomationCatalog.Ids.CheckUpdates);
        GuiAutomationCatalog.Register(_openLogButton, GuiAutomationCatalog.Ids.OpenLogDirectory);
        GuiAutomationCatalog.Register(_busyLabel, GuiAutomationCatalog.Ids.OperationState);
        GuiAutomationCatalog.Register(_warningLine1, GuiAutomationCatalog.Ids.CloseCodexWarningPrimary);
        GuiAutomationCatalog.Register(_warningLine2, GuiAutomationCatalog.Ids.CloseCodexWarningSecondary);
        GuiAutomationCatalog.Register(_logBox, GuiAutomationCatalog.Ids.LogOutput);
    }

    protected override async void OnLoad(EventArgs e)
    {
        base.OnLoad(e);
        // Disable all mutable entry points before the first asynchronous
        // settings read can yield and the form becomes visible.
        SetBusy(true, "初始化中...");
        await LoadStateAsync();
        if (_platformBoundary.UpdatesEnabled && !_paths.IsAutomation)
        {
            _ = CheckForUpdatesAsync(UpdateCheckTrigger.Automatic);
        }
    }

    /// <summary>
    /// Brings the running form back to the foreground. Invoked by the
    /// single-instance focus broker when a second copy of CodexProviderSync
    /// is launched and asks the first copy to take focus. We name it
    /// `RequestBringToFront` rather than overriding `Form.BringToFront`
    /// with the `new` keyword so we do not silently break if a future
    /// .NET version changes the base signature or marks it virtual.
    /// </summary>
    public void RequestBringToFront()
    {
        if (InvokeRequired)
        {
            BeginInvoke(new Action(RequestBringToFront));
            return;
        }
        if (WindowState == FormWindowState.Minimized)
        {
            WindowState = FormWindowState.Normal;
        }
        Show();
        Activate();
        TopMost = true;
        TopMost = false;
        Focus();
    }

    internal ApplicationOperationTraceHub ApplicationTraceHub => _applicationTraceHub;

    internal static bool IsApplicationBoundAutomationId(string automationId) =>
        automationId is
            GuiAutomationCatalog.Ids.RefreshStatus
            or GuiAutomationCatalog.Ids.Execute
            or GuiAutomationCatalog.Ids.Restore
            or GuiAutomationCatalog.Ids.PruneBackups;

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
        root.RowStyles.Add(new RowStyle(SizeType.Percent, 72));
        root.RowStyles.Add(new RowStyle(SizeType.Percent, 28));

        root.Controls.Add(BuildTopPanel(), 0, 0);
        root.Controls.Add(BuildMainPanel(), 0, 1);
        root.Controls.Add(BuildLogPanel(), 0, 2);

        Controls.Add(root);
    }

    private Control BuildTopPanel()
    {
        GroupBox group = new()
        {
            Text = "存储位置",
            Dock = DockStyle.Top,
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink
        };

        TableLayoutPanel panel = new()
        {
            Dock = DockStyle.Top,
            ColumnCount = 4,
            RowCount = 2,
            AutoSize = true,
            Padding = new Padding(10)
        };
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));

        Label recentLabel = new()
        {
            Text = "最近使用会自动保留在下拉列表中",
            AutoSize = true,
            ForeColor = SystemColors.GrayText,
            Anchor = AnchorStyles.Left
        };

        panel.Controls.Add(new Label { Text = "Codex Home", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 0);
        panel.Controls.Add(_codexHomeCombo, 1, 0);
        panel.Controls.Add(_browseButton, 2, 0);
        panel.Controls.Add(_refreshButton, 3, 0);
        panel.Controls.Add(new Label { Text = "SQLite Home（留空时按配置自动解析）", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 1);
        panel.Controls.Add(_sqliteHomeText, 1, 1);
        panel.Controls.Add(_browseSqliteHomeButton, 2, 1);
        panel.Controls.Add(recentLabel, 3, 1);
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
        main.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));

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
            Text = "刷新时会把扫描到的新 Provider 自动并入持久化列表",
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
        GroupBox group = new()
        {
            Text = "执行",
            Dock = DockStyle.Fill,
            MinimumSize = new Size(ActionGroupMinimumWidth, 0)
        };
        TableLayoutPanel panel = new()
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = 12,
            Padding = new Padding(12)
        };
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.Percent, 100));

        panel.Controls.Add(new Label { Text = "目标 Provider", AutoSize = true }, 0, 0);
        panel.Controls.Add(_selectedProviderValue, 0, 1);
        panel.Controls.Add(BuildUpdateConfigPanel(), 0, 2);
        panel.Controls.Add(BuildWarningPanel(), 0, 3);
        panel.Controls.Add(BuildBackupRetentionPanel(), 0, 4);
        panel.Controls.Add(_executeButton, 0, 5);
        panel.Controls.Add(BuildRestoreOptionsPanel(), 0, 6);
        panel.Controls.Add(_restoreButton, 0, 7);
        panel.Controls.Add(_openBackupButton, 0, 8);
        panel.Controls.Add(_pruneBackupsButton, 0, 9);
        panel.Controls.Add(_checkUpdateButton, 0, 10);
        panel.Controls.Add(_busyLabel, 0, 11);

        group.Controls.Add(panel);
        return group;
    }

    private Control BuildUpdateConfigPanel()
    {
        TableLayoutPanel panel = new()
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = 2,
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            Margin = new Padding(0, 2, 0, 8)
        };
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.Controls.Add(_updateConfigCheck, 0, 0);

        TableLayoutPanel modelOptions = new()
        {
            Dock = DockStyle.Top,
            ColumnCount = 2,
            RowCount = 4,
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            Margin = new Padding(16, 8, 0, 0)
        };
        modelOptions.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        modelOptions.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        modelOptions.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        modelOptions.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        modelOptions.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        modelOptions.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        Label modelHeader = new()
        {
            Text = "顶层 model:",
            AutoSize = true,
            Margin = new Padding(0, 4, 0, 0)
        };

        modelOptions.Controls.Add(modelHeader, 0, 0);
        modelOptions.SetColumnSpan(modelHeader, 2);
        modelOptions.Controls.Add(_modelAutoRadio, 0, 1);
        modelOptions.SetColumnSpan(_modelAutoRadio, 2);
        modelOptions.Controls.Add(_modelKeepRadio, 0, 2);
        modelOptions.SetColumnSpan(_modelKeepRadio, 2);
        modelOptions.Controls.Add(_modelCustomRadio, 0, 3);
        modelOptions.Controls.Add(_modelCustomText, 1, 3);
        panel.Controls.Add(modelOptions, 0, 1);

        _modelAutoRadio.CheckedChanged += (_, _) => UpdateControllerModelState();
        _modelKeepRadio.CheckedChanged += (_, _) => UpdateControllerModelState();
        _modelCustomRadio.CheckedChanged += (_, _) => UpdateControllerModelState();
        _modelCustomText.TextChanged += (_, _) => UpdateControllerModelState();
        _updateConfigCheck.CheckedChanged += (_, _) => UpdateControllerModelState();
        UpdateModelOptionsEnabled();
        return panel;
    }

    private void UpdateControllerModelState()
    {
        if (_renderingControllerState)
        {
            return;
        }

        _appController.SetUpdateConfig(_updateConfigCheck.Checked);
        if (_modelCustomRadio.Checked)
        {
            _appController.SetModelMode(ModelMode.Custom);
        }
        else if (_modelKeepRadio.Checked)
        {
            _appController.SetModelMode(ModelMode.KeepRootModel);
        }
        else
        {
            _appController.SetModelMode(ModelMode.FollowProvider);
        }
        _appController.SetCustomModel(_modelCustomText.Text);
        UpdateModelOptionsEnabled();
    }

    private void UpdateModelOptionsEnabled()
    {
        bool enabled = !_busy && _updateConfigCheck.Checked;
        _modelAutoRadio.Enabled = enabled;
        _modelKeepRadio.Enabled = enabled;
        _modelCustomRadio.Enabled = enabled;
        _modelCustomText.Enabled = enabled && _modelCustomRadio.Checked;
    }

    private Control BuildWarningPanel()
    {
        Panel panel = new()
        {
            Dock = DockStyle.Fill,
            Height = 72,
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
            Margin = new Padding(0, 2, 0, 8),
            AutoSize = true
        };
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));

        panel.Controls.Add(new Label
        {
            Text = "自动保留最近",
            AutoSize = true,
            Anchor = AnchorStyles.Left,
            Margin = new Padding(0, 6, 8, 0)
        }, 0, 0);
        panel.Controls.Add(_backupRetentionInput, 1, 0);
        panel.Controls.Add(new Label
        {
            Text = "份备份",
            AutoSize = true,
            Anchor = AnchorStyles.Left,
            Margin = new Padding(8, 6, 0, 0)
        }, 2, 0);

        return panel;
    }

    private Control BuildRestoreOptionsPanel()
    {
        TableLayoutPanel panel = new()
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            Margin = new Padding(0, 0, 0, 6),
            AutoSize = true
        };
        panel.RowCount = 4;
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));

        panel.Controls.Add(new Label
        {
            Text = "恢复内容",
            AutoSize = true,
            Anchor = AnchorStyles.Left,
            Margin = new Padding(0, 2, 0, 4)
        }, 0, 0);
        panel.Controls.Add(_restoreConfigCheck, 0, 1);
        panel.Controls.Add(_restoreDatabaseCheck, 0, 2);
        panel.Controls.Add(_restoreSessionsCheck, 0, 3);
        return panel;
    }

    private Control BuildLogPanel()
    {
        GroupBox group = new() { Text = "执行日志", Dock = DockStyle.Fill };
        TableLayoutPanel panel = new()
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = 2,
            Padding = new Padding(4)
        };
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.Percent, 100));

        FlowLayoutPanel toolbar = new()
        {
            Dock = DockStyle.Fill,
            AutoSize = true,
            FlowDirection = FlowDirection.RightToLeft,
            WrapContents = false
        };
        toolbar.Controls.Add(_openLogButton);
        panel.Controls.Add(toolbar, 0, 0);
        panel.Controls.Add(_logBox, 0, 1);
        group.Controls.Add(panel);
        return group;
    }

    private void WireEvents()
    {
        _browseButton.Click += async (_, _) => await BrowseCodexHomeAsync();
        _browseSqliteHomeButton.Click += async (_, _) => await BrowseSqliteHomeAsync();
        _refreshButton.Click += async (_, _) => await RunApplicationControlEventAsync(() => RefreshStatusAsync());
        _addProviderButton.Click += async (_, _) => await AddManualProviderAsync();
        _removeProviderButton.Click += async (_, _) => await RemoveManualProviderAsync();
        _backupRetentionInput.ValueChanged += async (_, _) => await PersistBackupRetentionAsync();
        _executeButton.Click += async (_, _) => await RunApplicationControlEventAsync(ExecuteSyncOrSwitchAsync);
        _restoreButton.Click += async (_, _) => await RunApplicationControlEventAsync(RestoreBackupAsync);
        _openBackupButton.Click += (_, _) => OpenBackupFolder();
        _pruneBackupsButton.Click += async (_, _) => await RunApplicationControlEventAsync(PruneBackupsAsync);
        _checkUpdateButton.Click += async (_, _) => await CheckForUpdatesAsync(UpdateCheckTrigger.Manual);
        _openLogButton.Click += (_, _) => OpenLogFolder();
        _providerList.SelectedIndexChanged += (_, _) => UpdateControllerProviderSelection();
        _codexHomeCombo.Leave += async (_, _) => await PersistHomeSelectionAsync();
        _sqliteHomeText.Leave += async (_, _) => await PersistSqliteHomeOverrideAsync(CaptureStorageSelection());
    }

    private void UpdateControllerProviderSelection()
    {
        if (_renderingControllerState)
        {
            return;
        }

        string? provider = _providerList.SelectedItems.Count == 0
            ? null
            : _providerList.SelectedItems[0].Tag as string;
        _appController.SetProvider(provider);
        UpdateSelectionLabel();
    }

    private async Task LoadStateAsync()
    {
        _loadingSettings = true;
        _settings = await _settingsService.LoadAsync();
        ApplyWindowBounds(_settings.WindowBounds);
        ReloadRecentHomes();
        _codexHomeCombo.Text = _settings.LastCodexHome ?? _paths.DefaultCodexHome;
        LoadSqliteHomeOverride(CurrentCodexHome());
        _backupRetentionInput.Value = Math.Max(_backupRetentionInput.Minimum, Math.Min(_backupRetentionInput.Maximum, _settings.BackupRetentionCount));
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 已加载设置: {_settingsService.SettingsPath}");
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 执行日志文件: {_executionLogService.CurrentLogPath}");
        _loadingSettings = false;
        await RunBusyAsync("刷新中...", async () =>
        {
            AppSnapshot snapshot = await Task.Run(async () => await _appController.InitializeAsync());
            await ApplyControllerRefreshAsync(snapshot);
        });
    }

    private async Task RefreshStatusAsync(string? preferredProviderId = null)
    {
        (string codexHome, string? sqliteHome) = CaptureStorageSelection();
        await PersistSqliteHomeOverrideAsync((codexHome, sqliteHome));
        await RunBusyAsync("刷新中...", () => RefreshStatusCoreAsync(codexHome, sqliteHome, preferredProviderId));
    }

    private async Task BrowseCodexHomeAsync()
    {
        string? selectedPath = _platformBoundary.PickFolder(this, new FolderPickerRequest(
            "选择 .codex 目录",
            Directory.Exists(CurrentCodexHome()) ? CurrentCodexHome() : _paths.DefaultCodexHome));
        if (selectedPath is null)
        {
            return;
        }

        _codexHomeCombo.Text = selectedPath;
        await PersistHomeSelectionAsync();
        await RefreshStatusAsync();
    }

    private async Task PersistHomeSelectionAsync()
    {
        (string codexHome, _) = CaptureStorageSelection();
        if (string.IsNullOrWhiteSpace(codexHome))
        {
            return;
        }

        _settings = _settingsService.RecordCodexHome(_settings, codexHome);
        _settings = _settingsService.UpdateState(_settings, SelectedProvider(), _settings.LastBackupDirectory, CaptureWindowBounds(), CurrentBackupRetentionCount());
        await _settingsService.SaveAsync(_settings);
        ReloadRecentHomes();
    }

    private async Task BrowseSqliteHomeAsync()
    {
        (_, string? sqliteHome) = CaptureStorageSelection();
        string? selectedPath = _platformBoundary.PickFolder(this, new FolderPickerRequest(
            "选择包含 state_5.sqlite 的目录",
            Directory.Exists(sqliteHome)
                ? sqliteHome!
                : CurrentCodexHome()));
        if (selectedPath is null)
        {
            return;
        }

        (string codexHome, _) = CaptureStorageSelection();
        _sqliteHomeText.Text = selectedPath;
        await PersistSqliteHomeOverrideAsync((codexHome, selectedPath));
        await RefreshStatusAsync();
    }

    private async Task PersistSqliteHomeOverrideAsync((string CodexHome, string? SqliteHome) selection)
    {
        if (_loadingSettings)
        {
            return;
        }
        _settings = _settingsService.RecordSqliteHomeOverride(
            _settings,
            selection.CodexHome,
            selection.SqliteHome);
        await _settingsService.SaveAsync(_settings);
    }

    private void LoadSqliteHomeOverride(string codexHome)
    {
        _sqliteOverrideCodexHome = Path.GetFullPath(codexHome);
        _sqliteHomeText.Text = _settingsService.GetSqliteHomeOverride(_settings, codexHome)
            ?? _paths.RequiredSqliteHomeOverride
            ?? string.Empty;
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
        RefreshControllerProviderOptions(provider);
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
        RefreshControllerProviderOptions(_currentStatus?.CurrentProvider.Provider);
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 已删除手动 Provider: {provider}");
    }

    private void RefreshControllerProviderOptions(string? preferredProviderId)
    {
        if (_currentStatus is null)
        {
            return;
        }

        _appController.ApplyProviderOptions(
            _syncService.BuildProviderOptions(_currentStatus, _settings),
            preferredProviderId);
        ReloadProviderList();
    }

    private async Task ExecuteSyncOrSwitchAsync()
    {
        (string codexHome, string? sqliteHome) = CaptureStorageSelection();
        _appController.SetStorage(codexHome, sqliteHome);
        UpdateControllerModelState();
        SyncRequestPreparation preparation = _appController.PrepareSyncRequest();
        if (preparation.ValidationIssues.Contains(AppValidationIssue.ProviderRequired))
        {
            MessageBox.Show(this, "请先选择目标 Provider。", Text, MessageBoxButtons.OK, MessageBoxIcon.Information);
            return;
        }

        if (!ConfirmCodexClosed("执行同步前，请先关闭已打开的 Codex CLI、Codex App、app-server 和相关终端。是否继续？"))
        {
            return;
        }

        if (preparation.ValidationIssues.Contains(AppValidationIssue.CustomModelRequired))
        {
            MessageBox.Show(this, "请填写自定义 model 名称,或改成 \"跟随 provider\"。", Text, MessageBoxButtons.OK, MessageBoxIcon.Information);
            return;
        }
        if (!preparation.IsValid)
        {
            MessageBox.Show(this, "当前状态无法执行同步，请先刷新后重试。", Text, MessageBoxButtons.OK, MessageBoxIcon.Information);
            return;
        }

        PreparedSyncRequest request = preparation.Request!;
        string provider = request.ProviderId;
        int backupRetentionCount = CurrentBackupRetentionCount();

        await RunBusyAsync("执行中...", async () =>
        {
            EnsureAutomationMutationBoundary(
                request.CodexHome,
                request.SqliteHomeOverride,
                selectedBackupPath: null);
            ApplicationWriteIntent intent = request switch
            {
                SwitchProviderRequest switchRequest => new SwitchIntent(
                    switchRequest.CodexHome,
                    switchRequest.SqliteHomeOverride,
                    switchRequest.ProviderId,
                    switchRequest.ModelSelection,
                    backupRetentionCount),
                SyncProviderRequest syncRequest => new SyncIntent(
                    syncRequest.CodexHome,
                    syncRequest.SqliteHomeOverride,
                    syncRequest.ProviderId,
                    backupRetentionCount),
                _ => throw new InvalidOperationException("Unsupported prepared sync request.")
            };
            SyncResult result = await ExecutePlannedWriteAsync<SyncResult>(
                intent,
                authorization => intent switch
                {
                    SwitchIntent switchIntent => Task.Run(() => _applicationService.SwitchAsync(
                        new SwitchApplicationRequest(switchIntent, authorization))),
                    SyncIntent syncIntent => Task.Run(() => _applicationService.SyncAsync(
                        new SyncApplicationRequest(syncIntent, authorization))),
                    _ => throw new InvalidOperationException("Unsupported Application write intent.")
                });

            await PersistSqliteHomeOverrideAsync((request.CodexHome, request.SqliteHomeOverride));
            _settings = _settingsService.UpdateState(_settings, provider, result.BackupDir, CaptureWindowBounds(), backupRetentionCount);
            await _settingsService.SaveAsync(_settings);
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 执行完成");
            AppendLog(TextFormatter.FormatSyncResult(
                result,
                request is SwitchProviderRequest ? "已切换并同步" : "已同步",
                TextFormatter.ChineseSimplified));
            AppendLog(FormatModelSyncOutcome(result.ModelSync));
            AppendLog(string.Empty);
            await RefreshStatusCoreAsync(request.CodexHome, request.SqliteHomeOverride, provider);
        });
    }

    private async Task RestoreBackupAsync()
    {
        (string codexHome, string? sqliteHome) = CaptureStorageSelection();
        string backupRoot = _currentStatus?.BackupRoot ?? AppConstants.DefaultBackupRoot(codexHome);
        string initialBackupDir = Directory.Exists(_settings.LastBackupDirectory)
            ? _settings.LastBackupDirectory!
            : backupRoot;

        string? selectedBackupPath = _platformBoundary.PickFolder(this, new FolderPickerRequest(
            "选择要恢复的备份目录",
            initialBackupDir));
        if (selectedBackupPath is null)
        {
            return;
        }

        bool restoreConfig = _restoreConfigCheck.Checked;
        bool restoreDatabase = _restoreDatabaseCheck.Checked;
        bool restoreSessions = _restoreSessionsCheck.Checked;
        if (!restoreConfig && !restoreDatabase && !restoreSessions)
        {
            MessageBox.Show(this, "请至少选择一种要恢复的内容。", Text, MessageBoxButtons.OK, MessageBoxIcon.Information);
            return;
        }

        string restoreTargets = string.Join("、", new[]
        {
            restoreConfig ? "配置文件（config.toml）" : null,
            restoreDatabase ? "线程数据库（SQLite）" : null,
            restoreSessions ? "会话文件元数据（rollout）" : null
        }.Where(static item => item is not null));

        DialogResult confirm = MessageBox.Show(
            this,
            $"确认恢复以下备份？{Environment.NewLine}{Environment.NewLine}{selectedBackupPath}{Environment.NewLine}{Environment.NewLine}将覆盖当前的: {restoreTargets}。",
            Text,
            MessageBoxButtons.OKCancel,
            MessageBoxIcon.Warning);
        if (confirm != DialogResult.OK)
        {
            return;
        }

        if (!ConfirmCodexClosed("恢复备份前，请先关闭已打开的 Codex CLI、Codex App、app-server 和相关终端。是否继续？"))
        {
            return;
        }

        bool allowSqliteHomeRelocation = false;
        try
        {
            EnsureAutomationMutationBoundary(codexHome, sqliteHome, selectedBackupPath);
            if (restoreDatabase)
            {
                BackupStorageInfo backupStorage = await _syncService.GetBackupStorageInfoAsync(selectedBackupPath);
                StatusSnapshot targetStatus = RequireApplicationSuccess(
                    await Task.Run(() => _applicationService.GetStatusAsync(
                        new ApplicationStatusRequest(codexHome, sqliteHome))));
                ValidateAutomationStatusSnapshot(targetStatus);
                if (backupStorage.Version >= 2
                    && !string.IsNullOrWhiteSpace(backupStorage.SqliteHome)
                    && !PathsEqual(backupStorage.SqliteHome, targetStatus.StateDbLocation is null
                        ? targetStatus.SqliteHome
                        : Path.GetDirectoryName(targetStatus.StateDbLocation.Path)!))
                {
                    string targetSqliteHome = targetStatus.StateDbLocation is null
                        ? targetStatus.SqliteHome
                        : Path.GetDirectoryName(targetStatus.StateDbLocation.Path)!;
                    if (restoreConfig)
                    {
                        MessageBox.Show(
                            this,
                            "恢复到不同 SQLite Home 时不能同时恢复 config.toml。请取消勾选“恢复配置文件”后重试，以保留当前目标配置。",
                            Text,
                            MessageBoxButtons.OK,
                            MessageBoxIcon.Warning);
                        return;
                    }
                    DialogResult relocationConfirm = MessageBox.Show(
                        this,
                        $"备份中的 SQLite Home 与当前目标不同。{Environment.NewLine}{Environment.NewLine}"
                        + $"备份来源: {backupStorage.SqliteHome}{Environment.NewLine}"
                        + $"恢复目标: {targetSqliteHome}{Environment.NewLine}{Environment.NewLine}"
                        + "确认将数据库恢复到当前目标吗？",
                        Text,
                        MessageBoxButtons.OKCancel,
                        MessageBoxIcon.Warning);
                    if (relocationConfirm != DialogResult.OK)
                    {
                        return;
                    }
                    allowSqliteHomeRelocation = true;
                }
            }
        }
        catch (Exception error)
        {
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 读取备份信息失败: {error}");
            MessageBox.Show(this, $"无法读取备份信息。{Environment.NewLine}{Environment.NewLine}{error.Message}", Text, MessageBoxButtons.OK, MessageBoxIcon.Error);
            return;
        }

        await RunBusyAsync("恢复中...", async () =>
        {
            EnsureAutomationMutationBoundary(codexHome, sqliteHome, selectedBackupPath);
            RestoreIntent intent = new(
                CodexHome: codexHome,
                SqliteHomeOverride: sqliteHome,
                BackupDirectory: selectedBackupPath,
                RestoreConfig: restoreConfig,
                RestoreDatabase: restoreDatabase,
                RestoreSessions: restoreSessions,
                AllowSqliteHomeRelocation: allowSqliteHomeRelocation);
            RestoreResult result = await ExecutePlannedWriteAsync<RestoreResult>(
                intent,
                authorization => Task.Run(() => _applicationService.RestoreAsync(
                    new RestoreApplicationRequest(intent, authorization))));
            _settings = _settingsService.UpdateState(_settings, SelectedProvider(), selectedBackupPath, CaptureWindowBounds(), CurrentBackupRetentionCount());
            await _settingsService.SaveAsync(_settings);
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 恢复完成");
            AppendLog(TextFormatter.FormatRestoreResult(result, TextFormatter.ChineseSimplified));
            AppendLog(string.Empty);
            await RefreshStatusCoreAsync(codexHome, sqliteHome);
        });
    }

    private void OpenBackupFolder()
    {
        string path = _currentStatus?.BackupRoot ?? AppConstants.DefaultBackupRoot(CurrentCodexHome());
        EnsureAutomationPath(path, GuiAutomationCatalog.Ids.OpenBackupDirectory);
        Directory.CreateDirectory(path);
        _platformBoundary.OpenPath(path);
    }

    private void OpenLogFolder()
    {
        try
        {
            Directory.CreateDirectory(_executionLogService.LogDirectory);
            _platformBoundary.OpenPath(_executionLogService.LogDirectory);
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 已打开日志目录: {_executionLogService.LogDirectory}");
        }
        catch (Exception error)
        {
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 打开日志目录失败: {error}");
            MessageBox.Show(this, $"无法打开日志目录。{Environment.NewLine}{Environment.NewLine}{error.Message}", Text, MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
    }

    private async Task PruneBackupsAsync()
    {
        (string codexHome, string? sqliteHome) = CaptureStorageSelection();
        if (!ConfirmBackupPrune())
        {
            return;
        }

        await RunBusyAsync("正在清理备份...", async () =>
        {
            EnsureAutomationMutationBoundary(codexHome, sqliteHome, selectedBackupPath: null);
            PruneIntent intent = new(codexHome, sqliteHome, CurrentBackupRetentionCount());
            BackupPruneResult result = await ExecutePlannedWriteAsync<BackupPruneResult>(
                intent,
                authorization => Task.Run(() => _applicationService.PruneAsync(
                    new PruneApplicationRequest(intent, authorization))));
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 旧备份清理完成");
            AppendLog(TextFormatter.FormatBackupPruneResult(result, TextFormatter.ChineseSimplified));
            AppendLog(string.Empty);
            await RefreshStatusCoreAsync(codexHome, sqliteHome);
        });
    }

    private async Task CheckForUpdatesAsync(UpdateCheckTrigger trigger)
    {
        if (!_platformBoundary.UpdatesEnabled)
        {
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] GUI automation mode blocks update checks and process launches");
            return;
        }
        bool automatic = trigger == UpdateCheckTrigger.Automatic;
        DateOnly today = _localDate();
        if (automatic && !UpdateCheckPolicy.ShouldRunAutomaticCheck(_settings, today))
        {
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 今日已尝试自动检查更新，已跳过");
            return;
        }

        if (_updateCheckInProgress)
        {
            if (!automatic)
            {
                AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 更新检查已在进行中");
            }
            return;
        }

        _updateCheckInProgress = true;
        UpdateCheckButtonState();
        try
        {
            if (automatic)
            {
                _settings = _settingsService.RecordAutomaticUpdateCheck(_settings, today);
                try
                {
                    await _settingsService.SaveAsync(_settings);
                }
                catch (Exception error)
                {
                    AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 保存自动更新检查日期失败:{Environment.NewLine}{error}");
                }

                await CheckForUpdatesCoreAsync(trigger);
            }
            else
            {
                await RunBusyAsync("正在检查更新...", () => CheckForUpdatesCoreAsync(trigger));
            }
        }
        catch (Exception error)
        {
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 更新检查出现未处理错误:{Environment.NewLine}{error}");
            if (UpdateCheckPolicy.ShouldShowFailureDialog(trigger))
            {
                MessageBox.Show(
                    this,
                    $"检查更新失败。{Environment.NewLine}{Environment.NewLine}{error.Message}",
                    Text,
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Error);
            }
        }
        finally
        {
            _updateCheckInProgress = false;
            UpdateCheckButtonState();
        }
    }

    private async Task CheckForUpdatesCoreAsync(UpdateCheckTrigger trigger)
    {
        bool automatic = trigger == UpdateCheckTrigger.Automatic;
        string checkKind = automatic ? "自动" : "手动";
        Version currentVersion = UpdateService.NormalizeVersion(GetType().Assembly.GetName().Version ?? new Version(0, 0, 0));
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 正在{checkKind}检查更新（最长 10 秒），当前版本: v{currentVersion}");

        UpdateCheckResult update;
        try
        {
            update = await _platformBoundary.CheckForUpdateAsync(
                _updateService,
                currentVersion);
        }
        catch (TimeoutException error)
        {
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {checkKind}检查更新超时（10 秒），已跳过，不影响正常使用{Environment.NewLine}{error}");
            if (UpdateCheckPolicy.ShouldShowFailureDialog(trigger))
            {
                MessageBox.Show(
                    this,
                    "检查更新超过 10 秒，已取消。请检查网络或代理设置后重试。",
                    Text,
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Warning);
            }
            return;
        }
        catch (Exception error)
        {
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {checkKind}检查更新失败，不影响正常使用{Environment.NewLine}{error}");
            if (UpdateCheckPolicy.ShouldShowFailureDialog(trigger))
            {
                MessageBox.Show(
                    this,
                    $"检查更新失败。{Environment.NewLine}{Environment.NewLine}{error.Message}",
                    Text,
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Error);
            }
            return;
        }

        if (!update.IsUpdateAvailable)
        {
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 未发现更高版本，GitHub 最新 Release: {update.LatestRelease.TagName}");
            if (UpdateCheckPolicy.ShouldShowNoUpdateDialog(trigger))
            {
                MessageBox.Show(this, $"当前已是最新版本（v{currentVersion}）。", Text, MessageBoxButtons.OK, MessageBoxIcon.Information);
            }
            return;
        }

        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 发现新版本: {update.LatestRelease.TagName}");
        if (automatic)
        {
            await WaitForIdleAsync();
            if (IsDisposed || Disposing)
            {
                return;
            }
        }

        DialogResult choice = MessageBox.Show(
            this,
            $"发现新版本 {update.LatestRelease.TagName}（当前 v{currentVersion}）。\n\n是否下载、校验并重启完成更新？",
            "发现更新",
            MessageBoxButtons.YesNo,
            MessageBoxIcon.Information);
        if (choice != DialogResult.Yes)
        {
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 已取消更新: {update.LatestRelease.TagName}");
            return;
        }

        bool ownsBusyState = automatic;
        if (ownsBusyState)
        {
            SetBusy(true, "正在下载更新...");
        }
        else
        {
            _busyLabel.Text = "正在下载更新...";
        }

        try
        {
            DownloadedUpdate downloadedUpdate = await _updateService.DownloadWindowsExeAsync(
                update.LatestRelease,
                _paths.UpdateDownloadDirectory);
            string targetExe = Environment.ProcessPath ?? throw new InvalidOperationException("无法确定当前程序文件路径。");
            _platformBoundary.StartUpdate(downloadedUpdate.Path, targetExe, downloadedUpdate.Sha256);
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 已下载并校验 {update.LatestRelease.TagName}，正在重启完成更新。");
            BeginInvoke(Close);
        }
        catch (Exception error)
        {
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 下载或应用更新失败:{Environment.NewLine}{error}");
            MessageBox.Show(
                this,
                $"下载或应用更新失败。{Environment.NewLine}{Environment.NewLine}{error.Message}",
                Text,
                MessageBoxButtons.OK,
                MessageBoxIcon.Error);
        }
        finally
        {
            if (ownsBusyState && !IsDisposed)
            {
                SetBusy(false, "就绪");
            }
        }
    }

    private async Task WaitForIdleAsync()
    {
        while (_busy && !IsDisposed && !Disposing)
        {
            await Task.Delay(100);
        }
    }

    private void ReloadRecentHomes()
    {
        string currentText = _codexHomeCombo.Text;
        _codexHomeCombo.BeginUpdate();
        _codexHomeCombo.Items.Clear();
        foreach (string home in _settings.RecentCodexHomes)
        {
            _codexHomeCombo.Items.Add(GuiAutomationCatalog.RecentCodexHome(home));
        }
        _codexHomeCombo.EndUpdate();
        if (!string.IsNullOrWhiteSpace(currentText))
        {
            _codexHomeCombo.Text = currentText;
        }
    }

    private void ReloadProviderList()
    {
        _renderingControllerState = true;
        try
        {
            _providerList.BeginUpdate();
            _providerList.Items.Clear();
            foreach (ProviderOptionState option in _appController.Snapshot.Providers)
            {
                ListViewItem item = new(option.Id)
                {
                    Tag = option.Id
                };
                GuiAutomationCatalog.RegisterProviderRow(item, option.Id);
                item.SubItems.Add(TextFormatter.FormatProviderSources(new ProviderOption
                {
                    Id = option.Id,
                    Sources = option.Sources,
                    IsCurrentProvider = option.IsCurrentProvider,
                    IsManual = option.IsManual,
                    IsSaved = option.IsSaved
                }, TextFormatter.ChineseSimplified));
                item.SubItems.Add(option.IsCurrentProvider ? "是" : string.Empty);
                item.SubItems.Add(option.IsManual ? "是" : string.Empty);
                item.SubItems.Add(option.IsSaved ? "是" : string.Empty);
                _providerList.Items.Add(item);
            }
            _providerList.EndUpdate();
            SelectProviderInList(_appController.Snapshot.SelectedProviderId);
            UpdateSelectionLabel();
        }
        finally
        {
            _renderingControllerState = false;
        }
    }

    private void SelectProviderInList(string? provider)
    {
        if (string.IsNullOrWhiteSpace(provider))
        {
            _providerList.SelectedItems.Clear();
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
        return _appController.Snapshot.SelectedProviderId;
    }

    private string CurrentCodexHome()
    {
        string text = _codexHomeCombo.Text.Trim();
        string path = string.IsNullOrWhiteSpace(text) ? _paths.DefaultCodexHome : text;
        EnsureAutomationPath(path, GuiAutomationCatalog.Ids.CodexHome);
        return path;
    }

    private string? CurrentSqliteHomeOverride()
    {
        string text = _sqliteHomeText.Text.Trim();
        if (string.IsNullOrWhiteSpace(text))
        {
            return _paths.RequiredSqliteHomeOverride;
        }
        EnsureAutomationPath(text, GuiAutomationCatalog.Ids.SqliteHome);
        return text;
    }

    internal void ValidateAutomationValue(string automationId, JsonElement value)
    {
        if (!_paths.IsAutomation
            || automationId is not (GuiAutomationCatalog.Ids.CodexHome or GuiAutomationCatalog.Ids.SqliteHome))
        {
            return;
        }
        string? path = value.ValueKind == JsonValueKind.String ? value.GetString() : null;
        if (automationId == GuiAutomationCatalog.Ids.SqliteHome && string.IsNullOrWhiteSpace(path))
        {
            return;
        }
        EnsureAutomationPath(path ?? string.Empty, automationId);
    }

    private void EnsureAutomationPath(string path, string automationId)
    {
        if (!_paths.IsAutomation)
        {
            return;
        }

        bool contained;
        try
        {
            contained = !string.IsNullOrWhiteSpace(path) && _paths.Contains(path);
        }
        catch (Exception error) when (error is IOException
            or UnauthorizedAccessException
            or ArgumentException
            or NotSupportedException)
        {
            throw new InvalidOperationException(
                $"{automationId} could not be proven to remain inside the GUI automation isolation root.",
                error);
        }

        if (!contained)
        {
            throw new InvalidOperationException(
                $"{automationId} must remain inside the GUI automation isolation root.");
        }
    }

    internal void ValidateAutomationStatusSnapshot(StatusSnapshot status)
    {
        ArgumentNullException.ThrowIfNull(status);
        if (!_paths.IsAutomation)
        {
            return;
        }

        EnsureAutomationPath(status.CodexHome, "status.codexHome");
        EnsureAutomationPath(status.SqliteHome, "status.sqliteHome");
        EnsureAutomationPath(status.BackupRoot, "status.backupRoot");
        if (status.StateDbLocation is { } stateDb)
        {
            EnsureAutomationPath(stateDb.Path, "status.stateDb");
        }
        foreach (string candidate in status.CheckedStateDbPaths)
        {
            EnsureAutomationPath(candidate, "status.checkedStateDb");
        }
        foreach (string rolloutPath in status.LockedRolloutFiles)
        {
            EnsureAutomationPath(rolloutPath, "status.lockedRollout");
        }
        foreach (string rolloutPath in status.UnreadableRolloutFiles)
        {
            EnsureAutomationPath(rolloutPath, "status.unreadableRollout");
        }
        foreach (TransactionRecoveryInfo pending in status.PendingTransactions)
        {
            EnsureAutomationPath(pending.BackupDirectory, "status.pendingBackup");
            EnsureAutomationPath(pending.JournalPath, "status.pendingJournal");
        }
    }

    internal void EnsureAutomationMutationBoundary(
        string codexHome,
        string? sqliteHome,
        string? selectedBackupPath)
    {
        if (!_paths.IsAutomation)
        {
            return;
        }

        EnsureAutomationPath(codexHome, "mutation.codexHome");
        string requiredSqliteHome = sqliteHome
            ?? _paths.RequiredSqliteHomeOverride
            ?? throw new InvalidOperationException(
                "GUI automation mutations require an explicit isolated SQLite Home.");
        EnsureAutomationPath(requiredSqliteHome, "mutation.sqliteHome");
        if (selectedBackupPath is not null)
        {
            EnsureAutomationPath(selectedBackupPath, "mutation.backup");
        }

        StatusSnapshot status = _currentStatus
            ?? throw new InvalidOperationException(
                "GUI automation mutations require a successfully validated status refresh.");
        ValidateAutomationStatusSnapshot(status);
        if (!PathsEqual(status.CodexHome, codexHome)
            || !PathsEqual(status.SqliteHome, requiredSqliteHome))
        {
            throw new InvalidOperationException(
                "GUI automation storage inputs changed after the validated status refresh.");
        }
    }

    internal (string CodexHome, string? SqliteHome) CaptureStorageSelection()
    {
        string codexHome = CurrentCodexHome();
        if (_sqliteOverrideCodexHome is null || !PathsEqual(_sqliteOverrideCodexHome, codexHome))
        {
            LoadSqliteHomeOverride(codexHome);
        }
        return (codexHome, CurrentSqliteHomeOverride());
    }

    private int CurrentBackupRetentionCount()
    {
        return Decimal.ToInt32(_backupRetentionInput.Value);
    }

    private void PersistUiState()
    {
        try
        {
            (string codexHome, string? sqliteHome) = CaptureStorageSelection();
            _settings = _settingsService.RecordCodexHome(_settings, codexHome);
            _settings = _settingsService.RecordSqliteHomeOverride(
                _settings,
                codexHome,
                sqliteHome);
            _settings = _settingsService.UpdateState(_settings, SelectedProvider(), _settings.LastBackupDirectory, CaptureWindowBounds(), CurrentBackupRetentionCount());
            _settingsService.Save(_settings);
        }
        catch
        {
            // Ignore shutdown persistence failures.
        }
    }

    private async Task RefreshStatusCoreAsync(
        string codexHome,
        string? sqliteHome,
        string? preferredProviderId = null)
    {
        AppSnapshot snapshot = await Task.Run(async () => await _appController.RefreshAsync(
            codexHome,
            sqliteHome,
            preferredProviderId));
        await ApplyControllerRefreshAsync(snapshot);
    }

    private async Task ApplyControllerRefreshAsync(AppSnapshot snapshot)
    {
        if (snapshot.Activity == AppActivity.Faulted || snapshot.Status is null)
        {
            throw new InvalidOperationException(snapshot.ErrorMessage ?? "Unable to refresh application state.");
        }

        ValidateAutomationStatusSnapshot(snapshot.Status);
        _currentStatus = snapshot.Status;
        _settings = await _settingsService.LoadAsync();
        _settings = _settingsService.UpdateState(
            _settings,
            snapshot.SelectedProviderId,
            _settings.LastBackupDirectory,
            CaptureWindowBounds(),
            CurrentBackupRetentionCount());
        await _settingsService.SaveAsync(_settings);

        _statusBox.Text = TextFormatter.FormatStatus(_currentStatus, TextFormatter.ChineseSimplified);
        ReloadRecentHomes();
        ReloadProviderList();
        _codexHomeCombo.Text = _currentStatus.CodexHome;
        AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 已刷新: {_currentStatus.CodexHome}");
    }

    private WindowBoundsState CaptureWindowBounds()
    {
        return WindowPlacementPolicy.Capture(Bounds, RestoreBounds, WindowState);
    }

    private void ApplyWindowBounds(WindowBoundsState? bounds)
    {
        Rectangle[] workingAreas = Screen.AllScreens.Select(screen => screen.WorkingArea).ToArray();
        Rectangle fallbackWorkingArea = Screen.PrimaryScreen?.WorkingArea
            ?? workingAreas.FirstOrDefault();
        WindowPlacement placement = WindowPlacementPolicy.Restore(
            bounds,
            workingAreas,
            fallbackWorkingArea,
            defaultSize: new Size(1280, 820),
            minimumSavedSize: new Size(800, 600));
        StartPosition = FormStartPosition.Manual;
        Bounds = placement.Bounds;
        if (placement.Maximized)
        {
            WindowState = FormWindowState.Maximized;
        }
    }

    private async Task RunApplicationControlEventAsync(Func<Task> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        try
        {
            await action();
        }
        finally
        {
            _applicationTraceHub.CompleteCurrentInvocation();
        }
    }

    private T RequireApplicationSuccess<T>(ApplicationOutcome<T> outcome)
        where T : class
    {
        AppendLog(
            $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Application {outcome.Operation} "
            + $"[{outcome.OperationId}] -> {outcome.Lifecycle}");
        foreach (ApplicationWarning warning in outcome.Warnings)
        {
            AppendLog($"  警告 [{warning.Code}]: {warning.Message}");
        }

        if (outcome.IsSuccess && outcome.Data is not null)
        {
            return outcome.Data;
        }

        string detail = outcome.Errors.Count == 0
            ? $"Application 操作结束状态为 {outcome.Lifecycle}。"
            : string.Join(Environment.NewLine, outcome.Errors.Select(FormatApplicationError));
        throw new GuiApplicationOutcomeException(
            outcome.OperationId,
            outcome.Operation,
            outcome.Lifecycle,
            detail);
    }

    private async Task<T> ExecutePlannedWriteAsync<T>(
        ApplicationWriteIntent intent,
        Func<ApplicationApplyAuthorization, Task<ApplicationOutcome<ApplicationWriteResult<T>>>> apply)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(intent);
        ArgumentNullException.ThrowIfNull(apply);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            ApplicationOperationPlan plan = RequireReadyPlan(
                await Task.Run(() => _applicationService.CreatePlanAsync(
                    new CreateApplicationPlanRequest(intent))));
            AppendPlanSummary(plan);
            ApplicationOutcome<ApplicationWriteResult<T>> outcome = await apply(
                new ApplicationApplyAuthorization(
                    Apply: true,
                    Plan: plan,
                    PlanDigest: plan.Digest));
            if (GuiApplicationOutcomePolicy.IsPlanStale(outcome) && attempt == 0)
            {
                AppendLog(
                    $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 计划应用前状态发生变化，"
                    + "将基于同一输入重新生成一次计划。");
                continue;
            }
            return RequireAppliedOutcome(outcome);
        }

        throw new InvalidOperationException("The bounded plan retry was exhausted.");
    }

    private ApplicationOperationPlan RequireReadyPlan(
        ApplicationOutcome<ApplicationOperationPlan> outcome)
    {
        ApplicationOperationPlan plan = RequireApplicationSuccess(outcome);
        if (outcome.Lifecycle != ApplicationOperationLifecycle.ReadyToApply)
        {
            throw new GuiApplicationOutcomeException(
                outcome.OperationId,
                outcome.Operation,
                outcome.Lifecycle,
                "Application did not return a ready-to-apply plan.");
        }
        return plan;
    }

    private T RequireAppliedOutcome<T>(
        ApplicationOutcome<ApplicationWriteResult<T>> outcome)
        where T : class
    {
        ApplicationWriteResult<T> writeResult = RequireApplicationSuccess(outcome);
        if (!GuiApplicationOutcomePolicy.IsAppliedSuccess(outcome))
        {
            throw new GuiApplicationOutcomeException(
                outcome.OperationId,
                outcome.Operation,
                outcome.Lifecycle,
                "Application returned a dry-run result where the GUI required explicit apply.");
        }
        return writeResult.Result!;
    }

    private void AppendPlanSummary(ApplicationOperationPlan plan)
    {
        AppendLog(
            $"计划摘要: id={plan.PlanId}, digest={plan.Digest}, "
            + $"目标={plan.Targets.Count}, 自动清理={plan.AutoPruneDeletionTargets.Count}, "
            + $"过期={plan.ExpiresAtUtc:O}");
        foreach (ApplicationPlanTarget target in plan.Targets)
        {
            AppendLog($"  {target.Action}: {target.Path} [{target.Fingerprint}]");
        }
        foreach (ApplicationPlanTarget target in plan.AutoPruneDeletionTargets)
        {
            AppendLog($"  auto-prune {target.Action}: {target.Path} [{target.Fingerprint}]");
        }
    }

    private static string FormatApplicationError(ApplicationError error)
    {
        List<string> evidence = [];
        if (!string.IsNullOrWhiteSpace(error.RollbackStatus))
        {
            evidence.Add($"rollback={error.RollbackStatus}");
        }
        if (!string.IsNullOrWhiteSpace(error.EvidencePath))
        {
            evidence.Add($"evidence={error.EvidencePath}");
        }
        string suffix = evidence.Count == 0 ? string.Empty : $" ({string.Join(", ", evidence)})";
        return $"[{error.Code}] {error.Message}{suffix}";
    }

    private async Task RunBusyAsync(string stateText, Func<Task> action)
    {
        SetBusy(true, stateText);
        try
        {
            await action();
        }
        catch (GuiApplicationOutcomeException error)
            when (error.Lifecycle == ApplicationOperationLifecycle.Cancelled)
        {
            AppendLog(
                $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 操作已取消 "
                + $"[{error.OperationId}]: {error.Message}");
        }
        catch (GuiApplicationOutcomeException error)
        {
            AppendLog(
                $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Application 错误 "
                + $"[{error.OperationId}]: {error}");
            string prefix = error.Lifecycle == ApplicationOperationLifecycle.RecoveryRequired
                ? "操作未能安全完成，需要恢复检查。"
                : "操作失败。";
            MessageBox.Show(
                this,
                $"{prefix}{Environment.NewLine}{Environment.NewLine}{error.Message}",
                Text,
                MessageBoxButtons.OK,
                MessageBoxIcon.Error);
        }
        catch (Exception error)
        {
            AppendLog($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 错误: {error}");
            MessageBox.Show(this, $"操作失败。{Environment.NewLine}{Environment.NewLine}{error.Message}", Text, MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
        finally
        {
            SetBusy(false, "就绪");
        }
    }

    private sealed class GuiApplicationOutcomeException : InvalidOperationException
    {
        internal GuiApplicationOutcomeException(
            string operationId,
            ApplicationOperationKind operation,
            ApplicationOperationLifecycle lifecycle,
            string message)
            : base(message)
        {
            OperationId = operationId;
            Operation = operation;
            Lifecycle = lifecycle;
        }

        internal string OperationId { get; }

        internal ApplicationOperationKind Operation { get; }

        internal ApplicationOperationLifecycle Lifecycle { get; }
    }

    private void SetBusy(bool busy, string stateText)
    {
        bool sqliteActionsSupported = _appController.Snapshot.Activity == AppActivity.Ready
            && _currentStatus?.SqliteAccess.Supported != false;
        _busy = busy;
        UseWaitCursor = busy;
        _busyLabel.Text = stateText;
        _busyLabel.ForeColor = busy ? Color.DarkOrange : Color.DarkGreen;
        _browseButton.Enabled = !busy;
        _browseSqliteHomeButton.Enabled = !busy;
        _refreshButton.Enabled = !busy;
        _addProviderButton.Enabled = !busy;
        _removeProviderButton.Enabled = !busy;
        _updateConfigCheck.Enabled = !busy;
        _backupRetentionInput.Enabled = !busy;
        _restoreConfigCheck.Enabled = !busy;
        _restoreDatabaseCheck.Enabled = !busy;
        _restoreSessionsCheck.Enabled = !busy;
        _executeButton.Enabled = !busy && sqliteActionsSupported;
        _restoreButton.Enabled = !busy && sqliteActionsSupported;
        _openBackupButton.Enabled = !busy;
        _pruneBackupsButton.Enabled = !busy;
        UpdateCheckButtonState();
        _openLogButton.Enabled = !busy;
        _providerList.Enabled = !busy;
        _manualProviderText.Enabled = !busy;
        _codexHomeCombo.Enabled = !busy;
        _sqliteHomeText.Enabled = !busy;
        UpdateModelOptionsEnabled();
    }

    private static bool PathsEqual(string left, string right)
    {
        return string.Equals(
            Path.GetFullPath(left),
            Path.GetFullPath(right),
            OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
    }

    private void UpdateCheckButtonState()
    {
        _checkUpdateButton.Enabled = !_busy && !_updateCheckInProgress;
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

        if (!_executionLogService.TryAppend(message, out Exception? error) && !_logFailureReported)
        {
            _logFailureReported = true;
            string warning = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 警告: 无法写入本地执行日志: {error?.Message}";
            _logBox.AppendText(Environment.NewLine);
            _logBox.AppendText(warning);
            _logBox.SelectionStart = _logBox.TextLength;
            _logBox.ScrollToCaret();
        }
    }

    private static string FormatModelSyncOutcome(ModelSyncOutcome outcome)
    {
        if (outcome.Source == "not-applicable")
        {
            return string.Empty;
        }
        if (outcome.Applied)
        {
            string source = outcome.Source switch
            {
                "explicit" => "手动指定",
                "provider-section" => "Provider 配置",
                _ => outcome.Source
            };
            return $"顶层 model: {outcome.Model}（来源: {source}）";
        }
        if (!string.IsNullOrEmpty(outcome.Warning))
        {
            return outcome.Source == "none"
                ? "顶层 model: 未改写（目标 Provider 配置中没有 model，已保留当前值；也可以选择自定义 model 后重试）"
                : $"顶层 model: 未改写（{outcome.Warning}）";
        }
        return outcome.Source == "keep-root-model"
            ? "顶层 model: 未改写（已请求保留）"
            : "顶层 model: 未改写";
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
