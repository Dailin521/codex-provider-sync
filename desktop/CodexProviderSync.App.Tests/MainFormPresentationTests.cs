using System.Drawing;
using System.Reflection;
using System.Windows.Forms;
using CodexProviderSync.Core;

namespace CodexProviderSync.App.Tests;

public sealed class MainFormPresentationTests
{
    [Fact]
    public void MainForm_UsesChineseChromeAndGreenPrimaryAction()
    {
        string root = Path.Combine(Path.GetTempPath(), $"codex-provider-ui-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            ExecutionLogService logService = new(root);
            using MainForm form = new(logService);

            Button execute = Field<Button>(form, "_executeButton");
            Assert.Equal("立即同步", execute.Text);
            Assert.Equal(Color.FromArgb(220, 252, 231), execute.BackColor);
            Assert.Equal(Color.FromArgb(22, 101, 52), execute.ForeColor);
            Assert.Equal(Color.FromArgb(134, 239, 172), execute.FlatAppearance.BorderColor);
            Assert.Equal(Color.FromArgb(187, 247, 208), execute.FlatAppearance.MouseOverBackColor);
            Assert.Equal(FlatStyle.Flat, execute.FlatStyle);

            Assert.Equal("浏览...", Field<Button>(form, "_browseButton").Text);
            Assert.Equal("浏览...", Field<Button>(form, "_browseSqliteHomeButton").Text);
            Assert.True(string.IsNullOrEmpty(Field<TextBox>(form, "_sqliteHomeText").Text));
            Assert.Equal("刷新", Field<Button>(form, "_refreshButton").Text);
            Assert.Equal("打开日志目录", Field<Button>(form, "_openLogButton").Text);
            Assert.Equal("就绪", Field<Label>(form, "_busyLabel").Text);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void CaptureStorageSelection_RebindsSqliteOverrideBeforeReturningNewCodexHome()
    {
        string root = Path.Combine(Path.GetTempPath(), $"codex-provider-ui-test-{Guid.NewGuid():N}");
        string codexHomeA = Path.Combine(root, "home-a");
        string codexHomeB = Path.Combine(root, "home-b");
        string sqliteHomeA = Path.Combine(root, "sqlite-a");
        string sqliteHomeB = Path.Combine(root, "sqlite-b");
        Directory.CreateDirectory(root);
        try
        {
            SettingsService settingsService = new(Path.Combine(root, "settings.json"));
            AppSettings settings = settingsService.RecordSqliteHomeOverride(
                settingsService.RecordSqliteHomeOverride(new AppSettings(), codexHomeA, sqliteHomeA),
                codexHomeB,
                sqliteHomeB);
            using MainForm form = new(new ExecutionLogService(root), settingsService);
            SetField(form, "_settings", settings);

            ComboBox codexHome = Field<ComboBox>(form, "_codexHomeCombo");
            TextBox sqliteHome = Field<TextBox>(form, "_sqliteHomeText");
            codexHome.Text = codexHomeA;
            Assert.Equal((codexHomeA, sqliteHomeA), form.CaptureStorageSelection());
            Assert.Equal(sqliteHomeA, sqliteHome.Text);

            codexHome.Text = codexHomeB;
            Assert.Equal((codexHomeB, sqliteHomeB), form.CaptureStorageSelection());
            Assert.Equal(sqliteHomeB, sqliteHome.Text);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void MainForm_DisablesSqliteActionsForUnsupportedStorage()
    {
        string root = Path.Combine(Path.GetTempPath(), $"codex-provider-ui-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            using MainForm form = new(new ExecutionLogService(root));
            SetField(form, "_currentStatus", new StatusSnapshot
            {
                CodexHome = @"C:\Users\user\.codex",
                SqliteAccess = new SqliteAccessInfo(false, "windows-wsl-unc", "unsupported"),
                CurrentProvider = new CurrentProviderInfo("openai", false),
                ConfiguredProviders = ["openai"],
                RolloutCounts = new ProviderCounts(),
                LockedRolloutFiles = [],
                UnreadableRolloutFiles = [],
                EncryptedContentCounts = new ProviderCounts(),
                SqliteCounts = null,
                BackupRoot = root,
                BackupSummary = new BackupSummary { Count = 0, TotalBytes = 0 }
            });

            Invoke(form, "SetBusy", false, "就绪");

            Assert.False(Field<Button>(form, "_executeButton").Enabled);
            Assert.False(Field<Button>(form, "_restoreButton").Enabled);
            Assert.True(Field<Button>(form, "_refreshButton").Enabled);
            Assert.True(Field<Button>(form, "_pruneBackupsButton").Enabled);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void MainForm_KeepsActionOptionsInsideTheExecutionPanelAtMinimumSize()
    {
        string root = Path.Combine(Path.GetTempPath(), $"codex-provider-ui-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            using MainForm form = new(new ExecutionLogService(root));
            form.Size = form.MinimumSize;
            PerformLayoutRecursively(form);

            GroupBox actionGroup = Descendants(form)
                .OfType<GroupBox>()
                .Single(control => control.Text == "执行");
            TableLayoutPanel mainLayout = Assert.IsType<TableLayoutPanel>(actionGroup.Parent);
            int actionColumn = mainLayout.GetColumn(actionGroup);
            Assert.Equal(SizeType.AutoSize, mainLayout.ColumnStyles[actionColumn].SizeType);

            RadioButton autoModel = Field<RadioButton>(form, "_modelAutoRadio");
            RadioButton keepModel = Field<RadioButton>(form, "_modelKeepRadio");
            RadioButton customModel = Field<RadioButton>(form, "_modelCustomRadio");
            TableLayoutPanel modelLayout = Assert.IsType<TableLayoutPanel>(autoModel.Parent);
            Assert.Same(modelLayout, keepModel.Parent);
            Assert.Same(modelLayout, customModel.Parent);
            Assert.NotEqual(modelLayout.GetRow(autoModel), modelLayout.GetRow(keepModel));
            Assert.NotEqual(modelLayout.GetRow(keepModel), modelLayout.GetRow(customModel));

            Assert.True(autoModel.Checked);
            customModel.Checked = true;
            Assert.False(autoModel.Checked);
            Assert.False(keepModel.Checked);
            Assert.True(customModel.Checked);
            keepModel.Checked = true;
            Assert.False(autoModel.Checked);
            Assert.True(keepModel.Checked);
            Assert.False(customModel.Checked);

            CheckBox updateConfig = Field<CheckBox>(form, "_updateConfigCheck");
            TextBox customModelText = Field<TextBox>(form, "_modelCustomText");
            updateConfig.Checked = true;
            customModel.Checked = true;
            Assert.True(customModelText.Enabled);
            Assert.False(autoModel.Checked);
            autoModel.Checked = true;
            Assert.False(customModel.Checked);
            Assert.False(customModelText.Enabled);

            Assert.Equal("同时更新 config.toml\r\n（切换 Provider）", updateConfig.Text);
            Assert.True(
                updateConfig.ClientSize.Height >= updateConfig.PreferredSize.Height,
                $"Update-config checkbox is too short for its text: control height {updateConfig.ClientSize.Height}, preferred height {updateConfig.PreferredSize.Height}.");

            Label modelHeader = modelLayout.Controls
                .OfType<Label>()
                .Single(control => control.Text == "顶层 model:");
            Rectangle updateConfigBounds = BoundsRelativeTo(updateConfig, actionGroup);
            Rectangle modelHeaderBounds = BoundsRelativeTo(modelHeader, actionGroup);
            int updateConfigGap = modelHeaderBounds.Top - updateConfigBounds.Bottom;
            Assert.True(
                updateConfigGap >= 8,
                $"Update-config text is too close to the model header: gap {updateConfigGap}px.");

            Control[] criticalControls =
            [
                updateConfig,
                autoModel,
                keepModel,
                customModel,
                Field<TextBox>(form, "_modelCustomText"),
                Field<CheckBox>(form, "_restoreSessionsCheck")
            ];

            foreach (Control control in criticalControls)
            {
                Rectangle bounds = BoundsRelativeTo(control, actionGroup);
                Assert.True(
                    bounds.Left >= 0 && bounds.Right <= actionGroup.ClientSize.Width,
                    $"{control.Name}/{control.Text} extends outside the execution panel: {bounds}, panel width {actionGroup.ClientSize.Width}.");
            }
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static Rectangle BoundsRelativeTo(Control control, Control ancestor)
    {
        Rectangle bounds = control.Bounds;
        for (Control? parent = control.Parent; parent is not null && parent != ancestor; parent = parent.Parent)
        {
            bounds.Offset(parent.Left, parent.Top);
        }
        return bounds;
    }

    private static IEnumerable<Control> Descendants(Control control)
    {
        foreach (Control child in control.Controls)
        {
            yield return child;
            foreach (Control descendant in Descendants(child))
            {
                yield return descendant;
            }
        }
    }

    private static void PerformLayoutRecursively(Control control)
    {
        control.PerformLayout();
        foreach (Control child in control.Controls)
        {
            PerformLayoutRecursively(child);
        }
    }

    [Fact]
    public void OpenBackupFolder_ReportsNoError_WhenShellReusesAnExistingProcess()
    {
        string root = Path.Combine(Path.GetTempPath(), $"codex-provider-ui-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            // Explorer commonly satisfies a folder request through a window it
            // already owns and then returns no Process handle. The boundary must
            // treat that as a successful open, not a failure.
            RecordingPlatformBoundary boundary = new();
            using MainForm form = new(new ExecutionLogService(root), platformBoundary: boundary);
            SetField(form, "_currentStatus", StatusWithBackupRoot(root));

            Invoke(form, "OpenBackupFolder");

            Assert.Equal(root, Assert.Single(boundary.OpenedPaths));
            Assert.DoesNotContain("打开备份目录失败", Field<TextBox>(form, "_logBox").Text);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    // The failure branch of OpenBackupFolder is deliberately not covered here:
    // its catch reports through MessageBox.Show, which has no injection seam in
    // MainForm, so a test driving it would block the runner on a modal dialog.

    private static StatusSnapshot StatusWithBackupRoot(string backupRoot) => new()
    {
        CodexHome = @"C:\Users\user\.codex",
        SqliteAccess = new SqliteAccessInfo(true, "windows", null),
        CurrentProvider = new CurrentProviderInfo("openai", false),
        ConfiguredProviders = ["openai"],
        RolloutCounts = new ProviderCounts(),
        LockedRolloutFiles = [],
        UnreadableRolloutFiles = [],
        EncryptedContentCounts = new ProviderCounts(),
        SqliteCounts = null,
        BackupRoot = backupRoot,
        BackupSummary = new BackupSummary { Count = 0, TotalBytes = 0 }
    };

    private sealed class RecordingPlatformBoundary : IAppPlatformBoundary
    {
        public List<string> OpenedPaths { get; } = [];

        public bool UpdatesEnabled => false;

        public string? PickFolder(IWin32Window owner, FolderPickerRequest request) => null;

        public Task<UpdateCheckResult> CheckForUpdateAsync(
            UpdateService updateService,
            Version currentVersion,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public void OpenPath(string path) => OpenedPaths.Add(path);

        public void StartUpdate(string downloadedExePath, string targetExePath, string expectedSha256) =>
            throw new NotSupportedException();
    }

    private static T Field<T>(MainForm form, string name) where T : class
    {
        return typeof(MainForm)
            .GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)?
            .GetValue(form) as T
            ?? throw new InvalidOperationException($"Unable to read {name}.");
    }

    private static void SetField<T>(MainForm form, string name, T value)
    {
        FieldInfo field = typeof(MainForm).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Unable to find {name}.");
        field.SetValue(form, value);
    }

    private static void Invoke(MainForm form, string name, params object[] arguments)
    {
        MethodInfo method = typeof(MainForm).GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Unable to find {name}.");
        method.Invoke(form, arguments);
    }
}
