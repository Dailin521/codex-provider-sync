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
