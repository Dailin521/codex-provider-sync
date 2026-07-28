using System.Drawing;
using System.Reflection;
using System.Windows.Forms;

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

    private static T Field<T>(MainForm form, string name) where T : class
    {
        return typeof(MainForm)
            .GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)?
            .GetValue(form) as T
            ?? throw new InvalidOperationException($"Unable to read {name}.");
    }
}
