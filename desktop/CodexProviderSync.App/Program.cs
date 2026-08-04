using CodexProviderSync.Core;
using CodexProviderSync.App.Automation;

namespace CodexProviderSync.App;

static class Program
{
    [STAThread]
    static void Main(string[] args)
    {
        // This parser is intentionally the first operation. In particular, do
        // not construct services whose defaults resolve AppData, temp, home, or
        // singleton paths before an automation descriptor has been validated.
        AutomationBootstrap bootstrap;
        try
        {
            bootstrap = AutomationBootstrap.ParseAndClaim(args);
        }
        catch (Exception error)
        {
            Console.Error.WriteLine($"GUI automation bootstrap refused startup: {error.Message}");
            return;
        }

        IAppPathProvider paths = bootstrap.Enabled
            ? new IsolatedAppPathProvider(bootstrap.IsolationRoot!)
            : new SystemAppPathProvider();
        IAppPlatformBoundary platformBoundary = bootstrap.Enabled
            ? new IsolatedAppPlatformBoundary(paths)
            : new SystemAppPlatformBoundary(paths);
        ExecutionLogService executionLogService = new(paths.LogDirectory);
        if (!bootstrap.Enabled && UpdateApplier.TryRun(args, executionLogService))
        {
            return;
        }

        if (!bootstrap.Enabled)
        {
            UpdateApplier.CleanupStaleUpdaterDirectories(paths.UpdaterRoot);
        }

        try
        {
            AppInstanceGuard guard = new(paths);
            using AppInstanceAcquisition acquisition = guard.Acquire();
            if (!acquisition.IsOwner)
            {
                if (!bootstrap.Enabled)
                {
                    FocusExistingInstanceAndExit(acquisition);
                }
                return;
            }

            ApplicationConfiguration.Initialize();
            SettingsService settingsService = new(paths.SettingsPath);
            AutomationIsolation.PrepareSettings(settingsService, paths);
            MainForm mainForm = new(
                executionLogService,
                settingsService,
                paths: paths,
                platformBoundary: platformBoundary);
            using FocusRequestServer? focusServer = bootstrap.Enabled
                ? null
                : new FocusRequestServer(mainForm.RequestBringToFront);
            using GuiAutomationBridge? automationBridge = bootstrap.Enabled
                ? new GuiAutomationBridge(mainForm, bootstrap, paths)
                : null;
            if (automationBridge is not null)
            {
                mainForm.Shown += (_, _) => automationBridge.Start();
            }
            else
            {
                focusServer!.Start();
            }
            System.Windows.Forms.Application.Run(mainForm);
        }
        catch (Exception error)
        {
            string logPath = paths.StartupErrorPath;
            Directory.CreateDirectory(Path.GetDirectoryName(logPath)!);
            File.WriteAllText(logPath, error.ToString());
            executionLogService.TryAppend(
                $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 启动失败{Environment.NewLine}{error}",
                out _);
            if (!bootstrap.Enabled)
            {
                MessageBox.Show(
                    $"Codex Provider Sync 启动失败。{Environment.NewLine}{Environment.NewLine}{error.Message}{Environment.NewLine}{Environment.NewLine}详细信息已写入:{Environment.NewLine}{logPath}",
                    "Codex Provider Sync",
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Error);
            }
            else
            {
                Console.Error.WriteLine($"GUI automation startup failed; isolated log: {logPath}");
            }
        }
    }

    private static void FocusExistingInstanceAndExit(AppInstanceAcquisition acquisition)
    {
        string detail = acquisition.ExistingOwner is { } owner
            ? $"pid={owner.ProcessId}, started={owner.StartedAt:O}"
            : "no owner metadata available";
        Console.WriteLine(
            $"Another Codex Provider Sync instance is already running ({detail}). Forwarding focus request and exiting.");

        try
        {
            using FocusRequestServer client = new(() => { });
            bool delivered = client
                .SendFocusRequestAsync(TimeSpan.FromSeconds(2))
                .GetAwaiter()
                .GetResult();
            if (!delivered)
            {
                Console.WriteLine(
                    "Focus request timed out; the existing instance may be busy. It will be brought to the foreground when it next becomes idle.");
            }
        }
        catch (Exception error)
        {
            Console.Error.WriteLine($"Failed to forward focus request: {error.Message}");
        }
    }
}
