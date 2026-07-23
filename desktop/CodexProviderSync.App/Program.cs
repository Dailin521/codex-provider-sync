using CodexProviderSync.Core;

namespace CodexProviderSync.App;

static class Program
{
    [STAThread]
    static void Main(string[] args)
    {
        ExecutionLogService executionLogService = new();
        if (UpdateApplier.TryRun(args, executionLogService))
        {
            return;
        }

        UpdateApplier.CleanupStaleUpdaterDirectories();

        try
        {
            SingleInstanceGuard guard = new();
            using SingleInstanceAcquisition acquisition = guard.Acquire("codex-provider-sync");
            if (!acquisition.IsOwner)
            {
                FocusExistingInstanceAndExit(acquisition);
                return;
            }

            ApplicationConfiguration.Initialize();
            MainForm mainForm = new(executionLogService);
            using FocusRequestServer focusServer = new(mainForm.BringToFront);
            focusServer.Start();
            Application.Run(mainForm);
        }
        catch (Exception error)
        {
            string logDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "codex-provider-sync");
            Directory.CreateDirectory(logDir);
            string logPath = Path.Combine(logDir, "startup-error.log");
            File.WriteAllText(logPath, error.ToString());
            executionLogService.TryAppend(
                $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] 启动失败{Environment.NewLine}{error}",
                out _);
            MessageBox.Show(
                $"Codex Provider Sync 启动失败。{Environment.NewLine}{Environment.NewLine}{error.Message}{Environment.NewLine}{Environment.NewLine}详细信息已写入:{Environment.NewLine}{logPath}",
                "Codex Provider Sync",
                MessageBoxButtons.OK,
                MessageBoxIcon.Error);
        }
    }

    private static void FocusExistingInstanceAndExit(SingleInstanceAcquisition acquisition)
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
