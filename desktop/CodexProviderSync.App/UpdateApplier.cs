using System.Diagnostics;

namespace CodexProviderSync.App;

internal static class UpdateApplier
{
    private const string ApplyArgument = "--apply-update";

    public static bool TryRun(string[] args)
    {
        if (args.Length == 0 || !string.Equals(args[0], ApplyArgument, StringComparison.Ordinal))
        {
            return false;
        }

        UpdateArguments? update = null;
        try
        {
            update = ParseArguments(args[1..]);
            Apply(update);
        }
        catch (Exception error)
        {
            TryRestartInstalledApplication(update?.Target);
            string downloadedUpdatePath = update?.Source ?? "unavailable";
            MessageBox.Show(
                $"Codex Provider Sync update failed.\n\n{error.Message}\n\nDownloaded update:\n{downloadedUpdatePath}\n\nThe installed version was restarted when possible. You can manually replace the EXE with the downloaded update.",
                "Codex Provider Sync",
                MessageBoxButtons.OK,
                MessageBoxIcon.Error);
        }

        return true;
    }

    public static void Start(string downloadedExePath, string targetExePath)
    {
        string updaterDirectory = Path.Combine(Path.GetTempPath(), "codex-provider-sync-updater", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(updaterDirectory);
        string updaterPath = Path.Combine(updaterDirectory, "CodexProviderSync.Updater.exe");
        File.Copy(Environment.ProcessPath ?? throw new InvalidOperationException("Unable to determine the current executable path."), updaterPath);

        ProcessStartInfo startInfo = new()
        {
            FileName = updaterPath,
            UseShellExecute = true
        };
        startInfo.ArgumentList.Add(ApplyArgument);
        startInfo.ArgumentList.Add("--pid");
        startInfo.ArgumentList.Add(Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture));
        startInfo.ArgumentList.Add("--source");
        startInfo.ArgumentList.Add(Path.GetFullPath(downloadedExePath));
        startInfo.ArgumentList.Add("--target");
        startInfo.ArgumentList.Add(Path.GetFullPath(targetExePath));

        _ = Process.Start(startInfo) ?? throw new InvalidOperationException("Unable to start the update helper.");
    }

    private static void Apply(UpdateArguments arguments)
    {
        if (!File.Exists(arguments.Source))
        {
            throw new FileNotFoundException("Downloaded update file was not found.", arguments.Source);
        }

        if (!File.Exists(arguments.Target))
        {
            throw new FileNotFoundException("Installed application file was not found.", arguments.Target);
        }

        WaitForProcessExit(arguments.ParentProcessId, TimeSpan.FromSeconds(30));
        string replacementPath = arguments.Target + ".update";
        string backupPath = arguments.Target + ".previous";
        try
        {
            File.Copy(arguments.Source, replacementPath, overwrite: true);
            File.Move(arguments.Target, backupPath, overwrite: true);
            try
            {
                File.Move(replacementPath, arguments.Target, overwrite: true);
            }
            catch
            {
                File.Move(backupPath, arguments.Target, overwrite: true);
                throw;
            }

            File.Delete(backupPath);
            File.Delete(arguments.Source);
            Process.Start(new ProcessStartInfo
            {
                FileName = arguments.Target,
                UseShellExecute = true
            });
        }
        finally
        {
            if (File.Exists(replacementPath))
            {
                File.Delete(replacementPath);
            }
        }
    }

    private static void WaitForProcessExit(int processId, TimeSpan timeout)
    {
        try
        {
            using Process parent = Process.GetProcessById(processId);
            if (!parent.WaitForExit((int)timeout.TotalMilliseconds))
            {
                throw new TimeoutException("The existing application did not exit before the update timed out.");
            }
        }
        catch (ArgumentException)
        {
            // The main application exited before the helper queried it.
        }
    }

    private static void TryRestartInstalledApplication(string? targetExePath)
    {
        if (string.IsNullOrWhiteSpace(targetExePath) || !File.Exists(targetExePath))
        {
            return;
        }

        try
        {
            Process.Start(new ProcessStartInfo
            {
                FileName = targetExePath,
                UseShellExecute = true
            });
        }
        catch
        {
            // The update error dialog still gives the user the target and downloaded paths.
        }
    }

    private static UpdateArguments ParseArguments(string[] args)
    {
        if (args.Length != 6 || args[0] != "--pid" || args[2] != "--source" || args[4] != "--target" ||
            !int.TryParse(args[1], out int parentProcessId) || parentProcessId <= 0)
        {
            throw new ArgumentException("Update helper arguments are invalid.");
        }

        return new UpdateArguments(parentProcessId, Path.GetFullPath(args[3]), Path.GetFullPath(args[5]));
    }

    private sealed record UpdateArguments(int ParentProcessId, string Source, string Target);
}
