using System.Diagnostics;
using System.Security.Cryptography;

namespace CodexProviderSync.App;

internal static class UpdateApplier
{
    private const string ApplyArgument = "--apply-update";
    private const string UpdaterDirectoryName = "codex-provider-sync-updater";

    public static bool TryRun(string[] args)
    {
        if (args.Length == 0 || !string.Equals(args[0], ApplyArgument, StringComparison.Ordinal))
        {
            return false;
        }

        UpdateArguments? update = null;
        UpdateApplyEngine engine = new(new SystemUpdateRuntime());
        try
        {
            update = ParseArguments(args[1..]);
            engine.Apply(update);
        }
        catch (Exception error)
        {
            engine.TryRestartInstalledApplication(update?.Target);
            string downloadedUpdatePath = update?.Source ?? "unavailable";
            MessageBox.Show(
                $"Codex Provider Sync update failed.\n\n{error.Message}\n\nDownloaded update:\n{downloadedUpdatePath}\n\nThe installed version was restarted when possible. You can manually replace the EXE with the downloaded update.",
                "Codex Provider Sync",
                MessageBoxButtons.OK,
                MessageBoxIcon.Error);
        }

        return true;
    }

    public static void Start(string downloadedExePath, string targetExePath, string expectedSha256)
    {
        CleanupStaleUpdaterDirectories();
        string updaterRoot = UpdaterRoot();
        string updaterDirectory = Path.Combine(updaterRoot, Guid.NewGuid().ToString("N"));
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
        startInfo.ArgumentList.Add("--sha256");
        startInfo.ArgumentList.Add(NormalizeSha256(expectedSha256));
        startInfo.ArgumentList.Add("--target");
        startInfo.ArgumentList.Add(Path.GetFullPath(targetExePath));

        _ = Process.Start(startInfo) ?? throw new InvalidOperationException("Unable to start the update helper.");
    }

    public static void CleanupStaleUpdaterDirectories()
    {
        string root = UpdaterRoot();
        string? currentDirectory = Environment.ProcessPath is { } processPath
            ? Path.GetDirectoryName(Path.GetFullPath(processPath))
            : null;
        try
        {
            CleanupStaleUpdaterDirectories(root, currentDirectory);
        }
        catch
        {
            // Cleanup must never prevent the main application from starting.
        }
    }

    internal static void CleanupStaleUpdaterDirectories(string root, string? currentDirectory)
    {
        if (!Directory.Exists(root))
        {
            return;
        }

        foreach (string directory in Directory.EnumerateDirectories(root))
        {
            if (string.Equals(Path.GetFullPath(directory), currentDirectory, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch
            {
                // A currently exiting helper can still hold its own EXE. A later launch retries cleanup.
            }
        }

        try
        {
            if (!Directory.EnumerateFileSystemEntries(root).Any())
            {
                Directory.Delete(root);
            }
        }
        catch
        {
            // Cleanup must never prevent the main application from starting.
        }
    }

    private static UpdateArguments ParseArguments(string[] args)
    {
        if (args.Length != 8 || args[0] != "--pid" || args[2] != "--source" || args[4] != "--sha256" || args[6] != "--target" ||
            !int.TryParse(args[1], out int parentProcessId) || parentProcessId <= 0)
        {
            throw new ArgumentException("Update helper arguments are invalid.");
        }

        return new UpdateArguments(
            parentProcessId,
            Path.GetFullPath(args[3]),
            Path.GetFullPath(args[7]),
            NormalizeSha256(args[5]));
    }

    private static string UpdaterRoot() => Path.Combine(Path.GetTempPath(), UpdaterDirectoryName);

    private static string NormalizeSha256(string value)
    {
        string hash = value.Trim();
        if (hash.Length != 64 || !hash.All(Uri.IsHexDigit))
        {
            throw new ArgumentException("Expected update SHA-256 is invalid.", nameof(value));
        }

        return hash.ToLowerInvariant();
    }
}

internal sealed record UpdateArguments(int ParentProcessId, string Source, string Target, string ExpectedSha256);

internal sealed class UpdateApplyEngine(IUpdateRuntime runtime)
{
    public void Apply(UpdateArguments arguments)
    {
        if (!runtime.FileExists(arguments.Source))
        {
            throw new FileNotFoundException("Downloaded update file was not found.", arguments.Source);
        }

        if (!runtime.FileExists(arguments.Target))
        {
            throw new FileNotFoundException("Installed application file was not found.", arguments.Target);
        }

        runtime.WaitForProcessExit(arguments.ParentProcessId, TimeSpan.FromSeconds(30));
        VerifySha256(arguments.Source, arguments.ExpectedSha256);

        string replacementPath = arguments.Target + ".update";
        string backupPath = arguments.Target + ".previous";
        try
        {
            TryDelete(backupPath);
            runtime.CopyFile(arguments.Source, replacementPath, overwrite: true);
            VerifySha256(replacementPath, arguments.ExpectedSha256);
            runtime.ReplaceFile(replacementPath, arguments.Target, backupPath);
            try
            {
                runtime.StartProcess(arguments.Target);
            }
            catch (Exception startError)
            {
                try
                {
                    runtime.ReplaceFile(backupPath, arguments.Target, replacementPath);
                }
                catch (Exception rollbackError)
                {
                    throw new AggregateException(
                        "The updated application could not be started and the previous version could not be restored.",
                        startError,
                        rollbackError);
                }

                throw new InvalidOperationException(
                    "The updated application could not be started. The previous version was restored.",
                    startError);
            }

            TryDelete(backupPath);
            TryDelete(arguments.Source);
        }
        finally
        {
            TryDelete(replacementPath);
        }
    }

    public void TryRestartInstalledApplication(string? targetExePath)
    {
        if (string.IsNullOrWhiteSpace(targetExePath) || !runtime.FileExists(targetExePath))
        {
            return;
        }

        try
        {
            runtime.StartProcess(targetExePath);
        }
        catch
        {
            // The update error dialog still gives the user the target and downloaded paths.
        }
    }

    private void VerifySha256(string path, string expectedSha256)
    {
        byte[] expectedHash = Convert.FromHexString(expectedSha256);
        byte[] actualHash = runtime.CalculateSha256(path);
        if (!CryptographicOperations.FixedTimeEquals(expectedHash, actualHash))
        {
            throw new InvalidDataException("The update file no longer matches the published SHA-256 checksum.");
        }
    }

    private void TryDelete(string path)
    {
        try
        {
            if (runtime.FileExists(path))
            {
                runtime.DeleteFile(path);
            }
        }
        catch
        {
            // Update cleanup is best-effort and must not turn a successful replacement into a failure.
        }
    }
}

internal interface IUpdateRuntime
{
    bool FileExists(string path);
    void CopyFile(string source, string destination, bool overwrite);
    void ReplaceFile(string replacement, string target, string backup);
    void DeleteFile(string path);
    byte[] CalculateSha256(string path);
    void WaitForProcessExit(int processId, TimeSpan timeout);
    void StartProcess(string path);
}

internal sealed class SystemUpdateRuntime : IUpdateRuntime
{
    public bool FileExists(string path) => File.Exists(path);

    public void CopyFile(string source, string destination, bool overwrite) => File.Copy(source, destination, overwrite);

    public void ReplaceFile(string replacement, string target, string backup) =>
        File.Replace(replacement, target, backup, ignoreMetadataErrors: true);

    public void DeleteFile(string path) => File.Delete(path);

    public byte[] CalculateSha256(string path)
    {
        using FileStream stream = new(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        return SHA256.HashData(stream);
    }

    public void WaitForProcessExit(int processId, TimeSpan timeout)
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

    public void StartProcess(string path)
    {
        _ = Process.Start(new ProcessStartInfo
        {
            FileName = path,
            UseShellExecute = true
        }) ?? throw new InvalidOperationException($"Unable to start {path}.");
    }
}
