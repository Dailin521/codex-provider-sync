using System.Text;

namespace CodexProviderSync.Core;

internal static class AtomicFile
{
    internal static async Task<bool> CopyAsync(
        string sourcePath,
        string destinationPath,
        bool overwrite,
        CancellationToken cancellationToken = default,
        Func<string, string, string, Task>? faultInjector = null)
    {
        string fullSourcePath = Path.GetFullPath(sourcePath);
        if (!File.Exists(fullSourcePath))
        {
            return false;
        }

        string fullDestinationPath = Path.GetFullPath(destinationPath);
        string? directory = Path.GetDirectoryName(fullDestinationPath);
        if (string.IsNullOrEmpty(directory))
        {
            throw new InvalidOperationException($"Cannot resolve the parent directory for {fullDestinationPath}.");
        }
        if (!overwrite && File.Exists(fullDestinationPath))
        {
            throw new IOException($"The destination file already exists: {fullDestinationPath}");
        }

        Directory.CreateDirectory(directory);
        string tempPath = CreateTempPath(directory, Path.GetFileName(fullDestinationPath));
        UnixFileMode? sourceMode = TryGetUnixMode(fullSourcePath);
        try
        {
            await using (FileStream source = new(
                fullSourcePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete,
                64 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            await using (FileStream destination = new(
                tempPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                64 * 1024,
                FileOptions.Asynchronous | FileOptions.WriteThrough))
            {
                if (faultInjector is not null)
                {
                    await faultInjector("before_stage_write", fullDestinationPath, tempPath);
                }
                await source.CopyToAsync(destination, cancellationToken);
                await destination.FlushAsync(cancellationToken);
                destination.Flush(flushToDisk: true);
            }

            File.SetLastWriteTimeUtc(tempPath, File.GetLastWriteTimeUtc(fullSourcePath));
            ApplyUnixMode(tempPath, sourceMode ?? OwnerReadWriteMode);
            if (faultInjector is not null)
            {
                await faultInjector("before_atomic_replace", fullDestinationPath, tempPath);
            }
            File.Move(tempPath, fullDestinationPath, overwrite);
            return true;
        }
        catch
        {
            TryDelete(tempPath);
            throw;
        }
    }

    internal static async Task WriteAllTextAsync(
        string filePath,
        string content,
        CancellationToken cancellationToken = default,
        Func<string, string, string, Task>? faultInjector = null)
    {
        string fullPath = Path.GetFullPath(filePath);
        string? directory = Path.GetDirectoryName(fullPath);
        if (string.IsNullOrEmpty(directory))
        {
            throw new InvalidOperationException($"Cannot resolve the parent directory for {fullPath}.");
        }

        Directory.CreateDirectory(directory);
        string tempPath = CreateTempPath(directory, Path.GetFileName(fullPath));
        UnixFileMode targetMode = TryGetUnixMode(fullPath) ?? OwnerReadWriteMode;
        try
        {
            byte[] bytes = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false).GetBytes(content);
            await using (FileStream stream = new(
                tempPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                64 * 1024,
                FileOptions.Asynchronous | FileOptions.WriteThrough))
            {
                if (faultInjector is not null)
                {
                    await faultInjector("before_stage_write", fullPath, tempPath);
                }
                await stream.WriteAsync(bytes, cancellationToken);
                await stream.FlushAsync(cancellationToken);
                stream.Flush(flushToDisk: true);
            }
            ApplyUnixMode(tempPath, targetMode);
            if (faultInjector is not null)
            {
                await faultInjector("before_atomic_replace", fullPath, tempPath);
            }
            File.Move(tempPath, fullPath, overwrite: true);
        }
        catch
        {
            TryDelete(tempPath);
            throw;
        }
    }

    private static string CreateTempPath(string directory, string fileName)
    {
        return Path.Combine(
            directory,
            $".{fileName}.provider-sync.{Environment.ProcessId}.{Guid.NewGuid():N}.tmp");
    }

    internal static async Task ReplaceOpenFileFromTempAsync(FileStream destination, string tempPath)
    {
        string destinationPath = destination.Name;
        UnixFileMode targetMode = TryGetUnixMode(destinationPath) ?? OwnerReadWriteMode;
        await using (FileStream staged = new(
            tempPath,
            FileMode.Open,
            FileAccess.ReadWrite,
            FileShare.None,
            64 * 1024,
            FileOptions.Asynchronous | FileOptions.WriteThrough))
        {
            await staged.FlushAsync();
            staged.Flush(flushToDisk: true);
        }

        ApplyUnixMode(tempPath, targetMode);
        await destination.DisposeAsync();
        File.Move(tempPath, destinationPath, overwrite: true);
    }

    private const UnixFileMode OwnerReadWriteMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;

    private static UnixFileMode? TryGetUnixMode(string path)
    {
        if (OperatingSystem.IsWindows() || !File.Exists(path))
        {
            return null;
        }
        return File.GetUnixFileMode(path);
    }

    private static void ApplyUnixMode(string path, UnixFileMode mode)
    {
        if (!OperatingSystem.IsWindows())
        {
            File.SetUnixFileMode(path, mode);
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
            // Cleanup must not hide the original write failure.
        }
    }
}
