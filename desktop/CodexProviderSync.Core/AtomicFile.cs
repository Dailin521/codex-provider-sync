using System.Text;

namespace CodexProviderSync.Core;

internal static class AtomicFile
{
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
        string tempPath = Path.Combine(
            directory,
            $".{Path.GetFileName(fullPath)}.provider-sync.{Environment.ProcessId}.{Guid.NewGuid():N}.tmp");
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

    internal static async Task ReplaceOpenFileFromTempAsync(FileStream destination, string tempPath)
    {
        string destinationPath = destination.Name;
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

        await destination.DisposeAsync();
        File.Move(tempPath, destinationPath, overwrite: true);
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
