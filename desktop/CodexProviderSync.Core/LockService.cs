using System.Runtime.InteropServices;
using System.Text.Json;

namespace CodexProviderSync.Core;

public sealed class LockService
{
    public async Task<LockHandle> AcquireLockAsync(string codexHome, string label = "codex-provider-sync")
    {
        string lockPath = AppConstants.LockPath(codexHome);
        Directory.CreateDirectory(Path.GetDirectoryName(lockPath)!);

        if (!CreateDirectory(lockPath, IntPtr.Zero))
        {
            int errorCode = Marshal.GetLastWin32Error();
            if (errorCode == 183)
            {
                throw new InvalidOperationException(
                    $"Lock already exists at {lockPath}. Close Codex/App and retry, or remove the stale lock if you are sure no sync is running.");
            }

            throw new IOException($"Unable to create lock directory at {lockPath}. Win32 error: {errorCode}");
        }

        try
        {
            LockOwner owner = new()
            {
                ProcessId = Environment.ProcessId,
                StartedAt = DateTimeOffset.UtcNow,
                Label = label,
                CurrentDirectory = Environment.CurrentDirectory
            };
            await File.WriteAllTextAsync(
                Path.Combine(lockPath, "owner.json"),
                JsonSerializer.Serialize(owner, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    WriteIndented = true
                }));
            return new LockHandle(lockPath);
        }
        catch
        {
            Directory.Delete(lockPath, recursive: true);
            throw;
        }
    }

    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
    private static extern bool CreateDirectory(string lpPathName, IntPtr lpSecurityAttributes);

    private sealed class LockOwner
    {
        public required int ProcessId { get; init; }
        public required DateTimeOffset StartedAt { get; init; }
        public required string Label { get; init; }
        public required string CurrentDirectory { get; init; }
    }
}

public sealed class LockHandle : IAsyncDisposable
{
    private readonly string _lockPath;
    private bool _released;

    public LockHandle(string lockPath)
    {
        _lockPath = lockPath;
    }

    public ValueTask DisposeAsync()
    {
        if (_released)
        {
            return ValueTask.CompletedTask;
        }

        _released = true;
        if (Directory.Exists(_lockPath))
        {
            Directory.Delete(_lockPath, recursive: true);
        }

        return ValueTask.CompletedTask;
    }
}
