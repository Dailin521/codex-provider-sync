using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace CodexProviderSync.Core;

/// <summary>
/// Cross-platform single-instance guard. The first process to call
/// <see cref="Acquire"/> owns the lock; subsequent callers get back a
/// <see cref="SingleInstanceAcquisition"/> with <c>IsOwner == false</c> and
/// the metadata of the existing owner so the caller can route a "focus"
/// request to it.
/// </summary>
public sealed class SingleInstanceGuard
{
    private const int Win32ErrorAlreadyExists = 183;
    private const int Win32ErrorAccessDenied = 5;
    private const int DefaultCreateRetryCount = 3;
    private const int DefaultCreateRetryDelayMs = 75;

    private static readonly JsonSerializerOptions OwnerJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true
    };

    public SingleInstanceGuard() : this(StandardOwnerProbe)
    {
    }

    /// <summary>
    /// Test-only constructor: lets callers inject a probe for "is this PID
    /// still alive?" so unit tests do not depend on a real running process.
    /// </summary>
    internal SingleInstanceGuard(Func<int, bool> isProcessAlive)
    {
        IsProcessAlive = isProcessAlive;
    }

    internal Func<int, bool> IsProcessAlive { get; }

    public string LockDirectory { get; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "codex-provider-sync",
        "singleton");

    public SingleInstanceAcquisition Acquire(string label = "codex-provider-sync")
    {
        Directory.CreateDirectory(LockDirectory);

        int attempts = 0;
        while (true)
        {
            int errorCode = TryCreateDirectory(LockDirectory);
            if (errorCode == 0)
            {
                WriteOwnerMetadata(label);
                return new SingleInstanceAcquisition(
                    isOwner: true,
                    existingOwner: null,
                    lockDirectory: LockDirectory,
                    guard: this);
            }

            if (errorCode != Win32ErrorAlreadyExists)
            {
                if (!IsTransientLockCreateError(errorCode) || attempts >= DefaultCreateRetryCount)
                {
                    throw new IOException(
                        $"Unable to acquire single-instance lock at {LockDirectory}. Win32 error: {errorCode}");
                }
                attempts += 1;
                System.Threading.Thread.Sleep(DefaultCreateRetryDelayMs);
                continue;
            }

            // Lock directory already exists. Inspect the owner and either
            // (a) refuse to start because the owner is still alive, or
            // (b) clean up and retry if the previous owner has died.
            SingleInstanceOwner? owner = ReadOwnerMetadata();
            if (owner is not null
                && owner.ProcessId != Environment.ProcessId
                && IsProcessAlive(owner.ProcessId))
            {
                return new SingleInstanceAcquisition(
                    isOwner: false,
                    existingOwner: owner,
                    lockDirectory: LockDirectory,
                    guard: this);
            }

            // Stale lock (previous owner died or wrote no metadata). Best-effort
            // cleanup; if the delete loses a race, the next iteration will retry.
            try
            {
                Directory.Delete(LockDirectory, recursive: true);
            }
            catch (IOException)
            {
                attempts += 1;
                if (attempts >= DefaultCreateRetryCount)
                {
                    throw new IOException(
                        $"Single-instance lock at {LockDirectory} is held by a stale owner and could not be cleared. Remove the directory manually and retry.");
                }
                System.Threading.Thread.Sleep(DefaultCreateRetryDelayMs);
            }
        }
    }

    private void WriteOwnerMetadata(string label)
    {
        SingleInstanceOwner owner = new()
        {
            ProcessId = Environment.ProcessId,
            StartedAt = DateTimeOffset.UtcNow,
            Label = label,
            CurrentDirectory = Environment.CurrentDirectory
        };
        File.WriteAllText(
            Path.Combine(LockDirectory, "owner.json"),
            JsonSerializer.Serialize(owner, OwnerJsonOptions));
    }

    internal SingleInstanceOwner? ReadOwnerMetadata()
    {
        string path = Path.Combine(LockDirectory, "owner.json");
        if (!File.Exists(path))
        {
            return null;
        }
        try
        {
            string text = File.ReadAllText(path);
            return JsonSerializer.Deserialize<SingleInstanceOwner>(text, OwnerJsonOptions);
        }
        catch (Exception)
        {
            // Corrupt metadata; treat as no owner so the caller can decide.
            return null;
        }
    }

    internal static int TryCreateDirectory(string path)
    {
        return OperatingSystem.IsWindows()
            ? TryCreateDirectoryWindows(path)
            : TryCreateDirectoryUnix(path);
    }

    internal static int TryCreateDirectoryWindows(string path)
    {
        return CreateDirectory(path, IntPtr.Zero) ? 0 : Marshal.GetLastWin32Error();
    }

    internal static int TryCreateDirectoryUnix(string path)
    {
        if (Mkdir(path, 448) == 0)
        {
            return 0;
        }
        int errorCode = Marshal.GetLastWin32Error();
        return errorCode switch
        {
            17 => Win32ErrorAlreadyExists,
            1 or 13 => Win32ErrorAccessDenied,
            _ => errorCode
        };
    }

    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
    private static extern bool CreateDirectory(string lpPathName, IntPtr lpSecurityAttributes);

    [DllImport("libc", SetLastError = true, EntryPoint = "mkdir")]
    private static extern int Mkdir(string pathname, uint mode);

    private static bool IsTransientLockCreateError(int errorCode)
    {
        return errorCode == Win32ErrorAccessDenied;
    }

    internal static bool StandardOwnerProbe(int processId)
    {
        if (processId <= 0)
        {
            return false;
        }
        try
        {
            using Process probe = Process.GetProcessById(processId);
            return !probe.HasExited;
        }
        catch (ArgumentException)
        {
            return false;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }
}

public sealed class SingleInstanceAcquisition : IDisposable
{
    private readonly SingleInstanceGuard _guard;
    private bool _disposed;

    internal SingleInstanceAcquisition(
        bool isOwner,
        SingleInstanceOwner? existingOwner,
        string lockDirectory,
        SingleInstanceGuard guard)
    {
        IsOwner = isOwner;
        ExistingOwner = existingOwner;
        LockDirectory = lockDirectory;
        _guard = guard;
    }

    public bool IsOwner { get; }

    public SingleInstanceOwner? ExistingOwner { get; }

    public string LockDirectory { get; }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;
        if (IsOwner && Directory.Exists(LockDirectory))
        {
            try
            {
                Directory.Delete(LockDirectory, recursive: true);
            }
            catch (IOException)
            {
                // Best-effort cleanup; the OS will release the directory on
                // process exit anyway.
            }
        }
    }
}

public sealed class SingleInstanceOwner
{
    public int ProcessId { get; set; }
    public DateTimeOffset StartedAt { get; set; }
    public string Label { get; set; } = string.Empty;
    public string CurrentDirectory { get; set; } = string.Empty;
}
