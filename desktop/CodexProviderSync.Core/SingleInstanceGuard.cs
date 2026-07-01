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
    private const int Win32ErrorSharingViolation = 32;
    private const int Win32ErrorLockViolation = 33;
    // The previous default budget (3 × 75 ms = 225 ms) was too tight for
    // Windows Defender / OneDrive / antivirus file-locking drivers which
    // can hold the directory open for several seconds on first launch.
    // 30 × 100 ms = 3 s gives the OS enough breathing room to release
    // the lock without making genuine failures drag on indefinitely.
    private const int DefaultCreateRetryCount = 30;
    private const int DefaultCreateRetryDelayMs = 100;
    private const int RaceWindowRetryCount = 20;
    private const int RaceWindowRetryDelayMs = 25;

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
        : this(isProcessAlive, DefaultLockDirectory())
    {
    }

    /// <summary>
    /// Test-only constructor: lets callers inject the lock directory and the
    /// process probe. Use this to point the guard at a temp folder instead
    /// of the user's real <c>%APPDATA%/codex-provider-sync/singleton</c>.
    /// </summary>
    internal SingleInstanceGuard(Func<int, bool> isProcessAlive, string lockDirectory)
    {
        IsProcessAlive = isProcessAlive;
        LockDirectory = lockDirectory;
    }

    internal Func<int, bool> IsProcessAlive { get; }

    public string LockDirectory { get; }

    private static string DefaultLockDirectory()
    {
        return Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "codex-provider-sync",
            "singleton");
    }

    public SingleInstanceAcquisition Acquire(string label = "codex-provider-sync")
    {
        // The lock directory itself is the contested resource, so it
        // must be created atomically below. Only its parent path
        // should be pre-created — using `Directory.CreateDirectory`
        // on the lock directory here would silently succeed on the
        // first launch, which then makes the atomic `TryCreateDirectory`
        // always report `Win32ErrorAlreadyExists` and route the first
        // caller into the stale-lock recovery branch. Two concurrent
        // launches could then race deleting each other's lock dir and
        // violate the single-instance guarantee.
        string? parentDirectory = Path.GetDirectoryName(LockDirectory);
        if (!string.IsNullOrEmpty(parentDirectory))
        {
            Directory.CreateDirectory(parentDirectory);
        }

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

            // If the lock dir exists but `owner.json` is missing
            // we have two possible cases:
//   (a) race window — the winning process atomically created
//       the lock dir but has not yet finished writing
//       `owner.json`. Do NOT delete the directory here; that
//       would race the winner's write and produce a
//       stale-lock IOException in both processes. Back off
//       briefly and let the winner finish.
//   (b) stale leftover — the previous owner died (or crashed)
//       and left behind arbitrary files (e.g., an unowned
//       `BLOCK` file from an interrupted cleanup). The lock
//       directory contains entries that are not `owner.json`,
//       so we can safely delete and reclaim.
// We disambiguate by checking the lock dir's contents: an
// empty lock dir signals the race window; a non-empty lock
// dir with no `owner.json` signals stale leftover. We give
// the race-window case a much larger retry budget (20 × 25 ms
// = 500 ms) than the default because the winner's write of a
// few-KB JSON file under anti-virus / filesystem contention
// can easily exceed a couple hundred milliseconds on Windows.
if (owner is null)
            {
                bool lockDirHasLeftovers = HasLockDirLeftovers();
                if (!lockDirHasLeftovers)
                {
                    attempts += 1;
                    if (attempts >= RaceWindowRetryCount)
                    {
                        throw new IOException(
                            $"Single-instance lock at {LockDirectory} exists but its owner metadata has not appeared yet. The owning process may be hung or under load.");
                    }
                    System.Threading.Thread.Sleep(RaceWindowRetryDelayMs);
                    continue;
                }
                // Fall through to the stale-leftover delete branch.
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
            : TryCreateDirectoryPosix(path);
    }

    internal static int TryCreateDirectoryWindows(string path)
    {
        return CreateDirectory(path, IntPtr.Zero) ? 0 : Marshal.GetLastWin32Error();
    }

    internal static int TryCreateDirectoryPosix(string path)
    {
        // 448 = 0700 octal; only the owner needs to read/write/execute the
        // singleton directory.
        if (PosixMkdir(path, 448) == 0)
        {
            return 0;
        }
        int errorCode = Marshal.GetLastWin32Error();
        return errorCode switch
        {
            17 => Win32ErrorAlreadyExists,   // EEXIST
            1 or 13 => Win32ErrorAccessDenied, // EPERM / EACCES
            _ => errorCode
        };
    }

    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
    private static extern bool CreateDirectory(string lpPathName, IntPtr lpSecurityAttributes);

    // Linux: glibc ships mkdir in libc. macOS: the same symbol resolves
    // through libSystem. Pin the EntryPoint and let the runtime pick the
    // right library on each platform via the DllImportResolver. Without
    // this, macOS hits an unresolved-symbol DllNotFoundException the
    // moment we call into this path.
    [DllImport("libc", EntryPoint = "mkdir", SetLastError = true)]
    private static extern int LibcMkdir(string pathname, uint mode);

    [DllImport("libSystem", EntryPoint = "mkdir", SetLastError = true)]
    private static extern int LibSystemMkdir(string pathname, uint mode);

    private static int PosixMkdir(string path, uint mode)
    {
        return OperatingSystem.IsMacOS()
            ? LibSystemMkdir(path, mode)
            : LibcMkdir(path, mode);
    }

    private static bool IsTransientLockCreateError(int errorCode)
    {
        // Access-denied, sharing-violation, and lock-violation are
        // all transient Windows errors that mean "the lock directory
        // is currently held by another process". Anti-virus and
        // cloud-sync drivers often hold these for a few seconds on
        // first launch; retrying through the default budget lets the
        // OS release them without surfacing an error to the user.
        return errorCode == Win32ErrorAccessDenied
            || errorCode == Win32ErrorSharingViolation
            || errorCode == Win32ErrorLockViolation;
    }

    // True when the lock directory exists, `owner.json` is not
    // present, and there is at least one other entry inside the
    // lock directory. Used to tell apart the "race window"
    // (lock dir is empty, winner is mid-write) from the
    // "stale leftover" (previous owner crashed and left
    // arbitrary files behind) cases without trusting timing.
    private bool HasLockDirLeftovers()
    {
        try
        {
            if (!Directory.Exists(LockDirectory))
            {
                return false;
            }
            foreach (string entry in Directory.EnumerateFileSystemEntries(LockDirectory))
            {
                string name = Path.GetFileName(entry);
                if (!string.Equals(name, "owner.json", StringComparison.Ordinal))
                {
                    return true;
                }
            }
            return false;
        }
        catch (IOException)
        {
            // Treat unreadable lock dirs as "leftovers" so the
            // caller can attempt the stale-delete branch rather
            // than spinning in the race-window backoff.
            return true;
        }
        catch (UnauthorizedAccessException)
        {
            return true;
        }
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
            // No process with that id.
            return false;
        }
        catch (InvalidOperationException)
        {
            // The process has already exited by the time we touched it.
            return false;
        }
        catch (System.ComponentModel.Win32Exception)
        {
            // On Linux the owner may belong to another user; we cannot
            // query it, so treat it as "alive" to be safe and avoid
            // stealing the lock out from under another user.
            return true;
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
