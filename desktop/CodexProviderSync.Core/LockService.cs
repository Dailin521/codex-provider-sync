using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace CodexProviderSync.Core;

public sealed class LockService
{
    private const int ProtocolVersion = 2;
    private const int Win32ErrorAlreadyExists = 183;
    private const int Win32ErrorAccessDenied = 5;
    private const int DefaultLockCreateRetryCount = 3;
    private const int DefaultLockCreateRetryDelayMs = 75;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true
    };

    private readonly Func<string, string, Task>? _testHook;

    public LockService()
    {
    }

    internal LockService(Func<string, string, Task>? testHook)
    {
        _testHook = testHook;
    }

    public Task<LockHandle> AcquireLockAsync(
        string codexHome,
        string label = "codex-provider-sync")
    {
        return AcquirePathLockAsync(AppConstants.LockPath(codexHome), label);
    }

    /// <summary>
    /// Acquires an operation lock at an explicit canonical path. Keeping this
    /// primitive path-based lets callers apply the same cross-runtime protocol
    /// to narrower resources (for example a resolved SQLite home) later.
    /// </summary>
    public async Task<LockHandle> AcquirePathLockAsync(
        string lockPath,
        string label = "codex-provider-sync")
    {
        string canonicalPath = Path.GetFullPath(lockPath);
        string parentPath = Path.GetDirectoryName(canonicalPath)
            ?? throw new InvalidOperationException($"Cannot resolve the parent directory for lock {canonicalPath}.");
        string claimsPath = canonicalPath + ".claims";
        Directory.CreateDirectory(parentPath);
        Directory.CreateDirectory(claimsPath);

        LockOwner owner = CreateCurrentOwner(label);
        string claimPath = Path.Combine(claimsPath, owner.InstanceId + ".json");
        string candidatePath = $"{canonicalPath}.candidate.{owner.ProcessId}.{owner.InstanceId}";
        bool claimPublished = false;

        try
        {
            await PublishClaimAsync(claimPath, owner);
            claimPublished = true;
            if (_testHook is not null)
            {
                await _testHook("claim-published", owner.InstanceId);
            }

            await AssertSoleLiveClaimAsync(canonicalPath, claimsPath, claimPath, owner);
            await ReclaimCanonicalIfStaleAsync(canonicalPath);

            // A contender can publish while a stale canonical lock is being
            // quarantined. Re-check immediately before publishing ours. A new
            // protocol contender will see this live claim and withdraw.
            await AssertSoleLiveClaimAsync(canonicalPath, claimsPath, claimPath, owner);

            Directory.CreateDirectory(candidatePath);
            await AtomicFile.WriteAllTextAsync(
                Path.Combine(candidatePath, "owner.json"),
                JsonSerializer.Serialize(owner, JsonOptions));

            try
            {
                Directory.Move(candidatePath, canonicalPath);
            }
            catch (Exception error) when (error is IOException or UnauthorizedAccessException)
            {
                throw LockAlreadyExists(canonicalPath, "another owner published the canonical lock first");
            }

            return new LockHandle(canonicalPath, claimsPath, claimPath, owner.InstanceId);
        }
        catch
        {
            TryDeleteDirectory(candidatePath);
            if (claimPublished)
            {
                await TryDeleteOwnedClaimAsync(claimPath, owner.InstanceId);
            }
            throw;
        }
    }

    private static LockOwner CreateCurrentOwner(string label)
    {
        using Process process = Process.GetCurrentProcess();
        string processStartedAt = FormatUtcSecond(process.StartTime.ToUniversalTime());
        return new LockOwner
        {
            ProtocolVersion = ProtocolVersion,
            Runtime = "dotnet",
            Pid = Environment.ProcessId,
            ProcessId = Environment.ProcessId,
            ProcessStartedAt = processStartedAt,
            InstanceId = Guid.NewGuid().ToString("D"),
            StartedAt = FormatUtcSecond(DateTime.UtcNow),
            Label = label,
            Cwd = Environment.CurrentDirectory,
            CurrentDirectory = Environment.CurrentDirectory
        };
    }

    private static async Task PublishClaimAsync(string claimPath, LockOwner owner)
    {
        string directory = Path.GetDirectoryName(claimPath)!;
        string tempPath = Path.Combine(
            directory,
            $".{Path.GetFileName(claimPath)}.{Environment.ProcessId}.{Guid.NewGuid():N}.tmp");
        try
        {
            byte[] bytes = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false)
                .GetBytes(JsonSerializer.Serialize(owner, JsonOptions));
            await using (FileStream stream = new(
                tempPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                16 * 1024,
                FileOptions.Asynchronous | FileOptions.WriteThrough))
            {
                await stream.WriteAsync(bytes);
                await stream.FlushAsync();
                stream.Flush(flushToDisk: true);
            }
            if (!OperatingSystem.IsWindows())
            {
                File.SetUnixFileMode(
                    tempPath,
                    UnixFileMode.UserRead | UnixFileMode.UserWrite);
            }
            File.Move(tempPath, claimPath, overwrite: false);
        }
        finally
        {
            TryDeleteFile(tempPath);
        }
    }

    private static async Task AssertSoleLiveClaimAsync(
        string canonicalPath,
        string claimsPath,
        string ownClaimPath,
        LockOwner ownOwner)
    {
        foreach (string candidate in Directory
            .EnumerateFiles(claimsPath, "*.json", SearchOption.TopDirectoryOnly)
            .Order(StringComparer.Ordinal))
        {
            if (PathComparer.Equals(Path.GetFullPath(candidate), Path.GetFullPath(ownClaimPath)))
            {
                OwnerReadResult ownRead = await ReadOwnerAsync(candidate, requireVersionTwo: true);
                if (!ownRead.Valid
                    || !string.Equals(ownRead.Owner!.InstanceId, ownOwner.InstanceId, StringComparison.OrdinalIgnoreCase))
                {
                    throw LockAlreadyExists(canonicalPath, "this process's claim identity changed before acquisition");
                }
                continue;
            }

            OwnerReadResult read = await ReadOwnerAsync(candidate, requireVersionTwo: true);
            if (!read.Valid)
            {
                throw LockAlreadyExists(canonicalPath, $"claim {candidate} cannot be verified and is retained fail-closed");
            }
            if (!string.Equals(
                    Path.GetFileNameWithoutExtension(candidate),
                    read.Owner!.InstanceId,
                    StringComparison.OrdinalIgnoreCase))
            {
                throw LockAlreadyExists(canonicalPath, $"claim {candidate} does not match its owner instanceId");
            }

            if (IsOwnerLive(read.Owner))
            {
                throw LockAlreadyExists(canonicalPath, $"another live claim ({read.Owner!.InstanceId}) exists");
            }

            if (!await TryQuarantineAndDeleteStaleClaimAsync(candidate, read.Owner!))
            {
                throw LockAlreadyExists(canonicalPath, $"stale claim {candidate} changed while it was being reclaimed");
            }
        }

        // Enumeration is a snapshot. Check once more for a claim published
        // during cleanup. Any other remaining final claim is conservatively a
        // contender; it will independently observe this live claim as well.
        string? otherClaim = Directory
            .EnumerateFiles(claimsPath, "*.json", SearchOption.TopDirectoryOnly)
            .FirstOrDefault(path => !PathComparer.Equals(
                Path.GetFullPath(path),
                Path.GetFullPath(ownClaimPath)));
        if (otherClaim is not null)
        {
            throw LockAlreadyExists(canonicalPath, $"a concurrent claim appeared at {otherClaim}");
        }
    }

    private static async Task<bool> TryQuarantineAndDeleteStaleClaimAsync(
        string claimPath,
        LockOwnerSnapshot expectedOwner)
    {
        string quarantinePath = $"{claimPath}.stale.{Environment.ProcessId}.{Guid.NewGuid():N}";
        try
        {
            File.Move(claimPath, quarantinePath, overwrite: false);
        }
        catch (FileNotFoundException)
        {
            return true;
        }
        catch (DirectoryNotFoundException)
        {
            return true;
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            return false;
        }

        OwnerReadResult moved = await ReadOwnerAsync(quarantinePath, requireVersionTwo: true);
        if (!moved.Valid || !SameOwnerGeneration(moved.Owner!, expectedOwner))
        {
            TryRestoreFile(quarantinePath, claimPath);
            return false;
        }

        TryDeleteFile(quarantinePath);
        return !File.Exists(quarantinePath);
    }

    private async Task ReclaimCanonicalIfStaleAsync(string canonicalPath)
    {
        if (!Directory.Exists(canonicalPath))
        {
            if (File.Exists(canonicalPath))
            {
                throw LockAlreadyExists(canonicalPath, "the canonical lock path is not a directory");
            }
            return;
        }

        string ownerPath = Path.Combine(canonicalPath, "owner.json");
        OwnerReadResult read = await ReadOwnerAsync(ownerPath, requireVersionTwo: false);
        if (!read.Valid)
        {
            throw LockAlreadyExists(canonicalPath, "owner.json cannot be verified and is retained fail-closed");
        }
        if (IsOwnerLive(read.Owner!))
        {
            throw LockAlreadyExists(canonicalPath, $"PID {read.Owner!.ProcessId} is still the verified owner");
        }
        if (_testHook is not null)
        {
            await _testHook("before-stale-canonical-reclaim", read.Owner!.InstanceId ?? string.Empty);
        }

        string quarantinePath = $"{canonicalPath}.stale.{Environment.ProcessId}.{Guid.NewGuid():N}";
        try
        {
            Directory.Move(canonicalPath, quarantinePath);
        }
        catch (DirectoryNotFoundException)
        {
            return;
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            throw LockAlreadyExists(canonicalPath, "the canonical lock changed during stale-owner reclamation");
        }

        OwnerReadResult moved = await ReadOwnerAsync(
            Path.Combine(quarantinePath, "owner.json"),
            requireVersionTwo: false);
        if (!moved.Valid || !SameOwnerGeneration(moved.Owner!, read.Owner!))
        {
            bool restored = TryRestoreDirectory(quarantinePath, canonicalPath);
            throw LockAlreadyExists(
                canonicalPath,
                restored
                    ? "the owner changed during reclamation, so the moved lock was restored"
                    : $"the owner changed during reclamation; it is preserved at {quarantinePath}");
        }

        TryDeleteDirectory(quarantinePath);
        if (Directory.Exists(quarantinePath))
        {
            throw new IOException($"Unable to remove quarantined stale lock {quarantinePath}.");
        }
    }

    private static async Task<OwnerReadResult> ReadOwnerAsync(
        string ownerPath,
        bool requireVersionTwo)
    {
        string text;
        try
        {
            text = await File.ReadAllTextAsync(ownerPath);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            return new OwnerReadResult(null, error.Message);
        }

        try
        {
            using JsonDocument document = JsonDocument.Parse(text);
            JsonElement root = document.RootElement;
            int? protocolVersion = TryReadInt(root, "protocolVersion");
            int? pid = TryReadInt(root, "pid");
            int? processId = TryReadInt(root, "processId");
            if (pid is not null && processId is not null && pid != processId)
            {
                return new OwnerReadResult(null, "pid and processId disagree");
            }
            int? effectivePid = pid ?? processId;
            if (effectivePid is null || effectivePid <= 0)
            {
                return new OwnerReadResult(null, "process identity is missing");
            }

            string? instanceId = TryReadString(root, "instanceId");
            string? processStartedAtText = TryReadString(root, "processStartedAt");
            DateTimeOffset? processStartedAt = null;
            if (processStartedAtText is not null)
            {
                if (!DateTimeOffset.TryParse(
                        processStartedAtText,
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                        out DateTimeOffset parsed))
                {
                    return new OwnerReadResult(null, "processStartedAt is invalid");
                }
                processStartedAt = TruncateToUtcSecond(parsed);
            }
            string? processStartMarker = TryReadString(root, "processStartMarker");

            if (protocolVersion is not null
                && protocolVersion is not (1 or ProtocolVersion))
            {
                return new OwnerReadResult(null, "owner protocol version is unsupported");
            }
            if (requireVersionTwo && protocolVersion != ProtocolVersion)
            {
                return new OwnerReadResult(null, "version 2 owner identity is required");
            }
            if (protocolVersion == ProtocolVersion
                && (pid is null
                    || processId is null
                    || string.IsNullOrWhiteSpace(instanceId)
                    || processStartedAt is null))
            {
                return new OwnerReadResult(null, "version 2 owner identity is incomplete");
            }
            if (processStartedAt is null && string.IsNullOrWhiteSpace(processStartMarker))
            {
                // Legacy records without a process start identity can only be
                // treated as live when their PID exists; they are never
                // reclaimed on a PID-reuse guess.
                processStartMarker = null;
            }

            return new OwnerReadResult(new LockOwnerSnapshot(
                protocolVersion ?? 0,
                effectivePid.Value,
                processStartedAt,
                processStartMarker,
                instanceId,
                text), null);
        }
        catch (Exception error) when (error is JsonException or InvalidOperationException)
        {
            return new OwnerReadResult(null, error.Message);
        }
    }

    private static bool IsOwnerLive(LockOwnerSnapshot owner)
    {
        try
        {
            using Process process = Process.GetProcessById(owner.ProcessId);
            if (process.HasExited)
            {
                return false;
            }

            if (owner.ProcessStartedAt is not null)
            {
                DateTimeOffset actual = TruncateToUtcSecond(process.StartTime.ToUniversalTime());
                return actual == owner.ProcessStartedAt.Value;
            }

            if (!string.IsNullOrWhiteSpace(owner.ProcessStartMarker))
            {
                bool? markerMatches = TryMatchLegacyProcessStartMarker(process, owner.ProcessStartMarker);
                return markerMatches ?? true;
            }

            // A legacy live PID without a comparable start identity is kept
            // fail-closed so PID reuse can never delete an active lock.
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
        catch (System.ComponentModel.Win32Exception)
        {
            return true;
        }
    }

    private static bool? TryMatchLegacyProcessStartMarker(Process process, string marker)
    {
        if (marker.StartsWith("windows:", StringComparison.Ordinal))
        {
            string ticksText = marker["windows:".Length..];
            return long.TryParse(ticksText, CultureInfo.InvariantCulture, out long ticks)
                ? process.StartTime.ToUniversalTime().Ticks == ticks
                : null;
        }

        if (marker.StartsWith("linux:", StringComparison.Ordinal) && OperatingSystem.IsLinux())
        {
            try
            {
                string stat = File.ReadAllText($"/proc/{process.Id}/stat");
                string bootId = File.ReadAllText("/proc/sys/kernel/random/boot_id").Trim();
                int closeParen = stat.LastIndexOf(')');
                if (closeParen < 0)
                {
                    return null;
                }
                string[] fields = stat[(closeParen + 1)..]
                    .Trim()
                    .Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (fields.Length <= 19)
                {
                    return null;
                }
                return string.Equals(
                    marker,
                    $"linux:{bootId}:{fields[19]}",
                    StringComparison.Ordinal);
            }
            catch
            {
                return null;
            }
        }

        int separator = marker.IndexOf(':');
        if (separator > 0
            && DateTimeOffset.TryParse(
                marker[(separator + 1)..],
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeLocal,
                out DateTimeOffset parsed))
        {
            return TruncateToUtcSecond(parsed) == TruncateToUtcSecond(process.StartTime.ToUniversalTime());
        }
        return null;
    }

    private static bool SameOwnerGeneration(LockOwnerSnapshot left, LockOwnerSnapshot right)
    {
        if (!string.IsNullOrWhiteSpace(left.InstanceId)
            || !string.IsNullOrWhiteSpace(right.InstanceId))
        {
            return string.Equals(left.InstanceId, right.InstanceId, StringComparison.OrdinalIgnoreCase)
                && left.ProcessId == right.ProcessId
                && left.ProcessStartedAt == right.ProcessStartedAt
                && string.Equals(left.ProcessStartMarker, right.ProcessStartMarker, StringComparison.Ordinal);
        }
        return string.Equals(left.RawText, right.RawText, StringComparison.Ordinal);
    }

    private static async Task<bool> TryDeleteOwnedClaimAsync(string claimPath, string instanceId)
    {
        if (!File.Exists(claimPath))
        {
            return true;
        }
        OwnerReadResult read = await ReadOwnerAsync(claimPath, requireVersionTwo: true);
        if (!read.Valid
            || !string.Equals(read.Owner!.InstanceId, instanceId, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }
        try
        {
            File.Delete(claimPath);
            return true;
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            return false;
        }
    }

    internal static async ValueTask ReleaseAsync(
        string canonicalPath,
        string claimsPath,
        string claimPath,
        string instanceId)
    {
        if (!Directory.Exists(canonicalPath))
        {
            if (!await TryDeleteOwnedClaimAsync(claimPath, instanceId))
            {
                throw new InvalidOperationException(
                    $"Refusing to delete claim {claimPath} because its owner identity changed.");
            }
            return;
        }

        OwnerReadResult current = await ReadOwnerAsync(
            Path.Combine(canonicalPath, "owner.json"),
            requireVersionTwo: true);
        if (!current.Valid
            || !string.Equals(current.Owner!.InstanceId, instanceId, StringComparison.OrdinalIgnoreCase))
        {
            await TryDeleteOwnedClaimAsync(claimPath, instanceId);
            throw new InvalidOperationException(
                $"Refusing to release lock {canonicalPath} because its owner identity changed.");
        }

        string releasePath = $"{canonicalPath}.release.{Environment.ProcessId}.{Guid.NewGuid():N}";
        try
        {
            Directory.Move(canonicalPath, releasePath);
        }
        catch (DirectoryNotFoundException)
        {
            if (!await TryDeleteOwnedClaimAsync(claimPath, instanceId))
            {
                throw new InvalidOperationException(
                    $"Refusing to delete claim {claimPath} because its owner identity changed.");
            }
            return;
        }

        OwnerReadResult moved = await ReadOwnerAsync(
            Path.Combine(releasePath, "owner.json"),
            requireVersionTwo: true);
        if (!moved.Valid
            || !string.Equals(moved.Owner!.InstanceId, instanceId, StringComparison.OrdinalIgnoreCase))
        {
            bool restored = TryRestoreDirectory(releasePath, canonicalPath);
            await TryDeleteOwnedClaimAsync(claimPath, instanceId);
            throw new InvalidOperationException(
                restored
                    ? $"Refusing to release lock {canonicalPath} because its owner identity changed; the lock was restored."
                    : $"Refusing to release lock {canonicalPath} because its owner identity changed; it is preserved at {releasePath}.");
        }

        Directory.Delete(releasePath, recursive: true);
        if (!await TryDeleteOwnedClaimAsync(claimPath, instanceId))
        {
            throw new InvalidOperationException(
                $"Released canonical lock {canonicalPath}, but refused to delete claim {claimPath} because its owner identity changed.");
        }

        _ = claimsPath; // The sibling claims directory intentionally persists.
    }

    private static InvalidOperationException LockAlreadyExists(string lockPath, string reason)
    {
        return new InvalidOperationException(
            $"Lock already exists at {lockPath}: {reason}. Close Codex/App and retry; do not remove it unless the recorded owner is known to be gone.");
    }

    private static int? TryReadInt(JsonElement root, string name)
    {
        return root.TryGetProperty(name, out JsonElement value)
            && value.ValueKind == JsonValueKind.Number
            && value.TryGetInt32(out int parsed)
                ? parsed
                : null;
    }

    private static string? TryReadString(JsonElement root, string name)
    {
        return root.TryGetProperty(name, out JsonElement value)
            && value.ValueKind == JsonValueKind.String
                ? value.GetString()
                : null;
    }

    private static string FormatUtcSecond(DateTime value)
    {
        return TruncateToUtcSecond(value).ToString(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            CultureInfo.InvariantCulture);
    }

    internal static string CurrentProcessStartedAtForTests()
    {
        using Process process = Process.GetCurrentProcess();
        return FormatUtcSecond(process.StartTime.ToUniversalTime());
    }

    internal static string? CurrentProcessStartMarkerForTests()
    {
        using Process process = Process.GetCurrentProcess();
        if (OperatingSystem.IsWindows())
        {
            return $"windows:{process.StartTime.ToUniversalTime().Ticks}";
        }
        if (OperatingSystem.IsLinux())
        {
            string stat = File.ReadAllText($"/proc/{process.Id}/stat");
            string bootId = File.ReadAllText("/proc/sys/kernel/random/boot_id").Trim();
            int closeParen = stat.LastIndexOf(')');
            string[] fields = stat[(closeParen + 1)..]
                .Trim()
                .Split(' ', StringSplitOptions.RemoveEmptyEntries);
            return $"linux:{bootId}:{fields[19]}";
        }
        if (OperatingSystem.IsMacOS())
        {
            return "darwin:" + process.StartTime.ToString(
                "ddd MMM d HH:mm:ss yyyy",
                CultureInfo.InvariantCulture);
        }
        return null;
    }

    private static DateTimeOffset TruncateToUtcSecond(DateTime value)
    {
        return TruncateToUtcSecond(new DateTimeOffset(value.ToUniversalTime()));
    }

    private static DateTimeOffset TruncateToUtcSecond(DateTimeOffset value)
    {
        DateTimeOffset utc = value.ToUniversalTime();
        return new DateTimeOffset(
            utc.Ticks - (utc.Ticks % TimeSpan.TicksPerSecond),
            TimeSpan.Zero);
    }

    private static bool TryRestoreDirectory(string source, string destination)
    {
        try
        {
            if (Directory.Exists(destination) || File.Exists(destination))
            {
                return false;
            }
            Directory.Move(source, destination);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static void TryRestoreFile(string source, string destination)
    {
        try
        {
            if (!File.Exists(destination))
            {
                File.Move(source, destination, overwrite: false);
            }
        }
        catch
        {
            // Preserve the quarantined claim for diagnosis if restoration is
            // not possible. Never delete an identity that failed validation.
        }
    }

    private static void TryDeleteFile(string path)
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
            // Best effort cleanup must not hide the ownership decision.
        }
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
        catch
        {
            // Best effort cleanup must not hide the ownership decision.
        }
    }

    private static StringComparer PathComparer => OperatingSystem.IsWindows()
        ? StringComparer.OrdinalIgnoreCase
        : StringComparer.Ordinal;

    internal static async Task CreateLockDirectoryAsync(
        string lockPath,
        int retryCount = DefaultLockCreateRetryCount,
        int retryDelayMs = DefaultLockCreateRetryDelayMs,
        Func<int, Task>? delayAsync = null,
        Func<string, int>? tryCreateDirectory = null)
    {
        delayAsync ??= static delay => Task.Delay(delay);
        tryCreateDirectory ??= TryCreateDirectory;

        int attempts = 0;
        while (true)
        {
            int errorCode = tryCreateDirectory(lockPath);
            if (errorCode == 0)
            {
                return;
            }

            if (errorCode == Win32ErrorAlreadyExists)
            {
                throw LockAlreadyExists(lockPath, "the canonical directory already exists");
            }

            if (!IsTransientLockCreateError(errorCode) || attempts >= retryCount)
            {
                throw new IOException($"Unable to create lock directory at {lockPath}. Win32 error: {errorCode}");
            }

            attempts += 1;
            await delayAsync(retryDelayMs);
        }
    }

    private static bool IsTransientLockCreateError(int errorCode)
    {
        return errorCode == Win32ErrorAccessDenied;
    }

    private static int TryCreateDirectory(string lockPath)
    {
        return OperatingSystem.IsWindows()
            ? TryCreateDirectoryWindows(lockPath)
            : TryCreateDirectoryUnix(lockPath);
    }

    private static int TryCreateDirectoryWindows(string lockPath)
    {
        return CreateDirectory(lockPath, IntPtr.Zero) ? 0 : Marshal.GetLastWin32Error();
    }

    private static int TryCreateDirectoryUnix(string lockPath)
    {
        if (Mkdir(lockPath, 448) == 0)
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

    private sealed class LockOwner
    {
        public required int ProtocolVersion { get; init; }
        public required string Runtime { get; init; }
        public required int Pid { get; init; }
        public required int ProcessId { get; init; }
        public required string ProcessStartedAt { get; init; }
        public required string InstanceId { get; init; }
        public required string StartedAt { get; init; }
        public required string Label { get; init; }
        public required string Cwd { get; init; }
        public required string CurrentDirectory { get; init; }
    }

    private sealed record LockOwnerSnapshot(
        int ProtocolVersion,
        int ProcessId,
        DateTimeOffset? ProcessStartedAt,
        string? ProcessStartMarker,
        string? InstanceId,
        string RawText);

    private sealed record OwnerReadResult(LockOwnerSnapshot? Owner, string? Error)
    {
        public bool Valid => Owner is not null;
    }
}

public sealed class LockHandle : IAsyncDisposable
{
    private readonly string _canonicalPath;
    private readonly string _claimsPath;
    private readonly string _claimPath;
    private readonly string _instanceId;
    private bool _released;

    internal LockHandle(
        string canonicalPath,
        string claimsPath,
        string claimPath,
        string instanceId)
    {
        _canonicalPath = canonicalPath;
        _claimsPath = claimsPath;
        _claimPath = claimPath;
        _instanceId = instanceId;
    }

    public string LockPath => _canonicalPath;

    public string InstanceId => _instanceId;

    public async ValueTask DisposeAsync()
    {
        if (_released)
        {
            return;
        }

        await LockService.ReleaseAsync(
            _canonicalPath,
            _claimsPath,
            _claimPath,
            _instanceId);
        _released = true;
    }
}
