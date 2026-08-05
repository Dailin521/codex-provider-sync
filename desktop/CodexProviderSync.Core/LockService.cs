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
    private const int Win32ErrorFileExists = 80;
    private const int UnixErrorAlreadyExists = 17;
    private const int Win32ErrorAccessDenied = 5;
    private const int DefaultLockCreateRetryCount = 3;
    private const int DefaultLockCreateRetryDelayMs = 75;
    private const int DefaultOwnedClaimDeleteRetryCount = 3;
    private const int DefaultOwnedClaimDeleteRetryDelayMs = 75;
    private const string BusyErrorDataKey = "codex-provider-sync/error-code";
    public const string OperationBusyErrorCode = "TARGET_BUSY";
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
        SetOwnerOnlyDirectoryMode(claimsPath);

        LockOwner owner = CreateCurrentOwner(label);
        string claimPath = Path.Combine(claimsPath, owner.InstanceId + ".json");
        string candidatePath = $"{canonicalPath}.candidate.{owner.ProcessId}.{owner.InstanceId}";
        string reservationMarkerPath = ReservationMarkerPath(canonicalPath, owner.InstanceId);
        bool claimPublished = false;
        bool canonicalReserved = false;
        bool ownerLinked = false;

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
            SetOwnerOnlyDirectoryMode(candidatePath);
            await AtomicFile.WriteAllTextAsync(
                Path.Combine(candidatePath, "owner.json"),
                JsonSerializer.Serialize(owner, JsonOptions));

            await CreateLockDirectoryAsync(canonicalPath);
            canonicalReserved = true;
            await WriteReservationMarkerAsync(reservationMarkerPath, owner.InstanceId);
            if (_testHook is not null)
            {
                await _testHook("canonical-reserved", owner.InstanceId);
            }

            try
            {
                CreateHardLinkNoReplace(
                    Path.Combine(candidatePath, "owner.json"),
                    Path.Combine(canonicalPath, "owner.json"));
            }
            catch (IOException error) when (IsAlreadyExistsError(error))
            {
                throw LockAlreadyExists(
                    canonicalPath,
                    "another owner populated the canonical reservation before owner.json could be published");
            }
            ownerLinked = true;
            if (!await ReservationMarkerMatchesAsync(reservationMarkerPath, owner.InstanceId))
            {
                throw LockAlreadyExists(
                    canonicalPath,
                    "the canonical reservation changed identity while owner.json was being published");
            }

            TryDeleteDirectory(candidatePath);
            return new LockHandle(canonicalPath, claimsPath, claimPath, owner.InstanceId);
        }
        catch (Exception acquisitionError)
        {
            TryDeleteDirectory(candidatePath);
            if (canonicalReserved && !ownerLinked)
            {
                if (await TryDeleteOwnedReservationMarkerAsync(reservationMarkerPath, owner.InstanceId))
                {
                    TryDeleteEmptyDirectory(canonicalPath);
                }
            }
            if (claimPublished && !ownerLinked)
            {
                OwnedClaimDeleteResult cleanup;
                try
                {
                    cleanup = await DeleteOwnedClaimWithRetriesAsync(
                        claimPath,
                        owner.InstanceId,
                        beforeDeleteAttemptAsync: _testHook is null
                            ? null
                            : () => _testHook("before-owned-claim-delete", owner.InstanceId));
                }
                catch (Exception cleanupError)
                {
                    throw new AggregateException(
                        $"Lock acquisition failed and owned-claim cleanup threw unexpectedly: {acquisitionError.Message} Cleanup failure: {cleanupError.Message}",
                        acquisitionError,
                        cleanupError);
                }
                if (!cleanup.Succeeded)
                {
                    throw new AggregateException(
                        $"Lock acquisition failed and owned-claim cleanup was incomplete: {acquisitionError.Message} Cleanup failure: {cleanup.Failure!.Message}",
                        acquisitionError,
                        cleanup.Failure!);
                }
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
        FileAttributes? attributes = TryGetPathAttributes(canonicalPath);
        if (attributes is null)
        {
            return;
        }
        if ((attributes.Value & FileAttributes.ReparsePoint) != 0)
        {
            throw LockAlreadyExists(canonicalPath, "the canonical lock path is a symbolic link or reparse point");
        }
        if ((attributes.Value & FileAttributes.Directory) == 0)
        {
            throw LockAlreadyExists(canonicalPath, "the canonical lock path is not a directory");
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
            bool restored = await TryRestoreQuarantinedOwnerAsync(
                quarantinePath,
                canonicalPath,
                moved.Owner?.InstanceId);
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
            text = await ReadIdentityTextAsync(ownerPath);
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

    private static async Task<string> ReadIdentityTextAsync(string path)
    {
        await using FileStream stream = OpenIdentityReadStream(path);
        using StreamReader reader = new(
            stream,
            Encoding.UTF8,
            detectEncodingFromByteOrderMarks: true,
            bufferSize: 4096,
            leaveOpen: true);
        return await reader.ReadToEndAsync();
    }

    private static FileStream OpenIdentityReadStream(string path)
    {
        return new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read | FileShare.Delete,
            bufferSize: 4096,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
    }

    internal static FileStream OpenIdentityReadStreamForTests(string path)
    {
        return OpenIdentityReadStream(path);
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

    private static async Task<OwnedClaimDeleteResult> DeleteOwnedClaimWithRetriesAsync(
        string claimPath,
        string instanceId,
        int retryCount = DefaultOwnedClaimDeleteRetryCount,
        int retryDelayMs = DefaultOwnedClaimDeleteRetryDelayMs,
        Func<int, Task>? delayAsync = null,
        Func<Task>? beforeDeleteAttemptAsync = null)
    {
        delayAsync ??= static delay => Task.Delay(delay);
        Exception? lastFailure = null;

        for (int attempt = 0; attempt <= retryCount; attempt += 1)
        {
            try
            {
                if (TryGetPathAttributes(claimPath) is null)
                {
                    return OwnedClaimDeleteResult.Success;
                }

                OwnerReadResult read = await ReadOwnerAsync(claimPath, requireVersionTwo: true);
                if (!read.Valid)
                {
                    throw new IOException(
                        $"Unable to verify owned claim {claimPath}: {read.Error ?? "unknown owner-read failure"}.");
                }

                if (!string.Equals(read.Owner!.InstanceId, instanceId, StringComparison.OrdinalIgnoreCase))
                {
                    return OwnedClaimDeleteResult.Failed(new InvalidOperationException(
                        $"Refusing to delete claim {claimPath}: expected instanceId {instanceId}, but found {read.Owner.InstanceId ?? "<missing>"}."));
                }

                if (beforeDeleteAttemptAsync is not null)
                {
                    await beforeDeleteAttemptAsync();
                }
                File.Delete(claimPath);
                return OwnedClaimDeleteResult.Success;
            }
            catch (Exception error) when (error is IOException or UnauthorizedAccessException)
            {
                lastFailure = error;
            }
            catch (Exception error)
            {
                return OwnedClaimDeleteResult.Failed(new IOException(
                    $"Unable to delete owned claim {claimPath} for instanceId {instanceId}.",
                    error));
            }

            if (attempt < retryCount)
            {
                await delayAsync(retryDelayMs);
            }
        }

        int attempts = retryCount + 1;
        return OwnedClaimDeleteResult.Failed(new IOException(
            $"Unable to delete owned claim {claimPath} for instanceId {instanceId} after {attempts} attempts. Last failure: {lastFailure?.Message ?? "unknown cleanup failure"}",
            lastFailure));
    }

    private static async Task<bool> TryDeleteOwnedClaimAsync(string claimPath, string instanceId)
    {
        return (await DeleteOwnedClaimWithRetriesAsync(claimPath, instanceId)).Succeeded;
    }

    internal static async ValueTask ReleaseAsync(
        string canonicalPath,
        string claimsPath,
        string claimPath,
        string instanceId)
    {
        FileAttributes? canonicalAttributes = TryGetPathAttributes(canonicalPath);
        if (canonicalAttributes is null)
        {
            if (!await TryDeleteOwnedClaimAsync(claimPath, instanceId))
            {
                throw new InvalidOperationException(
                    $"Refusing to delete claim {claimPath} because its owner identity changed.");
            }
            return;
        }
        if ((canonicalAttributes.Value & FileAttributes.ReparsePoint) != 0)
        {
            throw new InvalidOperationException(
                $"Refusing to release lock {canonicalPath} because the canonical path is a symbolic link or reparse point.");
        }
        if ((canonicalAttributes.Value & FileAttributes.Directory) == 0)
        {
            throw new InvalidOperationException(
                $"Refusing to release lock {canonicalPath} because the canonical path is not a directory.");
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
        if (!await ReservationMarkerMatchesAsync(
                ReservationMarkerPath(canonicalPath, instanceId),
                instanceId))
        {
            throw new InvalidOperationException(
                $"Refusing to release lock {canonicalPath} because its reservation identity changed.");
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
            bool restored = await TryRestoreQuarantinedOwnerAsync(
                releasePath,
                canonicalPath,
                moved.Owner?.InstanceId);
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

    public static bool IsOperationBusy(Exception error)
    {
        return error is InvalidOperationException
            && string.Equals(
                error.Data[BusyErrorDataKey] as string,
                OperationBusyErrorCode,
                StringComparison.Ordinal);
    }

    private static InvalidOperationException LockAlreadyExists(string lockPath, string reason)
    {
        InvalidOperationException error = new(
            $"Lock already exists at {lockPath}: {reason}. Close Codex/App and retry; do not remove it unless the recorded owner is known to be gone.");
        error.Data[BusyErrorDataKey] = OperationBusyErrorCode;
        return error;
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

    private static string ReservationMarkerPath(string canonicalPath, string instanceId)
    {
        return Path.Combine(canonicalPath, $".reservation.{instanceId}");
    }

    private static void SetOwnerOnlyDirectoryMode(string path)
    {
        if (!OperatingSystem.IsWindows())
        {
            File.SetUnixFileMode(
                path,
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }
    }

    private static async Task WriteReservationMarkerAsync(string markerPath, string instanceId)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(instanceId);
        await using FileStream stream = new(
            markerPath,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            4096,
            FileOptions.Asynchronous | FileOptions.WriteThrough);
        await stream.WriteAsync(bytes);
        await stream.FlushAsync();
        stream.Flush(flushToDisk: true);
        if (!OperatingSystem.IsWindows())
        {
            File.SetUnixFileMode(
                markerPath,
                UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }
    }

    private static async Task<bool> ReservationMarkerMatchesAsync(
        string markerPath,
        string instanceId)
    {
        try
        {
            return string.Equals(
                await ReadIdentityTextAsync(markerPath),
                instanceId,
                StringComparison.Ordinal);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            return false;
        }
    }

    private static async Task<bool> TryDeleteOwnedReservationMarkerAsync(
        string markerPath,
        string instanceId)
    {
        if (!await ReservationMarkerMatchesAsync(markerPath, instanceId))
        {
            return false;
        }
        TryDeleteFile(markerPath);
        return !File.Exists(markerPath);
    }

    private static FileAttributes? TryGetPathAttributes(string path)
    {
        try
        {
            return File.GetAttributes(path);
        }
        catch (Exception error) when (error is FileNotFoundException or DirectoryNotFoundException)
        {
            return null;
        }
    }

    private static void CreateHardLinkNoReplace(string existingPath, string newPath)
    {
        int result;
        if (OperatingSystem.IsWindows())
        {
            result = CreateHardLinkWindows(newPath, existingPath, IntPtr.Zero)
                ? 0
                : Marshal.GetLastWin32Error();
        }
        else
        {
            result = LinkUnix(existingPath, newPath) == 0
                ? 0
                : Marshal.GetLastWin32Error();
        }
        if (result != 0)
        {
            throw new IOException(
                $"Unable to publish lock owner at {newPath}. OS error: {result}",
                new System.ComponentModel.Win32Exception(result));
        }
    }

    private static bool IsAlreadyExistsError(IOException error)
    {
        return error.InnerException is System.ComponentModel.Win32Exception native
            && native.NativeErrorCode is UnixErrorAlreadyExists
                or Win32ErrorFileExists
                or Win32ErrorAlreadyExists;
    }

    private static async Task<bool> TryRestoreQuarantinedOwnerAsync(
        string source,
        string destination,
        string? instanceId)
    {
        string restorationToken = "restore-" + Guid.NewGuid().ToString("N");
        string restorationMarker = ReservationMarkerPath(destination, restorationToken);
        try
        {
            await CreateLockDirectoryAsync(destination, retryCount: 0);
            await WriteReservationMarkerAsync(restorationMarker, restorationToken);
            CreateHardLinkNoReplace(
                Path.Combine(source, "owner.json"),
                Path.Combine(destination, "owner.json"));

            if (!string.IsNullOrWhiteSpace(instanceId))
            {
                string sourceMarker = ReservationMarkerPath(source, instanceId);
                if (File.Exists(sourceMarker))
                {
                    CreateHardLinkNoReplace(
                        sourceMarker,
                        ReservationMarkerPath(destination, instanceId));
                }
            }
            if (!await TryDeleteOwnedReservationMarkerAsync(restorationMarker, restorationToken))
            {
                return false;
            }
            return true;
        }
        catch
        {
            // Never rename source over destination here: POSIX rename can
            // replace a concurrently-created empty directory. Keep source for
            // diagnosis and remove only a reservation that stayed empty.
            if (await TryDeleteOwnedReservationMarkerAsync(restorationMarker, restorationToken))
            {
                TryDeleteEmptyDirectory(destination);
            }
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

    private static void TryDeleteEmptyDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: false);
            }
        }
        catch
        {
            // A foreign population is retained fail-closed.
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

    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode, EntryPoint = "CreateHardLinkW")]
    private static extern bool CreateHardLinkWindows(
        string lpFileName,
        string lpExistingFileName,
        IntPtr lpSecurityAttributes);

    [DllImport("libc", SetLastError = true, EntryPoint = "mkdir")]
    private static extern int Mkdir(string pathname, uint mode);

    [DllImport("libc", SetLastError = true, EntryPoint = "link")]
    private static extern int LinkUnix(string oldpath, string newpath);

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

    private sealed record OwnedClaimDeleteResult(bool Succeeded, Exception? Failure)
    {
        internal static OwnedClaimDeleteResult Success { get; } = new(true, null);

        internal static OwnedClaimDeleteResult Failed(Exception failure) => new(false, failure);
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
