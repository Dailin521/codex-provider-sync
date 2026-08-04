using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CodexProviderSync.Application;

/// <summary>
/// Durable, single-use plan ledger for one-shot Application hosts.
/// The caller must provide an explicit, fully-qualified storage root. No
/// Codex Home, user profile, or environment-variable default is consulted.
/// </summary>
/// <remarks>
/// Each transition is an immutable, atomically-published receipt. In
/// particular, a claim receipt is flushed and published before
/// <see cref="TryClaimAsync"/> returns <see cref="ApplicationPlanClaimStatus.Claimed"/>.
/// A process that exits after that point cannot make the plan reusable.
/// This is a local-filesystem, process-crash guarantee. Portable .NET APIs do
/// not provide a cross-platform power-loss or hostile-network-filesystem
/// durability guarantee. The explicitly supplied root is a trust boundary and
/// must not be writable by an untrusted principal.
/// </remarks>
public sealed class FileApplicationPlanLedger : IApplicationPlanLedger
{
    private const int SchemaVersion = 1;
    private const int MaximumRecordBytes = 64 * 1024;
    private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan LockRetryDelay = TimeSpan.FromMilliseconds(10);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        AllowTrailingCommas = false,
        PropertyNameCaseInsensitive = false,
        ReadCommentHandling = JsonCommentHandling.Disallow,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        WriteIndented = false
    };

    private readonly string _entriesDirectory;
    private readonly string _locksDirectory;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _lockTimeout;

    public FileApplicationPlanLedger(
        string ledgerRoot,
        TimeProvider? timeProvider = null,
        TimeSpan? lockTimeout = null)
    {
        if (string.IsNullOrWhiteSpace(ledgerRoot))
        {
            throw new ArgumentException("An explicit plan-ledger root is required.", nameof(ledgerRoot));
        }
        if (!Path.IsPathFullyQualified(ledgerRoot))
        {
            throw new ArgumentException("The plan-ledger root must be fully qualified.", nameof(ledgerRoot));
        }

        LedgerRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(ledgerRoot));
        if (OperatingSystem.IsWindows()
            && LedgerRoot.StartsWith(@"\\", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "A network or WSL UNC path cannot be used as the plan-ledger root.",
                nameof(ledgerRoot));
        }
        string? volumeRoot = Path.GetPathRoot(LedgerRoot);
        if (string.IsNullOrEmpty(volumeRoot)
            || PathsEqual(LedgerRoot, Path.TrimEndingDirectorySeparator(volumeRoot)))
        {
            throw new ArgumentException("A filesystem root cannot be used as the plan-ledger root.", nameof(ledgerRoot));
        }

        _entriesDirectory = GetContainedPath(LedgerRoot, "entries");
        _locksDirectory = GetContainedPath(LedgerRoot, "locks");
        _timeProvider = timeProvider ?? TimeProvider.System;
        _lockTimeout = lockTimeout ?? DefaultLockTimeout;
        if (_lockTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(lockTimeout), "The plan-ledger lock timeout must be positive.");
        }
    }

    public string LedgerRoot { get; }

    public async Task RegisterAsync(
        ApplicationOperationPlan plan,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ValidatePlanId(plan.PlanId);
        ValidateRegistrationPlan(plan);
        cancellationToken.ThrowIfCancellationRequested();

        EnsureStorageLayout();
        await using FileStream planLock = await AcquirePlanLockAsync(plan.PlanId, cancellationToken);
        EnsureStorageLayout();
        LedgerPaths paths = GetPaths(plan.PlanId);

        if (PathExists(paths.Registration))
        {
            RegistrationRecord existing = await ReadRegistrationAsync(
                paths.Registration,
                plan.PlanId,
                cancellationToken);
            if (!FixedTimeEquals(existing.Digest, plan.Digest))
            {
                throw new InvalidOperationException(
                    $"Plan {plan.PlanId} is already registered with a different digest.");
            }

            return;
        }

        string? orphan = FirstExistingPath(paths.Claim, paths.Completion);
        if (orphan is not null)
        {
            throw Corrupt(
                plan.PlanId,
                orphan,
                "A claim or completion receipt exists without its registration record.");
        }

        RegistrationRecord registration = new()
        {
            SchemaVersion = SchemaVersion,
            RecordType = "registration",
            PlanId = plan.PlanId,
            Digest = plan.Digest,
            ProtocolVersion = plan.ProtocolVersion,
            CreatedByOperationId = plan.CreatedByOperationId,
            CreatedAtUtc = plan.CreatedAtUtc.ToUniversalTime(),
            ExpiresAtUtc = plan.ExpiresAtUtc.ToUniversalTime(),
            RegisteredAtUtc = _timeProvider.GetUtcNow().ToUniversalTime()
        };
        await CreateImmutableRecordAsync(paths.Registration, registration, cancellationToken);
    }

    public async Task<ApplicationPlanClaimResult> TryClaimAsync(
        string planId,
        string digest,
        CancellationToken cancellationToken = default)
    {
        ValidatePlanId(planId);
        ArgumentNullException.ThrowIfNull(digest);
        cancellationToken.ThrowIfCancellationRequested();

        EnsureStorageLayout();
        await using FileStream planLock = await AcquirePlanLockAsync(planId, cancellationToken);
        EnsureStorageLayout();
        LedgerPaths paths = GetPaths(planId);

        if (!PathExists(paths.Registration))
        {
            string? orphan = FirstExistingPath(paths.Claim, paths.Completion);
            if (orphan is not null)
            {
                throw Corrupt(
                    planId,
                    orphan,
                    "A claim or completion receipt exists without its registration record.");
            }

            return new ApplicationPlanClaimResult(ApplicationPlanClaimStatus.NotFound);
        }

        RegistrationRecord registration = await ReadRegistrationAsync(
            paths.Registration,
            planId,
            cancellationToken);
        if (!FixedTimeEquals(registration.Digest, digest))
        {
            return new ApplicationPlanClaimResult(ApplicationPlanClaimStatus.DigestMismatch);
        }

        if (PathExists(paths.Claim))
        {
            await ReadClaimAsync(paths.Claim, registration, cancellationToken);
            if (PathExists(paths.Completion))
            {
                await ReadCompletionAsync(paths.Completion, registration, cancellationToken);
            }

            return new ApplicationPlanClaimResult(ApplicationPlanClaimStatus.AlreadyUsed);
        }
        if (PathExists(paths.Completion))
        {
            throw Corrupt(
                planId,
                paths.Completion,
                "A completion receipt exists without its claim receipt.");
        }

        ClaimRecord claim = new()
        {
            SchemaVersion = SchemaVersion,
            RecordType = "claim",
            PlanId = planId,
            Digest = registration.Digest,
            ClaimedAtUtc = _timeProvider.GetUtcNow().ToUniversalTime()
        };
        bool published = await TryCreateClaimRecordAsync(paths.Claim, claim, cancellationToken);
        if (!published)
        {
            // FileMode.CreateNew is the authoritative cross-process claim
            // primitive. This branch also protects single-use semantics if a
            // platform does not honor FileShare.None exactly as expected.
            await ReadClaimAsync(paths.Claim, registration, cancellationToken);
            return new ApplicationPlanClaimResult(ApplicationPlanClaimStatus.AlreadyUsed);
        }

        // Do not observe cancellation after publishing the claim. Once this
        // method can report Claimed, the receipt is already durable and the
        // caller must treat the plan as permanently consumed.
        return new ApplicationPlanClaimResult(ApplicationPlanClaimStatus.Claimed);
    }

    public async Task CompleteAsync(
        string planId,
        ApplicationOperationLifecycle lifecycle,
        CancellationToken cancellationToken = default)
    {
        ValidatePlanId(planId);
        string lifecycleName = ToTerminalLifecycleName(lifecycle);
        cancellationToken.ThrowIfCancellationRequested();

        EnsureStorageLayout();
        await using FileStream planLock = await AcquirePlanLockAsync(planId, cancellationToken);
        EnsureStorageLayout();
        LedgerPaths paths = GetPaths(planId);

        if (!PathExists(paths.Registration))
        {
            string? evidence = FirstExistingPath(paths.Claim, paths.Completion);
            if (evidence is not null)
            {
                throw Corrupt(
                    planId,
                    evidence,
                    "A claim or completion receipt exists without its registration record.");
            }

            throw new InvalidOperationException($"Plan {planId} is not registered.");
        }

        RegistrationRecord registration = await ReadRegistrationAsync(
            paths.Registration,
            planId,
            cancellationToken);
        if (!PathExists(paths.Claim))
        {
            if (PathExists(paths.Completion))
            {
                throw Corrupt(
                    planId,
                    paths.Completion,
                    "A completion receipt exists without its claim receipt.");
            }

            throw new InvalidOperationException($"Plan {planId} has not been claimed.");
        }

        await ReadClaimAsync(paths.Claim, registration, cancellationToken);
        if (PathExists(paths.Completion))
        {
            CompletionRecord existing = await ReadCompletionAsync(
                paths.Completion,
                registration,
                cancellationToken);
            if (!string.Equals(existing.Lifecycle, lifecycleName, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Plan {planId} is already completed as {existing.Lifecycle} and cannot transition to {lifecycleName}.");
            }

            return;
        }

        CompletionRecord completion = new()
        {
            SchemaVersion = SchemaVersion,
            RecordType = "completion",
            PlanId = planId,
            Digest = registration.Digest,
            Lifecycle = lifecycleName,
            CompletedAtUtc = _timeProvider.GetUtcNow().ToUniversalTime()
        };
        await CreateImmutableRecordAsync(paths.Completion, completion, cancellationToken);
    }

    private static void ValidateRegistrationPlan(ApplicationOperationPlan plan)
    {
        if (!string.Equals(plan.ProtocolVersion, ApplicationProtocol.Version, StringComparison.Ordinal))
        {
            throw new ArgumentException("The plan protocol version is not supported.", nameof(plan));
        }
        if (string.IsNullOrWhiteSpace(plan.CreatedByOperationId))
        {
            throw new ArgumentException("The plan creator operation id is required.", nameof(plan));
        }
        if (plan.ExpiresAtUtc <= plan.CreatedAtUtc)
        {
            throw new ArgumentException("The plan expiry must be later than its creation time.", nameof(plan));
        }

        ValidateDigest(plan.Digest, nameof(plan));
    }

    private static void ValidatePlanId(string? planId)
    {
        if (string.IsNullOrEmpty(planId)
            || planId.Length > 128
            || !IsAsciiLetterOrDigit(planId[0])
            || planId.Any(static character =>
                !IsAsciiLetterOrDigit(character)
                && character != '-'
                && character != '_'))
        {
            throw new ArgumentException(
                "A plan id must contain 1-128 ASCII letters, digits, hyphens, or underscores and must start with a letter or digit.",
                nameof(planId));
        }
    }

    private static bool IsAsciiLetterOrDigit(char value)
    {
        return value is >= 'a' and <= 'z'
            or >= 'A' and <= 'Z'
            or >= '0' and <= '9';
    }

    private static void ValidateDigest(string? digest, string parameterName)
    {
        if (digest is null
            || digest.Length != 64
            || digest.Any(static character =>
                character is not (>= '0' and <= '9')
                and not (>= 'a' and <= 'f')))
        {
            throw new ArgumentException(
                "A plan digest must be a lowercase 64-character SHA-256 hexadecimal value.",
                parameterName);
        }
    }

    private void EnsureStorageLayout()
    {
        if (File.Exists(LedgerRoot))
        {
            throw new InvalidOperationException($"The plan-ledger root is a file: {LedgerRoot}");
        }

        Directory.CreateDirectory(LedgerRoot);
        EnsureSafeDirectory(LedgerRoot);
        Directory.CreateDirectory(_entriesDirectory);
        EnsureSafeDirectory(_entriesDirectory);
        Directory.CreateDirectory(_locksDirectory);
        EnsureSafeDirectory(_locksDirectory);
    }

    private async Task<FileStream> AcquirePlanLockAsync(
        string planId,
        CancellationToken cancellationToken)
    {
        string path = GetPaths(planId).Lock;
        long started = Stopwatch.GetTimestamp();
        IOException? lastError = null;
        while (Stopwatch.GetElapsedTime(started) < _lockTimeout)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureSafeFileOrMissing(path, planId);
            try
            {
                FileStream stream = new(
                    path,
                    FileMode.OpenOrCreate,
                    FileAccess.ReadWrite,
                    FileShare.None,
                    1,
                    FileOptions.Asynchronous | FileOptions.WriteThrough);
                try
                {
                    EnsureSafeFileOrMissing(path, planId);
                    return stream;
                }
                catch
                {
                    await stream.DisposeAsync();
                    throw;
                }
            }
            catch (ApplicationPlanLedgerCorruptionException)
            {
                throw;
            }
            catch (IOException error)
            {
                lastError = error;
                await Task.Delay(LockRetryDelay, cancellationToken);
            }
        }

        throw new TimeoutException(
            $"Timed out waiting for the durable plan-ledger lock for plan {planId}.",
            lastError);
    }

    private LedgerPaths GetPaths(string planId)
    {
        string key = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(planId)))
            .ToLowerInvariant();
        return new LedgerPaths(
            GetContainedPath(_entriesDirectory, $"{key}.registration.v1.json"),
            GetContainedPath(_entriesDirectory, $"{key}.claim.v1.json"),
            GetContainedPath(_entriesDirectory, $"{key}.completion.v1.json"),
            GetContainedPath(_locksDirectory, $"{key}.lock"));
    }

    private async Task<RegistrationRecord> ReadRegistrationAsync(
        string path,
        string planId,
        CancellationToken cancellationToken)
    {
        RegistrationRecord record = await ReadCanonicalRecordAsync<RegistrationRecord>(
            path,
            planId,
            cancellationToken);
        if (record.SchemaVersion != SchemaVersion
            || !string.Equals(record.RecordType, "registration", StringComparison.Ordinal)
            || !string.Equals(record.PlanId, planId, StringComparison.Ordinal)
            || !string.Equals(record.ProtocolVersion, ApplicationProtocol.Version, StringComparison.Ordinal)
            || string.IsNullOrWhiteSpace(record.CreatedByOperationId)
            || record.ExpiresAtUtc <= record.CreatedAtUtc
            || record.CreatedAtUtc.Offset != TimeSpan.Zero
            || record.ExpiresAtUtc.Offset != TimeSpan.Zero
            || record.RegisteredAtUtc.Offset != TimeSpan.Zero)
        {
            throw Corrupt(planId, path, "The registration record has invalid protocol fields.");
        }
        try
        {
            ValidateDigest(record.Digest, nameof(record.Digest));
        }
        catch (ArgumentException error)
        {
            throw Corrupt(planId, path, "The registration record has an invalid digest.", error);
        }

        return record;
    }

    private async Task<ClaimRecord> ReadClaimAsync(
        string path,
        RegistrationRecord registration,
        CancellationToken cancellationToken)
    {
        ClaimRecord record = await ReadCanonicalRecordAsync<ClaimRecord>(
            path,
            registration.PlanId,
            cancellationToken);
        if (record.SchemaVersion != SchemaVersion
            || !string.Equals(record.RecordType, "claim", StringComparison.Ordinal)
            || !string.Equals(record.PlanId, registration.PlanId, StringComparison.Ordinal)
            || !FixedTimeEquals(record.Digest, registration.Digest)
            || record.ClaimedAtUtc.Offset != TimeSpan.Zero)
        {
            throw Corrupt(registration.PlanId, path, "The claim receipt does not match its registration record.");
        }

        return record;
    }

    private async Task<CompletionRecord> ReadCompletionAsync(
        string path,
        RegistrationRecord registration,
        CancellationToken cancellationToken)
    {
        CompletionRecord record = await ReadCanonicalRecordAsync<CompletionRecord>(
            path,
            registration.PlanId,
            cancellationToken);
        if (record.SchemaVersion != SchemaVersion
            || !string.Equals(record.RecordType, "completion", StringComparison.Ordinal)
            || !string.Equals(record.PlanId, registration.PlanId, StringComparison.Ordinal)
            || !FixedTimeEquals(record.Digest, registration.Digest)
            || !IsTerminalLifecycleName(record.Lifecycle)
            || record.CompletedAtUtc.Offset != TimeSpan.Zero)
        {
            throw Corrupt(registration.PlanId, path, "The completion receipt does not match its registration record.");
        }

        return record;
    }

    private async Task<T> ReadCanonicalRecordAsync<T>(
        string path,
        string planId,
        CancellationToken cancellationToken)
        where T : class
    {
        EnsureSafeFile(path, planId);
        byte[] bytes;
        try
        {
            FileInfo info = new(path);
            if (info.Length is <= 0 or > MaximumRecordBytes)
            {
                throw Corrupt(planId, path, "The durable plan-ledger record has an invalid length.");
            }

            bytes = await File.ReadAllBytesAsync(path, cancellationToken);
        }
        catch (ApplicationPlanLedgerCorruptionException)
        {
            throw;
        }
        catch (FileNotFoundException error)
        {
            throw Corrupt(planId, path, "A durable plan-ledger record disappeared while it was being read.", error);
        }

        if (bytes.Length is <= 0 or > MaximumRecordBytes)
        {
            throw Corrupt(planId, path, "The durable plan-ledger record has an invalid length.");
        }

        T? record;
        try
        {
            record = JsonSerializer.Deserialize<T>(bytes, JsonOptions);
        }
        catch (JsonException error)
        {
            throw Corrupt(planId, path, "The durable plan-ledger record is not valid canonical JSON.", error);
        }
        catch (NotSupportedException error)
        {
            throw Corrupt(planId, path, "The durable plan-ledger record uses an unsupported schema.", error);
        }
        if (record is null)
        {
            throw Corrupt(planId, path, "The durable plan-ledger record is empty.");
        }

        byte[] canonical = SerializeCanonical(record);
        if (!bytes.AsSpan().SequenceEqual(canonical))
        {
            throw Corrupt(planId, path, "The durable plan-ledger record is not in canonical form.");
        }

        return record;
    }

    private async Task CreateImmutableRecordAsync<T>(
        string path,
        T record,
        CancellationToken cancellationToken)
        where T : class
    {
        EnsureSafeFileOrMissing(path, record switch
        {
            RegistrationRecord registration => registration.PlanId,
            ClaimRecord claim => claim.PlanId,
            CompletionRecord completion => completion.PlanId,
            _ => throw new ArgumentOutOfRangeException(nameof(record))
        });
        byte[] bytes = SerializeCanonical(record);
        string directory = Path.GetDirectoryName(path)!;
        string temporaryPath = GetContainedPath(
            directory,
            $".{Path.GetFileName(path)}.{Environment.ProcessId}.{Guid.NewGuid():N}.tmp");
        try
        {
            await using (FileStream stream = new(
                temporaryPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                4096,
                FileOptions.Asynchronous | FileOptions.WriteThrough))
            {
                await stream.WriteAsync(bytes, cancellationToken);
                await stream.FlushAsync(cancellationToken);
                stream.Flush(flushToDisk: true);
            }

            cancellationToken.ThrowIfCancellationRequested();
            File.Move(temporaryPath, path, overwrite: false);
        }
        catch
        {
            TryDeleteTemporaryFile(temporaryPath);
            throw;
        }
    }

    private async Task<bool> TryCreateClaimRecordAsync(
        string path,
        ClaimRecord record,
        CancellationToken cancellationToken)
    {
        EnsureSafeFileOrMissing(path, record.PlanId);
        byte[] bytes = SerializeCanonical(record);
        cancellationToken.ThrowIfCancellationRequested();

        FileStream stream;
        try
        {
            stream = new FileStream(
                path,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                4096,
                FileOptions.Asynchronous | FileOptions.WriteThrough);
        }
        catch (IOException) when (PathExists(path))
        {
            return false;
        }

        await using (stream)
        {
            // The successful CreateNew is the irreversible claim point. Ignore
            // caller cancellation from here onward. If an I/O failure tears
            // the receipt, it remains in place and future attempts fail closed
            // as corrupt rather than making the plan reusable.
            await stream.WriteAsync(bytes, CancellationToken.None);
            await stream.FlushAsync(CancellationToken.None);
            stream.Flush(flushToDisk: true);
        }

        return true;
    }

    private static byte[] SerializeCanonical<T>(T record)
    {
        byte[] json = JsonSerializer.SerializeToUtf8Bytes(record, JsonOptions);
        byte[] result = new byte[json.Length + 1];
        json.CopyTo(result, 0);
        result[^1] = (byte)'\n';
        return result;
    }

    private static string ToTerminalLifecycleName(ApplicationOperationLifecycle lifecycle)
    {
        return lifecycle switch
        {
            ApplicationOperationLifecycle.Succeeded => "succeeded",
            ApplicationOperationLifecycle.Failed => "failed",
            ApplicationOperationLifecycle.Cancelled => "cancelled",
            ApplicationOperationLifecycle.Rejected => "rejected",
            ApplicationOperationLifecycle.RecoveryRequired => "recoveryRequired",
            _ => throw new ArgumentOutOfRangeException(
                nameof(lifecycle),
                lifecycle,
                "Only terminal Application lifecycles can complete a claimed plan.")
        };
    }

    private static bool IsTerminalLifecycleName(string? lifecycle)
    {
        return lifecycle is "succeeded" or "failed" or "cancelled" or "rejected" or "recoveryRequired";
    }

    private static bool FixedTimeEquals(string? left, string? right)
    {
        if (left is null || right is null)
        {
            return false;
        }

        byte[] leftBytes = Encoding.UTF8.GetBytes(left);
        byte[] rightBytes = Encoding.UTF8.GetBytes(right);
        return leftBytes.Length == rightBytes.Length
            && CryptographicOperations.FixedTimeEquals(leftBytes, rightBytes);
    }

    private static void EnsureSafeDirectory(string path)
    {
        FileAttributes attributes = File.GetAttributes(path);
        if ((attributes & FileAttributes.Directory) == 0)
        {
            throw new InvalidOperationException($"Expected a plan-ledger directory at {path}.");
        }
        if ((attributes & FileAttributes.ReparsePoint) != 0)
        {
            throw new InvalidOperationException($"Plan-ledger directories cannot be reparse points: {path}");
        }
    }

    private static void EnsureSafeFile(string path, string planId)
    {
        if (!File.Exists(path))
        {
            throw Corrupt(planId, path, "The expected durable plan-ledger record is missing.");
        }

        EnsureSafeFileOrMissing(path, planId);
    }

    private static void EnsureSafeFileOrMissing(string path, string planId)
    {
        if (Directory.Exists(path))
        {
            throw Corrupt(planId, path, "A directory occupies a durable plan-ledger file path.");
        }
        if (!File.Exists(path))
        {
            return;
        }

        FileAttributes attributes = File.GetAttributes(path);
        if ((attributes & FileAttributes.ReparsePoint) != 0)
        {
            throw Corrupt(planId, path, "Durable plan-ledger files cannot be reparse points.");
        }
    }

    private static bool PathExists(string path)
    {
        return File.Exists(path) || Directory.Exists(path);
    }

    private static string? FirstExistingPath(params string[] paths)
    {
        return paths.FirstOrDefault(PathExists);
    }

    private static string GetContainedPath(string parent, string childName)
    {
        string fullParent = Path.TrimEndingDirectorySeparator(Path.GetFullPath(parent));
        string combined = Path.GetFullPath(Path.Combine(fullParent, childName));
        string? actualParent = Path.GetDirectoryName(combined);
        if (actualParent is null || !PathsEqual(fullParent, Path.TrimEndingDirectorySeparator(actualParent)))
        {
            throw new InvalidOperationException("A plan-ledger path escaped its storage root.");
        }

        return combined;
    }

    private static bool PathsEqual(string left, string right)
    {
        return string.Equals(
            left,
            right,
            OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
    }

    private static ApplicationPlanLedgerCorruptionException Corrupt(
        string planId,
        string evidencePath,
        string message,
        Exception? innerException = null)
    {
        return new ApplicationPlanLedgerCorruptionException(
            planId,
            Path.GetFullPath(evidencePath),
            message,
            innerException);
    }

    private static void TryDeleteTemporaryFile(string path)
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
            // Best-effort cleanup must not hide the durable write failure.
        }
    }

    private sealed record LedgerPaths(
        string Registration,
        string Claim,
        string Completion,
        string Lock);

    private sealed class RegistrationRecord
    {
        [JsonPropertyOrder(0)]
        public required int SchemaVersion { get; init; }

        [JsonPropertyOrder(1)]
        public required string RecordType { get; init; }

        [JsonPropertyOrder(2)]
        public required string PlanId { get; init; }

        [JsonPropertyOrder(3)]
        public required string Digest { get; init; }

        [JsonPropertyOrder(4)]
        public required string ProtocolVersion { get; init; }

        [JsonPropertyOrder(5)]
        public required string CreatedByOperationId { get; init; }

        [JsonPropertyOrder(6)]
        public required DateTimeOffset CreatedAtUtc { get; init; }

        [JsonPropertyOrder(7)]
        public required DateTimeOffset ExpiresAtUtc { get; init; }

        [JsonPropertyOrder(8)]
        public required DateTimeOffset RegisteredAtUtc { get; init; }
    }

    private sealed class ClaimRecord
    {
        [JsonPropertyOrder(0)]
        public required int SchemaVersion { get; init; }

        [JsonPropertyOrder(1)]
        public required string RecordType { get; init; }

        [JsonPropertyOrder(2)]
        public required string PlanId { get; init; }

        [JsonPropertyOrder(3)]
        public required string Digest { get; init; }

        [JsonPropertyOrder(4)]
        public required DateTimeOffset ClaimedAtUtc { get; init; }
    }

    private sealed class CompletionRecord
    {
        [JsonPropertyOrder(0)]
        public required int SchemaVersion { get; init; }

        [JsonPropertyOrder(1)]
        public required string RecordType { get; init; }

        [JsonPropertyOrder(2)]
        public required string PlanId { get; init; }

        [JsonPropertyOrder(3)]
        public required string Digest { get; init; }

        [JsonPropertyOrder(4)]
        public required string Lifecycle { get; init; }

        [JsonPropertyOrder(5)]
        public required DateTimeOffset CompletedAtUtc { get; init; }
    }
}

/// <summary>
/// Indicates that durable ledger evidence is missing, malformed, non-canonical,
/// or internally inconsistent. The evidence is retained in place so an
/// operator can inspect or recover it; the affected plan is never claimed.
/// </summary>
public sealed class ApplicationPlanLedgerCorruptionException : IOException
{
    public ApplicationPlanLedgerCorruptionException(
        string planId,
        string evidencePath,
        string message,
        Exception? innerException = null)
        : base($"{message} Plan: {planId}. Evidence: {evidencePath}", innerException)
    {
        PlanId = planId;
        EvidencePath = evidencePath;
    }

    public string Code { get; } = "plan_ledger_corrupt";

    public string PlanId { get; }

    public string EvidencePath { get; }
}
