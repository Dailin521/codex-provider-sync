using System.Security.Cryptography;
using System.Text;

namespace CodexProviderSync.Core;

/// <summary>
/// A read-only, deterministic view of every resource that a checked Core
/// write depends on. The snapshot is deliberately Core-owned so frontends do
/// not reproduce storage discovery or mutation rules.
/// </summary>
public sealed record CoreWritePlanSnapshot(
    string Operation,
    string StateFingerprint,
    string ExecutionToken,
    IReadOnlyList<CoreWritePlanTarget> Targets,
    IReadOnlyList<CoreWritePlanTarget> AutoPruneDeletionTargets,
    IReadOnlyList<CoreWritePlanWarning> Warnings);

public sealed record CoreWritePlanTarget(
    string Path,
    string Action,
    string Fingerprint);

public sealed record CoreWritePlanWarning(string Code, string Message);

public sealed class CoreWritePlanStaleException : InvalidOperationException
{
    public CoreWritePlanStaleException()
        : base("The Core write plan no longer matches the current storage state; create a new plan before applying.")
    {
    }
}

public sealed class CoreWritePlanExpiredException : InvalidOperationException
{
    public CoreWritePlanExpiredException()
        : base("The Core write plan expired before the mutation boundary; create a new plan before applying.")
    {
    }
}

internal enum CoreWriteFingerprintMode
{
    Content,
    RecursiveInventory,
    // SQLite file timestamps and WAL-index/SHM state can change on a read-only
    // open. Bind checked writes to durable main/WAL bytes instead.
    SqliteMainContent,
    SqliteWalContent
}

internal sealed record CoreWriteTargetSpec(
    string Path,
    string Action,
    CoreWriteFingerprintMode FingerprintMode = CoreWriteFingerprintMode.Content);

internal static class CoreWriteSnapshotBuilder
{
    private const string FormatVersion = "core-write-snapshot-v2";

    public static async Task<CoreWritePlanSnapshot> BuildAsync(
        string operation,
        string binding,
        IEnumerable<CoreWriteTargetSpec> targets,
        IEnumerable<CoreWriteTargetSpec>? autoPruneDeletionTargets = null,
        IEnumerable<CoreWritePlanWarning>? warnings = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operation);
        ArgumentNullException.ThrowIfNull(binding);

        IReadOnlyList<CoreWritePlanTarget> capturedTargets = await CaptureTargetsAsync(
            targets,
            cancellationToken);
        IReadOnlyList<CoreWritePlanTarget> capturedAutoPruneTargets = await CaptureTargetsAsync(
            autoPruneDeletionTargets ?? [],
            cancellationToken);
        IReadOnlyList<CoreWritePlanWarning> capturedWarnings = (warnings ?? [])
            .Select(static warning => warning with { })
            .OrderBy(static warning => warning.Code, StringComparer.Ordinal)
            .ThenBy(static warning => warning.Message, StringComparer.Ordinal)
            .ToArray();

        StringBuilder canonical = new();
        Append(canonical, "format", FormatVersion);
        Append(canonical, "operation", operation);
        Append(canonical, "binding", binding);
        foreach (CoreWritePlanTarget target in capturedTargets)
        {
            Append(canonical, "target.path", target.Path);
            Append(canonical, "target.action", target.Action);
            Append(canonical, "target.fingerprint", target.Fingerprint);
        }
        foreach (CoreWritePlanTarget target in capturedAutoPruneTargets)
        {
            Append(canonical, "autoPrune.path", target.Path);
            Append(canonical, "autoPrune.action", target.Action);
            Append(canonical, "autoPrune.fingerprint", target.Fingerprint);
        }
        foreach (CoreWritePlanWarning warning in capturedWarnings)
        {
            Append(canonical, "warning.code", warning.Code);
            Append(canonical, "warning.message", warning.Message);
        }

        string stateFingerprint = Sha256(canonical.ToString());
        string executionToken = Sha256($"{FormatVersion}\nexecution\n{operation}\n{stateFingerprint}");
        return new CoreWritePlanSnapshot(
            operation,
            stateFingerprint,
            executionToken,
            capturedTargets,
            capturedAutoPruneTargets,
            capturedWarnings);
    }

    public static void AssertExactMatch(
        CoreWritePlanSnapshot expected,
        CoreWritePlanSnapshot actual)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(actual);
        if (!string.Equals(expected.Operation, actual.Operation, StringComparison.Ordinal)
            || !FixedTimeEquals(expected.StateFingerprint, actual.StateFingerprint)
            || !FixedTimeEquals(expected.ExecutionToken, actual.ExecutionToken)
            || !expected.Targets.SequenceEqual(actual.Targets)
            || !expected.AutoPruneDeletionTargets.SequenceEqual(actual.AutoPruneDeletionTargets))
        {
            throw new CoreWritePlanStaleException();
        }
    }

    private static async Task<IReadOnlyList<CoreWritePlanTarget>> CaptureTargetsAsync(
        IEnumerable<CoreWriteTargetSpec> specs,
        CancellationToken cancellationToken)
    {
        CoreWriteTargetSpec[] normalized = specs
            .Select(static spec => new CoreWriteTargetSpec(
                Path.GetFullPath(spec.Path),
                spec.Action,
                spec.FingerprintMode))
            .DistinctBy(static spec => (spec.Path, spec.Action, spec.FingerprintMode))
            .OrderBy(static spec => spec.Path, StringComparer.Ordinal)
            .ThenBy(static spec => spec.Action, StringComparer.Ordinal)
            .ToArray();
        List<CoreWritePlanTarget> result = new(normalized.Length);
        Dictionary<(string Path, CoreWriteFingerprintMode Mode), string> fingerprintCache = [];
        foreach (CoreWriteTargetSpec spec in normalized)
        {
            cancellationToken.ThrowIfCancellationRequested();
            (string Path, CoreWriteFingerprintMode Mode) cacheKey = (spec.Path, spec.FingerprintMode);
            if (!fingerprintCache.TryGetValue(cacheKey, out string? fingerprint))
            {
                fingerprint = await FingerprintPathAsync(
                    spec.Path,
                    spec.FingerprintMode,
                    cancellationToken);
                fingerprintCache.Add(cacheKey, fingerprint);
            }
            result.Add(new CoreWritePlanTarget(spec.Path, spec.Action, fingerprint));
        }
        return result.AsReadOnly();
    }

    private static async Task<string> FingerprintPathAsync(
        string fullPath,
        CoreWriteFingerprintMode mode,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!File.Exists(fullPath) && !Directory.Exists(fullPath))
        {
            if (mode == CoreWriteFingerprintMode.SqliteWalContent)
            {
                return FingerprintEmptySqliteWal(fullPath);
            }
            return Sha256($"missing\n{fullPath}");
        }

        FileAttributes attributes = File.GetAttributes(fullPath);
        if ((attributes & FileAttributes.ReparsePoint) != 0)
        {
            DateTime lastWrite = File.GetLastWriteTimeUtc(fullPath);
            return Sha256($"reparse\n{fullPath}\n{(int)attributes}\n{lastWrite.Ticks}");
        }
        return (attributes & FileAttributes.Directory) != 0
            ? await FingerprintDirectoryAsync(fullPath, mode, cancellationToken)
            : mode switch
            {
                CoreWriteFingerprintMode.RecursiveInventory => FingerprintFileInventory(fullPath, attributes),
                CoreWriteFingerprintMode.SqliteMainContent => await FingerprintSqliteFileAsync(
                    fullPath,
                    normalizeEmptyWal: false,
                    cancellationToken),
                CoreWriteFingerprintMode.SqliteWalContent => await FingerprintSqliteFileAsync(
                    fullPath,
                    normalizeEmptyWal: true,
                    cancellationToken),
                _ => await FingerprintFileAsync(fullPath, cancellationToken)
            };
    }

    private static async Task<string> FingerprintDirectoryAsync(
        string directoryPath,
        CoreWriteFingerprintMode mode,
        CancellationToken cancellationToken)
    {
        StringBuilder canonical = new();
        Append(canonical, "type", "directory");
        Append(canonical, "path", directoryPath);
        string[] entries;
        try
        {
            entries = Directory.EnumerateFileSystemEntries(directoryPath)
                .Where(static path => !string.Equals(
                    Path.GetFileName(path),
                    "auth.json",
                    StringComparison.OrdinalIgnoreCase))
                .Order(StringComparer.Ordinal)
                .ToArray();
        }
        catch (DirectoryNotFoundException)
        {
            return Sha256($"missing\n{directoryPath}");
        }

        foreach (string entry in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Append(canonical, "entry.name", Path.GetFileName(entry));
            Append(
                canonical,
                "entry.fingerprint",
                await FingerprintPathAsync(entry, mode, cancellationToken));
        }
        return Sha256(canonical.ToString());
    }

    private static string FingerprintFileInventory(
        string filePath,
        FileAttributes attributes)
    {
        FileInfo info = new(filePath);
        return Sha256(
            $"file-inventory\n{filePath}\n{(int)attributes}\n{info.Length}\n{info.LastWriteTimeUtc.Ticks}");
    }

    private static async Task<string> FingerprintFileAsync(
        string filePath,
        CancellationToken cancellationToken)
    {
        FileInfo before = new(filePath);
        long beforeLength = before.Length;
        long beforeWriteTicks = before.LastWriteTimeUtc.Ticks;
        try
        {
            await using FileStream stream = new(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete,
                64 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            byte[] digest = await SHA256.HashDataAsync(stream, cancellationToken);
            FileInfo after = new(filePath);
            if (!after.Exists
                || after.Length != beforeLength
                || after.LastWriteTimeUtc.Ticks != beforeWriteTicks)
            {
                throw new CoreWritePlanStaleException();
            }
            return $"sha256:{Convert.ToHexString(digest).ToLowerInvariant()}:{beforeLength}:{beforeWriteTicks}";
        }
        catch (FileNotFoundException)
        {
            return Sha256($"missing\n{filePath}");
        }
    }

    private static async Task<string> FingerprintSqliteFileAsync(
        string filePath,
        bool normalizeEmptyWal,
        CancellationToken cancellationToken)
    {
        FileInfo before = new(filePath);
        long beforeLength = before.Length;
        try
        {
            await using FileStream stream = new(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete,
                64 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            byte[] digest = await SHA256.HashDataAsync(stream, cancellationToken);
            FileInfo after = new(filePath);
            if (!after.Exists || after.Length != beforeLength)
            {
                throw new CoreWritePlanStaleException();
            }

            if (normalizeEmptyWal && beforeLength == 0)
            {
                return FingerprintEmptySqliteWal(filePath);
            }

            return $"sqlite-sha256:{Convert.ToHexString(digest).ToLowerInvariant()}:{beforeLength}";
        }
        catch (FileNotFoundException)
        {
            return normalizeEmptyWal
                ? FingerprintEmptySqliteWal(filePath)
                : Sha256($"missing\n{filePath}");
        }
    }

    private static string FingerprintEmptySqliteWal(string filePath)
    {
        return Sha256($"sqlite-wal-empty\n{filePath}");
    }

    private static string Sha256(string value)
    {
        return "sha256:" + Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();
    }

    private static bool FixedTimeEquals(string left, string right)
    {
        byte[] leftBytes = Encoding.UTF8.GetBytes(left);
        byte[] rightBytes = Encoding.UTF8.GetBytes(right);
        return leftBytes.Length == rightBytes.Length
            && CryptographicOperations.FixedTimeEquals(leftBytes, rightBytes);
    }

    private static void Append(StringBuilder builder, string key, string value)
    {
        builder.Append(key.Length).Append(':').Append(key).Append('=')
            .Append(value.Length).Append(':').Append(value).Append(';');
    }
}
