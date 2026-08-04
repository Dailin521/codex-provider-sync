using System.Security.Cryptography;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace CodexProviderSync.App.Automation;

internal sealed record AutomationBootstrap(
    bool Enabled,
    string? DescriptorPath,
    string? IsolationRoot,
    string? PipeName,
    string? Token)
{
    internal const string Argument = "--gui-automation-descriptor";
    internal const string SentinelFileName = ".codex-provider-sync-test-root";
    internal const string SentinelContent = "codex-provider-sync isolated GUI automation root v1";
    private const string ClaimSuffix = ".claimed";
    private static readonly Regex PipePattern = new(
        "^CodexProviderSync\\.Automation\\.[0-9a-f]{32}$",
        RegexOptions.CultureInvariant | RegexOptions.NonBacktracking);
    private static readonly Regex TokenPattern = new(
        "^[0-9a-f]{64}$",
        RegexOptions.CultureInvariant | RegexOptions.NonBacktracking);

    internal static AutomationBootstrap ParseAndClaim(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);
        int index = Array.FindIndex(args, value => string.Equals(value, Argument, StringComparison.Ordinal));
        if (index < 0)
        {
            if (args.Any(value => value.StartsWith("--gui-automation", StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException("An unrecognized GUI automation option was supplied; startup is refused.");
            }
            return new AutomationBootstrap(false, null, null, null, null);
        }
        if (args.Length != 2 || index != 0 || string.IsNullOrWhiteSpace(args[1]))
        {
            throw new InvalidOperationException($"{Argument} must be the only option and must name one descriptor file.");
        }

        string descriptorPath = Path.GetFullPath(args[1]);
        RejectReparsePoint(descriptorPath, "automation descriptor");
        AutomationDescriptor descriptor;
        try
        {
            using FileStream stream = new(descriptorPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            descriptor = JsonSerializer.Deserialize<AutomationDescriptor>(stream, JsonOptions)
                ?? throw new InvalidDataException("Automation descriptor is empty.");
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or JsonException)
        {
            throw new InvalidOperationException("Unable to read the GUI automation descriptor.", error);
        }

        if (descriptor.SchemaVersion != 1)
        {
            throw new InvalidDataException("Unsupported GUI automation descriptor schema.");
        }
        if (string.IsNullOrWhiteSpace(descriptor.IsolationRoot)
            || string.IsNullOrWhiteSpace(descriptor.PipeName)
            || string.IsNullOrWhiteSpace(descriptor.Token))
        {
            throw new InvalidDataException("GUI automation descriptor fields are incomplete.");
        }

        string root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(descriptor.IsolationRoot));
        RejectReparsePoint(root, "automation isolation root");
        EnsureContained(root, descriptorPath);
        RejectNestedReparsePoints(root, descriptorPath);
        string sentinelPath = Path.Combine(root, SentinelFileName);
        RejectReparsePoint(sentinelPath, "automation isolation sentinel");
        string sentinel = File.ReadAllText(sentinelPath);
        if (!string.Equals(sentinel.Trim(), SentinelContent, StringComparison.Ordinal))
        {
            throw new InvalidDataException("GUI automation isolation sentinel is missing or invalid.");
        }
        if (!PipePattern.IsMatch(descriptor.PipeName))
        {
            throw new InvalidDataException("GUI automation pipe name is not a random scoped name.");
        }
        string pipeNonce = descriptor.PipeName["CodexProviderSync.Automation.".Length..];
        if (!Guid.TryParseExact(pipeNonce, "N", out Guid pipeGuid) || pipeGuid == Guid.Empty)
        {
            throw new InvalidDataException("GUI automation pipe name must contain a non-empty random GUID.");
        }
        if (!TokenPattern.IsMatch(descriptor.Token))
        {
            throw new InvalidDataException("GUI automation token must be 32 random bytes encoded as lowercase hex.");
        }
        byte[] tokenBytes = Convert.FromHexString(descriptor.Token);
        if (tokenBytes.Distinct().Count() < 8)
        {
            throw new InvalidDataException("GUI automation token does not contain enough entropy.");
        }

        string claimPath = descriptorPath + ClaimSuffix;
        try
        {
            using FileStream claim = new(
                claimPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                4096,
                FileOptions.WriteThrough);
            using StreamWriter writer = new(claim, leaveOpen: true);
            writer.Write($"pid={Environment.ProcessId};nonce={Convert.ToHexString(RandomNumberGenerator.GetBytes(16)).ToLowerInvariant()}");
            writer.Flush();
            claim.Flush(flushToDisk: true);
        }
        catch (IOException error)
        {
            throw new InvalidOperationException("GUI automation descriptor was already claimed; replay is refused.", error);
        }

        return new AutomationBootstrap(true, descriptorPath, root, descriptor.PipeName, descriptor.Token);
    }

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = false,
        UnmappedMemberHandling = System.Text.Json.Serialization.JsonUnmappedMemberHandling.Disallow
    };

    private static void EnsureContained(string root, string candidate)
    {
        StringComparison comparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
        string prefix = root + Path.DirectorySeparatorChar;
        if (!candidate.StartsWith(prefix, comparison))
        {
            throw new InvalidDataException("GUI automation descriptor must be stored inside its isolation root.");
        }
    }

    private static void RejectNestedReparsePoints(string root, string descriptorPath)
    {
        string? current = Path.GetDirectoryName(descriptorPath);
        while (current is not null && current.Length >= root.Length)
        {
            RejectReparsePoint(current, "automation descriptor path");
            if (string.Equals(current, root, OperatingSystem.IsWindows()
                ? StringComparison.OrdinalIgnoreCase
                : StringComparison.Ordinal))
            {
                return;
            }
            current = Path.GetDirectoryName(current);
        }
        throw new InvalidDataException("GUI automation descriptor path containment could not be verified.");
    }

    private static void RejectReparsePoint(string path, string label)
    {
        FileAttributes attributes;
        try
        {
            attributes = File.GetAttributes(path);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            throw new InvalidDataException($"The {label} does not exist or cannot be inspected.", error);
        }
        if ((attributes & FileAttributes.ReparsePoint) != 0)
        {
            throw new InvalidDataException($"The {label} cannot be a reparse point.");
        }
    }

    private sealed class AutomationDescriptor
    {
        public int SchemaVersion { get; init; }
        public string? IsolationRoot { get; init; }
        public string? PipeName { get; init; }
        public string? Token { get; init; }
    }
}
