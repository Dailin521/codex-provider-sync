using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace CodexProviderSync.GuiE2E;

public static class IsolationEnvironment
{
    private static readonly HashSet<string> AllowedInheritedVariables = new(StringComparer.OrdinalIgnoreCase)
    {
        "SystemRoot", "WINDIR", "SystemDrive", "COMSPEC", "PATH", "PATHEXT", "OS",
        "PROCESSOR_ARCHITECTURE", "PROCESSOR_IDENTIFIER", "PROCESSOR_LEVEL", "PROCESSOR_REVISION",
        "NUMBER_OF_PROCESSORS", "ProgramFiles", "ProgramFiles(x86)", "ProgramW6432",
        "CommonProgramFiles", "CommonProgramFiles(x86)", "CommonProgramW6432",
        "COMPUTERNAME", "USERDOMAIN", "USERNAME", "LANG", "LC_ALL",
        "DOTNET_ROOT", "DOTNET_ROOT_X64"
    };
    private static readonly string[] ForcedDirectories =
    [
        "HOME", "USERPROFILE", "CODEX_HOME", "CODEX_SQLITE_HOME",
        "APPDATA", "LOCALAPPDATA", "TEMP", "TMP"
    ];

    public static Dictionary<string, string?> Build(
        string root,
        IReadOnlyDictionary<string, string?> source)
    {
        string fullRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(root));
        Dictionary<string, string?> result = new(StringComparer.OrdinalIgnoreCase);
        foreach ((string name, string? value) in source)
        {
            if (!AllowedInheritedVariables.Contains(name)
                || IsCredentialVariable(name)
                || ForcedDirectories.Contains(name, StringComparer.OrdinalIgnoreCase))
            {
                continue;
            }
            result[name] = value;
        }

        result["HOME"] = Under(fullRoot, "env", "home");
        result["USERPROFILE"] = Under(fullRoot, "env", "profile");
        result["CODEX_HOME"] = Under(fullRoot, "codex-home");
        result["CODEX_SQLITE_HOME"] = Under(fullRoot, "sqlite-home");
        result["APPDATA"] = Under(fullRoot, "env", "appdata");
        result["LOCALAPPDATA"] = Under(fullRoot, "env", "localappdata");
        result["TEMP"] = Under(fullRoot, "env", "temp");
        result["TMP"] = Under(fullRoot, "env", "temp");
        result["DOTNET_BUNDLE_EXTRACT_BASE_DIR"] = Under(fullRoot, "env", "dotnet-bundle");
        foreach (string path in ForcedDirectories.Select(name => result[name]!)
            .Append(result["DOTNET_BUNDLE_EXTRACT_BASE_DIR"]!)
            .Distinct(StringComparer.OrdinalIgnoreCase))
        {
            Directory.CreateDirectory(path);
        }
        string isolatedProfile = result["USERPROFILE"]!;
        foreach (string shellFolder in new[] { "Desktop", "Documents", "Downloads" })
        {
            Directory.CreateDirectory(Path.Combine(isolatedProfile, shellFolder));
        }
        return result;
    }

    public static bool IsContained(string root, string candidate)
    {
        string fullRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(root));
        string fullCandidate = Path.GetFullPath(candidate);
        return string.Equals(fullRoot, fullCandidate, StringComparison.OrdinalIgnoreCase)
            || fullCandidate.StartsWith(fullRoot + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsCredentialVariable(string name)
    {
        string normalized = name.ToUpperInvariant();
        return normalized.Contains("TOKEN", StringComparison.Ordinal)
            || normalized.Contains("AUTH", StringComparison.Ordinal)
            || normalized.Contains("KEY", StringComparison.Ordinal)
            || normalized.Contains("SECRET", StringComparison.Ordinal)
            || normalized.Contains("PASSWORD", StringComparison.Ordinal)
            || normalized.Contains("PASSWD", StringComparison.Ordinal)
            || normalized.Contains("CREDENTIAL", StringComparison.Ordinal)
            || normalized.Contains("CLAUDE", StringComparison.Ordinal)
            || normalized.Contains("GITHUB", StringComparison.Ordinal)
            || normalized.Contains("OPENAI", StringComparison.Ordinal)
            || normalized.Contains("CODEX", StringComparison.Ordinal);
    }

    private static string Under(string root, params string[] segments) =>
        segments.Aggregate(root, Path.Combine);
}

public sealed record ManifestCoverageResult(
    bool Passed,
    IReadOnlyList<string> MissingEntryIds,
    IReadOnlyList<string> MissingScenarioIds);

public static class ManifestCoverageGate
{
    public static ManifestCoverageResult Evaluate(
        string manifestJson,
        IEnumerable<string> coveredEntryIds,
        IEnumerable<string> coveredScenarioIds)
    {
        using JsonDocument document = JsonDocument.Parse(manifestJson);
        JsonElement root = document.RootElement;
        HashSet<string> entries = new(StringComparer.Ordinal);
        HashSet<string> scenarios = new(StringComparer.Ordinal);
        AddEntry(root.GetProperty("window"), entries, scenarios);
        foreach (string collection in new[] { "controls", "templates", "dialogs" })
        {
            foreach (JsonElement entry in root.GetProperty(collection).EnumerateArray())
            {
                AddEntry(entry, entries, scenarios);
            }
        }

        HashSet<string> coveredEntries = coveredEntryIds.ToHashSet(StringComparer.Ordinal);
        HashSet<string> coveredScenarios = coveredScenarioIds.ToHashSet(StringComparer.Ordinal);
        string[] missingEntries = entries.Except(coveredEntries).Order(StringComparer.Ordinal).ToArray();
        string[] missingScenarios = scenarios.Except(coveredScenarios).Order(StringComparer.Ordinal).ToArray();
        return new ManifestCoverageResult(
            missingEntries.Length == 0 && missingScenarios.Length == 0,
            missingEntries,
            missingScenarios);
    }

    public static (IReadOnlyList<string> Entries, IReadOnlyList<string> Scenarios) ReadRequirements(string manifestJson)
    {
        using JsonDocument document = JsonDocument.Parse(manifestJson);
        HashSet<string> entries = new(StringComparer.Ordinal);
        HashSet<string> scenarios = new(StringComparer.Ordinal);
        AddEntry(document.RootElement.GetProperty("window"), entries, scenarios);
        foreach (string collection in new[] { "controls", "templates", "dialogs" })
        {
            foreach (JsonElement entry in document.RootElement.GetProperty(collection).EnumerateArray())
            {
                AddEntry(entry, entries, scenarios);
            }
        }
        return (entries.Order(StringComparer.Ordinal).ToArray(), scenarios.Order(StringComparer.Ordinal).ToArray());
    }

    private static void AddEntry(JsonElement entry, ISet<string> entries, ISet<string> scenarios)
    {
        entries.Add(entry.GetProperty("id").GetString()!);
        foreach (JsonElement scenario in entry.GetProperty("scenarioIds").EnumerateArray())
        {
            scenarios.Add(scenario.GetString()!);
        }
    }
}

public static class EvidenceRedactor
{
    public static string Redact(string evidence, IEnumerable<string> secrets)
    {
        string redacted = evidence;
        foreach (string secret in secrets.Where(static value => !string.IsNullOrEmpty(value)))
        {
            redacted = redacted.Replace(secret, "[REDACTED]", StringComparison.Ordinal);
            redacted = redacted.Replace(secret.ToUpperInvariant(), "[REDACTED]", StringComparison.Ordinal);
        }
        return redacted;
    }
}

public sealed record VerifiedApplicationLink(
    string OperationId,
    string Operation,
    string Lifecycle,
    IReadOnlyList<string> ErrorCodes);

public static class ApplicationLinkVerifier
{
    public static VerifiedApplicationLink Verify(
        JsonNode? result,
        string expectedOperation,
        string expectedLifecycle,
        IEnumerable<string>? expectedErrorCodes = null)
    {
        JsonObject root = result as JsonObject
            ?? throw new InvalidDataException("GUI invoke result did not contain an Application link object.");
        if (root["applicationOperationLinked"]?.GetValue<bool>() != true)
        {
            throw new InvalidDataException("GUI invoke result did not link an Application operation.");
        }
        string operationId = RequiredString(root, "applicationOperationId");
        string operation = RequiredString(root, "applicationOperation");
        string lifecycle = RequiredString(root, "applicationLifecycle");
        if (!string.Equals(operation, expectedOperation, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                $"Application operation mismatch: expected {expectedOperation}, observed {operation}.");
        }
        if (!string.Equals(lifecycle, expectedLifecycle, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                $"Application lifecycle mismatch for {operation}: expected {expectedLifecycle}, observed {lifecycle}.");
        }
        JsonArray operations = root["applicationOperations"] as JsonArray
            ?? throw new InvalidDataException("GUI invoke result omitted applicationOperations.");
        JsonObject[] matchingOperations = operations.OfType<JsonObject>().Where(candidate =>
            string.Equals(candidate["operationId"]?.GetValue<string>(), operationId, StringComparison.Ordinal)).ToArray();
        if (matchingOperations.Length != 1)
        {
            throw new InvalidDataException(
                "Primary applicationOperationId did not identify exactly one nested operation record.");
        }
        JsonObject nested = matchingOperations[0];
        string nestedOperation = RequiredString(nested, "operation");
        string nestedLifecycle = RequiredString(nested, "lifecycle");
        if (!string.Equals(nestedOperation, operation, StringComparison.Ordinal)
            || !string.Equals(nestedLifecycle, lifecycle, StringComparison.Ordinal))
        {
            throw new InvalidDataException("Primary and nested Application operation metadata disagree.");
        }
        string[] actualErrors = (nested["errorCodes"] as JsonArray
            ?? throw new InvalidDataException("Nested Application operation omitted errorCodes."))
            .Select(node => node?.GetValue<string>()
                ?? throw new InvalidDataException("Application errorCodes contains a null value."))
            .Order(StringComparer.Ordinal)
            .ToArray();
        string[] expectedErrors = (expectedErrorCodes ?? [])
            .Order(StringComparer.Ordinal)
            .ToArray();
        if (!actualErrors.SequenceEqual(expectedErrors, StringComparer.Ordinal))
        {
            throw new InvalidDataException(
                $"Application errorCodes mismatch for {operation}: expected [{string.Join(",", expectedErrors)}], "
                + $"observed [{string.Join(",", actualErrors)}].");
        }
        return new VerifiedApplicationLink(operationId, operation, lifecycle, actualErrors);
    }

    private static string RequiredString(JsonObject owner, string property)
    {
        string? value = owner[property]?.GetValue<string>();
        return string.IsNullOrWhiteSpace(value)
            ? throw new InvalidDataException($"Application link omitted {property}.")
            : value;
    }
}

public sealed record DialogContractResult(bool Passed, string? Error);

public static class DialogContractVerifier
{
    public static DialogContractResult Verify(
        string purpose,
        string className,
        string title,
        IEnumerable<string> text)
    {
        if (!string.Equals(className, "#32770", StringComparison.Ordinal))
        {
            return new(false, $"Expected native MessageBox class #32770, observed {className}.");
        }
        if (!string.Equals(title, "Codex Provider Sync", StringComparison.Ordinal))
        {
            return new(false, $"Unexpected MessageBox title: {title}.");
        }
        string[] labels = text.Where(value => !string.IsNullOrWhiteSpace(value)).ToArray();
        string combined = string.Join("\n", labels);
        DialogExpectation expectation = purpose switch
        {
            "validation" => new(["请输入要添加的 Provider ID"], true, false, []),
            "sync-cancel" or "sync-apply" or "switch-apply" =>
                new(["执行同步前", "是否继续"], true, true, []),
            "prune-cancel" or "prune-apply" =>
                new(["确认清理旧备份", "将只保留最近"], true, true, []),
            "update-no-update" => new(
                ["当前已是最新版本"],
                true,
                false,
                ["失败", "错误", "超时", "发现新版本", "failed", "error", "timeout"]),
            "restore-targets" => new(["确认恢复以下备份", "将覆盖当前的"], true, true, []),
            "restore-close-codex" => new(["恢复备份前", "是否继续"], true, true, []),
            "operation-failure" => new(
                [],
                true,
                false,
                ["是否继续", "当前已是最新版本", "确认清理", "确认恢复"]),
            _ => throw new ArgumentOutOfRangeException(nameof(purpose), purpose, "Unknown dialog contract purpose.")
        };
        foreach (string forbidden in expectation.ForbiddenFragments)
        {
            if (combined.Contains(forbidden, StringComparison.OrdinalIgnoreCase))
            {
                return new(false, $"MessageBox for {purpose} contained forbidden text '{forbidden}'.");
            }
        }
        foreach (string required in expectation.RequiredFragments)
        {
            if (!combined.Contains(required, StringComparison.OrdinalIgnoreCase))
            {
                return new(false, $"MessageBox for {purpose} omitted required text '{required}'.");
            }
        }
        bool hasAccept = labels.Any(IsAcceptButton);
        bool hasCancel = labels.Any(IsCancelButton);
        if (expectation.RequiresAccept && !hasAccept)
        {
            return new(false, $"MessageBox for {purpose} omitted an accept button.");
        }
        if (expectation.RequiresCancel && !hasCancel)
        {
            return new(false, $"MessageBox for {purpose} omitted a cancel button.");
        }
        if (string.Equals(purpose, "operation-failure", StringComparison.Ordinal)
            && (hasCancel || labels.All(value => IsAcceptButton(value) || IsCancelButton(value))))
        {
            return new(false, "Operation-failure MessageBox must contain error text and only an acknowledgement action.");
        }
        return new(true, null);
    }

    private static bool IsAcceptButton(string value) =>
        new[] { "确定", "OK", "是", "Yes" }.Contains(value.Trim('&'), StringComparer.OrdinalIgnoreCase);

    private static bool IsCancelButton(string value) =>
        new[] { "取消", "Cancel", "否", "No" }.Contains(value.Trim('&'), StringComparer.OrdinalIgnoreCase);

    private sealed record DialogExpectation(
        IReadOnlyList<string> RequiredFragments,
        bool RequiresAccept,
        bool RequiresCancel,
        IReadOnlyList<string> ForbiddenFragments);
}

public sealed record PruneRemovalEvidence(IReadOnlyList<string> RemovedPaths);

public static class PruneEvidenceContract
{
    public static PruneRemovalEvidence VerifyManagedRemoval(
        IReadOnlyList<string> managedBefore,
        IReadOnlyList<string> managedAfter,
        string expectedNewest)
    {
        if (managedBefore.Count <= 1
            || managedBefore.Distinct(StringComparer.Ordinal).Count() != managedBefore.Count)
        {
            throw new InvalidDataException("Prune evidence requires more than one distinct managed backup before apply.");
        }
        if (managedAfter.Count != 1
            || !string.Equals(managedAfter[0], expectedNewest, StringComparison.Ordinal))
        {
            throw new InvalidDataException("Prune did not retain exactly the expected newest managed backup.");
        }
        string[] removed = managedBefore
            .Where(path => !string.Equals(path, expectedNewest, StringComparison.Ordinal))
            .ToArray();
        if (removed.Length != managedBefore.Count - 1)
        {
            throw new InvalidDataException("Expected newest managed backup was absent or duplicated before prune.");
        }
        string[] stillPresent = removed.Where(Directory.Exists).ToArray();
        if (stillPresent.Length > 0)
        {
            throw new InvalidDataException(
                $"Prune removed metadata visibility but left managed backup directories on disk: {string.Join(",", stillPresent)}.");
        }
        return new PruneRemovalEvidence(removed);
    }

    public static async Task VerifySentinelAsync(
        string sentinelPath,
        string expectedSha256,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(sentinelPath))
        {
            throw new InvalidDataException($"Non-managed prune sentinel disappeared: {sentinelPath}.");
        }
        string actualSha256 = await Hashing.Sha256FileAsync(sentinelPath, cancellationToken);
        if (!string.Equals(actualSha256, expectedSha256, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                $"Non-managed prune sentinel content changed: expected {expectedSha256}, observed {actualSha256}.");
        }
    }
}

internal static class Hashing
{
    internal static async Task<string> Sha256FileAsync(string path, CancellationToken cancellationToken = default)
    {
        await using FileStream stream = new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        byte[] digest = await SHA256.HashDataAsync(stream, cancellationToken);
        return Convert.ToHexStringLower(digest);
    }

    internal static string Sha256Text(string text) =>
        Convert.ToHexStringLower(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(text)));
}

internal sealed class EvidenceDocument
{
    public int SchemaVersion { get; init; } = 1;
    public string RunId { get; init; } = Guid.NewGuid().ToString("N");
    public DateTimeOffset StartedAtUtc { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? FinishedAtUtc { get; set; }
    public bool Passed { get; set; }
    public Dictionary<string, object?> Environment { get; } = [];
    public Dictionary<string, object?> Executable { get; } = [];
    public Dictionary<string, object?> Manifest { get; } = [];
    public List<Dictionary<string, object?>> Controls { get; } = [];
    public List<Dictionary<string, object?>> Scenarios { get; } = [];
    public List<Dictionary<string, object?>> Dialogs { get; } = [];
    public List<Dictionary<string, object?>> FileDiffs { get; } = [];
    public List<JsonNode?> Trace { get; } = [];
    public Dictionary<string, object?> Restart { get; } = [];
    public List<string> Blockers { get; } = [];
    public List<string> Errors { get; } = [];

    public void Scenario(string id, string status, string evidence, params string[] entryIds) => Scenarios.Add(new()
    {
        ["id"] = id,
        ["status"] = status,
        ["evidence"] = evidence,
        ["entryIds"] = entryIds
    });
}
