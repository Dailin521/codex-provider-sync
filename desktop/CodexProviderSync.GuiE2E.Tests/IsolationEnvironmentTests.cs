using CodexProviderSync.GuiE2E;

namespace CodexProviderSync.GuiE2E.Tests;

public sealed class IsolationEnvironmentTests
{
    private static readonly string[] IsolatedDirectoryVariables =
    [
        "HOME",
        "USERPROFILE",
        "CODEX_HOME",
        "CODEX_SQLITE_HOME",
        "APPDATA",
        "LOCALAPPDATA",
        "TEMP",
        "TMP",
    ];

    [Fact]
    public void Build_MapsEveryUserDataDirectoryInsideIsolationRoot()
    {
        var root = Path.Combine(Path.GetTempPath(), "codex-provider-sync-tests", Guid.NewGuid().ToString("N"));
        var realProfile = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        var source = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["HOME"] = realProfile,
            ["USERPROFILE"] = realProfile,
            ["CODEX_HOME"] = Path.Combine(realProfile, ".codex"),
            ["CODEX_SQLITE_HOME"] = Path.Combine(realProfile, ".codex", "sqlite"),
            ["APPDATA"] = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            ["LOCALAPPDATA"] = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            ["TEMP"] = Path.GetTempPath(),
            ["TMP"] = Path.GetTempPath(),
            ["PATH"] = "safe-path-value",
        };

        var result = IsolationEnvironment.Build(root, source);

        Assert.Equal("safe-path-value", result["PATH"]);
        foreach (var variable in IsolatedDirectoryVariables)
        {
            Assert.True(result.TryGetValue(variable, out var value), $"{variable} was not mapped.");
            Assert.False(string.IsNullOrWhiteSpace(value));
            AssertPathIsWithin(root, value!);
        }
        foreach (var shellFolder in new[] { "Desktop", "Documents", "Downloads" })
        {
            var path = Path.Combine(result["USERPROFILE"]!, shellFolder);
            Assert.True(Directory.Exists(path), $"Isolated shell folder was not created: {shellFolder}");
            AssertPathIsWithin(root, path);
        }
    }

    [Fact]
    public void Build_RemovesCredentialBearingVariablesWithoutMutatingSource()
    {
        var root = Path.Combine(Path.GetTempPath(), "codex-provider-sync-tests", Guid.NewGuid().ToString("N"));
        var source = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["PATH"] = "safe-path-value",
            ["OPENAI_API_KEY"] = "openai-secret",
            ["CODEX_TOKEN"] = "codex-secret",
            ["GITHUB_TOKEN"] = "github-secret",
            ["GITHUB_PAT"] = "github-pat-secret",
            ["CLAUDE_API_KEY"] = "claude-secret",
            ["AZURE_CLIENT_SECRET"] = "azure-secret",
            ["DATABASE_PASSWORD"] = "password-secret",
            ["PACKAGE_CREDENTIAL"] = "credential-secret",
            ["AUTH_TOKEN"] = "auth-secret",
            ["NUGET_AUTH_TOKEN"] = "nuget-secret",
            ["CODEX_INTERNAL_CONTEXT"] = "codex-context",
            ["OPENAI_ORGANIZATION"] = "openai-organization",
            ["CLAUDE_CONFIG_DIR"] = "claude-config",
            ["GITHUB_ACTIONS"] = "true",
            ["UNRELATED_USER_SETTING"] = "must-not-be-inherited",
        };
        var original = source.ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.OrdinalIgnoreCase);

        var result = IsolationEnvironment.Build(root, source);

        Assert.Equal(
            original.OrderBy(pair => pair.Key, StringComparer.OrdinalIgnoreCase),
            source.OrderBy(pair => pair.Key, StringComparer.OrdinalIgnoreCase));
        Assert.Equal("safe-path-value", result["PATH"]);
        Assert.DoesNotContain(result.Keys, key => IsCredentialVariable(key));
        foreach (var blockedVendorVariable in source.Keys.Where(IsBlockedVendorVariable))
        {
            Assert.False(result.ContainsKey(blockedVendorVariable));
        }
        Assert.False(result.ContainsKey("UNRELATED_USER_SETTING"));
    }

    private static bool IsCredentialVariable(string name)
    {
        return name.Contains("TOKEN", StringComparison.OrdinalIgnoreCase)
            || name.Contains("API_KEY", StringComparison.OrdinalIgnoreCase)
            || name.Contains("AUTH", StringComparison.OrdinalIgnoreCase)
            || name.Equals("PAT", StringComparison.OrdinalIgnoreCase)
            || name.EndsWith("_PAT", StringComparison.OrdinalIgnoreCase)
            || name.Contains("KEY", StringComparison.OrdinalIgnoreCase)
            || name.Contains("SECRET", StringComparison.OrdinalIgnoreCase)
            || name.Contains("PASSWORD", StringComparison.OrdinalIgnoreCase)
            || name.Contains("CREDENTIAL", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsBlockedVendorVariable(string name)
    {
        return name.StartsWith("CODEX", StringComparison.OrdinalIgnoreCase)
            || name.StartsWith("OPENAI", StringComparison.OrdinalIgnoreCase)
            || name.StartsWith("CLAUDE", StringComparison.OrdinalIgnoreCase)
            || name.StartsWith("GITHUB", StringComparison.OrdinalIgnoreCase);
    }

    private static void AssertPathIsWithin(string root, string candidate)
    {
        var normalizedRoot = Path.GetFullPath(root).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            + Path.DirectorySeparatorChar;
        var normalizedCandidate = Path.GetFullPath(candidate).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            + Path.DirectorySeparatorChar;

        Assert.StartsWith(normalizedRoot, normalizedCandidate, StringComparison.OrdinalIgnoreCase);
    }
}
