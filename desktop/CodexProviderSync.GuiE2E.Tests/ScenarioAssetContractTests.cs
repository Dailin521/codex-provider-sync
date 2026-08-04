using System.Text.Json;

namespace CodexProviderSync.GuiE2E.Tests;

public sealed class ScenarioAssetContractTests
{
    [Fact]
    public void ScenarioAsset_CoversEveryEntry_AndPartitionsRequiredHeadfulScenarios()
    {
        var repositoryRoot = FindRepositoryRoot();
        using var manifest = JsonDocument.Parse(File.ReadAllText(Path.Combine(
            repositoryRoot,
            "desktop",
            "CodexProviderSync.App",
            "Automation",
            "gui-automation-manifest.v0.4.json")));
        using var asset = JsonDocument.Parse(File.ReadAllText(Path.Combine(
            repositoryRoot,
            "desktop",
            "CodexProviderSync.GuiE2E",
            "assets",
            "gui-e2e-scenarios.v0.4.json")));

        var manifestEntries = ManifestEntries(manifest.RootElement).ToArray();
        var expectedEntries = manifestEntries.Select(entry => entry.GetProperty("id").GetString()!).ToArray();
        var expectedScenarios = manifestEntries
            .SelectMany(entry => StringArray(entry, "scenarioIds"))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        var groups = asset.RootElement.GetProperty("groups").EnumerateArray().ToArray();
        var actualEntries = groups.SelectMany(group => StringArray(group, "entryIds")).ToArray();
        var requiredScenarios = groups.SelectMany(group => StringArray(group, "scenarioIds")).ToArray();
        var nonGatingScenarios = StringArray(asset.RootElement, "nonGatingManifestScenarioIds").ToArray();

        Assert.Equal(expectedEntries.Order(StringComparer.Ordinal), actualEntries.Order(StringComparer.Ordinal));
        Assert.Equal(
            expectedScenarios.Order(StringComparer.Ordinal),
            requiredScenarios.Concat(nonGatingScenarios).Order(StringComparer.Ordinal));
        Assert.DoesNotContain(
            actualEntries.GroupBy(id => id, StringComparer.Ordinal),
            group => group.Count() != 1);
        Assert.DoesNotContain(
            requiredScenarios.GroupBy(id => id, StringComparer.Ordinal),
            group => group.Count() != 1);
        Assert.DoesNotContain(
            nonGatingScenarios.GroupBy(id => id, StringComparer.Ordinal),
            group => group.Count() != 1);
        Assert.Empty(requiredScenarios.Intersect(nonGatingScenarios, StringComparer.Ordinal));
        Assert.NotEmpty(requiredScenarios);
        Assert.NotEmpty(nonGatingScenarios);
    }

    [Fact]
    public void ScenarioAsset_TargetsTheCurrentManifestVersion()
    {
        var repositoryRoot = FindRepositoryRoot();
        using var manifest = JsonDocument.Parse(File.ReadAllText(Path.Combine(
            repositoryRoot,
            "desktop",
            "CodexProviderSync.App",
            "Automation",
            "gui-automation-manifest.v0.4.json")));
        using var asset = JsonDocument.Parse(File.ReadAllText(Path.Combine(
            repositoryRoot,
            "desktop",
            "CodexProviderSync.GuiE2E",
            "assets",
            "gui-e2e-scenarios.v0.4.json")));

        Assert.Equal(
            manifest.RootElement.GetProperty("manifestVersion").GetString(),
            asset.RootElement.GetProperty("manifestVersion").GetString());
    }

    [Fact]
    public void ScenarioAsset_ForbidsUnitTestsFromClaimingARealGuiPass()
    {
        var repositoryRoot = FindRepositoryRoot();
        using var asset = JsonDocument.Parse(File.ReadAllText(Path.Combine(
            repositoryRoot,
            "desktop",
            "CodexProviderSync.GuiE2E",
            "assets",
            "gui-e2e-scenarios.v0.4.json")));
        var policy = asset.RootElement.GetProperty("executionPolicy");

        Assert.True(policy.GetProperty("requiresWindows").GetBoolean());
        Assert.True(policy.GetProperty("requiresUserInteractive").GetBoolean());
        Assert.True(policy.GetProperty("requiresActiveDesktop").GetBoolean());
        Assert.True(policy.GetProperty("requiresVisiblePublishedExe").GetBoolean());
        Assert.True(policy.GetProperty("requiresRealControlsAndEvents").GetBoolean());
        Assert.True(policy.GetProperty("blockedIsFailure").GetBoolean());
        Assert.True(policy.GetProperty("skippedIsFailure").GetBoolean());
        Assert.False(policy.GetProperty("unitTestsMayClaimGuiPass").GetBoolean());
    }

    [Fact]
    public void ScenarioAsset_RequiresConcreteEvidenceForEveryGroup()
    {
        var repositoryRoot = FindRepositoryRoot();
        using var asset = JsonDocument.Parse(File.ReadAllText(Path.Combine(
            repositoryRoot,
            "desktop",
            "CodexProviderSync.GuiE2E",
            "assets",
            "gui-e2e-scenarios.v0.4.json")));

        foreach (var group in asset.RootElement.GetProperty("groups").EnumerateArray())
        {
            string[] requiredEvidence = StringArray(group, "requiredEvidence").ToArray();
            Assert.NotEmpty(StringArray(group, "entryIds"));
            Assert.NotEmpty(StringArray(group, "scenarioIds"));
            Assert.NotEmpty(requiredEvidence);
            Assert.Equal(requiredEvidence.Length, requiredEvidence.Distinct(StringComparer.Ordinal).Count());
            Assert.All(requiredEvidence, reference =>
                Assert.Contains(reference, ScenarioRunner.SupportedEvidenceReferences));
        }
    }

    private static IEnumerable<JsonElement> ManifestEntries(JsonElement manifest)
    {
        yield return manifest.GetProperty("window");

        foreach (var sectionName in new[] { "controls", "templates", "dialogs" })
        {
            foreach (var entry in manifest.GetProperty(sectionName).EnumerateArray())
            {
                yield return entry;
            }
        }
    }

    private static IEnumerable<string> StringArray(JsonElement owner, string propertyName)
    {
        return owner.GetProperty(propertyName).EnumerateArray().Select(value => value.GetString()!);
    }

    private static string FindRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            var manifestPath = Path.Combine(
                directory.FullName,
                "desktop",
                "CodexProviderSync.App",
                "Automation",
                "gui-automation-manifest.v0.4.json");
            if (File.Exists(manifestPath))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate the repository root from the test output directory.");
    }
}
