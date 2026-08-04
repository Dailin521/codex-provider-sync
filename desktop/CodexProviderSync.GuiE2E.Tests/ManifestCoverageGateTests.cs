using CodexProviderSync.GuiE2E;

namespace CodexProviderSync.GuiE2E.Tests;

public sealed class ManifestCoverageGateTests
{
    private const string ManifestJson = """
        {
          "schemaVersion": "0.4",
          "window": {
            "id": "window.main",
            "scenarioIds": ["gui.launch"]
          },
          "controls": [
            {
              "id": "status.refresh",
              "scenarioIds": ["gui.status.refresh", "gui.status.failure"]
            }
          ],
          "templates": [
            {
              "id": "provider.row",
              "scenarioIds": ["gui.provider.dynamic-row"]
            }
          ],
          "dialogs": [
            {
              "id": "dialog.validation",
              "scenarioIds": ["gui.provider.validation"]
            }
          ]
        }
        """;

    private static readonly string[] EveryEntry =
    [
        "window.main",
        "status.refresh",
        "provider.row",
        "dialog.validation",
    ];

    private static readonly string[] EveryScenario =
    [
        "gui.launch",
        "gui.status.refresh",
        "gui.status.failure",
        "gui.provider.dynamic-row",
        "gui.provider.validation",
    ];

    [Fact]
    public void Evaluate_PassesOnlyWhenEveryManifestEntryAndScenarioIsCovered()
    {
        var result = ManifestCoverageGate.Evaluate(ManifestJson, EveryEntry, EveryScenario);

        Assert.True(result.Passed);
        Assert.Empty(result.MissingEntryIds);
        Assert.Empty(result.MissingScenarioIds);
    }

    [Fact]
    public void Evaluate_FailsAndNamesEveryUncoveredEntry()
    {
        var coveredEntries = EveryEntry.Where(id => id is not "provider.row" and not "dialog.validation");

        var result = ManifestCoverageGate.Evaluate(ManifestJson, coveredEntries, EveryScenario);

        Assert.False(result.Passed);
        Assert.Equal(
            new[] { "dialog.validation", "provider.row" },
            result.MissingEntryIds.Order(StringComparer.Ordinal));
        Assert.Empty(result.MissingScenarioIds);
    }

    [Fact]
    public void Evaluate_FailsAndNamesEveryUncoveredScenario()
    {
        var coveredScenarios = EveryScenario.Where(id => id is not "gui.status.failure" and not "gui.provider.validation");

        var result = ManifestCoverageGate.Evaluate(ManifestJson, EveryEntry, coveredScenarios);

        Assert.False(result.Passed);
        Assert.Empty(result.MissingEntryIds);
        Assert.Equal(
            new[] { "gui.provider.validation", "gui.status.failure" },
            result.MissingScenarioIds.Order(StringComparer.Ordinal));
    }

    [Fact]
    public void Evaluate_DoesNotTreatUnknownCoverageIdsAsManifestCoverage()
    {
        var result = ManifestCoverageGate.Evaluate(
            ManifestJson,
            ["not.a.real.entry"],
            ["not.a.real.scenario"]);

        Assert.False(result.Passed);
        Assert.Equal(EveryEntry.Order(StringComparer.Ordinal), result.MissingEntryIds.Order(StringComparer.Ordinal));
        Assert.Equal(EveryScenario.Order(StringComparer.Ordinal), result.MissingScenarioIds.Order(StringComparer.Ordinal));
    }
}
