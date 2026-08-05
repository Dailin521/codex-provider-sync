using System.Text.Json.Nodes;

namespace CodexProviderSync.GuiE2E;

internal sealed partial class ScenarioRunner
{
    private int _failureFinalizationStarted;

    internal async Task FinalizeFailureEvidenceAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _failureFinalizationStarted, 1) != 0)
        {
            return;
        }

        await CollectExistingTraceBestEffortAsync(cancellationToken);
        MaterializeFailureCoverage();
        _evidence.Manifest["failureEvidenceFinalized"] = true;
    }

    private async Task CollectExistingTraceBestEffortAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_fixture.TracePath))
        {
            _evidence.Manifest["failureTracePresent"] = false;
            _evidence.Manifest["failureTraceRecordCount"] = _evidence.Trace.Count;
            return;
        }

        HashSet<string> recorded = _evidence.Trace
            .Where(node => node is not null)
            .Select(node => node!.ToJsonString())
            .ToHashSet(StringComparer.Ordinal);
        int malformedLines = 0;
        string? collectionError = null;
        try
        {
            foreach (string line in await File.ReadAllLinesAsync(_fixture.TracePath, cancellationToken))
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }
                try
                {
                    JsonNode trace = JsonNode.Parse(line)
                        ?? throw new InvalidDataException("Trace record was JSON null.");
                    if (recorded.Add(trace.ToJsonString()))
                    {
                        _evidence.Trace.Add(trace);
                    }
                }
                catch (Exception error) when (error is not OperationCanceledException)
                {
                    malformedLines++;
                }
            }
        }
        catch (Exception error)
        {
            collectionError = error.Message;
        }

        _evidence.Manifest["failureTracePresent"] = true;
        _evidence.Manifest["failureTraceRecordCount"] = _evidence.Trace.Count;
        _evidence.Manifest["failureTraceMalformedLineCount"] = malformedLines;
        if (collectionError is not null)
        {
            _evidence.Manifest["failureTraceCollectionError"] = collectionError;
        }
    }

    private void MaterializeFailureCoverage()
    {
        foreach ((string id, ScenarioState state) in _scenarios.Where(pair => pair.Value.Status == "not-run").ToArray())
        {
            _scenarios[id] = new ScenarioState(
                "blocked",
                "The real-GUI run aborted before this required scenario produced PASS evidence.");
        }

        string[] passed = _scenarios
            .Where(pair => pair.Value.Status == "passed")
            .Select(pair => pair.Key)
            .ToArray();
        (IReadOnlyList<string> manifestEntries, IReadOnlyList<string> manifestScenarios) =
            ManifestCoverageGate.ReadRequirements(_manifestJson);
        ManifestCoverageResult entryCoverage = ManifestCoverageGate.Evaluate(
            _manifestJson,
            _coveredEntries,
            manifestScenarios);
        string[] missingRequiredScenarios = _scenarios.Keys
            .Except(passed, StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();
        bool coveragePassed = entryCoverage.MissingEntryIds.Count == 0
            && missingRequiredScenarios.Length == 0;

        _evidence.Manifest["declaredEntryCount"] = manifestEntries.Count;
        _evidence.Manifest["coveredEntryCount"] = manifestEntries.Count - entryCoverage.MissingEntryIds.Count;
        _evidence.Manifest["requiredHeadfulScenarioCount"] = _scenarios.Count;
        _evidence.Manifest["passedRequiredHeadfulScenarioCount"] = passed.Length;
        _evidence.Manifest["nonGatingManifestScenarioCount"] = _nonGatingScenarioIds.Count;
        _evidence.Manifest["nonGatingManifestScenarioIds"] = _nonGatingScenarioIds
            .Order(StringComparer.Ordinal)
            .ToArray();
        _evidence.Manifest["coveragePassed"] = coveragePassed;
        _evidence.Manifest["missingEntryIds"] = entryCoverage.MissingEntryIds;
        _evidence.Manifest["missingRequiredHeadfulScenarioIds"] = missingRequiredScenarios;

        _evidence.Scenarios.RemoveAll(item =>
            item.TryGetValue("id", out object? id)
            && id is string scenarioId
            && _scenarios.ContainsKey(scenarioId));
        foreach ((string id, ScenarioState state) in _scenarios.OrderBy(pair => pair.Key, StringComparer.Ordinal))
        {
            _evidence.Scenario(id, state.Status, state.Evidence, _scenarioEntries[id]);
        }

        if (!coveragePassed)
        {
            string blocker =
                $"Headful gate failed: {entryCoverage.MissingEntryIds.Count} manifest entries and "
                + $"{missingRequiredScenarios.Length} required scenarios lack PASS evidence.";
            if (!_evidence.Blockers.Contains(blocker, StringComparer.Ordinal))
            {
                _evidence.Blockers.Add(blocker);
            }
        }
    }
}
