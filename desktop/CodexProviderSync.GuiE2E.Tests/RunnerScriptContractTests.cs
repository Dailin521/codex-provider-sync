namespace CodexProviderSync.GuiE2E.Tests;

public sealed class RunnerScriptContractTests
{
    [Fact]
    public void RunnerScript_UsesAnIndependentWideWatchdog_AndKillsTheProcessTree()
    {
        string script = ReadRunnerScript();

        Assert.Contains("$harnessWatchdogSeconds = [Math]::Max(300, $TimeoutSeconds * 8)", script);
        Assert.Contains("$harnessProcess.WaitForExit($harnessWatchdogMilliseconds)", script);
        Assert.Contains("taskkill.exe", script);
        Assert.Contains("/PID $Process.Id /T /F", script);
        Assert.Contains("$watchdogExitCode = 124", script);
        Assert.Contains("$harnessExit = $watchdogExitCode", script);
    }

    [Fact]
    public void RunnerScript_PreservesHarnessFailureExitCode_AndAlwaysReportsEvidencePath()
    {
        string script = ReadRunnerScript();

        Assert.Contains("$harnessExit = $harnessProcess.ExitCode", script);
        Assert.Contains("Write-Host \"GUI E2E evidence: $evidencePath\"", script);
        Assert.Contains("if ($harnessExit -ne 0)", script);
        Assert.Contains("exit $harnessExit", script);
    }

    [Fact]
    public void RunnerScript_HardGatesTheCompleteHeadfulEvidenceContract()
    {
        string script = ReadRunnerScript();

        Assert.Contains("$expectedManifestEntries = 40", script);
        Assert.Contains("$expectedRequiredScenarios = 53", script);
        Assert.Contains("$evidence.passed -eq $true", script);
        Assert.Contains("$errorRows.Count -eq 0", script);
        Assert.Contains("$blockerRows.Count -eq 0", script);
        Assert.Contains("$evidence.manifest.declaredEntryCount -eq $expectedManifestEntries", script);
        Assert.Contains("$evidence.manifest.coveredEntryCount -eq $expectedManifestEntries", script);
        Assert.Contains("$evidence.manifest.requiredHeadfulScenarioCount -eq $expectedRequiredScenarios", script);
        Assert.Contains("$evidence.manifest.passedRequiredHeadfulScenarioCount -eq $expectedRequiredScenarios", script);
        Assert.Contains("$evidence.manifest.coveragePassed -eq $true", script);
        Assert.Contains("$scenarioRows.Count -eq $expectedRequiredScenarios", script);
        Assert.Contains("$invalidScenarioRows.Count -eq 0", script);
        Assert.Contains("$skippedOrBlockedRows.Count -eq 0", script);
    }

    [Fact]
    public void RunnerScript_VerifiesPublishedExecutableSha256AgainstEvidence()
    {
        string script = ReadRunnerScript();

        Assert.Contains("Get-FileHash -LiteralPath $appExe -Algorithm SHA256", script);
        Assert.Contains("$evidence.executable.sha256", script);
        Assert.Contains("[System.StringComparison]::OrdinalIgnoreCase", script);
    }

    private static string ReadRunnerScript()
    {
        string repositoryRoot = FindRepositoryRoot();
        return File.ReadAllText(Path.Combine(repositoryRoot, "scripts", "run-windows-gui-e2e.ps1"));
    }

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "scripts", "run-windows-gui-e2e.ps1")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate the repository root from the test output directory.");
    }
}
