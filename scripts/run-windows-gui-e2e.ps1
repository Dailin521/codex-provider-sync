param(
    [string]$Runtime = "win-x64",
    [string]$ArtifactsRoot = "artifacts\gui-e2e",
    [int]$TimeoutSeconds = 90
)

$ErrorActionPreference = "Stop"

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw "Real WinForms GUI E2E requires Windows. This is a FAIL, never a skip."
}
if (-not [Environment]::UserInteractive) {
    throw "Real WinForms GUI E2E requires Environment.UserInteractive=true. This is a FAIL, never a skip."
}
if ($TimeoutSeconds -lt 15 -or $TimeoutSeconds -gt 600) {
    throw "TimeoutSeconds must be between 15 and 600."
}

function ConvertTo-WindowsProcessArgument {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string]$Value
    )

    if ($Value.Length -gt 0 -and $Value -notmatch '[\s"]') {
        return $Value
    }

    $builder = [System.Text.StringBuilder]::new()
    [void]$builder.Append('"')
    $backslashes = 0
    foreach ($character in $Value.ToCharArray()) {
        if ($character -eq '\') {
            $backslashes++
            continue
        }
        if ($character -eq '"') {
            [void]$builder.Append(('\' * (($backslashes * 2) + 1)))
            [void]$builder.Append('"')
            $backslashes = 0
            continue
        }
        if ($backslashes -gt 0) {
            [void]$builder.Append(('\' * $backslashes))
            $backslashes = 0
        }
        [void]$builder.Append($character)
    }
    if ($backslashes -gt 0) {
        [void]$builder.Append(('\' * ($backslashes * 2)))
    }
    [void]$builder.Append('"')
    return $builder.ToString()
}

function Stop-GuiE2EProcessTree {
    param(
        [Parameter(Mandatory = $true)]
        [System.Diagnostics.Process]$Process
    )

    $Process.Refresh()
    if ($Process.HasExited) {
        return
    }

    $taskkill = Join-Path $env:SystemRoot "System32\taskkill.exe"
    if (Test-Path -LiteralPath $taskkill) {
        try {
            & $taskkill /PID $Process.Id /T /F | Out-Host
        }
        catch {
            Write-Warning "taskkill could not terminate the full GUI E2E process tree: $($_.Exception.Message)"
        }
    }
    $Process.Refresh()
    if (-not $Process.HasExited) {
        try {
            $Process.Kill($true)
        }
        catch {
            $Process.Refresh()
            if (-not $Process.HasExited) {
                $Process.Kill()
            }
        }
    }
    [void]$Process.WaitForExit(10000)
}

function Assert-EvidenceCondition {
    param(
        [Parameter(Mandatory = $true)]
        [bool]$Condition,
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    if (-not $Condition) {
        throw "GUI E2E evidence gate failed: $Message"
    }
}

$repoRoot = [System.IO.Path]::GetFullPath((Resolve-Path (Join-Path $PSScriptRoot "..")).Path)
$artifactsBase = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $ArtifactsRoot))
$workspacePrefix = $repoRoot.TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar
if ($artifactsBase.Equals($repoRoot, [System.StringComparison]::OrdinalIgnoreCase) `
    -or -not $artifactsBase.StartsWith($workspacePrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "ArtifactsRoot must remain inside the repository workspace: $artifactsBase"
}

$runId = "{0}-{1}" -f (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ"), ([Guid]::NewGuid().ToString("N"))
$runDirectory = Join-Path $artifactsBase $runId
$publishDirectory = Join-Path $runDirectory "publish"
$harnessDirectory = Join-Path $runDirectory "harness"
$isolationRoot = Join-Path $runDirectory "isolation"
$evidencePath = Join-Path $runDirectory "evidence.json"
$relativePublish = $publishDirectory.Substring($workspacePrefix.Length)

New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null

Write-Host "Publishing the real Windows GUI in Release mode..."
& (Join-Path $PSScriptRoot "publish-gui.ps1") `
    -Configuration Release `
    -Runtime $Runtime `
    -Output $relativePublish
if ($LASTEXITCODE -ne 0) {
    throw "Release GUI publish failed with exit code $LASTEXITCODE."
}

$appExe = Join-Path $publishDirectory "CodexProviderSync.exe"
$manifest = Join-Path $publishDirectory "Automation\gui-automation-manifest.v0.4.json"
if (-not (Test-Path -LiteralPath $appExe) -or -not (Test-Path -LiteralPath $manifest)) {
    throw "Release publish did not contain CodexProviderSync.exe and its GUI automation manifest."
}

Write-Host "Building infrastructure contract tests in Release mode..."
dotnet test (Join-Path $repoRoot "desktop\CodexProviderSync.GuiE2E.Tests\CodexProviderSync.GuiE2E.Tests.csproj") `
    -c Release `
    --nologo
if ($LASTEXITCODE -ne 0) {
    throw "GUI E2E infrastructure tests failed with exit code $LASTEXITCODE."
}

Write-Host "Publishing the headful GUI E2E driver in Release mode..."
dotnet publish (Join-Path $repoRoot "desktop\CodexProviderSync.GuiE2E\CodexProviderSync.GuiE2E.csproj") `
    -c Release `
    --runtime $Runtime `
    --self-contained true `
    -o $harnessDirectory `
    /p:PublishSingleFile=true `
    /p:IncludeNativeLibrariesForSelfExtract=true `
    /p:DebugType=None `
    /p:DebugSymbols=false
if ($LASTEXITCODE -ne 0) {
    throw "GUI E2E driver publish failed with exit code $LASTEXITCODE."
}

$harnessExe = Join-Path $harnessDirectory "CodexProviderSync.GuiE2E.exe"
$scenarios = Join-Path $harnessDirectory "assets\gui-e2e-scenarios.v0.4.json"
if (-not (Test-Path -LiteralPath $harnessExe) -or -not (Test-Path -LiteralPath $scenarios)) {
    throw "GUI E2E driver publish output is incomplete."
}

Write-Host "Launching the visible Release EXE and driving real controls/events..."
$harnessArguments = [string[]]@(
    "--exe", $appExe,
    "--manifest", $manifest,
    "--scenarios", $scenarios,
    "--evidence", $evidencePath,
    "--root", $isolationRoot,
    "--timeout-seconds", $TimeoutSeconds.ToString([System.Globalization.CultureInfo]::InvariantCulture)
)
$startInfo = [System.Diagnostics.ProcessStartInfo]::new()
$startInfo.FileName = $harnessExe
$startInfo.UseShellExecute = $false
$startInfo.CreateNoWindow = $false
if ($null -ne $startInfo.PSObject.Properties["ArgumentList"]) {
    foreach ($argument in $harnessArguments) {
        [void]$startInfo.ArgumentList.Add($argument)
    }
}
else {
    $startInfo.Arguments = ($harnessArguments | ForEach-Object { ConvertTo-WindowsProcessArgument $_ }) -join " "
}

$harnessWatchdogSeconds = [Math]::Max(300, $TimeoutSeconds * 8)
$harnessWatchdogMilliseconds = [int]($harnessWatchdogSeconds * 1000)
$watchdogExitCode = 124
$harnessProcess = [System.Diagnostics.Process]::Start($startInfo)
$completedWithinWatchdog = $harnessProcess.WaitForExit($harnessWatchdogMilliseconds)
if (-not $completedWithinWatchdog) {
    $harnessProcess.Refresh()
    if ($harnessProcess.HasExited) {
        $completedWithinWatchdog = $true
    }
}

if ($completedWithinWatchdog) {
    $harnessExit = $harnessProcess.ExitCode
}
else {
    Write-Host "GUI E2E watchdog expired after $harnessWatchdogSeconds seconds; terminating the harness and GUI process tree." -ForegroundColor Red
    Stop-GuiE2EProcessTree -Process $harnessProcess
    $harnessExit = $watchdogExitCode
}
$harnessProcess.Dispose()

Write-Host "GUI E2E evidence: $evidencePath"
Write-Host "GUI E2E isolated root: $isolationRoot"
if ($harnessExit -ne 0) {
    Write-Host "Real Windows GUI E2E failed with exit code $harnessExit. Inspect machine-readable evidence at $evidencePath" -ForegroundColor Red
    exit $harnessExit
}

$evidenceGateFailure = $null
try {
    Assert-EvidenceCondition (Test-Path -LiteralPath $evidencePath -PathType Leaf) `
        "the successful harness did not create evidence.json at $evidencePath"

    $evidence = Get-Content -Raw -LiteralPath $evidencePath | ConvertFrom-Json
    $expectedManifestEntries = 40
    $expectedRequiredScenarios = 53
    $errorRows = @($evidence.errors)
    $blockerRows = @($evidence.blockers)
    $scenarioRows = @($evidence.scenarios)
    $invalidScenarioRows = @($scenarioRows | Where-Object { $_.status -ne "passed" })
    $skippedOrBlockedRows = @($scenarioRows | Where-Object { $_.status -in @("skipped", "blocked") })

    Assert-EvidenceCondition ($evidence.passed -eq $true) "passed was not true"
    Assert-EvidenceCondition ($errorRows.Count -eq 0) "errors was not empty"
    Assert-EvidenceCondition ($blockerRows.Count -eq 0) "blockers was not empty"
    Assert-EvidenceCondition ($evidence.manifest.declaredEntryCount -eq $expectedManifestEntries) `
        "declaredEntryCount was not $expectedManifestEntries"
    Assert-EvidenceCondition ($evidence.manifest.coveredEntryCount -eq $expectedManifestEntries) `
        "coveredEntryCount was not $expectedManifestEntries"
    Assert-EvidenceCondition ($evidence.manifest.requiredHeadfulScenarioCount -eq $expectedRequiredScenarios) `
        "requiredHeadfulScenarioCount was not $expectedRequiredScenarios"
    Assert-EvidenceCondition ($evidence.manifest.passedRequiredHeadfulScenarioCount -eq $expectedRequiredScenarios) `
        "passedRequiredHeadfulScenarioCount was not $expectedRequiredScenarios"
    Assert-EvidenceCondition ($evidence.manifest.coveragePassed -eq $true) "coveragePassed was not true"
    Assert-EvidenceCondition ($scenarioRows.Count -eq $expectedRequiredScenarios) `
        "scenario row count was not $expectedRequiredScenarios"
    Assert-EvidenceCondition ($invalidScenarioRows.Count -eq 0) "one or more required scenarios did not have status=passed"
    Assert-EvidenceCondition ($skippedOrBlockedRows.Count -eq 0) "one or more required scenarios were skipped or blocked"
    Assert-EvidenceCondition ((@($scenarioRows.id | Sort-Object -Unique)).Count -eq $expectedRequiredScenarios) `
        "required scenario IDs were missing or duplicated"

    $publishedExeSha256 = (Get-FileHash -LiteralPath $appExe -Algorithm SHA256).Hash.ToLowerInvariant()
    $evidenceExeSha256 = [string]$evidence.executable.sha256
    Assert-EvidenceCondition (-not [string]::IsNullOrWhiteSpace($evidenceExeSha256)) `
        "executable.sha256 was missing"
    Assert-EvidenceCondition ($publishedExeSha256.Equals($evidenceExeSha256, [System.StringComparison]::OrdinalIgnoreCase)) `
        "published EXE SHA-256 did not match executable.sha256"
}
catch {
    $evidenceGateFailure = $_.Exception.Message
}

if ($null -ne $evidenceGateFailure) {
    Write-Host $evidenceGateFailure -ForegroundColor Red
    Write-Host "Harness exited successfully, but its machine-readable evidence failed the release gate. Inspect $evidencePath" -ForegroundColor Red
    exit 1
}

Write-Host "Real Windows GUI E2E PASS"
exit 0
