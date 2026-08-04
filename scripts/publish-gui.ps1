param(
    [string]$Configuration = "Release",
    [string]$Runtime = "win-x64",
    [string]$Output = "artifacts\win-x64"
)

$ErrorActionPreference = "Stop"

$repoRoot = [System.IO.Path]::GetFullPath((Resolve-Path (Join-Path $PSScriptRoot "..")).Path)
$project = Join-Path $repoRoot "desktop\CodexProviderSync.App\CodexProviderSync.App.csproj"
$automationProject = Join-Path $repoRoot "desktop\CodexProviderSync.Automation\CodexProviderSync.Automation.csproj"
$automationQuickStart = Join-Path $repoRoot "docs\AUTOMATION_QUICKSTART_ZH.md"
$outputDir = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $Output))
$automationOutputDir = [System.IO.Path]::GetFullPath("$outputDir-automation")

function Assert-WorkspaceOutputPath([string]$Candidate, [string]$Label) {
    $prefix = $repoRoot.TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar
    if ($Candidate.Equals($repoRoot, [System.StringComparison]::OrdinalIgnoreCase) `
        -or -not $Candidate.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "$Label must stay inside the repository workspace: $Candidate"
    }
}

Assert-WorkspaceOutputPath $outputDir "Publish output"
Assert-WorkspaceOutputPath $automationOutputDir "Automation staging output"

if (-not (Test-Path -LiteralPath $automationQuickStart -PathType Leaf)) {
    throw "Automation quick-start document is missing: $automationQuickStart"
}

if (Test-Path $outputDir) {
    try {
        Remove-Item -Recurse -Force $outputDir
    }
    catch {
        throw "Unable to clean publish output '$outputDir'. Close CodexProviderSync.exe if it is still running, or pass -Output to publish into a different directory."
    }
}

if (Test-Path $automationOutputDir) {
    Remove-Item -Recurse -Force $automationOutputDir
}

dotnet publish $project `
    --runtime $Runtime `
    -c $Configuration `
    --self-contained true `
    -o $outputDir `
    /p:PublishSingleFile=true `
    /p:IncludeNativeLibrariesForSelfExtract=true `
    /p:EnableCompressionInSingleFile=true `
    /p:DebugType=None `
    /p:DebugSymbols=false

if ($LASTEXITCODE -ne 0) {
    throw "GUI dotnet publish failed with exit code $LASTEXITCODE"
}

try {
    dotnet publish $automationProject `
        --runtime $Runtime `
        -c $Configuration `
        --self-contained true `
        -o $automationOutputDir `
        /p:PublishSingleFile=true `
        /p:IncludeNativeLibrariesForSelfExtract=true `
        /p:EnableCompressionInSingleFile=true `
        /p:DebugType=None `
        /p:DebugSymbols=false

    if ($LASTEXITCODE -ne 0) {
        throw "Business Automation API dotnet publish failed with exit code $LASTEXITCODE"
    }

    $automationExecutable = Join-Path $automationOutputDir "CodexProviderSync.Automation.exe"
    $automationSchema = Join-Path $automationOutputDir "automation-protocol-v0.4.schema.json"
    if (-not (Test-Path -LiteralPath $automationExecutable) -or -not (Test-Path -LiteralPath $automationSchema)) {
        throw "Business Automation API publish output is incomplete."
    }
    Copy-Item -LiteralPath $automationExecutable -Destination $outputDir -Force
    Copy-Item -LiteralPath $automationSchema -Destination $outputDir -Force
    Copy-Item -LiteralPath $automationQuickStart -Destination (Join-Path $outputDir "README-AUTOMATION.zh-CN.md") -Force
}
finally {
    if (Test-Path -LiteralPath $automationOutputDir) {
        Remove-Item -Recurse -Force -LiteralPath $automationOutputDir
    }
}

Write-Host "Windows GUI and Business Automation API published to $outputDir"
