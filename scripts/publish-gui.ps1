param(
    [string]$Configuration = "Release",
    [string]$Runtime = "win-x64",
    [string]$Output = "artifacts\win-x64"
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$project = Join-Path $repoRoot "desktop\CodexProviderSync.App\CodexProviderSync.App.csproj"
$outputDir = Join-Path $repoRoot $Output

if (Test-Path $outputDir) {
    try {
        Remove-Item -Recurse -Force $outputDir
    }
    catch {
        throw "Unable to clean publish output '$outputDir'. Close CodexProviderSync.exe if it is still running, or pass -Output to publish into a different directory."
    }
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
    throw "dotnet publish failed with exit code $LASTEXITCODE"
}

Write-Host "GUI build published to $outputDir"
