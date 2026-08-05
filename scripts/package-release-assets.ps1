param(
    [Parameter(Mandatory = $true)]
    [string]$Version,
    [string]$PublishOutput = "artifacts\win-x64",
    [string]$Output = "artifacts\release"
)

$ErrorActionPreference = "Stop"

if ($Version -notmatch '^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$') {
    throw "Version must be a semantic version without a leading v: $Version"
}

$repoRoot = [System.IO.Path]::GetFullPath((Resolve-Path (Join-Path $PSScriptRoot "..")).Path)
$publishDir = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $PublishOutput))
$assetRoot = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $Output))

function Assert-WorkspacePath([string]$Candidate, [string]$Label) {
    $prefix = $repoRoot.TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar
    if ($Candidate.Equals($repoRoot, [System.StringComparison]::OrdinalIgnoreCase) `
        -or -not $Candidate.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "$Label must stay inside the repository workspace: $Candidate"
    }
}

Assert-WorkspacePath $publishDir "Publish output"
Assert-WorkspacePath $assetRoot "Release asset output"

function Test-IsSameOrNestedPath([string]$Candidate, [string]$Parent) {
    $parentPrefix = $Parent.TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar
    return $Candidate.Equals($Parent, [System.StringComparison]::OrdinalIgnoreCase) `
        -or $Candidate.StartsWith($parentPrefix, [System.StringComparison]::OrdinalIgnoreCase)
}

if ((Test-IsSameOrNestedPath $assetRoot $publishDir) `
    -or (Test-IsSameOrNestedPath $publishDir $assetRoot)) {
    throw "Publish output and release asset output must be separate directory trees."
}

if (-not (Test-Path -LiteralPath $publishDir -PathType Container)) {
    throw "Publish output does not exist: $publishDir"
}

$requiredFiles = @(
    "CodexProviderSync.exe",
    "CodexProviderSync.Automation.exe",
    "automation-protocol-v0.4.schema.json",
    "README-AUTOMATION.zh-CN.md"
)
foreach ($fileName in $requiredFiles) {
    $source = Join-Path $publishDir $fileName
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "Release package input is missing: $source"
    }
}

if (Test-Path -LiteralPath $assetRoot) {
    Remove-Item -LiteralPath $assetRoot -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $assetRoot | Out-Null

$fullZip = Join-Path $assetRoot "codex-provider-sync-v$Version-win-x64.zip"
Compress-Archive -Path (Join-Path $publishDir "*") -DestinationPath $fullZip -Force
Copy-Item `
    -LiteralPath (Join-Path $publishDir "CodexProviderSync.exe") `
    -Destination (Join-Path $assetRoot "CodexProviderSync.exe") `
    -Force

$automationStage = Join-Path $assetRoot "automation-win-x64"
New-Item -ItemType Directory -Force -Path $automationStage | Out-Null
try {
    @(
        "CodexProviderSync.Automation.exe",
        "automation-protocol-v0.4.schema.json",
        "README-AUTOMATION.zh-CN.md"
    ) | ForEach-Object {
        Copy-Item `
            -LiteralPath (Join-Path $publishDir $_) `
            -Destination $automationStage `
            -Force
    }

    $automationZip = Join-Path $assetRoot "codex-provider-sync-v$Version-automation-win-x64.zip"
    Compress-Archive -Path (Join-Path $automationStage "*") -DestinationPath $automationZip -Force

    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $archive = [System.IO.Compression.ZipFile]::OpenRead($automationZip)
    try {
        $actualEntries = @($archive.Entries `
            | Where-Object { -not [string]::IsNullOrEmpty($_.Name) } `
            | ForEach-Object { $_.FullName.Replace('\', '/') } `
            | Sort-Object)
        $expectedEntries = @(
            "CodexProviderSync.Automation.exe",
            "README-AUTOMATION.zh-CN.md",
            "automation-protocol-v0.4.schema.json"
        ) | Sort-Object
        if ($actualEntries.Count -ne $expectedEntries.Count `
            -or (Compare-Object -ReferenceObject $expectedEntries -DifferenceObject $actualEntries)) {
            throw "Automation ZIP contents do not match the required three-file contract: $($actualEntries -join ', ')"
        }
    }
    finally {
        $archive.Dispose()
    }
}
finally {
    if (Test-Path -LiteralPath $automationStage) {
        Remove-Item -LiteralPath $automationStage -Recurse -Force
    }
}

$primaryAssets = @(
    Join-Path $assetRoot "CodexProviderSync.exe"
    Join-Path $assetRoot "codex-provider-sync-v$Version-automation-win-x64.zip"
    Join-Path $assetRoot "codex-provider-sync-v$Version-win-x64.zip"
)
$checksums = foreach ($asset in $primaryAssets | Sort-Object) {
    if (-not (Test-Path -LiteralPath $asset -PathType Leaf)) {
        throw "Expected release asset was not created: $asset"
    }
    $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $asset).Hash.ToLowerInvariant()
    $fileName = [System.IO.Path]::GetFileName($asset)
    "$hash  $fileName"
    Set-Content -LiteralPath "$asset.sha256" -Value "$hash  $fileName" -Encoding ASCII
}
Set-Content -LiteralPath (Join-Path $assetRoot "checksums.txt") -Value $checksums -Encoding ASCII

Write-Host "Release assets packaged to $assetRoot"
