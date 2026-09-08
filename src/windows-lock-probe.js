// This private storage protocol returns indices, never paths: Windows console
// code pages must not turn a locked Unicode filename into a writable target.
export const WINDOWS_LOCK_PROBE_SCRIPT = `
& {
  param([string]$manifestPath)
  $ErrorActionPreference = 'Stop'
  [string[]]$paths = Get-Content -Raw -Encoding UTF8 -LiteralPath $manifestPath | ConvertFrom-Json
  $lockedIndices = New-Object 'System.Collections.Generic.List[int]'
  for ($index = 0; $index -lt $paths.Count; $index++) {
    try {
      $stream = [System.IO.File]::Open($paths[$index], [System.IO.FileMode]::Open, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
      $stream.Close()
    } catch {
      $lockedIndices.Add($index)
    }
  }
  [Console]::Out.WriteLine(([ordered]@{
    schemaVersion = 1
    checkedCount = $paths.Count
    lockedIndices = @($lockedIndices.ToArray())
  } | ConvertTo-Json -Compress))
}
`.trim();

export function parseWindowsLockProbeResult(stdout, filePaths) {
  let result;
  try {
    result = JSON.parse(stdout);
  } catch {
    throw new Error("Invalid Windows rollout lock probe response.");
  }
  if (!result || result.schemaVersion !== 1 || result.checkedCount !== filePaths.length
    || !Array.isArray(result.lockedIndices)
    || Object.keys(result).length !== 3
    || result.lockedIndices.some((index) => !Number.isSafeInteger(index) || index < 0 || index >= filePaths.length)
    || new Set(result.lockedIndices).size !== result.lockedIndices.length) {
    throw new Error("Incomplete or invalid Windows rollout lock probe response.");
  }
  return result.lockedIndices.map((index) => filePaths[index]);
}
