param([string]$Source = "$PSScriptRoot/../src/windows-provider-bytes.cs")
$ErrorActionPreference = "Stop"
Add-Type -Path $Source
Add-Type -TypeDefinition @'
using System;
using System.IO;
public sealed class FaultingProviderStream : FileStream {
    readonly string kind;
    int writes, syncs;
    bool wrote;
    public FaultingProviderStream(string path, string kind)
        : base(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None) { this.kind = kind; }
    public override void Write(byte[] buffer, int offset, int count) {
        wrote = true;
        if (kind == "write" || kind == "restore") {
            if (++writes == 1) {
                base.Write(buffer, offset, Math.Min(3, count));
                throw new IOException("Injected partial write failure");
            }
            if (kind == "restore") throw new IOException("Injected recovery failure");
        }
        base.Write(buffer, offset, count);
    }
    public override void Flush(bool disk) {
        if (kind == "flush" && wrote && ++syncs == 1) throw new IOException("Injected flush failure");
        base.Flush(disk);
    }
}
'@
$root = Join-Path ([IO.Path]::GetTempPath()) ("provider-bytes-native-" + [Guid]::NewGuid())
[IO.Directory]::CreateDirectory($root) | Out-Null
try {
  $file = Join-Path $root "rollout-fixture.jsonl"
  $utf8 = [Text.UTF8Encoding]::new($false)
  $header = $utf8.GetBytes('{"type":"session_meta","payload":{"model_provider":"openai"}}' + "`n")
  $old = $utf8.GetBytes('"openai"')
  $new = $utf8.GetBytes('"prov_a"')
  $offset = $utf8.GetString($header).IndexOf('"openai"')
  $tail = $utf8.GetBytes("fixture tail`n")
  [IO.File]::WriteAllBytes($file, [byte[]]($header + $tail))
  [IO.File]::SetLastWriteTimeUtc($file, [DateTime]::new(2026, 1, 2, 3, 4, 5, [DateTimeKind]::Utc))
  $s = [IO.File]::Open($file, "Open", "ReadWrite", "None")
  try {
    # Read the same native identity used by the worker, without a Node install.
    $native = [ProviderByteFile].GetMethod("GetFileInformationByHandle", [Reflection.BindingFlags]"Static,NonPublic")
    $args = [object[]]@($s.SafeFileHandle.DangerousGetHandle(), $null)
    if (-not $native.Invoke($null, $args)) { throw "Native file identity unavailable" }
    $info = $args[1]
    $dev = [string]$info.Volume
    $ino = [string](([uint64]$info.IndexHigh -shl 32) -bor [uint64]$info.IndexLow)
    $mtime = ([double]($info.WriteTime - 116444736000000000L)) / 10000
    $size = $s.Length
    $result = [ProviderByteFile]::Apply($s, $header, $old, $new, $offset, $size, $mtime, $dev, $ino, $false)
    if ($result -ne "APPLIED_IN_PLACE") { throw "Apply did not use in-place: $result" }
    $native.Invoke($null, $args) | Out-Null
    if ($args[1].WriteTime -ne $info.WriteTime) { throw "Exclusive apply changed mtime" }
    $setTime = [ProviderByteFile].GetMethod("SetFileTime", [Reflection.BindingFlags]"Static,NonPublic")
    $setTime.Invoke($null, [object[]]@($s.SafeFileHandle.DangerousGetHandle(), [IntPtr]::Zero, [IntPtr]::Zero, [DateTime]::UtcNow.ToFileTimeUtc())) | Out-Null
    [ProviderByteFile]::Apply($s, $header, $old, $new, $offset, $size, $mtime, $dev, $ino, $true) | Out-Null
    $native.Invoke($null, $args) | Out-Null
    if ($args[1].WriteTime -ne $info.WriteTime) { throw "Crash recovery did not restore original mtime" }
    [ProviderByteFile]::Apply($s, $header, $old, $new, $offset, $size, $mtime, $dev, $ino, $false) | Out-Null
    try {
      $other = [IO.File]::Open($file, "Open", "ReadWrite", "None")
      $other.Dispose()
      throw "Exclusive handle did not block another writer"
    } catch [IO.IOException] { }
    # Model a crashed forward write or interrupted rollback, then append.
    $s.Position = $offset
    $s.Write($old, 0, 3)
    $s.Position = $s.Length
    $s.Write($tail, 0, $tail.Length)
    $s.Flush($true)
    [ProviderByteFile]::Apply($s, $header, $old, $new, $offset, $size, $mtime, $dev, $ino, $true) | Out-Null
    [ProviderByteFile]::Apply($s, $header, $old, $new, $offset, $size, $mtime, $dev, $ino, $true) | Out-Null
    $s.Position = 0
    $bytes = New-Object byte[] $s.Length
    $s.Read($bytes, 0, $bytes.Length) | Out-Null
    if ($utf8.GetString($bytes) -ne $utf8.GetString([byte[]]($header + $tail + $tail))) { throw "Recovery changed body or lost append" }
    $s.Position = $offset + 1
    $s.WriteByte(33)
    $s.Flush($true)
    $rejected = $false
    try { [ProviderByteFile]::Apply($s, $header, $old, $new, $offset, $size, $mtime, $dev, $ino, $true) | Out-Null }
    catch { $rejected = $true }
    if (-not $rejected) { throw "Unknown bytes were overwritten" }
    $otherIno = [string]([uint64]::Parse($ino) + 1)
    if ([ProviderByteFile]::Apply($s, $header, $old, $new, $offset, $size, $mtime, $dev, $otherIno, $false) -ne "SKIP_CHANGED") {
      throw "Pre-write identity change was not skipped"
    }
    $s.SetLength($size - 1)
    if ([ProviderByteFile]::Apply($s, $header, $old, $new, $offset, $size, $mtime, $dev, $ino, $false) -ne "SKIP_CHANGED") {
      throw "Pre-write truncation was not skipped"
    }
    $rejected = $false
    try { [ProviderByteFile]::Apply($s, $header, $old, $new, $offset, $size, $mtime, $dev, $ino, $true) | Out-Null }
    catch { $rejected = $true }
    if (-not $rejected) { throw "Truncated recovery was not rejected" }
  } finally { $s.Dispose() }
  Write-Output "PASS: native identity, exclusive handle, in-place write, mtime, partial recovery, idempotence, append, unknown-byte rejection"
  foreach ($kind in @("write", "flush", "restore")) {
    [IO.File]::WriteAllBytes($file, [byte[]]($header + $tail))
    $s = [FaultingProviderStream]::new($file, $kind)
    try {
      $args = [object[]]@($s.SafeFileHandle.DangerousGetHandle(), $null)
      $native.Invoke($null, $args) | Out-Null
      $info = $args[1]
      $dev = [string]$info.Volume
      $ino = [string](([uint64]$info.IndexHigh -shl 32) -bor [uint64]$info.IndexLow)
      $mtime = ([double]($info.WriteTime - 116444736000000000L)) / 10000
      $failed = $false
      try { [ProviderByteFile]::Apply($s, $header, $old, $new, $offset, $size, $mtime, $dev, $ino, $false) | Out-Null }
      catch { $failed = $true }
      if (-not $failed) { throw "Injected $kind failure was ignored" }
    } finally { $s.Dispose() }
    $isOriginal = $utf8.GetString([IO.File]::ReadAllBytes($file)) -eq $utf8.GetString([byte[]]($header + $tail))
    if (($kind -ne "restore") -and (-not $isOriginal)) { throw "Immediate recovery failed for $kind" }
    if ($kind -eq "restore") {
      if ($isOriginal) { throw "Recovery failure did not leave the expected partial bytes" }
      $s = [IO.File]::Open($file, "Open", "ReadWrite", "None")
      try { [ProviderByteFile]::Apply($s, $header, $old, $new, $offset, $size, $mtime, $dev, $ino, $true) | Out-Null }
      finally { $s.Dispose() }
      if ($utf8.GetString([IO.File]::ReadAllBytes($file)) -ne $utf8.GetString([byte[]]($header + $tail))) { throw "Later recovery failed" }
    }
  }
  Write-Output "PASS: native partial-write exception, Flush failure, failed immediate recovery and later recovery"
} finally { Remove-Item -LiteralPath $root -Recurse -Force }
