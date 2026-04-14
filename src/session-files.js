import { execFile } from "node:child_process";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";

import { SESSION_DIRS } from "./constants.js";

const execFileAsync = promisify(execFile);

function isRolloutFileBusyError(error) {
  const message = `${error?.code ?? ""} ${error?.message ?? ""}`.toLowerCase();
  return message.includes("ebusy")
    || message.includes("resource busy or locked")
    || message.includes("being used by another process")
    || message.includes("currently in use")
    || message.includes("eperm");
}

function wrapRolloutFileBusyError(error, filePath, action) {
  if (!isRolloutFileBusyError(error)) {
    return error;
  }
  return new Error(
    `Unable to ${action} rollout file because it is currently in use. Close Codex and the Codex app, then retry. Locked file: ${filePath}`
  );
}

async function getFileSnapshot(filePath) {
  const stat = await fsp.stat(filePath);
  return {
    size: stat.size,
    mtimeMs: stat.mtimeMs
  };
}

async function restoreFileMtime(filePath, snapshot) {
  if (!snapshot || typeof snapshot.mtimeMs !== "number") {
    return;
  }

  const stat = await fsp.stat(filePath);
  await fsp.utimes(filePath, stat.atimeMs / 1000, snapshot.mtimeMs / 1000);
}

async function overwriteFileInPlace(filePath, tmpPath, snapshot) {
  const replacement = await fsp.readFile(tmpPath);
  const handle = await fsp.open(filePath, "r+");

  try {
    await handle.truncate(0);
    await handle.writeFile(replacement);
    await handle.sync();
  } finally {
    await handle.close();
  }

  await restoreFileMtime(filePath, snapshot);
}

function snapshotMatches(change, snapshot) {
  return change.originalSize === snapshot.size
    && change.originalMtimeMs === snapshot.mtimeMs;
}

async function listJsonlFiles(rootDir) {
  const entries = await fsp.readdir(rootDir, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const fullPath = path.join(rootDir, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await listJsonlFiles(fullPath)));
      continue;
    }
    if (entry.isFile() && entry.name.startsWith("rollout-") && entry.name.endsWith(".jsonl")) {
      files.push(fullPath);
    }
  }
  return files;
}

async function readFirstLineRecord(filePath) {
  let handle;
  try {
    handle = await fsp.open(filePath, "r");
    let position = 0;
    let collected = Buffer.alloc(0);
    while (true) {
      const chunk = Buffer.alloc(64 * 1024);
      const { bytesRead } = await handle.read(chunk, 0, chunk.length, position);
      if (bytesRead === 0) {
        break;
      }
      position += bytesRead;
      collected = Buffer.concat([collected, chunk.subarray(0, bytesRead)]);
      const newlineIndex = collected.indexOf(0x0a);
      if (newlineIndex !== -1) {
        const crlf = newlineIndex > 0 && collected[newlineIndex - 1] === 0x0d;
        const lineBuffer = crlf ? collected.subarray(0, newlineIndex - 1) : collected.subarray(0, newlineIndex);
        return {
          firstLine: lineBuffer.toString("utf8"),
          separator: crlf ? "\r\n" : "\n",
          offset: newlineIndex + 1
        };
      }
    }
    return {
      firstLine: collected.toString("utf8"),
      separator: "",
      offset: collected.length
    };
  } catch (error) {
    throw wrapRolloutFileBusyError(error, filePath, "read");
  } finally {
    await handle?.close();
  }
}

function parseSessionMetaRecord(firstLine) {
  if (!firstLine) {
    return null;
  }
  try {
    const parsed = JSON.parse(firstLine);
    if (parsed?.type !== "session_meta" || typeof parsed?.payload !== "object" || parsed.payload === null) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function isValidWindowsRewriteResult(result) {
  return result === "APPLIED" || result === "SKIP_BUSY" || result === "SKIP_CHANGED";
}

function parseWindowsRewriteResults(stdout, changes) {
  const trimmed = stdout.trim();
  const parsed = trimmed ? JSON.parse(trimmed) : [];
  const results = Array.isArray(parsed) ? parsed : [parsed];

  if (results.length !== changes.length) {
    throw new Error(`Unexpected rewrite result count. Expected ${changes.length}, received ${results.length}.`);
  }

  return results.map((entry, index) => {
    const expectedPath = changes[index].path;
    if (entry?.path !== expectedPath || !isValidWindowsRewriteResult(entry?.result)) {
      throw new Error(`Unexpected rewrite result for ${expectedPath}: ${JSON.stringify(entry)}`);
    }
    return entry.result;
  });
}

async function invokeWindowsExclusiveRewriteBatch(changes, { requireOriginalMatch }) {
  if (!changes.length) {
    return [];
  }

  const tempDir = await fsp.mkdtemp(path.join(os.tmpdir(), "codex-provider-rewrite-"));
  const manifestPath = path.join(tempDir, "changes.json");
  const script = `
& {
  param([string]$manifestPath)

  function Read-FirstLineRecord([System.IO.FileStream]$stream) {
    $stream.Seek(0, [System.IO.SeekOrigin]::Begin) | Out-Null
    $buffer = New-Object byte[] (64 * 1024)
    $collected = New-Object System.IO.MemoryStream
    try {
      while ($true) {
        $bytesRead = $stream.Read($buffer, 0, $buffer.Length)
        if ($bytesRead -le 0) {
          break
        }

        $collected.Write($buffer, 0, $bytesRead)
        $bytes = $collected.ToArray()
        $newlineIndex = [Array]::IndexOf($bytes, [byte]10)
        if ($newlineIndex -ge 0) {
          $crlf = $newlineIndex -gt 0 -and $bytes[$newlineIndex - 1] -eq [byte]13
          $lineLength = if ($crlf) { $newlineIndex - 1 } else { $newlineIndex }
          return @{
            firstLine = [System.Text.Encoding]::UTF8.GetString($bytes, 0, $lineLength)
            offset = $newlineIndex + 1
          }
        }
      }

      return @{
        firstLine = [System.Text.Encoding]::UTF8.GetString($collected.ToArray())
        offset = [int]$collected.Length
      }
    } finally {
      $collected.Dispose()
    }
  }

  function Invoke-RewriteChange($change) {
    $path = [string]$change.path
    $tmpPath = "$path.provider-sync.$PID.$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()).tmp"
    $encoding = [System.Text.UTF8Encoding]::new($false)
    $source = $null
    $writer = $null
    $tempReader = $null

    try {
      try {
        $source = [System.IO.File]::Open($path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
      } catch {
        if (Test-Path $path) {
          return "SKIP_BUSY"
        }
        return "SKIP_CHANGED"
      }

      if ([bool]$change.requireOriginalMatch) {
        if ($source.Length -ne [int64]$change.originalSize) {
          return "SKIP_CHANGED"
        }

        $record = Read-FirstLineRecord $source
        if ($record.firstLine -ne [string]$change.originalFirstLine -or $record.offset -ne [int]$change.originalOffset) {
          return "SKIP_CHANGED"
        }

        $separator = [string]$change.originalSeparator
        $sourceOffset = [int64]$change.originalOffset
        $headerOnly = $sourceOffset -ge [int64]$change.originalSize
      } else {
        $record = Read-FirstLineRecord $source
        $separator = [string]$change.separator
        $sourceOffset = [int64]$record.offset
        $headerOnly = $record.offset -ge $source.Length
      }

      $writer = [System.IO.File]::Open($tmpPath, [System.IO.FileMode]::Create, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
      $firstLineBytes = $encoding.GetBytes([string]$change.updatedFirstLine)
      $writer.Write($firstLineBytes, 0, $firstLineBytes.Length)

      if (-not [string]::IsNullOrEmpty($separator)) {
        $separatorBytes = $encoding.GetBytes($separator)
        $writer.Write($separatorBytes, 0, $separatorBytes.Length)
      }

      if (-not $headerOnly) {
        $source.Seek($sourceOffset, [System.IO.SeekOrigin]::Begin) | Out-Null
        $source.CopyTo($writer)
      }

      $writer.Flush()
      $writer.Dispose()
      $writer = $null

      $tempReader = [System.IO.File]::OpenRead($tmpPath)
      $source.SetLength(0)
      $source.Seek(0, [System.IO.SeekOrigin]::Begin) | Out-Null
      $tempReader.CopyTo($source)
      $source.Flush()

      return "APPLIED"
    } finally {
      if ($tempReader) {
        $tempReader.Dispose()
      }
      if ($writer) {
        $writer.Dispose()
      }
      if ($source) {
        $source.Dispose()
      }
      Remove-Item -Path $tmpPath -Force -ErrorAction SilentlyContinue
    }
  }

  $changes = Get-Content -Raw -Encoding UTF8 -Path $manifestPath | ConvertFrom-Json
  if ($null -eq $changes) {
    $changes = @()
  } elseif ($changes -is [string] -or $changes -isnot [System.Collections.IEnumerable]) {
    $changes = @($changes)
  } else {
    $changes = @($changes)
  }

  $results = @(foreach ($change in $changes) {
    [pscustomobject]@{
      path = [string]$change.path
      result = Invoke-RewriteChange $change
    }
  })

  $results | ConvertTo-Json -Compress
}
`.trim();

  try {
    await fsp.writeFile(
      manifestPath,
      JSON.stringify(changes.map((change) => ({
        ...change,
        requireOriginalMatch
      }))),
      "utf8"
    );

    const { stdout } = await execFileAsync("powershell.exe", [
      "-NoProfile",
      "-ExecutionPolicy",
      "Bypass",
      "-Command",
      script,
      manifestPath
    ], {
      maxBuffer: 16 * 1024 * 1024
    });

    return parseWindowsRewriteResults(stdout, changes);
  } catch (error) {
    throw wrapRolloutFileBusyError(error, changes[0]?.path, "rewrite");
  } finally {
    await fsp.rm(tempDir, { recursive: true, force: true });
  }
}

async function invokeWindowsExclusiveRewrite(change, options) {
  const [result] = await invokeWindowsExclusiveRewriteBatch([change], options);
  return result;
}

async function rewriteFirstLine(filePath, nextFirstLine, separator) {
  if (process.platform === "win32") {
    const snapshot = await getFileSnapshot(filePath);
    const result = await invokeWindowsExclusiveRewrite(
      {
        path: filePath,
        separator,
        updatedFirstLine: nextFirstLine
      },
      { requireOriginalMatch: false }
    );

    if (result !== "APPLIED") {
      throw new Error(
        `Unable to rewrite rollout file because it is currently in use. Close Codex and the Codex app, then retry. Locked file: ${filePath}`
      );
    }

    await restoreFileMtime(filePath, snapshot);
    return;
  }

  const snapshot = await getFileSnapshot(filePath);
  const current = await readFirstLineRecord(filePath);
  const tmpPath = `${filePath}.provider-sync.${process.pid}.${Date.now()}.tmp`;
  const writer = fs.createWriteStream(tmpPath, { encoding: "utf8" });

  try {
    await new Promise((resolve, reject) => {
      writer.on("error", reject);
      writer.write(nextFirstLine);
      if (separator) {
        writer.write(separator);
      }

      const headerOnly =
        current.separator === "" &&
        current.offset === Buffer.byteLength(current.firstLine, "utf8");

      if (headerOnly) {
        writer.end();
        writer.once("finish", resolve);
        return;
      }

      const reader = fs.createReadStream(filePath, { start: current.offset });
      reader.on("error", reject);
      reader.on("end", () => writer.end());
      writer.once("finish", resolve);
      reader.pipe(writer, { end: false });
    });

    await overwriteFileInPlace(filePath, tmpPath, snapshot);
  } catch (error) {
    await fsp.rm(tmpPath, { force: true });
    throw wrapRolloutFileBusyError(error, filePath, "rewrite");
  } finally {
    await fsp.rm(tmpPath, { force: true });
  }
}

async function tryRewriteCollectedFirstLine(change) {
  const beforeSnapshot = await getFileSnapshot(change.path);
  if (!snapshotMatches(change, beforeSnapshot)) {
    return false;
  }

  const current = await readFirstLineRecord(change.path);
  if (current.firstLine !== change.originalFirstLine || current.offset !== change.originalOffset) {
    return false;
  }

  const tmpPath = `${change.path}.provider-sync.${process.pid}.${Date.now()}.tmp`;
  const writer = fs.createWriteStream(tmpPath, { encoding: "utf8" });

  try {
    await new Promise((resolve, reject) => {
      writer.on("error", reject);
      writer.write(change.updatedFirstLine);
      if (change.originalSeparator) {
        writer.write(change.originalSeparator);
      }

      const headerOnly = change.originalOffset >= change.originalSize;
      if (headerOnly) {
        writer.end();
        writer.once("finish", resolve);
        return;
      }

      const reader = fs.createReadStream(change.path, { start: change.originalOffset });
      reader.on("error", reject);
      reader.on("end", () => writer.end());
      writer.once("finish", resolve);
      reader.pipe(writer, { end: false });
    });

    const afterSnapshot = await getFileSnapshot(change.path);
    if (!snapshotMatches(change, afterSnapshot)) {
      await fsp.rm(tmpPath, { force: true });
      return false;
    }

    await overwriteFileInPlace(change.path, tmpPath, beforeSnapshot);
    return true;
  } catch (error) {
    await fsp.rm(tmpPath, { force: true });
    throw wrapRolloutFileBusyError(error, change.path, "rewrite");
  } finally {
    await fsp.rm(tmpPath, { force: true });
  }
}

async function findLockedFilesOnWindows(filePaths) {
  if (!filePaths.length) {
    return [];
  }
  const tempDir = await fsp.mkdtemp(path.join(os.tmpdir(), "codex-provider-locks-"));
  const manifestPath = path.join(tempDir, "paths.json");
  const script = `
& {
  param([string]$manifestPath)
  $paths = Get-Content -Raw -Encoding UTF8 -Path $manifestPath | ConvertFrom-Json
  foreach ($path in $paths) {
    try {
      $stream = [System.IO.File]::Open($path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
      $stream.Close()
    } catch {
      Write-Output $path
    }
  }
}
`.trim();

  try {
    await fsp.writeFile(manifestPath, JSON.stringify(filePaths), "utf8");
    const { stdout } = await execFileAsync("powershell.exe", [
      "-NoProfile",
      "-ExecutionPolicy",
      "Bypass",
      "-Command",
      script,
      manifestPath
    ]);
    return stdout
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);
  } catch (error) {
    throw new Error(`Unable to verify rollout file locks on Windows. ${error.message}`);
  } finally {
    await fsp.rm(tempDir, { recursive: true, force: true });
  }
}

export async function collectSessionChanges(codexHome, targetProvider, options = {}) {
  const {
    skipLockedReads = false
  } = options;
  const summaries = [];
  const lockedPaths = [];
  const providerCounts = {
    sessions: new Map(),
    archived_sessions: new Map()
  };

  for (const dirName of SESSION_DIRS) {
    const rootDir = path.join(codexHome, dirName);
    try {
      await fsp.access(rootDir);
    } catch {
      continue;
    }
    const rolloutPaths = await listJsonlFiles(rootDir);
    for (const rolloutPath of rolloutPaths) {
      let record;
      try {
        record = await readFirstLineRecord(rolloutPath);
      } catch (error) {
        if (skipLockedReads && isRolloutFileBusyError(error)) {
          lockedPaths.push(rolloutPath);
          continue;
        }
        throw error;
      }
      const parsed = parseSessionMetaRecord(record.firstLine);
      if (!parsed) {
        continue;
      }
      const currentProvider = parsed.payload.model_provider ?? "(missing)";
      providerCounts[dirName].set(currentProvider, (providerCounts[dirName].get(currentProvider) ?? 0) + 1);

      if (targetProvider !== "__status_only__" && parsed.payload.model_provider !== targetProvider) {
        const snapshot = await getFileSnapshot(rolloutPath);
        parsed.payload.model_provider = targetProvider;
        summaries.push({
          path: rolloutPath,
          threadId: parsed.payload.id ?? null,
          directory: dirName,
          originalFirstLine: record.firstLine,
          originalSeparator: record.separator,
          originalOffset: record.offset,
          originalSize: snapshot.size,
          originalMtimeMs: snapshot.mtimeMs,
          updatedFirstLine: JSON.stringify(parsed)
        });
      }
    }
  }

  return { changes: summaries, lockedPaths, providerCounts };
}

export async function applySessionChanges(changes) {
  const normalizedChanges = changes ?? [];
  const skippedPaths = [];
  const appliedPaths = [];
  let appliedChanges = 0;

  if (process.platform === "win32") {
    const results = await invokeWindowsExclusiveRewriteBatch(normalizedChanges, { requireOriginalMatch: true });
    for (let index = 0; index < normalizedChanges.length; index += 1) {
      if (results[index] === "APPLIED") {
        appliedChanges += 1;
        appliedPaths.push(normalizedChanges[index].path);
        await restoreFileMtime(normalizedChanges[index].path, normalizedChanges[index]);
      } else {
        skippedPaths.push(normalizedChanges[index].path);
      }
    }
  } else {
    for (const change of normalizedChanges) {
      if (await tryRewriteCollectedFirstLine(change)) {
        appliedChanges += 1;
        appliedPaths.push(change.path);
      } else {
        skippedPaths.push(change.path);
      }
    }
  }

  appliedPaths.sort((left, right) => left.localeCompare(right));
  skippedPaths.sort((left, right) => left.localeCompare(right));
  return {
    appliedChanges,
    appliedPaths,
    skippedPaths
  };
}

export async function assertSessionFilesWritable(changes) {
  if (!changes?.length || process.platform !== "win32") {
    return;
  }

  const lockedPaths = await findLockedFilesOnWindows(changes.map((change) => change.path));
  if (lockedPaths.length === 0) {
    return;
  }

  const preview = lockedPaths.slice(0, 5).join(", ");
  const extraCount = lockedPaths.length - Math.min(lockedPaths.length, 5);
  const suffix = extraCount > 0 ? ` (+${extraCount} more)` : "";
  throw new Error(
    `Unable to rewrite rollout files because ${lockedPaths.length} file(s) are currently in use. Close Codex and the Codex app, then retry. Locked file(s): ${preview}${suffix}`
  );
}

export async function splitLockedSessionChanges(changes) {
  if (!changes?.length || process.platform !== "win32") {
    return {
      writableChanges: changes ?? [],
      lockedChanges: []
    };
  }

  const lockedPaths = new Set(await findLockedFilesOnWindows(changes.map((change) => change.path)));
  if (lockedPaths.size === 0) {
    return {
      writableChanges: changes,
      lockedChanges: []
    };
  }

  const writableChanges = [];
  const lockedChanges = [];
  for (const change of changes) {
    if (lockedPaths.has(change.path)) {
      lockedChanges.push(change);
    } else {
      writableChanges.push(change);
    }
  }

  return {
    writableChanges,
    lockedChanges
  };
}

export async function restoreSessionChanges(manifestEntries) {
  if (!manifestEntries?.length) {
    return;
  }

  if (process.platform === "win32") {
    const changes = manifestEntries.map((entry) => ({
      path: entry.path,
      separator: entry.originalSeparator ?? "\n",
      updatedFirstLine: entry.originalFirstLine
    }));
    const snapshots = await Promise.all(changes.map(async (change) => ({
      path: change.path,
      snapshot: await getFileSnapshot(change.path)
    })));
    const results = await invokeWindowsExclusiveRewriteBatch(changes, { requireOriginalMatch: false });
    const firstFailureIndex = results.findIndex((result) => result !== "APPLIED");
    if (firstFailureIndex !== -1) {
      const filePath = changes[firstFailureIndex].path;
      throw new Error(
        `Unable to rewrite rollout file because it is currently in use. Close Codex and the Codex app, then retry. Locked file: ${filePath}`
      );
    }
    await Promise.all(
      snapshots.map(({ path: targetPath, snapshot }) => restoreFileMtime(targetPath, snapshot))
    );
    return;
  }

  for (const entry of manifestEntries) {
    await rewriteFirstLine(entry.path, entry.originalFirstLine, entry.originalSeparator ?? "\n");
  }
}

export function summarizeProviderCounts(providerCounts) {
  const result = {};
  for (const [scope, counts] of Object.entries(providerCounts)) {
    result[scope] = Object.fromEntries([...counts.entries()].sort(([left], [right]) => left.localeCompare(right)));
  }
  return result;
}
