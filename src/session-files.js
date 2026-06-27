import { execFile } from "node:child_process";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import readline from "node:readline";
import { promisify } from "node:util";

import { SESSION_DIRS } from "./constants.js";

const execFileAsync = promisify(execFile);
const ROLLOUT_SCAN_CHUNK_BYTES = 1024 * 1024;

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

function snapshotMatches(change, snapshot) {
  return change.originalSize === snapshot.size
    && change.originalMtimeMs === snapshot.mtimeMs;
}

function emptyEncryptedContentCounts() {
  return {
    sessions: {},
    archived_sessions: {}
  };
}

function incrementPlainCount(counts, directory, provider) {
  counts[directory][provider] = (counts[directory][provider] ?? 0) + 1;
}

function streamContainsText(filePath, text, startOffset) {
  const needle = Buffer.from(text);
  const safeStartOffset = Math.max(0, startOffset ?? 0);

  return new Promise((resolve, reject) => {
    let previous = Buffer.alloc(0);
    let settled = false;
    const stream = fs.createReadStream(filePath, {
      start: safeStartOffset,
      highWaterMark: ROLLOUT_SCAN_CHUNK_BYTES
    });

    function settle(value, error) {
      if (settled) {
        return;
      }
      settled = true;
      if (error) {
        reject(wrapRolloutFileBusyError(error, filePath, "scan"));
        return;
      }
      resolve(value);
    }

    stream.on("data", (chunk) => {
      const buffer = previous.length ? Buffer.concat([previous, chunk]) : chunk;
      if (buffer.indexOf(needle) !== -1) {
        settle(true);
        stream.destroy();
        return;
      }

      const keepBytes = Math.max(0, needle.length - 1);
      previous = keepBytes > 0
        ? buffer.subarray(Math.max(0, buffer.length - keepBytes))
        : Buffer.alloc(0);
    });
    stream.on("end", () => settle(false));
    stream.on("error", (error) => {
      if (settled) {
        return;
      }
      settle(false, error);
    });
  });
}

async function fileHasEncryptedContent(filePath, firstLine, startOffset) {
  if (firstLine.includes("encrypted_content")) {
    return true;
  }
  return streamContainsText(filePath, "encrypted_content", startOffset);
}

function recordHasUserEvent(record) {
  if (!record || typeof record !== "object") {
    return false;
  }
  if (record.type === "event_msg" && record.payload?.type === "user_message") {
    return true;
  }

  for (const key of ["payload", "item", "msg"]) {
    const value = record[key];
    if (value?.type === "message" && value.role === "user") {
      return true;
    }
  }

  return false;
}

function toDesktopWorkspacePath(value) {
  if (typeof value !== "string") {
    return value;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return value;
  }

  const extendedUnc = trimmed.match(/^\\\\\?\\UNC\\(.+)$/i);
  if (extendedUnc) {
    return `\\\\${extendedUnc[1]}`.replace(/\//g, "\\");
  }

  const extendedDrive = trimmed.match(/^\\\\\?\\([A-Za-z]:)(?:[\\/](.*))?$/);
  if (extendedDrive) {
    const [, drive, rest] = extendedDrive;
    return rest && rest.length > 0
      ? `${drive}\\${rest.replace(/\//g, "\\")}`
      : `${drive}\\`;
  }

  if (trimmed.startsWith("\\\\?\\")) {
    return trimmed.slice(4).replace(/\//g, "\\");
  }

  return value;
}

async function fileHasUserEvent(filePath, firstLine, startOffset) {
  try {
    if (recordHasUserEvent(JSON.parse(firstLine))) {
      return true;
    }
  } catch {
    // Keep scanning the rest of the rollout below.
  }

  const stream = fs.createReadStream(filePath, {
    encoding: "utf8",
    start: Math.max(0, startOffset ?? 0),
    highWaterMark: ROLLOUT_SCAN_CHUNK_BYTES
  });
  const lines = readline.createInterface({
    input: stream,
    crlfDelay: Infinity
  });

  try {
    for await (const line of lines) {
      if (!line) {
        continue;
      }
      try {
        if (recordHasUserEvent(JSON.parse(line))) {
          return true;
        }
      } catch {
        // Ignore malformed non-metadata lines; provider sync only needs positive evidence.
      }
    }
    return false;
  } catch (error) {
    throw wrapRolloutFileBusyError(error, filePath, "scan");
  } finally {
    lines.close();
    stream.destroy();
  }
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

// Scan the start of a rollout file looking for the first `turn_context`
// event and return its `payload.model` field. This is the field that the
// Codex GUI bottom-right uses to label old conversations, so we have to
// rewrite it (along with `payload.collaboration_mode.settings.model`) on
// every sync in addition to the per-thread SQLite `model` column.
//
// We stream line-by-line because individual `turn_context` lines
// can easily exceed 64 KB once Codex includes the
// `developer_instructions` blob — the previous code that capped
// the read at 64 KB silently missed those, which made the
// rollout model rewrite a no-op for sessions whose first turn
// was a long planning step. We deliberately extract `payload.model`
// with a regex on the raw JSON text instead of `JSON.parse`-ing
// the entire line: Codex writes opaque multi-KB strings
// (`developer_instructions`, raw tool output, etc.) into the
// payload, and round-tripping those through `JSON.parse` ->
// `JSON.stringify` would silently mangle embedded escape
// sequences. A regex anchored on `"type":"turn_context"` and
// then the first `"model":"<value>"` that follows within the
// same line is enough to pick up the model field of the first
// `turn_context` event we see, because rollout lines are
// single JSON objects.
async function readFirstTurnContextModel(rolloutPath, { firstLineOffset, firstLineLength } = {}) {
  let handle;
  try {
    handle = await fsp.open(rolloutPath, "r");
    const headerSkip = (firstLineOffset ?? 0) + (firstLineLength ?? 0);
    const stream = handle.createReadStream({ encoding: "utf8", start: headerSkip });
    const reader = readline.createInterface({ input: stream, crlfDelay: Infinity });

    const turnContextStart = /"type"\s*:\s*"turn_context"/;
    const modelField = /"model"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"/;

    try {
      for await (const line of reader) {
        if (!turnContextStart.test(line)) {
          continue;
        }
        const match = line.match(modelField);
        if (match && match[1].length > 0) {
          return match[1];
        }
      }
      return null;
    } finally {
      reader.close();
      stream.destroy();
    }
  } catch (error) {
    throw wrapRolloutFileBusyError(error, rolloutPath, "read");
  } finally {
    await handle?.close();
  }
}

// Replace `"model":"oldModel"` with `"model":"newModel"` in lines that
// represent a `turn_context` event. We intentionally do a per-line
// regex rewrite (rather than re-serializing the full JSON tree) because
// rollout files can be tens of megabytes, and Codex writes a lot of
// opaque payload (e.g. `developer_instructions`) that round-tripping
// through `JSON.parse`+`JSON.stringify` would silently mangle.
function rewriteTurnContextModelInLine(line, oldModel, newModel) {
  if (!line || !line.includes('"turn_context"')) {
    return line;
  }
  // We need TWO escapes here:
  //   - JSON-string escape so the old/new model name can be embedded
  //     back into a JSON string value (handle `\` and `"`).
  //   - regex escape so model names that happen to contain regex
  //     metacharacters (`.`, `+`, `*`, `?`, `(`, `)`, `|`, `[`, `]`,
  //     `{`, `}`, `^`, `$`, `\`) do not over-match or break the
  //     pattern. Without this, a model named `gpt-5.4-mini` would
  //     also match `gpt-5X4Xmini` and a model named `foo+bar` would
  //     either fail to compile or behave unexpectedly.
  // The order matters: regex-escape first so the JSON-string
  // escape of `\` (which inserts a backslash before every `\`) does
  // not double up the regex escapes we just inserted.
  const jsonEscape = (value) => value.replace(/\\/g, "\\\\").replace(/"/g, "\\\"");
  const regexEscape = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const pattern = new RegExp(`"model"\\s*:\\s*"${regexEscape(jsonEscape(oldModel))}"`, "g");
  return line.replace(pattern, `"model":"${jsonEscape(newModel)}"`);
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

async function restoreOriginalMtime(filePath, mtimeMs) {
  if (!Number.isFinite(mtimeMs)) {
    return;
  }
  const mtime = new Date(mtimeMs);
  try {
    const stat = await fsp.stat(filePath);
    await fsp.utimes(filePath, stat.atime, mtime);
  } catch {
    // Best effort only; rewriting metadata is still the primary operation.
  }
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

    return;
  }

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

    await fsp.rename(tmpPath, filePath);
  } catch (error) {
    await fsp.rm(tmpPath, { force: true });
    throw wrapRolloutFileBusyError(error, filePath, "rewrite");
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

    await fsp.rename(tmpPath, change.path);
    return true;
  } catch (error) {
    await fsp.rm(tmpPath, { force: true });
    throw wrapRolloutFileBusyError(error, change.path, "rewrite");
  }
}

// Rewrite the per-turn `model` field in every `turn_context` event of
// the rollout. This is what the Codex GUI bottom-right of an old
// conversation reads, so we have to keep it in sync with the
// root-level `model` from config.toml on every sync, not just the
// per-thread SQLite `model` column. We do this as a separate
// line-by-line pass (rather than re-serializing the whole JSON tree)
// to avoid round-tripping the multi-MB `developer_instructions` blob
// Codex writes into every `turn_context`, which can lose embedded
// backslashes or escape sequences when run through `JSON.stringify`.
async function rewriteRolloutModelField(change, targetModel) {
  if (!change || typeof change.originalModel !== "string" || change.originalModel.length === 0) {
    return false;
  }
  if (typeof targetModel !== "string" || targetModel.length === 0) {
    return false;
  }
  if (change.originalModel === targetModel) {
    return false;
  }

  const filePath = change.path;
  // Snapshot the file as it stands after the first-line rewrite so
  // we can detect concurrent appends by Codex while we read+rewrite.
  // The original `change` snapshot no longer matches because the
  // first-line rewrite already mutated size and mtime, so we
  // intentionally don't compare to `change.originalSize` here.
  const beforeStat = await fsp.stat(filePath);
  const beforeSnapshot = {
    size: beforeStat.size,
    mtimeMs: beforeStat.mtimeMs
  };

  let handle;
  try {
    handle = await fsp.open(filePath, "r+");
    const stream = handle.createReadStream({ encoding: "utf8" });
    const reader = readline.createInterface({ input: stream, crlfDelay: Infinity });
    const tmpPath = `${filePath}.provider-sync-model.${process.pid}.${Date.now()}.tmp`;
    const writer = fs.createWriteStream(tmpPath, { encoding: "utf8" });
    let firstLine = true;
    let replacements = 0;

    await new Promise((resolve, reject) => {
      reader.on("error", reject);
      writer.on("error", reject);
      reader.on("line", (line) => {
        const next = firstLine
          ? line
          : rewriteTurnContextModelInLine(line, change.originalModel, targetModel);
        if (next !== line) {
          replacements += 1;
        }
        if (!firstLine) {
          writer.write("\n");
        }
        firstLine = false;
        writer.write(next);
      });
      reader.on("close", () => {
        writer.end();
      });
      writer.on("finish", resolve);
    });

    if (replacements === 0) {
      await fsp.rm(tmpPath, { force: true });
      return false;
    }

    // Refuse to swap in the new file if Codex appended anything
    // between our snapshot and the rename — otherwise we would
    // silently drop those trailing events.
    const afterStat = await fsp.stat(filePath);
    if (afterStat.size !== beforeSnapshot.size || afterStat.mtimeMs !== beforeSnapshot.mtimeMs) {
      await fsp.rm(tmpPath, { force: true });
      return false;
    }

    await fsp.rename(tmpPath, filePath);
    return true;
  } catch (error) {
    throw wrapRolloutFileBusyError(error, filePath, "rewrite model field");
  } finally {
    await handle?.close();
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
    skipLockedReads = false,
    // Optional target model. When set, rollouts whose per-turn
    // `turn_context.model` already matches this value are skipped
    // entirely — no first-line rewrite, no model rewrite, no
    // backup, no entry in `changed files`. Without this gate the
    // sync would create meaningless backups and rewrite identical
    // content for every old session whenever the user happened to
    // leave the root-level `model` line unchanged across a
    // `sync` (no switch).
    targetModel = null
  } = options;
  const summaries = [];
  const lockedPaths = [];
  const providerCounts = {
    sessions: new Map(),
    archived_sessions: new Map()
  };
  const encryptedContentCounts = emptyEncryptedContentCounts();
  const userEventThreadIds = new Set();
  const threadCwdById = new Map();

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
      if (typeof parsed.payload.id === "string"
          && parsed.payload.id
          && typeof parsed.payload.cwd === "string"
          && parsed.payload.cwd.trim()) {
        threadCwdById.set(parsed.payload.id, toDesktopWorkspacePath(parsed.payload.cwd));
      }
      try {
        if (await fileHasEncryptedContent(rolloutPath, record.firstLine, record.offset)) {
          incrementPlainCount(encryptedContentCounts, dirName, currentProvider);
        }
        if (parsed.payload.id && await fileHasUserEvent(rolloutPath, record.firstLine, record.offset)) {
          userEventThreadIds.add(parsed.payload.id);
        }
      } catch (error) {
        if (skipLockedReads && isRolloutFileBusyError(error)) {
          lockedPaths.push(rolloutPath);
          continue;
        }
        throw error;
      }

      if (targetProvider !== "__status_only__") {
        const providerNeedsUpdate = parsed.payload.model_provider !== targetProvider;
        // Peek at the first `turn_context` event to capture the
        // per-turn model that the Codex GUI bottom-right reads. We
        // keep this on the summary so the rewrite step knows what
        // value to swap out, without making collectSessionChanges
        // require a target model.
        const originalModel = await readFirstTurnContextModel(rolloutPath, {
          firstLineOffset: 0,
          firstLineLength: record.offset
        });
        // The rollout file needs editing when either the provider
        // drifted OR the per-turn model drifted. The model-only
        // case happens after a `sync` (no switch) where the user
        // has updated the root-level `model` in config.toml but
        // their old rollouts still advertise the old name on
        // every `turn_context` line.
        //
        // We compare `originalModel` against `targetModel` so
        // that a plain `sync` whose root-level model already
        // matches every rollout produces zero changes, no
        // backups, and no entries in `changed files`. Without
        // this gate, every sync would have rewritten every
        // rollout file just to write the same content back.
        const modelNeedsUpdate = typeof originalModel === "string"
          && originalModel.length > 0
          && typeof targetModel === "string"
          && targetModel.length > 0
          && originalModel !== targetModel;
        if (!providerNeedsUpdate && !modelNeedsUpdate) {
          continue;
        }
        const snapshot = await getFileSnapshot(rolloutPath);
        let updatedFirstLine = null;
        if (providerNeedsUpdate) {
          parsed.payload.model_provider = targetProvider;
          updatedFirstLine = JSON.stringify(parsed);
        }
        summaries.push({
          path: rolloutPath,
          threadId: parsed.payload.id ?? null,
          directory: dirName,
          originalFirstLine: record.firstLine,
          originalSeparator: record.separator,
          originalOffset: record.offset,
          originalSize: snapshot.size,
          originalMtimeMs: snapshot.mtimeMs,
          originalProvider: currentProvider,
          originalModel,
          providerNeedsUpdate,
          updatedFirstLine
        });
      }
    }
  }

  return { changes: summaries, lockedPaths, providerCounts, encryptedContentCounts, userEventThreadIds, threadCwdById };
}

export async function applySessionChanges(changes, options = {}) {
  const normalizedChanges = changes ?? [];
  const { targetModel = null } = options ?? {};
  const skippedPaths = [];
  const appliedPaths = [];
  // Files where the provider/first-line rewrite succeeded but the
  // follow-up turn_context `model` rewrite could not finish (typically
  // because Codex grabbed an exclusive lock on the rollout between the
  // two passes). The user will see them in the "Skipped locked rollout
  // files" message so they know to rerun sync later; the first-line
  // change is durable and will not be lost.
  const partialRewritePaths = [];
  let appliedChanges = 0;

  // Run a turn_context model-field rewrite for one change, surfacing
  // busy/locked errors as `false` so the caller can move the change
  // out of `appliedPaths` and into `partialRewritePaths` instead of
  // letting the exception bubble up and leave the count inconsistent.
  async function tryRewriteModelField(change) {
    try {
      await rewriteRolloutModelField(change, targetModel);
      return { ok: true };
    } catch (error) {
      if (isRolloutFileBusyError(error)) {
        return { ok: false, busy: true, error };
      }
      throw error;
    }
  }

  if (process.platform === "win32") {
    // Split the batch into provider-rewrite work (which the
    // PowerShell script can do efficiently, including the
    // exclusive lock dance) and model-only work (which we run
    // in pure Node because the PowerShell batch does not know how
    // to skip the first-line rewrite while still walking the
    // body for `turn_context.model` updates).
    const providerChanges = normalizedChanges.filter((change) => change.providerNeedsUpdate !== false);
    const modelOnlyChanges = normalizedChanges.filter((change) => change.providerNeedsUpdate === false);
    const results = await invokeWindowsExclusiveRewriteBatch(providerChanges, { requireOriginalMatch: true });
    for (let index = 0; index < providerChanges.length; index += 1) {
      const change = providerChanges[index];
      if (results[index] === "APPLIED") {
        const modelResult = await tryRewriteModelField(change);
        if (modelResult.ok) {
          appliedChanges += 1;
          appliedPaths.push(change.path);
          await restoreOriginalMtime(change.path, change.originalMtimeMs);
        } else {
          // First-line was already rewritten; do NOT silently put
          // the change in `skippedPaths` (that would imply "nothing
          // happened"). Track it separately so the CLI can surface
          // "Updated N rollout files, N partially rewritten (run
          // sync again later to finish the turn_context pass)".
          partialRewritePaths.push(change.path);
        }
      } else {
        skippedPaths.push(change.path);
      }
    }

    for (const change of modelOnlyChanges) {
      const modelResult = await tryRewriteModelField(change);
      if (modelResult.ok) {
        appliedChanges += 1;
        appliedPaths.push(change.path);
      } else {
        partialRewritePaths.push(change.path);
      }
    }
  } else {
    for (const change of normalizedChanges) {
      let providerRewritten = true;
      if (change.providerNeedsUpdate !== false) {
        providerRewritten = await tryRewriteCollectedFirstLine(change);
      }
      if (providerRewritten) {
        const modelResult = await tryRewriteModelField(change);
        if (modelResult.ok) {
          if (change.providerNeedsUpdate !== false) {
            await restoreOriginalMtime(change.path, change.originalMtimeMs);
          }
          appliedChanges += 1;
          appliedPaths.push(change.path);
        } else {
          partialRewritePaths.push(change.path);
        }
      } else {
        skippedPaths.push(change.path);
      }
    }
  }

  appliedPaths.sort((left, right) => left.localeCompare(right));
  skippedPaths.sort((left, right) => left.localeCompare(right));
  partialRewritePaths.sort((left, right) => left.localeCompare(right));
  return {
    appliedChanges,
    appliedPaths,
    skippedPaths,
    partialRewritePaths
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
      updatedFirstLine: entry.originalFirstLine,
      originalMtimeMs: entry.originalMtimeMs
    }));
    const results = await invokeWindowsExclusiveRewriteBatch(changes, { requireOriginalMatch: false });
    const firstFailureIndex = results.findIndex((result) => result !== "APPLIED");
    if (firstFailureIndex !== -1) {
      const filePath = changes[firstFailureIndex].path;
      throw new Error(
        `Unable to rewrite rollout file because it is currently in use. Close Codex and the Codex app, then retry. Locked file: ${filePath}`
      );
    }
    for (const change of changes) {
      await restoreOriginalMtime(change.path, change.originalMtimeMs);
    }
    return;
  }

  for (const entry of manifestEntries) {
    await rewriteFirstLine(entry.path, entry.originalFirstLine, entry.originalSeparator ?? "\n");
    await restoreOriginalMtime(entry.path, entry.originalMtimeMs);
  }
}

export function summarizeProviderCounts(providerCounts) {
  const result = {};
  for (const [scope, counts] of Object.entries(providerCounts)) {
    result[scope] = Object.fromEntries([...counts.entries()].sort(([left], [right]) => left.localeCompare(right)));
  }
  return result;
}
