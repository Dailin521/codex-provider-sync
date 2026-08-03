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
// We stream line-by-line because individual `turn_context` lines can
// easily exceed 64 KB once Codex includes the `developer_instructions`
// blob — the previous code that capped the read at 64 KB silently
// missed those, which made the rollout model rewrite a no-op for
// sessions whose first turn was a long planning step. We stop as
// soon as we find a `turn_context` line, so the scan is O(1) for the
// common case and we never load multi-MB rollouts into memory just
// to read a header.
//
// For each line we find, we do a regex on the raw text instead of
// `JSON.parse`-ing the entire payload: Codex writes opaque multi-KB
// strings (`developer_instructions`, raw tool output, …) into the
// payload, and round-tripping those through `JSON.parse` -> `JSON.stringify`
// would silently mangle embedded escape sequences. Anchoring on
// `"type":"turn_context"` and grabbing the first `"model":"<value>"`
// that follows is enough for the first `turn_context` of the file,
// because rollout lines are single JSON objects.
const ROLLOUT_TURNCONTEXT_TYPE_RE = /"type"\s*:\s*"turn_context"/;

async function readTurnContextModels(rolloutPath, { firstLineOffset, firstLineLength } = {}) {
  const headerSkip = Math.max(0, firstLineOffset ?? 0);
  const headerLength = Math.max(0, firstLineLength ?? 0);
  const models = [];

  const stream = fs.createReadStream(rolloutPath, {
    encoding: "utf8",
    start: headerSkip + headerLength,
    highWaterMark: ROLLOUT_SCAN_CHUNK_BYTES
  });
  const lines = readline.createInterface({
    input: stream,
    crlfDelay: Infinity
  });

  try {
    for await (const line of lines) {
      if (!line.includes('"turn_context"')) {
        continue;
      }
      if (!ROLLOUT_TURNCONTEXT_TYPE_RE.test(line)) {
        continue;
      }
      for (const match of line.matchAll(buildTurnContextModelFieldRegex())) {
        try {
          const value = decodeJsonStringLiteral(match[1]);
          if (typeof value === "string" && value.length > 0) {
            models.push(value);
          }
        } catch {
          // Leave malformed model literals untouched.
        }
      }
    }
    return models;
  } catch (error) {
    throw wrapRolloutFileBusyError(error, rolloutPath, "read");
  } finally {
    lines.close();
    stream.destroy();
  }
}

// Replace the per-turn `model` field in a single rollout line, on the
// assumption that the line represents a `turn_context` event. We
// intentionally do a per-line regex rewrite (rather than
// re-serializing the full JSON tree) because rollout files can be
// tens of megabytes, and Codex writes a lot of opaque payload (e.g.
// `developer_instructions`) that round-tripping through
// `JSON.parse`+`JSON.stringify` would silently mangle.
//
// Unlike the previous implementation, this version does NOT take an
// `oldModel` parameter: it captures whatever model is currently in
// the line and replaces it with `newModel`. That makes it correct
// for sessions where the user has changed models mid-conversation
// (a real Codex workflow — switching from "gpt-5" to "gpt-4o-mini"
// for one follow-up turn, for example): the per-turn model must be
// normalised to the new root-level value regardless of what the
// previous per-turn value was. The captured `originalModel` is
// returned so the caller can hand it to the backup manifest and
// later restore it on a failed rollback.
//
// A `turn_context` line can carry more than one `model` field: the
// top-level `payload.model` and a nested
// `payload.collaboration_mode.settings.model`. We rewrite every
// occurrence in the line (the regex uses the `g` flag) so both
// stay in sync. The `originalModel` we report back is the
// top-level one — it is what the restore path uses to put the
// line back to its original state on a failed rollback.
//
// `g`-flagged RegExps are stateful: `String.prototype.match` and
// `RegExp.prototype.test` both advance `lastIndex` between calls,
// which is a footgun we explicitly do not want to inherit. We
// rebuild the regex on every call site instead, which is cheap
// (V8 caches the compiled pattern) and keeps each function pure.
function buildTurnContextModelFieldRegex() {
  return /"model"\s*:\s*"((?:[^"\\]|\\.)*)"/g;
}

function decodeJsonStringLiteral(literal) {
  // Treat the captured value as the inside of a JSON string literal
  // and run it through `JSON.parse` so escape sequences such as
  // `\\`, `\"`, `\n`, `\u00e9` round-trip the same way the rest of
  // the JSON parser would. We bracket the captured value in quotes
  // and feed the result back to `JSON.parse`.
  return JSON.parse(`"${literal}"`);
}

function encodeJsonStringLiteral(value) {
  // `JSON.stringify` produces a JSON string literal — including the
  // surrounding double quotes and the right escape sequences for
  // the value. That is exactly what we need to splice back into the
  // raw line as the new value of the `model` field.
  return JSON.stringify(value);
}

function rewriteTurnContextModelInLine(line, newModel) {
  if (!line || !line.includes('"turn_context"')) {
    return { line, replaced: false, originalModel: null };
  }
  const regex = buildTurnContextModelFieldRegex();
  // `matchAll` is non-mutating and returns a fresh iterator on
  // every call, so we can safely use a stateful regex here.
  const occurrences = [...line.matchAll(regex)];
  if (occurrences.length === 0) {
    return { line, replaced: false, originalModel: null };
  }
  const originalModels = [];
  try {
    for (const occurrence of occurrences) {
      originalModels.push(decodeJsonStringLiteral(occurrence[1]));
    }
  } catch {
    // The line looks like a turn_context but its `model` value is
    // not a clean JSON string literal. Refuse to touch it rather
    // than guess — the roll-out stays byte-identical.
    return { line, replaced: false, originalModel: null };
  }
  if (originalModels.some((model) => typeof model !== "string")) {
    return { line, replaced: false, originalModel: null };
  }
  // If every `model` field in the line already equals newModel,
  // there is nothing to rewrite. The line stays byte-identical.
  let alreadyMatches = true;
  for (const current of originalModels) {
    if (current !== newModel) {
      alreadyMatches = false;
      break;
    }
  }
  if (alreadyMatches) {
    return { line, replaced: false, originalModel: originalModels[0], originalModels };
  }
  const replacementRegex = buildTurnContextModelFieldRegex();
  const newLine = line.replace(replacementRegex, `"model":${encodeJsonStringLiteral(newModel)}`);
  return {
    line: newLine,
    replaced: true,
    originalModel: originalModels[0],
    originalModels
  };
}

function isValidWindowsRewriteResult(result) {
  return result === "APPLIED"
    || result === "APPLIED_IN_PLACE"
    || result === "SKIP_BUSY"
    || result === "SKIP_CHANGED";
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

        if ($null -ne $change.inPlaceByteOffset -and -not [string]::IsNullOrEmpty([string]$change.inPlaceReplacementBase64)) {
          $originalBytes = [Convert]::FromBase64String([string]$change.inPlaceOriginalBase64)
          $replacementBytes = [Convert]::FromBase64String([string]$change.inPlaceReplacementBase64)
          try {
            $source.Seek([int64]$change.inPlaceByteOffset, [System.IO.SeekOrigin]::Begin) | Out-Null
            $source.Write($replacementBytes, 0, $replacementBytes.Length)
            $source.Flush()
            return "APPLIED_IN_PLACE"
          } catch {
            $source.Seek([int64]$change.inPlaceByteOffset, [System.IO.SeekOrigin]::Begin) | Out-Null
            $source.Write($originalBytes, 0, $originalBytes.Length)
            $source.Flush()
            # The original bytes are restored; continue into the safe
            # full-file rewrite below.
          }
        }
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
    const manifestChanges = changes.map((change) => {
      const replacement = requireOriginalMatch ? getInPlaceProviderReplacement(change) : null;
      return {
        ...change,
        requireOriginalMatch,
        inPlaceByteOffset: replacement?.byteOffset ?? null,
        inPlaceOriginalBase64: replacement?.original.toString("base64") ?? null,
        inPlaceReplacementBase64: replacement?.replacement.toString("base64") ?? null
      };
    });
    await fsp.writeFile(
      manifestPath,
      JSON.stringify(manifestChanges),
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

function getInPlaceProviderReplacement(change) {
  if (change.modelRewriteRequired
      || change.modelOnlyChange
      || typeof change.originalFirstLine !== "string"
      || typeof change.originalProvider !== "string"
      || typeof change.updatedProvider !== "string") {
    return null;
  }

  const originalProviderJson = JSON.stringify(change.originalProvider);
  const updatedProviderJson = JSON.stringify(change.updatedProvider);
  const originalProvider = Buffer.from(originalProviderJson, "utf8");
  const updatedProvider = Buffer.from(updatedProviderJson, "utf8");
  if (originalProvider.length === 0 || originalProvider.length !== updatedProvider.length) {
    return null;
  }

  const providerFieldPattern = /"model_provider"\s*:\s*/g;
  let fieldMatch;
  while ((fieldMatch = providerFieldPattern.exec(change.originalFirstLine)) !== null) {
    const valueOffset = fieldMatch.index + fieldMatch[0].length;
    if (!change.originalFirstLine.startsWith(originalProviderJson, valueOffset)) {
      continue;
    }

    return {
      byteOffset: Buffer.byteLength(change.originalFirstLine.slice(0, valueOffset), "utf8"),
      original: originalProvider,
      replacement: updatedProvider
    };
  }

  return null;
}

async function tryRewriteProviderInPlace(change, replacement) {
  let handle;
  let writeStarted = false;
  try {
    handle = await fsp.open(change.path, "r+");
    const stat = await handle.stat();
    if (!snapshotMatches(change, { size: stat.size, mtimeMs: stat.mtimeMs })) {
      return "SKIP_CHANGED";
    }

    let totalWritten = 0;
    writeStarted = true;
    while (totalWritten < replacement.replacement.length) {
      const { bytesWritten } = await handle.write(
        replacement.replacement,
        totalWritten,
        replacement.replacement.length - totalWritten,
        replacement.byteOffset + totalWritten
      );
      if (bytesWritten <= 0) {
        throw new Error(`Unable to rewrite provider bytes in rollout file: ${change.path}`);
      }
      totalWritten += bytesWritten;
    }
    await handle.sync();
    return "APPLIED_IN_PLACE";
  } catch (error) {
    if (handle && writeStarted) {
      try {
        let totalRestored = 0;
        while (totalRestored < replacement.original.length) {
          const { bytesWritten } = await handle.write(
            replacement.original,
            totalRestored,
            replacement.original.length - totalRestored,
            replacement.byteOffset + totalRestored
          );
          if (bytesWritten <= 0) {
            throw new Error(`Unable to restore provider bytes in rollout file: ${change.path}`);
          }
          totalRestored += bytesWritten;
        }
        await handle.sync();
        return "FALLBACK";
      } catch (restoreError) {
        throw new AggregateError(
          [error, restoreError],
          `Unable to restore provider bytes after an in-place rewrite failure: ${change.path}`
        );
      }
    }
    throw wrapRolloutFileBusyError(error, change.path, "rewrite");
  } finally {
    await handle?.close();
  }
}

async function tryRewriteCollectedFirstLine(change) {
  const beforeSnapshot = await getFileSnapshot(change.path);
  if (!snapshotMatches(change, beforeSnapshot)) {
    return "SKIP_CHANGED";
  }

  const current = await readFirstLineRecord(change.path);
  if (current.firstLine !== change.originalFirstLine || current.offset !== change.originalOffset) {
    return "SKIP_CHANGED";
  }

  const inPlaceReplacement = getInPlaceProviderReplacement(change);
  if (inPlaceReplacement) {
    const inPlaceResult = await tryRewriteProviderInPlace(change, inPlaceReplacement);
    if (inPlaceResult !== "FALLBACK") {
      return inPlaceResult;
    }
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
      return "SKIP_CHANGED";
    }

    await fsp.rename(tmpPath, change.path);
    return "APPLIED";
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
//
// We pre-scan the file to detect the original line separator (LF vs
// CRLF) and whether the file had a trailing newline. We then write
// the rewritten content into a tmp file using the same separator and
// re-add the trailing newline if it was present. This is the
// behaviour the owner review asked for: "重写 rollout 时需要保留末尾
// 换行、换行格式和原始 mtime".
//
// Returns `{ replacedLines, originalTurnContextModels }`. The latter
// is an array of `{ lineIndex, originalModel }` entries (one per
// rewritten turn_context line) that the backup manifest stores so
// `restoreSessionChanges` can put the per-turn `model` field back to
// its original value on a failed rollback.
async function rewriteRolloutModelField(change, targetModel) {
  if (!change || typeof change.path !== "string") {
    return { replacedLines: 0, originalTurnContextModels: [] };
  }
  if (typeof targetModel !== "string" || targetModel.length === 0) {
    return { replacedLines: 0, originalTurnContextModels: [] };
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

  const lineSeparator = change.originalSeparator === "\r\n" ? "\r\n" : "\n";

  let handle;
  try {
    handle = await fsp.open(filePath, "r+");
    const openedStat = await handle.stat();
    if (openedStat.size !== beforeSnapshot.size || openedStat.mtimeMs !== beforeSnapshot.mtimeMs) {
      return { replacedLines: 0, originalTurnContextModels: [] };
    }
    const tail = Buffer.alloc(Math.min(2, openedStat.size));
    if (tail.length > 0) {
      await handle.read(tail, 0, tail.length, openedStat.size - tail.length);
    }
    const hasTrailingNewline = tail.length > 0 && tail[tail.length - 1] === 0x0a;
    const stream = handle.createReadStream({ encoding: "utf8" });
    const reader = readline.createInterface({ input: stream, crlfDelay: Infinity });
    const tmpPath = `${filePath}.provider-sync-model.${process.pid}.${Date.now()}.tmp`;
    const writer = fs.createWriteStream(tmpPath, { encoding: "utf8" });
    let firstLine = true;
    let replacements = 0;
    let lineIndex = -1;
    const originalTurnContextModels = [];

    await new Promise((resolve, reject) => {
      reader.on("error", reject);
      writer.on("error", reject);
      reader.on("line", (line) => {
        if (firstLine) {
          // The first line is the session_meta; it has no
          // per-turn model field. Write it through verbatim.
          writer.write(line);
          firstLine = false;
          lineIndex = 0;
          return;
        }
        lineIndex += 1;
        const result = rewriteTurnContextModelInLine(line, targetModel);
        if (result.replaced) {
          replacements += 1;
          originalTurnContextModels.push({
            lineIndex,
            originalModel: result.originalModel,
            originalModels: result.originalModels
          });
        }
        writer.write(lineSeparator);
        writer.write(result.line);
      });
      reader.on("close", () => {
        writer.end();
      });
      writer.on("finish", resolve);
    });

    if (replacements === 0) {
      await fsp.rm(tmpPath, { force: true });
      return { replacedLines: 0, originalTurnContextModels: [] };
    }

    // Preserve the original trailing newline state. If the file
    // had no terminator we leave it that way; if it had one
    // (LF or CRLF) we re-add it to the tmp file.
    if (hasTrailingNewline) {
      await fsp.appendFile(tmpPath, lineSeparator, "utf8");
    }

    // Refuse to swap in the new file if Codex appended anything
    // between our snapshot and the rename — otherwise we would
    // silently drop those trailing events.
    const afterStat = await fsp.stat(filePath);
    if (afterStat.size !== beforeSnapshot.size || afterStat.mtimeMs !== beforeSnapshot.mtimeMs) {
      await fsp.rm(tmpPath, { force: true });
      return { replacedLines: 0, originalTurnContextModels: [] };
    }

    await fsp.rename(tmpPath, filePath);
    return { replacedLines: replacements, originalTurnContextModels };
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

      // Peek at the first `turn_context` event to capture the
      // per-turn model that the Codex GUI bottom-right reads. We
      // keep this on the summary so the rewrite step knows what
      // value to swap out, without making collectSessionChanges
      // require a target model.
      const currentModels = await readTurnContextModels(rolloutPath, {
        firstLineOffset: 0,
        firstLineLength: record.offset
      });
      const originalModel = currentModels[0] ?? null;

      // A file is rewritten when EITHER the provider needs to
      // change OR the per-turn model needs to change. The
      // provider-unchanged-but-model-changed case was missing
      // before the owner review: when the user edited the
      // root-level `model = "..."` in config.toml but kept the
      // same provider, the rollout's turn_context.model was not
      // updated and the GUI would still show the old model.
      const providerChanged = targetProvider !== "__status_only__" && parsed.payload.model_provider !== targetProvider;
      const modelChanged = typeof targetModel === "string"
        && targetModel.length > 0
        && currentModels.some((currentModel) => currentModel !== targetModel);

      if (providerChanged || modelChanged) {
        const snapshot = await getFileSnapshot(rolloutPath);
        if (providerChanged) {
          parsed.payload.model_provider = targetProvider;
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
          updatedProvider: targetProvider,
          originalModel,
          modelRewriteRequired: modelChanged,
          modelOnlyChange: !providerChanged && modelChanged,
          updatedFirstLine: providerChanged ? JSON.stringify(parsed) : record.firstLine
        });
      }
    }
  }

  return { changes: summaries, lockedPaths, providerCounts, encryptedContentCounts, userEventThreadIds, threadCwdById };
}

export async function applySessionChanges(changes, options = {}) {
  const normalizedChanges = changes ?? [];
  const { targetModel = null, onBeforeApply, onApplied } = options ?? {};
  const skippedPaths = [];
  const appliedPaths = [];
  let appliedChanges = 0;
  let inPlaceChanges = 0;

  // A "model-only" change carries no first-line rewrite. The
  // provider is already correct on disk, so the only thing to
  // update is the per-turn `model` field on each turn_context
  // line. We still need the manifest entry so a failed
  // rollback can put the per-turn `model` values back, so we
  // synthesise an entry that records the no-op first-line
  // rewrite explicitly.
  const modelOnlyChanges = normalizedChanges.filter((change) => change?.modelOnlyChange);
  const firstLineChanges = normalizedChanges.filter((change) => !change?.modelOnlyChange);

  if (process.platform === "win32") {
    // Process one file per helper invocation. A failed Windows batch cannot
    // report which earlier members were already replaced, which was the root
    // cause of #69. Per-target calls let the durable coordinator observe each
    // successful mutation before the next target starts.
    for (const change of firstLineChanges) {
      await onBeforeApply?.(change);
      const [result] = await invokeWindowsExclusiveRewriteBatch([change], { requireOriginalMatch: true });
      if (result === "APPLIED" || result === "APPLIED_IN_PLACE") {
        appliedChanges += 1;
        inPlaceChanges += result === "APPLIED_IN_PLACE" ? 1 : 0;
        appliedPaths.push(change.path);
        if (change.modelRewriteRequired) {
          const modelResult = await rewriteRolloutModelField(change, targetModel);
          change.originalTurnContextModels = modelResult.originalTurnContextModels;
          change.appliedTurnContextRewrites = modelResult.replacedLines;
        }
        await restoreOriginalMtime(change.path, change.originalMtimeMs);
        await onApplied?.(change);
      } else {
        skippedPaths.push(change.path);
      }
    }
  } else {
    for (const change of firstLineChanges) {
      await onBeforeApply?.(change);
      const result = await tryRewriteCollectedFirstLine(change);
      if (result === "APPLIED" || result === "APPLIED_IN_PLACE") {
        appliedChanges += 1;
        inPlaceChanges += result === "APPLIED_IN_PLACE" ? 1 : 0;
        appliedPaths.push(change.path);
        if (change.modelRewriteRequired) {
          const modelResult = await rewriteRolloutModelField(change, targetModel);
          change.originalTurnContextModels = modelResult.originalTurnContextModels;
          change.appliedTurnContextRewrites = modelResult.replacedLines;
        }
        await restoreOriginalMtime(change.path, change.originalMtimeMs);
        await onApplied?.(change);
      } else {
        skippedPaths.push(change.path);
      }
    }
  }

  // For model-only changes, skip the first-line rewrite entirely
  // and go straight to the per-turn model field pass. We only
  // count the change as "applied" when the per-turn pass actually
  // rewrote at least one line, so the manifest does not get a
  // half-applied entry. We also restore the original mtime so
  // the file's timestamp is preserved exactly the way the user
  // set it.
  for (const change of modelOnlyChanges) {
    await onBeforeApply?.(change);
    let modelResult;
    try {
      modelResult = await rewriteRolloutModelField(change, targetModel);
    } catch (error) {
      skippedPaths.push(change.path);
      throw error;
    }
    if (modelResult.replacedLines > 0) {
      await restoreOriginalMtime(change.path, change.originalMtimeMs);
      appliedChanges += 1;
      appliedPaths.push(change.path);
      change.originalTurnContextModels = modelResult.originalTurnContextModels;
      change.appliedTurnContextRewrites = modelResult.replacedLines;
      await onApplied?.(change);
    } else {
      skippedPaths.push(change.path);
    }
  }

  appliedPaths.sort((left, right) => left.localeCompare(right));
  skippedPaths.sort((left, right) => left.localeCompare(right));
  return {
    appliedChanges,
    inPlaceChanges,
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
    const firstLineEntries = manifestEntries.filter((entry) => !entry.modelOnlyChange);
    const changes = firstLineEntries.map((entry) => ({
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
    for (const entry of manifestEntries) {
      if (entry.originalTurnContextModels?.length) {
        await restoreTurnContextModelsInFile(entry.path, entry.originalTurnContextModels, entry.originalSeparator);
      }
      await restoreOriginalMtime(entry.path, entry.originalMtimeMs);
    }
    return;
  }

  for (const entry of manifestEntries) {
    if (!entry.modelOnlyChange) {
      await rewriteFirstLine(entry.path, entry.originalFirstLine, entry.originalSeparator ?? "\n");
    }
    if (entry.originalTurnContextModels?.length) {
      await restoreTurnContextModelsInFile(entry.path, entry.originalTurnContextModels, entry.originalSeparator);
    }
    await restoreOriginalMtime(entry.path, entry.originalMtimeMs);
  }
}

// Walk a rollout file and restore the per-turn `model` field for
// every line that the backup manifest recorded. The manifest stores
// `lineIndex` values that are stable relative to the session_meta
// first line: index 0 is the first non-meta line, 1 is the second,
// and so on. Codex may have appended new events after the backup;
// those events are at indices beyond the manifest's range and are
// left alone.
//
// The rewrite path is line-by-line, identical in shape to
// `rewriteRolloutModelField`, and preserves the original line
// separator + trailing-newline state of the file.
async function restoreTurnContextModelsInFile(filePath, originalTurnContextModels, originalSeparator) {
  if (!filePath || !Array.isArray(originalTurnContextModels) || originalTurnContextModels.length === 0) {
    return;
  }
  // Build a quick lookup by index.
  const byIndex = new Map();
  for (const entry of originalTurnContextModels) {
    if (entry && typeof entry.lineIndex === "number" && typeof entry.originalModel === "string") {
      byIndex.set(entry.lineIndex, entry);
    }
  }
  if (byIndex.size === 0) {
    return;
  }

  const beforeStat = await fsp.stat(filePath);
  const beforeSnapshot = { size: beforeStat.size, mtimeMs: beforeStat.mtimeMs };

  const lineSeparator = originalSeparator === "\r\n" ? "\r\n" : "\n";

  let handle;
  try {
    handle = await fsp.open(filePath, "r+");
    const openedStat = await handle.stat();
    if (openedStat.size !== beforeSnapshot.size || openedStat.mtimeMs !== beforeSnapshot.mtimeMs) {
      return;
    }
    const tail = Buffer.alloc(Math.min(2, openedStat.size));
    if (tail.length > 0) {
      await handle.read(tail, 0, tail.length, openedStat.size - tail.length);
    }
    const hasTrailingNewline = tail.length > 0 && tail[tail.length - 1] === 0x0a;
    const stream = handle.createReadStream({ encoding: "utf8" });
    const reader = readline.createInterface({ input: stream, crlfDelay: Infinity });
    const tmpPath = `${filePath}.provider-sync-restore.${process.pid}.${Date.now()}.tmp`;
    const writer = fs.createWriteStream(tmpPath, { encoding: "utf8" });

    let firstLine = true;
    let lineIndex = -1;
    let replacements = 0;

    await new Promise((resolve, reject) => {
      reader.on("error", reject);
      writer.on("error", reject);
      reader.on("line", (line) => {
        if (firstLine) {
          writer.write(line);
          firstLine = false;
          lineIndex = 0;
          return;
        }
        lineIndex += 1;
        const restoreEntry = byIndex.get(lineIndex);
        if (restoreEntry !== undefined && line.includes('"turn_context"')) {
          // The current line is a turn_context line whose
          // per-turn `model` field we need to put back. We only
          // touch it if it currently holds some other value
          // (i.e. the value is not the original — if it is
          // already correct, skip the rewrite so the file stays
          // byte-identical and we don't burn IOPS on no-op
          // edits).
          const newLine = restoreTurnContextModelInLine(line, restoreEntry);
          if (newLine !== line) {
            replacements += 1;
            writer.write(lineSeparator);
            writer.write(newLine);
            return;
          }
        }
        writer.write(lineSeparator);
        writer.write(line);
      });
      reader.on("close", () => {
        writer.end();
      });
      writer.on("finish", resolve);
    });

    if (replacements === 0) {
      await fsp.rm(tmpPath, { force: true });
      return;
    }

    if (hasTrailingNewline) {
      await fsp.appendFile(tmpPath, lineSeparator, "utf8");
    }

    const afterStat = await fsp.stat(filePath);
    if (afterStat.size !== beforeSnapshot.size || afterStat.mtimeMs !== beforeSnapshot.mtimeMs) {
      await fsp.rm(tmpPath, { force: true });
      return;
    }

    await fsp.rename(tmpPath, filePath);
  } catch (error) {
    throw wrapRolloutFileBusyError(error, filePath, "restore turn_context model");
  } finally {
    await handle?.close();
  }
}

function restoreTurnContextModelInLine(line, backup) {
  if (!line || !line.includes('"turn_context"')) {
    return line;
  }
  // `matchAll` is the only safe way to inspect a `g`-flagged
  // regex's matches without poisoning `lastIndex` for the
  // subsequent `replace` call.
  const regex = buildTurnContextModelFieldRegex();
  const occurrences = [...line.matchAll(regex)];
  if (occurrences.length === 0) {
    return line;
  }
  const originalModels = Array.isArray(backup.originalModels)
    && backup.originalModels.length === occurrences.length
    ? backup.originalModels
    : Array.from({ length: occurrences.length }, () => backup.originalModel);
  const currentModels = [];
  try {
    for (const occurrence of occurrences) {
      currentModels.push(decodeJsonStringLiteral(occurrence[1]));
    }
  } catch {
    return line;
  }
  if (currentModels.every((model, index) => model === originalModels[index])) {
    return line;
  }
  const replacementRegex = buildTurnContextModelFieldRegex();
  let index = 0;
  return line.replace(
    replacementRegex,
    () => `"model":${encodeJsonStringLiteral(originalModels[index++])}`
  );
}

export function summarizeProviderCounts(providerCounts) {
  const result = {};
  for (const [scope, counts] of Object.entries(providerCounts)) {
    result[scope] = Object.fromEntries([...counts.entries()].sort(([left], [right]) => left.localeCompare(right)));
  }
  return result;
}
