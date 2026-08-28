import { execFile, spawn } from "node:child_process";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import readline from "node:readline";
import { promisify } from "node:util";
import { isDeepStrictEqual } from "node:util";

import { SESSION_DIRS } from "./constants.js";
import { syncDirectory } from "./atomic-file.js";

const execFileAsync = promisify(execFile);
const ROLLOUT_SCAN_CHUNK_BYTES = 1024 * 1024;

async function syncStagedFile(filePath) {
  const handle = await fsp.open(filePath, "r+");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

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
  const stat = await fsp.stat(filePath, { bigint: true });
  return {
    size: Number(stat.size),
    mtimeMs: Number(stat.mtimeNs) / 1e6,
    mode: Number(stat.mode),
    nlink: Number(stat.nlink),
    dev: String(stat.dev),
    ino: String(stat.ino)
  };
}

function snapshotMatches(change, snapshot) {
  if (change.originalSize !== snapshot.size
      || change.originalMtimeMs !== snapshot.mtimeMs) {
    return false;
  }
  if (change.originalDev !== undefined && String(change.originalDev) !== String(snapshot.dev)) {
    return false;
  }
  if (change.originalIno !== undefined && String(change.originalIno) !== String(snapshot.ino)) {
    return false;
  }
  return true;
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

async function listJsonlFiles(rootDir) {
  const entries = await fsp.readdir(rootDir, { withFileTypes: true });
  entries.sort((left, right) => left.name < right.name ? -1 : left.name > right.name ? 1 : 0);
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

async function readFirstLineRecordFromHandle(handle, maxBytes = Infinity) {
  let position = 0;
  let collected = Buffer.alloc(0);
  while (true) {
    if (position >= maxBytes) {
      const error = new Error("Fast mode requires a session metadata header smaller than 1 MiB; use full sync.");
      error.code = "FAST_MODE_UNSUPPORTED";
      throw error;
    }
    const chunk = Buffer.alloc(Math.min(64 * 1024, maxBytes - position));
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
}

async function readFirstLineRecord(filePath, maxBytes) {
  let handle;
  try {
    handle = await fsp.open(filePath, "r");
    return await readFirstLineRecordFromHandle(handle, maxBytes);
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

// One streaming pass supplies all body-dependent diagnostics and model undo
// evidence. Keep the existing detection rules; never reserialize message data.
const ROLLOUT_TURNCONTEXT_TYPE_RE = /"type"\s*:\s*"turn_context"/;

async function scanRolloutBody(
  rolloutPath,
  { firstLine, firstLineLength, targetModel = null } = {}
) {
  const headerLength = Math.max(0, firstLineLength ?? 0);
  const models = [];
  const originalTurnContextModels = [];
  let hasEncryptedContent = firstLine.includes("encrypted_content");
  let hasUserEvent = recordHasUserEvent(JSON.parse(firstLine));
  let lineIndex = 0;

  const stream = fs.createReadStream(rolloutPath, {
    encoding: "utf8",
    start: headerLength,
    highWaterMark: ROLLOUT_SCAN_CHUNK_BYTES
  });
  const lines = readline.createInterface({
    input: stream,
    crlfDelay: Infinity
  });

  try {
    for await (const line of lines) {
      lineIndex += 1;
      hasEncryptedContent ||= line.includes("encrypted_content");
      if (!hasUserEvent) {
        try { hasUserEvent = recordHasUserEvent(JSON.parse(line)); }
        catch { /* Malformed body lines provide no positive user-event evidence. */ }
      }
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
      if (typeof targetModel === "string" && targetModel.length > 0) {
        const rewrite = rewriteTurnContextModelInLine(line, targetModel);
        if (rewrite.replaced) {
          originalTurnContextModels.push({
            lineIndex,
            originalModel: rewrite.originalModel,
            originalModels: rewrite.originalModels
          });
        }
      }
    }
    return { models, originalTurnContextModels, hasEncryptedContent, hasUserEvent };
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

const SAFE_IN_PLACE_PROVIDER_ID_RE = /^[A-Za-z0-9._-]+$/;
const PROVIDER_MUTATION_STRATEGY = "provider_bytes_in_place";

function getInPlaceProviderMutation(change) {
  if (!change
      || (change.originalNlink !== undefined && change.originalNlink !== 1)
      || change.modelRewriteRequired
      || change.modelOnlyChange
      || typeof change.originalFirstLine !== "string"
      || typeof change.originalProvider !== "string"
      || typeof change.updatedProvider !== "string"
      || change.originalProvider === change.updatedProvider
      || !SAFE_IN_PLACE_PROVIDER_ID_RE.test(change.originalProvider)
      || !SAFE_IN_PLACE_PROVIDER_ID_RE.test(change.updatedProvider)) {
    return null;
  }

  const originalLiteral = JSON.stringify(change.originalProvider);
  const replacementLiteral = JSON.stringify(change.updatedProvider);
  const originalBytes = Buffer.from(originalLiteral, "utf8");
  const replacementBytes = Buffer.from(replacementLiteral, "utf8");
  if (originalBytes.length === 0 || originalBytes.length !== replacementBytes.length) {
    return null;
  }

  // Tokenize strings first: a regex on raw field text can match inside a JSON
  // string or miss an escaped duplicate key. Only one literal provider key and
  // one payload key anywhere in the header are eligible.
  const keys = [...change.originalFirstLine.matchAll(/"(?:[^"\\]|\\.)*"/g)]
    .filter((token) => /^\s*:/.test(change.originalFirstLine.slice(token.index + token[0].length)));
  const named = (name) => keys.filter((key) => JSON.parse(key[0]) === name);
  const fields = named("model_provider");
  if (fields.length !== 1 || named("payload").length !== 1
      || !fields[0][0].startsWith('"model_provider"')) {
    return null;
  }
  const field = fields[0];
  const valueOffset = field.index + field[0].length
    + change.originalFirstLine.slice(field.index + field[0].length).match(/^\s*:\s*/)[0].length;
  if (!change.originalFirstLine.startsWith(originalLiteral, valueOffset)) {
    return null;
  }
  const nextCharacter = change.originalFirstLine[valueOffset + originalLiteral.length];
  if (nextCharacter !== undefined && !/[\s,}]/.test(nextCharacter)) {
    return null;
  }
  const original = parseSessionMetaRecord(change.originalFirstLine);
  const replaced = change.originalFirstLine.slice(0, valueOffset) + replacementLiteral
    + change.originalFirstLine.slice(valueOffset + originalLiteral.length);
  if (original?.payload.model_provider !== change.originalProvider
      || !isDeepStrictEqual(JSON.parse(replaced), JSON.parse(change.updatedFirstLine))) {
    return null;
  }

  return {
    strategy: PROVIDER_MUTATION_STRATEGY,
    byteOffset: Buffer.byteLength(change.originalFirstLine.slice(0, valueOffset), "utf8"),
    originalBase64: originalBytes.toString("base64"),
    replacementBase64: replacementBytes.toString("base64"),
    originalSize: change.originalSize,
    originalMtimeMs: change.originalMtimeMs,
    originalDev: change.originalDev,
    originalIno: change.originalIno
  };
}

function decodeCanonicalBase64(value) {
  if (typeof value !== "string" || value.length === 0 || value.length % 4 !== 0
      || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) {
    return null;
  }
  const decoded = Buffer.from(value, "base64");
  return decoded.toString("base64") === value ? decoded : null;
}

export function validateProviderMutationDescriptor(mutation, targetPath, firstLine, separator = "") {
  if (!mutation || mutation.strategy !== PROVIDER_MUTATION_STRATEGY
      || !Number.isSafeInteger(mutation.byteOffset) || mutation.byteOffset < 0
      || !Number.isSafeInteger(mutation.originalSize) || mutation.originalSize < 0
      || !Number.isFinite(mutation.originalMtimeMs)) {
    throw new Error(`Invalid provider in-place mutation descriptor for ${targetPath}.`);
  }
  const originalBytes = decodeCanonicalBase64(mutation.originalBase64);
  const replacementBytes = decodeCanonicalBase64(mutation.replacementBase64);
  if (!originalBytes || !replacementBytes || originalBytes.length === 0
      || originalBytes.length !== replacementBytes.length
      || mutation.byteOffset + originalBytes.length > mutation.originalSize
      || !/^"[A-Za-z0-9._-]+"$/.test(originalBytes.toString("utf8"))
      || !/^"[A-Za-z0-9._-]+"$/.test(replacementBytes.toString("utf8"))) {
    throw new Error(`Invalid provider in-place mutation bytes for ${targetPath}.`);
  }
  if (typeof firstLine !== "string" || !["", "\n", "\r\n"].includes(separator)
      || typeof mutation.originalDev !== "string" || !/^\d+$/.test(mutation.originalDev)
      || typeof mutation.originalIno !== "string" || !/^\d+$/.test(mutation.originalIno)) {
    throw new Error(`Incomplete provider in-place recovery evidence for ${targetPath}.`);
  }
  const header = Buffer.from(firstLine, "utf8");
  const end = mutation.byteOffset + originalBytes.length;
  if (!header.subarray(mutation.byteOffset, end).equals(originalBytes)
      || header.length + Buffer.byteLength(separator) > mutation.originalSize) {
    throw new Error(`Provider mutation does not match the original header: ${targetPath}`);
  }
  const replaced = Buffer.concat([header.subarray(0, mutation.byteOffset), replacementBytes, header.subarray(end)]).toString("utf8");
  const expected = getInPlaceProviderMutation({
    originalFirstLine: firstLine,
    originalProvider: JSON.parse(originalBytes.toString()),
    updatedProvider: JSON.parse(replacementBytes.toString()),
    updatedFirstLine: replaced
  });
  if (!expected || expected.byteOffset !== mutation.byteOffset) {
    throw new Error(`Provider mutation targets an ambiguous JSON field: ${targetPath}`);
  }
  return { originalBytes, replacementBytes };
}

async function readBytesFully(handle, length, position) {
  const buffer = Buffer.alloc(length);
  let offset = 0;
  while (offset < buffer.length) {
    const { bytesRead } = await handle.read(
      buffer,
      offset,
      buffer.length - offset,
      position + offset
    );
    if (bytesRead <= 0) {
      return null;
    }
    offset += bytesRead;
  }
  return buffer;
}

async function defaultInPlaceWrite(handle, buffer, offset, length, position) {
  return handle.write(buffer, offset, length, position);
}

async function writeBytesFully(handle, bytes, position, writeImpl) {
  let offset = 0;
  while (offset < bytes.length) {
    const result = await writeImpl(
      handle,
      bytes,
      offset,
      bytes.length - offset,
      position + offset
    );
    const bytesWritten = typeof result === "number" ? result : result?.bytesWritten;
    if (!Number.isInteger(bytesWritten) || bytesWritten <= 0
        || bytesWritten > bytes.length - offset) {
      throw new Error("Provider in-place write made no valid forward progress.");
    }
    offset += bytesWritten;
  }
}

async function finishInPlaceWrite(handle, entry, expectedBytes, options = {}) {
  const mutation = entry.mutation ?? entry.inPlaceMutation;
  await (options.inPlaceSync ?? ((h) => h.sync()))(handle);
  const expected = Buffer.from(entry.originalFirstLine + entry.originalSeparator, "utf8");
  expectedBytes.copy(expected, mutation.byteOffset);
  const actual = await readBytesFully(handle, expected.length, 0);
  if (!actual?.equals(expected) || (await handle.stat()).size < mutation.originalSize) {
    throw new Error(`Provider in-place write verification failed: ${entry.path}`);
  }
  await assertInPlaceIdentity(handle, entry.path, mutation);
  // POSIX has no exclusive handle here. Even stat followed by utimes races an
  // append, so retain the actual write time instead of overwriting newer mtime.
}

async function assertInPlaceIdentity(handle, filePath, mutation) {
  const [opened, current] = await Promise.all([handle.stat({ bigint: true }), fsp.lstat(filePath, { bigint: true })]);
  if (!current.isFile() || current.isSymbolicLink() || opened.nlink !== 1n
      || String(opened.dev) !== mutation.originalDev || String(opened.ino) !== mutation.originalIno
      || opened.dev !== current.dev || opened.ino !== current.ino) {
    throw new Error(`Rollout identity changed before provider byte access: ${filePath}`);
  }
}

function isRecoverableProviderBytes(current, original, replacement) {
  // Forward short writes and interrupted rollback produce old* new* old* at
  // the differing positions. This excludes arbitrary edits and disjoint tears.
  let phase = 0;
  for (let i = 0; i < current.length; i += 1) {
    if (original[i] === replacement[i]) {
      if (current[i] !== original[i]) return false;
    } else if (current[i] === replacement[i]) {
      if (phase === 2) return false;
      phase = 1;
    } else if (current[i] === original[i]) {
      if (phase === 1) phase = 2;
    } else return false;
  }
  return true;
}

async function inspectProviderRecovery(handle, entry) {
  const mutation = entry.mutation ?? entry.inPlaceMutation;
  const { originalBytes, replacementBytes } = validateProviderMutationDescriptor(
    mutation, entry.path, entry.originalFirstLine, entry.originalSeparator);
  await assertInPlaceIdentity(handle, entry.path, mutation);
  const stat = await handle.stat();
  const expected = Buffer.from(entry.originalFirstLine + entry.originalSeparator, "utf8");
  const header = await readBytesFully(handle, expected.length, 0);
  if (stat.size < mutation.originalSize || !header) {
    throw new Error(`Rollout truncated before provider recovery: ${entry.path}`);
  }
  const end = mutation.byteOffset + originalBytes.length;
  const current = header.subarray(mutation.byteOffset, end);
  if (!header.subarray(0, mutation.byteOffset).equals(expected.subarray(0, mutation.byteOffset))
      || !header.subarray(end).equals(expected.subarray(end))
      || !isRecoverableProviderBytes(current, originalBytes, replacementBytes)) {
    throw new Error(`Unknown rollout bytes during provider recovery: ${entry.path}`);
  }
  return { current, originalBytes };
}

export async function validateProviderByteRestore(entry) {
  const handle = await fsp.open(entry.path, "r");
  try { await inspectProviderRecovery(handle, entry); }
  finally { await handle.close(); }
}

async function restoreProviderOnHandle(handle, entry, options = {}) {
  const mutation = entry.mutation ?? entry.inPlaceMutation;
  const { current, originalBytes } = await inspectProviderRecovery(handle, entry);
  if (!current.equals(originalBytes)) {
    await writeBytesFully(handle, originalBytes, mutation.byteOffset, options.inPlaceRestoreWrite ?? defaultInPlaceWrite);
  }
  await finishInPlaceWrite(handle, entry, originalBytes);
}

async function tryRewriteProviderInPlace(change, options = {}) {
  const mutation = change.inPlaceMutation;
  const { originalBytes, replacementBytes } = validateProviderMutationDescriptor(
    mutation, change.path, change.originalFirstLine, change.originalSeparator);
  const writeImpl = options.inPlaceWrite ?? defaultInPlaceWrite;
  let handle;
  let writeAttempted = false;
  try {
    const pathStat = await fsp.lstat(change.path);
    if (pathStat.isSymbolicLink() || !pathStat.isFile()) {
      return "SKIP_CHANGED";
    }
    handle = await fsp.open(change.path, "r+");
    const identity = await handle.stat({ bigint: true });
    const snapshot = {
      size: Number(identity.size),
      mtimeMs: Number(identity.mtimeNs) / 1e6,
      dev: String(identity.dev),
      ino: String(identity.ino)
    };
    if (!snapshotMatches(change, snapshot)
        || mutation.originalSize !== change.originalSize
        || mutation.originalMtimeMs !== change.originalMtimeMs) {
      return "SKIP_CHANGED";
    }
    const current = await readFirstLineRecordFromHandle(handle);
    if (current.firstLine !== change.originalFirstLine || current.offset !== change.originalOffset) {
      return "SKIP_CHANGED";
    }
    const currentBytes = await readBytesFully(handle, originalBytes.length, mutation.byteOffset);
    if (!currentBytes?.equals(originalBytes)) {
      return "SKIP_CHANGED";
    }
    try {
      await assertInPlaceIdentity(handle, change.path, mutation);
      if (!snapshotMatches(change, await getFileSnapshot(change.path))) return "SKIP_CHANGED";
    } catch {
      return "SKIP_CHANGED";
    }

    try {
      writeAttempted = true;
      await writeBytesFully(handle, replacementBytes, mutation.byteOffset, writeImpl);
      await finishInPlaceWrite(handle, change, replacementBytes, options);
    } catch (error) {
      if (writeAttempted) {
        try {
          await restoreProviderOnHandle(handle, change, options);
        } catch (restoreError) {
          const failure = new AggregateError(
            [error, restoreError],
            `Provider in-place write and immediate byte restoration both failed for ${change.path}.`
          );
          failure.code = "IN_PLACE_RESTORE_FAILED";
          throw failure;
        }
      }
      throw error;
    }
    return "APPLIED_IN_PLACE";
  } catch (error) {
    throw wrapRolloutFileBusyError(error, change.path, "rewrite provider bytes in place");
  } finally {
    await handle?.close();
  }
}

async function restoreProviderBytesInPlace(entry, options = {}) {
  let handle;
  try {
    const pathStat = await fsp.lstat(entry.path);
    if (pathStat.isSymbolicLink() || !pathStat.isFile()) {
      throw new Error(`Rollout path changed before in-place recovery: ${entry.path}`);
    }
    handle = await fsp.open(entry.path, "r+");
    await restoreProviderOnHandle(handle, entry, options);
    return "RESTORED_IN_PLACE";
  } finally {
    await handle?.close();
  }
}

const WINDOWS_REWRITE_PROTOCOL_VERSION = 1;
const WINDOWS_REWRITE_READY_TIMEOUT_MS = 15_000;

const WINDOWS_EXCLUSIVE_REWRITE_WORKER_SCRIPT = `
& {
  $ErrorActionPreference = "Stop"
  $ProgressPreference = "SilentlyContinue"
  $utf8 = [System.Text.UTF8Encoding]::new($false)
  [Console]::InputEncoding = $utf8
  [Console]::OutputEncoding = $utf8

  Add-Type -TypeDefinition @'
${fs.readFileSync(new URL("./windows-provider-bytes.cs", import.meta.url), "utf8")}
'@

  function Write-ProtocolMessage($value) {
    $json = $value | ConvertTo-Json -Compress -Depth 8
    [Console]::Out.WriteLine($json)
    [Console]::Out.Flush()
  }

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
    $replaceBackupPath = "$path.provider-sync.$PID.$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()).replace-backup"
    $encoding = [System.Text.UTF8Encoding]::new($false)
    $source = $null
    $writer = $null

    try {
      try {
        $source = [System.IO.File]::Open($path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
      } catch {
        if (Test-Path -LiteralPath $path) {
          return "SKIP_BUSY"
        }
        return "SKIP_CHANGED"
      }

      if ($null -ne $change.inPlaceMutation) {
        $m = $change.inPlaceMutation
        $header = $encoding.GetBytes([string]$change.originalFirstLine + [string]$change.originalSeparator)
        return [ProviderByteFile]::Apply($source, $header,
          [Convert]::FromBase64String([string]$m.originalBase64),
          [Convert]::FromBase64String([string]$m.replacementBase64),
          [int]$m.byteOffset, [long]$m.originalSize, [double]$m.originalMtimeMs,
          [string]$m.originalDev, [string]$m.originalIno, [bool]$change.restoreProviderBytes)
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

      $writer.Flush($true)
      $writer.Dispose()
      $writer = $null

      $source.Dispose()
      $source = $null
      try {
        [System.IO.File]::Replace($tmpPath, $path, $replaceBackupPath, $true)
      } catch {
        if (Test-Path -LiteralPath $path) {
          return "SKIP_BUSY"
        }
        return "SKIP_CHANGED"
      }

      return "APPLIED"
    } finally {
      if ($writer) {
        $writer.Dispose()
      }
      if ($source) {
        $source.Dispose()
      }
      Remove-Item -LiteralPath $tmpPath -Force -ErrorAction SilentlyContinue
      Remove-Item -LiteralPath $replaceBackupPath -Force -ErrorAction SilentlyContinue
    }
  }

  Write-ProtocolMessage ([ordered]@{
    protocolVersion = 1
    type = "ready"
  })

  while ($null -ne ($line = [Console]::In.ReadLine())) {
    if ([string]::IsNullOrWhiteSpace($line)) {
      continue
    }

    $request = $null
    try {
      $request = $line | ConvertFrom-Json
      $requestPath = [string]$request.path
      if (([int]$request.protocolVersion -ne 1) -or
          ([string]$request.type -ne "rewrite") -or
          ($null -eq $request.id) -or
          [string]::IsNullOrWhiteSpace($requestPath) -or
          (-not [System.IO.Path]::IsPathRooted($requestPath))) {
        throw [System.InvalidOperationException]::new("Invalid Windows rewrite worker request.")
      }

      $result = Invoke-RewriteChange $request
      Write-ProtocolMessage ([ordered]@{
        protocolVersion = 1
        type = "result"
        id = $request.id
        path = $requestPath
        result = $result
      })
    } catch {
      [Console]::Error.WriteLine($_.Exception.ToString())
      [Console]::Error.Flush()
      $errorId = $null
      $errorPath = $null
      if ($null -ne $request) {
        $errorId = $request.id
        $errorPath = [string]$request.path
      }
      Write-ProtocolMessage ([ordered]@{
        protocolVersion = 1
        type = "error"
        id = $errorId
        path = $errorPath
        message = $_.Exception.Message
      })
      exit 1
    }
  }
}
`.trim();

function formatWindowsRewriteWorkerError(message, stderr) {
  const detail = stderr.trim();
  return detail ? `${message} PowerShell diagnostics: ${detail}` : message;
}

function writeWorkerRequest(stream, request) {
  return new Promise((resolve, reject) => {
    stream.write(`${JSON.stringify(request)}\n`, "utf8", (error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

export async function createWindowsExclusiveRewriteWorker(options = {}) {
  const {
    spawnImpl = spawn,
    readyTimeoutMs = WINDOWS_REWRITE_READY_TIMEOUT_MS
  } = options;
  const child = spawnImpl("powershell.exe", [
      "-NoLogo",
      "-NoProfile",
      "-NonInteractive",
      "-InputFormat",
      "Text",
      "-OutputFormat",
      "Text",
      "-ExecutionPolicy",
      "Bypass",
      "-Command",
      WINDOWS_EXCLUSIVE_REWRITE_WORKER_SCRIPT
    ], {
      stdio: ["pipe", "pipe", "pipe"],
      windowsHide: true
    });

  let stderr = "";
  let spawnError = null;
  let exitInfo = null;
  let closed = false;
  let inFlight = false;
  let nextRequestId = 1;
  let stdinError = null;
  const stdoutLines = readline.createInterface({
    input: child.stdout,
    crlfDelay: Infinity
  });
  const stdoutIterator = stdoutLines[Symbol.asyncIterator]();
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    stderr = `${stderr}${chunk}`.slice(-64 * 1024);
  });
  child.stdin.on("error", (error) => {
    stdinError = error;
  });

  const completion = new Promise((resolve) => {
    child.once("error", (error) => {
      spawnError = error;
      resolve({ error, code: null, signal: null });
    });
    child.once("exit", (code, signal) => {
      exitInfo = { error: null, code, signal };
      resolve(exitInfo);
    });
  });

  async function readProtocolMessage(timeoutMs = null) {
    let timeoutId = null;
    const timeoutPromise = timeoutMs === null
      ? null
      : new Promise((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error(`Windows rewrite worker did not become ready within ${timeoutMs} ms.`));
        }, timeoutMs);
      });
    try {
      const nextLine = timeoutPromise
        ? await Promise.race([stdoutIterator.next(), timeoutPromise])
        : await stdoutIterator.next();
      if (nextLine.done) {
        const message = spawnError
          ? `Unable to start Windows rewrite worker: ${spawnError.message}`
          : `Windows rewrite worker closed stdout unexpectedly${exitInfo ? ` (exit ${exitInfo.code ?? "null"}, signal ${exitInfo.signal ?? "null"})` : ""}.`;
        throw new Error(formatWindowsRewriteWorkerError(message, stderr));
      }
      try {
        return JSON.parse(nextLine.value);
      } catch (error) {
        throw new Error(
          formatWindowsRewriteWorkerError(
            `Windows rewrite worker returned malformed JSON: ${error.message}`,
            stderr
          )
        );
      }
    } finally {
      if (timeoutId !== null) {
        clearTimeout(timeoutId);
      }
    }
  }

  try {
    const ready = await readProtocolMessage(readyTimeoutMs);
    if (ready?.protocolVersion !== WINDOWS_REWRITE_PROTOCOL_VERSION || ready?.type !== "ready") {
      throw new Error(`Unexpected Windows rewrite worker ready message: ${JSON.stringify(ready)}`);
    }
  } catch (error) {
    child.stdin.destroy();
    child.kill();
    stdoutLines.close();
    throw error;
  }

  return {
    pid: child.pid,
    async rewrite(change, { requireOriginalMatch }) {
      if (closed) {
        throw new Error("Windows rewrite worker is already closed.");
      }
      if (inFlight) {
        throw new Error("Windows rewrite worker already has an in-flight request.");
      }
      if (!change || typeof change.path !== "string" || !path.isAbsolute(change.path)) {
        throw new Error(`Windows rewrite worker requires an absolute rollout path: ${change?.path ?? "(missing)"}`);
      }
      if (change.inPlaceMutation) {
        validateProviderMutationDescriptor(change.inPlaceMutation, change.path,
          change.originalFirstLine, change.originalSeparator);
      }

      const id = nextRequestId;
      nextRequestId += 1;
      inFlight = true;
      try {
        await writeWorkerRequest(child.stdin, {
          ...change,
          protocolVersion: WINDOWS_REWRITE_PROTOCOL_VERSION,
          type: "rewrite",
          id,
          requireOriginalMatch: Boolean(requireOriginalMatch)
        });
        const response = await readProtocolMessage();
        if (response?.protocolVersion !== WINDOWS_REWRITE_PROTOCOL_VERSION
            || response?.type !== "result"
            || response?.id !== id
            || response?.path !== change.path
            || !isValidWindowsRewriteResult(response?.result)
            || (change.inPlaceMutation && response.result === "APPLIED")) {
          throw new Error(`Unexpected Windows rewrite worker response for ${change.path}: ${JSON.stringify(response)}`);
        }
        return response.result;
      } catch (error) {
        child.stdin.destroy();
        child.kill();
        throw wrapRolloutFileBusyError(
          new Error(
            formatWindowsRewriteWorkerError(
              `Windows rewrite worker failed for ${change.path}: ${stdinError?.message ?? error.message}`,
              stderr
            ),
            { cause: error }
          ),
          change.path,
          "rewrite"
        );
      } finally {
        inFlight = false;
      }
    },
    async close() {
      if (closed) {
        return;
      }
      closed = true;
      if (!child.stdin.destroyed) {
        child.stdin.end();
      }
      const completed = await completion;
      stdoutLines.close();
      if (completed.error) {
        throw new Error(formatWindowsRewriteWorkerError(
          `Windows rewrite worker failed to start: ${completed.error.message}`,
          stderr
        ));
      }
      if (completed.code !== 0) {
        throw new Error(formatWindowsRewriteWorkerError(
          `Windows rewrite worker exited with code ${completed.code ?? "null"} and signal ${completed.signal ?? "null"}.`,
          stderr
        ));
      }
    }
  };
}

async function invokeWindowsExclusiveRewriteBatch(changes, { requireOriginalMatch }) {
  if (!changes.length) {
    return [];
  }

  let worker = null;
  let primaryError = null;
  try {
    worker = await createWindowsExclusiveRewriteWorker();
    const results = [];
    for (const change of changes) {
      results.push(await worker.rewrite(change, { requireOriginalMatch }));
    }
    return results;
  } catch (error) {
    primaryError = error;
    throw error;
  } finally {
    if (worker) {
      try {
        await worker.close();
      } catch (closeError) {
        if (!primaryError) {
          throw closeError;
        }
      }
    }
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
  const sourceStat = await fsp.stat(filePath);
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

    await fsp.chmod(tmpPath, sourceStat.mode);
    await syncStagedFile(tmpPath);
    await fsp.rename(tmpPath, filePath);
    await syncDirectory(path.dirname(filePath));
  } catch (error) {
    await fsp.rm(tmpPath, { force: true });
    throw wrapRolloutFileBusyError(error, filePath, "rewrite");
  }
}

async function tryRewriteCollectedFirstLine(change, options = {}) {
  if (change.inPlaceMutation?.strategy === PROVIDER_MUTATION_STRATEGY) {
    return tryRewriteProviderInPlace(change, options);
  }

  const beforeSnapshot = await getFileSnapshot(change.path);
  if (!snapshotMatches(change, beforeSnapshot)) {
    return "SKIP_CHANGED";
  }

  const current = await readFirstLineRecord(change.path);
  if (current.firstLine !== change.originalFirstLine || current.offset !== change.originalOffset) {
    return "SKIP_CHANGED";
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

    await fsp.chmod(tmpPath, beforeSnapshot.mode);
    await syncStagedFile(tmpPath);
    await fsp.rename(tmpPath, change.path);
    await syncDirectory(path.dirname(change.path));
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

    // Validate the immutable scan-time rollback snapshot before replacing the
    // file. A turn_context appended after the first-line mutation must not be
    // rewritten and then discovered only after the destructive rename: that
    // new line has no original value in the backup manifest. Throwing here
    // leaves the appended line untouched and lets the transaction restore the
    // already-mutated first line.
    if (!modelSnapshotsEqual(change.originalTurnContextModels, originalTurnContextModels)) {
      await fsp.rm(tmpPath, { force: true });
      throw new Error(`Rollout turn_context model snapshot changed before rewrite: ${change.path}`);
    }

    await fsp.chmod(tmpPath, beforeStat.mode);
    await syncStagedFile(tmpPath);
    await fsp.rename(tmpPath, filePath);
    await syncDirectory(path.dirname(filePath));
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
    targetModel = null,
    fast = false
  } = options;
  if (typeof fast !== "boolean" || (fast && targetModel !== null)) {
    throw new Error("Fast mode requires a boolean fast option and no historical model rewrite.");
  }
  const summaries = [];
  const lockedPaths = [];
  const providerCounts = {
    sessions: new Map(),
    archived_sessions: new Map()
  };
  const encryptedContentCounts = fast ? null : emptyEncryptedContentCounts();
  const userEventThreadIds = fast ? null : new Set();
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
      let scanStart;
      try {
        scanStart = await getFileSnapshot(rolloutPath);
        record = await readFirstLineRecord(rolloutPath, fast ? 1024 * 1024 : undefined);
      } catch (error) {
        if (skipLockedReads && isRolloutFileBusyError(error)) {
          lockedPaths.push(rolloutPath);
          continue;
        }
        throw error;
      }
      const parsed = parseSessionMetaRecord(record.firstLine);
      if (!parsed) {
        if (fast) {
          const error = new Error(`Fast mode cannot validate session metadata: ${rolloutPath}`);
          error.code = "FAST_MODE_UNSUPPORTED";
          throw error;
        }
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
      let modelSnapshot = { models: [], originalTurnContextModels: [] };
      try {
        if (!fast) modelSnapshot = await scanRolloutBody(rolloutPath, {
          firstLine: record.firstLine, firstLineLength: record.offset, targetModel
        });
        if (modelSnapshot.hasEncryptedContent) {
          incrementPlainCount(encryptedContentCounts, dirName, currentProvider);
        }
        if (parsed.payload.id && modelSnapshot.hasUserEvent) {
          userEventThreadIds.add(parsed.payload.id);
        }
      } catch (error) {
        if (skipLockedReads && isRolloutFileBusyError(error)) {
          lockedPaths.push(rolloutPath);
          continue;
        }
        throw error;
      }

      const currentModels = modelSnapshot.models;
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
        if (snapshot.size !== scanStart.size || snapshot.mtimeMs !== scanStart.mtimeMs
            || snapshot.dev !== scanStart.dev || snapshot.ino !== scanStart.ino) {
          lockedPaths.push(rolloutPath);
          continue;
        }
        if (providerChanged) {
          parsed.payload.model_provider = targetProvider;
        }
        const change = {
          path: rolloutPath,
          threadId: parsed.payload.id ?? null,
          directory: dirName,
          originalFirstLine: record.firstLine,
          originalSeparator: record.separator,
          originalOffset: record.offset,
          originalSize: snapshot.size,
          originalMtimeMs: snapshot.mtimeMs,
          originalDev: snapshot.dev,
          originalIno: snapshot.ino,
          originalNlink: snapshot.nlink,
          originalProvider: currentProvider,
          updatedProvider: targetProvider,
          originalModel,
          originalTurnContextModels: modelSnapshot.originalTurnContextModels,
          modelRewriteRequired: modelChanged,
          modelOnlyChange: !providerChanged && modelChanged,
          updatedFirstLine: providerChanged ? JSON.stringify(parsed) : record.firstLine
        };
        change.inPlaceMutation = getInPlaceProviderMutation(change);
        if (fast && !change.inPlaceMutation) {
          const error = new Error(`Fast mode requires an unambiguous equal-length provider byte replacement: ${rolloutPath}. Run full sync explicitly for this file.`);
          error.code = "FAST_MODE_UNSUPPORTED";
          throw error;
        }
        summaries.push(change);
      }
    }
  }

  return { changes: summaries, lockedPaths, providerCounts, encryptedContentCounts, userEventThreadIds, threadCwdById };
}

export async function applySessionChanges(changes, options = {}) {
  const normalizedChanges = changes ?? [];
  const {
    targetModel = null,
    onBeforeApply,
    onMutation,
    onApplied,
    onSkipped,
    windowsRewriteWorkerFactory = createWindowsExclusiveRewriteWorker,
    inPlaceWrite,
    inPlaceRestoreWrite,
    inPlaceSync
  } = options ?? {};
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
    // Keep one PowerShell process alive, but send exactly one target at a time.
    // The coordinator persists applying/applied around each awaited request, so
    // an abrupt exit can never mutate a later rollout that has no journal entry.
    let worker = null;
    let primaryError = null;
    try {
      if (firstLineChanges.length > 0) {
        worker = await windowsRewriteWorkerFactory();
      }
      for (const change of firstLineChanges) {
        await onBeforeApply?.(change);
        const result = await worker.rewrite(change, { requireOriginalMatch: true });
        if (result === "APPLIED" || result === "APPLIED_IN_PLACE") {
          appliedChanges += 1;
          inPlaceChanges += result === "APPLIED_IN_PLACE" ? 1 : 0;
          appliedPaths.push(change.path);
          await onMutation?.(change, { stage: "firstLine", result });
          if (change.modelRewriteRequired) {
            const modelResult = await rewriteRolloutModelField(change, targetModel);
            retainOrValidateModelSnapshot(change, modelResult.originalTurnContextModels);
            change.appliedTurnContextRewrites = modelResult.replacedLines;
            if (modelResult.replacedLines > 0) {
              await onMutation?.(change, { stage: "model", result: "APPLIED" });
            }
          }
          if (result !== "APPLIED_IN_PLACE") await restoreOriginalMtime(change.path, change.originalMtimeMs);
          await onApplied?.(change);
        } else {
          skippedPaths.push(change.path);
          await onSkipped?.(change, result);
        }
      }
    } catch (error) {
      primaryError = error;
      throw error;
    } finally {
      if (worker) {
        try {
          await worker.close();
        } catch (closeError) {
          if (!primaryError) {
            throw closeError;
          }
        }
      }
    }
  } else {
    for (const change of firstLineChanges) {
      await onBeforeApply?.(change);
      const result = await tryRewriteCollectedFirstLine(change, {
        inPlaceWrite,
        inPlaceRestoreWrite,
        inPlaceSync
      });
      if (result === "APPLIED" || result === "APPLIED_IN_PLACE") {
        appliedChanges += 1;
        inPlaceChanges += result === "APPLIED_IN_PLACE" ? 1 : 0;
        appliedPaths.push(change.path);
        await onMutation?.(change, { stage: "firstLine", result });
        if (change.modelRewriteRequired) {
          const modelResult = await rewriteRolloutModelField(change, targetModel);
          retainOrValidateModelSnapshot(change, modelResult.originalTurnContextModels);
          change.appliedTurnContextRewrites = modelResult.replacedLines;
          if (modelResult.replacedLines > 0) {
            await onMutation?.(change, { stage: "model", result: "APPLIED" });
          }
        }
        if (result !== "APPLIED_IN_PLACE") await restoreOriginalMtime(change.path, change.originalMtimeMs);
        await onApplied?.(change);
      } else {
        skippedPaths.push(change.path);
        await onSkipped?.(change, result);
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
      retainOrValidateModelSnapshot(change, modelResult.originalTurnContextModels);
      await onMutation?.(change, { stage: "model", result: "APPLIED" });
      await restoreOriginalMtime(change.path, change.originalMtimeMs);
      appliedChanges += 1;
      appliedPaths.push(change.path);
      change.appliedTurnContextRewrites = modelResult.replacedLines;
      await onApplied?.(change);
    } else {
      skippedPaths.push(change.path);
      await onSkipped?.(change, "SKIP_CHANGED");
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

function retainOrValidateModelSnapshot(change, actualSnapshot) {
  const expected = change.originalTurnContextModels;
  if (!Array.isArray(expected) || expected.length === 0) {
    change.originalTurnContextModels = actualSnapshot;
    return;
  }
  if (JSON.stringify(expected) !== JSON.stringify(actualSnapshot)) {
    throw new Error(`Rollout turn_context model snapshot changed before rewrite: ${change.path}`);
  }
}

function modelSnapshotsEqual(expected, actual) {
  return Array.isArray(expected)
    && Array.isArray(actual)
    && JSON.stringify(expected) === JSON.stringify(actual);
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

export async function restoreSessionChanges(manifestEntries, options = {}) {
  if (!manifestEntries?.length) {
    return { restoredPaths: [], failures: [] };
  }

  const restoredPaths = [];
  const failures = [];
  let windowsWorker = null;
  async function restoreWindows(change) {
    windowsWorker ??= await (options.windowsRewriteWorkerFactory ?? createWindowsExclusiveRewriteWorker)();
    try {
      return await windowsWorker.rewrite(change, { requireOriginalMatch: false });
    } catch (error) {
      await windowsWorker.close().catch(() => {});
      windowsWorker = null;
      throw error;
    }
  }
  for (const entry of manifestEntries) {
    try {
      await options.onBeforeRestore?.(entry);
      if (entry.mutation) {
        validateProviderMutationDescriptor(entry.mutation, entry.path, entry.originalFirstLine, entry.originalSeparator);
        if (process.platform === "win32") {
          const result = await restoreWindows({
            ...entry, inPlaceMutation: entry.mutation, restoreProviderBytes: true
          });
          if (result !== "APPLIED_IN_PLACE") throw new Error(`Provider byte recovery failed: ${result}`);
        } else {
          await restoreProviderBytesInPlace(entry, options);
        }
      } else if (!entry.modelOnlyChange) {
        if (process.platform === "win32") {
          const result = await restoreWindows({
            path: entry.path,
            separator: entry.originalSeparator ?? "\n",
            updatedFirstLine: entry.originalFirstLine,
            originalMtimeMs: entry.originalMtimeMs
          });
          if (result !== "APPLIED") {
            throw new Error(
              `Unable to rewrite rollout file because it is currently in use. Close Codex and the Codex app, then retry. Locked file: ${entry.path}`
            );
          }
        } else {
          await rewriteFirstLine(entry.path, entry.originalFirstLine, entry.originalSeparator ?? "\n");
        }
      }
      if (entry.originalTurnContextModels?.length) {
        await restoreTurnContextModelsInFile(entry.path, entry.originalTurnContextModels, entry.originalSeparator);
      }
      if (!entry.mutation) await restoreOriginalMtime(entry.path, entry.originalMtimeMs);
      restoredPaths.push(entry.path);
      await options.onRestored?.(entry);
    } catch (error) {
      const failure = new Error(`Unable to restore rollout ${entry.path}: ${error.message}`, { cause: error });
      failure.path = entry.path;
      failures.push(failure);
      try {
        await options.onRestoreFailed?.(entry, error);
      } catch (observerError) {
        failures.push(new Error(
          `Unable to record rollout restore failure for ${entry.path}: ${observerError.message}`,
          { cause: observerError }
        ));
      }
    }
  }

  if (windowsWorker) {
    try { await windowsWorker.close(); }
    catch (error) { failures.push(error); }
  }

  if (failures.length > 0) {
    const aggregate = new AggregateError(
      failures,
      `Unable to restore ${failures.length} rollout target operation(s).`
    );
    aggregate.failures = failures.map((failure) => ({
      path: failure.path ?? null,
      message: failure.message
    }));
    throw aggregate;
  }
  return { restoredPaths, failures: [] };
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

    await fsp.chmod(tmpPath, beforeStat.mode);
    await syncStagedFile(tmpPath);
    await fsp.rename(tmpPath, filePath);
    await syncDirectory(path.dirname(filePath));
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
