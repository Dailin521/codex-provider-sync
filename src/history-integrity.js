import fs from "node:fs/promises";
import { trackScanFiles } from "./scan-progress.js";
import fsSync from "node:fs";
import path from "node:path";

import { SESSION_DIRS } from "./constants.js";

// This module deliberately has no repair capability and is not part of the
// ordinary status/sync path.  It reads rollout JSONL only when an explicit
// advanced diagnostic asks for it.
const DEFAULT_LIMITS = Object.freeze({
  maxFiles: 2_000,
  maxRecordsPerFile: 100_000,
  maxLineBytes: 256 * 1024,
  maxIssues: 100
});
const READ_CHUNK_BYTES = 16 * 1024;
// This is the product's bounded native-ID contract, not a UUID restriction:
// older and fixture session IDs such as "one" remain valid identifiers.
const NATIVE_SESSION_ID_RE = /^[A-Za-z0-9_-]{1,128}$/;

function pathKey(value) {
  const resolved = path.resolve(value);
  return process.platform === "win32" ? resolved.toLowerCase() : resolved;
}

function isWithinRoot(root, candidate) {
  const relative = path.relative(root, candidate);
  return relative === "" || (!path.isAbsolute(relative)
    && relative !== ".."
    && !relative.startsWith(`..${path.sep}`));
}

function boundedPositiveInteger(value, fallback, minimum = 1) {
  return Number.isSafeInteger(value) && value >= minimum ? value : fallback;
}

function normalizeLimits(storage) {
  const supplied = storage?.limits ?? storage ?? {};
  return {
    maxFiles: boundedPositiveInteger(supplied.maxFiles, DEFAULT_LIMITS.maxFiles),
    maxRecordsPerFile: boundedPositiveInteger(
      supplied.maxRecordsPerFile,
      DEFAULT_LIMITS.maxRecordsPerFile
    ),
    maxLineBytes: boundedPositiveInteger(supplied.maxLineBytes, DEFAULT_LIMITS.maxLineBytes),
    maxIssues: boundedPositiveInteger(supplied.maxIssues, DEFAULT_LIMITS.maxIssues)
  };
}

function fileIdentity(stat) {
  return {
    dev: String(stat.dev),
    ino: String(stat.ino),
    size: String(stat.size),
    mtimeNs: String(stat.mtimeNs),
    ctimeNs: String(stat.ctimeNs)
  };
}

function sameIdentity(left, right) {
  return left.dev === right.dev
    && left.ino === right.ino
    && left.size === right.size
    && left.mtimeNs === right.mtimeNs
    && left.ctimeNs === right.ctimeNs;
}

function initialCounts() {
  return {
    filesDiscovered: 0,
    filesScanned: 0,
    recordsRead: 0,
    sessionsWithId: 0,
    unsupportedFiles: 0,
    jsonCorruptRecords: 0,
    oversizedRecords: 0,
    duplicateOrdinals: 0,
    outOfOrderOrdinals: 0,
    changedFiles: 0,
    truncatedFiles: 0
  };
}

function initialSkipped() {
  return {
    symlinkOrReparse: 0,
    outOfRoot: 0,
    notRegular: 0,
    unreadable: 0,
    scanLimit: 0
  };
}

function emptyResult(limits) {
  return {
    version: 1,
    outcome: "no-findings",
    counts: initialCounts(),
    skipped: initialSkipped(),
    // session_index.jsonl is a best-effort title override index in the
    // currently supported history format. It is not a documented display
    // order/index authority, so this scanner must not infer consistency.
    displayIndex: {
      status: "unsupported",
      reason: "no-known-display-index-schema"
    },
    issues: [],
    issuesTruncated: false,
    limits
  };
}

function addIssue(result, limits, issue) {
  if (result.issues.length < limits.maxIssues) result.issues.push(issue);
  else result.issuesTruncated = true;
}

function safeSessionId(record) {
  const id = record?.type === "session_meta" ? record.payload?.id : null;
  return typeof id === "string" && NATIVE_SESSION_ID_RE.test(id)
    ? id
    : null;
}

async function safeDirectory(rootPath, codexHomePhysical) {
  try {
    const lexical = path.resolve(rootPath);
    const stat = await fs.lstat(lexical);
    if (stat.isSymbolicLink()) return { kind: "symlink-or-reparse" };
    if (!stat.isDirectory()) return { kind: "not-directory" };
    const physical = path.resolve(await fs.realpath(lexical));
    if (!isWithinRoot(codexHomePhysical, physical)) return { kind: "out-of-root" };
    return { kind: "directory", lexical, physical };
  } catch (error) {
    return error?.code === "ENOENT" ? { kind: "missing" } : { kind: "unreadable" };
  }
}

async function enumerateRollouts(directory, scope, codexHomePhysical, result, limits, scanBudget) {
  const files = [];
  async function walk(lexicalDirectory, physicalDirectory) {
    let entries;
    try {
      entries = await fs.readdir(lexicalDirectory, { withFileTypes: true });
    } catch {
      result.skipped.unreadable += 1;
      return;
    }
    entries.sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      const candidate = path.join(lexicalDirectory, entry.name);
      let stat;
      try {
        stat = await fs.lstat(candidate);
      } catch {
        result.skipped.unreadable += 1;
        continue;
      }
      if (stat.isSymbolicLink()) {
        result.skipped.symlinkOrReparse += 1;
        continue;
      }
      if (stat.isDirectory()) {
        let physical;
        try { physical = path.resolve(await fs.realpath(candidate)); }
        catch { result.skipped.unreadable += 1; continue; }
        if (!isWithinRoot(codexHomePhysical, physical) || !isWithinRoot(physicalDirectory, physical)) {
          result.skipped.outOfRoot += 1;
          continue;
        }
        await walk(candidate, physical);
        continue;
      }
      if (!stat.isFile()) {
        result.skipped.notRegular += 1;
        continue;
      }
      if (!entry.name.startsWith("rollout-") || !entry.name.endsWith(".jsonl")) continue;
      result.counts.filesDiscovered += 1;
      if (scanBudget.scheduled >= limits.maxFiles) {
        result.skipped.scanLimit += 1;
        continue;
      }
      scanBudget.scheduled += 1;
      files.push({ filePath: candidate, scope, rootPath: lexicalDirectory, rootPhysical: physicalDirectory });
    }
  }
  await walk(directory.lexical, directory.physical);
  return files;
}

async function openSafeFile(candidate) {
  const [named, currentRoot] = await Promise.all([
    fs.lstat(candidate.filePath, { bigint: true }),
    fs.realpath(candidate.rootPath)
  ]);
  if (pathKey(path.resolve(currentRoot)) !== pathKey(candidate.rootPhysical)) return { kind: "changed" };
  if (named.isSymbolicLink()) return { kind: "symlink-or-reparse" };
  if (!named.isFile()) return { kind: "not-regular" };
  const physical = path.resolve(await fs.realpath(candidate.filePath));
  if (!isWithinRoot(candidate.rootPhysical, physical)) return { kind: "out-of-root" };
  const handle = await fs.open(candidate.filePath, fsSync.constants.O_RDONLY);
  try {
    const opened = await handle.stat({ bigint: true });
    if (!opened.isFile() || !sameIdentity(fileIdentity(named), fileIdentity(opened))) {
      await handle.close();
      return { kind: "changed" };
    }
    return { kind: "open", handle, physical, identity: fileIdentity(opened) };
  } catch (error) {
    await handle.close().catch(() => {});
    throw error;
  }
}

async function unchangedAfterRead(candidate, handle, physical, identity) {
  const [finalStat, named, finalPhysical, finalRoot] = await Promise.all([
    handle.stat({ bigint: true }),
    fs.lstat(candidate.filePath, { bigint: true }),
    fs.realpath(candidate.filePath),
    fs.realpath(candidate.rootPath)
  ]);
  return !named.isSymbolicLink()
    && named.isFile()
    && pathKey(path.resolve(finalPhysical)) === pathKey(physical)
    && pathKey(path.resolve(finalRoot)) === pathKey(candidate.rootPhysical)
    && isWithinRoot(candidate.rootPhysical, path.resolve(finalPhysical))
    && sameIdentity(identity, fileIdentity(finalStat))
    && sameIdentity(identity, fileIdentity(named));
}

function recordIssue(result, limits, code, sessionId, scope, line) {
  addIssue(result, limits, { code, sessionId, scope, line });
}

function isSessionMetaHeader(record) {
  return record?.type === "session_meta"
    && record.payload !== null
    && typeof record.payload === "object"
    && !Array.isArray(record.payload);
}

function isRecordObject(record) {
  return record !== null
    && typeof record === "object"
    && !Array.isArray(record)
    && typeof record.type === "string";
}

function decodeJsonRecord(chunks, byteLength) {
  let text;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks, byteLength));
  } catch {
    return { kind: "invalid-utf8", record: null };
  }
  try {
    return { kind: "valid", record: JSON.parse(text) };
  } catch {
    return { kind: "json-corrupt", record: null };
  }
}

/**
 * Low-level bounded scanner exported for synthetic-fixture tests. It does not
 * return paths, raw JSON, or message bodies. `options.beforeFinalCheck` is a
 * test hook and is not used by the public integration path.
 */
export async function scanHistoryIntegrityFile(candidate, result, limits, options = {}) {
  let opened;
  try {
    opened = await openSafeFile(candidate);
  } catch {
    result.skipped.unreadable += 1;
    return;
  }
  if (opened.kind !== "open") {
    if (opened.kind === "changed") {
      result.counts.changedFiles += 1;
      recordIssue(result, limits, "changed-during-scan", null, candidate.scope, null);
    } else if (opened.kind === "symlink-or-reparse") result.skipped.symlinkOrReparse += 1;
    else if (opened.kind === "out-of-root") result.skipped.outOfRoot += 1;
    else result.skipped.notRegular += 1;
    return;
  }

  const { handle, physical, identity } = opened;
  let sessionId = null;
  let headerChecked = false;
  let headerSupported = false;
  let unsupportedFormatDetected = false;
  let recordsInFile = 0;
  let lineNumber = 0;
  let previousOrdinal = null;
  const seenOrdinals = new Set(); // bounded by maxRecordsPerFile.
  let stopAtLimit = false;
  let chunks = [];
  let lineBytes = 0;
  let oversized = false;
  let finished = false;
  let readCompleted = false;

  const consumeLine = () => {
    lineNumber += 1;
    if (oversized) {
      result.counts.oversizedRecords += 1;
      recordIssue(result, limits, "record-too-large", sessionId, candidate.scope, lineNumber);
    } else if (lineBytes > 0) {
      recordsInFile += 1;
      result.counts.recordsRead += 1;
      if (recordsInFile > limits.maxRecordsPerFile) {
        stopAtLimit = true;
      } else {
        const decoded = decodeJsonRecord(chunks, lineBytes);
        let record = decoded.record;
        if (decoded.kind !== "valid") {
          result.counts.jsonCorruptRecords += 1;
          recordIssue(result, limits, decoded.kind, sessionId, candidate.scope, lineNumber);
        }
        if (lineNumber === 1) {
          headerChecked = true;
          headerSupported = isSessionMetaHeader(record);
          if (headerSupported) {
            sessionId = safeSessionId(record);
            if (sessionId) result.counts.sessionsWithId += 1;
          }
        }
        if (record && !isRecordObject(record)) unsupportedFormatDetected = true;
        // No current repository contract defines ordinal semantics. Numeric root
        // values are therefore reported only as observations: gaps and starts
        // are intentionally ignored, while equal/decreasing values are facts.
        const ordinal = record?.ordinal;
        if (Number.isSafeInteger(ordinal)) {
          if (seenOrdinals.has(ordinal)) {
            result.counts.duplicateOrdinals += 1;
            recordIssue(result, limits, "ordinal-duplicate-observed", sessionId, candidate.scope, lineNumber);
          }
          if (previousOrdinal !== null && ordinal < previousOrdinal) {
            result.counts.outOfOrderOrdinals += 1;
            recordIssue(result, limits, "ordinal-out-of-order-observed", sessionId, candidate.scope, lineNumber);
          }
          seenOrdinals.add(ordinal);
          previousOrdinal = ordinal;
        }
      }
    } else if (lineNumber === 1) {
      headerChecked = true;
    }
    chunks = [];
    lineBytes = 0;
    oversized = false;
  };

  try {
    const { size } = await handle.stat({ bigint: true });
    const snapshotSize = Number(size);
    const buffer = Buffer.allocUnsafe(READ_CHUNK_BYTES);
    for (let position = 0; position < snapshotSize && !stopAtLimit;) {
      const { bytesRead } = await handle.read(buffer, 0, Math.min(buffer.length, snapshotSize - position), position);
      if (!bytesRead) break;
      position += bytesRead;
      let start = 0;
      while (start < bytesRead && !stopAtLimit) {
        const foundNewline = buffer.indexOf(0x0a, start);
        const newline = foundNewline >= 0 && foundNewline < bytesRead ? foundNewline : -1;
        const end = newline >= 0 ? newline : bytesRead;
        const segment = buffer.subarray(start, end);
        if (!oversized) {
          if (lineBytes + segment.length > limits.maxLineBytes) {
            oversized = true;
            chunks = [];
          } else if (segment.length > 0) {
            chunks.push(Buffer.from(segment));
            lineBytes += segment.length;
          }
        }
        if (newline >= 0) consumeLine();
        start = newline >= 0 ? newline + 1 : bytesRead;
      }
    }
    // JSONL's final newline is the commit boundary for this diagnostic. A
    // syntactically valid tail may still be an actively appended record, so it
    // is deliberately inconclusive rather than JSON corruption or healthy.
    if (!stopAtLimit && (lineBytes > 0 || oversized)) {
      result.counts.truncatedFiles += 1;
      recordIssue(result, limits, "unterminated-record", sessionId, candidate.scope, lineNumber + 1);
      chunks = [];
      lineBytes = 0;
      oversized = false;
    }
    if (!headerChecked || !headerSupported || unsupportedFormatDetected) {
      result.counts.unsupportedFiles += 1;
      recordIssue(result, limits, "unsupported-format", sessionId, candidate.scope, null);
    }
    if (stopAtLimit) {
      result.counts.truncatedFiles += 1;
      recordIssue(result, limits, "record-limit-reached", sessionId, candidate.scope, lineNumber);
    } else {
      finished = true;
      readCompleted = true;
    }
    await options.beforeFinalCheck?.();
    if (!(await unchangedAfterRead(candidate, handle, physical, identity))) {
      result.counts.changedFiles += 1;
      recordIssue(result, limits, "changed-during-scan", sessionId, candidate.scope, null);
    } else if (finished) {
      result.counts.filesScanned += 1;
    }
  } catch {
    if (readCompleted) {
      result.counts.changedFiles += 1;
      recordIssue(result, limits, "changed-during-scan", sessionId, candidate.scope, null);
    } else result.skipped.unreadable += 1;
  } finally {
    await handle.close().catch(() => {});
  }
}

/**
 * Read-only advanced diagnostic. `storage` is optional bounds configuration:
 * `{maxFiles, maxRecordsPerFile, maxLineBytes, maxIssues}` or `{limits: ...}`.
 * It intentionally does not accept external paths or a display-index schema.
 * @param {string} codexHome
 * @param {Record<string, unknown> & {signal?: AbortSignal, onProgress?: (event: {stage: string, status: string, progress?: number, count?: number}) => unknown}} [storage]
 */
export async function inspectHistoryIntegrity(codexHome, storage = undefined) {
  const limits = normalizeLimits(storage);
  const result = emptyResult(limits);
  const scanBudget = { scheduled: 0 };
  let codexHomePhysical;
  try {
    const home = path.resolve(codexHome);
    const stat = await fs.lstat(home);
    if (stat.isSymbolicLink() || !stat.isDirectory()) {
      result.skipped.symlinkOrReparse += 1;
      result.outcome = "inconclusive";
      return result;
    }
    codexHomePhysical = path.resolve(await fs.realpath(home));
  } catch {
    result.skipped.unreadable += 1;
    result.outcome = "inconclusive";
    return result;
  }

  for (const scope of SESSION_DIRS) {
    const directory = await safeDirectory(path.join(codexHome, scope), codexHomePhysical);
    if (directory.kind === "missing") continue;
    if (directory.kind !== "directory") {
      if (directory.kind === "symlink-or-reparse") result.skipped.symlinkOrReparse += 1;
      else if (directory.kind === "out-of-root") result.skipped.outOfRoot += 1;
      else if (directory.kind === "unreadable") result.skipped.unreadable += 1;
      else result.skipped.notRegular += 1;
      continue;
    }
    const files = await enumerateRollouts(directory, scope, codexHomePhysical, result, limits, scanBudget);
    const trackedFiles = storage?.onProgress || storage?.signal
      ? trackScanFiles(files, { ...storage, stage: `integrity_${scope}` }) : files;
    for (const candidate of trackedFiles) {
      await scanHistoryIntegrityFile(candidate, result, limits, storage?.testHooks);
    }
  }

  const hasFindings = result.counts.jsonCorruptRecords > 0
    || result.counts.duplicateOrdinals > 0
    || result.counts.outOfOrderOrdinals > 0;
  const inconclusive = result.counts.unsupportedFiles > 0
    || result.counts.oversizedRecords > 0
    || result.counts.changedFiles > 0
    || result.counts.truncatedFiles > 0
    || result.issuesTruncated
    || Object.values(result.skipped).some((count) => count > 0);
  result.outcome = hasFindings && inconclusive ? "findings-and-inconclusive"
    : hasFindings ? "findings"
      : inconclusive ? "inconclusive"
        : "no-findings";
  return result;
}
