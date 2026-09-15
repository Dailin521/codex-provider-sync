import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

import { CoreError } from "./core-error.js";
import { fileReadSkipReason, rolloutSkip, providerPathKey } from "./provider-skips.js";
import { collectProviderChanges, readProviderRevisionHeader } from "./session-files.js";
import { readSqliteProviderRevisionState } from "./sqlite-state.js";

const SESSION_SCOPES = ["sessions", "archived_sessions"];
const LOCKED_FILE_CODES = new Set(["EACCES", "EBUSY", "EPERM", "ETXTBSY"]);

function canonicalJsonValue(value) {
  if (Array.isArray(value)) return value.map(canonicalJsonValue);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value).sort().map((key) => [key, canonicalJsonValue(value[key])])
    );
  }
  return value;
}

export function stableStringify(value) {
  return JSON.stringify(canonicalJsonValue(value));
}

export function sha256Revision(value) {
  const bytes = Buffer.isBuffer(value) ? value : Buffer.from(String(value), "utf8");
  return createHash("sha256").update(bytes).digest("base64url");
}

function comparablePath(value, platform) {
  if (typeof value !== "string") return null;
  const resolved = path.resolve(value);
  return platform === "win32" ? resolved.toLowerCase() : resolved;
}

async function statOrNull(filePath, fsImpl) {
  try {
    return await fsImpl.stat(filePath, { bigint: true });
  } catch (error) {
    if (error?.code === "ENOENT") return null;
    throw error;
  }
}

function statIdentity(stats) {
  return {
    size: stats.size.toString(),
    mtimeNs: stats.mtimeNs.toString(),
    ctimeNs: stats.ctimeNs.toString()
  };
}

function sameStat(left, right) {
  return left && right
    && left.size === right.size
    && left.mtimeNs === right.mtimeNs
    && left.ctimeNs === right.ctimeNs;
}

async function physicalFileIdentity(filePath, fsImpl, reason) {
  let stats;
  try {
    stats = await fsImpl.lstat(filePath, { bigint: true });
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
    throw error;
  }
  if (!stats.isFile() || stats.isSymbolicLink() || stats.ino === 0n) {
    throw new CoreError("STALE_STATE", "The revision file identity cannot be verified.", { details: { reason } });
  }
  return {
    identity: {
      realPath: comparablePath(await fsImpl.realpath(filePath), process.platform),
      dev: String(stats.dev), ino: String(stats.ino), nlink: String(stats.nlink)
    },
    size: String(stats.size),
    snapshot: {
      size: Number(stats.size), mtimeMs: Number(stats.mtimeNs) / 1e6,
      mode: Number(stats.mode), nlink: Number(stats.nlink),
      dev: String(stats.dev), ino: String(stats.ino)
    }
  };
}

async function captureProviderHeader(filePath, fsImpl, minimumSize, onProviderHeader, allowIncomplete = false) {
  const before = await physicalFileIdentity(filePath, fsImpl, "rollout");
  if (minimumSize !== undefined && BigInt(before.size) < BigInt(minimumSize)) {
    throw new CoreError("STALE_STATE", "A planned rollout was truncated.", { details: { reason: "rollout", fileChanged: true } });
  }
  let header;
  let record;
  try {
    record = await readProviderRevisionHeader(filePath, { fsImpl });
    header = { headerHash: sha256Revision(record.firstLine + record.separator) };
  } catch (error) {
    if (allowIncomplete && error?.name === "RolloutMetadataLimitError") header = { incomplete: true };
    else {
      if (!LOCKED_FILE_CODES.has(error?.code)) throw error;
      const reason = fileReadSkipReason(error);
      header = { locked: reason === "locked", skipReason: reason, causeCode: error.code };
    }
  }
  const after = await physicalFileIdentity(filePath, fsImpl, "rollout");
  if (stableStringify(before.identity) !== stableStringify(after.identity)
      || BigInt(after.size) < BigInt(before.size)) {
    throw new CoreError("STALE_STATE", "A planned rollout was replaced or truncated.", { details: { reason: "rollout", fileChanged: true } });
  }
  onProviderHeader?.(filePath, {
    record, beforeSnapshot: before.snapshot, afterSnapshot: after.snapshot,
    locked: header.locked === true
  });
  return { ...after.identity, ...header, observedSize: after.size };
}

async function captureStableFile(filePath, fsImpl, { allowLocked = false } = {}) {
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const beforeStats = await statOrNull(filePath, fsImpl);
    if (!beforeStats) return { present: false };
    if (!beforeStats.isFile()) {
      throw new CoreError("STALE_STATE", "A revision target is not a regular file.", {
        details: { reason: "storage" }
      });
    }
    const before = statIdentity(beforeStats);
    try {
      const bytes = await fsImpl.readFile(filePath);
      const afterStats = await statOrNull(filePath, fsImpl);
      const after = afterStats ? statIdentity(afterStats) : null;
      if (sameStat(before, after)) {
        return { present: true, ...after, sha256: sha256Revision(bytes) };
      }
    } catch (error) {
      if (allowLocked && LOCKED_FILE_CODES.has(error?.code)) {
        return {
          present: true,
          ...before,
          locked: true,
          causeCode: error.code
        };
      }
      throw error;
    }
  }
  throw new CoreError("STALE_STATE", "A revision target changed while it was being captured.", {
    details: { reason: "storage" }
  });
}

async function captureStableMetadata(filePath, fsImpl, { allowLocked = false } = {}) {
  for (let attempt = 0; attempt < 2; attempt += 1) {
    let beforeStats;
    try {
      beforeStats = await statOrNull(filePath, fsImpl);
    } catch (error) {
      if (allowLocked && LOCKED_FILE_CODES.has(error?.code)) {
        return { present: true, locked: true, causeCode: error.code };
      }
      throw error;
    }
    if (!beforeStats) return { present: false };
    if (!beforeStats.isFile()) {
      throw new CoreError("STALE_STATE", "A revision target is not a regular file.", {
        details: { reason: "storage" }
      });
    }
    const before = statIdentity(beforeStats);
    try {
      const afterStats = await statOrNull(filePath, fsImpl);
      const after = afterStats ? statIdentity(afterStats) : null;
      if (sameStat(before, after)) return { present: true, ...after };
    } catch (error) {
      if (allowLocked && LOCKED_FILE_CODES.has(error?.code)) {
        return {
          present: true,
          ...before,
          locked: true,
          causeCode: error.code
        };
      }
      throw error;
    }
  }
  throw new CoreError("STALE_STATE", "A revision target changed while it was being captured.", {
    details: { reason: "storage" }
  });
}

async function listRolloutFiles(rootDir, fsImpl, optionalRoot = true) {
  try {
    const info = await fsImpl.lstat(rootDir);
    if (info.isSymbolicLink() || !info.isDirectory()) throw new CoreError("STALE_STATE", "A rollout directory is unsafe.", { details: { reason: "rollout" } });
  } catch (error) {
    if (optionalRoot && error?.code === "ENOENT") return [];
    throw error;
  }
  let entries;
  try {
    entries = await fsImpl.readdir(rootDir, { withFileTypes: true });
  } catch (error) {
    throw error;
  }
  entries.sort((left, right) => left.name.localeCompare(right.name));
  const files = [];
  for (const entry of entries) {
    const fullPath = path.join(rootDir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await listRolloutFiles(fullPath, fsImpl, false));
    } else if (entry.isFile() && entry.name.startsWith("rollout-") && entry.name.endsWith(".jsonl")) {
      files.push(fullPath);
    } else if (entry.isSymbolicLink()) {
      throw new CoreError("STALE_STATE", "A rollout revision contains an unsupported symbolic link.", {
        details: { reason: "rollout" }
      });
    }
  }
  return files;
}

export async function captureRolloutRevision(codexHome, { fsImpl = fs, mode = "content", minimumSizes = {}, expectedFiles = null } = {}, onProviderHeader = undefined) {
  if (!["content", "metadata", "provider", "status"].includes(mode)) {
    throw new CoreError("INVALID_INPUT", "Unsupported rollout revision mode.");
  }
  const manifest = [];
  const lockedRolloutFiles = [];
  const observedSizes = {};
  const fileBindings = [];
  const seen = new Set();
  const expected = expectedFiles ? new Map(expectedFiles.map(file => [providerPathKey(file.path), file])) : null;
  for (const scope of SESSION_SCOPES) {
    const scopeRoot = path.join(codexHome, scope);
    const rootPreviouslyPresent = expectedFiles?.some(file => path.relative(codexHome, file.path).split(path.sep)[0] === scope);
    for (const filePath of await listRolloutFiles(scopeRoot, fsImpl, !rootPreviouslyPresent)) {
      const relativePath = path.relative(codexHome, filePath).split(path.sep).join("/");
      const key = providerPathKey(filePath);
      seen.add(key);
      const prior = expected?.get(key);
      let record;
      let revision;
      if (expected && (!prior || prior.skip)) {
        const skip = prior?.skip ?? rolloutSkip(filePath, "deferred", "revalidate");
        revision = { skipped: skip.reason };
        record = { skip };
      } else {
        try {
          revision = mode === "provider" || mode === "status"
            ? await captureProviderHeader(filePath, fsImpl, minimumSizes[relativePath], (_, value) => { record = value; })
            : mode === "metadata"
            ? await captureStableMetadata(filePath, fsImpl, { allowLocked: true })
            : await captureStableFile(filePath, fsImpl, { allowLocked: true });
          if (revision.skipReason) record = { skip: rolloutSkip(filePath, revision.skipReason, expected ? "revalidate" : "scan", prior?.id) };
          if (prior && (prior.headerHash !== revision.headerHash || prior.realPath !== revision.realPath
              || prior.ino !== revision.ino || prior.dev !== revision.dev || prior.nlink !== revision.nlink
              || BigInt(revision.observedSize ?? 0) < BigInt(prior.observedSize ?? 0))) {
            record = { skip: rolloutSkip(filePath, "changed", "revalidate", prior.id) };
          }
        } catch (error) {
          const reason = ["provider", "status"].includes(mode) ? fileReadSkipReason(error) : null;
          if (!reason) throw error;
          record = { skip: rolloutSkip(filePath, reason, expected ? "revalidate" : "scan", prior?.id) };
          revision = { skipped: reason };
        }
      }
      onProviderHeader?.(filePath, record);
      fileBindings.push({ path: filePath, ...revision, ...(record?.skip ? { skip: record.skip } : {}) });
      const { observedSize, ...binding } = revision;
      if (observedSize !== undefined) observedSizes[relativePath] = observedSize;
      manifest.push({ path: relativePath, ...binding });
      if (revision.locked) lockedRolloutFiles.push(relativePath);
    }
  }
  if (expected) for (const [key, prior] of expected) if (!seen.has(key)) {
    const skip = prior.skip ?? rolloutSkip(prior.path, "missing", "revalidate", prior.id);
    onProviderHeader?.(prior.path, { skip });
    fileBindings.push({ ...prior, skip });
  }
  manifest.sort((left, right) => left.path.localeCompare(right.path));
  return {
    revision: sha256Revision(stableStringify(manifest)),
    fileCount: manifest.length,
    fileBindings,
    rolloutScanComplete: lockedRolloutFiles.length === 0 && !fileBindings.some(file => file.skip),
    lockedRolloutFiles,
    ...(["provider", "status"].includes(mode) ? { observedSizes } : {})
  };
}

// Keep the revision reader/manifest authoritative. Its private callback lends
// first-line facts to the existing Provider-only collector during this call.
// No body read, persistent cache, new manifest algorithm or Apply descriptor.
export async function collectProviderPreparationFacts(codexHome, targetProvider, { fsImpl = fs, expectedFiles = null } = {}) {
  const records = new Map();
  try {
    const rollout = await captureRolloutRevision(codexHome, { fsImpl, mode: "provider", expectedFiles },
      (filePath, record) => records.set(filePath, record));
    const scan = await collectProviderChanges(codexHome, targetProvider, { skipLockedReads: true }, records);
    const skips = new Map(scan.skippedItems.map(item => [providerPathKey(item.path), item]));
    const files = new Map(scan.files.map(file => [providerPathKey(file.path), file]));
    rollout.fileBindings = rollout.fileBindings.map(binding => ({ ...binding,
      id: files.get(providerPathKey(binding.path))?.id ?? binding.id ?? null,
      ...(skips.has(providerPathKey(binding.path)) ? { skip: skips.get(providerPathKey(binding.path)) } : {}) }));
    rollout.rolloutScanComplete = scan.skippedItems.length === 0;
    return { rollout, scan };
  } catch (error) {
    if (error?.name === "RolloutMetadataLimitError") {
      throw new CoreError("ROLLOUT_METADATA_TOO_LARGE", "Session metadata exceeds the 128 MiB supported limit.", { cause: error });
    }
    throw error;
  } finally {
    records.clear();
  }
}

export async function captureStateDbRevision(storage, { fsImpl = fs, platform = process.platform, mode = "content", schemaOnly = false } = {}) {
  const stateDbPath = storage.stateDbLocation?.path ?? null;
  if (!stateDbPath) {
    return sha256Revision(stableStringify({ stateDb: null }));
  }
  if (mode === "provider" || mode === "status") {
    const before = await physicalFileIdentity(stateDbPath, fsImpl, "state-db");
    let state;
    // Status is a Provider/archived snapshot, not a hash of changing chat/WAL bytes.
    // Unsupported WSL remains diagnostic-only; do not open its SQLite database.
    if (mode === "status" && storage.sqliteAccess?.supported === false) state = { unsupported: true };
    else {
      try { state = await readSqliteProviderRevisionState(stateDbPath, { includeArchived: mode === "status" });
        if (schemaOnly) state = { schema: state.schema, key: state.key }; }
      catch (error) {
        // Preserve Status's unreadable index presentation, never fabricate healthy counts.
        if (mode !== "status" || error?.code !== "SQLITE_UNREADABLE") throw error;
        state = { unreadable: true };
      }
    }
    const after = await physicalFileIdentity(stateDbPath, fsImpl, "state-db");
    if (stableStringify(before.identity) !== stableStringify(after.identity)) {
      throw new CoreError("STALE_STATE", "The planned State DB was replaced.", { details: { reason: "state-db" } });
    }
    return sha256Revision(stableStringify({ identity: after.identity, state }));
  }
  const manifest = [];
  for (const suffix of ["", "-wal", "-shm"]) {
    manifest.push({
      path: suffix || "state_5.sqlite",
      revision: await captureStableFile(`${stateDbPath}${suffix}`, fsImpl)
    });
  }
  return sha256Revision(stableStringify({
    stateDbPath: comparablePath(stateDbPath, platform),
    source: storage.stateDbLocation.source,
    manifest
  }));
}

async function listDirectoryFiles(rootDir, currentDir, fsImpl) {
  let entries = await fsImpl.readdir(currentDir, { withFileTypes: true });
  entries = entries.sort((left, right) => left.name.localeCompare(right.name));
  const files = [];
  for (const entry of entries) {
    const fullPath = path.join(currentDir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await listDirectoryFiles(rootDir, fullPath, fsImpl));
    } else if (entry.isFile()) {
      files.push({
        path: path.relative(rootDir, fullPath).split(path.sep).join("/"),
        revision: await captureStableFile(fullPath, fsImpl)
      });
    } else {
      throw new CoreError("RESTORE_VALIDATION_FAILED", "A managed backup contains an unsupported linked target.");
    }
  }
  return files;
}

export async function captureBackupRevision(backupDir, { fsImpl = fs } = {}) {
  const root = path.resolve(backupDir);
  const stats = await statOrNull(root, fsImpl);
  if (!stats?.isDirectory()) {
    throw new CoreError("RESTORE_VALIDATION_FAILED", "The selected managed backup is unavailable.");
  }
  const files = await listDirectoryFiles(root, root, fsImpl);
  files.sort((left, right) => left.path.localeCompare(right.path));
  return sha256Revision(stableStringify(files));
}

export function captureConfigRevision(configText) {
  return sha256Revision(Buffer.from(configText, "utf8"));
}

export function captureStorageRevision({ profileRevision, configRevision, storage, platform = process.platform }) {
  return sha256Revision(stableStringify({
    schemaVersion: 1,
    profileRevision,
    configRevision,
    codexHome: comparablePath(storage.codexHome, platform),
    sqliteHome: comparablePath(storage.sqliteHome, platform),
    sqliteHomeSource: storage.sqliteHomeSource,
    sqliteAccess: {
      supported: storage.sqliteAccess?.supported !== false,
      reason: storage.sqliteAccess?.reason ?? null
    },
    allowLegacyRootFallback: Boolean(storage.allowLegacyRootFallback),
    stateDbLocation: storage.stateDbLocation
      ? {
          path: comparablePath(storage.stateDbLocation.path, platform),
          source: storage.stateDbLocation.source
        }
      : null
  }));
}

export async function captureOperationRevisions({
  codexHome,
  profileRevision,
  configText,
  storage,
  backupDir = null,
  rolloutRevisionMode = "content",
  minimumRolloutSizes = {},
  providerScoped = false,
  platform = process.platform,
  fsImpl = fs
}, preparedRollout = null) {
  const configRevision = captureConfigRevision(configText);
  const [rollout, stateDbRevision, backupRevision] = await Promise.all([
    preparedRollout ?? captureRolloutRevision(codexHome, { fsImpl, mode: rolloutRevisionMode, minimumSizes: minimumRolloutSizes }),
    captureStateDbRevision(storage, { fsImpl, platform, schemaOnly: providerScoped, mode: ["provider", "status"].includes(rolloutRevisionMode) ? rolloutRevisionMode : "content" }),
    backupDir ? captureBackupRevision(backupDir, { fsImpl }) : Promise.resolve(null)
  ]);
  return {
    profileRevision,
    configRevision,
    storageRevision: captureStorageRevision({ profileRevision, configRevision, storage, platform }),
    rolloutRevision: rollout.revision,
    stateDbRevision,
    ...(rollout.observedSizes ? { providerRolloutSizes: rollout.observedSizes } : {}),
    ...(backupRevision ? { backupRevision } : {}),
    rolloutScanComplete: rollout.rolloutScanComplete,
    lockedRolloutFiles: rollout.lockedRolloutFiles,
    rolloutFileCount: rollout.fileCount
  };
}

export function revisionMismatch(expected, actual) {
  for (const [field, reason] of [
    ["profileRevision", "profile"],
    ["configRevision", "config"],
    ["storageRevision", "storage"],
    ["rolloutRevision", "rollout"],
    ["stateDbRevision", "state-db"],
    ["backupRevision", "backup"]
  ]) {
    if ((expected[field] ?? null) !== (actual[field] ?? null)) return reason;
  }
  return null;
}
