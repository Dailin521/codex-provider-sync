import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

import { CoreError } from "./core-error.js";

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

async function listRolloutFiles(rootDir, fsImpl) {
  let entries;
  try {
    entries = await fsImpl.readdir(rootDir, { withFileTypes: true });
  } catch (error) {
    if (error?.code === "ENOENT") return [];
    throw error;
  }
  entries.sort((left, right) => left.name.localeCompare(right.name));
  const files = [];
  for (const entry of entries) {
    const fullPath = path.join(rootDir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await listRolloutFiles(fullPath, fsImpl));
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

export async function captureRolloutRevision(codexHome, { fsImpl = fs } = {}) {
  const manifest = [];
  const lockedRolloutFiles = [];
  for (const scope of SESSION_SCOPES) {
    const scopeRoot = path.join(codexHome, scope);
    for (const filePath of await listRolloutFiles(scopeRoot, fsImpl)) {
      const relativePath = path.relative(codexHome, filePath).split(path.sep).join("/");
      const revision = await captureStableFile(filePath, fsImpl, { allowLocked: true });
      manifest.push({ path: relativePath, ...revision });
      if (revision.locked) lockedRolloutFiles.push(relativePath);
    }
  }
  manifest.sort((left, right) => left.path.localeCompare(right.path));
  return {
    revision: sha256Revision(stableStringify(manifest)),
    fileCount: manifest.length,
    rolloutScanComplete: lockedRolloutFiles.length === 0,
    lockedRolloutFiles
  };
}

export async function captureStateDbRevision(storage, { fsImpl = fs, platform = process.platform } = {}) {
  const stateDbPath = storage.stateDbLocation?.path ?? null;
  if (!stateDbPath) {
    return sha256Revision(stableStringify({ stateDb: null }));
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
  platform = process.platform,
  fsImpl = fs
}) {
  const configRevision = captureConfigRevision(configText);
  const [rollout, stateDbRevision, backupRevision] = await Promise.all([
    captureRolloutRevision(codexHome, { fsImpl }),
    captureStateDbRevision(storage, { fsImpl, platform }),
    backupDir ? captureBackupRevision(backupDir, { fsImpl }) : Promise.resolve(null)
  ]);
  return {
    profileRevision,
    configRevision,
    storageRevision: captureStorageRevision({ profileRevision, configRevision, storage, platform }),
    rolloutRevision: rollout.revision,
    stateDbRevision,
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

