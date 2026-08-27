import fs from "node:fs/promises";
import { createReadStream } from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";

import {
  BACKUP_NAMESPACE,
  DB_FILE_BASENAME,
  GLOBAL_STATE_BACKUP_FILE_BASENAME,
  GLOBAL_STATE_FILE_BASENAME,
  defaultBackupRoot
} from "./constants.js";
import { CoreError } from "./core-error.js";
import { writeFileAtomic } from "./atomic-file.js";
import {
  copyFileAtomic,
  prepareRestoreBackup,
  refreshBackupInventory,
  restoreBackup
} from "./backup.js";
import { sha256Revision, stableStringify } from "./operation-revision.js";
import {
  captureSessionRestoreEntries,
  restoreSessionChanges
} from "./session-files.js";
import {
  createSqliteOnlineBackup,
  restoreSqliteOnlineBackup
} from "./sqlite-state.js";
import {
  RestoreJournal,
  readRestoreJournal,
  reopenRestoreJournal
} from "./restore-journal.js";
import { resolveStateDbLockResource } from "./state-db-lock.js";
import { markBackupTransactionRolledBack } from "./transaction-journal.js";

export const RESTORE_SNAPSHOT_MANIFEST_BASENAME = "restore-snapshot.v2.json";

function compareOrdinal(left, right) {
  return left < right ? -1 : (left > right ? 1 : 0);
}

function pathKey(value, platform = process.platform) {
  const resolved = path.resolve(value);
  return platform === "win32" ? resolved.toLowerCase() : resolved;
}

function restoreBoundaryError(errorCode, targetKind, cause) {
  return new CoreError(
    errorCode,
    errorCode === "RECOVERY_REQUIRED"
      ? "A Restore target physical boundary can no longer be verified."
      : "A Restore target physical boundary cannot be verified.",
    {
      cause: cause instanceof Error ? cause : undefined,
      details: {
        operationKind: "restore",
        ...(errorCode === "LOCK_UNVERIFIABLE" ? { lockScope: "codex-home" } : {}),
        ...(typeof targetKind === "string" ? { targetKind } : {})
      }
    }
  );
}

async function resolveStablePhysicalDirectory(directory, platform, errorCode, targetKind = null) {
  try {
    const lexical = path.resolve(directory);
    const first = await fs.realpath(lexical);
    const stat = await fs.stat(first);
    if (!stat.isDirectory()) {
      throw new Error("Restore physical directory identity is not a directory.");
    }
    const second = await fs.realpath(lexical);
    if (pathKey(first, platform) !== pathKey(second, platform)) {
      throw new Error("Restore physical directory identity changed while it was resolved.");
    }
    return path.resolve(first);
  } catch (error) {
    if (error instanceof CoreError && error.code === errorCode) throw error;
    throw restoreBoundaryError(errorCode, targetKind, error);
  }
}

export async function captureStableRestoreSource(
  backupDir,
  { platform = process.platform, errorCode = "RESTORE_VALIDATION_FAILED" } = {}
) {
  const physicalBackupDir = await resolveStablePhysicalDirectory(
    backupDir,
    platform,
    errorCode,
    "sourceBackup"
  );
  return {
    backupId: path.basename(physicalBackupDir),
    backupDir: physicalBackupDir,
    revision: await captureRestoreSourceIdentity(physicalBackupDir)
  };
}

async function restoreTargetRelativePath(target, physicalHome, platform, errorCode) {
  const targetPath = path.resolve(target?.targetPath ?? "");
  const compare = (left, right) => platform === "win32"
    ? left.toLowerCase() === right.toLowerCase()
    : left === right;
  if (target.kind === "config") {
    if (!compare(path.basename(targetPath), "config.toml")) {
      throw restoreBoundaryError(errorCode, target.kind);
    }
    const parentPhysical = await resolveStablePhysicalDirectory(
      path.dirname(targetPath),
      platform,
      errorCode,
      target.kind
    );
    if (pathKey(parentPhysical, platform) !== pathKey(physicalHome, platform)) {
      throw restoreBoundaryError(errorCode, target.kind);
    }
    return ["config.toml"];
  } else if (target.kind === "globalState") {
    const allowed = [GLOBAL_STATE_FILE_BASENAME, GLOBAL_STATE_BACKUP_FILE_BASENAME];
    const fileName = path.basename(targetPath);
    if (!allowed.some((name) => compare(fileName, name))) {
      throw restoreBoundaryError(errorCode, target.kind);
    }
    const parentPhysical = await resolveStablePhysicalDirectory(
      path.dirname(targetPath),
      platform,
      errorCode,
      target.kind
    );
    if (pathKey(parentPhysical, platform) !== pathKey(physicalHome, platform)) {
      throw restoreBoundaryError(errorCode, target.kind);
    }
    return [allowed.find((name) => compare(fileName, name))];
  } else if (target.kind === "rollout") {
    if (!/^rollout-.*\.jsonl$/i.test(path.basename(targetPath))) {
      throw restoreBoundaryError(errorCode, target.kind);
    }
    let rawRoot = path.dirname(targetPath);
    while (true) {
      if (["sessions", "archived_sessions"].some((name) => compare(path.basename(rawRoot), name))) break;
      const parent = path.dirname(rawRoot);
      if (parent === rawRoot) throw restoreBoundaryError(errorCode, target.kind);
      rawRoot = parent;
    }
    const rootParentPhysical = await resolveStablePhysicalDirectory(
      path.dirname(rawRoot),
      platform,
      errorCode,
      target.kind
    );
    if (pathKey(rootParentPhysical, platform) !== pathKey(physicalHome, platform)) {
      throw restoreBoundaryError(errorCode, target.kind);
    }
    const nested = path.relative(rawRoot, targetPath);
    if (!nested
        || nested === ".."
        || nested.startsWith(`..${path.sep}`)
        || path.isAbsolute(nested)) {
      throw restoreBoundaryError(errorCode, target.kind);
    }
    const rootName = ["sessions", "archived_sessions"]
      .find((name) => compare(path.basename(rawRoot), name));
    return [rootName, ...nested.split(path.sep).filter(Boolean)];
  } else {
    throw restoreBoundaryError(errorCode, target.kind);
  }
}

async function verifyNonSqliteRestoreTargetBoundary(
  target,
  manifestStorage,
  runtimeStorage,
  { platform = process.platform, errorCode = "RECOVERY_REQUIRED" } = {}
) {
  if (target?.kind === "sqlite") return;
  const { physicalHome } = await verifyRestoreHomePhysicalIdentity(
    manifestStorage,
    runtimeStorage,
    { platform, errorCode, targetKind: target?.kind }
  );
  const segments = await restoreTargetRelativePath(target, physicalHome, platform, errorCode);
  let current = physicalHome;
  for (let index = 0; index < segments.length; index += 1) {
    current = path.join(current, segments[index]);
    let stat;
    try {
      stat = await fs.lstat(current);
    } catch (error) {
      const isLast = index === segments.length - 1;
      if (error?.code === "ENOENT"
          && isLast
          && (target.kind === "config" || target.kind === "globalState")) {
        return;
      }
      throw restoreBoundaryError(errorCode, target.kind, error);
    }
    if (stat.isSymbolicLink()) {
      throw restoreBoundaryError(errorCode, target.kind);
    }
    const isLast = index === segments.length - 1;
    if ((!isLast && !stat.isDirectory()) || (isLast && !stat.isFile())) {
      throw restoreBoundaryError(errorCode, target.kind);
    }
  }
}

async function verifyRestoreHomePhysicalIdentity(
  manifestStorage,
  runtimeStorage,
  { platform = process.platform, errorCode = "RECOVERY_REQUIRED", targetKind = null } = {}
) {
  if (!manifestStorage
      || typeof manifestStorage.codexHome !== "string"
      || typeof manifestStorage.codexHomePhysical !== "string"
      || !path.isAbsolute(manifestStorage.codexHome)
      || !path.isAbsolute(manifestStorage.codexHomePhysical)
      || typeof runtimeStorage?.codexHome !== "string") {
    throw restoreBoundaryError(errorCode, targetKind);
  }
  const lexicalHome = path.resolve(manifestStorage.codexHome);
  const recordedPhysicalHome = path.resolve(manifestStorage.codexHomePhysical);
  const [manifestPhysicalHome, runtimePhysicalHome] = await Promise.all([
    resolveStablePhysicalDirectory(lexicalHome, platform, errorCode, targetKind),
    resolveStablePhysicalDirectory(runtimeStorage.codexHome, platform, errorCode, targetKind)
  ]);
  if (pathKey(manifestPhysicalHome, platform) !== pathKey(recordedPhysicalHome, platform)
      || pathKey(runtimePhysicalHome, platform) !== pathKey(recordedPhysicalHome, platform)) {
    throw restoreBoundaryError(errorCode, targetKind);
  }
  return { lexicalHome, physicalHome: manifestPhysicalHome };
}

function emitRestoreProgress(onProgress, event) {
  if (typeof onProgress !== "function") return;
  try {
    const result = onProgress(event);
    if (result && typeof result.then === "function") result.catch(() => {});
  } catch {
    // Progress is observational and cannot alter Restore transaction state.
  }
}

function targetId(kind, targetPath, platform = process.platform) {
  return sha256Revision(`${kind}\0${pathKey(targetPath, platform)}`);
}

async function exists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch (error) {
    if (error?.code === "ENOENT") return false;
    throw error;
  }
}

async function digestFile(filePath) {
  const fullPath = path.resolve(filePath);
  for (let attempt = 0; attempt < 2; attempt += 1) {
    let before;
    try {
      before = await fs.stat(fullPath, { bigint: true });
    } catch (error) {
      if (error?.code === "ENOENT") {
        return { present: false, digestKind: "absent", digest: sha256Revision("absent") };
      }
      throw error;
    }
    if (!before.isFile()) {
      throw new CoreError("RESTORE_VALIDATION_FAILED", "A Restore target is not a regular file.");
    }
    const hash = createHash("sha256");
    await new Promise((resolve, reject) => {
      const stream = createReadStream(fullPath);
      stream.on("data", (chunk) => hash.update(chunk));
      stream.on("end", resolve);
      stream.on("error", reject);
    });
    const after = await fs.stat(fullPath, { bigint: true }).catch((error) => {
      if (error?.code === "ENOENT") return null;
      throw error;
    });
    if (after
        && before.size === after.size
        && before.mtimeNs === after.mtimeNs
        && before.ctimeNs === after.ctimeNs) {
      return {
        present: true,
        digestKind: "sha256-file",
        digest: hash.digest("base64url"),
        sizeBytes: Number(after.size)
      };
    }
  }
  throw new CoreError("STALE_STATE", "A Restore target changed while its digest was captured.", {
    details: { reason: "restore-target" }
  });
}

async function listIdentityFiles(rootDir, currentDir = rootDir) {
  const entries = await fs.readdir(currentDir, { withFileTypes: true });
  entries.sort((left, right) => compareOrdinal(left.name, right.name));
  const files = [];
  for (const entry of entries) {
    const fullPath = path.join(currentDir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await listIdentityFiles(rootDir, fullPath));
    } else if (entry.isFile()) {
      const digest = await digestFile(fullPath);
      files.push({
        path: path.relative(rootDir, fullPath).split(path.sep).join("/"),
        sha256: digest.digest
      });
    } else {
      throw new CoreError(
        "RESTORE_VALIDATION_FAILED",
        "A managed Restore source contains an unsupported linked entry."
      );
    }
  }
  return files;
}

// Cross-runtime Restore identity: relative POSIX-style path ordering plus
// SHA-256 of each file's bytes. Timestamps and platform file IDs are excluded
// so Node and .NET can produce exactly the same durable source identity.
export async function captureRestoreSourceIdentity(backupDir) {
  const root = path.resolve(backupDir);
  const files = await listIdentityFiles(root);
  files.sort((left, right) => compareOrdinal(left.path, right.path));
  return sha256Revision(stableStringify(files));
}

function digestSessionEntry(entry) {
  return {
    present: true,
    digestKind: "sha256-rollout-metadata",
    digest: sha256Revision(stableStringify({
      originalFirstLine: entry.originalFirstLine,
      originalSeparator: entry.originalSeparator ?? "\n",
      originalTurnContextModels: entry.originalTurnContextModels ?? []
    }))
  };
}

async function digestRolloutTarget(targetPath) {
  const [entry] = await captureSessionRestoreEntries([targetPath]);
  return digestSessionEntry(entry);
}

async function digestSqliteTarget(sqlitePath, scratchDir) {
  if (!await exists(sqlitePath)) {
    return { present: false, digestKind: "absent", digest: sha256Revision("absent") };
  }
  const scratchPath = path.join(scratchDir, `.sqlite-digest-${randomUUID()}.sqlite`);
  try {
    const backup = await createSqliteOnlineBackup({
      stateDbLocation: { path: path.resolve(sqlitePath), source: "restore-v2-digest" }
    }, scratchPath);
    if (!backup.databasePresent) {
      throw new CoreError("STALE_STATE", "The State DB disappeared while its Restore digest was captured.", {
        details: { reason: "state-db" }
      });
    }
    const bytes = await fs.readFile(scratchPath);
    if (bytes.length < 100 || bytes.subarray(0, 16).toString("binary") !== "SQLite format 3\0") {
      throw new CoreError("RESTORE_VALIDATION_FAILED", "Restore SQLite digest source has an invalid header.");
    }
    // Preserve the destination's rollback/WAL mode while comparing logical
    // DB content, and canonicalize the paired volatile change counters.
    bytes.fill(0, 18, 20);
    bytes.fill(0, 24, 28);
    bytes.fill(0, 92, 96);
    bytes.fill(0, 96, 100);
    return {
      present: true,
      digestKind: "sha256-sqlite-online-backup",
      digest: createHash("sha256").update(bytes).digest("base64url"),
      sizeBytes: bytes.length
    };
  } finally {
    await Promise.all([
      scratchPath,
      `${scratchPath}-wal`,
      `${scratchPath}-shm`
    ].map((value) => fs.rm(value, { force: true }).catch(() => {})));
  }
}

async function digestTarget(target, scratchDir) {
  if (target.kind === "rollout") return digestRolloutTarget(target.targetPath);
  if (target.kind === "sqlite") return digestSqliteTarget(target.targetPath, scratchDir);
  return digestFile(target.targetPath);
}

function sameDigest(left, right) {
  return Boolean(left && right)
    && left.present === right.present
    && left.digestKind === right.digestKind
    && left.digest === right.digest;
}

async function expectedPostDigest(target, pre, scratchDir) {
  if (target.kind === "rollout") return digestSessionEntry(target.sourceEntry);
  if (target.kind === "sqlite") return digestSqliteTarget(target.sourcePath, scratchDir);
  if (target.kind === "globalState" && target.sourceAction === "delete") {
    return { present: false, digestKind: "absent", digest: sha256Revision("absent") };
  }
  if (target.kind === "globalState" && target.sourceAction === "preserve") {
    return pre;
  }
  return digestFile(target.sourcePath);
}

function snapshotRelativePath(target) {
  if (target.kind === "config") return "config.toml";
  if (target.kind === "globalState") return path.basename(target.targetPath);
  if (target.kind === "sqlite") return path.join("db", "sqlite-home", DB_FILE_BASENAME);
  return null;
}

async function captureFileSnapshot(target, snapshotDir, pre) {
  const relativePath = snapshotRelativePath(target);
  if (!relativePath || !pre.present) return null;
  const destinationPath = path.join(snapshotDir, relativePath);
  await copyFileAtomic(target.targetPath, destinationPath);
  const copied = await digestFile(destinationPath);
  if (!sameDigest(pre, copied)) {
    throw new CoreError("BACKUP_FAILED", "A Restore pre-snapshot file did not match its source digest.");
  }
  return relativePath.split(path.sep).join("/");
}

function throwIfAborted(signal) {
  if (!signal?.aborted) return;
  const error = new Error("The Restore operation was cancelled.");
  error.name = "AbortError";
  error.code = "ABORT_ERR";
  throw error;
}

async function createPreRestoreSnapshot({
  operationId,
  storage,
  sourceBackup,
  sourcePlan,
  stateDbResource,
  resolvesOperationIds,
  faultInjector,
  signal,
  platform = process.platform
}) {
  throwIfAborted(signal);
  const codexHomePhysical = await resolveStablePhysicalDirectory(
    storage.codexHome,
    platform,
    "LOCK_UNVERIFIABLE"
  );
  const boundaryStorage = {
    codexHome: path.resolve(storage.codexHome),
    codexHomePhysical
  };
  const backupRoot = defaultBackupRoot(storage.codexHome);
  await fs.mkdir(backupRoot, { recursive: true });
  const backupId = `restore-v2-${operationId}`;
  const snapshotDir = path.join(backupRoot, backupId);
  await fs.mkdir(snapshotDir, { recursive: false });
  try {
    const rolloutEntries = [];
    const targets = [];
    for (const sourceTarget of sourcePlan.targets) {
      throwIfAborted(signal);
      await verifyNonSqliteRestoreTargetBoundary(
        sourceTarget,
        boundaryStorage,
        storage,
        { platform, errorCode: "LOCK_UNVERIFIABLE" }
      );
      const id = targetId(sourceTarget.kind, sourceTarget.targetPath, platform);
      let pre;
      let snapshotPath = null;
      let snapshotEntryIndex = null;
      if (sourceTarget.kind === "rollout") {
        const [entry] = await captureSessionRestoreEntries([sourceTarget.targetPath]);
        pre = digestSessionEntry(entry);
        snapshotEntryIndex = rolloutEntries.length;
        rolloutEntries.push(entry);
      } else if (sourceTarget.kind === "sqlite") {
        try {
          pre = await digestSqliteTarget(sourceTarget.targetPath, snapshotDir);
        } catch (error) {
          throw new Error(`Restore pre-target SQLite digest failed: ${error instanceof Error ? error.message : String(error)}`, {
            cause: error instanceof Error ? error : undefined
          });
        }
        if (pre.present) {
          snapshotPath = snapshotRelativePath(sourceTarget);
          const destinationPath = path.join(snapshotDir, snapshotPath);
          let backup;
          try {
            backup = await createSqliteOnlineBackup({
              stateDbLocation: {
                path: path.resolve(sourceTarget.targetPath),
                source: "restore-v2-pre-snapshot"
              }
            }, destinationPath);
          } catch (error) {
            throw new Error(`Restore SQLite pre-snapshot copy failed: ${error instanceof Error ? error.message : String(error)}`, {
              cause: error instanceof Error ? error : undefined
            });
          }
          if (!backup.databasePresent) {
            throw new CoreError("BACKUP_FAILED", "The State DB disappeared during the Restore pre-snapshot.");
          }
          let copied;
          try {
            copied = await digestSqliteTarget(destinationPath, snapshotDir);
          } catch (error) {
            throw new Error(`Restore SQLite pre-snapshot verification failed: ${error instanceof Error ? error.message : String(error)}`, {
              cause: error instanceof Error ? error : undefined
            });
          }
          if (!sameDigest(pre, copied)) {
            throw new CoreError("BACKUP_FAILED", "The Restore pre-snapshot SQLite digest did not verify.");
          }
        }
      } else {
        pre = await digestFile(sourceTarget.targetPath);
        snapshotPath = await captureFileSnapshot(sourceTarget, snapshotDir, pre);
      }
      await faultInjector?.({
        point: "after_restore_pre_snapshot_target_before_hash",
        targetKind: sourceTarget.kind,
        targetId: id
      });
      let expectedPost;
      try {
        expectedPost = await expectedPostDigest(sourceTarget, pre, snapshotDir);
      } catch (error) {
        throw new Error(
          `Restore expected-post digest failed for ${sourceTarget.kind}: ${error instanceof Error ? error.message : String(error)}`,
          { cause: error instanceof Error ? error : undefined }
        );
      }
      targets.push({
        id,
        kind: sourceTarget.kind,
        targetPath: path.resolve(sourceTarget.targetPath),
        pre,
        expectedPost,
        snapshotPath,
        snapshotEntryIndex
      });
    }

    const createdAt = new Date().toISOString();
    const sessionManifest = {
      version: 2,
      namespace: BACKUP_NAMESPACE,
      backupKind: "restore-pre-snapshot",
      codexHome: storage.codexHome,
      targetProvider: sourcePlan.metadata?.targetProvider ?? null,
      createdAt,
      appliedPaths: rolloutEntries.map((entry) => entry.path),
      files: rolloutEntries
    };
    await writeFileAtomic(
      path.join(snapshotDir, "session-meta-backup.json"),
      JSON.stringify(sessionManifest, null, 2),
      "utf8"
    );

    const globalStateFiles = {};
    for (const fileName of [GLOBAL_STATE_FILE_BASENAME, GLOBAL_STATE_BACKUP_FILE_BASENAME]) {
      globalStateFiles[fileName] = await exists(path.join(snapshotDir, fileName));
    }
    const sqliteTarget = targets.find((target) => target.kind === "sqlite") ?? null;
    const manifest = {
      schemaVersion: 2,
      protocolVersion: 2,
      operationKind: "restore",
      operationId,
      createdAt,
      sourceBackup,
      preRestoreSnapshot: { backupId, backupDir: path.resolve(snapshotDir) },
      storage: {
        codexHome: path.resolve(storage.codexHome),
        codexHomePhysical,
        sqliteHome: path.resolve(storage.sqliteHome),
        stateDbResourceKey: stateDbResource?.resourceKey ?? null,
        targetStateDbPath: sqliteTarget?.targetPath ?? null
      },
      requiredTargetKinds: [...new Set(targets.map((target) => target.kind))].sort(),
      resolvesOperationIds: [...new Set(resolvesOperationIds ?? [])].sort(),
      targets
    };
    const manifestText = `${JSON.stringify(manifest, null, 2)}\n`;
    const manifestSha256 = createHash("sha256").update(manifestText, "utf8").digest("base64url");
    await writeFileAtomic(
      path.join(snapshotDir, RESTORE_SNAPSHOT_MANIFEST_BASENAME),
      manifestText,
      "utf8"
    );
    await faultInjector?.({ point: "after_restore_pre_snapshot_manifest_before_prepared" });

    const metadata = {
      version: 2,
      namespace: BACKUP_NAMESPACE,
      backupKind: "restore-pre-snapshot",
      restoreOperationId: operationId,
      codexHome: storage.codexHome,
      sqliteHome: path.dirname(sqliteTarget?.targetPath ?? storage.sqliteHome),
      targetProvider: sourcePlan.metadata?.targetProvider ?? null,
      createdAt,
      dbFiles: [],
      sqliteDbFiles: sqliteTarget?.pre.present ? [DB_FILE_BASENAME] : [],
      stateDbPresent: Boolean(sqliteTarget?.pre.present),
      configPresent: targets.find((target) => target.kind === "config")?.pre.present ?? false,
      globalStateFiles,
      changedSessionFiles: rolloutEntries.length,
      restoreSnapshotManifestSha256: manifestSha256
    };
    await writeFileAtomic(
      path.join(snapshotDir, "metadata.json"),
      JSON.stringify(metadata, null, 2),
      "utf8"
    );
    const revision = manifestSha256;
    return {
      backupId,
      backupDir: path.resolve(snapshotDir),
      revision,
      manifestSha256,
      manifest,
      metadata
    };
  } catch (error) {
    await fs.rm(snapshotDir, { recursive: true, force: true }).catch(() => {});
    if (error instanceof CoreError || error?.name === "AbortError") throw error;
    throw new CoreError("BACKUP_FAILED", "Unable to create the Restore pre-snapshot.", {
      cause: error instanceof Error ? error : undefined
    });
  }
}

function sameStructuredValue(left, right) {
  return stableStringify(left) === stableStringify(right);
}

function manifestMatchesPrepared(manifest, prepared) {
  return sameStructuredValue(manifest.sourceBackup, prepared.sourceBackup)
    && sameStructuredValue(manifest.storage, prepared.storage)
    && sameStructuredValue(manifest.requiredTargetKinds, prepared.requiredTargetKinds)
    && sameStructuredValue(manifest.resolvesOperationIds, prepared.resolvesOperationIds)
    && sameStructuredValue(manifest.targets, prepared.targets);
}

async function readVerifiedSnapshot(journalSnapshot) {
  const prepared = journalSnapshot?.prepared;
  if (!prepared
      || typeof journalSnapshot?.snapshotDir !== "string"
      || typeof prepared.preRestoreSnapshot?.backupDir !== "string") {
    throw new CoreError("RECOVERY_REQUIRED", "Restore snapshot directory does not match its journal.");
  }
  const [journalSnapshotPhysical, preparedSnapshotPhysical] = await Promise.all([
    resolveStablePhysicalDirectory(
      journalSnapshot.snapshotDir,
      process.platform,
      "RECOVERY_REQUIRED"
    ),
    resolveStablePhysicalDirectory(
      prepared.preRestoreSnapshot.backupDir,
      process.platform,
      "RECOVERY_REQUIRED"
    )
  ]);
  if (pathKey(journalSnapshotPhysical) !== pathKey(preparedSnapshotPhysical)) {
    throw new CoreError("RECOVERY_REQUIRED", "Restore snapshot directory does not match its journal.");
  }
  const manifestPath = path.join(
    prepared.preRestoreSnapshot.backupDir,
    RESTORE_SNAPSHOT_MANIFEST_BASENAME
  );
  const text = await fs.readFile(manifestPath, "utf8");
  const digest = createHash("sha256").update(text, "utf8").digest("base64url");
  if (digest !== prepared.preRestoreSnapshot.manifestSha256) {
    throw new CoreError("RECOVERY_REQUIRED", "Restore snapshot manifest verification failed.");
  }
  let manifest;
  try {
    manifest = JSON.parse(text);
  } catch {
    throw new CoreError("RECOVERY_REQUIRED", "Restore snapshot manifest is invalid.");
  }
  let manifestSnapshotPhysical = null;
  if (typeof manifest?.preRestoreSnapshot?.backupDir === "string"
      && path.isAbsolute(manifest.preRestoreSnapshot.backupDir)) {
    manifestSnapshotPhysical = await resolveStablePhysicalDirectory(
      manifest.preRestoreSnapshot.backupDir,
      process.platform,
      "RECOVERY_REQUIRED"
    );
  }
  if (manifest?.schemaVersion !== 2
      || manifest?.protocolVersion !== 2
      || manifest?.operationKind !== "restore"
      || manifest?.operationId !== prepared.operationId
      || manifest?.preRestoreSnapshot?.backupId !== prepared.preRestoreSnapshot.backupId
      || manifestSnapshotPhysical === null
      || pathKey(manifestSnapshotPhysical) !== pathKey(journalSnapshotPhysical)
      || !manifestMatchesPrepared(manifest, prepared)) {
    throw new CoreError("RECOVERY_REQUIRED", "Restore snapshot identity verification failed.");
  }
  return manifest;
}

async function restoreTargetFromSnapshot(target, manifest, storage) {
  const snapshotDir = manifest.preRestoreSnapshot.backupDir;
  if (target.kind === "rollout") {
    const sessionManifest = JSON.parse(
      await fs.readFile(path.join(snapshotDir, "session-meta-backup.json"), "utf8")
    );
    const entry = sessionManifest.files?.[target.snapshotEntryIndex];
    if (!entry || pathKey(entry.path) !== pathKey(target.targetPath)) {
      throw new Error("Restore snapshot rollout entry is missing or mismatched.");
    }
    await restoreSessionChanges([entry]);
    return;
  }
  if (target.kind === "sqlite") {
    if (target.pre.present) {
      await restoreSqliteOnlineBackup(
        path.join(snapshotDir, target.snapshotPath),
        target.targetPath
      );
    } else {
      const sidecars = [`${target.targetPath}-wal`, `${target.targetPath}-shm`];
      if ((await Promise.all(sidecars.map(exists))).some(Boolean)) {
        throw new Error("Cannot remove a newly created State DB while SQLite sidecars are present.");
      }
      await fs.rm(target.targetPath, { force: true });
    }
    return;
  }
  if (target.pre.present) {
    await copyFileAtomic(path.join(snapshotDir, target.snapshotPath), target.targetPath);
  } else {
    await fs.rm(target.targetPath, { force: true });
  }
}

async function verifyManifestTargets(
  manifest,
  which,
  scratchDir,
  { storage = manifest.storage, platform = process.platform } = {}
) {
  const values = [];
  for (const target of manifest.targets) {
    await verifyNonSqliteRestoreTargetBoundary(
      target,
      manifest.storage,
      storage,
      { platform, errorCode: "RECOVERY_REQUIRED" }
    );
    const actual = await digestTarget(target, scratchDir);
    const expected = target[which];
    if (!sameDigest(actual, expected)) {
      throw new CoreError("RECOVERY_REQUIRED", "A Restore target digest does not match durable evidence.", {
        details: { targetKind: target.kind }
      });
    }
    values.push({ id: target.id, digest: actual.digest });
  }
  values.sort((left, right) => compareOrdinal(left.id, right.id));
  return {
    values,
    manifestSha256: sha256Revision(stableStringify(values))
  };
}

async function compensateRestore({
  journalSnapshot,
  journal,
  storage,
  stateDbResource,
  resolveStateDbResource,
  platform,
  faultInjector,
  mutateTargets = true,
  onProgress
}) {
  const manifest = await readVerifiedSnapshot(journalSnapshot);
  const currentStateDbKey = journalSnapshot.prepared.storage.stateDbResourceKey ?? null;
  if ((manifest.storage.stateDbResourceKey ?? null) !== currentStateDbKey) {
    throw new Error("Restore snapshot State DB identity is inconsistent with its journal.");
  }
  await verifyRestoreHomePhysicalIdentity(manifest.storage, storage, {
    platform,
    errorCode: "RECOVERY_REQUIRED"
  });
  emitRestoreProgress(onProgress, {
    stage: "rollback_restore",
    status: "start",
    count: manifest.targets.length
  });
  let compensatedCount = 0;
  for (const target of [...manifest.targets].reverse()) {
    if (target.kind === "sqlite") {
      if (!stateDbResource || !currentStateDbKey) {
        throw new Error("Restore compensation has no verified State DB lock identity.");
      }
      const currentResource = await resolveStateDbResource(target.targetPath, { platform });
      if (currentResource.resourceKey !== currentStateDbKey
          || currentResource.resourceKey !== stateDbResource.resourceKey) {
        throw new Error("Restore State DB physical identity changed before compensation.");
      }
    }
    await faultInjector?.({
      point: "after_restore_rollback_pending_before_target",
      targetKind: target.kind,
      targetId: target.id
    });
    await verifyNonSqliteRestoreTargetBoundary(
      target,
      manifest.storage,
      storage,
      { platform, errorCode: "RECOVERY_REQUIRED" }
    );
    if (mutateTargets) {
      await restoreTargetFromSnapshot(target, manifest, storage);
    }
    const actual = await digestTarget(target, manifest.preRestoreSnapshot.backupDir);
    if (!sameDigest(actual, target.pre)) {
      throw new Error(`Restore compensation digest failed for ${target.kind}.`);
    }
    await journal.targetCompensated(target.id, actual.digest);
    compensatedCount += 1;
    emitRestoreProgress(onProgress, {
      stage: "rollback_restore",
      status: "progress",
      progress: compensatedCount / manifest.targets.length,
      count: compensatedCount
    });
    await faultInjector?.({
      point: "after_restore_compensation_verify_before_next",
      targetKind: target.kind,
      targetId: target.id
    });
  }
  await verifyManifestTargets(manifest, "pre", manifest.preRestoreSnapshot.backupDir, {
    storage,
    platform
  });
  emitRestoreProgress(onProgress, {
    stage: "rollback_restore",
    status: "complete",
    progress: 1,
    count: compensatedCount
  });
}

async function acknowledgeCommittedRestore(journalSnapshot, {
  faultInjector,
  stateDbResource,
  storage = journalSnapshot?.prepared?.storage,
  onProgress,
  resolveStateDbResource = resolveStateDbLockResource,
  platform = process.platform
} = {}) {
  if (journalSnapshot.invalidTail
      || journalSnapshot.state !== "committed-pending-ack"
      || !journalSnapshot.prepared) {
    throw new CoreError("RECOVERY_REQUIRED", "Restore commit acknowledgement evidence is incomplete.");
  }
  const manifest = await readVerifiedSnapshot(journalSnapshot);
  await verifyRestoreHomePhysicalIdentity(manifest.storage, storage, {
    platform,
    errorCode: "RECOVERY_REQUIRED"
  });
  emitRestoreProgress(onProgress, {
    stage: "acknowledge_restore_commit",
    status: "start",
    count: manifest.targets.length
  });
  if (manifest.requiredTargetKinds.includes("sqlite")) {
    const sqliteTargets = manifest.targets.filter((target) => target.kind === "sqlite");
    if (!stateDbResource
        || sqliteTargets.length !== 1
        || manifest.storage.stateDbResourceKey !== stateDbResource.resourceKey) {
      throw new CoreError("RECOVERY_REQUIRED", "Restore State DB identity changed before commit acknowledgement.");
    }
    const currentResource = await resolveStateDbResource(sqliteTargets[0].targetPath, { platform });
    if (currentResource.resourceKey !== stateDbResource.resourceKey
        || currentResource.resourceKey !== manifest.storage.stateDbResourceKey) {
      throw new CoreError("RECOVERY_REQUIRED", "Restore State DB physical identity changed before commit acknowledgement.");
    }
  }
  const verified = await verifyManifestTargets(
    manifest,
    "expectedPost",
    manifest.preRestoreSnapshot.backupDir,
    { storage, platform }
  );
  const committedEvent = [...journalSnapshot.events]
    .reverse()
    .find((event) => event.state === "committed-pending-ack");
  if (!committedEvent || committedEvent.postManifestSha256 !== verified.manifestSha256) {
    throw new CoreError("RECOVERY_REQUIRED", "Restore post-commit manifest acknowledgement failed.");
  }
  const physicalSourceBackupDir = await resolveStablePhysicalDirectory(
    journalSnapshot.prepared.sourceBackup.backupDir,
    platform,
    "RECOVERY_REQUIRED",
    "sourceBackup"
  );
  await markBackupTransactionRolledBack(physicalSourceBackupDir);
  await faultInjector?.({ point: "after_restore_source_journal_ack_before_completed" });
  const journal = reopenRestoreJournal(journalSnapshot);
  await journal.completed();
  const completed = await readRestoreJournal(journal.filePath);
  if (completed.invalidTail || completed.state !== "completed") {
    throw new CoreError("RECOVERY_REQUIRED", "Restore completed acknowledgement did not persist.");
  }
  await refreshBackupInventory(manifest.preRestoreSnapshot.backupDir).catch(() => {});
  emitRestoreProgress(onProgress, {
    stage: "acknowledge_restore_commit",
    status: "complete",
    progress: 1,
    count: manifest.targets.length
  });
  return { completed, manifest };
}

export async function restoreJournalMatchesSource(journal, sourceBackup, platform = process.platform) {
  const prepared = journal?.prepared;
  if (!prepared || !sourceBackup) return false;
  if (prepared.sourceBackup.revision !== sourceBackup.revision) {
    return false;
  }
  try {
    const [preparedPhysical, runtimePhysical] = await Promise.all([
      resolveStablePhysicalDirectory(
        prepared.sourceBackup.backupDir,
        platform,
        "RECOVERY_REQUIRED"
      ),
      resolveStablePhysicalDirectory(
        sourceBackup.backupDir,
        platform,
        "RECOVERY_REQUIRED"
      )
    ]);
    return pathKey(preparedPhysical, platform) === pathKey(runtimePhysical, platform);
  } catch {
    return false;
  }
}

export async function restoreJournalMatchesPhysicalHome(
  journal,
  runtimeCodexHome,
  platform = process.platform
) {
  try {
    await verifyRestoreHomePhysicalIdentity(
      journal?.prepared?.storage,
      { codexHome: runtimeCodexHome },
      { platform, errorCode: "RECOVERY_REQUIRED" }
    );
    return true;
  } catch {
    return false;
  }
}

export function restoreJournalCoverageIsComplete(journal, requestedKinds) {
  const required = journal?.prepared?.requiredTargetKinds;
  if (!Array.isArray(required)) return false;
  const available = new Set(requestedKinds ?? []);
  return required.every((kind) => available.has(kind));
}

export async function acknowledgePendingRestore(journal, options = {}) {
  try {
    return await acknowledgeCommittedRestore(journal, options);
  } catch (error) {
    if (!journal.invalidTail && journal.state === "committed-pending-ack") {
      try {
        const writer = reopenRestoreJournal(journal);
        await writer.recoveryRequired("commit-ack-unverifiable");
      } catch {
        // Preserve the original verification failure and durable evidence.
      }
    }
    throw error;
  }
}

export async function executeRestoreV2({
  storage,
  sourceBackup,
  restoreConfig,
  restoreDatabase,
  restoreSessions,
  allowSqliteHomeRelocation,
  stateDbResource,
  resolvesOperationIds = [],
  faultInjector,
  signal,
  onProgress,
  platform = process.platform,
  resolveStateDbResource = resolveStateDbLockResource
}) {
  const operationId = randomUUID();
  const initialSourceRevision = await captureRestoreSourceIdentity(sourceBackup.backupDir);
  if (initialSourceRevision !== sourceBackup.revision) {
    throw new CoreError("STALE_STATE", "The managed Restore source changed before apply.", {
      details: { reason: "backup" }
    });
  }
  emitRestoreProgress(onProgress, {
    stage: "create_restore_pre_snapshot",
    status: "start",
    count: 0
  });
  throwIfAborted(signal);
  const sourcePlan = await prepareRestoreBackup(sourceBackup.backupDir, storage, {
    restoreConfig,
    restoreDatabase,
    restoreSessions,
    allowSqliteHomeRelocation
  });
  const snapshot = await createPreRestoreSnapshot({
    operationId,
    storage,
    sourceBackup,
    sourcePlan,
    stateDbResource,
    resolvesOperationIds,
    faultInjector,
    signal,
    platform
  });
  const preApplySourceRevision = await captureRestoreSourceIdentity(sourceBackup.backupDir);
  if (preApplySourceRevision !== sourceBackup.revision) {
    await fs.rm(snapshot.backupDir, { recursive: true, force: true }).catch(() => {});
    throw new CoreError("STALE_STATE", "The managed Restore source changed before mutation.", {
      details: { reason: "backup" }
    });
  }
  emitRestoreProgress(onProgress, {
    stage: "create_restore_pre_snapshot",
    status: "complete",
    progress: 1,
    count: snapshot.manifest.targets.length
  });
  try {
    throwIfAborted(signal);
  } catch (error) {
    await fs.rm(snapshot.backupDir, { recursive: true, force: true }).catch(() => {});
    throw error;
  }
  const prepared = {
    operationId,
    sourceBackup,
    preRestoreSnapshot: {
      backupId: snapshot.backupId,
      backupDir: snapshot.backupDir,
      revision: snapshot.revision,
      manifestSha256: snapshot.manifestSha256
    },
    storage: snapshot.manifest.storage,
    requiredTargetKinds: snapshot.manifest.requiredTargetKinds,
    resolvesOperationIds: snapshot.manifest.resolvesOperationIds,
    targets: snapshot.manifest.targets
  };
  let journal;
  try {
    emitRestoreProgress(onProgress, {
      stage: "persist_restore_journal",
      status: "start",
      count: 0
    });
    journal = await RestoreJournal.create(snapshot.backupDir, prepared);
    emitRestoreProgress(onProgress, {
      stage: "persist_restore_journal",
      status: "complete",
      progress: 1,
      count: 1
    });
  } catch (error) {
    await fs.rm(snapshot.backupDir, { recursive: true, force: true }).catch(() => {});
    throw new CoreError("BACKUP_FAILED", "Unable to persist the Restore journal before mutation.", {
      cause: error instanceof Error ? error : undefined
    });
  }

  const targetsByKey = new Map(
    snapshot.manifest.targets.map((target) => [
      `${target.kind}\0${pathKey(target.targetPath, platform)}`,
      target
    ])
  );
  const completed = new Map();
  let applyResult = null;
  try {
    await faultInjector?.({ point: "after_restore_prepared_before_applying" });
    throwIfAborted(signal);
    await journal.applying();
    emitRestoreProgress(onProgress, {
      stage: "apply_restore_targets",
      status: "start",
      count: snapshot.manifest.targets.length
    });
    applyResult = await restoreBackup(sourceBackup.backupDir, storage, {
      restoreConfig,
      restoreDatabase,
      restoreSessions,
      allowSqliteHomeRelocation,
      onBeforeTarget: async (sourceTarget) => {
        throwIfAborted(signal);
        const target = targetsByKey.get(
          `${sourceTarget.kind}\0${pathKey(sourceTarget.targetPath, platform)}`
        );
        if (!target) {
          throw new CoreError("RECOVERY_REQUIRED", "Restore attempted an undeclared target.");
        }
        await verifyNonSqliteRestoreTargetBoundary(
          target,
          snapshot.manifest.storage,
          storage,
          { platform, errorCode: "RECOVERY_REQUIRED" }
        );
        await journal.targetIntent(target.id);
        await faultInjector?.({
          point: "after_restore_target_intent_before_write",
          targetKind: target.kind,
          targetId: target.id
        });
        await verifyNonSqliteRestoreTargetBoundary(
          target,
          snapshot.manifest.storage,
          storage,
          { platform, errorCode: "RECOVERY_REQUIRED" }
        );
      },
      onAfterTarget: async (sourceTarget) => {
        const target = targetsByKey.get(
          `${sourceTarget.kind}\0${pathKey(sourceTarget.targetPath, platform)}`
        );
        await faultInjector?.({
          point: "after_restore_target_write_before_complete",
          targetKind: target.kind,
          targetId: target.id
        });
        await verifyNonSqliteRestoreTargetBoundary(
          target,
          snapshot.manifest.storage,
          storage,
          { platform, errorCode: "RECOVERY_REQUIRED" }
        );
        const actual = await digestTarget(target, snapshot.backupDir);
        if (!sameDigest(actual, target.expectedPost)) {
          throw new CoreError("RECOVERY_REQUIRED", "Restore target post-write digest verification failed.", {
            details: { targetKind: target.kind }
          });
        }
        await journal.targetCompleted(target.id, actual.digest);
        completed.set(target.id, actual.digest);
        emitRestoreProgress(onProgress, {
          stage: "apply_restore_targets",
          status: "progress",
          progress: completed.size / snapshot.manifest.targets.length,
          count: completed.size
        });
        await faultInjector?.({
          point: "after_restore_target_complete",
          targetKind: target.kind,
          targetId: target.id
        });
      }
    });
    throwIfAborted(signal);
    const verified = await verifyManifestTargets(
      snapshot.manifest,
      "expectedPost",
      snapshot.backupDir,
      { storage, platform }
    );
    if (completed.size !== snapshot.manifest.targets.length) {
      throw new CoreError("RECOVERY_REQUIRED", "Restore did not durably complete every declared target.");
    }
    emitRestoreProgress(onProgress, {
      stage: "apply_restore_targets",
      status: "complete",
      progress: 1,
      count: completed.size
    });
    await faultInjector?.({ point: "after_restore_targets_verify_before_committing" });
    emitRestoreProgress(onProgress, {
      stage: "commit_restore",
      status: "start",
      count: snapshot.manifest.targets.length
    });
    await journal.committing(verified.manifestSha256);
    await faultInjector?.({ point: "after_restore_committing_before_committed_pending_ack" });
    await journal.committedPendingAck(verified.manifestSha256);
    emitRestoreProgress(onProgress, {
      stage: "commit_restore",
      status: "complete",
      progress: 1,
      count: snapshot.manifest.targets.length
    });
    await faultInjector?.({ point: "after_restore_committed_pending_ack_before_completed" });
    const current = await readRestoreJournal(journal.filePath);
    const acknowledgement = await acknowledgeCommittedRestore(current, {
      faultInjector,
      stateDbResource,
      storage,
      onProgress,
      resolveStateDbResource,
      platform
    });
    return {
      ...applyResult,
      restoreVersion: 2,
      restoreOperationId: operationId,
      preRestoreSnapshotId: snapshot.backupId,
      restoreJournalState: acknowledgement.completed.state,
      resolvedOperationIds: snapshot.manifest.resolvesOperationIds
    };
  } catch (error) {
    let current;
    try {
      current = await readRestoreJournal(journal.filePath);
    } catch (journalReadError) {
      throw new CoreError("RECOVERY_REQUIRED", "Restore journal cannot be read after an interrupted operation.", {
        cause: journalReadError instanceof Error ? journalReadError : undefined,
        details: {
          operationKind: "restore",
          restoreOperationId: operationId,
          sourceBackupId: sourceBackup.backupId,
          preRestoreSnapshotId: snapshot.backupId
        }
      });
    }
    if (current.state === "completed") {
      return {
        ...applyResult,
        restoreVersion: 2,
        restoreOperationId: operationId,
        preRestoreSnapshotId: snapshot.backupId,
        restoreJournalState: "completed",
        resolvedOperationIds: snapshot.manifest.resolvesOperationIds
      };
    }
    if (current.state === "committed-pending-ack" && !current.invalidTail) {
      try {
        const acknowledgement = await acknowledgeCommittedRestore(current, {
          faultInjector,
          stateDbResource,
          storage,
          onProgress,
          resolveStateDbResource,
          platform
        });
        return {
          ...applyResult,
          restoreVersion: 2,
          restoreOperationId: operationId,
          preRestoreSnapshotId: snapshot.backupId,
          restoreJournalState: acknowledgement.completed.state,
          resolvedOperationIds: snapshot.manifest.resolvesOperationIds,
          commitAcknowledgementRecovered: true
        };
      } catch (ackError) {
        try {
          await reopenRestoreJournal(current).recoveryRequired("commit-ack-unverifiable");
        } catch {
          // The existing committed-pending-ack evidence remains the blocker.
        }
        throw new CoreError("RECOVERY_REQUIRED", "Restore committed, but its final acknowledgement is unverifiable.", {
          cause: ackError instanceof Error ? ackError : undefined,
          details: {
            operationKind: "restore",
            restoreOperationId: operationId,
            sourceBackupId: sourceBackup.backupId,
            preRestoreSnapshotId: snapshot.backupId
          }
        });
      }
    }
    if (current.invalidTail || !current.prepared) {
      throw new CoreError("RECOVERY_REQUIRED", "Restore journal evidence is incomplete; compensation was not attempted.", {
        details: {
          operationKind: "restore",
          restoreOperationId: operationId,
          sourceBackupId: sourceBackup.backupId,
          preRestoreSnapshotId: snapshot.backupId
        }
      });
    }
    const writer = reopenRestoreJournal(current);
    const mutationMayHaveOccurred = [...current.targetPhases.values()]
      .some((phase) => phase === "intent" || phase === "completed");
    try {
      if (current.state !== "rollback-pending") {
        await writer.rollbackPending(error?.code ?? "restore-failed");
      }
      const rollbackSnapshot = await readRestoreJournal(writer.filePath);
      await compensateRestore({
        journalSnapshot: rollbackSnapshot,
        journal: writer,
        storage,
        stateDbResource,
        resolveStateDbResource,
        platform,
        faultInjector,
        mutateTargets: mutationMayHaveOccurred,
        onProgress
      });
      await writer.rolledBack();
      const terminal = await readRestoreJournal(writer.filePath);
      if (terminal.invalidTail || terminal.state !== "rolled-back") {
        throw new Error("Restore rollback terminal state did not persist.");
      }
      if (!mutationMayHaveOccurred) throw error;
      throw new CoreError(
        "SYNC_FAILED_ROLLED_BACK",
        `Restore failed and all observed changes were rolled back. ${error instanceof Error ? error.message : String(error)}`,
        {
          cause: error instanceof Error ? error : undefined,
          details: {
            operationKind: "restore",
            restoreOperationId: operationId,
            sourceBackupId: sourceBackup.backupId,
            preRestoreSnapshotId: snapshot.backupId,
            rollbackStatus: "complete"
          }
        }
      );
    } catch (rollbackError) {
      if (rollbackError === error
          || (rollbackError?.code === "SYNC_FAILED_ROLLED_BACK"
            && rollbackError?.cause === error)) {
        throw rollbackError;
      }
      try {
        const latest = await readRestoreJournal(writer.filePath);
        if (!latest.invalidTail && latest.state !== "recovery-required") {
          await reopenRestoreJournal(latest).recoveryRequired("rollback-unverifiable");
        }
      } catch {
        // Keep all snapshot/journal evidence for explicit recovery.
      }
      throw new CoreError("RECOVERY_REQUIRED", "Restore failed and its compensation could not be verified.", {
        cause: rollbackError instanceof Error ? rollbackError : undefined,
        details: {
          operationKind: "restore",
          restoreOperationId: operationId,
          sourceBackupId: sourceBackup.backupId,
          preRestoreSnapshotId: snapshot.backupId
        }
      });
    }
  }
}

export function protectedRestoreBackupDirectories(journals) {
  const protectedPaths = new Set();
  for (const journal of journals ?? []) {
    if (!journal?.blocking) continue;
    protectedPaths.add(pathKey(journal.snapshotDir));
    if (journal.prepared?.sourceBackup?.backupDir) {
      protectedPaths.add(pathKey(journal.prepared.sourceBackup.backupDir));
    }
    if (journal.prepared?.preRestoreSnapshot?.backupDir) {
      protectedPaths.add(pathKey(journal.prepared.preRestoreSnapshot.backupDir));
    }
  }
  return protectedPaths;
}
