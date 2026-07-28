import fs from "node:fs/promises";
import path from "node:path";

import {
  BACKUP_NAMESPACE,
  DB_FILE_BASENAME,
  DEFAULT_BACKUP_RETENTION_COUNT,
  defaultBackupRoot,
  GLOBAL_STATE_BACKUP_FILE_BASENAME,
  GLOBAL_STATE_FILE_BASENAME
} from "./constants.js";
import { assertSessionFilesWritable, restoreSessionChanges } from "./session-files.js";
import { assertSqliteWritable, detectStateDb } from "./sqlite-state.js";
import { resolveStorageLayout, withStateDbLocation } from "./storage-layout.js";

function timestampSlug(date = new Date()) {
  return date.toISOString().replaceAll(":", "").replaceAll("-", "").replace(".", "");
}

async function copyIfPresent(sourcePath, destinationPath) {
  try {
    await fs.access(sourcePath);
  } catch {
    return false;
  }
  await fs.mkdir(path.dirname(destinationPath), { recursive: true });
  await fs.copyFile(sourcePath, destinationPath);
  return true;
}

function restoreDbTargetPath(codexHome, relativePath) {
  if (path.isAbsolute(relativePath) || relativePath.split(/[\\/]/).includes("..")) {
    throw new Error(`Invalid database backup path: ${relativePath}`);
  }
  return path.join(codexHome, relativePath);
}

function safeRelativePath(root, target) {
  const relativePath = path.relative(root, target);
  return relativePath && !relativePath.startsWith("..") && !path.isAbsolute(relativePath)
    ? relativePath
    : null;
}

function restoreSqliteTargetPath(sqliteHome, relativePath) {
  if (path.isAbsolute(relativePath) || relativePath.split(/[\\/]/).includes("..")) {
    throw new Error(`Invalid SQLite backup path: ${relativePath}`);
  }
  return path.join(sqliteHome, relativePath);
}

function storagePathsEqual(left, right) {
  const normalizedLeft = path.resolve(left);
  const normalizedRight = path.resolve(right);
  return process.platform === "win32"
    ? normalizedLeft.toLowerCase() === normalizedRight.toLowerCase()
    : normalizedLeft === normalizedRight;
}

function resolveRestoreSqliteHome(storage, metadata, stateDb) {
  if (stateDb) {
    return path.dirname(stateDb.path);
  }
  if (metadata.version >= 2 && metadata.sqliteHome && storage.sqliteHomeSource === "default") {
    const matchingCandidate = storage.stateDbCandidates.find((candidate) =>
      storagePathsEqual(path.dirname(candidate.path), metadata.sqliteHome)
    );
    if (matchingCandidate) {
      return path.dirname(matchingCandidate.path);
    }
  }
  return storage.sqliteHome;
}

async function removeIfPresent(targetPath) {
  await fs.rm(targetPath, { force: true });
}

async function backupGlobalStateFiles(codexHome, backupDir) {
  for (const fileName of [GLOBAL_STATE_FILE_BASENAME, GLOBAL_STATE_BACKUP_FILE_BASENAME]) {
    await copyIfPresent(path.join(codexHome, fileName), path.join(backupDir, fileName));
  }
}

export async function restoreGlobalStateFilesFromBackup(backupDir, codexHome) {
  for (const fileName of [GLOBAL_STATE_FILE_BASENAME, GLOBAL_STATE_BACKUP_FILE_BASENAME]) {
    await copyIfPresent(path.join(backupDir, fileName), path.join(codexHome, fileName));
  }
}

export async function createBackup({
  storage,
  codexHome,
  targetProvider,
  sessionChanges,
  configPath,
  configBackupText
}) {
  const effectiveStorage = storage ?? resolveStorageLayout({ codexHome, env: {} });
  codexHome = effectiveStorage.codexHome;
  const backupRoot = defaultBackupRoot(codexHome);
  const backupDir = path.join(backupRoot, timestampSlug());
  const dbDir = path.join(backupDir, "db");
  await fs.mkdir(dbDir, { recursive: true });

  const copiedDbFiles = [];
  const copiedSqliteDbFiles = [];
  const stateDb = Object.hasOwn(effectiveStorage, "stateDbLocation")
    ? effectiveStorage.stateDbLocation
    : await detectStateDb(effectiveStorage);
  const actualSqliteHome = stateDb ? path.dirname(stateDb.path) : effectiveStorage.sqliteHome;
  if (stateDb) {
    for (const suffix of ["", "-shm", "-wal"]) {
      const sourcePath = `${stateDb.path}${suffix}`;
      const sqliteRelativePath = `${DB_FILE_BASENAME}${suffix}`;
      const copied = await copyIfPresent(sourcePath, path.join(dbDir, "sqlite-home", sqliteRelativePath));
      if (!copied) {
        continue;
      }
      copiedSqliteDbFiles.push(sqliteRelativePath);

      const legacyRelativePath = safeRelativePath(codexHome, sourcePath);
      if (legacyRelativePath) {
        await copyIfPresent(sourcePath, path.join(dbDir, legacyRelativePath));
        copiedDbFiles.push(legacyRelativePath);
      }
    }
  }

  if (configBackupText !== undefined) {
    await fs.writeFile(path.join(backupDir, "config.toml"), configBackupText, "utf8");
  } else {
    await copyIfPresent(configPath, path.join(backupDir, "config.toml"));
  }
  await backupGlobalStateFiles(codexHome, backupDir);

  const sessionManifest = {
    version: 2,
    namespace: BACKUP_NAMESPACE,
    codexHome,
    targetProvider,
    createdAt: new Date().toISOString(),
    files: sessionChanges.map((change) => ({
      path: change.path,
      originalFirstLine: change.originalFirstLine,
      originalSeparator: change.originalSeparator,
      originalMtimeMs: change.originalMtimeMs,
      // Per-line record of the original turn_context.model values
      // so a failed rollback can put the per-turn model back to
      // what it was before the sync. Without this, a restore
      // would only rewind the session_meta line and leave the
      // per-turn `model` field pointing at the new value, which
      // is exactly the "half-completed state" the owner review
      // called out.
      originalTurnContextModels: change.originalTurnContextModels ?? [],
      modelOnlyChange: Boolean(change.modelOnlyChange)
    }))
  };
  await fs.writeFile(
    path.join(backupDir, "session-meta-backup.json"),
    JSON.stringify(sessionManifest, null, 2),
    "utf8"
  );

  await fs.writeFile(
    path.join(backupDir, "metadata.json"),
    JSON.stringify(
      {
        version: 2,
        namespace: BACKUP_NAMESPACE,
        codexHome,
        sqliteHome: actualSqliteHome,
        targetProvider,
        createdAt: sessionManifest.createdAt,
        dbFiles: copiedDbFiles,
        sqliteDbFiles: copiedSqliteDbFiles,
        changedSessionFiles: sessionChanges.length
      },
      null,
      2
    ),
    "utf8"
  );

  return backupDir;
}

export async function updateSessionBackupManifest(backupDir, sessionChanges) {
  const manifestPath = path.join(backupDir, "session-meta-backup.json");
  const metadataPath = path.join(backupDir, "metadata.json");
  const sessionManifest = JSON.parse(await fs.readFile(manifestPath, "utf8"));
  const metadata = JSON.parse(await fs.readFile(metadataPath, "utf8"));

  // Promote older manifests to the v2 schema so restoreSessionChanges
  // can rely on the per-line `originalTurnContextModels` field.
  if (sessionManifest.version !== 2) {
    sessionManifest.version = 2;
  }

  sessionManifest.files = sessionChanges.map((change) => ({
    path: change.path,
    originalFirstLine: change.originalFirstLine,
    originalSeparator: change.originalSeparator,
    originalMtimeMs: change.originalMtimeMs,
    originalTurnContextModels: change.originalTurnContextModels ?? [],
    modelOnlyChange: Boolean(change.modelOnlyChange)
  }));
  metadata.changedSessionFiles = sessionChanges.length;

  await fs.writeFile(manifestPath, JSON.stringify(sessionManifest, null, 2), "utf8");
  await fs.writeFile(metadataPath, JSON.stringify(metadata, null, 2), "utf8");
}

export async function getBackupSummary(codexHome) {
  const backupRoot = defaultBackupRoot(codexHome);
  const backupDirs = await listManagedBackupDirectories(backupRoot);
  let totalBytes = 0;
  for (const entry of backupDirs) {
    totalBytes += await getDirectorySize(entry.fullPath);
  }

  return {
    count: backupDirs.length,
    totalBytes
  };
}

export async function pruneBackups(codexHome, keepCount = DEFAULT_BACKUP_RETENTION_COUNT) {
  if (!Number.isInteger(keepCount) || keepCount < 0) {
    throw new Error(`Invalid keep count: ${keepCount}. Expected a non-negative integer.`);
  }

  const backupRoot = defaultBackupRoot(codexHome);
  const backupDirs = await listManagedBackupDirectories(backupRoot);
  const toDelete = backupDirs.slice(keepCount);
  let freedBytes = 0;
  for (const entry of toDelete) {
    freedBytes += await getDirectorySize(entry.fullPath);
    await fs.rm(entry.fullPath, { recursive: true, force: true });
  }

  return {
    backupRoot,
    deletedCount: toDelete.length,
    remainingCount: backupDirs.length - toDelete.length,
    freedBytes
  };
}

export async function restoreBackup(backupDir, storageOrCodexHome, options = {}) {
  const {
    restoreConfig = true,
    restoreDatabase = true,
    restoreSessions = true,
    allowSqliteHomeRelocation = false
  } = options;
  const storage = typeof storageOrCodexHome === "string"
    ? resolveStorageLayout({ codexHome: storageOrCodexHome, env: {} })
    : storageOrCodexHome;
  const codexHome = storage.codexHome;
  const metadataPath = path.join(backupDir, "metadata.json");
  const metadata = JSON.parse(await fs.readFile(metadataPath, "utf8"));
  if (metadata.namespace !== BACKUP_NAMESPACE || ![1, 2].includes(metadata.version)) {
    throw new Error(`Unsupported backup metadata in ${metadataPath}.`);
  }
  if (metadata.codexHome !== codexHome) {
    throw new Error(`Backup was created for ${metadata.codexHome}, not ${codexHome}.`);
  }

  let sessionManifest = null;
  if (restoreSessions) {
    const sessionManifestPath = path.join(backupDir, "session-meta-backup.json");
    sessionManifest = JSON.parse(await fs.readFile(sessionManifestPath, "utf8"));
    await assertSessionFilesWritable(sessionManifest.files ?? []);
  }

  let stateDb = null;
  let targetSqliteHome = null;
  let databaseRestorePlan = null;
  if (restoreDatabase) {
    stateDb = Object.hasOwn(storage, "stateDbLocation")
      ? storage.stateDbLocation
      : await detectStateDb(storage);
    if (!stateDb && storage.sqliteHomeSource !== "default") {
      throw new Error(`state_5.sqlite not found in SQLite home ${storage.sqliteHome}.`);
    }
    targetSqliteHome = resolveRestoreSqliteHome(storage, metadata, stateDb);
    const sqliteHomeRelocation = metadata.version >= 2
      && metadata.sqliteHome
      && !storagePathsEqual(metadata.sqliteHome, targetSqliteHome);
    if (sqliteHomeRelocation && !allowSqliteHomeRelocation) {
      throw new Error(
        `Backup SQLite home is ${metadata.sqliteHome}, but the current target is ${targetSqliteHome}. `
        + "Use --allow-sqlite-home-relocation with an explicit --sqlite-home to restore to a different location."
      );
    }
    if (sqliteHomeRelocation && restoreConfig) {
      throw new Error(
        "Cannot restore config.toml while relocating SQLite home. "
        + "Use --no-config to preserve the current target configuration."
      );
    }
    if (stateDb) {
      await assertSqliteWritable(withStateDbLocation(storage, stateDb));
    }

    const dbDir = path.join(backupDir, "db");
    const databaseFiles = metadata.version >= 2
      ? (metadata.sqliteDbFiles ?? [])
      : (metadata.dbFiles ?? []);
    if (!databaseFiles.some((fileName) => path.basename(fileName) === DB_FILE_BASENAME)) {
      throw new Error("Backup does not contain state_5.sqlite. Use --no-db to restore the remaining data.");
    }
    const databaseBackupRoot = metadata.version >= 2
      ? path.join(dbDir, "sqlite-home")
      : dbDir;
    const restoreRoot = metadata.version >= 2 ? targetSqliteHome : codexHome;
    const entries = [];
    for (const fileName of databaseFiles) {
      const targetPath = metadata.version >= 2
        ? restoreSqliteTargetPath(restoreRoot, fileName)
        : restoreDbTargetPath(restoreRoot, fileName);
      const sourcePath = path.join(databaseBackupRoot, fileName);
      await fs.access(sourcePath).catch(() => {
        throw new Error(`Backup declares a missing SQLite file: ${sourcePath}`);
      });
      entries.push({ fileName, sourcePath, targetPath });
    }

    const backedUpFiles = new Set(databaseFiles);
    const sidecarsToRemove = [];
    for (const baseFile of databaseFiles.filter((fileName) => path.basename(fileName) === DB_FILE_BASENAME)) {
      const basePath = metadata.version >= 2
        ? restoreSqliteTargetPath(restoreRoot, baseFile)
        : restoreDbTargetPath(restoreRoot, baseFile);
      for (const suffix of ["-shm", "-wal"]) {
        if (!backedUpFiles.has(`${baseFile}${suffix}`)) {
          sidecarsToRemove.push(`${basePath}${suffix}`);
        }
      }
    }
    databaseRestorePlan = { entries, sidecarsToRemove };
  }

  const configBackupPath = path.join(backupDir, "config.toml");
  if (restoreConfig) {
    await copyIfPresent(configBackupPath, path.join(codexHome, "config.toml"));
    await restoreGlobalStateFilesFromBackup(backupDir, codexHome);
  }

  if (databaseRestorePlan) {
    for (const sidecarPath of databaseRestorePlan.sidecarsToRemove) {
      await removeIfPresent(sidecarPath);
    }
    for (const { sourcePath, targetPath } of databaseRestorePlan.entries) {
      await fs.mkdir(path.dirname(targetPath), { recursive: true });
      await fs.copyFile(sourcePath, targetPath);
    }
  }

  if (restoreSessions) {
    await restoreSessionChanges(sessionManifest.files ?? []);
  }

  return metadata;
}

async function listManagedBackupDirectories(backupRoot) {
  let entries;
  try {
    entries = await fs.readdir(backupRoot, { withFileTypes: true });
  } catch (error) {
    if (error?.code === "ENOENT") {
      return [];
    }
    throw error;
  }

  const directories = entries
    .filter((entry) => entry.isDirectory())
    .map((entry) => ({
      name: entry.name,
      fullPath: path.join(backupRoot, entry.name)
    }));

  const managed = [];
  for (const entry of directories) {
    if (await isManagedBackupDirectory(entry.fullPath)) {
      managed.push(entry);
    }
  }

  return managed.sort((left, right) => right.name.localeCompare(left.name));
}

async function isManagedBackupDirectory(backupDir) {
  const metadataPath = path.join(backupDir, "metadata.json");
  try {
    const metadata = JSON.parse(await fs.readFile(metadataPath, "utf8"));
    return metadata?.namespace === BACKUP_NAMESPACE;
  } catch (error) {
    if (error?.code === "ENOENT") {
      return false;
    }
    return false;
  }
}

async function getDirectorySize(directoryPath) {
  let entries;
  try {
    entries = await fs.readdir(directoryPath, { withFileTypes: true });
  } catch (error) {
    if (error?.code === "ENOENT") {
      return 0;
    }
    throw error;
  }

  let total = 0;
  for (const entry of entries) {
    const fullPath = path.join(directoryPath, entry.name);
    if (entry.isDirectory()) {
      total += await getDirectorySize(fullPath);
      continue;
    }
    if (entry.isFile()) {
      const stat = await fs.stat(fullPath);
      total += stat.size;
    }
  }

  return total;
}
