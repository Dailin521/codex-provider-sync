import fs from "node:fs/promises";
import path from "node:path";

import {
  BACKUP_NAMESPACE,
  DB_FILE_BASENAME,
  DEFAULT_BACKUP_RETENTION_COUNT,
  defaultBackupRoot,
  GLOBAL_STATE_BACKUP_FILE_BASENAME,
  GLOBAL_STATE_FILE_BASENAME,
  SQLITE_DIR_BASENAME
} from "./constants.js";
import { assertSessionFilesWritable, restoreSessionChanges } from "./session-files.js";
import { assertSqliteWritable, existingStateDbPath, stateDbPath } from "./sqlite-state.js";

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

function dbBackupRelativePath(codexHome, dbPath, suffix) {
  const relativePath = path.relative(codexHome, `${dbPath}${suffix}`);
  return relativePath && !relativePath.startsWith("..") && !path.isAbsolute(relativePath)
    ? relativePath
    : `${DB_FILE_BASENAME}${suffix}`;
}

function restoreDbTargetPath(codexHome, relativePath) {
  return relativePath.includes("/") || relativePath.includes("\\")
    ? path.join(codexHome, relativePath)
    : path.join(path.dirname(stateDbPath(codexHome)), relativePath);
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
  codexHome,
  targetProvider,
  sessionChanges,
  configPath,
  configBackupText
}) {
  const backupRoot = defaultBackupRoot(codexHome);
  const backupDir = path.join(backupRoot, timestampSlug());
  const dbDir = path.join(backupDir, "db");
  await fs.mkdir(dbDir, { recursive: true });

  const copiedDbFiles = [];
  const dbPath = await existingStateDbPath(codexHome);
  if (dbPath) {
    for (const suffix of ["", "-shm", "-wal"]) {
      const relativePath = dbBackupRelativePath(codexHome, dbPath, suffix);
      const copied = await copyIfPresent(`${dbPath}${suffix}`, path.join(dbDir, relativePath));
      if (copied) {
        copiedDbFiles.push(relativePath);
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
    version: 1,
    namespace: BACKUP_NAMESPACE,
    codexHome,
    targetProvider,
    createdAt: new Date().toISOString(),
    files: sessionChanges.map((change) => ({
      path: change.path,
      originalFirstLine: change.originalFirstLine,
      originalSeparator: change.originalSeparator,
      originalMtimeMs: change.originalMtimeMs
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
        version: 1,
        namespace: BACKUP_NAMESPACE,
        codexHome,
        targetProvider,
        createdAt: sessionManifest.createdAt,
        dbFiles: copiedDbFiles,
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

  sessionManifest.files = sessionChanges.map((change) => ({
    path: change.path,
    originalFirstLine: change.originalFirstLine,
    originalSeparator: change.originalSeparator,
    originalMtimeMs: change.originalMtimeMs
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

export async function restoreBackup(backupDir, codexHome, options = {}) {
  const {
    restoreConfig = true,
    restoreDatabase = true,
    restoreSessions = true
  } = options;
  const metadataPath = path.join(backupDir, "metadata.json");
  const metadata = JSON.parse(await fs.readFile(metadataPath, "utf8"));
  if (metadata.codexHome !== codexHome) {
    throw new Error(`Backup was created for ${metadata.codexHome}, not ${codexHome}.`);
  }

  let sessionManifest = null;
  if (restoreSessions) {
    const sessionManifestPath = path.join(backupDir, "session-meta-backup.json");
    sessionManifest = JSON.parse(await fs.readFile(sessionManifestPath, "utf8"));
    await assertSessionFilesWritable(sessionManifest.files ?? []);
  }

  const configBackupPath = path.join(backupDir, "config.toml");
  if (restoreConfig) {
    await copyIfPresent(configBackupPath, path.join(codexHome, "config.toml"));
    await restoreGlobalStateFilesFromBackup(backupDir, codexHome);
  }

  if (restoreDatabase) {
    await assertSqliteWritable(codexHome);

    const dbDir = path.join(backupDir, "db");
    const backedUpFiles = new Set(metadata.dbFiles ?? []);
    for (const suffix of ["", "-shm", "-wal"]) {
      const fileName = `${DB_FILE_BASENAME}${suffix}`;
      const targetPath = path.join(path.dirname(stateDbPath(codexHome)), fileName);
      if (!backedUpFiles.has(path.join(SQLITE_DIR_BASENAME, fileName)) && !backedUpFiles.has(fileName)) {
        await removeIfPresent(targetPath);
      }
    }
    for (const fileName of metadata.dbFiles ?? []) {
      await copyIfPresent(path.join(dbDir, fileName), restoreDbTargetPath(codexHome, fileName));
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
