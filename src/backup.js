import fs from "node:fs/promises";
import path from "node:path";

import {
  BACKUP_NAMESPACE,
  DB_FILE_BASENAME,
  DEFAULT_BACKUP_RETENTION_COUNT,
  defaultBackupRoot
} from "./constants.js";
import { assertSessionFilesWritable, restoreSessionChanges } from "./session-files.js";
import { assertSqliteWritable } from "./sqlite-state.js";

function timestampSlug(date = new Date()) {
  return date.toISOString().replaceAll(":", "").replaceAll("-", "").replace(".", "");
}

async function copyIfPresent(sourcePath, destinationPath) {
  try {
    await fs.access(sourcePath);
  } catch {
    return false;
  }
  await fs.copyFile(sourcePath, destinationPath);
  return true;
}

async function removeIfPresent(targetPath) {
  await fs.rm(targetPath, { force: true });
}

export async function createBackup({
  codexHome,
  targetProvider,
  sessionChanges,
  deletedSessionFiles = [],
  configPath,
  configBackupText
}) {
  const backupRoot = defaultBackupRoot(codexHome);
  const backupDir = path.join(backupRoot, timestampSlug());
  const dbDir = path.join(backupDir, "db");
  await fs.mkdir(dbDir, { recursive: true });

  const copiedDbFiles = [];
  for (const suffix of ["", "-shm", "-wal"]) {
    const fileName = `${DB_FILE_BASENAME}${suffix}`;
    const copied = await copyIfPresent(path.join(codexHome, fileName), path.join(dbDir, fileName));
    if (copied) {
      copiedDbFiles.push(fileName);
    }
  }

  if (configBackupText !== undefined) {
    await fs.writeFile(path.join(backupDir, "config.toml"), configBackupText, "utf8");
  } else {
    await copyIfPresent(configPath, path.join(backupDir, "config.toml"));
  }

  const deletedFilesDir = path.join(backupDir, "deleted-session-files");
  await fs.mkdir(deletedFilesDir, { recursive: true });
  const deletedFilesManifest = [];
  let deletedFileCounter = 0;
  for (const rawFilePath of deletedSessionFiles ?? []) {
    const absolutePath = path.resolve(rawFilePath);
    const relativePath = path.relative(codexHome, absolutePath);
    if (!relativePath || relativePath.startsWith("..") || path.isAbsolute(relativePath)) {
      continue;
    }

    const backupFileName = `${String(deletedFileCounter).padStart(4, "0")}${path.extname(absolutePath) || ".jsonl"}`;
    const backupRelativePath = path.join("deleted-session-files", backupFileName);
    const copied = await copyIfPresent(absolutePath, path.join(backupDir, backupRelativePath));
    if (!copied) {
      continue;
    }

    deletedFilesManifest.push({
      path: absolutePath,
      relativePath,
      backupRelativePath
    });
    deletedFileCounter += 1;
  }

  const sessionManifest = {
    version: 1,
    namespace: BACKUP_NAMESPACE,
    codexHome,
    targetProvider,
    createdAt: new Date().toISOString(),
    files: sessionChanges.map((change) => ({
      path: change.path,
      originalFirstLine: change.originalFirstLine,
      originalSeparator: change.originalSeparator
    })),
    deletedFiles: deletedFilesManifest
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
        changedSessionFiles: sessionChanges.length,
        deletedSessionFiles: deletedFilesManifest.length
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
    originalSeparator: change.originalSeparator
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
    restoreSessions = true,
    restoreDeletedSessionFiles = true
  } = options;
  const metadataPath = path.join(backupDir, "metadata.json");
  const metadata = JSON.parse(await fs.readFile(metadataPath, "utf8"));
  if (metadata.codexHome !== codexHome) {
    throw new Error(`Backup was created for ${metadata.codexHome}, not ${codexHome}.`);
  }

  const needsSessionManifest = restoreSessions || restoreDeletedSessionFiles;
  let sessionManifest = null;
  if (needsSessionManifest) {
    const sessionManifestPath = path.join(backupDir, "session-meta-backup.json");
    sessionManifest = JSON.parse(await fs.readFile(sessionManifestPath, "utf8"));
  }

  if (restoreSessions) {
    await assertSessionFilesWritable(sessionManifest.files ?? []);
  }

  const configBackupPath = path.join(backupDir, "config.toml");
  if (restoreConfig) {
    await copyIfPresent(configBackupPath, path.join(codexHome, "config.toml"));
  }

  if (restoreDatabase) {
    await assertSqliteWritable(codexHome);

    const dbDir = path.join(backupDir, "db");
    const backedUpFiles = new Set(metadata.dbFiles ?? []);
    for (const suffix of ["", "-shm", "-wal"]) {
      const fileName = `${DB_FILE_BASENAME}${suffix}`;
      if (!backedUpFiles.has(fileName)) {
        await removeIfPresent(path.join(codexHome, fileName));
      }
    }
    for (const fileName of metadata.dbFiles ?? []) {
      await copyIfPresent(path.join(dbDir, fileName), path.join(codexHome, fileName));
    }
  }

  if (restoreSessions) {
    await restoreSessionChanges(sessionManifest.files ?? []);
  }

  if (restoreDeletedSessionFiles) {
    await restoreDeletedSessionFilesFromBackup(backupDir, codexHome, sessionManifest?.deletedFiles ?? []);
  }

  return metadata;
}

async function restoreDeletedSessionFilesFromBackup(backupDir, codexHome, deletedFiles) {
  for (const entry of deletedFiles ?? []) {
    const relativePath = String(entry?.relativePath ?? "").trim();
    const backupRelativePath = String(entry?.backupRelativePath ?? "").trim();
    if (!relativePath || !backupRelativePath) {
      continue;
    }

    const sourcePath = path.join(backupDir, backupRelativePath);
    const destinationPath = path.join(codexHome, relativePath);
    await fs.mkdir(path.dirname(destinationPath), { recursive: true });
    await copyIfPresent(sourcePath, destinationPath);
  }
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
