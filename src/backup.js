import fs from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";

import {
  BACKUP_NAMESPACE,
  DB_FILE_BASENAME,
  DEFAULT_BACKUP_RETENTION_COUNT,
  defaultBackupRoot,
  GLOBAL_STATE_BACKUP_FILE_BASENAME,
  GLOBAL_STATE_FILE_BASENAME
} from "./constants.js";
import { restoreSessionChanges } from "./session-files.js";
import {
  assertSqliteWritable,
  createSqliteOnlineBackup,
  detectStateDb,
  restoreSqliteOnlineBackup
} from "./sqlite-state.js";
import {
  assertSqliteAccessSupported,
  resolveStorageLayout,
  withStateDbLocation
} from "./storage-layout.js";
import {
  TRANSACTION_JOURNAL_BASENAME,
  findPendingTransactions,
  getStartedJournalTargets,
  readTransactionJournal
} from "./transaction-journal.js";
import { syncDirectory, writeFileAtomic } from "./atomic-file.js";

function timestampSlug(date = new Date()) {
  return date.toISOString().replaceAll(":", "").replaceAll("-", "").replace(".", "");
}

async function copyIfPresent(sourcePath, destinationPath) {
  try {
    await fs.access(sourcePath);
  } catch (error) {
    if (error?.code === "ENOENT") {
      return false;
    }
    throw error;
  }
  await copyFileAtomic(sourcePath, destinationPath);
  return true;
}

async function copyFileAtomic(sourcePath, destinationPath) {
  const fullDestination = path.resolve(destinationPath);
  const directory = path.dirname(fullDestination);
  const tempPath = path.join(
    directory,
    `.${path.basename(fullDestination)}.provider-sync.${process.pid}.${randomUUID()}.tmp`
  );
  await fs.mkdir(directory, { recursive: true });
  try {
    const sourceStat = await fs.stat(sourcePath);
    await fs.copyFile(sourcePath, tempPath);
    await fs.chmod(tempPath, sourceStat.mode);
    const handle = await fs.open(tempPath, "r+");
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
    await fs.rename(tempPath, fullDestination);
    await syncDirectory(directory);
  } catch (error) {
    await fs.rm(tempPath, { force: true }).catch(() => {});
    throw error;
  }
}

async function syncFile(filePath) {
  const handle = await fs.open(filePath, "r+");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
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

function pathComparisonKey(value) {
  const resolved = path.resolve(value);
  return process.platform === "win32" ? resolved.toLowerCase() : resolved;
}

function pathIsWithin(root, target) {
  const relativePath = path.relative(root, target);
  return relativePath !== ""
    && !relativePath.startsWith(`..${path.sep}`)
    && relativePath !== ".."
    && !path.isAbsolute(relativePath);
}

async function assertNoLinkedPathSegments(root, target) {
  const relativePath = path.relative(root, target);
  const segments = relativePath.split(path.sep).filter(Boolean);
  let current = root;
  for (const segment of [null, ...segments]) {
    if (segment !== null) {
      current = path.join(current, segment);
    }
    const stat = await fs.lstat(current);
    if (stat.isSymbolicLink()) {
      throw new Error(`Backup session target traverses a symbolic link or reparse point: ${current}`);
    }
  }
}

async function validateSessionManifestEntries(entries, codexHome) {
  const roots = ["sessions", "archived_sessions"].map((name) => path.resolve(codexHome, name));
  const seen = new Set();
  for (const entry of entries) {
    if (!entry || typeof entry.path !== "string" || !path.isAbsolute(entry.path)) {
      throw new Error("Backup session manifest contains a missing or non-absolute rollout path.");
    }
    const target = path.resolve(entry.path);
    const lexicalRoot = roots.find((root) => pathIsWithin(root, target));
    if (!lexicalRoot || !/^rollout-.*\.jsonl$/i.test(path.basename(target))) {
      throw new Error(`Backup session target is outside the allowed rollout roots: ${entry.path}`);
    }
    const key = pathComparisonKey(target);
    if (seen.has(key)) {
      throw new Error(`Backup session manifest contains a duplicate rollout target: ${entry.path}`);
    }
    seen.add(key);
    await assertNoLinkedPathSegments(lexicalRoot, target);
    const [canonicalRoot, canonicalTarget] = await Promise.all([
      fs.realpath(lexicalRoot),
      fs.realpath(target)
    ]);
    if (!pathIsWithin(canonicalRoot, canonicalTarget)) {
      throw new Error(`Backup session target resolves outside the allowed rollout roots: ${entry.path}`);
    }
    const stat = await fs.stat(canonicalTarget);
    if (!stat.isFile()) {
      throw new Error(`Backup session target is not a regular file: ${entry.path}`);
    }
  }
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
  const presence = {};
  for (const fileName of [GLOBAL_STATE_FILE_BASENAME, GLOBAL_STATE_BACKUP_FILE_BASENAME]) {
    presence[fileName] = await copyIfPresent(
      path.join(codexHome, fileName),
      path.join(backupDir, fileName)
    );
  }
  return presence;
}

export async function restoreGlobalStateFilesFromBackup(backupDir, codexHome, options = {}) {
  let metadata = null;
  try {
    metadata = JSON.parse(await fs.readFile(path.join(backupDir, "metadata.json"), "utf8"));
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }
  const selectedTargets = options.targetPaths
    ? new Set(options.targetPaths.map(pathComparisonKey))
    : null;
  for (const fileName of [GLOBAL_STATE_FILE_BASENAME, GLOBAL_STATE_BACKUP_FILE_BASENAME]) {
    const targetPath = path.join(codexHome, fileName);
    if (selectedTargets && !selectedTargets.has(pathComparisonKey(targetPath))) {
      continue;
    }
    const sourcePath = path.join(backupDir, fileName);
    const originalPresent = metadata?.globalStateFiles?.[fileName];
    if (originalPresent === true) {
      try {
        await fs.access(sourcePath);
      } catch {
        throw new Error(`Backup metadata says ${fileName} was present, but its backup copy is missing.`);
      }
      await copyFileAtomic(sourcePath, targetPath);
    } else if (originalPresent === false) {
      await removeIfPresent(targetPath);
    } else {
      // Legacy metadata did not record absence, so preserve its copy-only
      // behavior instead of deleting a file we cannot classify safely.
      await copyIfPresent(sourcePath, targetPath);
    }
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
  assertSqliteAccessSupported(effectiveStorage, "create a backup");
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
    const sqliteRelativePath = DB_FILE_BASENAME;
    const sqliteBackupPath = path.join(dbDir, "sqlite-home", sqliteRelativePath);
    const sqliteBackup = await createSqliteOnlineBackup(stateDb, sqliteBackupPath);
    if (!sqliteBackup.databasePresent) {
      throw new Error(`state_5.sqlite disappeared while creating a backup: ${stateDb.path}`);
    }
    copiedSqliteDbFiles.push(sqliteRelativePath);

    // Keep the v2 legacy mirror for readers that still consult dbFiles, but
    // derive it from the already-consistent standalone snapshot. Never copy
    // live WAL/SHM sidecars independently into a managed backup.
    const legacyRelativePath = safeRelativePath(codexHome, stateDb.path);
    if (legacyRelativePath) {
      await copyFileAtomic(sqliteBackupPath, path.join(dbDir, legacyRelativePath));
      copiedDbFiles.push(legacyRelativePath);
    }
  }

  if (configBackupText !== undefined) {
    const configBackupPath = path.join(backupDir, "config.toml");
    await writeFileAtomic(configBackupPath, configBackupText, "utf8");
    const configStat = await fs.stat(configPath);
    await fs.chmod(configBackupPath, configStat.mode);
    await syncFile(configBackupPath);
  } else {
    await copyIfPresent(configPath, path.join(backupDir, "config.toml"));
  }
  const globalStateFiles = await backupGlobalStateFiles(codexHome, backupDir);

  const sessionManifest = {
    version: 2,
    namespace: BACKUP_NAMESPACE,
    codexHome,
    targetProvider,
    createdAt: new Date().toISOString(),
    // Keep the full pre-mutation source of truth for the lifetime of the
    // backup. appliedPaths is only a compatibility hint for backups without a
    // transaction journal; journal `applying`/`applied` events decide which
    // entries a transactional restore must compensate.
    appliedPaths: null,
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
  await writeFileAtomic(
    path.join(backupDir, "session-meta-backup.json"),
    JSON.stringify(sessionManifest, null, 2),
    "utf8"
  );

  await writeMetadataWithInventory(backupDir, {
    version: 2,
    namespace: BACKUP_NAMESPACE,
    codexHome,
    sqliteHome: actualSqliteHome,
    targetProvider,
    createdAt: sessionManifest.createdAt,
    dbFiles: copiedDbFiles,
    sqliteDbFiles: copiedSqliteDbFiles,
    globalStateFiles,
    changedSessionFiles: sessionChanges.length
  });

  return backupDir;
}

export async function updateSessionBackupManifest(backupDir, sessionChanges, options = {}) {
  const manifestPath = path.join(backupDir, "session-meta-backup.json");
  const metadataPath = path.join(backupDir, "metadata.json");
  const sessionManifest = JSON.parse(await fs.readFile(manifestPath, "utf8"));
  const metadata = JSON.parse(await fs.readFile(metadataPath, "utf8"));

  // Promote older manifests to the v2 schema so restoreSessionChanges
  // can rely on the per-line `originalTurnContextModels` field.
  if (sessionManifest.version !== 2) {
    sessionManifest.version = 2;
  }

  const filesByPath = new Map(
    (sessionManifest.files ?? []).map((entry) => [pathComparisonKey(entry.path), entry])
  );
  for (const change of sessionChanges) {
    const existing = filesByPath.get(pathComparisonKey(change.path));
    if (!existing) {
      throw new Error(`Applied rollout is missing from the immutable backup manifest: ${change.path}`);
    }
    // Compatibility for callers that constructed a pre-v2 change without a
    // scan-time model snapshot. Never remove or replace a full-original entry.
    if ((!existing.originalTurnContextModels?.length)
        && change.originalTurnContextModels?.length) {
      existing.originalTurnContextModels = change.originalTurnContextModels;
    }
  }
  sessionManifest.appliedPaths = sessionChanges.map((change) => path.resolve(change.path));
  metadata.changedSessionFiles = sessionChanges.length;

  await writeFileAtomic(
    manifestPath,
    JSON.stringify(sessionManifest, null, 2),
    "utf8",
    { faultInjector: options.faultInjector }
  );
  await writeMetadataWithInventory(backupDir, metadata, {
    faultInjector: options.faultInjector
  });
}

export async function refreshBackupInventory(backupDir, options = {}) {
  const normalizedBackupDir = path.resolve(backupDir);
  const metadataPath = path.join(normalizedBackupDir, "metadata.json");
  const metadata = JSON.parse(await fs.readFile(metadataPath, "utf8"));
  if (metadata?.namespace !== BACKUP_NAMESPACE || !new Set([1, 2]).has(metadata.version)) {
    throw new Error(`Unsupported backup metadata in ${metadataPath}.`);
  }
  await writeMetadataWithInventory(normalizedBackupDir, metadata, options);
}

export async function getBackupSummary(codexHome) {
  const backupRoot = defaultBackupRoot(codexHome);
  const backupDirs = await listManagedBackupDirectories(backupRoot);
  let totalBytes = 0;
  for (const entry of backupDirs) {
    totalBytes += await getBackupDirectorySize(entry.fullPath);
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
  const pending = await findPendingTransactions(codexHome);
  const protectedBackups = new Set(
    pending.map((transaction) => pathComparisonKey(path.dirname(transaction.filePath)))
  );
  const toDelete = backupDirs
    .slice(keepCount)
    .filter((entry) => !protectedBackups.has(pathComparisonKey(entry.fullPath)));
  let freedBytes = 0;
  for (const entry of toDelete) {
    freedBytes += await getBackupDirectorySize(entry.fullPath);
    await fs.rm(entry.fullPath, { recursive: true, force: true });
  }

  return {
    backupRoot,
    deletedCount: toDelete.length,
    remainingCount: backupDirs.length - toDelete.length,
    freedBytes
  };
}

async function selectSessionRestoreEntries(backupDir, sessionManifest) {
  const files = sessionManifest.files ?? [];
  const journalPath = path.join(backupDir, TRANSACTION_JOURNAL_BASENAME);
  try {
    const journal = await readTransactionJournal(journalPath);
    // A damaged tail cannot prove that no later target was mutated before its
    // journal record became unreadable. Restore the immutable full-original
    // manifest for any invalid/empty journal; only a fully valid prefix may
    // narrow recovery to its applying/applied targets.
    if (journal.invalidTail || journal.events.length === 0) {
      return files;
    }
    if (journal.events.length > 0) {
      const startedPaths = new Set(
        getStartedJournalTargets(journal, "rollout").map(pathComparisonKey)
      );
      return files.filter((entry) => startedPaths.has(pathComparisonKey(entry.path)));
    }
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }

  if (Array.isArray(sessionManifest.appliedPaths)) {
    const applied = new Set(sessionManifest.appliedPaths.map(pathComparisonKey));
    return files.filter((entry) => applied.has(pathComparisonKey(entry.path)));
  }
  return files;
}

async function readValidatedBackupMetadata(backupDir, codexHome) {
  const metadataPath = path.join(backupDir, "metadata.json");
  const metadata = JSON.parse(await fs.readFile(metadataPath, "utf8"));
  if (metadata.namespace !== BACKUP_NAMESPACE || ![1, 2].includes(metadata.version)) {
    throw new Error(`Unsupported backup metadata in ${metadataPath}.`);
  }
  if (typeof metadata.codexHome !== "string" || !storagePathsEqual(metadata.codexHome, codexHome)) {
    throw new Error(`Backup was created for ${metadata.codexHome}, not ${codexHome}.`);
  }
  return metadata;
}

async function backupFileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch (error) {
    if (error?.code === "ENOENT") {
      return false;
    }
    throw error;
  }
}

export async function getBackupRecoveryCoverage(backupDir, storageOrCodexHome) {
  const storage = typeof storageOrCodexHome === "string"
    ? resolveStorageLayout({ codexHome: storageOrCodexHome, env: {} })
    : storageOrCodexHome;
  const codexHome = storage.codexHome;
  const metadata = await readValidatedBackupMetadata(backupDir, codexHome);
  const databaseFiles = metadata.version >= 2
    ? metadata.sqliteDbFiles
    : metadata.dbFiles;
  if (!Array.isArray(databaseFiles)
      || databaseFiles.some((fileName) => typeof fileName !== "string")) {
    throw new Error(`Backup metadata contains an invalid SQLite file manifest: ${path.join(backupDir, "metadata.json")}`);
  }
  for (const fileName of databaseFiles) {
    if (path.isAbsolute(fileName) || fileName.split(/[\\/]/).includes("..")) {
      throw new Error(`Invalid SQLite backup path: ${fileName}`);
    }
  }

  const sessionManifestPath = path.join(backupDir, "session-meta-backup.json");
  const sessionManifest = JSON.parse(await fs.readFile(sessionManifestPath, "utf8"));
  if (sessionManifest.namespace !== BACKUP_NAMESPACE || ![1, 2].includes(sessionManifest.version)) {
    throw new Error(`Unsupported session backup manifest in ${sessionManifestPath}.`);
  }
  if (typeof sessionManifest.codexHome !== "string"
      || !storagePathsEqual(sessionManifest.codexHome, codexHome)) {
    throw new Error(`Session backup was created for ${sessionManifest.codexHome}, not ${codexHome}.`);
  }
  if (!Array.isArray(sessionManifest.files)) {
    throw new Error(`Session backup manifest has an invalid files collection: ${sessionManifestPath}`);
  }
  await validateSessionManifestEntries(sessionManifest.files, codexHome);

  let globalState = false;
  if (metadata.version >= 2) {
    if (!metadata.globalStateFiles
        || typeof metadata.globalStateFiles !== "object"
        || Array.isArray(metadata.globalStateFiles)) {
      throw new Error(`Backup metadata contains invalid global-state presence data: ${path.join(backupDir, "metadata.json")}`);
    }
    for (const fileName of [GLOBAL_STATE_FILE_BASENAME, GLOBAL_STATE_BACKUP_FILE_BASENAME]) {
      if (typeof metadata.globalStateFiles[fileName] !== "boolean") {
        throw new Error(
          `Backup metadata lacks a boolean presence record for ${fileName}: ${path.join(backupDir, "metadata.json")}`
        );
      }
    }
    // A complete v2 presence map is itself recovery coverage. Two false
    // values mean rollback must delete both targets, not that there is no
    // global-state work to restore.
    globalState = true;
  } else {
    globalState = await backupFileExists(path.join(backupDir, GLOBAL_STATE_FILE_BASENAME))
      || await backupFileExists(path.join(backupDir, GLOBAL_STATE_BACKUP_FILE_BASENAME));
  }

  return {
    config: await backupFileExists(path.join(backupDir, "config.toml")),
    globalState,
    database: databaseFiles.some((fileName) => path.basename(fileName) === DB_FILE_BASENAME),
    sessions: sessionManifest.files.length > 0
  };
}

export async function restoreBackup(backupDir, storageOrCodexHome, options = {}) {
  const {
    restoreConfig = true,
    restoreGlobalState = restoreConfig,
    restoreDatabase = true,
    restoreSessions = true,
    allowSqliteHomeRelocation = false,
    globalStateTargetPaths = null,
    sessionTargetPaths = null
  } = options;
  const storage = typeof storageOrCodexHome === "string"
    ? resolveStorageLayout({ codexHome: storageOrCodexHome, env: {} })
    : storageOrCodexHome;
  assertSqliteAccessSupported(storage, "restore");
  const codexHome = storage.codexHome;
  const metadata = await readValidatedBackupMetadata(backupDir, codexHome);

  let sessionManifest = null;
  let sessionRestoreEntries = [];
  if (restoreSessions) {
    const sessionManifestPath = path.join(backupDir, "session-meta-backup.json");
    sessionManifest = JSON.parse(await fs.readFile(sessionManifestPath, "utf8"));
    if (sessionManifest.namespace !== BACKUP_NAMESPACE || ![1, 2].includes(sessionManifest.version)) {
      throw new Error(`Unsupported session backup manifest in ${sessionManifestPath}.`);
    }
    if (typeof sessionManifest.codexHome !== "string"
        || !storagePathsEqual(sessionManifest.codexHome, codexHome)) {
      throw new Error(`Session backup was created for ${sessionManifest.codexHome}, not ${codexHome}.`);
    }
    await validateSessionManifestEntries(sessionManifest.files ?? [], codexHome);
    if (sessionTargetPaths) {
      const selected = new Set(sessionTargetPaths.map(pathComparisonKey));
      sessionRestoreEntries = (sessionManifest.files ?? [])
        .filter((entry) => selected.has(pathComparisonKey(entry.path)));
    } else {
      sessionRestoreEntries = await selectSessionRestoreEntries(backupDir, sessionManifest);
    }
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

    const mainEntries = entries.filter(({ fileName }) => path.basename(fileName) === DB_FILE_BASENAME);
    if (mainEntries.length !== 1) {
      throw new Error("Backup must contain exactly one state_5.sqlite restore source.");
    }
    databaseRestorePlan = mainEntries[0];
  }

  const configBackupPath = path.join(backupDir, "config.toml");
  if (restoreConfig) {
    await copyIfPresent(configBackupPath, path.join(codexHome, "config.toml"));
  }
  if (restoreGlobalState) {
    await restoreGlobalStateFilesFromBackup(backupDir, codexHome, {
      targetPaths: globalStateTargetPaths
    });
  }

  if (databaseRestorePlan) {
    await restoreSqliteOnlineBackup(
      databaseRestorePlan.sourcePath,
      databaseRestorePlan.targetPath
    );
  }

  if (restoreSessions) {
    await restoreSessionChanges(sessionRestoreEntries);
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

async function writeMetadataWithInventory(backupDir, metadata, options = {}) {
  const metadataPath = path.join(backupDir, "metadata.json");
  const payload = await getDirectoryInventory(backupDir, metadataPath);
  const fileCount = payload.fileCount + 1;
  let sizeBytes = 0;
  let serialized = "";
  for (let attempt = 0; attempt < 8; attempt += 1) {
    serialized = JSON.stringify({ ...metadata, sizeBytes, fileCount }, null, 2);
    const nextSizeBytes = payload.sizeBytes + Buffer.byteLength(serialized, "utf8");
    if (nextSizeBytes === sizeBytes) {
      await writeFileAtomic(metadataPath, serialized, "utf8", options);
      return;
    }
    sizeBytes = nextSizeBytes;
  }
  throw new Error(`Backup metadata inventory did not converge: ${metadataPath}`);
}

async function getBackupDirectorySize(backupDir) {
  try {
    const metadata = JSON.parse(
      await fs.readFile(path.join(backupDir, "metadata.json"), "utf8")
    );
    if (metadata?.namespace === BACKUP_NAMESPACE
        && Number.isSafeInteger(metadata.sizeBytes)
        && metadata.sizeBytes >= 0
        && Number.isSafeInteger(metadata.fileCount)
        && metadata.fileCount >= 1) {
      return metadata.sizeBytes;
    }
  } catch {
    // Older or damaged inventory fields fall back to the recursive scan below.
  }
  return getDirectorySize(backupDir);
}

async function getDirectorySize(directoryPath) {
  return (await getDirectoryInventory(directoryPath)).sizeBytes;
}

async function getDirectoryInventory(directoryPath, excludedFilePath = null) {
  let entries;
  try {
    entries = await fs.readdir(directoryPath, { withFileTypes: true });
  } catch (error) {
    if (error?.code === "ENOENT") {
      return { sizeBytes: 0, fileCount: 0 };
    }
    throw error;
  }

  let sizeBytes = 0;
  let fileCount = 0;
  const excluded = excludedFilePath === null ? null : path.resolve(excludedFilePath);
  for (const entry of entries) {
    const fullPath = path.join(directoryPath, entry.name);
    if (entry.isDirectory()) {
      const child = await getDirectoryInventory(fullPath, excluded);
      sizeBytes += child.sizeBytes;
      fileCount += child.fileCount;
      continue;
    }
    if (entry.isFile()) {
      if (excluded !== null && path.resolve(fullPath) === excluded) {
        continue;
      }
      const stat = await fs.stat(fullPath);
      sizeBytes += stat.size;
      fileCount += 1;
    }
  }

  return { sizeBytes, fileCount };
}
