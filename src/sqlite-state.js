import fs from "node:fs/promises";
import path from "node:path";

import { syncDirectory } from "./atomic-file.js";
import { DB_FILE_BASENAME, SESSION_DIRS, SQLITE_DIR_BASENAME } from "./constants.js";
import { CoreError } from "./core-error.js";
import { openDatabase } from "./sqlite.js";
import { resolveStorageLayout } from "./storage-layout.js";

const DEFAULT_BUSY_TIMEOUT_MS = 0;

export function stateDbPath(codexHome) {
  return path.join(codexHome, SQLITE_DIR_BASENAME, DB_FILE_BASENAME);
}

export function legacyStateDbPath(codexHome) {
  return path.join(codexHome, DB_FILE_BASENAME);
}

function normalizeStorage(storageOrCodexHome) {
  if (typeof storageOrCodexHome === "string") {
    return resolveStorageLayout({ codexHome: storageOrCodexHome, env: {} });
  }
  return storageOrCodexHome;
}

export function stateDbCandidates(storageOrCodexHome) {
  return normalizeStorage(storageOrCodexHome).stateDbCandidates;
}

async function countRolloutFilesInDir(rootDir) {
  let entries;
  try {
    entries = await fs.readdir(rootDir, { withFileTypes: true });
  } catch {
    return 0;
  }

  let count = 0;
  for (const entry of entries) {
    const fullPath = path.join(rootDir, entry.name);
    if (entry.isDirectory()) {
      count += await countRolloutFilesInDir(fullPath);
      continue;
    }
    if (entry.isFile() && entry.name.startsWith("rollout-") && entry.name.endsWith(".jsonl")) {
      count += 1;
    }
  }
  return count;
}

async function countRolloutFiles(codexHome) {
  let count = 0;
  for (const dirname of SESSION_DIRS) {
    count += await countRolloutFilesInDir(path.join(codexHome, dirname));
  }
  return count;
}

function tableExists(db, tableName) {
  return Boolean(db
    .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
    .get(tableName));
}

function maxThreadTimestampMs(db) {
  if (tableHasColumn(db, "threads", "updated_at_ms")) {
    return Number(db.prepare("SELECT COALESCE(MAX(updated_at_ms), 0) AS value FROM threads").get().value) || 0;
  }
  if (tableHasColumn(db, "threads", "updated_at")) {
    return (Number(db.prepare("SELECT COALESCE(MAX(updated_at), 0) AS value FROM threads").get().value) || 0) * 1000;
  }
  if (tableHasColumn(db, "threads", "created_at_ms")) {
    return Number(db.prepare("SELECT COALESCE(MAX(created_at_ms), 0) AS value FROM threads").get().value) || 0;
  }
  if (tableHasColumn(db, "threads", "created_at")) {
    return (Number(db.prepare("SELECT COALESCE(MAX(created_at), 0) AS value FROM threads").get().value) || 0) * 1000;
  }
  return 0;
}

async function readStateDbCandidateStats(candidate, priority) {
  let db;
  try {
    db = await openDatabase(candidate.path, { readOnly: true });
    if (!tableExists(db, "threads")) {
      throw new Error("threads table not found");
    }
    const threadCount = Number(db.prepare("SELECT COUNT(*) AS count FROM threads").get().count) || 0;
    return {
      candidate,
      priority,
      threadCount,
      maxThreadTimestampMs: maxThreadTimestampMs(db),
      mtimeMs: (await fs.stat(candidate.path)).mtimeMs
    };
  } finally {
    db?.close();
  }
}

function compareStateDbCandidateStats(a, b) {
  if (a.rolloutDistance !== b.rolloutDistance) {
    return a.rolloutDistance - b.rolloutDistance;
  }
  if (a.threadCount !== b.threadCount) {
    return b.threadCount - a.threadCount;
  }
  if (a.maxThreadTimestampMs !== b.maxThreadTimestampMs) {
    return b.maxThreadTimestampMs - a.maxThreadTimestampMs;
  }
  if (a.mtimeMs !== b.mtimeMs) {
    return b.mtimeMs - a.mtimeMs;
  }
  return a.priority - b.priority;
}

export async function detectStateDb(storageOrCodexHome) {
  const storage = normalizeStorage(storageOrCodexHome);
  const existingCandidates = [];
  const candidates = stateDbCandidates(storage);
  for (const [priority, candidate] of candidates.entries()) {
    try {
      await fs.access(candidate.path);
      existingCandidates.push({ candidate, priority });
    } catch {
      // Try the next known Codex state DB location.
    }
  }
  if (existingCandidates.length === 0) {
    return null;
  }

  const rolloutCount = await countRolloutFiles(storage.codexHome);
  const readableCandidates = [];
  for (const { candidate, priority } of existingCandidates) {
    try {
      const stats = await readStateDbCandidateStats(candidate, priority);
      readableCandidates.push({
        ...stats,
        rolloutDistance: rolloutCount > 0 ? Math.abs(stats.threadCount - rolloutCount) : 0
      });
    } catch {
      // Keep unreadable candidates as a fallback so existing status/error
      // handling still points at state_5.sqlite when no usable DB exists.
    }
  }

  if (readableCandidates.length === 0) {
    return existingCandidates[0].candidate;
  }

  return readableCandidates.sort(compareStateDbCandidateStats)[0].candidate;
}

async function resolveStateDbLocation(storageOrLocation) {
  if (!storageOrLocation) {
    return null;
  }
  if (Object.hasOwn(storageOrLocation, "stateDbLocation")) {
    return storageOrLocation.stateDbLocation;
  }
  if (typeof storageOrLocation.path === "string" && typeof storageOrLocation.source === "string") {
    return storageOrLocation;
  }
  return detectStateDb(storageOrLocation);
}

export async function existingStateDbPath(storageOrLocation) {
  return (await resolveStateDbLocation(storageOrLocation))?.path ?? null;
}

function tableHasColumn(db, tableName, columnName) {
  return db
    .prepare(`PRAGMA table_info(${JSON.stringify(tableName)})`)
    .all()
    .some((column) => column.name === columnName);
}

function normalizeBusyTimeoutMs(busyTimeoutMs) {
  return Number.isInteger(busyTimeoutMs) && busyTimeoutMs >= 0
    ? busyTimeoutMs
    : DEFAULT_BUSY_TIMEOUT_MS;
}

function setBusyTimeout(db, busyTimeoutMs) {
  db.exec(`PRAGMA busy_timeout = ${normalizeBusyTimeoutMs(busyTimeoutMs)}`);
}

export function configureSqliteWriteDurability(db) {
  db.exec("PRAGMA synchronous = FULL");
  const synchronous = Number(db.prepare("PRAGMA synchronous").get().synchronous);
  if (synchronous !== 2) {
    throw new Error(`Unable to configure SQLite synchronous=FULL (reported ${synchronous}).`);
  }
  return { synchronous: "full", value: synchronous };
}

function sqlitePrimaryResultCode(error) {
  return Number.isInteger(error?.errcode) ? error.errcode & 0xff : null;
}

function isSqliteBusyError(error) {
  if (error instanceof CoreError) return error.code === "SQLITE_BUSY";
  const primaryCode = sqlitePrimaryResultCode(error);
  return error?.code === "SQLITE_BUSY"
    || error?.code === "SQLITE_LOCKED"
    || (error?.code === "ERR_SQLITE_ERROR" && (primaryCode === 5 || primaryCode === 6));
}

function isSqliteMalformedError(error) {
  if (error instanceof CoreError) return error.code === "SQLITE_UNREADABLE";
  const primaryCode = sqlitePrimaryResultCode(error);
  return error?.code === "SQLITE_CORRUPT"
    || error?.code === "SQLITE_NOTADB"
    || (error?.code === "ERR_SQLITE_ERROR" && (primaryCode === 11 || primaryCode === 26));
}

function sqliteErrorDetails(error) {
  const primaryCode = sqlitePrimaryResultCode(error);
  return {
    ...(typeof error?.code === "string" ? { causeCode: error.code } : {}),
    ...(primaryCode !== null ? { sqlitePrimaryCode: primaryCode } : {})
  };
}

export function wrapSqliteBusyError(error, action) {
  if (!isSqliteBusyError(error)) {
    return error;
  }
  if (error instanceof CoreError) return error;
  return new CoreError(
    "SQLITE_BUSY",
    `Unable to ${action} because state_5.sqlite is currently in use. Close Codex and the Codex app, then retry. Original error: ${error.message}`,
    { cause: error, details: sqliteErrorDetails(error) }
  );
}

export function wrapSqliteMalformedError(error, action) {
  if (!isSqliteMalformedError(error)) {
    return error;
  }
  if (error instanceof CoreError) return error;
  return new CoreError(
    "SQLITE_UNREADABLE",
    `Unable to ${action} because state_5.sqlite is malformed or unreadable. Close Codex, back up or repair the database, then retry. Original error: ${error.message}`,
    { cause: error, details: sqliteErrorDetails(error) }
  );
}

export async function readSqliteProviderCounts(storageOrLocation) {
  const dbPath = await existingStateDbPath(storageOrLocation);
  if (!dbPath) {
    return null;
  }

  let db;
  try {
    db = await openDatabase(dbPath);
    const rows = db.prepare(`
      SELECT
        CASE
          WHEN model_provider IS NULL OR model_provider = '' THEN '(missing)'
          ELSE model_provider
        END AS model_provider,
        archived,
        COUNT(*) AS count
      FROM threads
      GROUP BY model_provider, archived
      ORDER BY archived, model_provider
    `).all();
    const result = {
      sessions: {},
      archived_sessions: {}
    };
    for (const row of rows) {
      const bucket = row.archived ? result.archived_sessions : result.sessions;
      bucket[row.model_provider] = row.count;
    }
    return result;
  } catch (error) {
    if (isSqliteMalformedError(error)) {
      return {
        sessions: {},
        archived_sessions: {},
        unreadable: true,
        error: "state_5.sqlite is malformed or unreadable"
      };
    }
    if (isSqliteBusyError(error)) {
      return {
        sessions: {},
        archived_sessions: {},
        unreadable: true,
        error: "state_5.sqlite is currently in use"
      };
    }
    throw error;
  } finally {
    db?.close();
  }
}

export async function readSqliteRepairStats(storageOrLocation, options = {}) {
  const dbPath = await existingStateDbPath(storageOrLocation);
  if (!dbPath) {
    return null;
  }

  let db;
  try {
    db = await openDatabase(dbPath, { readOnly: true });
    let modelRowsNeedingRepair = 0;
    if (typeof options.targetModel === "string"
        && options.targetModel
        && tableHasColumn(db, "threads", "model")) {
      modelRowsNeedingRepair = Number(db.prepare(`
        SELECT COUNT(*) AS count
        FROM threads
        WHERE COALESCE(model, '') <> ?
      `).get(options.targetModel)?.count) || 0;
    }
    let userEventRowsNeedingRepair = 0;
    if (tableHasColumn(db, "threads", "has_user_event") && options.userEventThreadIds?.size) {
      const stmt = db.prepare("SELECT has_user_event FROM threads WHERE id = ?");
      for (const threadId of options.userEventThreadIds) {
        const row = stmt.get(threadId);
        if (row && Number(row.has_user_event) !== 1) {
          userEventRowsNeedingRepair += 1;
        }
      }
    }

    let cwdRowsNeedingRepair = 0;
    if (tableHasColumn(db, "threads", "cwd") && options.threadCwdById?.size) {
      const stmt = db.prepare("SELECT cwd FROM threads WHERE id = ?");
      for (const [threadId, cwd] of options.threadCwdById) {
        if (typeof threadId !== "string" || !threadId || typeof cwd !== "string" || !cwd.trim()) {
          continue;
        }
        const row = stmt.get(threadId);
        if (row && row.cwd !== cwd) {
          cwdRowsNeedingRepair += 1;
        }
      }
    }

    return {
      modelRowsNeedingRepair,
      userEventRowsNeedingRepair,
      cwdRowsNeedingRepair
    };
  } catch (error) {
    throw wrapSqliteMalformedError(
      wrapSqliteBusyError(error, "read SQLite repair diagnostics"),
      "read SQLite repair diagnostics"
    );
  } finally {
    db?.close();
  }
}

const SQLITE_REPAIR_TARGETS = new Set(["models", "cwd", "userEvent"]);

function normalizeSqliteRepairTargets(value) {
  if (!Array.isArray(value)
      || value.some((target) => typeof target !== "string" || !SQLITE_REPAIR_TARGETS.has(target))) {
    throw new CoreError("INVALID_INPUT", "SQLite repair targets are invalid.");
  }
  return new Set(value);
}

export async function applySqliteRepairs(storageOrLocation, afterUpdateOrOptions, maybeOptions) {
  const afterUpdate = typeof afterUpdateOrOptions === "function" ? afterUpdateOrOptions : null;
  const options = typeof afterUpdateOrOptions === "function"
    ? (maybeOptions ?? {})
    : (afterUpdateOrOptions ?? {});
  const targets = normalizeSqliteRepairTargets(options.targets ?? []);
  const targetModel = options.targetModel ?? null;
  if (targets.has("models") && (typeof targetModel !== "string" || !targetModel)) {
    throw new CoreError("INVALID_INPUT", "Model repair requires the current root model.");
  }

  const emptyResult = {
    updatedRows: 0,
    providerRowsUpdated: 0,
    modelRowsUpdated: 0,
    userEventRowsUpdated: 0,
    cwdRowsUpdated: 0,
    databasePresent: false
  };
  const dbPath = await existingStateDbPath(storageOrLocation);
  if (!dbPath) {
    if (afterUpdate) await afterUpdate(emptyResult);
    return emptyResult;
  }

  let db;
  let transactionOpen = false;
  try {
    db = await openDatabase(dbPath);
    setBusyTimeout(db, options.busyTimeoutMs);
    configureSqliteWriteDurability(db);
    db.exec("BEGIN IMMEDIATE");
    transactionOpen = true;

    let modelRowsUpdated = 0;
    if (targets.has("models") && tableHasColumn(db, "threads", "model")) {
      modelRowsUpdated = db.prepare(`
        UPDATE threads
        SET model = ?
        WHERE COALESCE(model, '') <> ?
      `).run(targetModel, targetModel).changes ?? 0;
    }

    let userEventRowsUpdated = 0;
    if (targets.has("userEvent")
        && tableHasColumn(db, "threads", "has_user_event")
        && options.userEventThreadIds?.size) {
      const statement = db.prepare(`
        UPDATE threads
        SET has_user_event = 1
        WHERE id = ? AND COALESCE(has_user_event, 0) <> 1
      `);
      for (const threadId of options.userEventThreadIds) {
        userEventRowsUpdated += statement.run(threadId).changes ?? 0;
      }
    }

    let cwdRowsUpdated = 0;
    if (targets.has("cwd")
        && tableHasColumn(db, "threads", "cwd")
        && options.threadCwdById?.size) {
      const statement = db.prepare(`
        UPDATE threads
        SET cwd = ?
        WHERE id = ? AND COALESCE(cwd, '') <> ?
      `);
      for (const [threadId, cwd] of options.threadCwdById) {
        if (typeof threadId !== "string" || !threadId || typeof cwd !== "string" || !cwd.trim()) continue;
        cwdRowsUpdated += statement.run(cwd, threadId, cwd).changes ?? 0;
      }
    }

    const result = {
      updatedRows: modelRowsUpdated + userEventRowsUpdated + cwdRowsUpdated,
      providerRowsUpdated: 0,
      modelRowsUpdated,
      userEventRowsUpdated,
      cwdRowsUpdated,
      databasePresent: true
    };
    if (afterUpdate) await afterUpdate(result);
    options.onCommitAttempt?.(result);
    db.exec("COMMIT");
    transactionOpen = false;
    await options.afterCommit?.(result);
    return result;
  } catch (error) {
    if (transactionOpen) {
      try { db.exec("ROLLBACK"); }
      catch { /* Preserve the original failure. */ }
    }
    throw wrapSqliteMalformedError(
      wrapSqliteBusyError(error, "repair session metadata"),
      "repair session metadata"
    );
  } finally {
    db?.close();
  }
}

export async function assertSqliteWritable(storageOrLocation, options = {}) {
  const dbPath = await existingStateDbPath(storageOrLocation);
  if (!dbPath) {
    return { databasePresent: false };
  }

  let db;
  try {
    db = await openDatabase(dbPath);
    setBusyTimeout(db, options.busyTimeoutMs);
    configureSqliteWriteDurability(db);
    db.exec("BEGIN IMMEDIATE");
    db.exec("ROLLBACK");
    return { databasePresent: true };
  } catch (error) {
    throw wrapSqliteMalformedError(
      wrapSqliteBusyError(error, "update session provider metadata"),
      "update session provider metadata"
    );
  } finally {
    db?.close();
  }
}

export async function updateSqliteProvider(storageOrLocation, targetProvider, afterUpdateOrOptions, maybeOptions) {
  const afterUpdate = typeof afterUpdateOrOptions === "function" ? afterUpdateOrOptions : null;
  const options = typeof afterUpdateOrOptions === "function"
    ? (maybeOptions ?? {})
    : (afterUpdateOrOptions ?? {});
  // When provided, the per-thread `model` column is rewritten alongside
  // `model_provider` so old sessions pick up the new active model in
  // the Codex UI's bottom-right label. Pass null to leave the column
  // untouched (legacy behaviour for callers that do not track model).
  const targetModel = options.targetModel ?? null;

  const dbPath = await existingStateDbPath(storageOrLocation);
  if (!dbPath) {
    if (afterUpdate) {
      await afterUpdate({
        updatedRows: 0,
        providerRowsUpdated: 0,
        modelRowsUpdated: 0,
        userEventRowsUpdated: 0,
        cwdRowsUpdated: 0,
        databasePresent: false
      });
    }
    return {
      updatedRows: 0,
      providerRowsUpdated: 0,
      modelRowsUpdated: 0,
      userEventRowsUpdated: 0,
      cwdRowsUpdated: 0,
      databasePresent: false
    };
  }

  let db;
  let transactionOpen = false;
  try {
    db = await openDatabase(dbPath);
    setBusyTimeout(db, options.busyTimeoutMs);
    configureSqliteWriteDurability(db);
    db.exec("BEGIN IMMEDIATE");
    transactionOpen = true;
    // When a target model is provided, align every thread's `model` column
    // with it alongside `model_provider`. This is what makes the bottom-right
    // of the Codex UI show the active model for old sessions, instead of the
    // name that was in effect when each thread was originally created.
    // The `model` column is only present in newer Codex schemas, so guard
    // with tableHasColumn to keep legacy layouts working.
    const wantsModel = targetModel != null && targetModel.length > 0
      && tableHasColumn(db, "threads", "model");
    // Keep the update shape and counters identical to .NET: provider and
    // optional model are independent writes, and a row changed in both
    // columns contributes two to updatedRows.
    const providerResult = db.prepare(`
      UPDATE threads
      SET model_provider = ?
      WHERE COALESCE(model_provider, '') <> ?
    `).run(targetProvider, targetProvider);
    let modelUpdatedRows = 0;
    if (wantsModel) {
      modelUpdatedRows = db.prepare(`
        UPDATE threads
        SET model = ?
        WHERE COALESCE(model, '') <> ?
      `).run(targetModel, targetModel).changes ?? 0;
    }
    let userEventUpdatedRows = 0;
    if (tableHasColumn(db, "threads", "has_user_event") && options.userEventThreadIds?.size) {
      const userEventStmt = db.prepare(`
        UPDATE threads
        SET has_user_event = 1
        WHERE id = ? AND COALESCE(has_user_event, 0) <> 1
      `);
      for (const threadId of options.userEventThreadIds) {
        userEventUpdatedRows += userEventStmt.run(threadId).changes ?? 0;
      }
    }
    let cwdUpdatedRows = 0;
    if (tableHasColumn(db, "threads", "cwd") && options.threadCwdById?.size) {
      const cwdStmt = db.prepare(`
        UPDATE threads
        SET cwd = ?
        WHERE id = ? AND COALESCE(cwd, '') <> ?
      `);
      for (const [threadId, cwd] of options.threadCwdById) {
        if (typeof threadId !== "string" || !threadId || typeof cwd !== "string" || !cwd.trim()) {
          continue;
        }
        cwdUpdatedRows += cwdStmt.run(cwd, threadId, cwd).changes ?? 0;
      }
    }
    const providerUpdatedRows = providerResult.changes ?? 0;
    const updatedRows = providerUpdatedRows + modelUpdatedRows + userEventUpdatedRows + cwdUpdatedRows;
    const result = {
      updatedRows,
      providerRowsUpdated: providerUpdatedRows,
      modelRowsUpdated: modelUpdatedRows,
      userEventRowsUpdated: userEventUpdatedRows,
      cwdRowsUpdated: cwdUpdatedRows,
      databasePresent: true
    };
    if (afterUpdate) {
      await afterUpdate(result);
    }
    // Record the boundary before COMMIT. A COMMIT exception cannot prove the
    // transaction was not made durable, so the coordinator must compensate
    // from its bound backup whenever this attempt covered actual mutations.
    options.onCommitAttempt?.(result);
    db.exec("COMMIT");
    transactionOpen = false;
    await options.afterCommit?.(result);
    return result;
  } catch (error) {
    if (transactionOpen) {
      try {
        db.exec("ROLLBACK");
      } catch {
        // Ignore rollback failures and surface the original error.
      }
    }
    throw wrapSqliteMalformedError(
      wrapSqliteBusyError(error, "update session provider metadata"),
      "update session provider metadata"
    );
  } finally {
    db?.close();
  }
}

function readSqliteConnectionMetadata(db) {
  return {
    journalMode: String(db.prepare("PRAGMA journal_mode").get().journal_mode).toLowerCase(),
    pageSize: Number(db.prepare("PRAGMA page_size").get().page_size),
    userVersion: Number(db.prepare("PRAGMA user_version").get().user_version),
    applicationId: Number(db.prepare("PRAGMA application_id").get().application_id)
  };
}

async function readStandaloneSqliteHeaderMetadata(dbPath) {
  const handle = await fs.open(dbPath, "r");
  try {
    const header = Buffer.alloc(100);
    const { bytesRead } = await handle.read(header, 0, header.length, 0);
    if (bytesRead !== header.length
      || header.subarray(0, 16).toString("binary") !== "SQLite format 3\u0000") {
      throw new Error("SQLite online backup did not produce a valid standalone database header.");
    }
    const rawPageSize = header.readUInt16BE(16);
    return {
      journalMode: header[18] === 2 && header[19] === 2 ? "wal" : "delete",
      pageSize: rawPageSize === 1 ? 65536 : rawPageSize,
      userVersion: header.readInt32BE(60),
      applicationId: header.readInt32BE(68)
    };
  } finally {
    await handle.close();
  }
}

async function pathExists(filePath) {
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

/**
 * Create one consistent SQLite database file via the active driver's official
 * online-backup API. WAL/SHM sidecars are intentionally neither copied nor
 * emitted; metadata is read from the standalone main-file header.
 */
export async function createSqliteOnlineBackup(storageOrLocation, destinationPath, options = {}) {
  const dbPath = await existingStateDbPath(storageOrLocation);
  if (!dbPath) {
    return { databasePresent: false, backupPath: null, driver: null, metadata: null };
  }

  const fullSourcePath = path.resolve(dbPath);
  const fullDestinationPath = path.resolve(destinationPath);
  const sourceIdentityPath = process.platform === "win32" ? fullSourcePath.toLowerCase() : fullSourcePath;
  const destinationIdentityPath = process.platform === "win32"
    ? fullDestinationPath.toLowerCase()
    : fullDestinationPath;
  if (sourceIdentityPath === destinationIdentityPath) {
    throw new Error("SQLite online backup destination must differ from the source database.");
  }
  await fs.mkdir(path.dirname(fullDestinationPath), { recursive: true });
  if (await pathExists(fullDestinationPath)) {
    throw new Error("SQLite online backup destination already exists.");
  }

  let db;
  let backupPhase = "source-open";
  try {
    // Read-only is deliberate: a database that disappears after discovery
    // must fail here instead of being silently recreated as an empty file.
    db = await openDatabase(fullSourcePath, { readOnly: true });
    backupPhase = "source-metadata";
    setBusyTimeout(db, options.busyTimeoutMs);
    const sourceMetadata = readSqliteConnectionMetadata(db);
    const driver = db.driver ?? "unknown";
    backupPhase = "destination-backup";
    await db.backup(fullDestinationPath, options.backupOptions ?? {});

    backupPhase = "destination-sync";
    const handle = await fs.open(fullDestinationPath, "r+");
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
    await syncDirectory(path.dirname(fullDestinationPath));

    const sidecars = [`${fullDestinationPath}-wal`, `${fullDestinationPath}-shm`];
    if ((await Promise.all(sidecars.map(pathExists))).some(Boolean)) {
      throw new Error("SQLite online backup unexpectedly emitted a WAL/SHM sidecar.");
    }
    const backupMetadata = await readStandaloneSqliteHeaderMetadata(fullDestinationPath);
    return {
      databasePresent: true,
      backupPath: fullDestinationPath,
      driver,
      metadata: {
        source: sourceMetadata,
        backup: backupMetadata,
        preserved: {
          journalMode: sourceMetadata.journalMode === backupMetadata.journalMode,
          pageSize: sourceMetadata.pageSize === backupMetadata.pageSize,
          userVersion: sourceMetadata.userVersion === backupMetadata.userVersion,
          applicationId: sourceMetadata.applicationId === backupMetadata.applicationId
        }
      }
    };
  } catch (error) {
    await Promise.all([
      fullDestinationPath,
      `${fullDestinationPath}-wal`,
      `${fullDestinationPath}-shm`
    ].map((filePath) => fs.rm(filePath, { force: true }).catch(() => {})));
    const phasedError = new Error(
      `SQLite online backup failed during ${backupPhase}: ${error instanceof Error ? error.message : String(error)}`,
      { cause: error instanceof Error ? error : undefined }
    );
    if (typeof error?.code === "string") phasedError.code = error.code;
    throw wrapSqliteMalformedError(
      wrapSqliteBusyError(phasedError, "create a consistent SQLite online backup"),
      "create a consistent SQLite online backup"
    );
  } finally {
    db?.close();
  }
}

/**
 * Restore a standalone SQLite snapshot through SQLite's online-backup API.
 *
 * The backup is the source and the live database is the destination. SQLite
 * keeps a write transaction open on the destination for the operation and
 * rolls it back if the backup does not finish. In particular, live WAL/SHM
 * files must never be unlinked or copied by this path.
 */
export async function restoreSqliteOnlineBackup(sourcePath, destinationPath, options = {}) {
  const fullSourcePath = path.resolve(sourcePath);
  const fullDestinationPath = path.resolve(destinationPath);
  const sourceIdentityPath = process.platform === "win32"
    ? fullSourcePath.toLowerCase()
    : fullSourcePath;
  const destinationIdentityPath = process.platform === "win32"
    ? fullDestinationPath.toLowerCase()
    : fullDestinationPath;
  if (sourceIdentityPath === destinationIdentityPath) {
    throw new Error("SQLite online restore source must differ from the destination database.");
  }

  let source;
  try {
    const sourceStat = await fs.stat(fullSourcePath);
    if (!sourceStat.isFile()) {
      throw new Error(`SQLite restore source is not a regular file: ${fullSourcePath}`);
    }

    // Open and inspect the source before SQLite is allowed to touch the live
    // destination. readOnly also closes the disappearance race without ever
    // creating an empty source database.
    source = await openDatabase(fullSourcePath, { readOnly: true });
    setBusyTimeout(source, options.busyTimeoutMs);
    readSqliteConnectionMetadata(source);

    await fs.mkdir(path.dirname(fullDestinationPath), { recursive: true });
    await source.backup(fullDestinationPath, options.backupOptions ?? {});

    await syncDirectory(path.dirname(fullDestinationPath));
    return {
      sourcePath: fullSourcePath,
      destinationPath: fullDestinationPath,
      driver: source.driver ?? "unknown"
    };
  } catch (error) {
    // Never delete or replace destination artifacts here. SQLite owns the
    // destination transaction and rolls it back on an unfinished backup.
    throw wrapSqliteMalformedError(
      wrapSqliteBusyError(error, "restore a consistent SQLite online backup"),
      "restore a consistent SQLite online backup"
    );
  } finally {
    source?.close();
  }
}
