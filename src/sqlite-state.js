import fs from "node:fs/promises";
import path from "node:path";

import { DB_FILE_BASENAME, SESSION_DIRS, SQLITE_DIR_BASENAME } from "./constants.js";
import { openDatabase } from "./sqlite.js";

const DEFAULT_BUSY_TIMEOUT_MS = 5000;

export function stateDbPath(codexHome) {
  return path.join(codexHome, SQLITE_DIR_BASENAME, DB_FILE_BASENAME);
}

export function legacyStateDbPath(codexHome) {
  return path.join(codexHome, DB_FILE_BASENAME);
}

export function stateDbCandidates(codexHome) {
  return [
    {
      path: stateDbPath(codexHome),
      relativePath: path.join(SQLITE_DIR_BASENAME, DB_FILE_BASENAME),
      source: "sqlite-dir",
      priority: 0
    },
    {
      path: legacyStateDbPath(codexHome),
      relativePath: DB_FILE_BASENAME,
      source: "legacy-root",
      priority: 1
    }
  ];
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
    return db.prepare("SELECT COALESCE(MAX(updated_at_ms), 0) AS value FROM threads").get().value ?? 0;
  }
  if (tableHasColumn(db, "threads", "updated_at")) {
    return (db.prepare("SELECT COALESCE(MAX(updated_at), 0) AS value FROM threads").get().value ?? 0) * 1000;
  }
  if (tableHasColumn(db, "threads", "created_at_ms")) {
    return db.prepare("SELECT COALESCE(MAX(created_at_ms), 0) AS value FROM threads").get().value ?? 0;
  }
  if (tableHasColumn(db, "threads", "created_at")) {
    return (db.prepare("SELECT COALESCE(MAX(created_at), 0) AS value FROM threads").get().value ?? 0) * 1000;
  }
  return 0;
}

async function readStateDbCandidateStats(candidate) {
  let db;
  try {
    db = await openDatabase(candidate.path);
    if (!tableExists(db, "threads")) {
      throw new Error("threads table not found");
    }
    const threadCount = db.prepare("SELECT COUNT(*) AS count FROM threads").get().count ?? 0;
    return {
      ...candidate,
      readable: true,
      threadCount,
      maxThreadTimestampMs: maxThreadTimestampMs(db),
      mtimeMs: (await fs.stat(candidate.path)).mtimeMs
    };
  } finally {
    db?.close();
  }
}

function compareStateDbCandidates(a, b) {
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

export async function detectStateDb(codexHome) {
  const existingCandidates = [];
  for (const candidate of stateDbCandidates(codexHome)) {
    try {
      await fs.access(candidate.path);
      existingCandidates.push(candidate);
    } catch {
      // Try the next known Codex state DB location.
    }
  }
  if (existingCandidates.length === 0) {
    return null;
  }

  const rolloutCount = await countRolloutFiles(codexHome);
  const readableCandidates = [];
  for (const candidate of existingCandidates) {
    try {
      const candidateWithStats = await readStateDbCandidateStats(candidate);
      readableCandidates.push({
        ...candidateWithStats,
        rolloutDistance: rolloutCount > 0 ? Math.abs(candidateWithStats.threadCount - rolloutCount) : 0
      });
    } catch {
      // Keep malformed/unreadable candidates as a fallback so existing error
      // reporting still points at state_5.sqlite when no usable DB exists.
    }
  }

  if (readableCandidates.length === 0) {
    return existingCandidates[0];
  }

  return readableCandidates.sort(compareStateDbCandidates)[0];
}

export async function existingStateDbPath(codexHome) {
  return (await detectStateDb(codexHome))?.path ?? null;
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

function isSqliteBusyError(error) {
  const message = `${error?.code ?? ""} ${error?.message ?? ""}`.toLowerCase();
  return message.includes("database is locked") || message.includes("sqlite_busy") || message.includes("busy");
}

function isSqliteMalformedError(error) {
  const message = `${error?.code ?? ""} ${error?.message ?? ""}`.toLowerCase();
  return message.includes("database disk image is malformed")
    || message.includes("sqlite_corrupt")
    || message.includes("malformed")
    || message.includes("not a database");
}

export function wrapSqliteBusyError(error, action) {
  if (!isSqliteBusyError(error)) {
    return error;
  }
  return new Error(
    `Unable to ${action} because state_5.sqlite is currently in use. Close Codex and the Codex app, then retry. Original error: ${error.message}`
  );
}

export function wrapSqliteMalformedError(error, action) {
  if (!isSqliteMalformedError(error)) {
    return error;
  }
  return new Error(
    `Unable to ${action} because state_5.sqlite is malformed or unreadable. Close Codex, back up or repair the database, then retry. Original error: ${error.message}`
  );
}

export async function readSqliteProviderCounts(codexHome) {
  const dbPath = await existingStateDbPath(codexHome);
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

export async function readSqliteRepairStats(codexHome, options = {}) {
  const dbPath = await existingStateDbPath(codexHome);
  if (!dbPath) {
    return null;
  }

  let db;
  try {
    db = await openDatabase(dbPath);
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

export async function assertSqliteWritable(codexHome, options = {}) {
  const dbPath = await existingStateDbPath(codexHome);
  if (!dbPath) {
    return { databasePresent: false };
  }

  let db;
  try {
    db = await openDatabase(dbPath);
    setBusyTimeout(db, options.busyTimeoutMs);
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

export async function updateSqliteProvider(codexHome, targetProvider, afterUpdateOrOptions, maybeOptions) {
  const afterUpdate = typeof afterUpdateOrOptions === "function" ? afterUpdateOrOptions : null;
  const options = typeof afterUpdateOrOptions === "function"
    ? (maybeOptions ?? {})
    : (afterUpdateOrOptions ?? {});

  const dbPath = await existingStateDbPath(codexHome);
  if (!dbPath) {
    if (afterUpdate) {
      await afterUpdate({
        updatedRows: 0,
        providerRowsUpdated: 0,
        userEventRowsUpdated: 0,
        cwdRowsUpdated: 0,
        databasePresent: false
      });
    }
    return {
      updatedRows: 0,
      providerRowsUpdated: 0,
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
    db.exec("BEGIN IMMEDIATE");
    transactionOpen = true;
    const stmt = db.prepare(`
      UPDATE threads
      SET model_provider = ?
      WHERE COALESCE(model_provider, '') <> ?
    `);
    const result = stmt.run(targetProvider, targetProvider);
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
    const updatedRows = (result.changes ?? 0) + userEventUpdatedRows + cwdUpdatedRows;
    if (afterUpdate) {
      await afterUpdate({
        updatedRows,
        providerRowsUpdated: result.changes ?? 0,
        userEventRowsUpdated: userEventUpdatedRows,
        cwdRowsUpdated: cwdUpdatedRows,
        databasePresent: true
      });
    }
    db.exec("COMMIT");
    transactionOpen = false;
    return {
      updatedRows,
      providerRowsUpdated: result.changes ?? 0,
      userEventRowsUpdated: userEventUpdatedRows,
      cwdRowsUpdated: cwdUpdatedRows,
      databasePresent: true
    };
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
