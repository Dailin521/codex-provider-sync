import fs from "node:fs/promises";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

import { DB_FILE_BASENAME } from "./constants.js";

const DEFAULT_BUSY_TIMEOUT_MS = 5000;
const SQLITE_IN_CLAUSE_CHUNK_SIZE = 400;

export function stateDbPath(codexHome) {
  return path.join(codexHome, DB_FILE_BASENAME);
}

function openDatabase(dbPath) {
  return new DatabaseSync(dbPath);
}

function normalizeBusyTimeoutMs(busyTimeoutMs) {
  return Number.isInteger(busyTimeoutMs) && busyTimeoutMs >= 0
    ? busyTimeoutMs
    : DEFAULT_BUSY_TIMEOUT_MS;
}

function setBusyTimeout(db, busyTimeoutMs) {
  db.exec(`PRAGMA busy_timeout = ${normalizeBusyTimeoutMs(busyTimeoutMs)}`);
}

function chunkArray(values, chunkSize = SQLITE_IN_CLAUSE_CHUNK_SIZE) {
  const chunks = [];
  for (let index = 0; index < values.length; index += chunkSize) {
    chunks.push(values.slice(index, index + chunkSize));
  }
  return chunks;
}

function uniqueNonEmptyStrings(values) {
  const result = [];
  const seen = new Set();
  for (const value of values ?? []) {
    const normalized = String(value ?? "").trim();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    result.push(normalized);
  }
  return result;
}

function makeInClause(chunkLength) {
  return new Array(chunkLength).fill("?").join(", ");
}

function isSqliteBusyError(error) {
  const message = `${error?.code ?? ""} ${error?.message ?? ""}`.toLowerCase();
  return message.includes("database is locked") || message.includes("sqlite_busy") || message.includes("busy");
}

function wrapSqliteBusyError(error, action) {
  if (!isSqliteBusyError(error)) {
    return error;
  }
  return new Error(
    `Unable to ${action} because state_5.sqlite is currently in use. Close Codex and the Codex app, then retry. Original error: ${error.message}`
  );
}

export async function readSqliteProviderCounts(codexHome) {
  const dbPath = stateDbPath(codexHome);
  try {
    await fs.access(dbPath);
  } catch {
    return null;
  }

  const db = openDatabase(dbPath);
  try {
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
  } finally {
    db.close();
  }
}

export async function assertSqliteWritable(codexHome, options = {}) {
  const dbPath = stateDbPath(codexHome);
  try {
    await fs.access(dbPath);
  } catch {
    return { databasePresent: false };
  }

  const db = openDatabase(dbPath);
  try {
    setBusyTimeout(db, options.busyTimeoutMs);
    db.exec("BEGIN IMMEDIATE");
    db.exec("ROLLBACK");
    return { databasePresent: true };
  } catch (error) {
    throw wrapSqliteBusyError(error, "update session provider metadata");
  } finally {
    db.close();
  }
}

export async function updateSqliteProvider(codexHome, targetProvider, afterUpdateOrOptions, maybeOptions) {
  const afterUpdate = typeof afterUpdateOrOptions === "function" ? afterUpdateOrOptions : null;
  const options = typeof afterUpdateOrOptions === "function"
    ? (maybeOptions ?? {})
    : (afterUpdateOrOptions ?? {});

  const dbPath = stateDbPath(codexHome);
  try {
    await fs.access(dbPath);
  } catch {
    if (afterUpdate) {
      await afterUpdate({ updatedRows: 0, databasePresent: false });
    }
    return { updatedRows: 0, databasePresent: false };
  }

  const db = openDatabase(dbPath);
  let transactionOpen = false;
  try {
    setBusyTimeout(db, options.busyTimeoutMs);
    db.exec("BEGIN IMMEDIATE");
    transactionOpen = true;
    const stmt = db.prepare(`
      UPDATE threads
      SET model_provider = ?
      WHERE COALESCE(model_provider, '') <> ?
    `);
    const result = stmt.run(targetProvider, targetProvider);
    if (afterUpdate) {
      await afterUpdate({
        updatedRows: result.changes ?? 0,
        databasePresent: true
      });
    }
    db.exec("COMMIT");
    transactionOpen = false;
    return { updatedRows: result.changes ?? 0, databasePresent: true };
  } catch (error) {
    if (transactionOpen) {
      try {
        db.exec("ROLLBACK");
      } catch {
        // Ignore rollback failures and surface the original error.
      }
    }
    throw wrapSqliteBusyError(error, "update session provider metadata");
  } finally {
    db.close();
  }
}

export async function listSqliteThreadsByIds(codexHome, sessionIds, options = {}) {
  const normalizedIds = uniqueNonEmptyStrings(sessionIds);
  if (normalizedIds.length === 0) {
    return { databasePresent: false, threads: [] };
  }

  const dbPath = stateDbPath(codexHome);
  try {
    await fs.access(dbPath);
  } catch {
    return { databasePresent: false, threads: [] };
  }

  const db = openDatabase(dbPath);
  try {
    setBusyTimeout(db, options.busyTimeoutMs);
    const rows = [];
    for (const chunk of chunkArray(normalizedIds)) {
      const stmt = db.prepare(`
        SELECT
          id,
          rollout_path,
          archived,
          model_provider
        FROM threads
        WHERE id IN (${makeInClause(chunk.length)})
      `);
      rows.push(...stmt.all(...chunk));
    }
    return {
      databasePresent: true,
      threads: rows
    };
  } catch (error) {
    throw wrapSqliteBusyError(error, "read session metadata");
  } finally {
    db.close();
  }
}

export async function deleteSqliteThreadsByIds(codexHome, sessionIds, options = {}) {
  const normalizedIds = uniqueNonEmptyStrings(sessionIds);
  if (normalizedIds.length === 0) {
    return {
      databasePresent: false,
      deletedThreads: 0,
      deletedThreadIds: []
    };
  }

  const dbPath = stateDbPath(codexHome);
  try {
    await fs.access(dbPath);
  } catch {
    return {
      databasePresent: false,
      deletedThreads: 0,
      deletedThreadIds: []
    };
  }

  const db = openDatabase(dbPath);
  let transactionOpen = false;
  try {
    setBusyTimeout(db, options.busyTimeoutMs);
    db.exec("BEGIN IMMEDIATE");
    transactionOpen = true;

    const deletedThreadIds = [];
    for (const chunk of chunkArray(normalizedIds)) {
      const selectStmt = db.prepare(`
        SELECT id
        FROM threads
        WHERE id IN (${makeInClause(chunk.length)})
      `);
      const selectedRows = selectStmt.all(...chunk);
      deletedThreadIds.push(...selectedRows.map((row) => row.id));
    }

    if (deletedThreadIds.length > 0) {
      for (const chunk of chunkArray(deletedThreadIds)) {
        const inClause = makeInClause(chunk.length);
        db.prepare(`
          DELETE FROM thread_dynamic_tools
          WHERE thread_id IN (${inClause})
        `).run(...chunk);
        db.prepare(`
          DELETE FROM thread_spawn_edges
          WHERE child_thread_id IN (${inClause})
             OR parent_thread_id IN (${inClause})
        `).run(...chunk, ...chunk);
        db.prepare(`
          DELETE FROM threads
          WHERE id IN (${inClause})
        `).run(...chunk);
      }
    }

    db.exec("COMMIT");
    transactionOpen = false;
    return {
      databasePresent: true,
      deletedThreads: deletedThreadIds.length,
      deletedThreadIds
    };
  } catch (error) {
    if (transactionOpen) {
      try {
        db.exec("ROLLBACK");
      } catch {
        // Ignore rollback failures and surface the original error.
      }
    }
    throw wrapSqliteBusyError(error, "delete sessions from SQLite");
  } finally {
    db.close();
  }
}
