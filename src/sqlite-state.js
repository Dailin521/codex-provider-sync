import fs from "node:fs/promises";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

import { DB_FILE_BASENAME } from "./constants.js";

const DEFAULT_BUSY_TIMEOUT_MS = 5000;

export function stateDbPath(codexHome) {
  return path.join(codexHome, DB_FILE_BASENAME);
}

function openDatabase(dbPath) {
  return new DatabaseSync(dbPath);
}

function normalizeThreadIds(threadIds) {
  return [...new Set(
    (threadIds ?? [])
      .filter((threadId) => typeof threadId === "string")
      .map((threadId) => threadId.trim())
      .filter(Boolean)
  )];
}

function detectThreadActivityColumn(db) {
  const rows = db.prepare("PRAGMA table_info(threads)").all();
  const columns = new Set(rows.map((row) => row.name));
  if (columns.has("updated_at_ms")) {
    return "updated_at_ms";
  }
  if (columns.has("updated_at")) {
    return "updated_at";
  }
  return null;
}

function toActivityTimestampMs(columnName, value) {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    return null;
  }
  if (columnName === "updated_at_ms") {
    return value;
  }
  if (columnName === "updated_at") {
    return value * 1000;
  }
  return null;
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

export async function readSqliteThreadActivityTimestamps(codexHome, threadIds) {
  const normalizedThreadIds = normalizeThreadIds(threadIds);
  if (normalizedThreadIds.length === 0) {
    return new Map();
  }

  const dbPath = stateDbPath(codexHome);
  try {
    await fs.access(dbPath);
  } catch {
    return new Map();
  }

  const db = openDatabase(dbPath);
  try {
    const activityColumn = detectThreadActivityColumn(db);
    if (!activityColumn) {
      return new Map();
    }

    const result = new Map();
    const chunkSize = 500;
    for (let index = 0; index < normalizedThreadIds.length; index += chunkSize) {
      const chunk = normalizedThreadIds.slice(index, index + chunkSize);
      const placeholders = chunk.map(() => "?").join(", ");
      const rows = db.prepare(`
        SELECT id, ${activityColumn} AS activity_value
        FROM threads
        WHERE id IN (${placeholders})
      `).all(...chunk);

      for (const row of rows) {
        const timestampMs = toActivityTimestampMs(activityColumn, row.activity_value);
        if (Number.isFinite(timestampMs)) {
          result.set(row.id, timestampMs);
        }
      }
    }

    return result;
  } catch {
    return new Map();
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
