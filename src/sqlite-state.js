import fs from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";

import { DB_FILE_BASENAME, SQLITE_DIR_BASENAME } from "./constants.js";
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
      source: "sqlite-dir"
    },
    {
      path: legacyStateDbPath(codexHome),
      relativePath: DB_FILE_BASENAME,
      source: "legacy-root"
    }
  ];
}

export async function detectStateDb(codexHome) {
  for (const candidate of stateDbCandidates(codexHome)) {
    try {
      await fs.access(candidate.path);
      return candidate;
    } catch {
      // Try the next known Codex state DB location.
    }
  }
  return null;
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
  // Walk every candidate database, not just the first one. The
  // newer Codex layout puts the DB in `<home>/sqlite/state_5.sqlite`,
  // but the legacy root DB at `<home>/state_5.sqlite` is still kept
  // around by the Codex App for older project sessions. Summing
  // counts from both locations gives an accurate distribution for
  // `status` reports; without this, the user sees half the picture
  // when both databases coexist.
  const candidates = stateDbCandidates(codexHome).filter((c) => existsSync(c.path));
  if (candidates.length === 0) {
    return null;
  }

  const aggregated = {
    sessions: {},
    archived_sessions: {}
  };
  let sawUnreadable = false;
  let unreadableMessage = null;

  for (const candidate of candidates) {
    let db;
    try {
      db = await openDatabase(candidate.path);
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
      for (const row of rows) {
        const bucket = row.archived ? aggregated.archived_sessions : aggregated.sessions;
        bucket[row.model_provider] = (bucket[row.model_provider] ?? 0) + row.count;
      }
    } catch (error) {
      if (isSqliteMalformedError(error)) {
        sawUnreadable = true;
        unreadableMessage = "state_5.sqlite is malformed or unreadable";
      } else if (isSqliteBusyError(error)) {
        sawUnreadable = true;
        unreadableMessage = "state_5.sqlite is currently in use";
      } else {
        throw error;
      }
    } finally {
      db?.close();
    }
  }

  if (sawUnreadable) {
    return {
      sessions: aggregated.sessions,
      archived_sessions: aggregated.archived_sessions,
      unreadable: true,
      error: unreadableMessage
    };
  }
  return aggregated;
}

export async function readSqliteRepairStats(codexHome, options = {}) {
  // Same multi-DB story as readSqliteProviderCounts: aggregate
  // repair diagnostics across both candidate databases so the
  // CLI can flag every thread that needs attention, not just the
  // ones in the database the user happens to open first.
  const candidates = stateDbCandidates(codexHome).filter((c) => existsSync(c.path));
  if (candidates.length === 0) {
    return null;
  }

  let userEventRowsNeedingRepair = 0;
  let cwdRowsNeedingRepair = 0;

  for (const candidate of candidates) {
    let db;
    try {
      db = await openDatabase(candidate.path);
      if (tableHasColumn(db, "threads", "has_user_event") && options.userEventThreadIds?.size) {
        const stmt = db.prepare("SELECT has_user_event FROM threads WHERE id = ?");
        for (const threadId of options.userEventThreadIds) {
          const row = stmt.get(threadId);
          if (row && Number(row.has_user_event) !== 1) {
            userEventRowsNeedingRepair += 1;
          }
        }
      }

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
    } catch (error) {
      throw wrapSqliteMalformedError(
        wrapSqliteBusyError(error, "read SQLite repair diagnostics"),
        "read SQLite repair diagnostics"
      );
    } finally {
      db?.close();
    }
  }

  return {
    userEventRowsNeedingRepair,
    cwdRowsNeedingRepair
  };
}

export async function assertSqliteWritable(codexHome, options = {}) {
  // The writable check must cover every candidate database
  // too — otherwise the user could pass the assert on the new
  // sqlite/state_5.sqlite and then hit BUSY during sync on the
  // legacy root database (or vice versa).
  const candidates = stateDbCandidates(codexHome).filter((c) => existsSync(c.path));
  if (candidates.length === 0) {
    return { databasePresent: false };
  }

  for (const candidate of candidates) {
    let db;
    try {
      db = await openDatabase(candidate.path);
      setBusyTimeout(db, options.busyTimeoutMs);
      db.exec("BEGIN IMMEDIATE");
      db.exec("ROLLBACK");
    } catch (error) {
      throw wrapSqliteMalformedError(
        wrapSqliteBusyError(error, "update session provider metadata"),
        "update session provider metadata"
      );
    } finally {
      db?.close();
    }
  }
  return { databasePresent: true };
}

export async function updateSqliteProvider(codexHome, targetProvider, afterUpdateOrOptions, maybeOptions) {
  const afterUpdate = typeof afterUpdateOrOptions === "function" ? afterUpdateOrOptions : null;
  const options = typeof afterUpdateOrOptions === "function"
    ? (maybeOptions ?? {})
    : (afterUpdateOrOptions ?? {});
  // When provided, the per-thread `model` column is rewritten alongside
  // `model_provider` so old sessions pick up the new active model in
  // the Codex UI's bottom-right label. Pass null to leave the column
  // untouched (legacy behaviour for callers that do not track model).
  const targetModel = options.targetModel ?? null;

  // Walk every candidate database, not just the first one. Codex
  // stores its state database in two locations (`<home>/sqlite/
  // state_5.sqlite` for newer installs and `<home>/state_5.sqlite`
  // for older installs), and the Codex App GUI keeps the legacy
  // root database alive as long as it is on disk — including for
  // the older project sessions it created before the new location
  // was introduced. If we only update the first hit, the GUI keeps
  // reading stale `model_provider` / `model` values for those older
  // sessions and either shows the old provider label or sends
  // requests with the wrong model name.
  const candidates = stateDbCandidates(codexHome).filter((c) => existsSync(c.path));
  if (candidates.length === 0) {
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

  let totalUpdatedRows = 0;
  let totalProviderRowsUpdated = 0;
  let totalUserEventRowsUpdated = 0;
  let totalCwdRowsUpdated = 0;
  let totalDatabasePresent = false;

  for (const candidate of candidates) {
    const result = await updateSingleSqliteDatabase(
      candidate.path,
      targetProvider,
      targetModel,
      options);
    totalUpdatedRows += result.updatedRows;
    totalProviderRowsUpdated += result.providerRowsUpdated;
    totalUserEventRowsUpdated += result.userEventRowsUpdated;
    totalCwdRowsUpdated += result.cwdRowsUpdated;
    totalDatabasePresent = totalDatabasePresent || result.databasePresent;
  }

  if (afterUpdate) {
    // Run the rollout rewrite once, after both candidate databases
    // have been updated. The rewrite is keyed off the session
    // change collection, not the SQLite state, so it does not
    // matter whether the data lives in the new or the legacy
    // database — we only want to do it once, after the SQLite
    // phase is fully done.
    await afterUpdate({
      updatedRows: totalUpdatedRows,
      providerRowsUpdated: totalProviderRowsUpdated,
      userEventRowsUpdated: totalUserEventRowsUpdated,
      cwdRowsUpdated: totalCwdRowsUpdated,
      databasePresent: totalDatabasePresent
    });
  }

  return {
    updatedRows: totalUpdatedRows,
    providerRowsUpdated: totalProviderRowsUpdated,
    userEventRowsUpdated: totalUserEventRowsUpdated,
    cwdRowsUpdated: totalCwdRowsUpdated,
    databasePresent: totalDatabasePresent
  };
}

async function updateSingleSqliteDatabase(dbPath, targetProvider, targetModel, options) {
  let db;
  let transactionOpen = false;
  try {
    db = await openDatabase(dbPath);
    setBusyTimeout(db, options.busyTimeoutMs);
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
    const stmt = db.prepare(wantsModel
      ? `UPDATE threads
         SET model_provider = ?, model = ?
         WHERE COALESCE(model_provider, '') <> ? OR COALESCE(model, '') <> ?`
      : `UPDATE threads
         SET model_provider = ?
         WHERE COALESCE(model_provider, '') <> ?`);
    const result = wantsModel
      ? stmt.run(targetProvider, targetModel, targetProvider, targetModel)
      : stmt.run(targetProvider, targetProvider);
    const providerRowsUpdated = result.changes ?? 0;
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
    const updatedRows = providerRowsUpdated + userEventUpdatedRows + cwdUpdatedRows;
    db.exec("COMMIT");
    transactionOpen = false;
return {
      updatedRows,
      providerRowsUpdated,
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
