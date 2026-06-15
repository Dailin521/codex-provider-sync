import fs from "node:fs/promises";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

import {
  GLOBAL_STATE_BACKUP_FILE_BASENAME,
  GLOBAL_STATE_FILE_BASENAME
} from "./constants.js";
import {
  stateDbPaths,
  wrapSqliteBusyError,
  wrapSqliteMalformedError
} from "./sqlite-state.js";

export function globalStatePath(codexHome) {
  return path.join(codexHome, GLOBAL_STATE_FILE_BASENAME);
}

export function globalStateBackupPath(codexHome) {
  return path.join(codexHome, GLOBAL_STATE_BACKUP_FILE_BASENAME);
}

export function normalizeComparablePath(value) {
  if (typeof value !== "string") {
    return null;
  }
  let normalized = value.trim();
  if (!normalized) {
    return null;
  }
  const extendedUnc = normalized.match(/^\\\\\?\\UNC\\(.+)$/i);
  normalized = extendedUnc ? `\\\\${extendedUnc[1]}` : normalized.replace(/^\\\\\?\\/, "");
  normalized = normalized.replace(/\//g, "\\");
  normalized = normalized.replace(/\\+$/, "");
  if (/^[A-Za-z]:$/.test(normalized)) {
    normalized += "\\";
  }
  return normalized.toLowerCase();
}

function tableHasColumn(db, tableName, columnName) {
  return db
    .prepare(`PRAGMA table_info("${tableName.replaceAll("\"", "\"\"")}")`)
    .all()
    .some((column) => column.name === columnName);
}

function toPathArray(value) {
  if (Array.isArray(value)) {
    return value.filter((entry) => typeof entry === "string" && entry.trim());
  }
  if (typeof value === "string" && value.trim()) {
    return [value];
  }
  return [];
}

function dedupePaths(paths) {
  const seen = new Set();
  const result = [];
  for (const value of paths ?? []) {
    const comparable = normalizeComparablePath(value);
    if (!comparable || seen.has(comparable)) {
      continue;
    }
    seen.add(comparable);
    result.push(value);
  }
  return result;
}

function arraysEqual(left, right) {
  if (left.length !== right.length) {
    return false;
  }
  return left.every((value, index) => value === right[index]);
}

function countArrayChanges(previous, next) {
  const compared = Math.max(previous.length, next.length);
  let changed = 0;
  for (let index = 0; index < compared; index += 1) {
    if ((previous[index] ?? null) !== (next[index] ?? null)) {
      changed += 1;
    }
  }
  return changed;
}

export function toDesktopWorkspacePath(value) {
  if (typeof value !== "string") {
    return value;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return value;
  }

  const extendedUnc = trimmed.match(/^\\\\\?\\UNC\\(.+)$/i);
  if (extendedUnc) {
    return `\\\\${extendedUnc[1]}`.replace(/\//g, "\\");
  }

  const extendedDrive = trimmed.match(/^\\\\\?\\([A-Za-z]:)(?:[\\/](.*))?$/);
  if (extendedDrive) {
    const [, drive, rest] = extendedDrive;
    return rest && rest.length > 0
      ? `${drive}\\${rest.replace(/\//g, "\\")}`
      : `${drive}\\`;
  }

  if (trimmed.startsWith("\\\\?\\")) {
    return trimmed.slice(4).replace(/\//g, "\\");
  }

  return value;
}

function resolveStoredPath(value, cwdStats) {
  const comparable = normalizeComparablePath(value);
  if (!comparable) {
    return value;
  }

  const matches = (cwdStats ?? []).filter((entry) => entry.normalizedCwd === comparable);
  if (matches.length === 0) {
    return toDesktopWorkspacePath(value);
  }

  matches.sort((left, right) => (
    (right.count - left.count)
    || (right.updatedAtMs - left.updatedAtMs)
    || left.cwd.localeCompare(right.cwd)
  ));
  return toDesktopWorkspacePath(matches[0].cwd);
}

function readWorkspaceRootsFromGlobalState(state) {
  const savedRoots = toPathArray(state["electron-saved-workspace-roots"]);
  const projectOrder = toPathArray(state["project-order"]);
  const activeRoots = toPathArray(state["active-workspace-roots"]);
  return dedupePaths(
    (projectOrder.length > 0
      ? [...projectOrder, ...savedRoots, ...activeRoots]
      : [...savedRoots, ...activeRoots]
    ).map(toDesktopWorkspacePath)
  );
}

function copyResolvedObjectKeys(input, cwdStats) {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return input;
  }

  const result = {};
  for (const [key, value] of Object.entries(input)) {
    const resolved = resolveStoredPath(key, cwdStats);
    if (result[resolved] === undefined || resolved === key) {
      result[resolved] = value;
    }
  }
  return result;
}

export async function readThreadCwdStats(codexHome) {
  const dbPath = (await stateDbPaths(codexHome)).at(-1);
  if (!dbPath) {
    return [];
  }

  let db;
  try {
    db = new DatabaseSync(dbPath, { readOnly: true });
    if (!tableHasColumn(db, "threads", "cwd")) {
      return [];
    }
    const updatedAtExpression = tableHasColumn(db, "threads", "updated_at_ms")
      ? (tableHasColumn(db, "threads", "updated_at")
          ? "COALESCE(MAX(updated_at_ms), MAX(updated_at) * 1000, 0)"
          : "COALESCE(MAX(updated_at_ms), 0)")
      : (tableHasColumn(db, "threads", "updated_at")
          ? "COALESCE(MAX(updated_at) * 1000, 0)"
          : "0");
    const rows = db.prepare(`
      SELECT
        cwd,
        COUNT(*) AS count,
        ${updatedAtExpression} AS updated_at_ms
      FROM threads
      WHERE cwd IS NOT NULL AND cwd <> ''
      GROUP BY cwd
      ORDER BY count DESC, updated_at_ms DESC, cwd
    `).all();

    return rows
      .filter((row) => typeof row.cwd === "string" && row.cwd)
      .map((row) => ({
        cwd: row.cwd,
        normalizedCwd: normalizeComparablePath(row.cwd),
        count: Number(row.count) || 0,
        updatedAtMs: Number(row.updated_at_ms) || 0
      }))
      .filter((row) => row.normalizedCwd);
  } catch (error) {
    throw wrapSqliteMalformedError(
      wrapSqliteBusyError(error, "update session provider metadata"),
      "update session provider metadata"
    );
  } finally {
    db?.close();
  }
}

function buildTimeExpression(columns) {
  const expressions = [];
  if (columns.has("updated_at_ms")) {
    expressions.push("updated_at_ms");
  }
  if (columns.has("updated_at")) {
    expressions.push("updated_at * 1000");
  }
  if (columns.has("created_at_ms")) {
    expressions.push("created_at_ms");
  }
  if (columns.has("created_at")) {
    expressions.push("created_at * 1000");
  }
  expressions.push("0");
  return `COALESCE(${expressions.join(", ")})`;
}

function formatRankPreview(ranks, maxCount = 12) {
  const preview = ranks.slice(0, maxCount).join(", ");
  const remaining = ranks.length - Math.min(ranks.length, maxCount);
  return remaining > 0 ? `${preview} (+${remaining} more)` : preview;
}

export async function readProjectThreadVisibility(codexHome, options = {}) {
  const pageSize = Number.isInteger(options.pageSize) && options.pageSize > 0
    ? options.pageSize
    : 50;
  const filePath = globalStatePath(codexHome);
  let state;
  try {
    state = JSON.parse(await fs.readFile(filePath, "utf8"));
  } catch (error) {
    if (error?.code === "ENOENT") {
      return [];
    }
    throw error;
  }

  const roots = readWorkspaceRootsFromGlobalState(state);
  if (roots.length === 0) {
    return [];
  }

  const dbPath = (await stateDbPaths(codexHome)).at(-1);
  if (!dbPath) {
    return roots.map((root) => ({
      root,
      interactiveThreads: 0,
      firstPageThreads: 0,
      exactCwdMatches: 0,
      verbatimCwdRows: 0,
      ranks: [],
      rankPreview: "",
      providerCounts: {}
    }));
  }

  let db;
  try {
    db = new DatabaseSync(dbPath, { readOnly: true });
    const columns = new Set(db.prepare('PRAGMA table_info("threads")').all().map((column) => column.name));
    if (!columns.has("cwd")) {
      return [];
    }

    const sourceFilter = columns.has("source")
      ? "AND source IN ('cli', 'vscode')"
      : "";
    const archivedFilter = columns.has("archived")
      ? "AND archived = 0"
      : "";
    const firstUserFilter = columns.has("first_user_message")
      ? "AND first_user_message <> ''"
      : "";
    const timeExpression = buildTimeExpression(columns);
    const providerExpression = columns.has("model_provider")
      ? "model_provider"
      : "'' AS model_provider";

    const rows = db.prepare(`
      SELECT
        id,
        cwd,
        ${providerExpression},
        ${timeExpression} AS sort_ts
      FROM threads
      WHERE cwd IS NOT NULL AND cwd <> ''
        ${archivedFilter}
        ${firstUserFilter}
        ${sourceFilter}
      ORDER BY sort_ts DESC, id DESC
    `).all();

    const rankedRows = rows.map((row, index) => ({
      ...row,
      rank: index + 1,
      normalizedCwd: normalizeComparablePath(row.cwd),
      desktopCwd: toDesktopWorkspacePath(row.cwd)
    }));

    return roots.map((root) => {
      const normalizedRoot = normalizeComparablePath(root);
      const exactRoot = toDesktopWorkspacePath(root);
      const matchingRows = rankedRows.filter((row) => row.normalizedCwd === normalizedRoot);
      const ranks = matchingRows.map((row) => row.rank);
      const providerCounts = {};
      let exactCwdMatches = 0;
      let verbatimCwdRows = 0;
      for (const row of matchingRows) {
        const provider = row.model_provider || "(missing)";
        providerCounts[provider] = (providerCounts[provider] ?? 0) + 1;
        if (row.cwd === exactRoot || row.desktopCwd === exactRoot) {
          exactCwdMatches += row.cwd === exactRoot ? 1 : 0;
        }
        if (typeof row.cwd === "string" && row.cwd.startsWith("\\\\?\\")) {
          verbatimCwdRows += 1;
        }
      }
      return {
        root: exactRoot,
        interactiveThreads: matchingRows.length,
        firstPageThreads: ranks.filter((rank) => rank <= pageSize).length,
        exactCwdMatches,
        verbatimCwdRows,
        topRank: ranks[0] ?? null,
        ranks,
        rankPreview: formatRankPreview(ranks),
        providerCounts
      };
    });
  } catch (error) {
    throw wrapSqliteMalformedError(
      wrapSqliteBusyError(error, "read project thread visibility diagnostics"),
      "read project thread visibility diagnostics"
    );
  } finally {
    db?.close();
  }
}

export async function syncWorkspaceRoots(codexHome, options = {}) {
  const filePath = globalStatePath(codexHome);
  const backupPath = globalStateBackupPath(codexHome);

  let originalText;
  try {
    originalText = await fs.readFile(filePath, "utf8");
  } catch (error) {
    if (error?.code === "ENOENT") {
      return {
        present: false,
        updated: false,
        updatedWorkspaceRoots: 0,
        savedWorkspaceRootCount: 0
      };
    }
    throw error;
  }

  const state = JSON.parse(originalText);
  const cwdStats = options.cwdStats ?? await readThreadCwdStats(codexHome);
  const existingSavedRoots = toPathArray(state["electron-saved-workspace-roots"]);
  const existingProjectOrder = toPathArray(state["project-order"]);
  const existingActiveRoots = toPathArray(state["active-workspace-roots"]);

  const nextSavedRoots = dedupePaths(
    (existingProjectOrder.length > 0
      ? [...existingProjectOrder, ...existingSavedRoots, ...existingActiveRoots]
      : [...existingSavedRoots, ...existingActiveRoots]
    ).map((value) => resolveStoredPath(value, cwdStats))
  );
  const nextProjectOrder = dedupePaths(
    (existingProjectOrder.length > 0
      ? [...existingProjectOrder, ...existingSavedRoots]
      : [...nextSavedRoots]
    ).map((value) => resolveStoredPath(value, cwdStats))
  );
  const nextActiveRoots = dedupePaths(existingActiveRoots.map((value) => resolveStoredPath(value, cwdStats)));
  const nextLabels = copyResolvedObjectKeys(state["electron-workspace-root-labels"], cwdStats);
  const nextOpenTargets = (
    state["open-in-target-preferences"]
    && typeof state["open-in-target-preferences"] === "object"
    && !Array.isArray(state["open-in-target-preferences"])
  )
    ? {
        ...state["open-in-target-preferences"],
        perPath: copyResolvedObjectKeys(state["open-in-target-preferences"].perPath, cwdStats)
      }
    : state["open-in-target-preferences"];

  const originalActiveValue = state["active-workspace-roots"];
  const nextActiveValue = Array.isArray(originalActiveValue)
    ? nextActiveRoots
    : (nextActiveRoots[0] ?? originalActiveValue);

  const savedRootsChanged = !arraysEqual(existingSavedRoots, nextSavedRoots);
  const projectOrderChanged = !arraysEqual(existingProjectOrder, nextProjectOrder);
  const activeRootsChanged = JSON.stringify(originalActiveValue ?? null) !== JSON.stringify(nextActiveValue ?? null);
  const labelsChanged = JSON.stringify(state["electron-workspace-root-labels"] ?? null) !== JSON.stringify(nextLabels ?? null);
  const openTargetsChanged = JSON.stringify(state["open-in-target-preferences"] ?? null) !== JSON.stringify(nextOpenTargets ?? null);

  state["electron-saved-workspace-roots"] = nextSavedRoots;
  state["project-order"] = nextProjectOrder;
  state["active-workspace-roots"] = nextActiveValue;
  if (nextLabels !== undefined) {
    state["electron-workspace-root-labels"] = nextLabels;
  }
  if (nextOpenTargets !== undefined) {
    state["open-in-target-preferences"] = nextOpenTargets;
  }

  const nextText = `${JSON.stringify(state, null, 2)}\n`;
  const backupMissing = await fs.access(backupPath).then(() => false).catch(() => true);
  const updated = savedRootsChanged || projectOrderChanged || activeRootsChanged || labelsChanged || openTargetsChanged || backupMissing;
  if (updated) {
    await fs.writeFile(filePath, nextText, "utf8");
    await fs.writeFile(backupPath, nextText, "utf8");
  }

  return {
    present: true,
    updated,
    updatedWorkspaceRoots: countArrayChanges(existingSavedRoots, nextSavedRoots),
    savedWorkspaceRootCount: nextSavedRoots.length
  };
}
