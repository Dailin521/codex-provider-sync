import fs from "node:fs/promises";
import path from "node:path";

import {
  DEFAULT_BACKUP_RETENTION_COUNT,
  DEFAULT_PROVIDER,
  defaultBackupRoot,
  defaultCodexHome
} from "./constants.js";
import {
  configDeclaresProvider,
  listConfiguredProviderIds,
  readConfigText,
  readCurrentProviderFromConfigText,
  setRootProviderInConfigText,
  writeConfigText
} from "./config-file.js";
import {
  createBackup,
  getBackupSummary,
  pruneBackups,
  restoreBackup,
  updateSessionBackupManifest
} from "./backup.js";
import { acquireLock } from "./locking.js";
import {
  applySessionChanges,
  collectSessionFilesByThreadIds,
  collectSessionChanges,
  deleteSessionFiles,
  restoreSessionChanges,
  splitLockedSessionChanges,
  summarizeProviderCounts
} from "./session-files.js";
import {
  assertSqliteWritable,
  deleteSqliteThreadsByIds,
  listSqliteThreadsByIds,
  readSqliteProviderCounts,
  updateSqliteProvider
} from "./sqlite-state.js";

function normalizeCodexHome(explicitCodexHome) {
  return path.resolve(explicitCodexHome ?? process.env.CODEX_HOME ?? defaultCodexHome());
}

async function ensureCodexHome(codexHome) {
  await fs.access(codexHome);
}

function formatCounts(counts) {
  return Object.entries(counts ?? {})
    .map(([provider, count]) => `${provider}: ${count}`)
    .join(", ") || "(none)";
}

function formatBytes(bytes) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  return unitIndex === 0 ? `${bytes} B` : `${value.toFixed(value >= 10 ? 1 : 2).replace(/\.0$/, "")} ${units[unitIndex]}`;
}

function emitProgress(onProgress, event) {
  if (typeof onProgress === "function") {
    onProgress(event);
  }
}

function normalizeSessionIds(sessionIds) {
  const normalized = [];
  const seen = new Set();
  for (const rawValue of sessionIds ?? []) {
    const parts = String(rawValue ?? "").split(",");
    for (const part of parts) {
      const id = part.trim();
      if (!id || seen.has(id)) {
        continue;
      }
      seen.add(id);
      normalized.push(id);
    }
  }
  return normalized;
}

export async function getStatus({ codexHome: explicitCodexHome } = {}) {
  const codexHome = normalizeCodexHome(explicitCodexHome);
  await ensureCodexHome(codexHome);
  const configPath = path.join(codexHome, "config.toml");
  const configText = await readConfigText(configPath);
  const current = readCurrentProviderFromConfigText(configText);
  const configuredProviders = listConfiguredProviderIds(configText);
  const { providerCounts } = await collectSessionChanges(codexHome, "__status_only__");
  const sqliteCounts = await readSqliteProviderCounts(codexHome);
  const backupSummary = await getBackupSummary(codexHome);

  return {
    codexHome,
    currentProvider: current.provider,
    currentProviderImplicit: current.implicit,
    configuredProviders,
    rolloutCounts: summarizeProviderCounts(providerCounts),
    sqliteCounts,
    backupRoot: defaultBackupRoot(codexHome),
    backupSummary
  };
}

export function renderStatus(status) {
  const lines = [
    `Codex home: ${status.codexHome}`,
    `Current provider: ${status.currentProvider}${status.currentProviderImplicit ? " (implicit default)" : ""}`,
    `Configured providers: ${status.configuredProviders.join(", ")}`,
    `Backups: ${status.backupSummary.count} (${formatBytes(status.backupSummary.totalBytes)})`,
    `Backup root: ${status.backupRoot}`
  ];

  lines.push("");
  lines.push("Rollout files:");
  lines.push(`  sessions: ${formatCounts(status.rolloutCounts.sessions)}`);
  lines.push(`  archived_sessions: ${formatCounts(status.rolloutCounts.archived_sessions)}`);

  lines.push("");
  lines.push("SQLite state:");
  if (!status.sqliteCounts) {
    lines.push("  state_5.sqlite not found");
  } else {
    lines.push(`  sessions: ${formatCounts(status.sqliteCounts.sessions)}`);
    lines.push(`  archived_sessions: ${formatCounts(status.sqliteCounts.archived_sessions)}`);
  }

  return lines.join("\n");
}

export async function runSync({
  codexHome: explicitCodexHome,
  provider,
  configBackupText,
  keepCount = DEFAULT_BACKUP_RETENTION_COUNT,
  sqliteBusyTimeoutMs,
  onProgress
} = {}) {
  if (!Number.isInteger(keepCount) || keepCount < 1) {
    throw new Error(`Invalid automatic keep count: ${keepCount}. Expected an integer greater than or equal to 1.`);
  }

  const codexHome = normalizeCodexHome(explicitCodexHome);
  await ensureCodexHome(codexHome);
  const configPath = path.join(codexHome, "config.toml");
  const configText = await readConfigText(configPath);
  const current = readCurrentProviderFromConfigText(configText);
  const targetProvider = provider ?? current.provider ?? DEFAULT_PROVIDER;

  const releaseLock = await acquireLock(codexHome, "sync");
  let backupDir = null;
  let backupDurationMs = 0;
  try {
    emitProgress(onProgress, { stage: "scan_rollout_files", status: "start" });
    const {
      changes,
      lockedPaths: lockedReadPaths,
      providerCounts
    } = await collectSessionChanges(codexHome, targetProvider, { skipLockedReads: true });
    emitProgress(onProgress, {
      stage: "scan_rollout_files",
      status: "complete",
      scannedChanges: changes.length,
      lockedReadCount: lockedReadPaths.length
    });

    emitProgress(onProgress, { stage: "check_locked_rollout_files", status: "start" });
    const {
      writableChanges,
      lockedChanges
    } = await splitLockedSessionChanges(changes);
    emitProgress(onProgress, {
      stage: "check_locked_rollout_files",
      status: "complete",
      writableCount: writableChanges.length,
      lockedCount: lockedChanges.length + lockedReadPaths.length
    });

    const skippedRolloutFiles = [...new Set([
      ...lockedReadPaths,
      ...lockedChanges.map((change) => change.path)
    ])].sort((left, right) => left.localeCompare(right));
    await assertSqliteWritable(codexHome, { busyTimeoutMs: sqliteBusyTimeoutMs });

    emitProgress(onProgress, {
      stage: "create_backup",
      status: "start",
      writableCount: writableChanges.length
    });
    const backupStartedAt = Date.now();
    backupDir = await createBackup({
      codexHome,
      targetProvider,
      sessionChanges: writableChanges,
      configPath,
      configBackupText
    });
    backupDurationMs = Date.now() - backupStartedAt;
    emitProgress(onProgress, {
      stage: "create_backup",
      status: "complete",
      backupDir,
      durationMs: backupDurationMs
    });

    let sessionRestoreNeeded = false;
    let appliedSessionChanges = [];
    try {
      let applyResult = { appliedChanges: 0, appliedPaths: [], skippedPaths: [] };
      emitProgress(onProgress, { stage: "update_sqlite", status: "start" });
      emitProgress(onProgress, {
        stage: "rewrite_rollout_files",
        status: "start",
        writableCount: writableChanges.length
      });
      const sqliteResult = await updateSqliteProvider(
        codexHome,
        targetProvider,
        async () => {
          if (writableChanges.length === 0) {
            return;
          }
          applyResult = await applySessionChanges(writableChanges);
          const appliedPathSet = new Set(applyResult.appliedPaths ?? []);
          appliedSessionChanges = writableChanges.filter((change) => appliedPathSet.has(change.path));
          sessionRestoreNeeded = appliedSessionChanges.length > 0;
          await updateSessionBackupManifest(backupDir, appliedSessionChanges);
        },
        { busyTimeoutMs: sqliteBusyTimeoutMs }
      );
      emitProgress(onProgress, {
        stage: "rewrite_rollout_files",
        status: "complete",
        appliedChanges: applyResult.appliedChanges,
        skippedChanges: applyResult.skippedPaths.length
      });
      emitProgress(onProgress, {
        stage: "update_sqlite",
        status: "complete",
        updatedRows: sqliteResult.updatedRows
      });
      const skippedLockedRolloutFiles = [...new Set([
        ...skippedRolloutFiles,
        ...applyResult.skippedPaths
      ])].sort((left, right) => left.localeCompare(right));
      let autoPruneResult = null;
      let autoPruneWarning = null;
      emitProgress(onProgress, {
        stage: "clean_backups",
        status: "start",
        keepCount
      });
      try {
        autoPruneResult = await pruneBackups(codexHome, keepCount);
      } catch (pruneError) {
        autoPruneWarning = `Automatic backup cleanup failed: ${pruneError instanceof Error ? pruneError.message : String(pruneError)}`;
      }
      emitProgress(onProgress, {
        stage: "clean_backups",
        status: "complete",
        deletedCount: autoPruneResult?.deletedCount ?? 0,
        warning: autoPruneWarning
      });
      return {
        codexHome,
        targetProvider,
        previousProvider: current.provider,
        backupDir,
        backupDurationMs,
        changedSessionFiles: applyResult.appliedChanges,
        skippedLockedRolloutFiles,
        sqliteRowsUpdated: sqliteResult.updatedRows,
        sqlitePresent: sqliteResult.databasePresent,
        rolloutCountsBefore: summarizeProviderCounts(providerCounts),
        autoPruneResult,
        autoPruneWarning
      };
    } catch (error) {
      if (sessionRestoreNeeded) {
        try {
          await restoreSessionChanges(appliedSessionChanges.map((change) => ({
            path: change.path,
            originalFirstLine: change.originalFirstLine,
            originalSeparator: change.originalSeparator
          })));
        } catch (restoreError) {
          throw new Error(
            `Failed to restore rollout files after sync error. Original error: ${error.message}. Restore error: ${restoreError.message}`
          );
        }
      }
      throw error;
    }
  } finally {
    await releaseLock();
  }
}

export async function runDeleteSessions({
  codexHome: explicitCodexHome,
  sessionIds,
  keepCount = DEFAULT_BACKUP_RETENTION_COUNT,
  onProgress
} = {}) {
  if (!Number.isInteger(keepCount) || keepCount < 1) {
    throw new Error(`Invalid automatic keep count: ${keepCount}. Expected an integer greater than or equal to 1.`);
  }

  const requestedSessionIds = normalizeSessionIds(sessionIds);
  if (requestedSessionIds.length === 0) {
    throw new Error("Missing session id. Usage: codex-provider delete <session-id> [more-session-ids...]");
  }

  const codexHome = normalizeCodexHome(explicitCodexHome);
  await ensureCodexHome(codexHome);
  const configPath = path.join(codexHome, "config.toml");
  const configText = await readConfigText(configPath);
  const current = readCurrentProviderFromConfigText(configText);

  const releaseLock = await acquireLock(codexHome, "delete-sessions");
  let backupDir = null;
  let backupDurationMs = 0;
  try {
    emitProgress(onProgress, { stage: "scan_rollout_files", status: "start" });
    const [fileScan, sqliteThreadResult] = await Promise.all([
      collectSessionFilesByThreadIds(codexHome, requestedSessionIds, { skipLockedReads: true }),
      listSqliteThreadsByIds(codexHome, requestedSessionIds)
    ]);
    emitProgress(onProgress, {
      stage: "scan_rollout_files",
      status: "complete",
      matchedRolloutFiles: fileScan.matches.length,
      sqliteMatches: sqliteThreadResult.threads.length,
      lockedReadCount: fileScan.lockedPaths.length
    });

    const sqliteThreadsById = new Map(sqliteThreadResult.threads.map((row) => [row.id, row]));
    const fileMatchesById = new Map();
    for (const match of fileScan.matches) {
      if (!fileMatchesById.has(match.threadId)) {
        fileMatchesById.set(match.threadId, []);
      }
      fileMatchesById.get(match.threadId).push(match);
    }

    emitProgress(onProgress, { stage: "check_locked_rollout_files", status: "start" });
    const splitInput = fileScan.matches.map((match) => ({
      path: match.path,
      threadId: match.threadId
    }));
    const { writableChanges, lockedChanges } = await splitLockedSessionChanges(splitInput);
    const lockedPathSet = new Set([
      ...fileScan.lockedPaths,
      ...lockedChanges.map((change) => change.path)
    ]);
    const lockedThreadIdSet = new Set(lockedChanges.map((change) => change.threadId));
    const sqlitePathToId = new Map(
      sqliteThreadResult.threads
        .filter((row) => row.rollout_path)
        .map((row) => [path.resolve(row.rollout_path), row.id])
    );
    for (const lockedPath of fileScan.lockedPaths) {
      const matchedThreadId = sqlitePathToId.get(path.resolve(lockedPath));
      if (matchedThreadId) {
        lockedThreadIdSet.add(matchedThreadId);
      }
    }
    emitProgress(onProgress, {
      stage: "check_locked_rollout_files",
      status: "complete",
      writableCount: writableChanges.length,
      lockedCount: lockedPathSet.size
    });

    const eligibleSessionIds = requestedSessionIds.filter((sessionId) => !lockedThreadIdSet.has(sessionId));
    const eligiblePaths = writableChanges.map((change) => change.path);
    const eligiblePathSet = new Set(eligiblePaths);
    const requestedSessionIdSet = new Set(requestedSessionIds);
    const fileMatchedIdSet = new Set(fileScan.matches.map((match) => match.threadId));
    const sqliteMatchedIdSet = new Set(sqliteThreadResult.threads.map((row) => row.id));
    const touchedIdSet = new Set([...fileMatchedIdSet, ...sqliteMatchedIdSet]);
    const missingSessionIds = requestedSessionIds.filter((sessionId) => !touchedIdSet.has(sessionId));
    const skippedLockedSessionIds = requestedSessionIds.filter((sessionId) => lockedThreadIdSet.has(sessionId));
    const deletableSqliteIds = eligibleSessionIds.filter((sessionId) => sqliteMatchedIdSet.has(sessionId));

    if (deletableSqliteIds.length === 0 && eligiblePaths.length === 0) {
      return {
        codexHome,
        currentProvider: current.provider,
        backupDir: null,
        backupDurationMs: 0,
        requestedSessionIds,
        deletedSessionIds: [],
        missingSessionIds,
        skippedLockedSessionIds,
        skippedRolloutSessionIds: [],
        deletedRolloutFiles: 0,
        skippedRolloutFiles: [...new Set(fileScan.lockedPaths)].sort((left, right) => left.localeCompare(right)),
        sqliteRowsDeleted: 0,
        sqlitePresent: sqliteThreadResult.databasePresent,
        autoPruneResult: null,
        autoPruneWarning: null
      };
    }

    emitProgress(onProgress, {
      stage: "create_backup",
      status: "start",
      eligibleSessionCount: eligibleSessionIds.length,
      eligibleRolloutFileCount: eligiblePaths.length
    });
    const backupStartedAt = Date.now();
    backupDir = await createBackup({
      codexHome,
      targetProvider: current.provider,
      sessionChanges: [],
      deletedSessionFiles: eligiblePaths,
      configPath,
      configBackupText: configText
    });
    backupDurationMs = Date.now() - backupStartedAt;
    emitProgress(onProgress, {
      stage: "create_backup",
      status: "complete",
      backupDir,
      durationMs: backupDurationMs
    });

    emitProgress(onProgress, {
      stage: "rewrite_rollout_files",
      status: "start",
      eligibleRolloutFileCount: eligiblePaths.length
    });
    const deletedFileResult = await deleteSessionFiles(eligiblePaths);
    const deletedPathSet = new Set(deletedFileResult.deletedPaths);
    const skippedPathSet = new Set(deletedFileResult.skippedPaths);
    emitProgress(onProgress, {
      stage: "rewrite_rollout_files",
      status: "complete",
      deletedRolloutFiles: deletedFileResult.deletedPaths.length,
      skippedRolloutFiles: deletedFileResult.skippedPaths.length
    });

    const skippedAfterDeleteIds = new Set();
    for (const [threadId, matches] of fileMatchesById.entries()) {
      if (lockedThreadIdSet.has(threadId)) {
        continue;
      }
      for (const match of matches) {
        if (!eligiblePathSet.has(match.path)) {
          continue;
        }
        if (skippedPathSet.has(match.path)) {
          skippedAfterDeleteIds.add(threadId);
        }
      }
    }

    const sqliteDeleteIds = deletableSqliteIds.filter((sessionId) => !skippedAfterDeleteIds.has(sessionId));
    emitProgress(onProgress, {
      stage: "update_sqlite",
      status: "start",
      eligibleSessionCount: sqliteDeleteIds.length
    });
    const sqliteDeleteResult = await deleteSqliteThreadsByIds(codexHome, sqliteDeleteIds);
    emitProgress(onProgress, {
      stage: "update_sqlite",
      status: "complete",
      deletedRows: sqliteDeleteResult.deletedThreads
    });

    let autoPruneResult = null;
    let autoPruneWarning = null;
    emitProgress(onProgress, {
      stage: "clean_backups",
      status: "start",
      keepCount
    });
    try {
      autoPruneResult = await pruneBackups(codexHome, keepCount);
    } catch (pruneError) {
      autoPruneWarning = `Automatic backup cleanup failed: ${pruneError instanceof Error ? pruneError.message : String(pruneError)}`;
    }
    emitProgress(onProgress, {
      stage: "clean_backups",
      status: "complete",
      deletedCount: autoPruneResult?.deletedCount ?? 0,
      warning: autoPruneWarning
    });

    const skippedRolloutSessionIds = requestedSessionIds.filter((sessionId) => skippedAfterDeleteIds.has(sessionId));
    const deletedSessionIdSet = new Set(sqliteDeleteResult.deletedThreadIds);
    for (const threadId of fileMatchedIdSet) {
      if (!requestedSessionIdSet.has(threadId)) {
        continue;
      }
      if (lockedThreadIdSet.has(threadId) || skippedAfterDeleteIds.has(threadId)) {
        continue;
      }
      if (deletedSessionIdSet.has(threadId)) {
        continue;
      }
      const matches = fileMatchesById.get(threadId) ?? [];
      const hasRemaining = matches.some((match) => !deletedPathSet.has(match.path));
      if (!hasRemaining && matches.length > 0) {
        deletedSessionIdSet.add(threadId);
      }
    }

    return {
      codexHome,
      currentProvider: current.provider,
      backupDir,
      backupDurationMs,
      requestedSessionIds,
      deletedSessionIds: [...deletedSessionIdSet].sort((left, right) => left.localeCompare(right)),
      missingSessionIds,
      skippedLockedSessionIds,
      skippedRolloutSessionIds,
      deletedRolloutFiles: deletedFileResult.deletedPaths.length,
      skippedRolloutFiles: [...new Set([
        ...fileScan.lockedPaths,
        ...deletedFileResult.skippedPaths
      ])].sort((left, right) => left.localeCompare(right)),
      sqliteRowsDeleted: sqliteDeleteResult.deletedThreads,
      sqlitePresent: sqliteDeleteResult.databasePresent,
      autoPruneResult,
      autoPruneWarning
    };
  } finally {
    await releaseLock();
  }
}

export async function runSwitch({
  codexHome: explicitCodexHome,
  provider,
  keepCount = DEFAULT_BACKUP_RETENTION_COUNT,
  onProgress
}) {
  if (!provider) {
    throw new Error("Missing provider id. Usage: codex-provider switch <provider-id>");
  }

  const codexHome = normalizeCodexHome(explicitCodexHome);
  await ensureCodexHome(codexHome);
  const configPath = path.join(codexHome, "config.toml");
  const originalConfigText = await readConfigText(configPath);
  if (!configDeclaresProvider(originalConfigText, provider)) {
    throw new Error(`Provider "${provider}" is not available in config.toml. Configure it first or use one of: ${listConfiguredProviderIds(originalConfigText).join(", ")}`);
  }

  const nextConfigText = setRootProviderInConfigText(originalConfigText, provider);
  emitProgress(onProgress, {
    stage: "update_config",
    status: "start",
    provider
  });
  await writeConfigText(configPath, nextConfigText);
  emitProgress(onProgress, {
    stage: "update_config",
    status: "complete",
    provider
  });

  try {
    const syncResult = await runSync({
      codexHome,
      provider,
      configBackupText: originalConfigText,
      keepCount,
      onProgress
    });
    return {
      ...syncResult,
      configUpdated: true
    };
  } catch (error) {
    await writeConfigText(configPath, originalConfigText);
    throw error;
  }
}

export async function runRestore({
  codexHome: explicitCodexHome,
  backupDir
}) {
  if (!backupDir) {
    throw new Error("Missing backup path. Usage: codex-provider restore <backup-dir>");
  }
  const codexHome = normalizeCodexHome(explicitCodexHome);
  await ensureCodexHome(codexHome);
  const releaseLock = await acquireLock(codexHome, "restore");
  try {
    return await restoreBackup(path.resolve(backupDir), codexHome);
  } finally {
    await releaseLock();
  }
}

export async function runPruneBackups({
  codexHome: explicitCodexHome,
  keepCount = DEFAULT_BACKUP_RETENTION_COUNT
} = {}) {
  if (!Number.isInteger(keepCount) || keepCount < 0) {
    throw new Error(`Invalid keep count: ${keepCount}. Expected a non-negative integer.`);
  }

  const codexHome = normalizeCodexHome(explicitCodexHome);
  await ensureCodexHome(codexHome);
  const releaseLock = await acquireLock(codexHome, "prune-backups");
  try {
    return await pruneBackups(codexHome, keepCount);
  } finally {
    await releaseLock();
  }
}
