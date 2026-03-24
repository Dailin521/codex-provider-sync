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
  collectSessionChanges,
  restoreSessionChanges,
  splitLockedSessionChanges,
  summarizeProviderCounts
} from "./session-files.js";
import {
  assertSqliteWritable,
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
  sqliteBusyTimeoutMs
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
  try {
    const {
      changes,
      lockedPaths: lockedReadPaths,
      providerCounts
    } = await collectSessionChanges(codexHome, targetProvider, { skipLockedReads: true });
    const {
      writableChanges,
      lockedChanges
    } = await splitLockedSessionChanges(changes);
    const skippedRolloutFiles = [...new Set([
      ...lockedReadPaths,
      ...lockedChanges.map((change) => change.path)
    ])].sort((left, right) => left.localeCompare(right));
    await assertSqliteWritable(codexHome, { busyTimeoutMs: sqliteBusyTimeoutMs });
    backupDir = await createBackup({
      codexHome,
      targetProvider,
      sessionChanges: writableChanges,
      configPath,
      configBackupText
    });

    let sessionRestoreNeeded = false;
    let appliedSessionChanges = [];
    try {
      let applyResult = { appliedChanges: 0, appliedPaths: [], skippedPaths: [] };
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
      const skippedLockedRolloutFiles = [...new Set([
        ...skippedRolloutFiles,
        ...applyResult.skippedPaths
      ])].sort((left, right) => left.localeCompare(right));
      let autoPruneResult = null;
      let autoPruneWarning = null;
      try {
        autoPruneResult = await pruneBackups(codexHome, keepCount);
      } catch (pruneError) {
        autoPruneWarning = `Automatic backup cleanup failed: ${pruneError instanceof Error ? pruneError.message : String(pruneError)}`;
      }
      return {
        codexHome,
        targetProvider,
        previousProvider: current.provider,
        backupDir,
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

export async function runSwitch({
  codexHome: explicitCodexHome,
  provider,
  keepCount = DEFAULT_BACKUP_RETENTION_COUNT
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
  await writeConfigText(configPath, nextConfigText);

  try {
    const syncResult = await runSync({
      codexHome,
      provider,
      configBackupText: originalConfigText,
      keepCount
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
