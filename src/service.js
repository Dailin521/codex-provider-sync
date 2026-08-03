import path from "node:path";

import {
  DEFAULT_BACKUP_RETENTION_COUNT,
  DEFAULT_PROVIDER,
  defaultBackupRoot
} from "./constants.js";
import {
  configDeclaresProvider,
  listConfiguredProviderIds,
  readConfigText,
  readCurrentProviderFromConfigText,
  readProviderModel,
  readRootModelFromConfigText,
  setRootModelInConfigText,
  setRootProviderInConfigText,
  writeConfigText
} from "./config-file.js";
import {
  createBackup,
  getBackupSummary,
  pruneBackups,
  restoreBackup,
  restoreGlobalStateFilesFromBackup,
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
  detectStateDb,
  readSqliteProviderCounts,
  readSqliteRepairStats,
  updateSqliteProvider
} from "./sqlite-state.js";
import {
  readProjectThreadVisibility,
  readThreadCwdStats,
  syncWorkspaceRoots
} from "./workspace-roots.js";
import {
  assertSqliteAccessSupported,
  ensureCodexHome,
  isConfiguredSqliteHome,
  missingConfiguredStateDbError,
  normalizeCodexHome,
  resolveStorageLayout,
  withStateDbLocation
} from "./storage-layout.js";
import {
  TransactionJournal,
  assertNoPendingTransactions,
  findPendingTransactions,
  markBackupTransactionRolledBack
} from "./transaction-journal.js";

export class SyncTransactionError extends Error {
  constructor(
    originalError,
    rollbackErrors,
    backupDir,
    completedTargets,
    uncompletedTargets,
    { rollbackStatus = "incomplete", recoveryRequired = true } = {}
  ) {
    const message = recoveryRequired
      ? `Failed to restore state after sync error. Original error: ${originalError.message}. Restore error: ${rollbackErrors.join("; ")}`
      : `Provider sync failed and all observed changes were rolled back. Original error: ${originalError.message}`;
    super(message, { cause: originalError });
    this.name = "SyncTransactionError";
    this.code = recoveryRequired ? "RECOVERY_REQUIRED" : "SYNC_FAILED_ROLLED_BACK";
    this.originalError = originalError;
    this.rollbackErrors = rollbackErrors;
    this.backupDir = backupDir;
    this.completedTargets = completedTargets;
    this.uncompletedTargets = uncompletedTargets;
    this.rollbackStatus = rollbackStatus;
    this.recoveryRequired = recoveryRequired;
    this.recoveryInstructions = recoveryRequired
      ? `Restore the managed backup at ${backupDir}, inspect the pending transaction journal, then retry.`
      : "No manual recovery is required. Inspect the original error, correct its cause, and retry.";
  }
}

function throwIfAborted(signal) {
  if (!signal?.aborted) {
    return;
  }
  const error = new Error("The provider-sync operation was cancelled.");
  error.name = "AbortError";
  error.code = "ABORT_ERR";
  throw error;
}

async function prepareStorage({ codexHome: explicitCodexHome, sqliteHome, configText, storage, platform }) {
  if (storage) {
    return storage;
  }
  const codexHome = normalizeCodexHome(explicitCodexHome);
  const layout = resolveStorageLayout({ codexHome, sqliteHome, configText, platform });
  await ensureCodexHome(layout);
  if (!layout.sqliteAccess.supported) {
    return withStateDbLocation(layout, null);
  }
  return withStateDbLocation(layout, await detectStateDb(layout));
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

function sumCounts(counts) {
  return Object.values(counts ?? {}).reduce((total, value) => total + value, 0);
}

function buildEncryptedContentWarning(encryptedContentCounts, targetProvider) {
  const riskyProviders = new Set();
  for (const scope of ["sessions", "archived_sessions"]) {
    for (const [provider, count] of Object.entries(encryptedContentCounts?.[scope] ?? {})) {
      if (count > 0 && provider !== targetProvider) {
        riskyProviders.add(provider);
      }
    }
  }
  const total = sumCounts(encryptedContentCounts?.sessions) + sumCounts(encryptedContentCounts?.archived_sessions);
  if (riskyProviders.size === 0) {
    return null;
  }
  return `Encrypted content warning: ${total} rollout file(s) contain encrypted_content from provider(s) ${[...riskyProviders].sort().join(", ")}. Visibility metadata can be synchronized to ${targetProvider}, but continuing or compacting those histories may fail with invalid_encrypted_content. Return to the original provider/account or start a new session if you need reliable continuation.`;
}

export async function getStatus({ codexHome: explicitCodexHome, sqliteHome, platform } = {}) {
  const codexHome = normalizeCodexHome(explicitCodexHome);
  const configPath = path.join(codexHome, "config.toml");
  const configText = await readConfigText(configPath);
  const storage = await prepareStorage({ codexHome, sqliteHome, configText, platform });
  const current = readCurrentProviderFromConfigText(configText);
  const configuredProviders = listConfiguredProviderIds(configText);
  const {
    providerCounts,
    encryptedContentCounts,
    lockedPaths,
    userEventThreadIds,
    threadCwdById
  } = await collectSessionChanges(codexHome, "__status_only__", { skipLockedReads: true });
  const stateDbLocation = storage.stateDbLocation;
  const sqliteCounts = storage.sqliteAccess.supported
    ? await readSqliteProviderCounts(storage)
    : null;
  const sqliteRepairStats = sqliteCounts && !sqliteCounts.unreadable
    ? await readSqliteRepairStats(storage, { userEventThreadIds, threadCwdById })
    : null;
  const projectThreadVisibility = !storage.sqliteAccess.supported || sqliteCounts?.unreadable
    ? []
    : await readProjectThreadVisibility(storage);
  const backupSummary = await getBackupSummary(codexHome);
  const pendingTransactions = await findPendingTransactions(codexHome);

  return {
    codexHome,
    sqliteHome: storage.sqliteHome,
    sqliteHomeSource: storage.sqliteHomeSource,
    sqliteAccess: storage.sqliteAccess,
    checkedStateDbPaths: storage.stateDbCandidates.map((candidate) => candidate.path),
    currentProvider: current.provider,
    currentProviderImplicit: current.implicit,
    configuredProviders,
    rolloutCounts: summarizeProviderCounts(providerCounts),
    lockedRolloutFiles: lockedPaths,
    encryptedContentCounts,
    encryptedContentWarning: buildEncryptedContentWarning(encryptedContentCounts, current.provider ?? DEFAULT_PROVIDER),
    sqliteCounts,
    stateDbLocation,
    sqliteRepairStats,
    projectThreadVisibility,
    backupRoot: defaultBackupRoot(codexHome),
    backupSummary,
    pendingTransactions: pendingTransactions.map((transaction) => ({
      operationId: transaction.operationId ?? null,
      state: transaction.state,
      backupDir: transaction.backupDir,
      journalPath: transaction.filePath
    }))
  };
}

export function renderStatus(status) {
  const lines = [
    `Codex home: ${status.codexHome}`,
    `SQLite home: ${status.sqliteHome} (source: ${status.sqliteHomeSource})`,
    `Current provider: ${status.currentProvider}${status.currentProviderImplicit ? " (implicit default)" : ""}`,
    `Configured providers: ${status.configuredProviders.join(", ")}`,
    `Backups: ${status.backupSummary.count} (${formatBytes(status.backupSummary.totalBytes)})`,
    `Backup root: ${status.backupRoot}`
  ];

  if (status.pendingTransactions?.length) {
    lines.push("");
    lines.push("Recovery required:");
    for (const transaction of status.pendingTransactions) {
      lines.push(`  ${transaction.state}: ${transaction.backupDir}`);
    }
    lines.push("  Run restore with the listed backup before the next write operation.");
  }

  lines.push("");
  lines.push("Rollout files:");
  lines.push(`  sessions: ${formatCounts(status.rolloutCounts.sessions)}`);
  lines.push(`  archived_sessions: ${formatCounts(status.rolloutCounts.archived_sessions)}`);
  if (status.encryptedContentCounts) {
    lines.push(`  encrypted_content sessions: ${formatCounts(status.encryptedContentCounts.sessions)}`);
    lines.push(`  encrypted_content archived_sessions: ${formatCounts(status.encryptedContentCounts.archived_sessions)}`);
  }
  if (status.encryptedContentWarning) {
    lines.push(`  ${status.encryptedContentWarning}`);
  }
  if (status.lockedRolloutFiles?.length) {
    lines.push(`  Locked rollout files skipped during status scan: ${status.lockedRolloutFiles.length}`);
  }

  lines.push("");
  lines.push("SQLite state:");
  if (!status.sqliteAccess?.supported) {
    lines.push(`  ${status.sqliteAccess.message}`);
    return lines.join("\n");
  }
  if (status.stateDbLocation) {
    const legacyNote = status.stateDbLocation.source === "legacy-root" ? " (legacy root)" : "";
    lines.push(`  database: ${status.stateDbLocation.path}${legacyNote}`);
  } else {
    lines.push(`  database: not found (checked ${status.checkedStateDbPaths.join(", ")})`);
  }
  if (status.sqliteCounts?.unreadable) {
    lines.push(`  ${status.sqliteCounts.error ?? "state_5.sqlite is malformed or unreadable"}`);
  } else if (!status.sqliteCounts) {
    lines.push("  state_5.sqlite not found");
  } else {
    lines.push(`  sessions: ${formatCounts(status.sqliteCounts.sessions)}`);
    lines.push(`  archived_sessions: ${formatCounts(status.sqliteCounts.archived_sessions)}`);
    if (status.sqliteRepairStats?.userEventRowsNeedingRepair) {
      lines.push(`  user-event flags needing repair: ${status.sqliteRepairStats.userEventRowsNeedingRepair}`);
    }
    if (status.sqliteRepairStats?.cwdRowsNeedingRepair) {
      lines.push(`  cwd paths needing repair: ${status.sqliteRepairStats.cwdRowsNeedingRepair}`);
    }
  }

  if (status.projectThreadVisibility?.length) {
    lines.push("");
    lines.push("Project visibility:");
    for (const project of status.projectThreadVisibility) {
      const providers = formatCounts(project.providerCounts);
      const rankText = project.rankPreview || "(none)";
      lines.push(
        `  ${project.root}: interactive ${project.interactiveThreads}, first page ${project.firstPageThreads}/50, ranks ${rankText}, exact cwd ${project.exactCwdMatches}/${project.interactiveThreads}, verbatim cwd ${project.verbatimCwdRows}, providers ${providers}`
      );
    }
  }

  return lines.join("\n");
}

export async function runSync(options = {}) {
  return runSyncCore(options);
}

async function runSyncCore({
  codexHome: explicitCodexHome,
  sqliteHome,
  storage: providedStorage,
  provider,
  configBackupText,
  keepCount = DEFAULT_BACKUP_RETENTION_COUNT,
  sqliteBusyTimeoutMs,
  onProgress,
  model = null,
  platform,
  faultInjector,
  signal
} = {}, { afterBackup } = {}) {
  if (!Number.isInteger(keepCount) || keepCount < 1) {
    throw new Error(`Invalid automatic keep count: ${keepCount}. Expected an integer greater than or equal to 1.`);
  }

  const codexHome = providedStorage?.codexHome ?? normalizeCodexHome(explicitCodexHome);
  const configPath = path.join(codexHome, "config.toml");
  const releaseLock = await acquireLock(codexHome, "sync");
  let backupDir = null;
  let journal = null;
  let backupDurationMs = 0;
  try {
    await assertNoPendingTransactions(codexHome);
    throwIfAborted(signal);
    const configText = await readConfigText(configPath);
    const storage = await prepareStorage({ codexHome, sqliteHome, configText, storage: providedStorage, platform });
    assertSqliteAccessSupported(storage, "sync");
    if (!storage.stateDbLocation && isConfiguredSqliteHome(storage)) {
      throw missingConfiguredStateDbError(storage);
    }
    const current = readCurrentProviderFromConfigText(configText);
    const targetProvider = provider ?? current.provider ?? DEFAULT_PROVIDER;
    emitProgress(onProgress, { stage: "scan_rollout_files", status: "start" });
    const {
      changes,
      lockedPaths: lockedReadPaths,
      providerCounts,
      encryptedContentCounts,
      userEventThreadIds,
      threadCwdById
    } = await collectSessionChanges(codexHome, targetProvider, { skipLockedReads: true, targetModel: model });
    const cwdStats = await readThreadCwdStats(storage);
    const encryptedContentWarning = buildEncryptedContentWarning(encryptedContentCounts, targetProvider);
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
    await assertSqliteWritable(storage, { busyTimeoutMs: sqliteBusyTimeoutMs });

    emitProgress(onProgress, {
      stage: "create_backup",
      status: "start",
      writableCount: writableChanges.length
    });
    throwIfAborted(signal);
    await faultInjector?.({ point: "before_backup" });
    const backupStartedAt = Date.now();
    backupDir = await createBackup({
      storage,
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

    const globalStatePath = path.join(codexHome, ".codex-global-state.json");
    const globalStateBackupPath = path.join(codexHome, ".codex-global-state.json.bak");
    const potentialTargets = [
      ...writableChanges.map((change) => change.path),
      globalStatePath,
      globalStateBackupPath,
      ...(configBackupText !== undefined ? [configPath] : []),
      ...(storage.stateDbCandidates ?? []).map((candidate) => candidate.path)
    ].map((targetPath) => path.resolve(targetPath));
    journal = await TransactionJournal.create(backupDir, {
      codexHome,
      targetProvider,
      potentialTargets
    });

    let sessionRestoreNeeded = false;
    let appliedSessionChanges = [];
    const completedTargets = [];
    const recordCompletedTarget = (targetPath) => {
      const fullPath = path.resolve(targetPath);
      if (!completedTargets.includes(fullPath)) {
        completedTargets.push(fullPath);
      }
    };
    let globalStateRestoreNeeded = false;
    let workspaceRootResult = {
      updated: false,
      updatedWorkspaceRoots: 0,
      savedWorkspaceRootCount: 0
    };
    try {
      if (typeof afterBackup === "function") {
        throwIfAborted(signal);
        await journal.applying("config", configPath);
        await afterBackup(backupDir);
        await journal.applied("config", configPath);
        recordCompletedTarget(configPath);
        await faultInjector?.({ point: "after_config_apply", path: configPath });
      }

      let applyResult = { appliedChanges: 0, appliedPaths: [], skippedPaths: [] };
      emitProgress(onProgress, { stage: "update_sqlite", status: "start" });
      emitProgress(onProgress, {
        stage: "rewrite_rollout_files",
        status: "start",
        writableCount: writableChanges.length
      });
      const sqliteResult = await updateSqliteProvider(
        storage,
        targetProvider,
        async () => {
          if (writableChanges.length > 0) {
            applyResult = await applySessionChanges(writableChanges, {
              targetModel: model,
              onBeforeApply: async (change) => {
                throwIfAborted(signal);
                await journal.applying("rollout", change.path);
                await faultInjector?.({
                  point: "before_rollout_apply",
                  path: change.path,
                  targetIndex: appliedSessionChanges.length + 1
                });
              },
              onApplied: async (change) => {
                appliedSessionChanges.push(change);
                sessionRestoreNeeded = true;
                await journal.applied("rollout", change.path);
                recordCompletedTarget(change.path);
                await updateSessionBackupManifest(backupDir, appliedSessionChanges);
                await faultInjector?.({ point: "after_rollout_apply", path: change.path, appliedCount: appliedSessionChanges.length });
              }
            });
          }
          workspaceRootResult = await syncWorkspaceRoots(storage, {
            cwdStats,
            onBeforeWrite: async (targetPath) => {
              throwIfAborted(signal);
              await journal.applying("globalState", targetPath);
            },
            onApplied: async (targetPath) => {
              globalStateRestoreNeeded = true;
              await journal.applied("globalState", targetPath);
              recordCompletedTarget(targetPath);
              await faultInjector?.({ point: "after_global_state_apply", path: targetPath });
            }
          });
        },
        { busyTimeoutMs: sqliteBusyTimeoutMs, userEventThreadIds, threadCwdById, targetModel: model }
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
      recordCompletedTarget(storage.stateDbLocation?.path ?? storage.sqliteHome);
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
      const result = {
        codexHome,
        sqliteHome: storage.sqliteHome,
        sqliteHomeSource: storage.sqliteHomeSource,
        targetProvider,
        previousProvider: current.provider,
        backupDir,
        backupDurationMs,
        changedSessionFiles: applyResult.appliedChanges,
        skippedLockedRolloutFiles,
        sqliteRowsUpdated: sqliteResult.updatedRows,
        sqliteProviderRowsUpdated: sqliteResult.providerRowsUpdated,
        sqliteUserEventRowsUpdated: sqliteResult.userEventRowsUpdated,
        sqliteCwdRowsUpdated: sqliteResult.cwdRowsUpdated,
        updatedWorkspaceRoots: workspaceRootResult.updatedWorkspaceRoots,
        savedWorkspaceRootCount: workspaceRootResult.savedWorkspaceRootCount,
        sqlitePresent: sqliteResult.databasePresent,
        rolloutCountsBefore: summarizeProviderCounts(providerCounts),
        encryptedContentCounts,
        encryptedContentWarning,
        autoPruneResult,
        autoPruneWarning
      };
      await journal.committed();
      return result;
    } catch (error) {
      const restoreFailures = [];
      try {
        await journal?.rollingBack(error);
      } catch (journalError) {
        restoreFailures.push(`transaction journal: ${journalError.message}`);
      }
      if (sessionRestoreNeeded) {
        try {
          await faultInjector?.({ point: "before_rollout_rollback", appliedCount: appliedSessionChanges.length });
          await restoreSessionChanges(appliedSessionChanges);
        } catch (restoreError) {
          restoreFailures.push(`rollout files: ${restoreError.message}`);
        }
      }
      if (globalStateRestoreNeeded && backupDir) {
        try {
          await faultInjector?.({ point: "before_global_state_rollback" });
          await restoreGlobalStateFilesFromBackup(backupDir, codexHome);
        } catch (restoreError) {
          restoreFailures.push(`global state: ${restoreError.message}`);
        }
      }
      if (configBackupText !== undefined) {
        try {
          await writeConfigText(configPath, configBackupText);
        } catch (restoreError) {
          restoreFailures.push(`config: ${restoreError.message}`);
        }
      }
      if (restoreFailures.length === 0) {
        try {
          await journal?.rolledBack();
        } catch (journalError) {
          restoreFailures.push(`transaction journal: ${journalError.message}`);
        }
      }
      if (restoreFailures.length > 0) {
        try {
          await journal?.recoveryRequired(error, restoreFailures);
        } catch {
          // Preserve the original and rollback errors even if the journal is
          // no longer writable.
        }
        const completedSet = new Set(completedTargets);
        const uncompletedTargets = potentialTargets.filter((targetPath) => !completedSet.has(targetPath));
        throw new SyncTransactionError(
          error,
          restoreFailures,
          backupDir,
          completedTargets,
          uncompletedTargets,
          { rollbackStatus: "incomplete", recoveryRequired: true }
        );
      }
      const completedSet = new Set(completedTargets);
      throw new SyncTransactionError(
        error,
        [],
        backupDir,
        completedTargets,
        potentialTargets.filter((targetPath) => !completedSet.has(targetPath)),
        { rollbackStatus: "complete", recoveryRequired: false }
      );
    }
  } finally {
    await releaseLock();
  }
}

export async function runSwitch({
  codexHome: explicitCodexHome,
  sqliteHome,
  provider,
  model,
  keepRootModel = false,
  keepCount = DEFAULT_BACKUP_RETENTION_COUNT,
  onProgress,
  platform
}) {
  if (!provider) {
    throw new Error("Missing provider id. Usage: codex-provider switch <provider-id>");
  }

  const codexHome = normalizeCodexHome(explicitCodexHome);
  const configPath = path.join(codexHome, "config.toml");
  const originalConfigText = await readConfigText(configPath);
  const storage = await prepareStorage({ codexHome, sqliteHome, configText: originalConfigText, platform });
  assertSqliteAccessSupported(storage, "switch");
  if (!storage.stateDbLocation && isConfiguredSqliteHome(storage)) {
    throw missingConfiguredStateDbError(storage);
  }
  if (!configDeclaresProvider(originalConfigText, provider)) {
    throw new Error(`Provider "${provider}" is not available in config.toml. Configure it first or use one of: ${listConfiguredProviderIds(originalConfigText).join(", ")}`);
  }

  if (model !== undefined && model !== null && keepRootModel) {
    throw new Error("--model and --keep-root-model are mutually exclusive. Pick one.");
  }

  let nextConfigText = setRootProviderInConfigText(originalConfigText, provider);
  let modelSync = { applied: false, source: "none", model: null, warning: null };

  if (model !== undefined && model !== null) {
    if (typeof model !== "string" || model.length === 0) {
      throw new Error(`Invalid --model value: ${model}. Expected a non-empty string.`);
    }
    nextConfigText = setRootModelInConfigText(nextConfigText, model);
    modelSync = { applied: true, source: "explicit", model, warning: null };
  } else if (!keepRootModel) {
    const providerModel = readProviderModel(originalConfigText, provider);
    if (providerModel) {
      nextConfigText = setRootModelInConfigText(nextConfigText, providerModel);
      modelSync = { applied: true, source: "provider-section", model: providerModel, warning: null };
    } else if (provider !== DEFAULT_PROVIDER) {
      modelSync = {
        applied: false,
        source: "none",
        model: null,
        warning: `Provider "${provider}" has no model field in [model_providers.${provider}]; root-level model left unchanged. Use --model <name> to set it explicitly, or --keep-root-model to suppress this warning.`
      };
    }
  }

  let configMutationAttempted = false;
  try {
    // `nextConfigText` has the final root-level `model` value. Use that to
    // drive the per-thread rewrite so old sessions match new sessions.
    let modelForThreads = null;
    if (modelSync.applied && modelSync.model) {
      modelForThreads = modelSync.model;
    } else {
      modelForThreads = readRootModelFromConfigText(nextConfigText);
    }
    const syncResult = await runSyncCore(
      {
        codexHome,
        storage,
        provider,
        configBackupText: originalConfigText,
        keepCount,
        onProgress,
        model: modelForThreads
      },
      {
        afterBackup: async () => {
          emitProgress(onProgress, {
            stage: "update_config",
            status: "start",
            provider
          });
          configMutationAttempted = true;
          await writeConfigText(configPath, nextConfigText);
          emitProgress(onProgress, {
            stage: "update_config",
            status: "complete",
            provider
          });
        }
      }
    );
    return {
      ...syncResult,
      configUpdated: true,
      modelSync
    };
  } catch (error) {
    if (configMutationAttempted) {
      await writeConfigText(configPath, originalConfigText);
    }
    throw error;
  }
}

export async function runRestore({
  codexHome: explicitCodexHome,
  sqliteHome,
  backupDir,
  restoreConfig = true,
  restoreDatabase = true,
  restoreSessions = true,
  allowSqliteHomeRelocation = false,
  platform
}) {
  if (!backupDir) {
    throw new Error("Missing backup path. Usage: codex-provider restore <backup-dir>");
  }
  const codexHome = normalizeCodexHome(explicitCodexHome);
  if (allowSqliteHomeRelocation && !(typeof sqliteHome === "string" && sqliteHome.trim())) {
    throw new Error("--allow-sqlite-home-relocation requires an explicit --sqlite-home path.");
  }
  const configText = await readConfigText(path.join(codexHome, "config.toml"));
  const storage = await prepareStorage({ codexHome, sqliteHome, configText, platform });
  assertSqliteAccessSupported(storage, "restore");
  if (restoreDatabase && !storage.stateDbLocation && isConfiguredSqliteHome(storage)) {
    throw missingConfiguredStateDbError(storage);
  }
  const releaseLock = await acquireLock(codexHome, "restore");
  try {
    const normalizedBackupDir = path.resolve(backupDir);
    const result = await restoreBackup(normalizedBackupDir, storage, {
      restoreConfig,
      restoreDatabase,
      restoreSessions,
      allowSqliteHomeRelocation
    });
    await markBackupTransactionRolledBack(normalizedBackupDir);
    return result;
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
  await ensureCodexHome(resolveStorageLayout({ codexHome, env: {} }));
  const releaseLock = await acquireLock(codexHome, "prune-backups");
  try {
    return await pruneBackups(codexHome, keepCount);
  } finally {
    await releaseLock();
  }
}
