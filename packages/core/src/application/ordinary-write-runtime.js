// @ts-nocheck

import {
  DEFAULT_BACKUP_RETENTION_COUNT,
  DEFAULT_PROVIDER,
  CoreError,
  acquireLock,
  assertSqliteAccessSupported,
  isConfiguredSqliteHome,
  missingConfiguredStateDbError,
  normalizeCodexHome,
  pruneManagedBackups,
  codexStorage,
  path
} from "../infrastructure/node-core-ports.js";
import { assertNoPendingRestoreTransactions, verifyExpectedPlanState } from "./plan-context.js";
import { sqliteProviderRowsToChange } from "./provider-counts.js";
import { repairSqliteRowsToChange } from "./repair-targets.js";
import { emitProgress, prepareStorage, throwIfAborted } from "./runtime-support.js";
import { sqliteTransaction, undoBackup } from "./runtime-context.js";

const {
  readConfigText,
  readCurrentProviderFromConfigText,
  readRootModelFromConfigText
} = codexStorage.config;
const {
  applySessionChanges,
  collectProviderChanges,
  collectRepairChanges,
  splitLockedSessionChanges,
  summarizeProviderCounts
} = codexStorage.sessions;
const {
  assertSqliteWritable,
  readSqliteProviderCounts,
  readSqliteRepairStats
} = codexStorage.stateDb;
const {
  cwdStatsFromThreadCwdMap,
  readWorkspaceRootRepairStats,
  syncWorkspaceRoots
} = codexStorage.globalState;

async function tryRefreshBackupInventory(backupDir) {
  try {
    await undoBackup.refreshInventory(backupDir);
  } catch {
    // Preserve the mutation diagnosis; inventory refresh is only bookkeeping.
  }
}

function emptySqliteMutationResult(databasePresent) {
  return {
    updatedRows: 0,
    providerRowsUpdated: 0,
    modelRowsUpdated: 0,
    userEventRowsUpdated: 0,
    cwdRowsUpdated: 0,
    databasePresent
  };
}

export async function executeOrdinaryWrite({
  codexHome: explicitCodexHome,
  sqliteHome,
  storage: providedStorage,
  expectedConfigText,
  keepCount = DEFAULT_BACKUP_RETENTION_COUNT,
  sqliteBusyTimeoutMs,
  onProgress,
  platform,
  faultInjector,
  signal,
  expectedPlanState
} = {}, {
  afterBackup,
  targetProvider: targetProviderOverride,
  configBackupText,
  configMutationExpected = false,
  operationKind = "sync",
  repair = null
} = {}) {
  if (!Number.isInteger(keepCount) || keepCount < 1) {
    throw new CoreError(
      "INVALID_INPUT",
      `Invalid automatic keep count: ${keepCount}. Expected an integer greater than or equal to 1.`
    );
  }

  const codexHome = providedStorage?.codexHome ?? normalizeCodexHome(explicitCodexHome);
  const configPath = path.join(codexHome, "config.toml");
  const releaseLock = await acquireLock(codexHome, operationKind);
  let backupDir = null;
  let backupDurationMs = 0;
  try {
    throwIfAborted(signal);
    const configText = await readConfigText(configPath);
    if (!expectedPlanState && expectedConfigText !== undefined && configText !== expectedConfigText) {
      throw new CoreError("PLAN_STALE", "config.toml changed after confirmation. Refresh and retry.");
    }
    if (!expectedPlanState && configBackupText !== undefined && configText !== configBackupText) {
      throw new CoreError("PLAN_STALE", "config.toml changed before the switch acquired its lock. Refresh and retry.");
    }

    const storage = await prepareStorage({ codexHome, sqliteHome, configText, storage: providedStorage, platform });
    assertSqliteAccessSupported(storage, operationKind);
    if (!storage.stateDbLocation && isConfiguredSqliteHome(storage)) {
      throw missingConfiguredStateDbError(storage);
    }
    await assertNoPendingRestoreTransactions(codexHome);
    await verifyExpectedPlanState({ expectedPlanState, codexHome, configText, storage, platform });

    const current = readCurrentProviderFromConfigText(configText);
    const targetProvider = targetProviderOverride ?? current.provider ?? DEFAULT_PROVIDER;
    const repairTargets = new Set(repair?.targets ?? []);
    const targetModel = repairTargets.has("models") ? readRootModelFromConfigText(configText) : null;
    if (repairTargets.has("models") && !targetModel) {
      throw new CoreError("INVALID_INPUT", "Model repair requires a root model in config.toml.");
    }

    emitProgress(onProgress, { stage: "scan_rollout_files", status: "start" });
    const scan = repair
      ? await collectRepairChanges(codexHome, [...repairTargets], { skipLockedReads: true, targetModel })
      : await collectProviderChanges(codexHome, targetProvider, { skipLockedReads: true });
    const { changes, lockedPaths: lockedReadPaths, providerCounts } = scan;
    emitProgress(onProgress, { stage: "check_locked_rollout_files", status: "start" });
    const { writableChanges, lockedChanges } = await splitLockedSessionChanges(changes);
    const initiallySkipped = [...new Set([
      ...lockedReadPaths,
      ...lockedChanges.map((change) => change.path)
    ])].sort((left, right) => left.localeCompare(right));
    emitProgress(onProgress, {
      stage: "scan_rollout_files",
      status: "complete",
      scannedChanges: changes.length,
      lockedReadCount: lockedReadPaths.length
    });
    emitProgress(onProgress, {
      stage: "check_locked_rollout_files",
      status: "complete",
      writableCount: writableChanges.length,
      lockedCount: initiallySkipped.length
    });

    let sqliteRowsToWrite = 0;
    let sqliteStats = null;
    if (repair) {
      sqliteStats = storage.stateDbLocation
        ? await readSqliteRepairStats(storage, {
            targetModel,
            userEventThreadIds: scan.userEventThreadIds,
            threadCwdById: scan.threadCwdById
          })
        : null;
      sqliteRowsToWrite = repairSqliteRowsToChange(sqliteStats, [...repairTargets]);
    } else {
      const sqliteCounts = storage.stateDbLocation ? await readSqliteProviderCounts(storage) : null;
      if (sqliteCounts?.unreadable === true) {
        await assertSqliteWritable(storage, { busyTimeoutMs: sqliteBusyTimeoutMs });
      }
      sqliteRowsToWrite = sqliteProviderRowsToChange(sqliteCounts, targetProvider);
    }
    const workspaceStats = repairTargets.has("workspaceRoots")
      ? await readWorkspaceRootRepairStats(storage, {
          cwdStats: cwdStatsFromThreadCwdMap(scan.threadCwdById)
        })
      : null;
    const workspaceMutationExpected = workspaceStats?.needsRepair === true;
    const writeExpected = configMutationExpected
      || writableChanges.length > 0
      || sqliteRowsToWrite > 0
      || workspaceMutationExpected;

    if (!writeExpected) {
      return {
        codexHome,
        sqliteHome: storage.sqliteHome,
        sqliteHomeSource: storage.sqliteHomeSource,
        targetProvider,
        ...(repair ? { repairTargets: [...repairTargets], targetModel } : {}),
        previousProvider: current.provider,
        backupDir: null,
        backupDurationMs: 0,
        noop: initiallySkipped.length === 0,
        partial: initiallySkipped.length > 0,
        partialReason: initiallySkipped.length > 0 ? "locked-session" : null,
        retryRecommended: initiallySkipped.length > 0,
        changedSessionFiles: 0,
        inPlaceSessionFiles: 0,
        rewrittenSessionFiles: 0,
        skippedLockedRolloutFiles: initiallySkipped,
        skippedChangedRolloutFiles: [],
        sqliteRowsUpdated: 0,
        sqliteProviderRowsUpdated: 0,
        sqliteModelRowsUpdated: 0,
        sqliteUserEventRowsUpdated: 0,
        sqliteCwdRowsUpdated: 0,
        updatedWorkspaceRoots: 0,
        savedWorkspaceRootCount: workspaceStats?.savedWorkspaceRootCount ?? 0,
        sqlitePresent: Boolean(storage.stateDbLocation),
        rolloutCountsBefore: summarizeProviderCounts(providerCounts),
        autoPruneResult: null,
        autoPruneWarning: null
      };
    }

    if (storage.stateDbLocation) {
      await assertSqliteWritable(storage, { busyTimeoutMs: sqliteBusyTimeoutMs });
    }
    throwIfAborted(signal);
    await faultInjector?.({ point: "before_backup" });
    emitProgress(onProgress, { stage: "create_backup", status: "start", writableCount: writableChanges.length });
    throwIfAborted(signal);
    // UndoBackup covers only stores this operation can actually mutate. The
    // flags are computed after the locked-session and SQLite/workspace
    // preflight, so an unused store is neither read nor later restored.
    const targetKinds = {
      config: configMutationExpected,
      globalState: workspaceMutationExpected,
      sqlite: sqliteRowsToWrite > 0,
      rollout: writableChanges.length > 0
    };
    const backupStartedAt = Date.now();
    backupDir = await undoBackup.capture({
      storage,
      codexHome,
      targetProvider,
      sessionChanges: writableChanges,
      configPath,
      configBackupText,
      targetKinds,
      faultInjector
    });
    backupDurationMs = Date.now() - backupStartedAt;
    emitProgress(onProgress, {
      stage: "create_backup",
      status: "complete",
      durationMs: backupDurationMs,
      backupDir
    });
    throwIfAborted(signal);

    let failedStage = "mutation";
    let applyResult = {
      appliedChanges: 0,
      inPlaceChanges: 0,
      appliedPaths: [],
      skippedPaths: [],
      skippedLockedPaths: [],
      skippedChangedPaths: []
    };
    let sqliteResult = emptySqliteMutationResult(Boolean(storage.stateDbLocation));
    let workspaceRootResult = {
      updated: false,
      updatedWorkspaceRoots: 0,
      savedWorkspaceRootCount: workspaceStats?.savedWorkspaceRootCount ?? 0
    };
    let mutationStarted = false;
    try {
      // Cancellation is intentionally closed after this boundary. Retrying the
      // same operation is the recovery model for ordinary writes.
      if (configMutationExpected && typeof afterBackup === "function") {
        failedStage = "update_config";
        emitProgress(onProgress, { stage: failedStage, status: "start" });
        await afterBackup(backupDir);
        mutationStarted = true;
        await faultInjector?.({ point: "after_config_mutation_before_applied", path: configPath });
        emitProgress(onProgress, { stage: failedStage, status: "complete" });
      }

      if (writableChanges.length > 0) {
        failedStage = "rewrite_rollout_files";
        emitProgress(onProgress, { stage: failedStage, status: "start", writableCount: writableChanges.length });
        applyResult = await applySessionChanges(writableChanges, {
          ...(repairTargets.has("models") ? { targetModel } : {}),
          onBeforeApply: (change) => faultInjector?.({ point: "before_rollout_apply", path: change.path }),
          onMutation: (change, mutation) => {
            mutationStarted = true;
            return faultInjector?.({
              point: "after_rollout_mutation_before_applied",
              path: change.path,
              mutation
            });
          },
          onApplied: (change) => faultInjector?.({ point: "after_rollout_apply", path: change.path }),
          onSkipped: (change, reason) => faultInjector?.({ point: "after_rollout_skip", path: change.path, reason })
        });
        emitProgress(onProgress, {
          stage: failedStage,
          status: "complete",
          appliedChanges: applyResult.appliedChanges,
          skippedChanges: applyResult.skippedPaths.length
        });
      }

      if (workspaceMutationExpected) {
        failedStage = "repair_workspace_roots";
        workspaceRootResult = await syncWorkspaceRoots(storage, {
          cwdStats: cwdStatsFromThreadCwdMap(scan.threadCwdById),
          onApplied: (targetPath) => {
            mutationStarted = true;
            return faultInjector?.({ point: "after_global_state_apply", path: targetPath });
          }
        });
      }

      if (sqliteRowsToWrite > 0) {
        failedStage = "update_sqlite";
        emitProgress(onProgress, { stage: failedStage, status: "start" });
        const sqliteOptions = {
          busyTimeoutMs: sqliteBusyTimeoutMs,
          ...(repair
            ? {
                targets: [...repairTargets].filter((target) => target !== "workspaceRoots"),
                targetModel,
                userEventThreadIds: scan.userEventThreadIds,
                threadCwdById: scan.threadCwdById
              }
            : {}),
          onCommitAttempt: () => {
            mutationStarted = true;
          },
          afterCommit: () => faultInjector?.({
            point: "after_sqlite_commit_before_ack",
            path: storage.stateDbLocation?.path ?? null
          })
        };
        sqliteResult = repair
          ? await sqliteTransaction.repair(storage, sqliteOptions)
          : await sqliteTransaction.updateProvider(storage, targetProvider, sqliteOptions);
        await faultInjector?.({ point: "after_sqlite_commit", path: storage.stateDbLocation?.path ?? null });
        emitProgress(onProgress, { stage: failedStage, status: "complete", updatedRows: sqliteResult.updatedRows });
      }
    } catch (error) {
      if (!mutationStarted) throw error;
      const skippedLockedRolloutFiles = [...new Set([
        ...initiallySkipped,
        ...(applyResult.skippedLockedPaths ?? [])
      ])].sort((left, right) => left.localeCompare(right));
      const skippedChangedRolloutFiles = [...new Set(
        applyResult.skippedChangedPaths ?? []
      )].sort((left, right) => left.localeCompare(right));
      await tryRefreshBackupInventory(backupDir);
      return {
        codexHome,
        sqliteHome: storage.sqliteHome,
        sqliteHomeSource: storage.sqliteHomeSource,
        targetProvider,
        ...(repair ? { repairTargets: [...repairTargets], targetModel } : {}),
        previousProvider: current.provider,
        backupDir,
        backupDurationMs,
        partial: true,
        partialReason: "mutation-failed",
        failedStage,
        retryRecommended: true,
        failureCode: typeof error?.code === "string" ? error.code : "WRITE_FAILED",
        partialWarning: `The operation stopped during ${failedStage}. Run the same operation again to converge, or restore the backup manually.`,
        changedSessionFiles: applyResult.appliedChanges ?? 0,
        inPlaceSessionFiles: applyResult.inPlaceChanges ?? 0,
        rewrittenSessionFiles: Math.max(0, (applyResult.appliedChanges ?? 0) - (applyResult.inPlaceChanges ?? 0)),
        skippedLockedRolloutFiles,
        skippedChangedRolloutFiles,
        sqliteRowsUpdated: sqliteResult.updatedRows,
        sqliteProviderRowsUpdated: sqliteResult.providerRowsUpdated,
        sqliteModelRowsUpdated: sqliteResult.modelRowsUpdated,
        sqliteUserEventRowsUpdated: sqliteResult.userEventRowsUpdated,
        sqliteCwdRowsUpdated: sqliteResult.cwdRowsUpdated,
        updatedWorkspaceRoots: workspaceRootResult.updatedWorkspaceRoots,
        savedWorkspaceRootCount: workspaceRootResult.savedWorkspaceRootCount,
        sqlitePresent: sqliteResult.databasePresent,
        rolloutCountsBefore: summarizeProviderCounts(providerCounts),
        autoPruneResult: null,
        autoPruneWarning: null
      };
    }

    const skippedLockedRolloutFiles = [...new Set([
      ...initiallySkipped,
      ...(applyResult.skippedLockedPaths ?? [])
    ])].sort((left, right) => left.localeCompare(right));
    const skippedChangedRolloutFiles = [...new Set(
      applyResult.skippedChangedPaths ?? []
    )].sort((left, right) => left.localeCompare(right));
    let backupInventoryWarning = null;
    try {
      await undoBackup.refreshInventory(backupDir, { faultInjector });
    } catch (error) {
      backupInventoryWarning = "Backup inventory refresh did not complete.";
    }
    let autoPruneResult = null;
    let autoPruneWarning = null;
    emitProgress(onProgress, { stage: "clean_backups", status: "start", keepCount });
    try {
      autoPruneResult = await pruneManagedBackups(codexHome, keepCount);
    } catch (error) {
      autoPruneWarning = "Automatic backup cleanup did not complete.";
    }
    emitProgress(onProgress, {
      stage: "clean_backups",
      status: "complete",
      deletedCount: autoPruneResult?.deletedCount ?? 0
    });

    return {
      codexHome,
      sqliteHome: storage.sqliteHome,
      sqliteHomeSource: storage.sqliteHomeSource,
      targetProvider,
      ...(repair ? { repairTargets: [...repairTargets], targetModel } : {}),
      previousProvider: current.provider,
      backupDir,
      backupDurationMs,
      partial: skippedLockedRolloutFiles.length > 0 || skippedChangedRolloutFiles.length > 0,
      partialReason: skippedLockedRolloutFiles.length > 0
        ? "locked-session"
        : (skippedChangedRolloutFiles.length > 0 ? "rollout-changed" : null),
      retryRecommended: skippedLockedRolloutFiles.length > 0 || skippedChangedRolloutFiles.length > 0,
      changedSessionFiles: applyResult.appliedChanges,
      inPlaceSessionFiles: applyResult.inPlaceChanges ?? 0,
      rewrittenSessionFiles: Math.max(0, applyResult.appliedChanges - (applyResult.inPlaceChanges ?? 0)),
      skippedLockedRolloutFiles,
      skippedChangedRolloutFiles,
      sqliteRowsUpdated: sqliteResult.updatedRows,
      sqliteProviderRowsUpdated: sqliteResult.providerRowsUpdated,
      ...(repair
        ? {
            sqliteModelRowsUpdated: sqliteResult.modelRowsUpdated,
            sqliteUserEventRowsUpdated: sqliteResult.userEventRowsUpdated,
            sqliteCwdRowsUpdated: sqliteResult.cwdRowsUpdated,
            updatedWorkspaceRoots: workspaceRootResult.updatedWorkspaceRoots,
            savedWorkspaceRootCount: workspaceRootResult.savedWorkspaceRootCount
          }
        : {}),
      sqlitePresent: sqliteResult.databasePresent,
      rolloutCountsBefore: summarizeProviderCounts(providerCounts),
      autoPruneResult,
      backupInventoryWarning,
      autoPruneWarning
    };
  } finally {
    await releaseLock();
  }
}
