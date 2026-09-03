// @ts-nocheck

import {
  DEFAULT_BACKUP_RETENTION_COUNT,
  DEFAULT_PROVIDER,
  CoreError,
  isConfiguredSqliteHome,
  missingConfiguredStateDbError,
  codexStorage
} from "../infrastructure/node-core-ports.js";
import { executeOrdinaryWrite } from "./ordinary-write-runtime.js";
import { preparePlanContext } from "./plan-context.js";
import { normalizeRepairTargets, repairSqliteRowsToChange } from "./repair-targets.js";
import { operationRuntime, sqliteTransaction } from "./runtime-context.js";

const {
  applySessionChanges,
  collectRepairChanges,
  splitLockedSessionChanges,
  summarizeProviderCounts
} = codexStorage.sessions;
const { readCurrentProviderFromConfigText, readRootModelFromConfigText } = codexStorage.config;
const { readSqliteRepairStats } = codexStorage.stateDb;
const {
  cwdStatsFromThreadCwdMap,
  readWorkspaceRootRepairStats,
  syncWorkspaceRoots
} = codexStorage.globalState;

function sortedUnique(paths) {
  return [...new Set(paths)].sort((left, right) => left.localeCompare(right));
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

function repairResult({ context, state, current, targets, targetModel, scan, initiallySkipped, workspaceStats, outcome, error }) {
  const applyResult = state.outputs.rollout ?? {
    appliedChanges: 0,
    inPlaceChanges: 0,
    skippedLockedPaths: [],
    skippedChangedPaths: []
  };
  const sqliteResult = state.outputs.sqlite ?? emptySqliteMutationResult(Boolean(context.storage.stateDbLocation));
  const workspaceRootResult = state.outputs.globalState ?? {
    updatedWorkspaceRoots: 0,
    savedWorkspaceRootCount: workspaceStats?.savedWorkspaceRootCount ?? 0
  };
  const skippedLockedRolloutFiles = sortedUnique([
    ...initiallySkipped,
    ...(applyResult.skippedLockedPaths ?? [])
  ]);
  const skippedChangedRolloutFiles = sortedUnique(applyResult.skippedChangedPaths ?? []);
  const partialFromSessions = skippedLockedRolloutFiles.length > 0 || skippedChangedRolloutFiles.length > 0;
  const partialFailure = outcome === "partial";
  return {
    codexHome: context.codexHome,
    sqliteHome: context.storage.sqliteHome,
    sqliteHomeSource: context.storage.sqliteHomeSource,
    targetProvider: current.provider ?? DEFAULT_PROVIDER,
    repairTargets: targets,
    targetModel,
    previousProvider: current.provider,
    backupDir: state.backupDir,
    backupDurationMs: state.backupDurationMs,
    ...(partialFailure
      ? {
          partial: true,
          partialReason: "mutation-failed",
          failedStage: state.failedStage,
          retryRecommended: true,
          failureCode: typeof error?.code === "string" ? error.code : "WRITE_FAILED",
          partialWarning: `The operation stopped during ${state.failedStage}. Run the same operation again to converge, or restore the backup manually.`
        }
      : {
          partial: partialFromSessions,
          partialReason: skippedLockedRolloutFiles.length > 0
            ? "locked-session"
            : (skippedChangedRolloutFiles.length > 0 ? "rollout-changed" : null),
          retryRecommended: partialFromSessions
        }),
    changedSessionFiles: applyResult.appliedChanges ?? 0,
    inPlaceSessionFiles: applyResult.inPlaceChanges ?? 0,
    rewrittenSessionFiles: Math.max(0, (applyResult.appliedChanges ?? 0) - (applyResult.inPlaceChanges ?? 0)),
    skippedLockedRolloutFiles,
    skippedChangedRolloutFiles,
    sqliteRowsUpdated: sqliteResult.updatedRows ?? 0,
    sqliteProviderRowsUpdated: sqliteResult.providerRowsUpdated ?? 0,
    sqliteModelRowsUpdated: sqliteResult.modelRowsUpdated ?? 0,
    sqliteUserEventRowsUpdated: sqliteResult.userEventRowsUpdated ?? 0,
    sqliteCwdRowsUpdated: sqliteResult.cwdRowsUpdated ?? 0,
    updatedWorkspaceRoots: workspaceRootResult.updatedWorkspaceRoots ?? 0,
    savedWorkspaceRootCount: workspaceRootResult.savedWorkspaceRootCount ?? 0,
    sqlitePresent: sqliteResult.databasePresent,
    rolloutCountsBefore: summarizeProviderCounts(scan.providerCounts),
    autoPruneResult: state.autoPruneResult,
    backupInventoryWarning: state.backupInventoryWarning,
    autoPruneWarning: state.autoPruneWarning
  };
}

export async function buildRepairWriteProgram(context, targets) {
  const current = readCurrentProviderFromConfigText(context.configText);
  const targetModel = targets.includes("models")
    ? readRootModelFromConfigText(context.configText)
    : null;
  if (targets.includes("models") && !targetModel) {
    throw new CoreError("INVALID_INPUT", "Model repair requires a root model in config.toml.");
  }
  context.emitProgress({ stage: "scan_rollout_files", status: "start" });
  const scan = await collectRepairChanges(context.codexHome, targets, {
    skipLockedReads: true,
    targetModel
  });
  context.emitProgress({ stage: "check_locked_rollout_files", status: "start" });
  const { writableChanges, lockedChanges } = await splitLockedSessionChanges(scan.changes);
  const initiallySkipped = sortedUnique([
    ...scan.lockedPaths,
    ...lockedChanges.map((change) => change.path)
  ]);
  context.emitProgress({
    stage: "scan_rollout_files",
    status: "complete",
    scannedChanges: scan.changes.length,
    lockedReadCount: scan.lockedPaths.length
  });
  context.emitProgress({
    stage: "check_locked_rollout_files",
    status: "complete",
    writableCount: writableChanges.length,
    lockedCount: initiallySkipped.length
  });

  const sqliteStats = context.storage.stateDbLocation
    ? await readSqliteRepairStats(context.storage, {
        targetModel,
        userEventThreadIds: scan.userEventThreadIds,
        threadCwdById: scan.threadCwdById
      })
    : null;
  const sqliteRowsToWrite = repairSqliteRowsToChange(sqliteStats, targets);
  const cwdStats = cwdStatsFromThreadCwdMap(scan.threadCwdById);
  const workspaceStats = targets.includes("workspaceRoots")
    ? await readWorkspaceRootRepairStats(context.storage, { cwdStats })
    : null;
  const workspaceMutationExpected = workspaceStats?.needsRepair === true;
  const targetKinds = {
    config: false,
    rollout: writableChanges.length > 0,
    globalState: workspaceMutationExpected,
    sqlite: sqliteRowsToWrite > 0
  };

  return {
    targetKinds,
    backup: {
      targetProvider: current.provider ?? DEFAULT_PROVIDER,
      sessionChanges: writableChanges,
      writableCount: writableChanges.length,
      configPath: context.configPath
    },
    noMutationResult: () => ({
      codexHome: context.codexHome,
      sqliteHome: context.storage.sqliteHome,
      sqliteHomeSource: context.storage.sqliteHomeSource,
      targetProvider: current.provider ?? DEFAULT_PROVIDER,
      repairTargets: targets,
      targetModel,
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
      sqlitePresent: Boolean(context.storage.stateDbLocation),
      rolloutCountsBefore: summarizeProviderCounts(scan.providerCounts),
      autoPruneResult: null,
      autoPruneWarning: null
    }),
    steps: {
      ...(writableChanges.length > 0
        ? {
            rollout: {
              stage: "rewrite_rollout_files",
              start: { writableCount: writableChanges.length },
              complete: (result) => ({
                appliedChanges: result.appliedChanges,
                skippedChanges: result.skippedPaths.length
              }),
              run: ({ context: writeContext }) => applySessionChanges(writableChanges, {
                ...(targets.includes("models") ? { targetModel } : {}),
                onBeforeApply: (change) => writeContext.faultInjector?.({ point: "before_rollout_apply", path: change.path }),
                onMutation: (change, mutation) => {
                  writeContext.markMutation();
                  return writeContext.faultInjector?.({
                    point: "after_rollout_mutation_before_applied",
                    path: change.path,
                    mutation
                  });
                },
                onApplied: (change) => writeContext.faultInjector?.({ point: "after_rollout_apply", path: change.path }),
                onSkipped: (change, reason) => writeContext.faultInjector?.({ point: "after_rollout_skip", path: change.path, reason })
              })
            }
          }
        : {}),
      ...(workspaceMutationExpected
        ? {
            globalState: {
              // This historical mutation intentionally had no progress event.
              silent: true,
              stage: "repair_workspace_roots",
              run: ({ context: writeContext }) => syncWorkspaceRoots(writeContext.storage, {
                cwdStats,
                onApplied: (targetPath) => {
                  writeContext.markMutation();
                  return writeContext.faultInjector?.({ point: "after_global_state_apply", path: targetPath });
                }
              })
            }
          }
        : {}),
      ...(sqliteRowsToWrite > 0
        ? {
            sqlite: {
              stage: "update_sqlite",
              complete: (result) => ({ updatedRows: result.updatedRows }),
              run: async ({ context: writeContext }) => {
                const result = await sqliteTransaction.repair(writeContext.storage, {
                  busyTimeoutMs: writeContext.sqliteBusyTimeoutMs,
                  targets: targets.filter((target) => target !== "workspaceRoots"),
                  targetModel,
                  userEventThreadIds: scan.userEventThreadIds,
                  threadCwdById: scan.threadCwdById,
                  onCommitAttempt: () => writeContext.markMutation(),
                  afterCommit: () => writeContext.faultInjector?.({
                    point: "after_sqlite_commit_before_ack",
                    path: writeContext.storage.stateDbLocation?.path ?? null
                  })
                });
                await writeContext.faultInjector?.({
                  point: "after_sqlite_commit",
                  path: writeContext.storage.stateDbLocation?.path ?? null
                });
                return result;
              }
            }
          }
        : {})
    },
    toResult: ({ state, outcome, error }) => repairResult({
      context,
      state,
      current,
      targets,
      targetModel,
      scan,
      initiallySkipped,
      workspaceStats,
      outcome,
      error
    })
  };
}

export async function prepareRepairPlan(options) {
  const targets = normalizeRepairTargets(options.targets);
  const keepCount = options.keepCount ?? DEFAULT_BACKUP_RETENTION_COUNT;
  if (!Number.isInteger(keepCount) || keepCount < 1) {
    throw new CoreError("INVALID_INPUT", "Repair keepCount must be an integer greater than or equal to 1.");
  }
  const needsBody = targets.includes("models") || targets.includes("userEvent");
  const rolloutRevisionMode = needsBody ? "content" : "metadata";
  const context = await preparePlanContext({
    ...options,
    rolloutScanMode: "metadata",
    rolloutRevisionMode
  }, "repair");
  if (!context.storage.stateDbLocation && isConfiguredSqliteHome(context.storage)) {
    throw missingConfiguredStateDbError(context.storage);
  }
  const targetModel = targets.includes("models")
    ? readRootModelFromConfigText(context.configText)
    : null;
  if (targets.includes("models") && !targetModel) {
    throw new CoreError("INVALID_INPUT", "Model repair requires a root model in config.toml.");
  }
  const scan = await collectRepairChanges(context.codexHome, targets, {
    skipLockedReads: true,
    targetModel
  });
  const { writableChanges, lockedChanges } = await splitLockedSessionChanges(scan.changes);
  const lockedCount = new Set([
    ...scan.lockedPaths,
    ...lockedChanges.map((change) => change.path)
  ]).size;
  const sqliteStats = context.storage.stateDbLocation
    ? await readSqliteRepairStats(context.storage, {
        targetModel,
        userEventThreadIds: scan.userEventThreadIds,
        threadCwdById: scan.threadCwdById
      })
    : null;
  const workspaceStats = targets.includes("workspaceRoots")
    ? await readWorkspaceRootRepairStats(context.storage, {
        cwdStats: cwdStatsFromThreadCwdMap(scan.threadCwdById)
      })
    : null;
  const warnings = lockedCount > 0
    ? [`${lockedCount} rollout file(s) are currently locked and may produce a partial result.`]
    : [];
  const sqliteRowsToChange = repairSqliteRowsToChange(sqliteStats, targets);
  const workspaceRootsToChange = workspaceStats?.workspaceRootsNeedingRepair ?? 0;
  const summary = {
    profile: { id: context.profile.id, revision: context.profile.revision },
    storageRevision: context.revisions.storageRevision,
    configRevision: context.revisions.configRevision,
    rolloutRevision: context.revisions.rolloutRevision,
    stateDbRevision: context.revisions.stateDbRevision,
    target: {
      targets,
      ...(targetModel ? { model: targetModel } : {})
    },
    impact: {
      rolloutFilesToChange: targets.includes("models") ? writableChanges.length : 0,
      sqliteRowsToChange,
      workspaceRootsToChange,
      lockedRolloutFiles: lockedCount,
      backupExpected: writableChanges.length > 0
        || sqliteRowsToChange > 0
        || workspaceRootsToChange > 0
    },
    warnings
  };
  return operationRuntime.issuePreparedPlan("repair", summary, {
    codexHome: context.codexHome,
    platform: options.platform,
    actor: "manual",
    executionOptions: {
      codexHome: context.codexHome,
      ...(context.sqliteHome ? { sqliteHome: context.sqliteHome } : {}),
      targets,
      keepCount,
      sqliteBusyTimeoutMs: options.sqliteBusyTimeoutMs,
      onProgress: options.onProgress,
      platform: options.platform,
      faultInjector: options.faultInjector,
      signal: options.signal
    },
    expectedPlanState: {
      profile: context.profile,
      profileResolver: options.profileResolver,
      revisions: context.revisions,
      rolloutRevisionMode
    },
    statusOptions: {
      codexHome: context.codexHome,
      ...(context.sqliteHome ? { sqliteHome: context.sqliteHome } : {}),
      profileId: context.profile.id,
      profileRevision: context.profile.suppliedRevision,
      rolloutScanMode: "metadata",
      platform: options.platform
    }
  });
}

export async function prepareRepair(options = {}) {
  return prepareRepairPlan(options);
}

export async function executeRepair({ targets, ...options } = {}) {
  const normalizedTargets = normalizeRepairTargets(targets);
  return executeOrdinaryWrite(
    { ...options, operationKind: "repair" },
    (context) => buildRepairWriteProgram(context, normalizedTargets)
  );
}


export async function applyRepair(input, control) {
  return operationRuntime.applyPrepared(input, "repair", (options) => executeRepair(options), control);
}

export function createRepairUseCase() {
  return Object.freeze({ prepareRepair, applyRepair });
}
