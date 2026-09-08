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
import { emitProgress, throwIfAborted } from "./runtime-support.js";
import { normalizeRepairSessionIds, normalizeRepairTargets, repairSqliteRowsToChange } from "./repair-targets.js";
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

const SAFE_MODEL_ID = /^[A-Za-z0-9._:/-]{1,160}$/;
function buildRepairPreview({ scan, sqliteStats, targets, targetModel }) {
  const byId = new Map();
  const affectedIds = new Set(sqliteStats?.affectedSessionIds ?? []);
  const add = (id, change) => {
    if (typeof id !== "string" || !id) return;
    const entry = byId.get(id) ?? { sessionId: id, changes: [] };
    if (!entry.changes.some((item) => item.target === change.target)) entry.changes.push(change);
    byId.set(id, entry);
  };
  if (targets.includes("models")) for (const change of scan.changes) {
    if (change.modelRewriteRequired) {
      affectedIds.add(change.threadId);
      add(change.threadId, { target: "models", before: SAFE_MODEL_ID.test(change.originalModel ?? "") ? change.originalModel : "different", after: SAFE_MODEL_ID.test(targetModel ?? "") ? targetModel : "target-config-model" });
    }
  }
  for (const row of sqliteStats?.previewRows ?? []) {
    if (targets.includes("models") && Object.hasOwn(row, "model") && typeof targetModel === "string" && row.model !== targetModel) add(row.id, { target: "models", before: SAFE_MODEL_ID.test(row.model ?? "") ? row.model : "different", after: SAFE_MODEL_ID.test(targetModel) ? targetModel : "target-config-model" });
    if (targets.includes("userEvent") && Object.hasOwn(row, "has_user_event") && scan.userEventThreadIds?.has(row.id) && Number(row.has_user_event) !== 1) add(row.id, { target: "userEvent", before: "false", after: "true" });
    if (targets.includes("cwd") && Object.hasOwn(row, "cwd") && scan.threadCwdById?.has(row.id) && row.cwd !== scan.threadCwdById.get(row.id)) add(row.id, { target: "cwd", before: "different", after: "rollout-cwd" });
  }
  const entries = [...byId.values()].sort((a, b) => a.sessionId.localeCompare(b.sessionId));
  return { entries: entries.slice(0, 100), total: affectedIds.size, truncated: affectedIds.size > 100 };
}

async function repairResult({ context, state, current, targets, targetModel, scan, initiallySkipped, workspaceStats, outcome, error, sessionIds }) {
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
  const result = {
    codexHome: context.codexHome,
    sqliteHome: context.storage.sqliteHome,
    sqliteHomeSource: context.storage.sqliteHomeSource,
    targetProvider: current.provider ?? DEFAULT_PROVIDER,
    repairTargets: targets,
    scope: sessionIds ? "selected" : "all",
    ...(sessionIds ? { sessionIds: [...sessionIds] } : {}),
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
  context.emitProgress({ stage: "verify_repair", status: "start" });
  try {
    const verifyScan = await collectRepairChanges(context.codexHome, targets, { skipLockedReads: true, targetModel, sessionIds });
    const verifyStats = context.storage.stateDbLocation ? await readSqliteRepairStats(context.storage, {
      targetModel, userEventThreadIds: verifyScan.userEventThreadIds, threadCwdById: verifyScan.threadCwdById, sessionIds
    }) : null;
    const remainingRolloutFiles = verifyScan.changes.length;
    const remainingSqliteRows = repairSqliteRowsToChange(verifyStats, targets);
    const remainingWorkspaceRoots = targets.includes("workspaceRoots")
      ? (await readWorkspaceRootRepairStats(context.storage, { cwdStats: cwdStatsFromThreadCwdMap(verifyScan.threadCwdById) }))?.workspaceRootsNeedingRepair ?? 0
      : 0;
    const skippedSessions = new Set([...initiallySkipped, ...(applyResult.skippedLockedPaths ?? []), ...(applyResult.skippedChangedPaths ?? []), ...verifyScan.lockedPaths]).size;
    const verifiedSessionIds = new Set([...(verifyScan.nativeSessionIds ?? []), ...(verifyStats?.selectedSessionIdsFound ?? [])]);
    const missingSelectedSession = sessionIds && [...sessionIds].some((id) => !verifiedSessionIds.has(id));
    result.verification = {
      status: verifyStats?.unreadable || missingSelectedSession ? "unavailable" : (remainingRolloutFiles || remainingSqliteRows || remainingWorkspaceRoots || skippedSessions ? "remaining" : "verified"),
      remainingRolloutFiles, remainingSqliteRows, remainingWorkspaceRoots, skippedSessions
    };
    if (result.verification.status !== "verified") {
      result.partial = true;
      result.retryRecommended = true;
      result.partialReason ??= result.verification.status === "unavailable" ? "verification-unavailable" : "verification-remaining";
    }
  } catch {
    result.verification = { status: "unavailable", remainingRolloutFiles: 0, remainingSqliteRows: 0, remainingWorkspaceRoots: 0, skippedSessions: 0 };
    result.partial = true;
    result.retryRecommended = true;
    result.partialReason ??= "verification-unavailable";
  }
  context.emitProgress({ stage: "verify_repair", status: "complete" });
  return result;
}

export async function buildRepairWriteProgram(context, targets, sessionIds = null) {
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
    targetModel,
    sessionIds
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
        threadCwdById: scan.threadCwdById,
        sessionIds
      })
    : null;
  const sqliteRowsToWrite = repairSqliteRowsToChange(sqliteStats, targets);
  if (sessionIds) {
    const found = new Set([...(scan.nativeSessionIds ?? []), ...(sqliteStats?.selectedSessionIdsFound ?? [])]);
    const missing = [...sessionIds].filter((id) => !found.has(id));
    if (missing.length) throw new CoreError("INVALID_INPUT", "One or more selected native session IDs no longer exist.");
  }
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
      scope: sessionIds ? "selected" : "all",
      ...(sessionIds ? { sessionIds: [...sessionIds] } : {}),
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
      verification: {
        status: sqliteRowsToWrite || writableChanges.length || workspaceMutationExpected || initiallySkipped.length ? "remaining" : "verified",
        remainingRolloutFiles: writableChanges.length,
        remainingSqliteRows: sqliteRowsToWrite,
        remainingWorkspaceRoots: workspaceStats?.workspaceRootsNeedingRepair ?? 0,
        skippedSessions: initiallySkipped.length
      },
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
                  sessionIds,
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
      error,
      sessionIds
    })
  };
}

export async function prepareRepairPlan(options) {
  // Request observers/signals are never retained in the subsequent Apply plan.
  const { onProgress, signal } = options.requestControl ?? {};
  const stage = (name) => {
    throwIfAborted(signal);
    emitProgress(onProgress, { stage: name, status: "running" });
  };
  stage("prepare_repair_context");
  const targets = normalizeRepairTargets(options.targets);
  const sessionIds = normalizeRepairSessionIds(options.sessionIds, targets);
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
    targetModel,
    sessionIds,
    onProgress,
    signal
  });
  stage("inspect_repair_sqlite");
  const { writableChanges, lockedChanges } = await splitLockedSessionChanges(scan.changes);
  const lockedCount = new Set([
    ...scan.lockedPaths,
    ...lockedChanges.map((change) => change.path)
  ]).size;
  const sqliteStats = context.storage.stateDbLocation
    ? await readSqliteRepairStats(context.storage, {
        targetModel,
        userEventThreadIds: scan.userEventThreadIds,
        threadCwdById: scan.threadCwdById,
        sessionIds
      })
    : null;
  if (sessionIds) {
    const found = new Set([...(scan.nativeSessionIds ?? []), ...(sqliteStats?.selectedSessionIdsFound ?? [])]);
    if ([...sessionIds].some((id) => !found.has(id))) {
      throw new CoreError("INVALID_INPUT", "One or more selected native session IDs no longer exist.");
    }
  }
  stage("inspect_workspace_roots");
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
  stage("build_repair_preview");
  const preview = buildRepairPreview({ scan, sqliteStats, targets, targetModel });
  const summary = {
    profile: { id: context.profile.id, revision: context.profile.revision },
    storageRevision: context.revisions.storageRevision,
    configRevision: context.revisions.configRevision,
    rolloutRevision: context.revisions.rolloutRevision,
    stateDbRevision: context.revisions.stateDbRevision,
    target: {
      targets,
      scope: sessionIds ? "selected" : "all",
      ...(sessionIds ? { sessionIds: [...sessionIds] } : {}),
      ...(targetModel ? { model: targetModel } : {})
    },
    impact: {
      rolloutFilesToChange: targets.includes("models") ? writableChanges.length : 0,
      sqliteRowsToChange,
      ...(targets.includes("models") ? { sqliteModelRowsToChange: sqliteStats?.modelRowsNeedingRepair ?? 0 } : {}),
      ...(targets.includes("cwd") ? { sqliteCwdRowsToChange: sqliteStats?.cwdRowsNeedingRepair ?? 0 } : {}),
      ...(targets.includes("userEvent") ? { sqliteUserEventRowsToChange: sqliteStats?.userEventRowsNeedingRepair ?? 0 } : {}),
      workspaceRootsToChange,
      ...(workspaceStats ? { workspaceSettingsChangeKinds: workspaceStats.workspaceSettingsChangeKinds ?? [] } : {}),
      lockedRolloutFiles: lockedCount,
      repairPreview: preview.entries,
      repairPreviewTotal: preview.total,
      repairPreviewTruncated: preview.truncated,
      backupExpected: writableChanges.length > 0
        || sqliteRowsToChange > 0
        || workspaceRootsToChange > 0
    },
    warnings
  };
  throwIfAborted(signal);
  emitProgress(onProgress, { stage: "build_repair_preview", status: "completed" });
  throwIfAborted(signal);
  return operationRuntime.issuePreparedPlan("repair", summary, {
    codexHome: context.codexHome,
    platform: options.platform,
    actor: "manual",
    executionOptions: {
      codexHome: context.codexHome,
      ...(context.sqliteHome ? { sqliteHome: context.sqliteHome } : {}),
      targets,
      ...(sessionIds ? { sessionIds: [...sessionIds] } : {}),
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
  const sessionIds = normalizeRepairSessionIds(options.sessionIds, normalizedTargets);
  return executeOrdinaryWrite(
    { ...options, operationKind: "repair" },
    (context) => buildRepairWriteProgram(context, normalizedTargets, sessionIds)
  );
}


export async function applyRepair(input, control) {
  return operationRuntime.applyPrepared(input, "repair", (options) => executeRepair(options), control);
}

export function createRepairUseCase() {
  return Object.freeze({ prepareRepair, applyRepair });
}
