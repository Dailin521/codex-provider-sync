// @ts-nocheck

import {
  DEFAULT_BACKUP_RETENTION_COUNT,
  CoreError,
  isConfiguredSqliteHome,
  missingConfiguredStateDbError,
  codexStorage
} from "../infrastructure/node-core-ports.js";
import { executeOrdinaryWrite } from "./ordinary-write-runtime.js";
import { preparePlanContext } from "./plan-context.js";
import { normalizeRepairTargets, repairSqliteRowsToChange } from "./repair-targets.js";
import { operationRuntime } from "./runtime-context.js";

const { collectRepairChanges, splitLockedSessionChanges } = codexStorage.sessions;
const { readRootModelFromConfigText, } = codexStorage.config;
const { readSqliteRepairStats } = codexStorage.stateDb;
const { cwdStatsFromThreadCwdMap, readWorkspaceRootRepairStats } = codexStorage.globalState;

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
  return executeOrdinaryWrite(options, {
    operationKind: "repair",
    repair: { targets: normalizedTargets }
  });
}


export async function applyRepair(input, control) {
  return operationRuntime.applyPrepared(input, "repair", (options) => executeRepair(options), control);
}

export function createRepairUseCase() {
  return Object.freeze({ prepareRepair, applyRepair });
}
