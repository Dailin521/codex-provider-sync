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
import { sqliteProviderRowsToChange } from "./provider-counts.js";
import { operationRuntime } from "./runtime-context.js";

const { collectProviderChanges, splitLockedSessionChanges } = codexStorage.sessions;
const { readCurrentProviderFromConfigText } = codexStorage.config;

export async function executeProviderSyncMutation(options = {}, settings = {}) {
  return executeOrdinaryWrite(options, settings);
}

export async function prepareProviderPlan(operation, options, switchIntent = null) {
  const keepCount = options.keepCount ?? DEFAULT_BACKUP_RETENTION_COUNT;
  if (!Number.isInteger(keepCount) || keepCount < 1) {
    throw new CoreError(
      "INVALID_INPUT",
      `Invalid automatic keep count: ${keepCount}. Expected an integer greater than or equal to 1.`
    );
  }
  const context = await preparePlanContext({ ...options, rolloutScanMode: "metadata" }, operation);
  if (!context.storage.stateDbLocation && isConfiguredSqliteHome(context.storage)) {
    throw missingConfiguredStateDbError(context.storage);
  }
  const current = readCurrentProviderFromConfigText(context.configText);
  const targetProvider = switchIntent?.provider
    ?? current.provider
    ?? DEFAULT_PROVIDER;
  const scan = await collectProviderChanges(context.codexHome, targetProvider, {
    skipLockedReads: true
  });
  const { writableChanges, lockedChanges } = await splitLockedSessionChanges(scan.changes);
  const lockedCount = new Set([
    ...scan.lockedPaths,
    ...lockedChanges.map((change) => change.path)
  ]).size;
  const warnings = [];
  if (lockedCount > 0) {
    warnings.push(`${lockedCount} rollout file(s) are currently locked and may produce a partial result.`);
  }
  if (switchIntent?.modelSync.warning) warnings.push(switchIntent.modelSync.warning);

  const sqliteRowsToChange = sqliteProviderRowsToChange(context.status.sqliteCounts, targetProvider);
  const summary = {
    profile: { id: context.profile.id, revision: context.profile.revision },
    storageRevision: context.revisions.storageRevision,
    configRevision: context.revisions.configRevision,
    rolloutRevision: context.revisions.rolloutRevision,
    stateDbRevision: context.revisions.stateDbRevision,
    target: {
      provider: targetProvider,
      ...(switchIntent
        ? { model: switchIntent.rootModel, modelMode: switchIntent.modelMode }
        : {})
    },
    impact: {
      rolloutFilesToChange: writableChanges.length,
      sqliteRowsToChange,
      lockedRolloutFiles: context.revisions.lockedRolloutFiles,
      backupExpected: writableChanges.length > 0
        || sqliteRowsToChange > 0
        || switchIntent?.configMutationExpected === true
    },
    warnings
  };
  const executionOptions = operation === "switch"
    ? {
        codexHome: context.codexHome,
        ...(context.sqliteHome ? { sqliteHome: context.sqliteHome } : {}),
        provider: switchIntent.provider,
        model: options.model,
        keepRootModel: Boolean(options.keepRootModel),
        keepCount,
        onProgress: options.onProgress,
        platform: options.platform,
        faultInjector: options.faultInjector,
        signal: options.signal
      }
    : {
        codexHome: context.codexHome,
        ...(context.sqliteHome ? { sqliteHome: context.sqliteHome } : {}),
        keepCount,
        sqliteBusyTimeoutMs: options.sqliteBusyTimeoutMs,
        onProgress: options.onProgress,
        platform: options.platform,
        faultInjector: options.faultInjector,
        signal: options.signal
      };
  return operationRuntime.issuePreparedPlan(operation, summary, {
    codexHome: context.codexHome,
    platform: options.platform,
    actor: options.__actor === "watch" ? "watch" : "manual",
    executionOptions,
    expectedPlanState: {
      profile: context.profile,
      profileResolver: options.profileResolver,
      revisions: context.revisions,
      rolloutRevisionMode: "metadata"
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

export async function prepareSync(options = {}) {
  for (const removed of ["provider", "model", "fast", "syncMode"]) {
    if (Object.hasOwn(options, removed)) {
      throw new CoreError("INVALID_INPUT", `prepareSync no longer accepts ${removed}.`);
    }
  }
  return prepareProviderPlan("sync", options);
}


export async function applySync(input, control) {
  return operationRuntime.applyPrepared(input, "sync", (options) => executeProviderSyncMutation(options), control);
}

// Internal Watch seam. It preserves Watch's manual-intent arbitration without
// exposing the facade or a generic transport surface to the scheduler.
export async function prepareWatchProviderSync(options = {}) {
  return prepareSync({ ...options, __actor: "watch" });
}

export async function applyWatchProviderSync(planId) {
  return applySync({ schemaVersion: 1, planId });
}

export function createProviderSyncUseCase() {
  return Object.freeze({ prepareSync, applySync });
}
