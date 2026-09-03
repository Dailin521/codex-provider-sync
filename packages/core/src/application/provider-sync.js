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
import { operationRuntime, sqliteTransaction } from "./runtime-context.js";

const {
  applySessionChanges,
  collectProviderChanges,
  splitLockedSessionChanges,
  summarizeProviderCounts
} = codexStorage.sessions;
const { readCurrentProviderFromConfigText } = codexStorage.config;
const { assertSqliteWritable, readSqliteProviderCounts } = codexStorage.stateDb;

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

function sortedUnique(paths) {
  return [...new Set(paths)].sort((left, right) => left.localeCompare(right));
}

function providerResult({ context, state, current, targetProvider, scan, initiallySkipped, outcome, error }) {
  const applyResult = state.outputs.rollout ?? {
    appliedChanges: 0,
    inPlaceChanges: 0,
    skippedLockedPaths: [],
    skippedChangedPaths: []
  };
  const sqliteResult = state.outputs.sqlite ?? emptySqliteMutationResult(Boolean(context.storage.stateDbLocation));
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
    targetProvider,
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
    sqlitePresent: sqliteResult.databasePresent,
    rolloutCountsBefore: summarizeProviderCounts(scan.providerCounts),
    autoPruneResult: state.autoPruneResult,
    backupInventoryWarning: state.backupInventoryWarning,
    autoPruneWarning: state.autoPruneWarning
  };
}

/**
 * Internal narrow seam for Switch and Watch. It owns the provider scan and
 * its concrete mutation steps; callers can only add a config step before the
 * provider convergence work. It is deliberately not a CoreFacade API.
 */
export async function buildProviderWriteProgram(context, settings = {}) {
  const current = readCurrentProviderFromConfigText(context.configText);
  const configStep = settings.createConfigStep
    ? await settings.createConfigStep(context)
    : null;
  const targetProvider = configStep?.targetProvider
    ?? settings.targetProvider
    ?? current.provider
    ?? DEFAULT_PROVIDER;

  context.emitProgress({ stage: "scan_rollout_files", status: "start" });
  const scan = await collectProviderChanges(context.codexHome, targetProvider, { skipLockedReads: true });
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

  const sqliteCounts = context.storage.stateDbLocation
    ? await readSqliteProviderCounts(context.storage)
    : null;
  const sqliteRowsToWrite = sqliteProviderRowsToChange(sqliteCounts, targetProvider);
  const targetKinds = {
    config: Boolean(configStep),
    rollout: writableChanges.length > 0,
    globalState: false,
    sqlite: sqliteRowsToWrite > 0
  };

  return {
    targetKinds,
    // An unreadable database is still an admission failure even when a count
    // cannot prove rows need changing; preserve the prior direct preflight.
    preflight: sqliteCounts?.unreadable === true
      ? async () => assertSqliteWritable(context.storage, { busyTimeoutMs: context.sqliteBusyTimeoutMs })
      : null,
    backup: {
      targetProvider,
      sessionChanges: writableChanges,
      writableCount: writableChanges.length,
      configPath: context.configPath,
      ...(configStep ? { configBackupText: configStep.configBackupText } : {})
    },
    noMutationResult: () => ({
      codexHome: context.codexHome,
      sqliteHome: context.storage.sqliteHome,
      sqliteHomeSource: context.storage.sqliteHomeSource,
      targetProvider,
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
      sqlitePresent: Boolean(context.storage.stateDbLocation),
      rolloutCountsBefore: summarizeProviderCounts(scan.providerCounts),
      autoPruneResult: null,
      autoPruneWarning: null
    }),
    steps: {
      ...(configStep
        ? {
            config: {
              stage: "update_config",
              start: configStep.start,
              complete: configStep.complete,
              run: async ({ context: writeContext }) => {
                await configStep.run({ context: writeContext });
                writeContext.markMutation();
                await writeContext.faultInjector?.({
                  point: "after_config_mutation_before_applied",
                  path: writeContext.configPath
                });
                return { updated: true };
              }
            }
          }
        : {}),
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
      ...(sqliteRowsToWrite > 0
        ? {
            sqlite: {
              stage: "update_sqlite",
              complete: (result) => ({ updatedRows: result.updatedRows }),
              run: async ({ context: writeContext }) => {
                const result = await sqliteTransaction.updateProvider(writeContext.storage, targetProvider, {
                  busyTimeoutMs: writeContext.sqliteBusyTimeoutMs,
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
    toResult: ({ state, outcome, error }) => providerResult({
      context,
      state,
      current,
      targetProvider,
      scan,
      initiallySkipped,
      outcome,
      error
    })
  };
}

export async function executeProviderSyncMutation(options = {}, settings = {}) {
  return executeOrdinaryWrite(
    { ...options, operationKind: settings.operationKind ?? "sync" },
    (context) => buildProviderWriteProgram(context, settings)
  );
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
      lockedRolloutFiles: lockedCount,
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
