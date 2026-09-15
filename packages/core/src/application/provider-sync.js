// @ts-nocheck

import {
  DEFAULT_BACKUP_RETENTION_COUNT,
  DEFAULT_PROVIDER,
  CoreError,
  withFailureStage,
  isConfiguredSqliteHome,
  missingConfiguredStateDbError,
  codexStorage,
  summarizeSkips, rolloutSkip, uniqueSkips, selectProviderRows, updateSessionBackupManifest
} from "../infrastructure/node-core-ports.js";
import { executeOrdinaryWrite } from "./ordinary-write-runtime.js";
import { preparePlanContext } from "./plan-context.js";
import { inspectSessionUsage } from "./session-usage.js";
import { operationRuntime, sqliteTransaction } from "./runtime-context.js";

const {
  applySessionChanges,
  collectProviderPreparationFacts,
  summarizeProviderCounts
} = codexStorage.sessions;
const { readCurrentProviderFromConfigText, configDeclaresProvider } = codexStorage.config;
const { assertSqliteWritable, readSqliteProviderCounts, readSqliteProviderRevisionState } = codexStorage.stateDb;

function assertConfiguredProvider(configText, provider) {
  if (!configDeclaresProvider(configText, provider)) {
    throw new CoreError("INVALID_INPUT", "The current Provider is not defined in config.toml. Configure it with your Provider tool before syncing.", {
      details: { reason: "provider-not-configured" }
    });
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

function sortedUnique(paths) {
  return [...new Set(paths)].sort((left, right) => left.localeCompare(right));
}

function providerResult({ context, state, current, targetProvider, scan, initiallySkipped, outcome, error }) {
  const applyResult = state.outputs.rollout ?? state.data.rolloutResult ?? {
    appliedChanges: 0,
    inPlaceChanges: 0,
    skippedLockedPaths: [],
    skippedChangedPaths: []
  };
  const sqliteResult = state.outputs.sqlite ?? state.data.sqliteResult ?? emptySqliteMutationResult(Boolean(context.storage.stateDbLocation));
  const skippedLockedRolloutFiles = sortedUnique([
    ...initiallySkipped,
    ...(applyResult.skippedLockedPaths ?? [])
  ]);
  const skippedChangedRolloutFiles = sortedUnique([...(scan.skippedItems ?? []).filter(item => ["changed", "missing"].includes(item.reason)).map(item => item.path), ...(applyResult.skippedChangedPaths ?? [])]);
  const skipSummary = summarizeSkips([...(scan.skippedItems ?? []),
    ...initiallySkipped.map(filePath => rolloutSkip(filePath, "locked", "revalidate")),
    ...(state.data.skippedItems ?? []), ...(sqliteResult.skippedItems ?? [])], (state.data.unconfirmed ?? 0) + (state.data.sqliteUnconfirmed ?? 0));
  const partialFromSessions = skipSummary.total > 0;
  const partialFailure = outcome === "partial";
  return {
    codexHome: context.codexHome,
    sqliteHome: context.storage.sqliteHome,
    sqliteHomeSource: context.storage.sqliteHomeSource,
    targetProvider,
    configUpdated: state.outputs.config?.updated === true || state.data.configUpdated === true,
    skipSummary,
    unconfirmedSessionFiles: state.data.unconfirmed ?? 0,
    previousProvider: current.provider,
    backupDir: state.backupDir,
    backupDurationMs: state.backupDurationMs,
    ...(state.data.fileUpdateTiming ? { fileUpdateTiming: state.data.fileUpdateTiming } : {}),
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
            : (skippedChangedRolloutFiles.length > 0 ? "rollout-changed" : (partialFromSessions ? "skipped-data" : null)),
          retryRecommended: skipSummary.retryRecommended
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
    backupScopeWarning: state.data.backupScopeWarning ?? null,
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

  assertConfiguredProvider(context.configText, targetProvider);

  context.emitProgress({ stage: "scan_rollout_files", status: "start" });
  const facts = await withFailureStage("scan_rollout_files", () => collectProviderPreparationFacts(context.codexHome, targetProvider, {
    expectedFiles: context.expectedPlanState?.providerScope?.files ?? null
  }));
  const scan = facts.scan;
  context.emitProgress({ stage: "check_locked_rollout_files", status: "start" });
  const { writableChanges, lockedPaths: initiallySkipped } = await withFailureStage("check_locked_rollout_files", () => inspectSessionUsage(scan));
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
    ? await withFailureStage("preflight_sqlite", () => readSqliteProviderCounts(context.storage))
    : null;
  const sqliteState = context.expectedPlanState?.providerScope?.sqliteState
    ?? (context.storage.stateDbLocation ? await readSqliteProviderRevisionState(context.storage.stateDbLocation.path) : { rows: [] });
  const selection = selectProviderRows(context.codexHome, scan, sqliteState, targetProvider,
    initiallySkipped.map(filePath => rolloutSkip(filePath, "locked", "revalidate")));
  if (context.expectedPlanState?.providerScope?.eligibleRowIds) {
    const eligible = new Set(context.expectedPlanState.providerScope.eligibleRowIds);
    selection.rows = selection.rows.filter(row => eligible.has(String(row.id)));
  }
  const expectedRowIds = new Set(sqliteState.rows.map(row => String(row.id)));
  if (context.expectedPlanState?.providerScope && context.storage.stateDbLocation) {
    const currentState = await readSqliteProviderRevisionState(context.storage.stateDbLocation.path);
    if (currentState.schema !== sqliteState.schema || JSON.stringify(currentState.identity) !== JSON.stringify(sqliteState.identity)) {
      throw new CoreError("STALE_STATE", "The thread index changed before backup.", { details: { reason: "state-db" } });
    }
    const currentRows = new Map(currentState.rows.map(row => [String(row.id), row]));
    const changedRows = new Set();
    for (const expected of sqliteState.rows) {
      const id = String(expected.id);
      const row = currentRows.get(id);
      if (!row || row.model_provider !== expected.model_provider || row.rollout_path !== expected.rollout_path) {
        changedRows.add(id);
        selection.skippedItems.push({ kind: "sqlite", id, reason: row ? "row-changed" : "row-missing", stage: "revalidate", retryable: true });
      }
    }
    selection.rows = selection.rows.filter(row => !changedRows.has(String(row.id)));
    for (const row of currentState.rows) if (!expectedRowIds.has(String(row.id)) && row.model_provider !== targetProvider) {
      selection.skippedItems.push({ kind: "sqlite", id: String(row.id), reason: "deferred", stage: "revalidate", retryable: true });
    }
  }
  const sqliteRowsToWrite = selection.rows.length;
  selection.skippedItems.push(...(context.expectedPlanState?.providerScope?.excludedRows ?? []));
  const initialSkips = uniqueSkips([...(scan.skippedItems ?? []),
    ...initiallySkipped.map(filePath => rolloutSkip(filePath, "locked", "revalidate")), ...selection.skippedItems]);
  const initialSummary = summarizeSkips(initialSkips);
  const definitelyUnwritten = new Set(writableChanges.map(change => change.path));
  const attemptedPaths = new Set();
  const appliedPaths = new Set();
  const writeSkips = [];
  const partialRollout = { appliedChanges: 0, inPlaceChanges: 0, appliedPaths: [], skippedPaths: [], skippedLockedPaths: [], skippedChangedPaths: [] };
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
      noop: initialSummary.total === 0,
      partial: initialSummary.total > 0,
      partialReason: initialSummary.total > 0 ? "skipped-data" : null,
      retryRecommended: initialSummary.retryRecommended,
      skipSummary: initialSummary,
      configUpdated: false,
      unconfirmedSessionFiles: 0,
      changedSessionFiles: 0,
      inPlaceSessionFiles: 0,
      rewrittenSessionFiles: 0,
      skippedLockedRolloutFiles: initiallySkipped,
      skippedChangedRolloutFiles: (scan.skippedItems ?? []).filter(item => ["changed", "missing"].includes(item.reason)).map(item => item.path),
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
              run: async ({ context: writeContext, state }) => {
                await configStep.run({ context: writeContext });
                writeContext.markMutation();
                state.data.configUpdated = true;
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
              run: async ({ context: writeContext, state }) => {
                state.data.rolloutResult = partialRollout;
                state.data.skippedItems = [...selection.skippedItems, ...writeSkips];
                try {
                  return await applySessionChanges(writableChanges, {
                    onTiming: timing => { state.data.fileUpdateTiming = timing; },
                    onBeforeApply: async change => {
                      await writeContext.faultInjector?.({ point: "before_rollout_apply", path: change.path });
                      attemptedPaths.add(change.path);
                      definitelyUnwritten.delete(change.path);
                    },
                    onMutation: async (change, mutation) => {
                      appliedPaths.add(change.path);
                      partialRollout.appliedPaths.push(change.path);
                      partialRollout.appliedChanges += 1;
                      if (mutation.result === "APPLIED_IN_PLACE") partialRollout.inPlaceChanges += 1;
                      writeContext.markMutation();
                      await writeContext.faultInjector?.({ point: "after_rollout_mutation_before_applied", path: change.path, mutation });
                    },
                    onUnwritten: change => { definitelyUnwritten.add(change.path); },
                    onApplied: change => writeContext.faultInjector?.({ point: "after_rollout_apply", path: change.path }),
                    onSkipped: async (change, result) => {
                      definitelyUnwritten.add(change.path);
                      const reason = result === "SKIP_BUSY" ? "locked" : result === "SKIP_MISSING" ? "missing"
                        : result === "SKIP_UNREADABLE" ? "unreadable" : result === "SKIP_NOT_APPLIED" ? "write-not-applied" : "changed";
                      const skip = rolloutSkip(change.path, reason, "write", change.threadId);
                      writeSkips.push(skip);
                      state.data.skippedItems.push(skip);
                      partialRollout.skippedPaths.push(change.path);
                      if (reason === "locked") partialRollout.skippedLockedPaths.push(change.path);
                      if (reason === "changed" || reason === "missing") partialRollout.skippedChangedPaths.push(change.path);
                      await writeContext.faultInjector?.({ point: "after_rollout_skip", path: change.path, reason: result });
                    }
                  });
                } catch (error) {
                  state.data.unconfirmed = [...attemptedPaths].filter(filePath => !appliedPaths.has(filePath) && !definitelyUnwritten.has(filePath)).length;
                  if (state.data.unconfirmed > 0) writeContext.markMutation();
                  throw error;
                }
              }
            }
          }
        : {}),
      ...(sqliteRowsToWrite > 0
        ? {
            sqlite: {
              stage: "update_sqlite",
              complete: (result) => ({ updatedRows: result.updatedRows }),
              run: async ({ context: writeContext, state }) => {
                const finalSelection = selectProviderRows(context.codexHome, scan, sqliteState, targetProvider,
                  [...initiallySkipped.map(filePath => rolloutSkip(filePath, "locked", "revalidate")), ...writeSkips]);
                const eligible = new Set(selection.rows.map(row => String(row.id)));
                finalSelection.rows = finalSelection.rows.filter(row => eligible.has(String(row.id)));
                state.data.skippedItems = uniqueSkips([...(state.data.skippedItems ?? []), ...selection.skippedItems, ...finalSelection.skippedItems]);
                const result = await sqliteTransaction.updateProvider(writeContext.storage, targetProvider, {
                  busyTimeoutMs: writeContext.sqliteBusyTimeoutMs,
                  plannedRows: finalSelection.rows,
                  expectedRows: sqliteState.rows,
                  expectedSchema: sqliteState.schema,
                  expectedIdentity: sqliteState.identity,
                  expectedRowIds: [...expectedRowIds],
                  onCommitAttempt: result => {
                    state.data.sqliteUnconfirmed = result.updatedRows;
                    if (result.updatedRows > 0) writeContext.markMutation();
                  },
                  afterCommit: async result => {
                    state.data.sqliteUnconfirmed = 0;
                    state.data.sqliteResult = result;
                    await writeContext.faultInjector?.({ point: "after_sqlite_commit_before_ack", path: writeContext.storage.stateDbLocation?.path ?? null });
                  }
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
    finalize: async ({ state }) => {
      state.data.skippedItems = uniqueSkips([...(state.data.skippedItems ?? []), ...selection.skippedItems]);
      if (!state.backupDir || writableChanges.length === 0) return;
      try {
        await updateSessionBackupManifest(state.backupDir, writableChanges.filter(change => !definitelyUnwritten.has(change.path)));
      } catch {
        state.data.backupScopeWarning = "Backup restore scope could not be narrowed; conservative restore coverage remains.";
      }
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
  const context = await preparePlanContext({ ...options, rolloutScanMode: "metadata" }, operation, {
    resolveProviderTarget(configText) {
      const provider = switchIntent?.provider ?? readCurrentProviderFromConfigText(configText).provider ?? DEFAULT_PROVIDER;
      assertConfiguredProvider(configText, provider);
      return provider;
    }
  });
  if (!context.storage.stateDbLocation && isConfiguredSqliteHome(context.storage)) {
    throw missingConfiguredStateDbError(context.storage);
  }
  const current = readCurrentProviderFromConfigText(context.configText);
  const targetProvider = switchIntent?.provider
    ?? current.provider
    ?? DEFAULT_PROVIDER;
  assertConfiguredProvider(context.configText, targetProvider);
  const scan = context.providerScan;
  const { writableChanges, lockedPaths } = await withFailureStage("prepare_usage", () => inspectSessionUsage(scan));
  const lockedCount = lockedPaths.length;
  const warnings = [];
  if (lockedCount > 0) {
    warnings.push(`${lockedCount} rollout file(s) are currently locked and may produce a partial result.`);
  }
  if (switchIntent?.modelSync.warning) warnings.push(switchIntent.modelSync.warning);

  const sqliteState = context.storage.stateDbLocation
    ? await readSqliteProviderRevisionState(context.storage.stateDbLocation.path) : { rows: [] };
  const selection = selectProviderRows(context.codexHome, scan, sqliteState, targetProvider,
    lockedPaths.map(filePath => rolloutSkip(filePath, "locked", "plan")));
  const skipSummary = summarizeSkips([...(scan.skippedItems ?? []),
    ...lockedPaths.map(filePath => rolloutSkip(filePath, "locked", "plan")), ...selection.skippedItems]);
  const sqliteRowsToChange = selection.rows.length;
  const lockedSet = new Set(lockedPaths);
  const providerFiles = context.providerFiles.map(file => lockedSet.has(file.path)
    ? { ...file, skip: file.skip ?? rolloutSkip(file.path, "locked", "plan", file.id) } : file);
  const summary = {
    profile: { id: context.profile.id, revision: context.profile.revision },
    storageRevision: context.revisions.storageRevision,
    configRevision: context.revisions.configRevision,
    rolloutRevision: context.revisions.rolloutRevision,
    stateDbRevision: context.revisions.stateDbRevision,
    target: {
      provider: targetProvider,
      ...(switchIntent
        ? {
            model: switchIntent.rootModel,
            modelMode: switchIntent.modelMode,
            previousProvider: switchIntent.previousProvider,
            previousRootModel: switchIntent.previousRootModel
          }
        : {})
    },
    impact: {
      rolloutFilesToChange: writableChanges.length,
      skipSummary,
      sqliteRowsToChange,
      lockedRolloutFiles: lockedCount,
      sessionActivity: context.status.sessionActivity,
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
      rolloutRevisionMode: context.rolloutRevisionMode,
      providerScope: { files: providerFiles, sqliteState, eligibleRowIds: selection.rows.map(row => String(row.id)), excludedRows: selection.skippedItems }
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
