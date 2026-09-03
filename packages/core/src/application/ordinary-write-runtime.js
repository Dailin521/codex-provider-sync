// @ts-nocheck

// Generic executor for ordinary convergent writes. It owns admission,
// persistence boundaries and outcome handling only. A use case supplies a
// WriteProgram after its own scan/preflight has determined the actual targets.
import {
  DEFAULT_BACKUP_RETENTION_COUNT,
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
import { emitProgress, prepareStorage, throwIfAborted } from "./runtime-support.js";
import { undoBackup } from "./runtime-context.js";

const { readConfigText } = codexStorage.config;
const { assertSqliteWritable } = codexStorage.stateDb;
const WRITE_TARGET_KINDS = Object.freeze(["config", "rollout", "globalState", "sqlite"]);

function hasTargets(targetKinds) {
  return WRITE_TARGET_KINDS.some((kind) => targetKinds[kind]);
}

function assertWriteProgram(program) {
  if (!program || typeof program !== "object"
      || !program.targetKinds || typeof program.targetKinds !== "object"
      || typeof program.noMutationResult !== "function"
      || typeof program.toResult !== "function") {
    throw new TypeError("WriteProgram is incomplete.");
  }
  for (const kind of WRITE_TARGET_KINDS) {
    if (typeof program.targetKinds[kind] !== "boolean") {
      throw new TypeError(`WriteProgram targetKinds.${kind} must be a boolean.`);
    }
    const step = program.steps?.[kind];
    if (Boolean(step) !== program.targetKinds[kind]) {
      throw new TypeError(`WriteProgram targetKinds.${kind} must match its mutation step.`);
    }
    if (step && (typeof step !== "object"
        || typeof step.stage !== "string" || !step.stage
        || typeof step.run !== "function")) {
      throw new TypeError(`WriteProgram ${kind} step is invalid.`);
    }
  }
}

async function tryRefreshBackupInventory(backupDir) {
  try {
    await undoBackup.refreshInventory(backupDir);
  } catch {
    // Bookkeeping never replaces the mutation diagnosis.
  }
}

/**
 * Execute a use-case supplied WriteProgram under the common lightweight
 * ordinary-write protocol. The builder and each step are intentionally the
 * only location allowed to know what data is scanned or mutated.
 */
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
  expectedPlanState,
  operationKind = "sync"
} = {}, buildProgram) {
  if (typeof buildProgram !== "function") {
    throw new TypeError("executeOrdinaryWrite requires a WriteProgram builder.");
  }
  if (!Number.isInteger(keepCount) || keepCount < 1) {
    throw new CoreError(
      "INVALID_INPUT",
      `Invalid automatic keep count: ${keepCount}. Expected an integer greater than or equal to 1.`
    );
  }
  if (typeof operationKind !== "string" || !operationKind) {
    throw new TypeError("executeOrdinaryWrite requires a non-empty operation kind.");
  }

  const codexHome = providedStorage?.codexHome ?? normalizeCodexHome(explicitCodexHome);
  const configPath = path.join(codexHome, "config.toml");
  const releaseLock = await acquireLock(codexHome, operationKind);
  const state = {
    backupDir: null,
    backupDurationMs: 0,
    mutationStarted: false,
    failedStage: "mutation",
    data: Object.create(null),
    outputs: Object.create(null),
    backupInventoryWarning: null,
    autoPruneResult: null,
    autoPruneWarning: null
  };
  try {
    throwIfAborted(signal);
    const configText = await readConfigText(configPath);
    if (!expectedPlanState && expectedConfigText !== undefined && configText !== expectedConfigText) {
      throw new CoreError("PLAN_STALE", "config.toml changed after confirmation. Refresh and retry.");
    }
    const storage = await prepareStorage({
      codexHome,
      sqliteHome,
      configText,
      storage: providedStorage,
      platform
    });
    assertSqliteAccessSupported(storage, operationKind);
    if (!storage.stateDbLocation && isConfiguredSqliteHome(storage)) {
      throw missingConfiguredStateDbError(storage);
    }
    await assertNoPendingRestoreTransactions(codexHome);
    await verifyExpectedPlanState({ expectedPlanState, codexHome, configText, storage, platform });

    const context = {
      codexHome,
      configPath,
      configText,
      storage,
      keepCount,
      sqliteBusyTimeoutMs,
      onProgress,
      platform,
      faultInjector,
      signal,
      emitProgress: (event) => emitProgress(onProgress, event),
      markMutation() {
        state.mutationStarted = true;
      }
    };
    const program = await buildProgram(context);
    assertWriteProgram(program);

    // A use case may need to turn an indeterminate scan (for example an
    // unreadable SQLite database) into a typed pre-mutation failure even when
    // no concrete target could be counted.
    if (typeof program.preflight === "function") await program.preflight({ context, state });
    if (!hasTargets(program.targetKinds)) {
      return program.noMutationResult({ context, state });
    }
    // Every ordinary write retains one common SQLite admission check. Even a
    // file-only convergence must fail before backup when the active State DB
    // is busy, so a later retry observes one coherent store.
    if (storage.stateDbLocation) {
      await assertSqliteWritable(storage, { busyTimeoutMs: sqliteBusyTimeoutMs });
    }
    throwIfAborted(signal);
    await faultInjector?.({ point: "before_backup" });
    context.emitProgress({
      stage: "create_backup",
      status: "start",
      writableCount: program.backup?.writableCount ?? 0
    });
    throwIfAborted(signal);
    const backupStartedAt = Date.now();
    state.backupDir = await undoBackup.capture({
      storage,
      codexHome,
      targetKinds: program.targetKinds,
      faultInjector,
      ...(program.backup ?? {})
    });
    state.backupDurationMs = Date.now() - backupStartedAt;
    context.emitProgress({
      stage: "create_backup",
      status: "complete",
      durationMs: state.backupDurationMs,
      backupDir: state.backupDir
    });
    // Cancellation closes at this exact point. Partial/retry is the ordinary
    // write recovery model once any supplied step starts mutating.
    throwIfAborted(signal);

    try {
      for (const key of ["config", "rollout", "globalState", "sqlite"]) {
        const step = program.steps?.[key];
        if (!step) continue;
        state.failedStage = step.stage;
        if (!step.silent) {
          context.emitProgress({ stage: step.stage, status: "start", ...(step.start ?? {}) });
        }
        state.outputs[key] = await step.run({ context, state });
        if (!step.silent) {
          context.emitProgress({
            stage: step.stage,
            status: "complete",
            ...(typeof step.complete === "function" ? step.complete(state.outputs[key]) : (step.complete ?? {}))
          });
        }
      }
    } catch (error) {
      if (!state.mutationStarted) throw error;
      await tryRefreshBackupInventory(state.backupDir);
      return program.toResult({ context, state, outcome: "partial", error });
    }

    try {
      await undoBackup.refreshInventory(state.backupDir, { faultInjector });
    } catch {
      state.backupInventoryWarning = "Backup inventory refresh did not complete.";
    }
    context.emitProgress({ stage: "clean_backups", status: "start", keepCount });
    try {
      state.autoPruneResult = await pruneManagedBackups(codexHome, keepCount);
    } catch {
      state.autoPruneWarning = "Automatic backup cleanup did not complete.";
    }
    context.emitProgress({
      stage: "clean_backups",
      status: "complete",
      deletedCount: state.autoPruneResult?.deletedCount ?? 0
    });
    return program.toResult({ context, state, outcome: "completed", error: null });
  } finally {
    await releaseLock();
  }
}
