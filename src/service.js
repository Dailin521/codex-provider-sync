import path from "node:path";
import fs from "node:fs/promises";

import {
  DEFAULT_BACKUP_RETENTION_COUNT,
  DEFAULT_PROVIDER,
  defaultBackupRoot
} from "./constants.js";
import { CoreError } from "./core-error.js";
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
  getBackupRecoveryCoverage,
  getBackupSummary,
  pruneBackups,
  refreshBackupInventory,
  restoreBackup,
  restoreGlobalStateFilesFromBackup
} from "./backup.js";
import { acquireLock } from "./locking.js";
import {
  applySessionChanges,
  collectSessionChanges,
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
  getAppliedJournalTargets,
  getStartedJournalTargets,
  readTransactionJournal,
  markBackupTransactionRolledBack
} from "./transaction-journal.js";

function pathComparisonKey(value) {
  const resolved = path.resolve(value);
  return process.platform === "win32" ? resolved.toLowerCase() : resolved;
}

function uniqueResolvedPaths(values) {
  const pathsByKey = new Map();
  for (const value of values) {
    if (typeof value !== "string" || !value) {
      continue;
    }
    const resolved = path.resolve(value);
    pathsByKey.set(pathComparisonKey(resolved), resolved);
  }
  return [...pathsByKey.values()];
}

export class SyncTransactionError extends CoreError {
  constructor(
    originalError,
    rollbackErrors,
    backupDir,
    completedTargets,
    uncompletedTargets,
    { rollbackStatus = "incomplete", recoveryRequired = true } = {}
  ) {
    const code = recoveryRequired ? "RECOVERY_REQUIRED" : "SYNC_FAILED_ROLLED_BACK";
    const message = recoveryRequired
      ? `Failed to restore state after sync error. Original error: ${originalError.message}. Restore error: ${rollbackErrors.join("; ")}`
      : `Provider sync failed and all observed changes were rolled back. Original error: ${originalError.message}`;
    const recoveryInstructions = recoveryRequired
      ? `Restore the managed backup at ${backupDir}, inspect the pending transaction journal, then retry.`
      : "No manual recovery is required. Inspect the original error, correct its cause, and retry.";
    super(code, message, {
      cause: originalError,
      recoveryRequired,
      suggestedAction: recoveryInstructions
    });
    this.name = "SyncTransactionError";
    this.originalError = originalError;
    this.rollbackErrors = rollbackErrors;
    this.backupDir = backupDir;
    this.completedTargets = completedTargets;
    this.uncompletedTargets = uncompletedTargets;
    this.rollbackStatus = rollbackStatus;
    this.recoveryRequired = recoveryRequired;
    this.recoveryInstructions = recoveryInstructions;
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

function emitProgress(onProgress, event) {
  if (typeof onProgress !== "function") {
    return;
  }
  try {
    const observerResult = onProgress(event);
    if (observerResult && typeof observerResult.then === "function") {
      observerResult.catch(() => {
        // Progress is an observer channel. Async observer failures must not
        // change transaction state or surface as unhandled rejections.
      });
    }
  } catch {
    // Progress is non-authoritative. A UI/CLI observer failure must never
    // trigger compensation before commit or turn a committed operation into
    // an apparent failure afterwards.
  }
}

async function commitJournalWithReconciliation(journal, faultInjector) {
  let acknowledgementError = null;
  try {
    await journal.committed();
    await faultInjector?.({ point: "after_transaction_journal_commit_before_ack" });
  } catch (error) {
    acknowledgementError = error;
  }

  let persisted;
  try {
    persisted = await readTransactionJournal(journal.filePath);
  } catch (readError) {
    if (acknowledgementError) {
      throw new AggregateError(
        [acknowledgementError, readError],
        `Unable to reconcile transaction commit acknowledgement: ${acknowledgementError.message}`,
        { cause: acknowledgementError }
      );
    }
    throw readError;
  }
  if (persisted.terminal
      && persisted.state === "committed"
      && persisted.operationId === journal.operationId) {
    return;
  }
  if (acknowledgementError) {
    throw acknowledgementError;
  }
  throw new Error(`Transaction journal did not persist a valid committed terminal state: ${journal.filePath}`);
}

// Rewrites the retained backup's recorded size and file count after the journal
// reached a terminal state, so status and pruning do not trust an inventory
// captured before those journal records existed. Used on the rollback paths,
// where the caller is already reporting a failure: a bookkeeping problem here
// must never replace the original error.
async function tryRefreshBackupInventory(backupDir) {
  try {
    await refreshBackupInventory(backupDir);
  } catch {
    // The original sync failure and its rollback details are the authoritative
    // diagnosis and must reach the caller unchanged.
  }
}

async function rollbackJournalWithReconciliation(journal, faultInjector) {
  let acknowledgementError = null;
  try {
    await journal.rolledBack();
    await faultInjector?.({ point: "after_transaction_journal_rollback_before_ack" });
  } catch (error) {
    acknowledgementError = error;
  }

  let persisted;
  try {
    persisted = await readTransactionJournal(journal.filePath);
  } catch (readError) {
    if (acknowledgementError) {
      throw new AggregateError(
        [acknowledgementError, readError],
        `Unable to reconcile transaction rollback acknowledgement: ${acknowledgementError.message}`,
        { cause: acknowledgementError }
      );
    }
    throw readError;
  }
  if (persisted.terminal
      && persisted.state === "rolledBack"
      && persisted.operationId === journal.operationId) {
    return;
  }
  if (acknowledgementError) {
    throw acknowledgementError;
  }
  throw new Error(`Transaction journal did not persist a valid rolledBack terminal state: ${journal.filePath}`);
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

export async function getStatus({
  codexHome: explicitCodexHome,
  sqliteHome,
  storage: providedStorage,
  configText: providedConfigText,
  platform
} = {}) {
  const codexHome = providedStorage?.codexHome ?? normalizeCodexHome(explicitCodexHome);
  const configPath = path.join(codexHome, "config.toml");
  const configText = providedConfigText ?? await readConfigText(configPath);
  const storage = await prepareStorage({ codexHome, sqliteHome, configText, storage: providedStorage, platform });
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

/** @deprecated Compatibility adapter. New transports must use prepareSync/applySync once available. */
export async function runSync(options = {}) {
  return runSyncCore(options);
}

async function runSyncCore({
  codexHome: explicitCodexHome,
  sqliteHome,
  storage: providedStorage,
  provider,
  configBackupText,
  expectedConfigText,
  keepCount = DEFAULT_BACKUP_RETENTION_COUNT,
  sqliteBusyTimeoutMs,
  onProgress,
  model = null,
  platform,
  faultInjector,
  signal
} = {}, { afterBackup } = {}) {
  if (!Number.isInteger(keepCount) || keepCount < 1) {
    throw new CoreError(
      "INVALID_INPUT",
      `Invalid automatic keep count: ${keepCount}. Expected an integer greater than or equal to 1.`
    );
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
    if (expectedConfigText !== undefined && configText !== expectedConfigText) {
      throw new CoreError(
        "PLAN_STALE",
        "config.toml changed after the operation was confirmed. Refresh and retry."
      );
    }
    if (configBackupText !== undefined && configText !== configBackupText) {
      throw new CoreError(
        "PLAN_STALE",
        "config.toml changed before the switch operation acquired its lock. Refresh and retry."
      );
    }
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
    const globalStatePresent = await fs.access(globalStatePath).then(() => true).catch(() => false);
    const sqliteTarget = storage.stateDbLocation?.path ?? null;
    const potentialTargets = uniqueResolvedPaths([
      ...writableChanges.map((change) => change.path),
      ...(globalStatePresent ? [globalStatePath, globalStateBackupPath] : []),
      ...(configBackupText !== undefined ? [configPath] : []),
      ...(sqliteTarget ? [sqliteTarget] : [])
    ]);
    journal = await TransactionJournal.create(backupDir, {
      codexHome,
      targetProvider,
      potentialTargets
    });

    let sessionRestoreNeeded = false;
    let appliedSessionChanges = [];
    let sqliteMutationCommitted = false;
    let sqliteCommitAttempted = false;
    let configMutationAttempted = false;
    const completedTargets = [];
    const completedTargetKeys = new Set();
    let transactionCommitted = false;
    const recordCompletedTarget = (targetPath) => {
      const fullPath = path.resolve(targetPath);
      const key = pathComparisonKey(fullPath);
      if (!completedTargetKeys.has(key)) {
        completedTargetKeys.add(key);
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
        configMutationAttempted = true;
        await afterBackup(backupDir);
        recordCompletedTarget(configPath);
        await faultInjector?.({ point: "after_config_mutation_before_applied", path: configPath });
        await journal.applied("config", configPath);
        await faultInjector?.({ point: "after_config_apply", path: configPath });
      }

      let applyResult = { appliedChanges: 0, appliedPaths: [], skippedPaths: [] };
      emitProgress(onProgress, { stage: "update_sqlite", status: "start" });
      emitProgress(onProgress, {
        stage: "rewrite_rollout_files",
        status: "start",
        writableCount: writableChanges.length
      });
      if (sqliteTarget) {
        await journal.applying("sqlite", sqliteTarget);
      }
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
              onMutation: async (change, mutation) => {
                recordCompletedTarget(change.path);
                await faultInjector?.({
                  point: "after_rollout_mutation_before_applied",
                  path: change.path,
                  mutation
                });
              },
              onApplied: async (change) => {
                appliedSessionChanges.push(change);
                sessionRestoreNeeded = true;
                await journal.applied("rollout", change.path);
                await faultInjector?.({ point: "after_rollout_apply", path: change.path, appliedCount: appliedSessionChanges.length });
              },
              onSkipped: async (change, reason) => {
                await journal.skipped("rollout", change.path);
                await faultInjector?.({ point: "after_rollout_skip", path: change.path, reason });
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
              recordCompletedTarget(targetPath);
              await journal.applied("globalState", targetPath);
              await faultInjector?.({ point: "after_global_state_apply", path: targetPath });
            }
          });
          throwIfAborted(signal);
        },
        {
          busyTimeoutMs: sqliteBusyTimeoutMs,
          userEventThreadIds,
          threadCwdById,
          targetModel: model,
          onCommitAttempt(result) {
            sqliteCommitAttempted = result.databasePresent && result.updatedRows > 0;
          },
          afterCommit: () => faultInjector?.({
            point: "after_sqlite_commit_before_ack",
            path: sqliteTarget
          })
        }
      );
      sqliteMutationCommitted = sqliteResult.databasePresent && sqliteResult.updatedRows > 0;
      if (sqliteMutationCommitted) {
        recordCompletedTarget(sqliteTarget);
      }
      throwIfAborted(signal);
      if (sqliteTarget) {
        if (sqliteMutationCommitted) {
          await journal.applied("sqlite", sqliteTarget);
        } else {
          await journal.skipped("sqlite", sqliteTarget);
        }
        await faultInjector?.({ point: "after_sqlite_commit", path: sqliteTarget });
      }
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
      const skippedLockedRolloutFiles = [...new Set([
        ...skippedRolloutFiles,
        ...applyResult.skippedPaths
      ])].sort((left, right) => left.localeCompare(right));
      throwIfAborted(signal);
      await faultInjector?.({ point: "before_transaction_commit", completedCount: completedTargets.length });
      await commitJournalWithReconciliation(journal, faultInjector);
      transactionCommitted = true;
      // The transaction is committed and every target is on disk. Refreshing the
      // inventory only corrects the recorded size and file count in
      // metadata.json, so a failure must degrade to a warning: throwing would
      // report a successful sync as failed and skip the pruning below.
      let backupInventoryWarning = null;
      try {
        await refreshBackupInventory(backupDir);
      } catch (inventoryError) {
        backupInventoryWarning = `Backup inventory refresh failed: ${inventoryError instanceof Error ? inventoryError.message : String(inventoryError)}`;
      }
      await faultInjector?.({ point: "after_transaction_commit", completedCount: completedTargets.length });
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
      autoPruneWarning = [backupInventoryWarning, autoPruneWarning]
        .filter((part) => typeof part === "string" && part.trim().length > 0)
        .map((part) => part.trim())
        .join(" | ") || null;
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
      return result;
    } catch (error) {
      if (transactionCommitted) {
        throw error;
      }
      try {
        const persistedTerminal = journal
          ? await readTransactionJournal(journal.filePath)
          : null;
        if (persistedTerminal?.terminal && persistedTerminal.state === "committed") {
          // A terminal commit is authoritative even if an observer or the
          // acknowledgement path failed before the in-memory flag advanced.
          // Never append rollback events or compensate committed state.
          throw error;
        }
      } catch (reconciliationError) {
        if (reconciliationError === error) {
          throw error;
        }
        // A journal read failure is handled by the recovery path below.
      }

      const restoreFailures = [];
      try {
        await journal?.rollingBack(error);
      } catch (journalError) {
        restoreFailures.push(`transaction journal: ${journalError.message}`);
      }
      let journalSnapshot = null;
      try {
        journalSnapshot = journal ? await readTransactionJournal(journal.filePath) : null;
      } catch (journalError) {
        restoreFailures.push(`transaction journal read: ${journalError.message}`);
      }
      const startedRolloutTargets = journalSnapshot
        ? getStartedJournalTargets(journalSnapshot, "rollout")
        : (sessionRestoreNeeded
          ? appliedSessionChanges.map((change) => change.path)
          : writableChanges.map((change) => change.path));
      const startedGlobalStateTargets = journalSnapshot
        ? getStartedJournalTargets(journalSnapshot, "globalState")
        : (globalStateRestoreNeeded || globalStatePresent
          ? [globalStatePath, globalStateBackupPath]
          : []);
      const startedConfigTargets = journalSnapshot
        ? getStartedJournalTargets(journalSnapshot, "config")
        : (configMutationAttempted ? [configPath] : []);

      if (startedRolloutTargets.length > 0) {
        try {
          await faultInjector?.({ point: "before_rollout_rollback", appliedCount: startedRolloutTargets.length });
          await restoreBackup(backupDir, storage, {
            restoreConfig: false,
            restoreGlobalState: false,
            restoreDatabase: false,
            restoreSessions: true,
            sessionTargetPaths: startedRolloutTargets
          });
        } catch (restoreError) {
          restoreFailures.push(`rollout files: ${restoreError.message}`);
        }
      }
      if (startedGlobalStateTargets.length > 0 && backupDir) {
        try {
          await faultInjector?.({ point: "before_global_state_rollback" });
          await restoreGlobalStateFilesFromBackup(backupDir, codexHome, {
            targetPaths: startedGlobalStateTargets
          });
        } catch (restoreError) {
          restoreFailures.push(`global state: ${restoreError.message}`);
        }
      }
      if (startedConfigTargets.length > 0 && configBackupText !== undefined) {
        try {
          await faultInjector?.({ point: "before_config_rollback", path: configPath });
          await writeConfigText(configPath, configBackupText);
        } catch (restoreError) {
          restoreFailures.push(`config: ${restoreError.message}`);
        }
      }
      const sqliteRestoreRequired = sqliteMutationCommitted || sqliteCommitAttempted;
      if (sqliteRestoreRequired && backupDir) {
        try {
          const sqliteTarget = storage.stateDbLocation?.path ?? storage.sqliteHome;
          await faultInjector?.({ point: "before_sqlite_rollback", path: sqliteTarget });
          await restoreBackup(backupDir, storage, {
            restoreConfig: false,
            restoreDatabase: true,
            restoreSessions: false
          });
        } catch (restoreError) {
          restoreFailures.push(`SQLite: ${restoreError.message}`);
        }
      }
      if (restoreFailures.length === 0) {
        try {
          if (journal) {
            await rollbackJournalWithReconciliation(journal, faultInjector);
          }
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
        await tryRefreshBackupInventory(backupDir);
        const persistedCompletedTargets = journalSnapshot
          ? uniqueResolvedPaths([...getAppliedJournalTargets(journalSnapshot), ...completedTargets])
          : uniqueResolvedPaths(completedTargets);
        const uncompletedTargets = uniqueResolvedPaths([
          ...startedRolloutTargets,
          ...startedGlobalStateTargets,
          ...startedConfigTargets,
          ...(sqliteRestoreRequired && sqliteTarget ? [sqliteTarget] : [])
        ]);
        throw new SyncTransactionError(
          error,
          restoreFailures,
          backupDir,
          persistedCompletedTargets,
          uncompletedTargets,
          { rollbackStatus: "incomplete", recoveryRequired: true }
        );
      }
      await tryRefreshBackupInventory(backupDir);
      const persistedCompletedTargets = journalSnapshot
        ? uniqueResolvedPaths([...getAppliedJournalTargets(journalSnapshot), ...completedTargets])
        : uniqueResolvedPaths(completedTargets);
      throw new SyncTransactionError(
        error,
        [],
        backupDir,
        persistedCompletedTargets,
        [],
        { rollbackStatus: "complete", recoveryRequired: false }
      );
    }
  } finally {
    await releaseLock();
  }
}

/** @deprecated Compatibility adapter. New transports must use prepareSwitch/applySwitch once available. */
export async function runSwitch({
  codexHome: explicitCodexHome,
  sqliteHome,
  storage: providedStorage,
  expectedConfigText,
  provider,
  model,
  keepRootModel = false,
  keepCount = DEFAULT_BACKUP_RETENTION_COUNT,
  onProgress,
  platform,
  faultInjector,
  signal
}) {
  if (!provider) {
    throw new CoreError(
      "INVALID_INPUT",
      "Missing provider id. Usage: codex-provider switch <provider-id>"
    );
  }

  const codexHome = providedStorage?.codexHome ?? normalizeCodexHome(explicitCodexHome);
  const configPath = path.join(codexHome, "config.toml");
  const originalConfigText = await readConfigText(configPath);
  if (expectedConfigText !== undefined && originalConfigText !== expectedConfigText) {
    throw new CoreError(
      "PLAN_STALE",
      "config.toml changed after the operation was confirmed. Refresh and retry."
    );
  }
  const storage = await prepareStorage({ codexHome, sqliteHome, configText: originalConfigText, storage: providedStorage, platform });
  assertSqliteAccessSupported(storage, "switch");
  if (!storage.stateDbLocation && isConfiguredSqliteHome(storage)) {
    throw missingConfiguredStateDbError(storage);
  }
  if (!configDeclaresProvider(originalConfigText, provider)) {
    throw new CoreError(
      "INVALID_INPUT",
      `Provider "${provider}" is not available in config.toml. Configure it first or use one of: ${listConfiguredProviderIds(originalConfigText).join(", ")}`
    );
  }

  if (model !== undefined && model !== null && keepRootModel) {
    throw new CoreError("INVALID_INPUT", "--model and --keep-root-model are mutually exclusive. Pick one.");
  }

  let nextConfigText = setRootProviderInConfigText(originalConfigText, provider);
  let modelSync = { applied: false, source: "none", model: null, warning: null };

  if (model !== undefined && model !== null) {
    if (typeof model !== "string" || model.length === 0) {
      throw new CoreError(
        "INVALID_INPUT",
        `Invalid --model value: ${model}. Expected a non-empty string.`
      );
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
      model: modelForThreads,
      faultInjector,
      signal
    },
    {
      afterBackup: async () => {
        emitProgress(onProgress, {
          stage: "update_config",
          status: "start",
          provider
        });
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
}

/** @deprecated Compatibility adapter. New transports must use prepareRestore/applyRestore once available. */
export async function runRestore({
  codexHome: explicitCodexHome,
  sqliteHome,
  storage: providedStorage,
  expectedConfigText,
  backupDir,
  restoreConfig = true,
  restoreDatabase = true,
  restoreSessions = true,
  allowSqliteHomeRelocation = false,
  platform,
  faultInjector
}) {
  if (!backupDir) {
    throw new CoreError(
      "INVALID_INPUT",
      "Missing backup path. Usage: codex-provider restore <backup-dir>"
    );
  }
  const codexHome = providedStorage?.codexHome ?? normalizeCodexHome(explicitCodexHome);
  if (allowSqliteHomeRelocation && !(typeof sqliteHome === "string" && sqliteHome.trim())) {
    throw new CoreError(
      "INVALID_INPUT",
      "--allow-sqlite-home-relocation requires an explicit --sqlite-home path."
    );
  }
  const releaseLock = await acquireLock(codexHome, "restore");
  try {
    const configText = await readConfigText(path.join(codexHome, "config.toml"));
    if (expectedConfigText !== undefined && configText !== expectedConfigText) {
      throw new CoreError(
        "PLAN_STALE",
        "config.toml changed after the operation was confirmed. Refresh and retry."
      );
    }
    const storage = await prepareStorage({ codexHome, sqliteHome, configText, storage: providedStorage, platform });
    assertSqliteAccessSupported(storage, "restore");
    if (restoreDatabase && !storage.stateDbLocation && isConfiguredSqliteHome(storage)) {
      throw missingConfiguredStateDbError(storage);
    }
    const normalizedBackupDir = path.resolve(backupDir);
    let boundJournal = null;
    try {
      boundJournal = await readTransactionJournal(
        path.join(normalizedBackupDir, "transaction-journal.jsonl")
      );
    } catch (error) {
      if (error?.code !== "ENOENT") {
        throw error;
      }
    }
    if (boundJournal && !boundJournal.terminal) {
      const journalUncertain = boundJournal.invalidTail || boundJournal.events.length === 0;
      let conservativeCoverage = null;
      if (journalUncertain) {
        try {
          conservativeCoverage = await getBackupRecoveryCoverage(normalizedBackupDir, storage);
        } catch (coverageError) {
          const recoveryError = new CoreError(
            "RECOVERY_REQUIRED",
            coverageError instanceof Error ? coverageError.message : String(coverageError),
            {
              cause: coverageError instanceof Error ? coverageError : undefined,
              suggestedAction: "Restore the complete transaction-bound backup before retrying."
            }
          );
          recoveryError.backupDir = normalizedBackupDir;
          throw recoveryError;
        }
      }
      const missingKinds = [];
      if ((getStartedJournalTargets(boundJournal, "rollout").length > 0
          || conservativeCoverage?.sessions) && !restoreSessions) {
        missingKinds.push("rollout sessions");
      }
      if ((getStartedJournalTargets(boundJournal, "sqlite").length > 0
          || conservativeCoverage?.database) && !restoreDatabase) {
        missingKinds.push("SQLite database");
      }
      if ((getStartedJournalTargets(boundJournal, "config").length > 0
          || conservativeCoverage?.config) && !restoreConfig) {
        missingKinds.push("config.toml");
      }
      if ((getStartedJournalTargets(boundJournal, "globalState").length > 0
          || conservativeCoverage?.globalState) && !restoreConfig) {
        missingKinds.push("global state");
      }
      if (missingKinds.length > 0) {
        const error = new CoreError(
          "RECOVERY_REQUIRED",
          `Partial restore would leave a pending transaction unresolved. Include: ${missingKinds.join(", ")}.`,
          { suggestedAction: "Include every affected target kind in the explicit recovery restore." }
        );
        error.backupDir = normalizedBackupDir;
        error.missingRestoreKinds = missingKinds;
        throw error;
      }
    }
    const result = await restoreBackup(normalizedBackupDir, storage, {
      restoreConfig,
      restoreDatabase,
      restoreSessions,
      allowSqliteHomeRelocation
    });
    await markBackupTransactionRolledBack(normalizedBackupDir);
    // The restore and its journal marker are already durable. Refreshing the
    // inventory only corrects metadata.json bookkeeping, so surface a failure as
    // a warning instead of reporting a completed restore as failed.
    try {
      await refreshBackupInventory(normalizedBackupDir, { faultInjector });
    } catch (inventoryError) {
      return {
        ...result,
        backupInventoryWarning: `Backup inventory refresh failed: ${inventoryError instanceof Error ? inventoryError.message : String(inventoryError)}`
      };
    }
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
    throw new CoreError(
      "INVALID_INPUT",
      `Invalid keep count: ${keepCount}. Expected a non-negative integer.`
    );
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
