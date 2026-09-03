// @ts-nocheck

import {
  CoreError,
  acquireLock,
  assertSqliteAccessSupported,
  captureBackupRevision,
  captureStableRestoreSource,
  codexStorage,
  getBackupUndoTargets,
  findPendingTransactions,
  getBackupRecoveryCoverage,
  getStartedJournalTargets,
  isConfiguredSqliteHome,
  listBackups,
  missingConfiguredStateDbError,
  normalizeCodexHome,
  operationCoordinator,
  path,
  fs,
  readTransactionJournal,
  refreshBackupInventory,
  resolveRestoreStateDbTargetPath,
  restoreJournalCoverageIsComplete,
  restoreJournalMatchesPhysicalHome,
  restoreJournalMatchesSource
} from "../infrastructure/node-core-ports.js";
import { preparePlanContext, verifyExpectedPlanState } from "./plan-context.js";
import { physicalDirectoryComparisonKey, prepareStorage } from "./runtime-support.js";
import { restoreRecovery, operationRuntime } from "./runtime-context.js";

const { readConfigText } = codexStorage.config;

export async function executeRestore({
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
  faultInjector,
  signal,
  onProgress,
  expectedPlanState
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
  let stateDbResource = null;
  try {
    const configText = await readConfigText(path.join(codexHome, "config.toml"));
    if (!expectedPlanState && expectedConfigText !== undefined && configText !== expectedConfigText) {
      throw new CoreError(
        "PLAN_STALE",
        "config.toml changed after the operation was confirmed. Refresh and retry."
      );
    }
    const storage = await prepareStorage({ codexHome, sqliteHome, configText, storage: providedStorage, platform });
    assertSqliteAccessSupported(storage, "restore");
    const sourceBackup = await captureStableRestoreSource(backupDir, { platform });
    const normalizedBackupDir = sourceBackup.backupDir;
    const undoTargets = await getBackupUndoTargets(normalizedBackupDir, storage);
    const effectiveRestore = {
      config: restoreConfig && undoTargets.config,
      globalState: restoreConfig && undoTargets.globalState,
      database: restoreDatabase && undoTargets.sqlite,
      sessions: restoreSessions && undoTargets.rollout
    };
    const skippedNotCapturedTargetKinds = [
      ...(restoreConfig && !undoTargets.config ? ["config"] : []),
      ...(restoreConfig && !undoTargets.globalState ? ["globalState"] : []),
      ...(restoreDatabase && !undoTargets.sqlite ? ["sqlite"] : []),
      ...(restoreSessions && !undoTargets.rollout ? ["rollout"] : [])
    ];
    const restoreWarnings = skippedNotCapturedTargetKinds.map(
      (kind) => `Restore skipped ${kind}: the selected backup did not capture this target.`
    );
    if (effectiveRestore.database && !storage.stateDbLocation && isConfiguredSqliteHome(storage)) {
      throw missingConfiguredStateDbError(storage);
    }
    if (effectiveRestore.database) {
      const stateDbTargetPath = await resolveRestoreStateDbTargetPath(normalizedBackupDir, storage);
      stateDbResource = await restoreRecovery.resolveResource(stateDbTargetPath, { platform });
    }
    await verifyExpectedPlanState({
      expectedPlanState,
      codexHome,
      configText,
      storage,
      platform,
      backupDir: normalizedBackupDir
    });
    const requestedKinds = [
      ...(effectiveRestore.config ? ["config"] : []),
      ...(effectiveRestore.globalState ? ["globalState"] : []),
      ...(effectiveRestore.database ? ["sqlite"] : []),
      ...(effectiveRestore.sessions ? ["rollout"] : [])
    ];
    const pending = await findPendingTransactions(codexHome);
    const legacyPending = pending.filter((transaction) => transaction.operationKind !== "restore");
    const restorePending = pending.filter((transaction) => transaction.operationKind === "restore");
    const normalizedBackupKey = await physicalDirectoryComparisonKey(normalizedBackupDir);
    const foreignLegacy = [];
    for (const transaction of legacyPending) {
      const transactionBackupKey = await physicalDirectoryComparisonKey(transaction.backupDir);
      if (normalizedBackupKey === null || transactionBackupKey !== normalizedBackupKey) {
        foreignLegacy.push(transaction);
      }
    }
    const boundRestore = [];
    const foreignRestore = [];
    for (const transaction of restorePending) {
      const preparedSource = transaction.prepared?.sourceBackup;
      const physicalHomeMatches = await restoreJournalMatchesPhysicalHome(
        transaction,
        storage.codexHome,
        platform
      );
      const sourceMatches = await restoreJournalMatchesSource(
        transaction,
        sourceBackup,
        platform
      );
      const committedSourceLocationMatches = transaction.state === "committed-pending-ack"
        && preparedSource
        && await restoreJournalMatchesSource(
          transaction,
          { ...sourceBackup, revision: preparedSource.revision },
          platform
        );
      if ((sourceMatches
          || committedSourceLocationMatches)
          && physicalHomeMatches
          && restoreJournalCoverageIsComplete(transaction, requestedKinds)) {
        boundRestore.push(transaction);
      } else {
        foreignRestore.push(transaction);
      }
    }
    if (foreignLegacy.length > 0 || foreignRestore.length > 0) {
      throw new CoreError(
        "RECOVERY_REQUIRED",
        "An unrelated unfinished transaction must be resolved before this restore.",
        {
          details: { operationKind: "restore", foreignPendingCount: foreignLegacy.length + foreignRestore.length },
          suggestedAction: "Restore the transaction-bound managed backup before starting another write."
        }
      );
    }
    const committedPendingAck = boundRestore.filter(
      (transaction) => transaction.state === "committed-pending-ack" && !transaction.invalidTail
    );
    if (committedPendingAck.length > 0) {
      if (committedPendingAck.length !== 1 || boundRestore.length !== 1) {
        throw new CoreError(
          "RECOVERY_REQUIRED",
          "Multiple Restore acknowledgements cannot be reconciled automatically.",
          { details: { operationKind: "restore" } }
        );
      }
      const acknowledgement = await restoreRecovery.acknowledge(committedPendingAck[0], {
        faultInjector,
        stateDbResource,
        storage,
        onProgress,
        platform
      });
      const metadata = JSON.parse(
        await fs.readFile(path.join(normalizedBackupDir, "metadata.json"), "utf8")
      );
      return {
        ...metadata,
        restoreVersion: 2,
        restoreOperationId: acknowledgement.completed.operationId,
        preRestoreSnapshotId: acknowledgement.manifest.preRestoreSnapshot.backupId,
        restoreJournalState: "completed",
        commitAcknowledgementRecovered: true,
        resolvedOperationIds: acknowledgement.manifest.resolvesOperationIds ?? []
      };
    }
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
          || conservativeCoverage?.sessions) && !effectiveRestore.sessions) {
        missingKinds.push("rollout sessions");
      }
      if ((getStartedJournalTargets(boundJournal, "sqlite").length > 0
          || conservativeCoverage?.database) && !effectiveRestore.database) {
        missingKinds.push("SQLite database");
      }
      if ((getStartedJournalTargets(boundJournal, "config").length > 0
          || conservativeCoverage?.config) && !effectiveRestore.config) {
        missingKinds.push("config.toml");
      }
      if ((getStartedJournalTargets(boundJournal, "globalState").length > 0
          || conservativeCoverage?.globalState) && !effectiveRestore.globalState) {
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
    const result = await restoreRecovery.execute({
      storage,
      sourceBackup,
      restoreConfig: effectiveRestore.config,
      restoreGlobalState: effectiveRestore.globalState,
      restoreDatabase: effectiveRestore.database,
      restoreSessions: effectiveRestore.sessions,
      allowSqliteHomeRelocation,
      stateDbResource,
      resolvesOperationIds: boundRestore
        .map((transaction) => transaction.operationId)
        .filter((value) => typeof value === "string" && value.length > 0),
      faultInjector,
      signal,
      onProgress,
      platform
    });
    // The Restore and its journals are already durable. Refreshing the
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
    return restoreWarnings.length > 0
      ? { ...result, skippedNotCapturedTargetKinds, restoreWarnings }
      : result;
  } finally {
    await releaseLock();
  }
}


async function resolvePreparedBackup(options, codexHome) {
  if (typeof options.backupId === "string" && options.backupId) {
    const inventory = await listBackups(codexHome);
    const selected = inventory.backups.find((backup) => backup.id === options.backupId);
    if (!selected) {
      throw new CoreError("RESTORE_VALIDATION_FAILED", "The selected managed backup is unavailable.");
    }
    const source = await captureStableRestoreSource(selected.path, { platform: options.platform });
    return { ...source, metadata: selected.metadata };
  }
  if (typeof options.backupDir === "string" && options.backupDir) {
    const source = await captureStableRestoreSource(options.backupDir, { platform: options.platform });
    return { ...source, metadata: null };
  }
  throw new CoreError("INVALID_INPUT", "A managed backupId is required for Restore preparation.");
}

export async function prepareRestore(options = {}) {
  const restoreConfig = options.restoreConfig !== false;
  const restoreDatabase = options.restoreDatabase !== false;
  const restoreSessions = options.restoreSessions !== false;
  const hasBackupId = typeof options.backupId === "string" && Boolean(options.backupId.trim());
  const hasBackupDir = typeof options.backupDir === "string" && Boolean(options.backupDir.trim());
  if (!hasBackupId && !hasBackupDir) {
    throw new CoreError("INVALID_INPUT", "A managed backupId is required for Restore preparation.");
  }
  if (options.allowSqliteHomeRelocation
      && !(typeof options.sqliteHome === "string" && options.sqliteHome.trim())) {
    throw new CoreError(
      "INVALID_INPUT",
      "--allow-sqlite-home-relocation requires an explicit --sqlite-home path."
    );
  }
  const codexHome = options.storage?.codexHome ?? normalizeCodexHome(options.codexHome);
  if (operationCoordinator.isActive(codexHome, options.platform)) {
    throw new CoreError("OPERATION_BUSY", "Lock already exists for this Codex Home; another write operation is active.", {
      details: { busyScope: "codex-home" }
    });
  }
  // Validate config/storage/WSL state before opening any backup path. This
  // preserves the compatibility contract for stale confirmation and WSL UNC
  // diagnostics while the source is canonicalized immediately afterwards.
  const context = await preparePlanContext(options, "restore");
  const backup = await resolvePreparedBackup(options, codexHome);
  const undoTargets = await getBackupUndoTargets(backup.backupDir, context.storage);
  const effectiveRestore = {
    config: restoreConfig && undoTargets.config,
    globalState: restoreConfig && undoTargets.globalState,
    database: restoreDatabase && undoTargets.sqlite,
    sessions: restoreSessions && undoTargets.rollout
  };
  const skippedNotCapturedTargetKinds = [
    ...(restoreConfig && !undoTargets.config ? ["config"] : []),
    ...(restoreConfig && !undoTargets.globalState ? ["globalState"] : []),
    ...(restoreDatabase && !undoTargets.sqlite ? ["sqlite"] : []),
    ...(restoreSessions && !undoTargets.rollout ? ["rollout"] : [])
  ];
  const revisions = {
    ...context.revisions,
    backupRevision: await captureBackupRevision(backup.backupDir)
  };
  if (effectiveRestore.database && !context.storage.stateDbLocation && isConfiguredSqliteHome(context.storage)) {
    throw missingConfiguredStateDbError(context.storage);
  }
  if (effectiveRestore.database) {
    await resolveRestoreStateDbTargetPath(backup.backupDir, context.storage);
  }
  const warnings = [];
  for (const kind of skippedNotCapturedTargetKinds) {
    warnings.push(`Restore skipped ${kind}: the selected backup did not capture this target.`);
  }
  if (options.allowSqliteHomeRelocation) {
    if (restoreConfig) {
      throw new CoreError(
        "INVALID_INPUT",
        "Cannot restore config.toml while relocating SQLite home. Disable config restore to preserve the current target configuration."
      );
    }
    warnings.push("SQLite Home relocation is explicit; config.toml will not be restored.");
  }
  const summary = {
    profile: { id: context.profile.id, revision: context.profile.revision },
    storageRevision: revisions.storageRevision,
    configRevision: revisions.configRevision,
    rolloutRevision: revisions.rolloutRevision,
    stateDbRevision: revisions.stateDbRevision,
    backupRevision: revisions.backupRevision,
    target: {
      backupId: backup.backupId,
      restoreConfig,
      restoreDatabase,
      restoreSessions,
      allowSqliteHomeRelocation: Boolean(options.allowSqliteHomeRelocation)
    },
    impact: {
      rolloutFilesToChange: effectiveRestore.sessions ? (backup.metadata?.changedSessionFiles ?? 0) : 0,
      stateDbFilesToChange: effectiveRestore.database ? 1 : 0,
      configFilesToChange: effectiveRestore.config ? 1 : 0,
      lockedRolloutFiles: context.revisions.lockedRolloutFiles,
      backupExpected: true
    },
    warnings
  };
  return operationRuntime.issuePreparedPlan("restore", summary, {
    codexHome: context.codexHome,
    platform: options.platform,
    actor: "manual",
    executionOptions: {
      codexHome: context.codexHome,
      ...(context.sqliteHome ? { sqliteHome: context.sqliteHome } : {}),
      backupDir: backup.backupDir,
      restoreConfig,
      restoreDatabase,
      restoreSessions,
      allowSqliteHomeRelocation: Boolean(options.allowSqliteHomeRelocation),
      platform: options.platform,
      faultInjector: options.faultInjector,
      signal: options.signal,
      onProgress: options.onProgress
    },
    expectedPlanState: {
      profile: context.profile,
      profileResolver: options.profileResolver,
      revisions,
      rolloutRevisionMode: context.rolloutRevisionMode
    },
    sourceBackup: { backupId: backup.backupId, backupDir: backup.backupDir },
    statusOptions: {
      codexHome: context.codexHome,
      ...(context.sqliteHome ? { sqliteHome: context.sqliteHome } : {}),
      profileId: context.profile.id,
      profileRevision: context.profile.suppliedRevision,
      platform: options.platform
    }
  });
}


export async function applyRestore(input, control) {
  return operationRuntime.applyPrepared(input, "restore", (options) => executeRestore(options), control);
}

export function createRestoreUseCase() {
  return Object.freeze({ prepareRestore, applyRestore });
}
