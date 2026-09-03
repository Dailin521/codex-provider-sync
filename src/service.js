import path from "node:path";
import fs from "node:fs/promises";

import {
  DEFAULT_BACKUP_RETENTION_COUNT,
  DEFAULT_LOCK_NAME,
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
  listBackups,
  pruneBackups as pruneManagedBackups,
  refreshBackupInventory,
  resolveRestoreStateDbTargetPath,
  restoreBackup,
  restoreGlobalStateFilesFromBackup
} from "./backup.js";
import { acquireLock, inspectPathLock } from "./locking.js";
import { acquireStateDbLock, resolveStateDbLockResource } from "./state-db-lock.js";
import { PlanLedger } from "./plan-ledger.js";
import { sharedOperationCoordinator as operationCoordinator } from "./operation-coordinator.js";
import {
  captureBackupRevision,
  captureOperationRevisions,
  captureStorageRevision,
  revisionMismatch,
  sha256Revision,
  stableStringify
} from "./operation-revision.js";
import {
  applySessionChanges,
  collectSessionChanges,
  collectStatusRolloutMetadata,
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
  readTransactionJournal
} from "./transaction-journal.js";
import {
  acknowledgePendingRestore,
  captureStableRestoreSource,
  executeRestoreV2,
  restoreJournalCoverageIsComplete,
  restoreJournalMatchesPhysicalHome,
  restoreJournalMatchesSource
} from "./restore-v2.js";

const planLedger = new PlanLedger();

function issuePreparedPlan(operation, summary, internal) {
  const plan = planLedger.issue(operation, summary, internal);
  if (internal.actor === "manual") {
    operationCoordinator.registerManualIntent(
      internal.codexHome,
      plan.planId,
      plan.expiresAt,
      internal.platform
    );
  }
  return plan;
}

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

async function physicalDirectoryComparisonKey(value) {
  try {
    const lexical = path.resolve(value);
    const first = path.resolve(await fs.realpath(lexical));
    const stat = await fs.stat(first);
    const second = path.resolve(await fs.realpath(lexical));
    if (!stat.isDirectory() || pathComparisonKey(first) !== pathComparisonKey(second)) return null;
    return pathComparisonKey(first);
  } catch {
    return null;
  }
}

async function releaseWriteLocks(releaseStateDbLock, releaseHomeLock) {
  const failures = [];
  if (releaseStateDbLock) {
    try {
      await releaseStateDbLock();
    } catch (error) {
      failures.push(error);
    }
  }
  try {
    await releaseHomeLock();
  } catch (error) {
    failures.push(error);
  }
  if (failures.length === 1) throw failures[0];
  if (failures.length > 1) {
    throw new AggregateError(failures, "Failed to release one or more write-operation locks.");
  }
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

function normalizeProfileId(value) {
  const profileId = value ?? "default";
  if (typeof profileId !== "string" || !/^[A-Za-z0-9._-]{1,80}$/.test(profileId)) {
    throw new CoreError("INVALID_INPUT", "The storage profile id is invalid.");
  }
  return profileId;
}

function comparableProfilePath(value, platform) {
  if (typeof value !== "string" || !value) return null;
  const resolved = path.resolve(value);
  return platform === "win32" ? resolved.toLowerCase() : resolved;
}

function createProfileSnapshot({
  profileId,
  suppliedRevision,
  codexHome,
  sqliteHome,
  platform = process.platform
}) {
  if (suppliedRevision !== undefined && suppliedRevision !== null
      && (typeof suppliedRevision !== "string" || !suppliedRevision || suppliedRevision.length > 512)) {
    throw new CoreError("INVALID_INPUT", "The storage profile revision is invalid.");
  }
  const id = normalizeProfileId(profileId);
  const revision = sha256Revision(stableStringify({
    schemaVersion: 1,
    id,
    suppliedRevision: suppliedRevision ?? null,
    codexHome: comparableProfilePath(codexHome, platform),
    sqliteHome: comparableProfilePath(sqliteHome, platform)
  }));
  return Object.freeze({
    id,
    revision,
    suppliedRevision: suppliedRevision ?? null,
    codexHome: path.resolve(codexHome),
    sqliteHome: typeof sqliteHome === "string" && sqliteHome ? path.resolve(sqliteHome) : null
  });
}

function profileFromOptions(options, codexHome, sqliteHome, platform) {
  return createProfileSnapshot({
    profileId: options.profile?.id ?? options.profileId,
    suppliedRevision: options.profile?.revision ?? options.profileRevision,
    codexHome,
    sqliteHome,
    platform
  });
}

function explicitSqliteHomeFromOptions(options) {
  if (typeof options.sqliteHome === "string" && options.sqliteHome.trim()) return options.sqliteHome;
  if (options.storage?.sqliteHomeSource !== "default"
      && typeof options.storage?.sqliteHome === "string"
      && options.storage.sqliteHome.trim()) {
    return options.storage.sqliteHome;
  }
  return undefined;
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

async function scanStatus({
  codexHome: explicitCodexHome,
  sqliteHome,
  storage: providedStorage,
  configText: providedConfigText,
  profile,
  profileId,
  profileRevision,
  rolloutScanMode = "full",
  platform
} = {}) {
  const codexHome = providedStorage?.codexHome ?? normalizeCodexHome(explicitCodexHome);
  const configPath = path.join(codexHome, "config.toml");
  const configText = providedConfigText ?? await readConfigText(configPath);
  const storage = await prepareStorage({ codexHome, sqliteHome, configText, storage: providedStorage, platform });
  const current = readCurrentProviderFromConfigText(configText);
  const configuredProviders = listConfiguredProviderIds(configText);
  const metadataOnly = rolloutScanMode === "metadata";
  const rolloutScan = metadataOnly
    ? await collectStatusRolloutMetadata(codexHome, { skipLockedReads: true })
    : await collectSessionChanges(codexHome, "__status_only__", { skipLockedReads: true });
  const { providerCounts, lockedPaths } = rolloutScan;
  const incompletePaths = metadataOnly ? rolloutScan.incompletePaths : [];
  const encryptedContentCounts = metadataOnly
    ? { sessions: {}, archived_sessions: {} }
    : rolloutScan.encryptedContentCounts;
  const userEventThreadIds = metadataOnly ? new Set() : rolloutScan.userEventThreadIds;
  const threadCwdById = metadataOnly ? new Map() : rolloutScan.threadCwdById;
  const stateDbLocation = storage.stateDbLocation;
  const sqliteCounts = storage.sqliteAccess.supported
    ? await readSqliteProviderCounts(storage)
    : null;
  const sqliteRepairStats = !metadataOnly && sqliteCounts && !sqliteCounts.unreadable
    ? await readSqliteRepairStats(storage, { userEventThreadIds, threadCwdById })
    : null;
  let projectThreadVisibility = [];
  let projectThreadVisibilityAvailable = !metadataOnly
    && storage.sqliteAccess.supported
    && !sqliteCounts?.unreadable;
  if (!metadataOnly && storage.sqliteAccess.supported && !sqliteCounts?.unreadable) {
    try {
      projectThreadVisibility = await readProjectThreadVisibility(storage);
    } catch {
      // Project visibility is an optional diagnostic projection. Older/minimal
      // Codex schemas may not expose every column it needs; that must not block
      // Status, Plan preparation, or a safe provider sync.
      projectThreadVisibility = [];
      projectThreadVisibilityAvailable = false;
    }
  }
  const backupSummary = await getBackupSummary(codexHome);
  const pendingTransactions = await findPendingTransactions(codexHome);
  const trustedProfile = createProfileSnapshot({
    profileId: profile?.id ?? profileId,
    suppliedRevision: profile?.revision ?? profileRevision,
    codexHome,
    // Profile identity is the trusted caller/server selection. Effective
    // config-derived SQLite storage belongs in storageRevision, not here.
    sqliteHome,
    platform
  });
  const configRevision = sha256Revision(Buffer.from(configText, "utf8"));
  const resolvedStorageRevision = captureStorageRevision({
    profileRevision: trustedProfile.revision,
    configRevision,
    storage,
    platform
  });

  return {
    schemaVersion: 1,
    snapshotAt: new Date().toISOString(),
    storageRevision: resolvedStorageRevision,
    profile: { id: trustedProfile.id, revision: trustedProfile.revision },
    profileId: trustedProfile.id,
    profileRevision: trustedProfile.suppliedRevision ?? trustedProfile.revision,
    pathComparisonCaseInsensitive: (platform ?? process.platform) === "win32",
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
    projectThreadVisibilityAvailable,
    backupRoot: defaultBackupRoot(codexHome),
    backupSummary,
    pendingRecovery: pendingTransactions.length > 0,
    operationInProgress: null,
    rolloutScanComplete: lockedPaths.length === 0 && incompletePaths.length === 0,
    pendingTransactions: pendingTransactions.map((transaction) => ({
      operationId: transaction.operationId ?? null,
      operationKind: transaction.operationKind ?? "sync",
      state: transaction.state,
      sourceBackupId: transaction.prepared?.sourceBackup?.backupId ?? path.basename(transaction.backupDir),
      preRestoreSnapshotId: transaction.prepared?.preRestoreSnapshot?.backupId ?? null,
      backupDir: transaction.backupDir,
      journalPath: transaction.filePath
    }))
  };
}

function publicProfileMetadata(profile) {
  return {
    id: profile.id,
    publicRevision: profile.suppliedRevision ?? profile.revision
  };
}

function externalOperationFromLock({ inspection = null, error = null, scope }) {
  const owner = inspection?.owner ?? null;
  return {
    operationId: owner?.instanceId ?? null,
    operation: typeof owner?.label === "string" && owner.label ? owner.label : "unknown",
    actor: "external",
    runtime: typeof owner?.runtime === "string" ? owner.runtime : null,
    startedAt: typeof owner?.startedAt === "string" ? owner.startedAt : null,
    busyScope: scope,
    lockState: inspection?.state === "active" ? "active" : "unverifiable",
    ...(error?.code ? { errorCode: error.code } : {})
  };
}

async function inspectStatusLock(lockPath, options) {
  try {
    return { inspection: await inspectPathLock(lockPath, options), error: null };
  } catch (error) {
    if (error?.code === "LOCK_UNVERIFIABLE"
        || error?.code === "OPERATION_BUSY"
        || error?.code === "PERMISSION_DENIED") {
      return { inspection: null, error };
    }
    throw error;
  }
}

async function blockedStatus(codexHome, profile, operation, platform, details = null) {
  const status = operationCoordinator.statusForBlockedWrite(
    codexHome,
    operation,
    platform,
    publicProfileMetadata(profile)
  );
  if (!status.profile) {
    status.profile = { id: profile.id, revision: profile.revision };
    status.profileId = profile.id;
    status.profileRevision = profile.suppliedRevision ?? profile.revision;
    status.pathComparisonCaseInsensitive = (platform ?? process.platform) === "win32";
  }
  status.statusReadBlocked = details ?? { reason: "write-operation" };
  if (operation.lockState === "unverifiable") {
    status.rolloutScanComplete = false;
    try {
      const pendingTransactions = await findPendingTransactions(codexHome);
      status.pendingTransactions = pendingTransactions.map((transaction) => ({
        operationId: transaction.operationId ?? null,
        operationKind: transaction.operationKind ?? "sync",
        state: transaction.state,
        sourceBackupId: transaction.prepared?.sourceBackup?.backupId ?? path.basename(transaction.backupDir),
        preRestoreSnapshotId: transaction.prepared?.preRestoreSnapshot?.backupId ?? null,
        backupDir: transaction.backupDir,
        journalPath: transaction.filePath
      }));
      status.pendingRecovery = pendingTransactions.length > 0;
    } catch {
      status.pendingTransactions ??= [];
      status.pendingRecovery = true;
    }
  }
  return status;
}

export async function getStatus(options = {}) {
  const codexHome = options.storage?.codexHome ?? normalizeCodexHome(options.codexHome);
  const platform = options.platform ?? process.platform;
  const sqliteHome = explicitSqliteHomeFromOptions(options);
  const rolloutRevisionMode = options.rolloutScanMode === "metadata" ? "metadata" : "content";
  const profile = profileFromOptions(options, codexHome, sqliteHome, platform);
  const activeSnapshot = operationCoordinator.statusDuringWrite(
    codexHome,
    platform,
    publicProfileMetadata(profile)
  );
  if (activeSnapshot) return activeSnapshot;

  const homeLockPath = path.join(codexHome, "tmp", DEFAULT_LOCK_NAME);
  const homeBefore = await inspectStatusLock(homeLockPath, { scope: "codex-home", platform });
  if (homeBefore.error || homeBefore.inspection.state !== "absent") {
    const operation = externalOperationFromLock({
      inspection: homeBefore.inspection,
      error: homeBefore.error,
      scope: "codex-home"
    });
    return blockedStatus(codexHome, profile, operation, platform, {
      reason: "codex-home-lock",
      lockState: operation.lockState
    });
  }

  const configPath = path.join(codexHome, "config.toml");
  const configText = options.configText ?? await readConfigText(configPath);
  const storage = await prepareStorage({
    codexHome,
    sqliteHome,
    configText,
    storage: options.storage,
    platform
  });
  let stateResource = null;
  if (storage.stateDbLocation?.path) {
    stateResource = await resolveStateDbLockResource(storage.stateDbLocation.path, { platform });
    const stateBefore = await inspectStatusLock(stateResource.lockPath, {
      scope: "state-db",
      resourceKey: stateResource.resourceKey,
      platform
    });
    if (stateBefore.error || stateBefore.inspection.state !== "absent") {
      const operation = externalOperationFromLock({
        inspection: stateBefore.inspection,
        error: stateBefore.error,
        scope: "state-db"
      });
      return blockedStatus(codexHome, profile, operation, platform, {
        reason: "state-db-lock",
        lockState: operation.lockState
      });
    }
  }

  let beforeRevision;
  try {
    beforeRevision = await captureOperationRevisions({
      codexHome,
      profileRevision: profile.revision,
      configText,
      storage,
      rolloutRevisionMode,
      platform
    });
  } catch (error) {
    return blockedStatus(
      codexHome,
      profile,
      externalOperationFromLock({ error, scope: "codex-home" }),
      platform,
      { reason: "revision-unverifiable" }
    );
  }

  let snapshot = await scanStatus({
    ...options,
    codexHome,
    sqliteHome,
    storage,
    configText,
    profileId: profile.id,
    profileRevision: profile.suppliedRevision,
    platform
  });

  const homeAfter = await inspectStatusLock(homeLockPath, { scope: "codex-home", platform });
  let stateAfter = { inspection: { state: "absent" }, error: null };
  if (!homeAfter.error && homeAfter.inspection.state === "absent" && stateResource) {
    stateAfter = await inspectStatusLock(stateResource.lockPath, {
      scope: "state-db",
      resourceKey: stateResource.resourceKey,
      platform
    });
  }
  if (homeAfter.error || homeAfter.inspection.state !== "absent"
      || stateAfter.error || stateAfter.inspection.state !== "absent") {
    const source = homeAfter.error || homeAfter.inspection.state !== "absent"
      ? { ...homeAfter, scope: "codex-home" }
      : { ...stateAfter, scope: "state-db" };
    const operation = externalOperationFromLock(source);
    return blockedStatus(codexHome, profile, operation, platform, {
      reason: `${source.scope}-lock`,
      lockState: operation.lockState
    });
  }

  let afterRevision;
  try {
    const latestConfigText = await readConfigText(configPath);
    afterRevision = await captureOperationRevisions({
      codexHome,
      profileRevision: profile.revision,
      configText: latestConfigText,
      storage,
      rolloutRevisionMode,
      platform
    });
  } catch (error) {
    return blockedStatus(
      codexHome,
      profile,
      externalOperationFromLock({ error, scope: "codex-home" }),
      platform,
      { reason: "revision-unverifiable" }
    );
  }
  let driftReason = revisionMismatch(beforeRevision, afterRevision);
  if (driftReason === "state-db" || driftReason === "rollout") {
    // Opening a WAL database for read-only Status can legitimately create or
    // refresh its SHM sidecar. Retry once from that new complete baseline; a
    // real concurrent writer will either expose its lock or drift again.
    snapshot = await scanStatus({
      ...options,
      codexHome,
      sqliteHome,
      storage,
      configText,
      profileId: profile.id,
      profileRevision: profile.suppliedRevision,
      platform
    });
    const retryConfigText = await readConfigText(configPath);
    const retryRevision = await captureOperationRevisions({
      codexHome,
      profileRevision: profile.revision,
      configText: retryConfigText,
      storage,
      rolloutRevisionMode,
      platform
    });
    driftReason = revisionMismatch(afterRevision, retryRevision);
    afterRevision = retryRevision;
  }
  if (driftReason) {
    return blockedStatus(
      codexHome,
      profile,
      externalOperationFromLock({ scope: driftReason === "state-db" ? "state-db" : "codex-home" }),
      platform,
      { reason: "state-changed-during-status", revision: driftReason }
    );
  }

  const homeFinal = await inspectStatusLock(homeLockPath, { scope: "codex-home", platform });
  if (homeFinal.error || homeFinal.inspection.state !== "absent") {
    const operation = externalOperationFromLock({
      inspection: homeFinal.inspection,
      error: homeFinal.error,
      scope: "codex-home"
    });
    return blockedStatus(codexHome, profile, operation, platform, {
      reason: "codex-home-lock",
      lockState: operation.lockState
    });
  }
  if (stateResource) {
    const stateFinal = await inspectStatusLock(stateResource.lockPath, {
      scope: "state-db",
      resourceKey: stateResource.resourceKey,
      platform
    });
    if (stateFinal.error || stateFinal.inspection.state !== "absent") {
      const operation = externalOperationFromLock({
        inspection: stateFinal.inspection,
        error: stateFinal.error,
        scope: "state-db"
      });
      return blockedStatus(codexHome, profile, operation, platform, {
        reason: "state-db-lock",
        lockState: operation.lockState
      });
    }
  }

  operationCoordinator.cacheStatus(codexHome, snapshot, platform);
  return snapshot;
}

async function verifyExpectedPlanState({
  expectedPlanState,
  codexHome,
  configText,
  storage,
  platform,
  backupDir = null
}) {
  if (!expectedPlanState) return null;
  let currentProfile = expectedPlanState.profile;
  if (typeof expectedPlanState.profileResolver === "function") {
    try {
      const resolved = await expectedPlanState.profileResolver(expectedPlanState.profile.id);
      currentProfile = createProfileSnapshot({
        profileId: resolved?.id,
        suppliedRevision: resolved?.revision,
        codexHome: resolved?.codexHome,
        sqliteHome: resolved?.sqliteHome,
        platform
      });
    } catch (error) {
      throw new CoreError("STALE_STATE", "The selected storage profile changed after preparation.", {
        cause: error instanceof Error ? error : undefined,
        details: { reason: "profile" }
      });
    }
  }
  const actual = await captureOperationRevisions({
    codexHome,
    profileRevision: currentProfile.revision,
    configText,
    storage,
    backupDir,
    rolloutRevisionMode: expectedPlanState.rolloutRevisionMode ?? "content",
    platform
  });
  const reason = revisionMismatch(expectedPlanState.revisions, actual);
  if (reason) {
    throw new CoreError("STALE_STATE", "Protected state changed after the operation was prepared.", {
      details: { reason }
    });
  }
  return actual;
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
  fast = false,
  platform,
  faultInjector,
  signal,
  expectedPlanState
} = {}, { afterBackup } = {}) {
  if (typeof fast !== "boolean" || (fast && model !== null)) {
    throw new CoreError(
      "INVALID_INPUT",
      "Fast sync preserves historical models; do not supply a model."
    );
  }
  if (!Number.isInteger(keepCount) || keepCount < 1) {
    throw new CoreError(
      "INVALID_INPUT",
      `Invalid automatic keep count: ${keepCount}. Expected an integer greater than or equal to 1.`
    );
  }

  const codexHome = providedStorage?.codexHome ?? normalizeCodexHome(explicitCodexHome);
  const configPath = path.join(codexHome, "config.toml");
  const releaseLock = await acquireLock(codexHome, "sync");
  let releaseStateDbLock = null;
  let backupDir = null;
  let journal = null;
  let backupDurationMs = 0;
  try {
    throwIfAborted(signal);
    const configText = await readConfigText(configPath);
    if (!expectedPlanState && expectedConfigText !== undefined && configText !== expectedConfigText) {
      throw new CoreError(
        "PLAN_STALE",
        "config.toml changed after the operation was confirmed. Refresh and retry."
      );
    }
    if (!expectedPlanState && configBackupText !== undefined && configText !== configBackupText) {
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
    if (storage.stateDbLocation?.path) {
      ({ release: releaseStateDbLock } = await acquireStateDbLock(
        storage.stateDbLocation.path,
        "sync",
        { platform }
      ));
    }
    await assertNoPendingTransactions(codexHome);
    await verifyExpectedPlanState({
      expectedPlanState,
      codexHome,
      configText,
      storage,
      platform
    });
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
    } = await collectSessionChanges(codexHome, targetProvider, { skipLockedReads: true, targetModel: model, fast });
    const cwdStats = await readThreadCwdStats(storage);
    const encryptedContentWarning = fast
      ? "Fast mode: history models, user-event flags and encrypted content were not checked. Metadata alignment does not guarantee continuation with another provider."
      : buildEncryptedContentWarning(encryptedContentCounts, targetProvider);
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
      configBackupText,
      fast
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
        autoPruneResult = await pruneManagedBackups(codexHome, keepCount);
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
        inPlaceSessionFiles: applyResult.inPlaceChanges ?? 0,
        rewrittenSessionFiles: Math.max(0, applyResult.appliedChanges - (applyResult.inPlaceChanges ?? 0)),
        ...(fast ? { scanScope: "metadata", unchecked: ["historyModels", "userEventFlags", "encryptedContent"] } : {}),
        providerSync: {
          mode: fast ? "fast" : "full",
          rolloutScanScope: fast ? "metadata" : "full",
          inPlaceSessionFiles: applyResult.inPlaceChanges ?? 0,
          rewrittenSessionFiles: Math.max(0, applyResult.appliedChanges - (applyResult.inPlaceChanges ?? 0)),
          unchecked: fast ? ["historyModels", "userEventFlags", "encryptedContent"] : []
        },
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
        ? (journalSnapshot.invalidTail || journalSnapshot.events.length === 0
          ? writableChanges.map((change) => change.path)
          : getStartedJournalTargets(journalSnapshot, "rollout"))
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
    await releaseWriteLocks(releaseStateDbLock, releaseLock);
  }
}

function buildSwitchIntent(originalConfigText, provider, model, keepRootModel) {
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
  const modelForThreads = modelSync.applied && modelSync.model
    ? modelSync.model
    : readRootModelFromConfigText(nextConfigText);
  return { nextConfigText, modelSync, modelForThreads };
}

async function runSwitchCore({
  codexHome: explicitCodexHome,
  sqliteHome,
  storage: providedStorage,
  expectedConfigText,
  provider,
  model,
  keepRootModel = false,
  fast = false,
  keepCount = DEFAULT_BACKUP_RETENTION_COUNT,
  onProgress,
  platform,
  faultInjector,
  signal,
  expectedPlanState
}) {
  if (typeof fast !== "boolean" || (fast && model !== undefined && model !== null)) {
    throw new CoreError(
      "INVALID_INPUT",
      "Fast switch preserves root and historical models; fast mode and an explicit model cannot be combined."
    );
  }
  if (fast) keepRootModel = true;
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
  await faultInjector?.({
    point: "after_switch_storage_preflight",
    stateDbPath: storage.stateDbLocation?.path ?? null
  });
  const { nextConfigText, modelSync, modelForThreads } = buildSwitchIntent(
    originalConfigText,
    provider,
    model,
    keepRootModel
  );
  const syncResult = await runSyncCore(
    {
      codexHome,
      sqliteHome,
      provider,
      configBackupText: originalConfigText,
      keepCount,
      onProgress,
      model: fast ? null : modelForThreads,
      fast,
      faultInjector,
      signal,
      expectedPlanState,
      platform
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

async function runRestoreCore({
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
  let releaseStateDbLock = null;
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
    if (restoreDatabase && !storage.stateDbLocation && isConfiguredSqliteHome(storage)) {
      throw missingConfiguredStateDbError(storage);
    }
    const sourceBackup = await captureStableRestoreSource(backupDir, { platform });
    const normalizedBackupDir = sourceBackup.backupDir;
    if (restoreDatabase) {
      const stateDbTargetPath = await resolveRestoreStateDbTargetPath(normalizedBackupDir, storage);
      ({ release: releaseStateDbLock, resource: stateDbResource } = await acquireStateDbLock(
        stateDbTargetPath,
        "restore",
        { platform }
      ));
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
      ...(restoreConfig ? ["config", "globalState"] : []),
      ...(restoreDatabase ? ["sqlite"] : []),
      ...(restoreSessions ? ["rollout"] : [])
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
      const acknowledgement = await acknowledgePendingRestore(committedPendingAck[0], {
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
    const result = await executeRestoreV2({
      storage,
      sourceBackup,
      restoreConfig,
      restoreDatabase,
      restoreSessions,
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
    return result;
  } finally {
    await releaseWriteLocks(releaseStateDbLock, releaseLock);
  }
}

function sqliteRowsToChange(sqliteCounts, targetProvider, sqliteRepairStats) {
  let count = 0;
  for (const scope of ["sessions", "archived_sessions"]) {
    for (const [provider, providerCount] of Object.entries(sqliteCounts?.[scope] ?? {})) {
      if (provider !== targetProvider && Number.isSafeInteger(providerCount)) count += providerCount;
    }
  }
  count += sqliteRepairStats?.userEventRowsNeedingRepair ?? 0;
  count += sqliteRepairStats?.cwdRowsNeedingRepair ?? 0;
  return count;
}

function normalizeSyncMode(options = {}) {
  const mode = options.syncMode ?? (options.fast === true ? "fast" : "full");
  if (!new Set(["full", "fast"]).has(mode)) {
    throw new CoreError("INVALID_INPUT", "syncMode must be full or fast.");
  }
  if (options.fast !== undefined && typeof options.fast !== "boolean") {
    throw new CoreError("INVALID_INPUT", "fast must be boolean when provided.");
  }
  if (options.fast !== undefined && options.syncMode !== undefined
      && options.fast !== (mode === "fast")) {
    throw new CoreError("INVALID_INPUT", "fast and syncMode describe conflicting modes.");
  }
  return mode;
}

async function preparePlanContext(options, operation, { backupDir = null } = {}) {
  const codexHome = options.storage?.codexHome ?? normalizeCodexHome(options.codexHome);
  if (operationCoordinator.isActive(codexHome, options.platform)) {
    throw new CoreError("OPERATION_BUSY", "Lock already exists for this Codex Home; another write operation is active.", {
      details: { busyScope: "codex-home" }
    });
  }
  const sqliteHome = explicitSqliteHomeFromOptions(options);
  const configPath = path.join(codexHome, "config.toml");
  const configText = await readConfigText(configPath);
  if (options.expectedConfigText !== undefined && configText !== options.expectedConfigText) {
    throw new CoreError(
      "PLAN_STALE",
      "config.toml changed after the operation was confirmed. Refresh and retry."
    );
  }
  const storage = await prepareStorage({ codexHome, sqliteHome, configText, platform: options.platform });
  assertSqliteAccessSupported(storage, operation);
  const profile = profileFromOptions(options, codexHome, sqliteHome, options.platform);
  const status = await scanStatus({
    codexHome,
    sqliteHome,
    storage,
    configText,
    profileId: profile.id,
    profileRevision: profile.suppliedRevision,
    rolloutScanMode: options.rolloutScanMode ?? "full",
    platform: options.platform
  });
  // Capture the executable revision after all read-only status queries have
  // closed their SQLite handles; opening a WAL database may legitimately
  // update its SHM sidecar.
  const revisions = await captureOperationRevisions({
    codexHome,
    profileRevision: profile.revision,
    configText,
    storage,
    backupDir,
    rolloutRevisionMode: options.rolloutScanMode === "metadata" ? "metadata" : "content",
    platform: options.platform
  });
  operationCoordinator.cacheStatus(codexHome, status, options.platform);
  return { codexHome, sqliteHome, configText, storage, profile, revisions, status };
}

async function issueSyncLikePlan(operation, options, switchIntent = null) {
  const syncMode = normalizeSyncMode(options);
  const fast = syncMode === "fast";
  const keepCount = options.keepCount ?? DEFAULT_BACKUP_RETENTION_COUNT;
  if (!Number.isInteger(keepCount) || keepCount < 1) {
    throw new CoreError(
      "INVALID_INPUT",
      `Invalid automatic keep count: ${keepCount}. Expected an integer greater than or equal to 1.`
    );
  }
  const context = await preparePlanContext({
    ...options,
    rolloutScanMode: fast ? "metadata" : "full"
  }, operation);
  if (!context.storage.stateDbLocation && isConfiguredSqliteHome(context.storage)) {
    throw missingConfiguredStateDbError(context.storage);
  }
  const current = readCurrentProviderFromConfigText(context.configText);
  const targetProvider = switchIntent?.provider
    ?? options.provider
    ?? current.provider
    ?? DEFAULT_PROVIDER;
  const targetModel = switchIntent?.modelForThreads ?? options.model ?? null;
  if (targetModel !== null && targetModel !== undefined
      && (typeof targetModel !== "string" || !targetModel)) {
    throw new CoreError("INVALID_INPUT", "The target model must be a non-empty string or null.");
  }
  if (fast && targetModel !== null) {
    throw new CoreError(
      "INVALID_INPUT",
      "Fast mode preserves historical models and cannot include a model rewrite."
    );
  }
  const scan = await collectSessionChanges(context.codexHome, targetProvider, {
    skipLockedReads: true,
    targetModel,
    fast
  });
  const { lockedChanges } = await splitLockedSessionChanges(scan.changes);
  const lockedCount = new Set([
    ...scan.lockedPaths,
    ...lockedChanges.map((change) => change.path)
  ]).size;
  const warnings = [];
  if (!context.status.projectThreadVisibilityAvailable) {
    warnings.push("Project visibility diagnostics are unavailable; the write operation will still validate and protect the global state with backup-first recovery.");
  }
  const encryptedWarning = fast
    ? "Fast mode checks rollout metadata only; historical models, user-event flags and encrypted content are not inspected."
    : buildEncryptedContentWarning(scan.encryptedContentCounts, targetProvider);
  if (encryptedWarning) warnings.push(encryptedWarning);
  if (lockedCount > 0) {
    warnings.push(`${lockedCount} rollout file(s) are currently locked and may produce a partial result.`);
  }
  if (switchIntent?.modelSync.warning) warnings.push(switchIntent.modelSync.warning);

  const inPlaceEligibleSessionFiles = scan.changes
    .filter((change) => Boolean(change.inPlaceMutation)).length;
  const rewriteRequiredSessionFiles = scan.changes.length - inPlaceEligibleSessionFiles;

  const summary = {
    profile: { id: context.profile.id, revision: context.profile.revision },
    storageRevision: context.revisions.storageRevision,
    configRevision: context.revisions.configRevision,
    rolloutRevision: context.revisions.rolloutRevision,
    stateDbRevision: context.revisions.stateDbRevision,
    target: {
      provider: targetProvider,
      model: targetModel,
      ...(switchIntent ? { modelMode: switchIntent.modelMode } : {})
    },
    providerSync: {
      mode: syncMode,
      rolloutScanScope: fast ? "metadata" : "full",
      providerWritePolicy: fast ? "require-in-place" : "prefer-in-place",
      historicalModelSync: fast ? "preserved" : "enabled",
      unchecked: fast ? ["historyModels", "userEventFlags", "encryptedContent"] : [],
      inPlaceEligibleSessionFiles,
      rewriteRequiredSessionFiles
    },
    impact: {
      rolloutFilesToChange: scan.changes.length,
      sqliteRowsToChange: sqliteRowsToChange(
        context.status.sqliteCounts,
        targetProvider,
        context.status.sqliteRepairStats
      ),
      workspaceRootsToChange: context.status.sqliteRepairStats?.cwdRowsNeedingRepair ?? 0,
      lockedRolloutFiles: context.revisions.lockedRolloutFiles,
      backupExpected: true
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
        fast,
        onProgress: options.onProgress,
        platform: options.platform,
        faultInjector: options.faultInjector,
        signal: options.signal
      }
    : {
        codexHome: context.codexHome,
        ...(context.sqliteHome ? { sqliteHome: context.sqliteHome } : {}),
        provider: targetProvider,
        keepCount,
        fast,
        sqliteBusyTimeoutMs: options.sqliteBusyTimeoutMs,
        onProgress: options.onProgress,
        model: targetModel,
        platform: options.platform,
        faultInjector: options.faultInjector,
        signal: options.signal
      };
  return issuePreparedPlan(operation, summary, {
    codexHome: context.codexHome,
    platform: options.platform,
    actor: options.__actor === "watch" ? "watch" : "manual",
    executionOptions,
    expectedPlanState: {
      profile: context.profile,
      profileResolver: options.profileResolver,
      revisions: context.revisions,
      rolloutRevisionMode: fast ? "metadata" : "content"
    },
    statusOptions: {
      codexHome: context.codexHome,
      ...(context.sqliteHome ? { sqliteHome: context.sqliteHome } : {}),
      profileId: context.profile.id,
      profileRevision: context.profile.suppliedRevision,
      ...(fast ? { rolloutScanMode: "metadata" } : {}),
      platform: options.platform
    }
  });
}

export async function prepareSync(options = {}) {
  return issueSyncLikePlan("sync", options);
}

export async function prepareSwitch(options = {}) {
  if (!options.provider) {
    throw new CoreError("INVALID_INPUT", "Missing provider id. Usage: codex-provider switch <provider-id>");
  }
  const codexHome = options.storage?.codexHome ?? normalizeCodexHome(options.codexHome);
  if (operationCoordinator.isActive(codexHome, options.platform)) {
    throw new CoreError("OPERATION_BUSY", "Lock already exists for this Codex Home; another write operation is active.", {
      details: { busyScope: "codex-home" }
    });
  }
  const configText = await readConfigText(path.join(codexHome, "config.toml"));
  if (options.expectedConfigText !== undefined && configText !== options.expectedConfigText) {
    throw new CoreError("PLAN_STALE", "config.toml changed after the operation was confirmed. Refresh and retry.");
  }
  const syncMode = normalizeSyncMode(options);
  if (syncMode === "fast" && options.model !== undefined && options.model !== null) {
    throw new CoreError(
      "INVALID_INPUT",
      "Fast switch preserves root and historical models; an explicit model cannot be combined with fast mode."
    );
  }
  const keepRootModel = syncMode === "fast" ? true : Boolean(options.keepRootModel);
  const intent = buildSwitchIntent(configText, options.provider, options.model, keepRootModel);
  return issueSyncLikePlan("switch", { ...options, syncMode, keepRootModel }, {
    provider: options.provider,
    modelForThreads: syncMode === "fast" ? null : intent.modelForThreads,
    modelSync: intent.modelSync,
    modelMode: options.model !== undefined && options.model !== null
      ? "explicit"
      : (keepRootModel ? "keep-root-model" : "provider-default")
  });
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
  const revisions = {
    ...context.revisions,
    backupRevision: await captureBackupRevision(backup.backupDir)
  };
  if (restoreDatabase && !context.storage.stateDbLocation && isConfiguredSqliteHome(context.storage)) {
    throw missingConfiguredStateDbError(context.storage);
  }
  if (restoreDatabase) {
    await resolveRestoreStateDbTargetPath(backup.backupDir, context.storage);
  }
  const warnings = [];
  if (options.allowSqliteHomeRelocation) {
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
      rolloutFilesToChange: restoreSessions ? (backup.metadata?.changedSessionFiles ?? 0) : 0,
      stateDbFilesToChange: restoreDatabase ? 1 : 0,
      configFilesToChange: restoreConfig ? 1 : 0,
      lockedRolloutFiles: context.revisions.lockedRolloutFiles,
      backupExpected: true
    },
    warnings
  };
  return issuePreparedPlan("restore", summary, {
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
      revisions
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

function operationWarnings(result) {
  return [
    result?.encryptedContentWarning,
    result?.autoPruneWarning,
    result?.backupInventoryWarning,
    result?.modelSync?.warning
  ].filter((warning) => typeof warning === "string" && warning.trim());
}

function operationResult(operation, operationId, result, sourceBackup = null) {
  const partial = Array.isArray(result?.skippedLockedRolloutFiles)
    && result.skippedLockedRolloutFiles.length > 0;
  const backupDir = result?.backupDir ?? sourceBackup?.backupDir ?? null;
  return {
    schemaVersion: 1,
    operationId,
    operation,
    outcome: partial ? "partial" : "completed",
    backup: backupDir
      ? {
          backupId: operation === "restore" ? sourceBackup?.backupId : path.basename(backupDir),
          path: backupDir
        }
      : null,
    ...(result?.providerSync ? { providerSync: result.providerSync } : {}),
    warnings: operationWarnings(result),
    result
  };
}

function composeProgressObservers(primary, secondary) {
  if (typeof primary !== "function") return secondary;
  if (typeof secondary !== "function" || primary === secondary) return primary;
  return (event) => {
    emitProgress(primary, event);
    emitProgress(secondary, event);
  };
}

function notifyOperationStarted(observer, value) {
  if (typeof observer !== "function") return;
  try {
    const result = observer(value);
    if (result && typeof result.then === "function") result.catch(() => {});
  } catch {
    // Runtime lifecycle observers are non-authoritative, just like progress.
  }
}

function attachOperationId(error, operationId) {
  if (!error || (typeof error !== "object" && typeof error !== "function")) return;
  if (typeof error.operationId === "string" && error.operationId) return;
  try {
    Object.defineProperty(error, "operationId", {
      configurable: true,
      enumerable: true,
      value: operationId,
      writable: false
    });
  } catch {
    // Error correlation is observational. Preserve the original failure when
    // a frozen third-party error cannot be annotated.
  }
}

async function applyPrepared(input, operation, execute, control = {}) {
  const entry = planLedger.consume(input, operation);
  const internal = entry.internal;
  const active = operationCoordinator.begin(internal.codexHome, operation, {
    actor: internal.actor,
    planId: input.planId,
    platform: internal.platform
  });
  notifyOperationStarted(control.onOperationStarted, {
    operationId: active.operationId,
    operation
  });
  try {
    const result = await execute({
      ...internal.executionOptions,
      ...(control.signal ? { signal: control.signal } : {}),
      ...(typeof control.faultInjector === "function"
        ? { faultInjector: control.faultInjector }
        : {}),
      onProgress: composeProgressObservers(
        internal.executionOptions.onProgress,
        control.onProgress
      ),
      expectedPlanState: internal.expectedPlanState
    });
    return operationResult(operation, active.operationId, result, internal.sourceBackup);
  } catch (error) {
    attachOperationId(error, active.operationId);
    if (error?.name === "AbortError" && error?.code === "ABORT_ERR") {
      throw new CoreError(
        "OPERATION_CANCELLED",
        "The provider-sync operation was cancelled before commit.",
        { operationId: active.operationId, cause: error }
      );
    }
    throw error;
  } finally {
    operationCoordinator.end(internal.codexHome, active.operationId, internal.platform);
    try {
      await getStatus(internal.statusOptions);
    } catch {
      // Keep the last complete snapshot. A status refresh is observational and
      // cannot change the transaction result or replace it with partial state.
    }
  }
}

export async function applySync(input, control) {
  return applyPrepared(input, "sync", (options) => runSyncCore(options), control);
}

export async function applySwitch(input, control) {
  return applyPrepared(input, "switch", (options) => runSwitchCore(options), control);
}

export async function applyRestore(input, control) {
  return applyPrepared(input, "restore", (options) => runRestoreCore(options), control);
}

// Internal scheduler hook used by Watch. It exposes completion only for a
// same-process manual operation; external writers remain event-driven and are
// never polled or queued behind.
export function waitForManualOperationEnd({ codexHome, platform } = {}) {
  return operationCoordinator.waitForManualOperation(
    normalizeCodexHome(codexHome),
    platform ?? process.platform
  );
}

/** @deprecated Compatibility adapter. New transports must use prepareSync/applySync. */
export async function runSync(options = {}) {
  const plan = await prepareSync(options);
  return (await applySync({ schemaVersion: 1, planId: plan.planId })).result;
}

/** @deprecated Compatibility adapter. New transports must use prepareSwitch/applySwitch. */
export async function runSwitch(options = {}) {
  const plan = await prepareSwitch(options);
  return (await applySwitch({ schemaVersion: 1, planId: plan.planId })).result;
}

/** @deprecated Compatibility adapter. New transports must use prepareRestore/applyRestore. */
export async function runRestore(options = {}) {
  const plan = await prepareRestore(options);
  return (await applyRestore({ schemaVersion: 1, planId: plan.planId })).result;
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
    return await pruneManagedBackups(codexHome, keepCount);
  } finally {
    await releaseLock();
  }
}

export async function pruneBackups(options = {}) {
  return runPruneBackups(options);
}
