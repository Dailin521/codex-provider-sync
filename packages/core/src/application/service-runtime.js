// @ts-nocheck

import { createConcurrencyGuard } from "./concurrency-guard.js";
import { createOperationRuntime } from "./operation-runtime.js";
import { createPlanApplyGuard } from "./plan-apply-guard.js";
import { createRestoreRecovery } from "../infrastructure/restore-recovery.js";
import { createSqliteTransaction } from "../infrastructure/sqlite-transaction.js";
import { createUndoBackup } from "../infrastructure/undo-backup.js";
import {
  DEFAULT_BACKUP_RETENTION_COUNT,
  DEFAULT_LOCK_NAME,
  DEFAULT_PROVIDER,
  defaultBackupRoot,
  CoreError,
  createBackup,
  getBackupRecoveryCoverage,
  getBackupSummary,
  listBackups,
  pruneManagedBackups,
  refreshBackupInventory,
  resolveRestoreStateDbTargetPath,
  restoreBackup,
  acquireLock,
  inspectPathLock,
  resolveStateDbLockResource,
  PlanLedger,
  operationCoordinator,
  captureBackupRevision,
  captureOperationRevisions,
  captureStorageRevision,
  revisionMismatch,
  sha256Revision,
  stableStringify,
  assertSqliteAccessSupported,
  ensureCodexHome,
  isConfiguredSqliteHome,
  missingConfiguredStateDbError,
  normalizeCodexHome,
  resolveStorageLayout,
  withStateDbLocation,
  findPendingTransactions,
  getStartedJournalTargets,
  readTransactionJournal,
  acknowledgePendingRestore,
  captureStableRestoreSource,
  executeRestoreV2,
  restoreJournalCoverageIsComplete,
  restoreJournalMatchesPhysicalHome,
  restoreJournalMatchesSource,
  codexStorage,
  path,
  fs
} from "../infrastructure/node-core-ports.js";

const {
  configDeclaresProvider,
  listConfiguredProviderIds,
  readConfigText,
  readCurrentProviderFromConfigText,
  readProviderModel,
  readRootModelFromConfigText,
  setRootModelInConfigText,
  setRootProviderInConfigText,
  writeConfigText
} = codexStorage.config;
const {
  applySessionChanges,
  collectDiagnosticsFacts,
  collectProviderChanges,
  collectRepairChanges,
  collectSessionChanges,
  collectStatusRolloutMetadata,
  splitLockedSessionChanges,
  summarizeProviderCounts
} = codexStorage.sessions;
const {
  applySqliteRepairs,
  assertSqliteWritable,
  detectStateDb,
  readSqliteProviderCounts,
  readSqliteRepairStats,
  updateSqliteProvider
} = codexStorage.stateDb;
const {
  cwdStatsFromThreadCwdMap,
  readProjectThreadVisibility,
  readThreadCwdStats,
  readWorkspaceRootRepairStats,
  syncWorkspaceRoots
} = codexStorage.globalState;

const planLedger = new PlanLedger();
const concurrencyGuard = createConcurrencyGuard({ coordinator: operationCoordinator });
const planApplyGuard = createPlanApplyGuard({ planLedger, concurrencyGuard });
const operationRuntime = createOperationRuntime({
  planApplyGuard,
  concurrencyGuard,
  CoreError,
  getStatus,
  normalizeCodexHome,
  toOperationResult: operationResult,
  emitProgress
});
const sqliteTransaction = createSqliteTransaction({
  updateProvider: updateSqliteProvider,
  repair: applySqliteRepairs
});
const undoBackup = createUndoBackup({
  capture: createBackup,
  refreshInventory: refreshBackupInventory
});
const restoreRecovery = createRestoreRecovery({
  resolveResource: resolveStateDbLockResource,
  acknowledge: acknowledgePendingRestore,
  execute: executeRestoreV2
});

function pathComparisonKey(value) {
  const resolved = path.resolve(value);
  return process.platform === "win32" ? resolved.toLowerCase() : resolved;
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

// Rewrites the retained backup's recorded size and file count after the journal
// reached a terminal state, so status and pruning do not trust an inventory
// captured before those journal records existed. Used on the rollback paths,
// where the caller is already reporting a failure: a bookkeeping problem here
// must never replace the original error.
async function tryRefreshBackupInventory(backupDir) {
  try {
    await undoBackup.refreshInventory(backupDir);
  } catch {
    // The original sync failure and its rollback details are the authoritative
    // diagnosis and must reach the caller unchanged.
  }
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
  rolloutScanMode = "metadata",
  platform
} = {}) {
  const codexHome = providedStorage?.codexHome ?? normalizeCodexHome(explicitCodexHome);
  const configPath = path.join(codexHome, "config.toml");
  const configText = providedConfigText ?? await readConfigText(configPath);
  const storage = await prepareStorage({ codexHome, sqliteHome, configText, storage: providedStorage, platform });
  const current = readCurrentProviderFromConfigText(configText);
  const currentModel = readRootModelFromConfigText(configText);
  const configuredProviders = listConfiguredProviderIds(configText);
  const metadataOnly = rolloutScanMode === "metadata";
  const rolloutScan = metadataOnly
    ? await collectStatusRolloutMetadata(codexHome, { skipLockedReads: true })
    : await collectDiagnosticsFacts(codexHome, {
        skipLockedReads: true,
        targetModel: currentModel
      });
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
    ? await readSqliteRepairStats(storage, {
        targetModel: currentModel,
        userEventThreadIds,
        threadCwdById
      })
    : null;
  const desiredCwdStats = !metadataOnly
    ? cwdStatsFromThreadCwdMap(threadCwdById)
    : [];
  const workspaceRootRepairStats = !metadataOnly
    ? await readWorkspaceRootRepairStats(storage, { cwdStats: desiredCwdStats })
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
    currentModel,
    configuredProviders,
    rolloutCounts: summarizeProviderCounts(providerCounts),
    lockedRolloutFiles: lockedPaths,
    encryptedContentCounts,
    encryptedContentWarning: buildEncryptedContentWarning(encryptedContentCounts, current.provider ?? DEFAULT_PROVIDER),
    sqliteCounts,
    stateDbLocation,
    sqliteRepairStats,
    workspaceRootRepairStats,
    diagnosticIssues: metadataOnly
      ? null
      : {
          rootModelAvailable: typeof currentModel === "string" && currentModel.length > 0,
          rolloutModelFilesNeedingRepair: rolloutScan.changes?.filter((change) => change.modelRewriteRequired).length ?? 0,
          sqliteModelRowsNeedingRepair: sqliteRepairStats?.modelRowsNeedingRepair ?? 0,
          cwdRowsNeedingRepair: sqliteRepairStats?.cwdRowsNeedingRepair ?? 0,
          userEventRowsNeedingRepair: sqliteRepairStats?.userEventRowsNeedingRepair ?? 0,
          workspaceRootsNeedingRepair: workspaceRootRepairStats?.workspaceRootsNeedingRepair ?? 0,
          encryptedContentFiles: sumCounts(encryptedContentCounts?.sessions)
            + sumCounts(encryptedContentCounts?.archived_sessions)
        },
    projectThreadVisibility,
    projectThreadVisibilityAvailable,
    backupRoot: defaultBackupRoot(codexHome),
    backupSummary,
    pendingRecovery: pendingTransactions.some((transaction) => transaction.operationKind === "restore"),
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
      status.pendingRecovery = pendingTransactions.some(
        (transaction) => transaction.operationKind === "restore"
      );
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
  const rolloutRevisionMode = options.rolloutScanMode === "full" ? "content" : "metadata";
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
  if (homeAfter.error || homeAfter.inspection.state !== "absent") {
    const source = { ...homeAfter, scope: "codex-home" };
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

async function assertNoPendingRestoreTransactions(codexHome) {
  const pending = await findPendingTransactions(codexHome);
  const blocking = pending.filter((entry) => entry.operationKind === "restore");
  if (blocking.length === 0) return;
  throw new CoreError(
    "PENDING_TRANSACTION",
    "An unfinished Restore must be resolved before another write operation can start.",
    {
      recoveryRequired: true,
      details: { pendingCount: blocking.length }
    }
  );
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

async function runSyncCore({
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
  expectedPlanState
} = {}, {
  afterBackup,
  targetProvider: targetProviderOverride,
  configBackupText,
  configMutationExpected = false,
  operationKind = "sync",
  repair = null
} = {}) {
  if (!Number.isInteger(keepCount) || keepCount < 1) {
    throw new CoreError(
      "INVALID_INPUT",
      `Invalid automatic keep count: ${keepCount}. Expected an integer greater than or equal to 1.`
    );
  }

  const codexHome = providedStorage?.codexHome ?? normalizeCodexHome(explicitCodexHome);
  const configPath = path.join(codexHome, "config.toml");
  const releaseLock = await acquireLock(codexHome, operationKind);
  let backupDir = null;
  let backupDurationMs = 0;
  try {
    throwIfAborted(signal);
    const configText = await readConfigText(configPath);
    if (!expectedPlanState && expectedConfigText !== undefined && configText !== expectedConfigText) {
      throw new CoreError("PLAN_STALE", "config.toml changed after confirmation. Refresh and retry.");
    }
    if (!expectedPlanState && configBackupText !== undefined && configText !== configBackupText) {
      throw new CoreError("PLAN_STALE", "config.toml changed before the switch acquired its lock. Refresh and retry.");
    }

    const storage = await prepareStorage({ codexHome, sqliteHome, configText, storage: providedStorage, platform });
    assertSqliteAccessSupported(storage, operationKind);
    if (!storage.stateDbLocation && isConfiguredSqliteHome(storage)) {
      throw missingConfiguredStateDbError(storage);
    }
    await assertNoPendingRestoreTransactions(codexHome);
    await verifyExpectedPlanState({ expectedPlanState, codexHome, configText, storage, platform });

    const current = readCurrentProviderFromConfigText(configText);
    const targetProvider = targetProviderOverride ?? current.provider ?? DEFAULT_PROVIDER;
    const repairTargets = new Set(repair?.targets ?? []);
    const targetModel = repairTargets.has("models") ? readRootModelFromConfigText(configText) : null;
    if (repairTargets.has("models") && !targetModel) {
      throw new CoreError("INVALID_INPUT", "Model repair requires a root model in config.toml.");
    }

    emitProgress(onProgress, { stage: "scan_rollout_files", status: "start" });
    const scan = repair
      ? await collectRepairChanges(codexHome, [...repairTargets], { skipLockedReads: true, targetModel })
      : await collectProviderChanges(codexHome, targetProvider, { skipLockedReads: true });
    const { changes, lockedPaths: lockedReadPaths, providerCounts } = scan;
    emitProgress(onProgress, { stage: "check_locked_rollout_files", status: "start" });
    const { writableChanges, lockedChanges } = await splitLockedSessionChanges(changes);
    const initiallySkipped = [...new Set([
      ...lockedReadPaths,
      ...lockedChanges.map((change) => change.path)
    ])].sort((left, right) => left.localeCompare(right));
    emitProgress(onProgress, {
      stage: "scan_rollout_files",
      status: "complete",
      scannedChanges: changes.length,
      lockedReadCount: lockedReadPaths.length
    });
    emitProgress(onProgress, {
      stage: "check_locked_rollout_files",
      status: "complete",
      writableCount: writableChanges.length,
      lockedCount: initiallySkipped.length
    });

    let sqliteRowsToWrite = 0;
    let sqliteStats = null;
    if (repair) {
      sqliteStats = storage.stateDbLocation
        ? await readSqliteRepairStats(storage, {
            targetModel,
            userEventThreadIds: scan.userEventThreadIds,
            threadCwdById: scan.threadCwdById
          })
        : null;
      sqliteRowsToWrite = repairSqliteRowsToChange(sqliteStats, [...repairTargets]);
    } else {
      const sqliteCounts = storage.stateDbLocation ? await readSqliteProviderCounts(storage) : null;
      if (sqliteCounts?.unreadable === true) {
        await assertSqliteWritable(storage, { busyTimeoutMs: sqliteBusyTimeoutMs });
      }
      sqliteRowsToWrite = sqliteProviderRowsToChange(sqliteCounts, targetProvider);
    }
    const workspaceStats = repairTargets.has("workspaceRoots")
      ? await readWorkspaceRootRepairStats(storage, {
          cwdStats: cwdStatsFromThreadCwdMap(scan.threadCwdById)
        })
      : null;
    const workspaceMutationExpected = workspaceStats?.needsRepair === true;
    const writeExpected = configMutationExpected
      || writableChanges.length > 0
      || sqliteRowsToWrite > 0
      || workspaceMutationExpected;

    if (!writeExpected) {
      return {
        codexHome,
        sqliteHome: storage.sqliteHome,
        sqliteHomeSource: storage.sqliteHomeSource,
        targetProvider,
        ...(repair ? { repairTargets: [...repairTargets], targetModel } : {}),
        previousProvider: current.provider,
        backupDir: null,
        backupDurationMs: 0,
        noop: initiallySkipped.length === 0,
        partial: initiallySkipped.length > 0,
        partialReason: initiallySkipped.length > 0 ? "locked-session" : null,
        changedSessionFiles: 0,
        inPlaceSessionFiles: 0,
        rewrittenSessionFiles: 0,
        skippedLockedRolloutFiles: initiallySkipped,
        sqliteRowsUpdated: 0,
        sqliteProviderRowsUpdated: 0,
        sqliteModelRowsUpdated: 0,
        sqliteUserEventRowsUpdated: 0,
        sqliteCwdRowsUpdated: 0,
        updatedWorkspaceRoots: 0,
        savedWorkspaceRootCount: workspaceStats?.savedWorkspaceRootCount ?? 0,
        sqlitePresent: Boolean(storage.stateDbLocation),
        rolloutCountsBefore: summarizeProviderCounts(providerCounts),
        autoPruneResult: null,
        autoPruneWarning: null
      };
    }

    if (sqliteRowsToWrite > 0) {
      await assertSqliteWritable(storage, { busyTimeoutMs: sqliteBusyTimeoutMs });
    }
    throwIfAborted(signal);
    await faultInjector?.({ point: "before_backup" });
    emitProgress(onProgress, { stage: "create_backup", status: "start", writableCount: writableChanges.length });
    throwIfAborted(signal);
    const backupStartedAt = Date.now();
    backupDir = await undoBackup.capture({
      storage,
      codexHome,
      targetProvider,
      sessionChanges: writableChanges,
      configPath,
      configBackupText,
      faultInjector
    });
    backupDurationMs = Date.now() - backupStartedAt;
    emitProgress(onProgress, {
      stage: "create_backup",
      status: "complete",
      durationMs: backupDurationMs,
      backupDir
    });

    let failedStage = "mutation";
    let applyResult = { appliedChanges: 0, inPlaceChanges: 0, appliedPaths: [], skippedPaths: [] };
    let sqliteResult = emptySqliteMutationResult(Boolean(storage.stateDbLocation));
    let workspaceRootResult = {
      updated: false,
      updatedWorkspaceRoots: 0,
      savedWorkspaceRootCount: workspaceStats?.savedWorkspaceRootCount ?? 0
    };
    let mutationStarted = false;
    try {
      // Cancellation is intentionally closed after this boundary. Retrying the
      // same operation is the recovery model for ordinary writes.
      if (configMutationExpected && typeof afterBackup === "function") {
        failedStage = "update_config";
        emitProgress(onProgress, { stage: failedStage, status: "start" });
        await afterBackup(backupDir);
        mutationStarted = true;
        await faultInjector?.({ point: "after_config_mutation_before_applied", path: configPath });
        emitProgress(onProgress, { stage: failedStage, status: "complete" });
      }

      if (writableChanges.length > 0) {
        failedStage = "rewrite_rollout_files";
        emitProgress(onProgress, { stage: failedStage, status: "start", writableCount: writableChanges.length });
        applyResult = await applySessionChanges(writableChanges, {
          ...(repairTargets.has("models") ? { targetModel } : {}),
          onBeforeApply: (change) => faultInjector?.({ point: "before_rollout_apply", path: change.path }),
          onMutation: (change, mutation) => {
            mutationStarted = true;
            return faultInjector?.({
              point: "after_rollout_mutation_before_applied",
              path: change.path,
              mutation
            });
          },
          onApplied: (change) => faultInjector?.({ point: "after_rollout_apply", path: change.path }),
          onSkipped: (change, reason) => faultInjector?.({ point: "after_rollout_skip", path: change.path, reason })
        });
        emitProgress(onProgress, {
          stage: failedStage,
          status: "complete",
          appliedChanges: applyResult.appliedChanges,
          skippedChanges: applyResult.skippedPaths.length
        });
      }

      if (workspaceMutationExpected) {
        failedStage = "repair_workspace_roots";
        workspaceRootResult = await syncWorkspaceRoots(storage, {
          cwdStats: cwdStatsFromThreadCwdMap(scan.threadCwdById),
          onApplied: (targetPath) => {
            mutationStarted = true;
            return faultInjector?.({ point: "after_global_state_apply", path: targetPath });
          }
        });
      }

      if (sqliteRowsToWrite > 0) {
        failedStage = "update_sqlite";
        emitProgress(onProgress, { stage: failedStage, status: "start" });
        const sqliteOptions = {
          busyTimeoutMs: sqliteBusyTimeoutMs,
          ...(repair
            ? {
                targets: [...repairTargets].filter((target) => target !== "workspaceRoots"),
                targetModel,
                userEventThreadIds: scan.userEventThreadIds,
                threadCwdById: scan.threadCwdById
              }
            : {}),
          onCommitAttempt: () => {
            mutationStarted = true;
          },
          afterCommit: () => faultInjector?.({
            point: "after_sqlite_commit_before_ack",
            path: storage.stateDbLocation?.path ?? null
          })
        };
        sqliteResult = repair
          ? await sqliteTransaction.repair(storage, sqliteOptions)
          : await sqliteTransaction.updateProvider(storage, targetProvider, sqliteOptions);
        await faultInjector?.({ point: "after_sqlite_commit", path: storage.stateDbLocation?.path ?? null });
        emitProgress(onProgress, { stage: failedStage, status: "complete", updatedRows: sqliteResult.updatedRows });
      }
    } catch (error) {
      if (!mutationStarted) throw error;
      const skippedLockedRolloutFiles = [...new Set([
        ...initiallySkipped,
        ...(applyResult.skippedPaths ?? [])
      ])].sort((left, right) => left.localeCompare(right));
      await tryRefreshBackupInventory(backupDir);
      return {
        codexHome,
        sqliteHome: storage.sqliteHome,
        sqliteHomeSource: storage.sqliteHomeSource,
        targetProvider,
        ...(repair ? { repairTargets: [...repairTargets], targetModel } : {}),
        previousProvider: current.provider,
        backupDir,
        backupDurationMs,
        partial: true,
        partialReason: "mutation-failed",
        failedStage,
        retryRecommended: true,
        failureCode: typeof error?.code === "string" ? error.code : "WRITE_FAILED",
        partialWarning: `The operation stopped during ${failedStage}. Run the same operation again to converge, or restore the backup manually.`,
        changedSessionFiles: applyResult.appliedChanges ?? 0,
        inPlaceSessionFiles: applyResult.inPlaceChanges ?? 0,
        rewrittenSessionFiles: Math.max(0, (applyResult.appliedChanges ?? 0) - (applyResult.inPlaceChanges ?? 0)),
        skippedLockedRolloutFiles,
        sqliteRowsUpdated: sqliteResult.updatedRows,
        sqliteProviderRowsUpdated: sqliteResult.providerRowsUpdated,
        sqliteModelRowsUpdated: sqliteResult.modelRowsUpdated,
        sqliteUserEventRowsUpdated: sqliteResult.userEventRowsUpdated,
        sqliteCwdRowsUpdated: sqliteResult.cwdRowsUpdated,
        updatedWorkspaceRoots: workspaceRootResult.updatedWorkspaceRoots,
        savedWorkspaceRootCount: workspaceRootResult.savedWorkspaceRootCount,
        sqlitePresent: sqliteResult.databasePresent,
        rolloutCountsBefore: summarizeProviderCounts(providerCounts),
        autoPruneResult: null,
        autoPruneWarning: null
      };
    }

    const skippedLockedRolloutFiles = [...new Set([
      ...initiallySkipped,
      ...(applyResult.skippedPaths ?? [])
    ])].sort((left, right) => left.localeCompare(right));
    let backupInventoryWarning = null;
    try {
      await undoBackup.refreshInventory(backupDir, { faultInjector });
    } catch (error) {
      backupInventoryWarning = "Backup inventory refresh did not complete.";
    }
    let autoPruneResult = null;
    let autoPruneWarning = null;
    emitProgress(onProgress, { stage: "clean_backups", status: "start", keepCount });
    try {
      autoPruneResult = await pruneManagedBackups(codexHome, keepCount);
    } catch (error) {
      autoPruneWarning = "Automatic backup cleanup did not complete.";
    }
    emitProgress(onProgress, {
      stage: "clean_backups",
      status: "complete",
      deletedCount: autoPruneResult?.deletedCount ?? 0
    });

    return {
      codexHome,
      sqliteHome: storage.sqliteHome,
      sqliteHomeSource: storage.sqliteHomeSource,
      targetProvider,
      ...(repair ? { repairTargets: [...repairTargets], targetModel } : {}),
      previousProvider: current.provider,
      backupDir,
      backupDurationMs,
      partial: skippedLockedRolloutFiles.length > 0,
      partialReason: skippedLockedRolloutFiles.length > 0 ? "locked-session" : null,
      changedSessionFiles: applyResult.appliedChanges,
      inPlaceSessionFiles: applyResult.inPlaceChanges ?? 0,
      rewrittenSessionFiles: Math.max(0, applyResult.appliedChanges - (applyResult.inPlaceChanges ?? 0)),
      skippedLockedRolloutFiles,
      sqliteRowsUpdated: sqliteResult.updatedRows,
      sqliteProviderRowsUpdated: sqliteResult.providerRowsUpdated,
      ...(repair
        ? {
            sqliteModelRowsUpdated: sqliteResult.modelRowsUpdated,
            sqliteUserEventRowsUpdated: sqliteResult.userEventRowsUpdated,
            sqliteCwdRowsUpdated: sqliteResult.cwdRowsUpdated,
            updatedWorkspaceRoots: workspaceRootResult.updatedWorkspaceRoots,
            savedWorkspaceRootCount: workspaceRootResult.savedWorkspaceRootCount
          }
        : {}),
      sqlitePresent: sqliteResult.databasePresent,
      rolloutCountsBefore: summarizeProviderCounts(providerCounts),
      autoPruneResult,
      backupInventoryWarning,
      autoPruneWarning
    };
  } finally {
    await releaseLock();
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
  const rootModel = modelSync.applied && modelSync.model
    ? modelSync.model
    : readRootModelFromConfigText(nextConfigText);
  return { nextConfigText, modelSync, rootModel };
}

async function runSwitchCore({
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
  signal,
  expectedPlanState
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
  await faultInjector?.({
    point: "after_switch_storage_preflight",
    stateDbPath: storage.stateDbLocation?.path ?? null
  });
  const { nextConfigText, modelSync } = buildSwitchIntent(
    originalConfigText,
    provider,
    model,
    keepRootModel
  );
  const syncResult = await runSyncCore(
    {
      codexHome,
      sqliteHome,
      keepCount,
      onProgress,
      faultInjector,
      signal,
      expectedPlanState,
      platform
    },
    {
      targetProvider: provider,
      configBackupText: originalConfigText,
      configMutationExpected: nextConfigText !== originalConfigText,
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
    configUpdated: nextConfigText !== originalConfigText,
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
    const result = await restoreRecovery.execute({
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
    await releaseLock();
  }
}

function sqliteProviderRowsToChange(sqliteCounts, targetProvider) {
  let count = 0;
  for (const scope of ["sessions", "archived_sessions"]) {
    for (const [provider, providerCount] of Object.entries(sqliteCounts?.[scope] ?? {})) {
      if (provider !== targetProvider && Number.isSafeInteger(providerCount)) count += providerCount;
    }
  }
  return count;
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
    rolloutScanMode: options.rolloutScanMode ?? "metadata",
    platform: options.platform
  });
  // Capture the executable revision after all read-only status queries have
  // closed their SQLite handles; opening a WAL database may legitimately
  // update its SHM sidecar.
  const rolloutRevisionMode = options.rolloutRevisionMode
    ?? (options.rolloutScanMode === "full" ? "content" : "metadata");
  const revisions = await captureOperationRevisions({
    codexHome,
    profileRevision: profile.revision,
    configText,
    storage,
    backupDir,
    rolloutRevisionMode,
    platform: options.platform
  });
  operationCoordinator.cacheStatus(codexHome, status, options.platform);
  return { codexHome, sqliteHome, configText, storage, profile, revisions, status, rolloutRevisionMode };
}

async function issueProviderPlan(operation, options, switchIntent = null) {
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
  return issueProviderPlan("sync", options);
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
  for (const removed of ["fast", "syncMode"]) {
    if (Object.hasOwn(options, removed)) {
      throw new CoreError("INVALID_INPUT", `prepareSwitch no longer accepts ${removed}.`);
    }
  }
  const keepRootModel = Boolean(options.keepRootModel);
  const intent = buildSwitchIntent(configText, options.provider, options.model, keepRootModel);
  return issueProviderPlan("switch", { ...options, keepRootModel }, {
    provider: options.provider,
    rootModel: intent.rootModel,
    modelSync: intent.modelSync,
    configMutationExpected: intent.nextConfigText !== configText,
    modelMode: options.model !== undefined && options.model !== null
      ? "explicit"
      : (keepRootModel ? "keep-root-model" : "provider-default")
  });
}

const REPAIR_TARGET_ORDER = ["models", "cwd", "userEvent", "workspaceRoots"];

function normalizeRepairTargets(targets) {
  if (!Array.isArray(targets) || targets.length === 0) {
    throw new CoreError("INVALID_INPUT", "Repair requires at least one target.");
  }
  const selected = new Set();
  for (const target of targets) {
    if (typeof target !== "string" || !REPAIR_TARGET_ORDER.includes(target)) {
      throw new CoreError("INVALID_INPUT", `Unknown Repair target: ${String(target)}.`);
    }
    if (selected.has(target)) {
      throw new CoreError("INVALID_INPUT", `Duplicate Repair target: ${target}.`);
    }
    selected.add(target);
  }
  if (selected.has("workspaceRoots")) selected.add("cwd");
  return REPAIR_TARGET_ORDER.filter((target) => selected.has(target));
}

function repairSqliteRowsToChange(stats, targets) {
  const selected = new Set(targets);
  return (selected.has("models") ? stats?.modelRowsNeedingRepair ?? 0 : 0)
    + (selected.has("cwd") ? stats?.cwdRowsNeedingRepair ?? 0 : 0)
    + (selected.has("userEvent") ? stats?.userEventRowsNeedingRepair ?? 0 : 0);
}

async function issueRepairPlan(options) {
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
  return issueRepairPlan(options);
}

async function runRepairCore({ targets, ...options } = {}) {
  const normalizedTargets = normalizeRepairTargets(targets);
  return runSyncCore(options, {
    operationKind: "repair",
    repair: { targets: normalizedTargets }
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

function operationWarnings(result) {
  return [
    result?.partialWarning,
    result?.autoPruneWarning,
    result?.backupInventoryWarning,
    result?.modelSync?.warning
  ].filter((warning) => typeof warning === "string" && warning.trim());
}

function operationResult(operation, operationId, result, sourceBackup = null) {
  const partial = result?.partial === true
    || (Array.isArray(result?.skippedLockedRolloutFiles)
      && result.skippedLockedRolloutFiles.length > 0);
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
    warnings: operationWarnings(result),
    result
  };
}

export async function applySync(input, control) {
  return operationRuntime.applyPrepared(input, "sync", (options) => runSyncCore(options), control);
}

export async function applySwitch(input, control) {
  return operationRuntime.applyPrepared(input, "switch", (options) => runSwitchCore(options), control);
}

export async function applyRepair(input, control) {
  return operationRuntime.applyPrepared(input, "repair", (options) => runRepairCore(options), control);
}

export async function applyRestore(input, control) {
  return operationRuntime.applyPrepared(input, "restore", (options) => runRestoreCore(options), control);
}

// Internal scheduler hook used by Watch. It exposes completion only for a
// same-process manual operation; external writers remain event-driven and are
// never polled or queued behind.
export function waitForManualOperationEnd({ codexHome, platform } = {}) {
  return operationRuntime.waitForManualOperationEnd({ codexHome, platform });
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

/** @deprecated Compatibility adapter. New transports must use prepareRepair/applyRepair. */
export async function runRepair(options = {}) {
  const plan = await prepareRepair(options);
  return (await applyRepair({ schemaVersion: 1, planId: plan.planId })).result;
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
