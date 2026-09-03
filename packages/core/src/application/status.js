// @ts-nocheck

import {
  DEFAULT_LOCK_NAME,
  DEFAULT_PROVIDER,
  defaultBackupRoot,
  getBackupSummary,
  inspectPathLock,
  normalizeCodexHome,
  operationCoordinator,
  captureOperationRevisions,
  captureStorageRevision,
  revisionMismatch,
  sha256Revision,
  findPendingTransactions,
  codexStorage,
  path
} from "../infrastructure/node-core-ports.js";
import {
  createProfileSnapshot,
  explicitSqliteHomeFromOptions,
  pathComparisonKey,
  prepareStorage,
  profileFromOptions,
  sumCounts
} from "./runtime-support.js";

const {
  listConfiguredProviderIds,
  readConfigText,
  readCurrentProviderFromConfigText,
  readRootModelFromConfigText
} = codexStorage.config;
const {
  collectDiagnosticsFacts,
  collectStatusRolloutMetadata,
  summarizeProviderCounts
} = codexStorage.sessions;
const { readSqliteProviderCounts, readSqliteRepairStats } = codexStorage.stateDb;
const {
  cwdStatsFromThreadCwdMap,
  readProjectThreadVisibility,
  readWorkspaceRootRepairStats
} = codexStorage.globalState;

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

export async function scanStatus({
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

export function createStatusUseCase() {
  return Object.freeze({ getStatus });
}
