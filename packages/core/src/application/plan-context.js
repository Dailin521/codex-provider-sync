// @ts-nocheck

import {
  CoreError,
  withFailureStage,
  assertSqliteAccessSupported,
  captureOperationRevisions,
  findPendingTransactions,
  normalizeCodexHome,
  operationCoordinator,
  revisionMismatch,
  codexStorage,
  path
} from "../infrastructure/node-core-ports.js";
import {
  createProfileSnapshot,
  explicitSqliteHomeFromOptions,
  prepareStorage,
  profileFromOptions
} from "./runtime-support.js";
import { scanStatus } from "./status.js";

const { readConfigText } = codexStorage.config;
const { collectProviderPreparationFacts } = codexStorage.sessions;

export async function verifyExpectedPlanState({
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
    minimumRolloutSizes: expectedPlanState.revisions.providerRolloutSizes,
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

export async function assertNoPendingRestoreTransactions(codexHome) {
  const pending = await findPendingTransactions(codexHome);
  const blocking = pending.filter((entry) => entry.operationKind === "restore");
  if (blocking.length === 0) return;
  throw new CoreError(
    "PENDING_TRANSACTION",
    "An unfinished Restore must be resolved before another write operation can start.",
    { recoveryRequired: true, details: { pendingCount: blocking.length } }
  );
}

export async function preparePlanContext(options, operation, { backupDir = null, resolveProviderTarget = null } = {}) {
  const codexHome = options.storage?.codexHome ?? normalizeCodexHome(options.codexHome);
  if (operationCoordinator.isActive(codexHome, options.platform)) {
    throw new CoreError("OPERATION_BUSY", "Lock already exists for this Codex Home; another write operation is active.", {
      details: { busyScope: "codex-home" }
    });
  }
  const sqliteHome = explicitSqliteHomeFromOptions(options);
  const configPath = path.join(codexHome, "config.toml");
  const configText = await withFailureStage("prepare_config", () => readConfigText(configPath));
  if (options.expectedConfigText !== undefined && configText !== options.expectedConfigText) {
    throw new CoreError("PLAN_STALE", "config.toml changed after the operation was confirmed. Refresh and retry.");
  }
  const storage = await withFailureStage("prepare_storage", () => prepareStorage({ codexHome, sqliteHome, configText, platform: options.platform }));
  assertSqliteAccessSupported(storage, operation);
  const profile = profileFromOptions(options, codexHome, sqliteHome, options.platform);
  // The use case supplies this resolver directly, never through caller options.
  // Only Provider Prepare shares ephemeral facts; Apply/Repair/Restore still read afresh.
  const providerFacts = resolveProviderTarget && (operation === "sync" || operation === "switch")
    ? await withFailureStage("prepare_rollouts", () => collectProviderPreparationFacts(codexHome, resolveProviderTarget(configText))) : null;
  const status = await withFailureStage("prepare_status", () => scanStatus({
    codexHome,
    sqliteHome,
    storage,
    configText,
    profileId: profile.id,
    profileRevision: profile.suppliedRevision,
    rolloutScanMode: options.rolloutScanMode ?? "metadata",
    // Provider previews show the same independent ownership metric as Overview.
    // Actual write targets are still probed separately by the use case.
    includeSessionActivity: operation === "sync" || operation === "switch",
    platform: options.platform
  }, providerFacts ? { ...providerFacts.scan, incompletePaths: [] } : null));
  const rolloutRevisionMode = (operation === "sync" || operation === "switch" ? "provider" : options.rolloutRevisionMode)
    ?? (options.rolloutScanMode === "full" ? "content" : "metadata");
  const revisions = await withFailureStage("prepare_revisions", () => captureOperationRevisions({
    codexHome,
    profileRevision: profile.revision,
    configText,
    storage,
    backupDir,
    rolloutRevisionMode,
    platform: options.platform
  }, providerFacts?.rollout));
  operationCoordinator.cacheStatus(codexHome, status, options.platform);
  return { codexHome, sqliteHome, configText, storage, profile, revisions, status, rolloutRevisionMode,
    providerScan: providerFacts?.scan };
}
