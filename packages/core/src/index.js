// @ts-check

// C4 keeps the proven high-risk implementation in root src/. This factory is
// the only transitional import allowed to cross that boundary. Product inputs
// contain a profile selector only; trusted hosts resolve all filesystem paths.
import { createHash } from "node:crypto";
import path from "node:path";

import {
  CoreError,
  applyRestore as applyRestoreInternal,
  applySwitch as applySwitchInternal,
  applySync as applySyncInternal,
  getDiagnostics as getDiagnosticsInternal,
  getHistorySession as getHistorySessionInternal,
  getStatus as getStatusInternal,
  getWatchStatus as getWatchStatusInternal,
  listBackups as listBackupsInternal,
  listHistory as listHistoryInternal,
  prepareRestore as prepareRestoreInternal,
  prepareSwitch as prepareSwitchInternal,
  prepareSync as prepareSyncInternal,
  pruneBackups as pruneBackupsInternal,
  startWatch as startWatchInternal,
  stopWatch as stopWatchInternal
} from "../../../src/public-api.js";

/** @typedef {{profileId: string, profileRevision?: string}} ProfileSelector */
/** @typedef {{id: string, revision: string, codexHome: string, sqliteHome?: string}} ResolvedProfile */
/** @typedef {(selector: ProfileSelector) => ResolvedProfile | Promise<ResolvedProfile>} ProfileResolver */
/** @typedef {Record<string, unknown>} JsonRecord */
/** @typedef {{
 * getStatus: (input: JsonRecord) => Promise<unknown>,
 * prepareSync: (input: JsonRecord) => Promise<unknown>,
 * applySync: (input: JsonRecord) => Promise<unknown>,
 * prepareSwitch: (input: JsonRecord) => Promise<unknown>,
 * applySwitch: (input: JsonRecord) => Promise<unknown>,
 * listBackups: (input: JsonRecord) => Promise<unknown>,
 * prepareRestore: (input: JsonRecord) => Promise<unknown>,
 * applyRestore: (input: JsonRecord) => Promise<unknown>,
 * pruneBackups: (input: JsonRecord) => Promise<unknown>,
 * listHistory: (input: JsonRecord) => Promise<unknown>,
 * getHistorySession: (input: JsonRecord) => Promise<unknown>,
 * startWatch: (input: JsonRecord) => Promise<unknown>,
 * stopWatch: (input: JsonRecord) => Promise<unknown>,
 * getWatchStatus: (input?: JsonRecord) => Promise<unknown>,
 * getDiagnostics: (input: JsonRecord) => Promise<unknown>
 * }} CoreFacade */

const PROFILE_ID_PATTERN = /^[A-Za-z0-9._-]{1,80}$/;

/** @param {unknown} value @returns {value is Record<string, unknown>} */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** @param {unknown} input */
function requireProfileSelector(input) {
  if (!isRecord(input) || !isRecord(input.profile)) {
    throw new CoreError("INVALID_INPUT", "A trusted profile selector is required.");
  }
  const selector = input.profile;
  const allowedKeys = new Set(["profileId", "profileRevision"]);
  if (Object.keys(selector).some((key) => !allowedKeys.has(key))
      || typeof selector.profileId !== "string"
      || !PROFILE_ID_PATTERN.test(selector.profileId)
      || (selector.profileRevision !== undefined
        && (typeof selector.profileRevision !== "string"
          || !selector.profileRevision
          || selector.profileRevision.length > 512))) {
    throw new CoreError("INVALID_INPUT", "The profile selector is invalid.");
  }
  return /** @type {ProfileSelector} */ ({
    profileId: selector.profileId,
    ...(selector.profileRevision === undefined
      ? {}
      : { profileRevision: selector.profileRevision })
  });
}

/** @param {ResolvedProfile} value @param {ProfileSelector} selector */
function validateResolvedProfile(value, selector) {
  if (!isRecord(value)
      || typeof value.id !== "string"
      || value.id !== selector.profileId
      || !PROFILE_ID_PATTERN.test(value.id)
      || typeof value.revision !== "string"
      || !value.revision
      || value.revision.length > 512
      || typeof value.codexHome !== "string"
      || !path.isAbsolute(value.codexHome)
      || (value.sqliteHome !== undefined
        && (typeof value.sqliteHome !== "string" || !path.isAbsolute(value.sqliteHome)))) {
    throw new CoreError("INVALID_INPUT", "The trusted profile resolver returned an invalid profile.");
  }
  if (selector.profileRevision !== undefined && selector.profileRevision !== value.revision) {
    throw new CoreError("PROFILE_CHANGED", "The selected profile changed. Prepare the operation again.");
  }
  return Object.freeze({
    id: value.id,
    revision: value.revision,
    codexHome: path.resolve(value.codexHome),
    ...(value.sqliteHome ? { sqliteHome: path.resolve(value.sqliteHome) } : {})
  });
}

/** @param {ResolvedProfile} profile @param {string} [revision] */
function rootProfileInput(profile, revision = profile.revision) {
  return {
    codexHome: profile.codexHome,
    ...(profile.sqliteHome ? { sqliteHome: profile.sqliteHome } : {}),
    profileId: profile.id,
    profileRevision: revision
  };
}

/** @param {unknown} value @param {ResolvedProfile} profile */
function withPublicProfile(value, profile) {
  if (!isRecord(value)) return value;
  return { ...value, profile: { id: profile.id, revision: profile.revision } };
}

/** @param {unknown} value */
function publicWarnings(value) {
  if (!Array.isArray(value)) return [];
  /** @type {string[]} */
  const result = [];
  for (const warning of value.filter((entry) => typeof entry === "string")) {
    let projected;
    if (warning.startsWith("Backup inventory refresh failed:")) {
      projected = "Backup inventory refresh failed.";
    } else if (warning.startsWith("Automatic backup cleanup failed:")) {
      projected = "Automatic backup cleanup failed.";
    } else if (warning.startsWith("Encrypted content warning:")) {
      projected = "Some encrypted histories may require their original Provider or account for continuation.";
    } else if (/^\d+ rollout file\(s\) are currently locked/.test(warning)) {
      projected = "One or more rollout files are locked and may be skipped.";
    } else if (warning.startsWith("Provider \"") && warning.includes("has no model field")) {
      projected = "The selected Provider has no default model; the root model will remain unchanged.";
    } else if (warning === "Project visibility diagnostics are unavailable; the write operation will still validate and protect the global state with backup-first recovery.") {
      projected = "Project visibility diagnostics are unavailable; backup-first protection remains enabled.";
    } else if (warning === "SQLite Home relocation is explicit; config.toml will not be restored.") {
      projected = "SQLite Home relocation is confirmed; config.toml will not be restored.";
    } else {
      projected = "The operation produced an additional warning.";
    }
    if (!result.includes(projected)) result.push(projected);
  }
  return result;
}

/** @param {unknown} value */
function publicOperationState(value) {
  if (!isRecord(value)) return null;
  /** @type {Record<string, string>} */
  const result = {};
  for (const key of [
    "operationId",
    "operation",
    "actor",
    "runtime",
    "startedAt",
    "busyScope",
    "lockState",
    "errorCode"
  ]) {
    const candidate = value[key];
    if (typeof candidate === "string") result[key] = candidate;
  }
  return result;
}

/** @param {unknown} value */
function publicStatus(value) {
  if (!isRecord(value)) return value;
  const rolloutCounts = isRecord(value.rolloutCounts) ? value.rolloutCounts : {};
  const sqliteCounts = value.sqliteCounts ?? {};
  const provider = typeof value.currentProvider === "string" && value.currentProvider
    ? value.currentProvider
    : "openai";
  /** @param {unknown} distribution */
  const matchesProvider = (distribution) => isRecord(distribution)
    && ["sessions", "archived_sessions"].every((scope) => {
      const counts = isRecord(distribution[scope]) ? distribution[scope] : {};
      return Object.entries(counts).every(([candidate, count]) => Number(count) === 0 || candidate === provider);
    });
  const operation = publicOperationState(value.operationInProgress);
  const locked = Array.isArray(value.lockedRolloutFiles)
    ? value.lockedRolloutFiles.filter((entry) => typeof entry === "string").map((entry) => path.basename(entry))
    : [];
  const pending = Array.isArray(value.pendingTransactions)
    ? value.pendingTransactions.filter(isRecord).map((transaction) => ({
        operationId: typeof transaction.operationId === "string" ? transaction.operationId : null,
        state: typeof transaction.state === "string" ? transaction.state : "unknown"
      }))
    : [];
  const backupSummary = isRecord(value.backupSummary) ? value.backupSummary : {};
  const blocked = isRecord(value.statusReadBlocked) && typeof value.statusReadBlocked.reason === "string"
    ? { reason: value.statusReadBlocked.reason }
    : undefined;
  const sqliteReadable = isRecord(sqliteCounts) && sqliteCounts.unreadable !== true;
  return {
    schemaVersion: 1,
    snapshotAt: value.snapshotAt,
    storageRevision: value.storageRevision,
    profile: value.profile,
    currentProvider: provider,
    ...(typeof value.currentModel === "string" || value.currentModel === null
      ? { currentModel: value.currentModel }
      : {}),
    rolloutCounts,
    ...(isRecord(value.modelCounts) ? { modelCounts: value.modelCounts } : {}),
    sqliteCounts,
    codexHomeSource: "profile",
    sqliteHomeSource: value.sqliteHomeSource,
    backupSummary: {
      count: Number.isSafeInteger(backupSummary.count) ? backupSummary.count : 0,
      totalBytes: Number.isSafeInteger(backupSummary.totalBytes) ? backupSummary.totalBytes : 0
    },
    pendingRecovery: pending.length > 0 || value.pendingRecovery === true,
    pendingTransactions: pending,
    operationInProgress: operation,
    rolloutScanComplete: value.rolloutScanComplete === true && locked.length === 0,
    lockedRolloutFiles: locked,
    currentProviderImplicit: value.currentProviderImplicit === true,
    configuredProviders: Array.isArray(value.configuredProviders)
      ? value.configuredProviders.filter((entry) => typeof entry === "string")
      : [],
    alignment: {
      aligned: Boolean(!operation
        && !blocked
        && sqliteReadable
        && value.rolloutScanComplete === true
        && locked.length === 0
        && matchesProvider(rolloutCounts)
        && matchesProvider(sqliteCounts)),
      sqliteReadable,
      targetProvider: provider
    },
    ...(blocked ? { statusReadBlocked: blocked } : {})
  };
}

/** @param {unknown} value */
function publicPlan(value) {
  if (!isRecord(value)) return value;
  const target = isRecord(value.target) ? value.target : {};
  const impact = isRecord(value.impact) ? value.impact : {};
  /** @type {Record<string, string | boolean | null>} */
  const publicTarget = {};
  for (const key of ["provider", "model", "modelMode", "backupId"]) {
    const candidate = target[key];
    if (typeof candidate === "string" || candidate === null) publicTarget[key] = candidate;
  }
  for (const key of ["restoreConfig", "restoreDatabase", "restoreSessions", "allowSqliteHomeRelocation"]) {
    if (typeof target[key] === "boolean") publicTarget[key] = target[key];
  }
  /** @type {Record<string, unknown>} */
  const publicImpact = {};
  for (const [key, candidate] of Object.entries(impact)) {
    if (typeof candidate === "boolean" || (Number.isSafeInteger(candidate) && Number(candidate) >= 0)) {
      publicImpact[key] = candidate;
    }
  }
  if (Array.isArray(impact.lockedRolloutFiles)) {
    publicImpact.lockedRolloutFiles = impact.lockedRolloutFiles
      .filter((entry) => typeof entry === "string")
      .map((entry) => path.basename(entry));
  }
  return {
    schemaVersion: 1,
    planId: value.planId,
    operation: value.operation,
    createdAt: value.createdAt,
    expiresAt: value.expiresAt,
    profile: value.profile,
    storageRevision: value.storageRevision,
    configRevision: value.configRevision,
    rolloutRevision: value.rolloutRevision,
    stateDbRevision: value.stateDbRevision,
    ...(typeof value.backupRevision === "string" ? { backupRevision: value.backupRevision } : {}),
    target: publicTarget,
    impact: publicImpact,
    warnings: publicWarnings(value.warnings),
    requiresConfirmation: value.requiresConfirmation === true
  };
}

/** @param {unknown} value */
function publicOperationResult(value) {
  if (!isRecord(value)) return value;
  const backup = isRecord(value.backup) && typeof value.backup.backupId === "string"
    ? { backupId: value.backup.backupId }
    : null;
  const source = isRecord(value.result) ? value.result : {};
  /** @type {Record<string, unknown>} */
  const result = {};
  for (const key of [
    "targetProvider",
    "targetModel",
    "modelSource"
  ]) {
    const candidate = source[key];
    if (typeof candidate === "string" || candidate === null) result[key] = candidate;
  }
  for (const [key, candidate] of Object.entries(source)) {
    if ((key.endsWith("Count") || key.endsWith("Changed") || key.endsWith("Updated") || key.endsWith("Restored"))
        && Number.isSafeInteger(candidate)
        && Number(candidate) >= 0) {
      result[key] = candidate;
    }
  }
  if (Array.isArray(source.skippedLockedRolloutFiles)) {
    result.skippedLockedRolloutFiles = source.skippedLockedRolloutFiles
      .filter((entry) => typeof entry === "string")
      .map((entry) => path.basename(entry));
  }
  return {
    schemaVersion: 1,
    operationId: value.operationId,
    operation: value.operation,
    outcome: value.outcome,
    backup,
    warnings: publicWarnings(value.warnings),
    result
  };
}

/** @param {unknown} value */
function publicBackupMetadata(value) {
  const metadata = isRecord(value) ? value : {};
  /** @type {Record<string, string | number>} */
  const result = {};
  for (const key of ["version", "namespace", "targetProvider", "createdAt"] ) {
    const candidate = metadata[key];
    if (typeof candidate === "string") result[key] = candidate;
    else if (typeof candidate === "number" && Number.isSafeInteger(candidate)) result[key] = candidate;
  }
  for (const key of ["changedSessionFiles", "fileCount"] ) {
    const candidate = metadata[key];
    if (Number.isSafeInteger(candidate) && Number(candidate) >= 0) result[key] = Number(candidate);
  }
  return result;
}

/** @param {unknown} value */
function publicHistorySummary(value) {
  if (!isRecord(value)) return value;
  return {
    id: value.id,
    title: value.title,
    provider: value.provider,
    ...(value.model === undefined ? {} : { model: value.model }),
    archived: value.archived,
    ...(typeof value.createdAt === "string" ? { createdAt: value.createdAt } : {}),
    updatedAt: value.updatedAt,
    messageCount: value.messageCount
  };
}

/** @param {string} value */
function compositeRevision(value) {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

/**
 * Create the shared Core facade for a trusted host. The resolver is the only
 * component allowed to translate product profile identifiers into paths.
 * @param {{resolveProfile: ProfileResolver}} options
 */
export function createCoreFacade({ resolveProfile }) {
  if (typeof resolveProfile !== "function") {
    throw new TypeError("createCoreFacade requires a trusted resolveProfile function.");
  }

  /** @param {ProfileSelector} selector */
  async function resolveTrusted(selector) {
    return validateResolvedProfile(await resolveProfile(selector), selector);
  }

  function currentProfileResolver() {
    /** @param {string} profileId */
    return async (profileId) => resolveTrusted({ profileId: String(profileId) });
  }

  /** @param {unknown} input */
  async function trustedInput(input) {
    const selector = requireProfileSelector(input);
    const profile = await resolveTrusted(selector);
    return { input: /** @type {JsonRecord} */ (input), profile };
  }

  /** @type {CoreFacade} */
  const facade = {
    async getStatus(input) {
      const trusted = await trustedInput(input);
      return publicStatus(withPublicProfile(
        await getStatusInternal(rootProfileInput(trusted.profile)),
        trusted.profile
      ));
    },

    async prepareSync(input) {
      const trusted = await trustedInput(input);
      const plan = await prepareSyncInternal({
        ...rootProfileInput(trusted.profile),
        ...(trusted.input.keepCount === undefined ? {} : { keepCount: trusted.input.keepCount }),
        profileResolver: currentProfileResolver()
      });
      return publicPlan(withPublicProfile(plan, trusted.profile));
    },

    async applySync(input) {
      return publicOperationResult(await applySyncInternal(input));
    },

    async prepareSwitch(input) {
      const trusted = await trustedInput(input);
      const provider = trusted.input.provider;
      const modelMode = trusted.input.modelMode;
      if (typeof provider !== "string" || !provider
          || !["provider-default", "keep-root-model", "explicit"].includes(String(modelMode))) {
        throw new CoreError("INVALID_INPUT", "The Switch Provider input is invalid.");
      }
      if ((modelMode === "explicit" && (typeof trusted.input.model !== "string" || !trusted.input.model))
          || (modelMode !== "explicit" && trusted.input.model !== undefined)) {
        throw new CoreError("INVALID_INPUT", "The selected model mode and model are inconsistent.");
      }
      const plan = await prepareSwitchInternal({
        ...rootProfileInput(trusted.profile),
        provider,
        ...(modelMode === "explicit" ? { model: trusted.input.model } : {}),
        ...(modelMode === "keep-root-model" ? { keepRootModel: true } : {}),
        ...(trusted.input.keepCount === undefined ? {} : { keepCount: trusted.input.keepCount }),
        profileResolver: currentProfileResolver()
      });
      return publicPlan(withPublicProfile(plan, trusted.profile));
    },

    async applySwitch(input) {
      return publicOperationResult(await applySwitchInternal(input));
    },

    async listBackups(input) {
      const trusted = await trustedInput(input);
      const inventoryValue = await listBackupsInternal(trusted.profile.codexHome);
      const inventory = isRecord(inventoryValue) ? inventoryValue : {};
      const backups = Array.isArray(inventory.backups) ? inventory.backups : [];
      return {
        backups: backups.filter(isRecord).map((backup) => ({
          backupId: backup.id,
          sizeBytes: backup.sizeBytes,
          metadata: publicBackupMetadata(backup.metadata)
        }))
      };
    },

    async prepareRestore(input) {
      const trusted = await trustedInput(input);
      let executionProfile = trusted.profile;
      let profileResolver = currentProfileResolver();
      if (trusted.input.relocationTargetProfileId !== undefined) {
        if (trusted.input.allowSqliteHomeRelocation !== true
            || trusted.input.restoreConfig !== false
            || typeof trusted.input.relocationTargetProfileId !== "string") {
          throw new CoreError("INVALID_INPUT", "SQLite relocation requires an explicit target and config restore disabled.");
        }
        const targetId = trusted.input.relocationTargetProfileId;
        const target = await resolveTrusted({ profileId: targetId });
        if (!target.sqliteHome) {
          throw new CoreError("INVALID_INPUT", "The relocation target profile has no explicit SQLite Home.");
        }
        const revision = compositeRevision(JSON.stringify([
          trusted.profile.id,
          trusted.profile.revision,
          target.id,
          target.revision
        ]));
        executionProfile = { ...trusted.profile, sqliteHome: target.sqliteHome, revision };
        profileResolver = async (/** @type {string} */ profileId) => {
          const [current, currentTarget] = await Promise.all([
            resolveTrusted({ profileId: String(profileId) }),
            resolveTrusted({ profileId: targetId })
          ]);
          if (!currentTarget.sqliteHome) {
            throw new CoreError("PROFILE_CHANGED", "The relocation target profile changed.");
          }
          return {
            ...current,
            sqliteHome: currentTarget.sqliteHome,
            revision: compositeRevision(JSON.stringify([
              current.id,
              current.revision,
              currentTarget.id,
              currentTarget.revision
            ]))
          };
        };
      }
      const plan = await prepareRestoreInternal({
        ...rootProfileInput(executionProfile),
        backupId: trusted.input.backupId,
        restoreConfig: trusted.input.restoreConfig,
        restoreDatabase: trusted.input.restoreDatabase,
        restoreSessions: trusted.input.restoreSessions,
        ...(trusted.input.allowSqliteHomeRelocation === undefined
          ? {}
          : { allowSqliteHomeRelocation: trusted.input.allowSqliteHomeRelocation }),
        profileResolver
      });
      return publicPlan(withPublicProfile(plan, trusted.profile));
    },

    async applyRestore(input) {
      return publicOperationResult(await applyRestoreInternal(input));
    },

    async pruneBackups(input) {
      const trusted = await trustedInput(input);
      const result = await pruneBackupsInternal({
        codexHome: trusted.profile.codexHome,
        keepCount: trusted.input.keepCount
      });
      if (!isRecord(result)) return result;
      return { deletedCount: result.deletedCount, remainingCount: result.remainingCount, freedBytes: result.freedBytes };
    },

    async listHistory(input) {
      const trusted = await trustedInput(input);
      const { profile: _profile, ...options } = trusted.input;
      const resultValue = await listHistoryInternal(trusted.profile.codexHome, options);
      if (!isRecord(resultValue)) return resultValue;
      return {
        ...resultValue,
        sessions: Array.isArray(resultValue.sessions) ? resultValue.sessions.map(publicHistorySummary) : []
      };
    },

    async getHistorySession(input) {
      const trusted = await trustedInput(input);
      if (typeof trusted.input.sessionId !== "string" || !trusted.input.sessionId) {
        throw new CoreError("INVALID_INPUT", "sessionId is required.");
      }
      const resultValue = await getHistorySessionInternal(
        trusted.profile.codexHome,
        trusted.input.sessionId,
        trusted.input.messageLimit === undefined
          ? {}
          : { messageLimit: trusted.input.messageLimit }
      );
      if (!isRecord(resultValue)) return resultValue;
      return { ...resultValue, session: publicHistorySummary(resultValue.session) };
    },

    async startWatch(input) {
      const trusted = await trustedInput(input);
      return startWatchInternal({
        ...rootProfileInput(trusted.profile),
        ...(trusted.input.includeStateDb === undefined ? {} : { includeStateDb: trusted.input.includeStateDb }),
        ...(trusted.input.debounceMs === undefined ? {} : { debounceMs: trusted.input.debounceMs }),
        ...(trusted.input.once === undefined ? {} : { once: trusted.input.once })
      });
    },

    async stopWatch(input) {
      return stopWatchInternal(input);
    },

    async getWatchStatus(input = {}) {
      return input.watchId
        ? getWatchStatusInternal({ watchId: input.watchId })
        : getWatchStatusInternal();
    },

    async getDiagnostics(input) {
      const trusted = await trustedInput(input);
      const value = await getDiagnosticsInternal(rootProfileInput(trusted.profile));
      if (!isRecord(value)) return value;
      const runtime = isRecord(value.runtime) ? value.runtime : {};
      const storage = isRecord(value.storage) ? value.storage : {};
      const provider = isRecord(value.provider) ? value.provider : {};
      const safety = isRecord(value.safety) ? value.safety : {};
      return {
        schemaVersion: 1,
        generatedAt: value.generatedAt,
        runtime: {
          ...(typeof runtime.node === "string" ? { node: runtime.node } : {}),
          ...(typeof runtime.platform === "string" ? { platform: runtime.platform } : {}),
          ...(typeof runtime.arch === "string" ? { arch: runtime.arch } : {})
        },
        storage: {
          ...(typeof storage.sqliteHomeSource === "string" ? { sqliteHomeSource: storage.sqliteHomeSource } : {}),
          stateDbFound: storage.stateDbLocation !== null,
          sqliteSupported: !isRecord(storage.sqliteAccess) || storage.sqliteAccess.supported !== false
        },
        provider: {
          ...(typeof provider.current === "string" ? { current: provider.current } : {}),
          implicit: provider.implicit === true,
          configured: Array.isArray(provider.configured)
            ? provider.configured.filter((entry) => typeof entry === "string")
            : [],
          ...(isRecord(provider.rolloutCounts) ? { rolloutCounts: provider.rolloutCounts } : {}),
          ...(isRecord(provider.sqliteCounts) ? { sqliteCounts: provider.sqliteCounts } : {})
        },
        safety: {
          ...(typeof safety.storageRevision === "string" ? { storageRevision: safety.storageRevision } : {}),
          pendingRecovery: safety.pendingRecovery === true,
          pendingTransactions: Array.isArray(safety.pendingTransactions)
            ? safety.pendingTransactions.filter(isRecord).map((transaction) => ({
                operationId: typeof transaction.operationId === "string" ? transaction.operationId : null,
                state: typeof transaction.state === "string" ? transaction.state : "unknown"
              }))
            : [],
          operationInProgress: publicOperationState(safety.operationInProgress),
          rolloutScanComplete: safety.rolloutScanComplete === true,
          lockedRolloutCount: Number.isSafeInteger(safety.lockedRolloutCount) ? safety.lockedRolloutCount : 0,
          projectThreadVisibilityAvailable: safety.projectThreadVisibilityAvailable === true
        }
      };
    }
  };

  return Object.freeze(facade);
}
