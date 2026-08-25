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
function publicOperationResult(value) {
  if (!isRecord(value)) return value;
  const backup = isRecord(value.backup) && typeof value.backup.backupId === "string"
    ? { backupId: value.backup.backupId }
    : null;
  return { ...value, backup };
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
    cwd: value.cwd,
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
      return withPublicProfile(await getStatusInternal(rootProfileInput(trusted.profile)), trusted.profile);
    },

    async prepareSync(input) {
      const trusted = await trustedInput(input);
      const plan = await prepareSyncInternal({
        ...rootProfileInput(trusted.profile),
        ...(trusted.input.keepCount === undefined ? {} : { keepCount: trusted.input.keepCount }),
        profileResolver: currentProfileResolver()
      });
      return withPublicProfile(plan, trusted.profile);
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
      return withPublicProfile(plan, trusted.profile);
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
      return withPublicProfile(plan, trusted.profile);
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
      return getDiagnosticsInternal(rootProfileInput(trusted.profile));
    }
  };

  return Object.freeze(facade);
}
