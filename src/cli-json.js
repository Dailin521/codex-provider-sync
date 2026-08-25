export const CLI_JSON_SCHEMA_VERSION = 1;

const SUCCESS_OUTCOMES = new Set(["completed", "noop", "partial"]);
const FAILURE_OUTCOMES = new Set([
  "failed",
  "failed_rolled_back",
  "recovery_required",
  "cancelled",
  "stale"
]);
const STALE_CODES = new Set([
  "INVALID_INPUT",
  "PLAN_EXPIRED",
  "PLAN_STALE",
  "STALE_STATE",
  "PROFILE_CHANGED",
  "STORAGE_CHANGED"
]);
const RECOVERY_CODES = new Set(["RECOVERY_REQUIRED", "PENDING_TRANSACTION"]);
const BUSY_CODES = new Set(["OPERATION_BUSY", "LOCK_UNVERIFIABLE", "SQLITE_BUSY"]);
const SEVERITIES = new Set(["info", "warning", "error", "fatal"]);
const WARNING_ERROR_CODES = new Set([
  "PROFILE_CHANGED",
  "STORAGE_CHANGED",
  "PLAN_STALE",
  "PLAN_EXPIRED",
  "STALE_STATE",
  "SQLITE_BUSY",
  "ROLLOUT_LOCKED",
  "ROLLOUT_CHANGED",
  "OPERATION_BUSY"
]);
const LOCK_SCOPES = new Set(["codex-home", "state-db"]);
const SAFE_REASONS = new Set([
  "profile",
  "config",
  "storage",
  "rollout",
  "state-db",
  "windows-wsl-unc"
]);
const SQLITE_HOME_SOURCES = new Set(["cli", "config", "env", "default"]);
const OPERATION_KINDS = new Set(["sync", "switch", "restore", "prune-backups", "watch"]);
const PENDING_STATES = new Set([
  "prepared",
  "applying",
  "committing",
  "committed",
  "committed-pending-ack",
  "rollback-pending",
  "rollingBack",
  "rolledBack",
  "rolled-back",
  "recoveryRequired",
  "recovery-required"
]);
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SAFE_CAUSE_CODES = new Set([
  "ENOENT",
  "EACCES",
  "EPERM",
  "EIO",
  "EBUSY",
  "SQLITE_BUSY",
  "SQLITE_LOCKED",
  "SQLITE_CORRUPT",
  "SQLITE_NOTADB",
  "ERR_SQLITE_ERROR"
]);

const CLI_ERROR_MESSAGES = Object.freeze({
  INVALID_INPUT: "The command input is invalid.",
  PROFILE_CHANGED: "The selected profile changed. Prepare the operation again.",
  STORAGE_CHANGED: "The resolved storage changed. Prepare the operation again.",
  PLAN_STALE: "The prepared operation is stale. Prepare it again.",
  PLAN_EXPIRED: "The prepared operation expired. Prepare it again.",
  STALE_STATE: "The protected state changed. Prepare the operation again.",
  CODEX_HOME_NOT_FOUND: "The selected Codex Home was not found.",
  STATE_DB_NOT_FOUND: "The selected state database was not found.",
  SQLITE_UNSUPPORTED_PATH: "The selected SQLite path is not supported by this runtime.",
  SQLITE_BUSY: "The state database is busy. Close Codex processes and retry.",
  SQLITE_UNREADABLE: "The state database is unreadable or malformed.",
  ROLLOUT_LOCKED: "One or more rollout files are locked.",
  ROLLOUT_CHANGED: "One or more rollout files changed during the operation.",
  PENDING_TRANSACTION: "An unfinished transaction must be resolved before another write.",
  BACKUP_FAILED: "The required backup could not be completed.",
  SYNC_FAILED_ROLLED_BACK: "The operation failed and its changes were rolled back.",
  RECOVERY_REQUIRED: "The operation requires explicit recovery.",
  RESTORE_VALIDATION_FAILED: "The selected backup or restore target failed validation.",
  PERMISSION_DENIED: "The operation does not have permission to access a required resource.",
  OPERATION_BUSY: "Another write operation is using the protected resource.",
  LOCK_UNVERIFIABLE: "The lock owner or protected resource identity cannot be verified.",
  OPERATION_CANCELLED: "The operation was cancelled.",
  CORE_RUNTIME_CRASHED: "The Core runtime stopped unexpectedly.",
  PROTOCOL_VERSION_MISMATCH: "The client and Core protocol versions are incompatible.",
  INTERNAL_ERROR: "An internal error occurred."
});

const WARNING_MESSAGES = Object.freeze({
  warnings: "The operation completed with a warning.",
  encryptedContentWarning: "Existing encrypted content may not be usable with the target provider.",
  autoPruneWarning: "Automatic backup cleanup did not complete.",
  backupInventoryWarning: "Backup inventory refresh did not complete.",
  modelWarning: "The selected provider has no default model; the root model was not changed."
});

function assertPlainJsonData(value, seen = new WeakSet(), depth = 0) {
  if (depth > 16) throw new TypeError("CLI JSON data exceeds the maximum nesting depth.");
  if (value === undefined || value === null
      || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number" && Number.isFinite(value)) return;
  if (typeof value !== "object") throw new TypeError("CLI JSON data must be plain JSON data.");
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype) {
    throw new TypeError("CLI JSON objects must use the plain object prototype.");
  }
  if (seen.has(value)) throw new TypeError("CLI JSON data must not contain cycles.");
  seen.add(value);
  for (const entry of Array.isArray(value) ? value : Object.values(value)) {
    assertPlainJsonData(entry, seen, depth + 1);
  }
  seen.delete(value);
}

function uniqueStrings(values) {
  return [...new Set(values.filter((value) => typeof value === "string" && value))];
}

function safeString(value, maxLength = 4096, { allowEmpty = false } = {}) {
  return typeof value === "string"
      && value.length <= maxLength
      && (allowEmpty || value.length > 0)
    ? value
    : undefined;
}

function safeNumber(value, { integer = false, minimum = Number.NEGATIVE_INFINITY } = {}) {
  return typeof value === "number"
      && Number.isFinite(value)
      && (!integer || Number.isInteger(value))
      && value >= minimum
    ? value
    : undefined;
}

function safeBoolean(value) {
  return typeof value === "boolean" ? value : undefined;
}

function safeUuid(value) {
  return typeof value === "string" && UUID_PATTERN.test(value) ? value : undefined;
}

function put(target, key, value) {
  if (value !== undefined) target[key] = value;
}

function putNullable(target, key, value, normalize) {
  if (value === null) target[key] = null;
  else put(target, key, normalize(value));
}

function sanitizeStringArray(value, maxLength = 32768) {
  if (!Array.isArray(value)) return undefined;
  return value
    .map((entry) => safeString(entry, maxLength, { allowEmpty: true }))
    .filter((entry) => entry !== undefined);
}

function sanitizeNumberArray(value) {
  if (!Array.isArray(value)) return undefined;
  return value
    .map((entry) => safeNumber(entry, { integer: true, minimum: 0 }))
    .filter((entry) => entry !== undefined);
}

function sanitizeCountMap(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const result = {};
  for (const [key, count] of Object.entries(value)) {
    if (!safeString(key, 256) || key.includes("\0")) continue;
    const normalized = safeNumber(count, { integer: true, minimum: 0 });
    if (normalized !== undefined) result[key] = normalized;
  }
  return result;
}

function sanitizeDistribution(value, { includeReadState = false } = {}) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const result = {};
  put(result, "sessions", sanitizeCountMap(value.sessions));
  put(result, "archived_sessions", sanitizeCountMap(value.archived_sessions));
  if (includeReadState) {
    put(result, "unreadable", safeBoolean(value.unreadable));
    if (value.error) result.error = "state_5.sqlite is unavailable.";
  }
  return result;
}

function sanitizePruneResult(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const result = {};
  put(result, "backupRoot", safeString(value.backupRoot, 32768));
  put(result, "deletedCount", safeNumber(value.deletedCount, { integer: true, minimum: 0 }));
  put(result, "remainingCount", safeNumber(value.remainingCount, { integer: true, minimum: 0 }));
  put(result, "freedBytes", safeNumber(value.freedBytes, { integer: true, minimum: 0 }));
  return result;
}

function sanitizeBackupInfo(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const result = {};
  put(result, "backupId", safeString(value.backupId, 256));
  put(result, "backupDir", safeString(value.backupDir, 32768));
  put(result, "createdAt", safeString(value.createdAt, 64));
  put(result, "sizeBytes", safeNumber(value.sizeBytes, { integer: true, minimum: 0 }));
  put(result, "fileCount", safeNumber(value.fileCount, { integer: true, minimum: 0 }));
  return result;
}

function sanitizeModelSync(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const result = {};
  put(result, "applied", safeBoolean(value.applied));
  put(result, "source", safeString(value.source, 64));
  putNullable(result, "model", value.model, (entry) => safeString(entry, 512, { allowEmpty: true }));
  putNullable(
    result,
    "warning",
    value.warning,
    () => WARNING_MESSAGES.modelWarning
  );
  return result;
}

function sanitizeSyncResult(value) {
  const result = {};
  for (const key of ["codexHome", "sqliteHome", "backupDir"]) {
    put(result, key, safeString(value[key], 32768));
  }
  for (const key of ["sqliteHomeSource", "targetProvider", "previousProvider"]) {
    putNullable(result, key, value[key], (entry) => safeString(entry, 512, { allowEmpty: true }));
  }
  for (const key of [
    "backupDurationMs",
    "changedSessionFiles",
    "sqliteRowsUpdated",
    "sqliteProviderRowsUpdated",
    "sqliteUserEventRowsUpdated",
    "sqliteCwdRowsUpdated",
    "updatedWorkspaceRoots",
    "savedWorkspaceRootCount"
  ]) {
    put(result, key, safeNumber(value[key], { integer: true, minimum: 0 }));
  }
  put(result, "sqlitePresent", safeBoolean(value.sqlitePresent));
  put(result, "skippedLockedRolloutFiles", sanitizeStringArray(value.skippedLockedRolloutFiles));
  put(result, "rolloutCountsBefore", sanitizeDistribution(value.rolloutCountsBefore));
  put(result, "encryptedContentCounts", sanitizeDistribution(value.encryptedContentCounts));
  if (value.encryptedContentWarning) {
    result.encryptedContentWarning = WARNING_MESSAGES.encryptedContentWarning;
  } else if (value.encryptedContentWarning === null) {
    result.encryptedContentWarning = null;
  }
  putNullable(result, "autoPruneResult", value.autoPruneResult, sanitizePruneResult);
  if (value.autoPruneWarning) result.autoPruneWarning = WARNING_MESSAGES.autoPruneWarning;
  else if (value.autoPruneWarning === null) result.autoPruneWarning = null;
  put(result, "modelSync", sanitizeModelSync(value.modelSync));
  put(result, "noop", safeBoolean(value.noop));
  put(result, "operationId", safeUuid(value.operationId));
  put(result, "backup", sanitizeBackupInfo(value.backup));
  if (Array.isArray(value.warnings)) {
    result.warnings = value.warnings.length > 0 ? [WARNING_MESSAGES.warnings] : [];
  }
  return result;
}

function sanitizeStatusResult(value) {
  const result = {};
  put(result, "schemaVersion", safeNumber(value.schemaVersion, { integer: true, minimum: 1 }));
  put(result, "snapshotAt", safeString(value.snapshotAt, 64));
  put(result, "storageRevision", safeString(value.storageRevision, 256));
  put(result, "codexHome", safeString(value.codexHome, 32768));
  put(result, "sqliteHome", safeString(value.sqliteHome, 32768));
  put(result, "sqliteHomeSource", SQLITE_HOME_SOURCES.has(value.sqliteHomeSource)
    ? value.sqliteHomeSource
    : undefined);
  if (value.sqliteAccess && typeof value.sqliteAccess === "object") {
    const sqliteAccess = {};
    put(sqliteAccess, "supported", safeBoolean(value.sqliteAccess.supported));
    putNullable(sqliteAccess, "reason", value.sqliteAccess.reason, (entry) => (
      SAFE_REASONS.has(entry) ? entry : undefined
    ));
    result.sqliteAccess = sqliteAccess;
  }
  put(result, "checkedStateDbPaths", sanitizeStringArray(value.checkedStateDbPaths));
  put(result, "currentProvider", safeString(value.currentProvider, 512));
  put(result, "currentProviderImplicit", safeBoolean(value.currentProviderImplicit));
  put(result, "configuredProviders", sanitizeStringArray(value.configuredProviders, 512));
  put(result, "rolloutCounts", sanitizeDistribution(value.rolloutCounts));
  put(result, "lockedRolloutFiles", sanitizeStringArray(value.lockedRolloutFiles));
  put(result, "encryptedContentCounts", sanitizeDistribution(value.encryptedContentCounts));
  if (value.encryptedContentWarning) {
    result.encryptedContentWarning = WARNING_MESSAGES.encryptedContentWarning;
  } else if (value.encryptedContentWarning === null) {
    result.encryptedContentWarning = null;
  }
  putNullable(result, "sqliteCounts", value.sqliteCounts, (entry) => (
    sanitizeDistribution(entry, { includeReadState: true })
  ));
  if (value.stateDbLocation && typeof value.stateDbLocation === "object") {
    const location = {};
    put(location, "path", safeString(value.stateDbLocation.path, 32768));
    put(location, "relativePath", safeString(value.stateDbLocation.relativePath, 32768));
    put(location, "source", safeString(value.stateDbLocation.source, 64));
    result.stateDbLocation = location;
  } else if (value.stateDbLocation === null) {
    result.stateDbLocation = null;
  }
  if (value.sqliteRepairStats && typeof value.sqliteRepairStats === "object") {
    const stats = {};
    put(stats, "userEventRowsNeedingRepair", safeNumber(
      value.sqliteRepairStats.userEventRowsNeedingRepair,
      { integer: true, minimum: 0 }
    ));
    put(stats, "cwdRowsNeedingRepair", safeNumber(
      value.sqliteRepairStats.cwdRowsNeedingRepair,
      { integer: true, minimum: 0 }
    ));
    result.sqliteRepairStats = stats;
  } else if (value.sqliteRepairStats === null) {
    result.sqliteRepairStats = null;
  }
  if (Array.isArray(value.projectThreadVisibility)) {
    result.projectThreadVisibility = value.projectThreadVisibility.map((project) => {
      const item = {};
      if (!project || typeof project !== "object" || Array.isArray(project)) return item;
      put(item, "root", safeString(project.root, 32768));
      for (const key of [
        "interactiveThreads",
        "firstPageThreads",
        "exactCwdMatches",
        "verbatimCwdRows",
        "topRank"
      ]) {
        putNullable(item, key, project[key], (entry) => (
          safeNumber(entry, { integer: true, minimum: 0 })
        ));
      }
      put(item, "ranks", sanitizeNumberArray(project.ranks));
      put(item, "rankPreview", safeString(project.rankPreview, 2048, { allowEmpty: true }));
      put(item, "providerCounts", sanitizeCountMap(project.providerCounts));
      return item;
    });
  }
  put(result, "backupRoot", safeString(value.backupRoot, 32768));
  if (value.backupSummary && typeof value.backupSummary === "object") {
    const summary = {};
    put(summary, "count", safeNumber(value.backupSummary.count, { integer: true, minimum: 0 }));
    put(summary, "totalBytes", safeNumber(value.backupSummary.totalBytes, { integer: true, minimum: 0 }));
    result.backupSummary = summary;
  }
  if (Array.isArray(value.pendingTransactions)) {
    result.pendingTransactions = value.pendingTransactions.map((transaction) => {
      const item = {};
      if (!transaction || typeof transaction !== "object" || Array.isArray(transaction)) return item;
      putNullable(item, "operationId", transaction.operationId, safeUuid);
      put(item, "state", PENDING_STATES.has(transaction.state) ? transaction.state : undefined);
      put(item, "backupDir", safeString(transaction.backupDir, 32768));
      put(item, "journalPath", safeString(transaction.journalPath, 32768));
      return item;
    });
  }
  put(result, "operationInProgress", safeBoolean(value.operationInProgress));
  put(result, "rolloutScanComplete", safeBoolean(value.rolloutScanComplete));
  return result;
}

function sanitizeBooleanMap(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const result = {};
  for (const [key, present] of Object.entries(value)) {
    const normalizedKey = safeString(key, 128);
    const normalizedValue = safeBoolean(present);
    if (normalizedKey && normalizedValue !== undefined) result[normalizedKey] = normalizedValue;
  }
  return result;
}

function sanitizeRestoreResult(value) {
  const result = {};
  put(result, "version", safeNumber(value.version, { integer: true, minimum: 1 }));
  put(result, "namespace", value.namespace === "provider-sync" ? value.namespace : undefined);
  for (const key of ["codexHome", "sqliteHome", "backupDir"]) {
    put(result, key, safeString(value[key], 32768));
  }
  put(result, "targetProvider", safeString(value.targetProvider, 512));
  put(result, "createdAt", safeString(value.createdAt, 64));
  put(result, "dbFiles", sanitizeStringArray(value.dbFiles));
  put(result, "sqliteDbFiles", sanitizeStringArray(value.sqliteDbFiles));
  put(result, "globalStateFiles", sanitizeBooleanMap(value.globalStateFiles));
  put(result, "changedSessionFiles", safeNumber(value.changedSessionFiles, { integer: true, minimum: 0 }));
  put(result, "sizeBytes", safeNumber(value.sizeBytes, { integer: true, minimum: 0 }));
  put(result, "fileCount", safeNumber(value.fileCount, { integer: true, minimum: 0 }));
  if (value.backupInventoryWarning) {
    result.backupInventoryWarning = WARNING_MESSAGES.backupInventoryWarning;
  }
  put(result, "operationId", safeUuid(value.operationId));
  put(result, "backup", sanitizeBackupInfo(value.backup));
  if (Array.isArray(value.warnings)) {
    result.warnings = value.warnings.length > 0 ? [WARNING_MESSAGES.warnings] : [];
  }
  return result;
}

function sanitizeLauncherResult(value) {
  const result = {};
  for (const key of ["targetDir", "cmdPath", "vbsPath"]) {
    put(result, key, safeString(value[key], 32768));
  }
  putNullable(result, "codexHome", value.codexHome, (entry) => safeString(entry, 32768));
  putNullable(result, "sqliteHome", value.sqliteHome, (entry) => safeString(entry, 32768));
  return result;
}

function sanitizeCommandResult(command, value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  if (command === "help") {
    const result = {};
    put(result, "text", safeString(value.text, 65536, { allowEmpty: true }));
    putNullable(result, "requestedCommand", value.requestedCommand, (entry) => safeString(entry, 128));
    return result;
  }
  if (command === "status") return sanitizeStatusResult(value);
  if (command === "sync" || command === "switch") return sanitizeSyncResult(value);
  if (command === "restore") return sanitizeRestoreResult(value);
  if (command === "prune-backups") return sanitizePruneResult(value) ?? {};
  if (command === "install-windows-launcher") return sanitizeLauncherResult(value);
  throw new TypeError(`Unsupported CLI JSON command result: ${String(command)}`);
}

export function collectCliWarnings(result) {
  if (!result || typeof result !== "object") return [];
  return uniqueStrings([
    ...(Array.isArray(result.warnings) && result.warnings.length > 0
      ? [WARNING_MESSAGES.warnings]
      : []),
    result.encryptedContentWarning ? WARNING_MESSAGES.encryptedContentWarning : null,
    result.autoPruneWarning ? WARNING_MESSAGES.autoPruneWarning : null,
    result.backupInventoryWarning ? WARNING_MESSAGES.backupInventoryWarning : null,
    result.modelSync?.warning ? WARNING_MESSAGES.modelWarning : null
  ]);
}

export function inferCliSuccessOutcome(result) {
  if (SUCCESS_OUTCOMES.has(result?.outcome)) return result.outcome;
  if (Array.isArray(result?.skippedLockedRolloutFiles)
      && result.skippedLockedRolloutFiles.length > 0) return "partial";
  if (result?.noop === true) return "noop";
  return "completed";
}

export function createCliSuccessEnvelope(command, result, options = {}) {
  assertPlainJsonData(result);
  const outcome = options.outcome ?? inferCliSuccessOutcome(result);
  if (!SUCCESS_OUTCOMES.has(outcome)) {
    throw new TypeError(`Invalid successful CLI outcome: ${String(outcome)}`);
  }
  const warnings = options.warnings === undefined
    ? collectCliWarnings(result)
    : (Array.isArray(options.warnings) && options.warnings.length > 0
      ? [WARNING_MESSAGES.warnings]
      : []);
  return {
    schemaVersion: CLI_JSON_SCHEMA_VERSION,
    command,
    ok: true,
    outcome,
    result: sanitizeCommandResult(command, result ?? {}),
    warnings,
    error: null
  };
}

function internalErrorDto() {
  return {
    code: "INTERNAL_ERROR",
    message: CLI_ERROR_MESSAGES.INTERNAL_ERROR,
    severity: "fatal",
    retryable: false,
    recoveryRequired: false
  };
}

function canonicalErrorSeverity(code) {
  if (code === "OPERATION_CANCELLED") return "info";
  if (code === "CORE_RUNTIME_CRASHED") return "fatal";
  if (WARNING_ERROR_CODES.has(code)) return "warning";
  return "error";
}

function normalizePublicDetails(details) {
  if (!details || typeof details !== "object" || Array.isArray(details)) return undefined;
  const normalized = {};
  if (LOCK_SCOPES.has(details.busyScope)) normalized.busyScope = details.busyScope;
  if (LOCK_SCOPES.has(details.lockScope)) normalized.lockScope = details.lockScope;
  if (SAFE_CAUSE_CODES.has(details.causeCode)) {
    normalized.causeCode = details.causeCode;
  }
  if (SAFE_REASONS.has(details.reason)) normalized.reason = details.reason;
  if (details.missing === "config.toml" || details.missing === "state_5.sqlite") {
    normalized.missing = details.missing;
  }
  if (SQLITE_HOME_SOURCES.has(details.sqliteHomeSource)) {
    normalized.sqliteHomeSource = details.sqliteHomeSource;
  }
  for (const key of ["sqlitePrimaryCode", "sqliteExtendedCode"]) {
    const code = safeNumber(details[key], { integer: true, minimum: 0 });
    if (code !== undefined && code <= 0xffff) normalized[key] = code;
  }
  if (OPERATION_KINDS.has(details.operationKind)) normalized.operationKind = details.operationKind;
  return Object.keys(normalized).length > 0 ? normalized : undefined;
}

export function normalizeCliErrorDto(dto) {
  try {
    assertPlainJsonData(dto);
    if (!dto || typeof dto !== "object" || Array.isArray(dto)) return internalErrorDto();
    const code = dto.code;
    const message = typeof code === "string" && Object.hasOwn(CLI_ERROR_MESSAGES, code)
      ? CLI_ERROR_MESSAGES[code]
      : undefined;
    if (typeof message !== "string"
        || typeof dto.message !== "string"
        || !dto.message
        || !SEVERITIES.has(dto.severity)
        || typeof dto.retryable !== "boolean"
        || typeof dto.recoveryRequired !== "boolean") {
      return internalErrorDto();
    }
    if (code === "INTERNAL_ERROR") return internalErrorDto();
    const details = normalizePublicDetails(dto.details);
    const operationId = safeUuid(dto.operationId);
    return {
      code,
      message,
      severity: canonicalErrorSeverity(code),
      retryable: true,
      recoveryRequired: RECOVERY_CODES.has(code),
      ...(operationId ? { operationId } : {}),
      ...(details ? { details } : {})
    };
  } catch {
    return internalErrorDto();
  }
}

export function inferCliFailureOutcome(errorDto) {
  const code = errorDto?.code;
  if (code === "OPERATION_CANCELLED") return "cancelled";
  if (RECOVERY_CODES.has(code) || errorDto?.recoveryRequired === true) return "recovery_required";
  if (code === "SYNC_FAILED_ROLLED_BACK") return "failed_rolled_back";
  if (["PLAN_EXPIRED", "PLAN_STALE", "STALE_STATE", "PROFILE_CHANGED", "STORAGE_CHANGED"].includes(code)) {
    return "stale";
  }
  return "failed";
}

export function createCliFailureEnvelope(command, dto) {
  const error = normalizeCliErrorDto(dto);
  const outcome = inferCliFailureOutcome(error);
  if (!FAILURE_OUTCOMES.has(outcome)) throw new TypeError("Invalid CLI failure outcome.");
  return {
    schemaVersion: CLI_JSON_SCHEMA_VERSION,
    command,
    ok: false,
    outcome,
    result: null,
    warnings: [],
    error
  };
}

export function cliJsonExitCode(envelope) {
  if (envelope?.ok) return envelope.outcome === "partial" ? 3 : 0;
  const code = envelope?.error?.code;
  if (code === "OPERATION_CANCELLED") return 130;
  if (RECOVERY_CODES.has(code) || envelope?.error?.recoveryRequired === true) return 4;
  if (BUSY_CODES.has(code)) return 5;
  if (STALE_CODES.has(code)) return 2;
  return 1;
}
