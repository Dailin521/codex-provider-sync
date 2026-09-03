const ERROR_DEFINITIONS = Object.freeze({
  INVALID_INPUT: { severity: "error", retryable: true, recoveryRequired: false },
  PROFILE_CHANGED: { severity: "warning", retryable: true, recoveryRequired: false },
  STORAGE_CHANGED: { severity: "warning", retryable: true, recoveryRequired: false },
  PLAN_STALE: { severity: "warning", retryable: true, recoveryRequired: false },
  PLAN_EXPIRED: { severity: "warning", retryable: true, recoveryRequired: false },
  STALE_STATE: { severity: "warning", retryable: true, recoveryRequired: false },
  FAST_MODE_UNSUPPORTED: { severity: "error", retryable: true, recoveryRequired: false },
  CODEX_HOME_NOT_FOUND: { severity: "error", retryable: true, recoveryRequired: false },
  STATE_DB_NOT_FOUND: { severity: "error", retryable: true, recoveryRequired: false },
  SQLITE_UNSUPPORTED_PATH: { severity: "error", retryable: true, recoveryRequired: false },
  SQLITE_BUSY: { severity: "warning", retryable: true, recoveryRequired: false },
  SQLITE_UNREADABLE: { severity: "error", retryable: true, recoveryRequired: false },
  ROLLOUT_LOCKED: { severity: "warning", retryable: true, recoveryRequired: false },
  ROLLOUT_CHANGED: { severity: "warning", retryable: true, recoveryRequired: false },
  PENDING_TRANSACTION: { severity: "error", retryable: true, recoveryRequired: true },
  BACKUP_FAILED: { severity: "error", retryable: true, recoveryRequired: false },
  SYNC_FAILED_ROLLED_BACK: { severity: "error", retryable: true, recoveryRequired: false },
  RECOVERY_REQUIRED: { severity: "error", retryable: true, recoveryRequired: true },
  RESTORE_VALIDATION_FAILED: { severity: "error", retryable: true, recoveryRequired: false },
  PERMISSION_DENIED: { severity: "error", retryable: true, recoveryRequired: false },
  OPERATION_BUSY: { severity: "warning", retryable: true, recoveryRequired: false },
  LOCK_UNVERIFIABLE: { severity: "error", retryable: true, recoveryRequired: false },
  OPERATION_CANCELLED: { severity: "info", retryable: true, recoveryRequired: false },
  CORE_RUNTIME_CRASHED: { severity: "fatal", retryable: true, recoveryRequired: false },
  PROTOCOL_VERSION_MISMATCH: { severity: "error", retryable: true, recoveryRequired: false },
  INTERNAL_ERROR: { severity: "fatal", retryable: false, recoveryRequired: false }
});

const SEVERITIES = new Set(["info", "warning", "error", "fatal"]);
const LOCK_SCOPES = new Set(["codex-home", "state-db"]);
const CORE_ERROR_CODE_SET = new Set(Object.keys(ERROR_DEFINITIONS));

export const CORE_ERROR_CODES = Object.freeze(Object.keys(ERROR_DEFINITIONS));

function cloneJsonValue(value, fieldName, depth = 0) {
  if (depth > 12) {
    throw new TypeError(`${fieldName} exceeds the maximum supported nesting depth.`);
  }
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TypeError(`${fieldName} may only contain finite numbers.`);
    }
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((entry) => cloneJsonValue(entry, fieldName, depth + 1));
  }
  if (typeof value === "object" && Object.getPrototypeOf(value) === Object.prototype) {
    const result = {};
    for (const [key, entry] of Object.entries(value)) {
      if (entry !== undefined) {
        result[key] = cloneJsonValue(entry, fieldName, depth + 1);
      }
    }
    return result;
  }
  throw new TypeError(`${fieldName} must be JSON-serializable plain data.`);
}

function deepFreezeJsonValue(value) {
  if (value && typeof value === "object") {
    for (const entry of Object.values(value)) {
      deepFreezeJsonValue(entry);
    }
    Object.freeze(value);
  }
  return value;
}

function normalizeDetails(details) {
  if (details === undefined) return undefined;
  const normalized = cloneJsonValue(details, "CoreError details");
  if (normalized === null || Array.isArray(normalized) || typeof normalized !== "object") {
    throw new TypeError("CoreError details must be a plain object.");
  }
  return deepFreezeJsonValue(normalized);
}

function validateScopedError(code, details) {
  if (code === "OPERATION_BUSY" && !LOCK_SCOPES.has(details?.busyScope)) {
    throw new TypeError("OPERATION_BUSY requires details.busyScope to be codex-home or state-db.");
  }
  if (code === "LOCK_UNVERIFIABLE" && !LOCK_SCOPES.has(details?.lockScope)) {
    throw new TypeError("LOCK_UNVERIFIABLE requires details.lockScope to be codex-home or state-db.");
  }
}

function normalizeCode(code) {
  if (!CORE_ERROR_CODE_SET.has(code)) {
    throw new TypeError(`Unknown CoreError code: ${String(code)}`);
  }
  return code;
}

function optionalString(value, fieldName) {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || !value) {
    throw new TypeError(`${fieldName} must be a non-empty string when provided.`);
  }
  return value;
}

export class CoreError extends Error {
  constructor(code, message, options = {}) {
    const normalizedCode = normalizeCode(code);
    if (typeof message !== "string" || !message) {
      throw new TypeError("CoreError message must be a non-empty string.");
    }
    const details = normalizeDetails(options.details);
    validateScopedError(normalizedCode, details);
    super(message, options.cause === undefined ? undefined : { cause: options.cause });

    const defaults = ERROR_DEFINITIONS[normalizedCode];
    const severity = options.severity ?? defaults.severity;
    if (!SEVERITIES.has(severity)) {
      throw new TypeError(`Invalid CoreError severity: ${String(severity)}`);
    }
    if (options.retryable !== undefined && typeof options.retryable !== "boolean") {
      throw new TypeError("CoreError retryable must be boolean when provided.");
    }
    if (options.recoveryRequired !== undefined && typeof options.recoveryRequired !== "boolean") {
      throw new TypeError("CoreError recoveryRequired must be boolean when provided.");
    }

    this.name = "CoreError";
    this.code = normalizedCode;
    this.severity = severity;
    this.retryable = options.retryable ?? defaults.retryable;
    this.recoveryRequired = options.recoveryRequired ?? defaults.recoveryRequired;
    this.operationId = optionalString(options.operationId, "CoreError operationId");
    this.details = details;
    this.suggestedAction = optionalString(options.suggestedAction, "CoreError suggestedAction");
  }

  toDto() {
    return {
      code: this.code,
      message: this.message,
      severity: this.severity,
      retryable: this.retryable,
      recoveryRequired: this.recoveryRequired,
      ...(this.operationId ? { operationId: this.operationId } : {}),
      ...(this.details ? { details: this.details } : {}),
      ...(this.suggestedAction ? { suggestedAction: this.suggestedAction } : {})
    };
  }
}

function safeCauseCode(error) {
  return typeof error?.code === "string" && error.code && error.code.length <= 120
    ? error.code
    : undefined;
}

function mappedCode(error, fallbackCode) {
  if (error?.name === "AbortError" && error?.code === "ABORT_ERR") {
    return "OPERATION_CANCELLED";
  }
  if (CORE_ERROR_CODE_SET.has(error?.code)) {
    return error.code;
  }
  if (error?.code === "EACCES" || error?.code === "EPERM") {
    return "PERMISSION_DENIED";
  }
  return normalizeCode(fallbackCode);
}

export function toCoreErrorDto(error, {
  fallbackCode = "INTERNAL_ERROR",
  operationId,
  details,
  suggestedAction
} = {}) {
  if (error instanceof CoreError) {
    return error.toDto();
  }

  const message = error instanceof Error ? error.message : String(error);
  const causeCode = safeCauseCode(error);
  const normalizedDetails = {
    ...(details ?? {}),
    ...(causeCode && !CORE_ERROR_CODE_SET.has(causeCode) ? { causeCode } : {})
  };
  const code = mappedCode(error, fallbackCode);
  try {
    return new CoreError(code, message || "An unknown Core error occurred.", {
      operationId,
      details: Object.keys(normalizedDetails).length > 0 ? normalizedDetails : undefined,
      suggestedAction,
      cause: error instanceof Error ? error : undefined
    }).toDto();
  } catch (conversionError) {
    if (code === "INTERNAL_ERROR") throw conversionError;
    return new CoreError("INTERNAL_ERROR", message || "An unknown Core error occurred.", {
      details: causeCode ? { causeCode } : undefined,
      cause: error instanceof Error ? error : conversionError
    }).toDto();
  }
}
