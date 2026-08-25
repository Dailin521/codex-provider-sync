import type { JsonObject } from "./json.js";

export const CORE_ERROR_CODES = [
  "INVALID_INPUT",
  "PROFILE_CHANGED",
  "STORAGE_CHANGED",
  "PLAN_STALE",
  "PLAN_EXPIRED",
  "STALE_STATE",
  "CODEX_HOME_NOT_FOUND",
  "STATE_DB_NOT_FOUND",
  "SQLITE_UNSUPPORTED_PATH",
  "SQLITE_BUSY",
  "SQLITE_UNREADABLE",
  "ROLLOUT_LOCKED",
  "ROLLOUT_CHANGED",
  "PENDING_TRANSACTION",
  "BACKUP_FAILED",
  "SYNC_FAILED_ROLLED_BACK",
  "RECOVERY_REQUIRED",
  "RESTORE_VALIDATION_FAILED",
  "PERMISSION_DENIED",
  "OPERATION_BUSY",
  "LOCK_UNVERIFIABLE",
  "OPERATION_CANCELLED",
  "CORE_RUNTIME_CRASHED",
  "PROTOCOL_VERSION_MISMATCH",
  "INTERNAL_ERROR"
] as const;

export type CoreErrorCode = typeof CORE_ERROR_CODES[number];
export type CoreErrorSeverity = "info" | "warning" | "error" | "fatal";

export interface CoreErrorDto {
  code: CoreErrorCode;
  message: string;
  severity: CoreErrorSeverity;
  retryable: boolean;
  recoveryRequired: boolean;
  operationId?: string;
  details?: JsonObject;
}

export const PUBLIC_CORE_ERROR_MESSAGES: Readonly<Record<CoreErrorCode, string>> = Object.freeze({
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

const WARNING_CODES = new Set<CoreErrorCode>([
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
const RECOVERY_CODES = new Set<CoreErrorCode>(["PENDING_TRANSACTION", "RECOVERY_REQUIRED"]);
const LOCK_SCOPES = new Set(["codex-home", "state-db"]);
const SAFE_REASONS = new Set([
  "profile",
  "config",
  "storage",
  "rollout",
  "state-db",
  "windows-wsl-unc"
]);
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
const SQLITE_HOME_SOURCES = new Set(["cli", "config", "env", "default"]);
const OPERATION_KINDS = new Set(["sync", "switch", "restore", "prune-backups", "watch"]);
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const CORE_ERROR_CODE_SET = new Set<string>(CORE_ERROR_CODES);

function objectRecord(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function record(value: unknown): Record<string, unknown> | null {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return null;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null
    ? value as Record<string, unknown>
    : null;
}

function ownValue(source: Record<string, unknown> | null, key: string): unknown {
  if (!source) return undefined;
  const descriptor = Object.getOwnPropertyDescriptor(source, key);
  return descriptor && "value" in descriptor ? descriptor.value : undefined;
}

function publicSeverity(code: CoreErrorCode): CoreErrorSeverity {
  if (code === "OPERATION_CANCELLED") return "info";
  if (code === "CORE_RUNTIME_CRASHED" || code === "INTERNAL_ERROR") return "fatal";
  return WARNING_CODES.has(code) ? "warning" : "error";
}

function sanitizeDetails(value: unknown): JsonObject | undefined {
  const source = record(value);
  if (!source) return undefined;
  const details: JsonObject = {};
  const busyScope = ownValue(source, "busyScope");
  const lockScope = ownValue(source, "lockScope");
  const causeCode = ownValue(source, "causeCode");
  const reason = ownValue(source, "reason");
  const missing = ownValue(source, "missing");
  const sqliteHomeSource = ownValue(source, "sqliteHomeSource");
  const operationKind = ownValue(source, "operationKind");
  if (LOCK_SCOPES.has(String(busyScope))) details.busyScope = String(busyScope);
  if (LOCK_SCOPES.has(String(lockScope))) details.lockScope = String(lockScope);
  if (SAFE_CAUSE_CODES.has(String(causeCode))) details.causeCode = String(causeCode);
  if (SAFE_REASONS.has(String(reason))) details.reason = String(reason);
  if (missing === "config.toml" || missing === "state_5.sqlite") details.missing = missing;
  if (SQLITE_HOME_SOURCES.has(String(sqliteHomeSource))) {
    details.sqliteHomeSource = String(sqliteHomeSource);
  }
  for (const key of ["sqlitePrimaryCode", "sqliteExtendedCode"] as const) {
    const candidate = ownValue(source, key);
    if (Number.isInteger(candidate) && Number(candidate) >= 0 && Number(candidate) <= 0xffff) {
      details[key] = Number(candidate);
    }
  }
  if (OPERATION_KINDS.has(String(operationKind))) details.operationKind = String(operationKind);
  return Object.keys(details).length > 0 ? details : undefined;
}

export function createPublicCoreErrorDto(
  code: CoreErrorCode,
  options: { operationId?: unknown; details?: unknown } = {}
): CoreErrorDto {
  if (!CORE_ERROR_CODE_SET.has(code)) code = "INTERNAL_ERROR";
  if (code === "INTERNAL_ERROR") {
    return {
      code,
      message: PUBLIC_CORE_ERROR_MESSAGES[code],
      severity: "fatal",
      retryable: false,
      recoveryRequired: false
    };
  }
  const details = sanitizeDetails(options.details);
  if ((code === "OPERATION_BUSY" && details?.busyScope === undefined)
      || (code === "LOCK_UNVERIFIABLE" && details?.lockScope === undefined)) {
    return createPublicCoreErrorDto("INTERNAL_ERROR");
  }
  const operationId = typeof options.operationId === "string" && UUID_PATTERN.test(options.operationId)
    ? options.operationId
    : undefined;
  return {
    code,
    message: PUBLIC_CORE_ERROR_MESSAGES[code],
    severity: publicSeverity(code),
    retryable: true,
    recoveryRequired: RECOVERY_CODES.has(code),
    ...(operationId ? { operationId } : {}),
    ...(details ? { details } : {})
  };
}

export function sanitizePublicCoreErrorDto(value: unknown): CoreErrorDto {
  const source = objectRecord(value);
  const candidate = ownValue(source, "code");
  const code = typeof candidate === "string" && CORE_ERROR_CODE_SET.has(candidate)
    ? candidate as CoreErrorCode
    : "INTERNAL_ERROR";
  return createPublicCoreErrorDto(code, {
    operationId: ownValue(source, "operationId"),
    details: ownValue(source, "details")
  });
}

export function isCanonicalPublicCoreErrorDto(value: unknown): value is CoreErrorDto {
  const source = record(value);
  if (!source) return false;
  const code = ownValue(source, "code");
  if (typeof code !== "string" || !CORE_ERROR_CODE_SET.has(code)) return false;
  const expected = sanitizePublicCoreErrorDto(source);
  const sourceKeys = Object.keys(source).sort();
  const expectedKeys = Object.keys(expected).sort();
  if (sourceKeys.length !== expectedKeys.length
      || sourceKeys.some((key, index) => key !== expectedKeys[index])) return false;
  for (const key of expectedKeys) {
    if (key === "details") continue;
    if (ownValue(source, key) !== expected[key as keyof CoreErrorDto]) return false;
  }
  const actualDetails = record(ownValue(source, "details"));
  const expectedDetails = expected.details;
  if (expectedDetails === undefined) return actualDetails === null;
  if (!actualDetails) return false;
  const actualDetailKeys = Object.keys(actualDetails).sort();
  const expectedDetailKeys = Object.keys(expectedDetails).sort();
  return actualDetailKeys.length === expectedDetailKeys.length
    && actualDetailKeys.every((key, index) => (
      key === expectedDetailKeys[index]
      && ownValue(actualDetails, key) === expectedDetails[key]
    ));
}
