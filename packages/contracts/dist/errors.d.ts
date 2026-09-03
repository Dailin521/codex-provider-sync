import type { JsonObject } from "./json.js";
export declare const CORE_ERROR_CODES: readonly ["INVALID_INPUT", "PROFILE_CHANGED", "STORAGE_CHANGED", "PLAN_STALE", "PLAN_EXPIRED", "STALE_STATE", "FAST_MODE_UNSUPPORTED", "CODEX_HOME_NOT_FOUND", "STATE_DB_NOT_FOUND", "SQLITE_UNSUPPORTED_PATH", "SQLITE_BUSY", "SQLITE_UNREADABLE", "ROLLOUT_LOCKED", "ROLLOUT_CHANGED", "PENDING_TRANSACTION", "BACKUP_FAILED", "SYNC_FAILED_ROLLED_BACK", "RECOVERY_REQUIRED", "RESTORE_VALIDATION_FAILED", "PERMISSION_DENIED", "OPERATION_BUSY", "LOCK_UNVERIFIABLE", "OPERATION_CANCELLED", "CORE_RUNTIME_CRASHED", "PROTOCOL_VERSION_MISMATCH", "INTERNAL_ERROR"];
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
export declare const PUBLIC_CORE_ERROR_MESSAGES: Readonly<Record<CoreErrorCode, string>>;
export declare function createPublicCoreErrorDto(code: CoreErrorCode, options?: {
    operationId?: unknown;
    details?: unknown;
}): CoreErrorDto;
export declare function sanitizePublicCoreErrorDto(value: unknown): CoreErrorDto;
export declare function isCanonicalPublicCoreErrorDto(value: unknown): value is CoreErrorDto;
