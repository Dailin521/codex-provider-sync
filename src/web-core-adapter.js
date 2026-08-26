import {
  CORE_PROTOCOL_VERSION,
  ContractValidationError,
  assertCoreRequestEnvelope,
  createCoreFailureEnvelope,
  createCoreSuccessEnvelope,
  createPublicCoreErrorDto,
  isCoreErrorCode
} from "../packages/contracts/dist/index.js";
import { createCoreFacade } from "../packages/core/src/index.js";

const CONFLICT_CODES = new Set([
  "PROFILE_CHANGED",
  "STORAGE_CHANGED",
  "PLAN_STALE",
  "PLAN_EXPIRED",
  "STALE_STATE",
  "ROLLOUT_CHANGED",
  "PENDING_TRANSACTION",
  "RECOVERY_REQUIRED",
  "OPERATION_BUSY"
]);

function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function safeString(value) {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function httpStatusForCode(code) {
  if (code === "INVALID_INPUT" || code === "PROTOCOL_VERSION_MISMATCH") return 400;
  if (code === "CODEX_HOME_NOT_FOUND" || code === "STATE_DB_NOT_FOUND") return 404;
  if (code === "PERMISSION_DENIED") return 403;
  if (code === "SQLITE_BUSY" || code === "LOCK_UNVERIFIABLE") return 423;
  if (CONFLICT_CODES.has(code)) return 409;
  if (code === "OPERATION_CANCELLED") return 499;
  return 500;
}

function publicErrorFromException(error, fallbackCode = "INTERNAL_ERROR") {
  const source = isRecord(error) ? error : {};
  const candidate = safeString(source.code);
  const code = isCoreErrorCode(candidate) ? candidate : fallbackCode;
  return createPublicCoreErrorDto(code, {
    operationId: source.operationId,
    details: source.details
  });
}

function correlationFromUnknown(value) {
  const source = isRecord(value) ? value : {};
  return {
    requestId: safeString(source.requestId) ?? "invalid-request",
    operationId: safeString(source.operationId)
  };
}

export function createWebCoreFacade(stateStore) {
  if (!stateStore || typeof stateStore.getProfile !== "function") {
    throw new TypeError("A trusted Web UI state store is required.");
  }
  return createCoreFacade({
    async resolveProfile(selector) {
      const profile = stateStore.getProfile(selector.profileId);
      return {
        id: profile.id,
        revision: profile.revision,
        codexHome: profile.codexHome,
        ...(profile.sqliteHome ? { sqliteHome: profile.sqliteHome } : {})
      };
    }
  });
}

export async function dispatchWebCoreRequest(coreFacade, value) {
  let request;
  try {
    assertCoreRequestEnvelope(value);
    request = value;
  } catch (error) {
    const correlation = correlationFromUnknown(value);
    const dto = publicErrorFromException(
      error,
      error instanceof ContractValidationError && error.code === "PROTOCOL_VERSION_MISMATCH"
        ? "PROTOCOL_VERSION_MISMATCH"
        : "INVALID_INPUT"
    );
    return {
      statusCode: httpStatusForCode(dto.code),
      envelope: {
        protocolVersion: CORE_PROTOCOL_VERSION,
        requestId: correlation.requestId,
        ...(correlation.operationId ? { operationId: correlation.operationId } : {}),
        ok: false,
        error: dto
      },
      activity: { method: null, ok: false, code: dto.code }
    };
  }

  try {
    const handler = coreFacade[request.method];
    if (typeof handler !== "function") {
      throw Object.assign(new Error("Unknown Core method."), { code: "INVALID_INPUT" });
    }
    const result = await handler.call(coreFacade, request.payload);
    const operationId = isRecord(result) ? safeString(result.operationId) : undefined;
    return {
      statusCode: 200,
      envelope: createCoreSuccessEnvelope(request, result, operationId),
      activity: {
        method: request.method,
        ok: true,
        operationId: operationId ?? request.operationId ?? null
      }
    };
  } catch (error) {
    const dto = publicErrorFromException(error);
    return {
      statusCode: httpStatusForCode(dto.code),
      envelope: createCoreFailureEnvelope(request, dto),
      activity: {
        method: request.method,
        ok: false,
        code: dto.code,
        operationId: dto.operationId ?? request.operationId ?? null
      }
    };
  }
}
