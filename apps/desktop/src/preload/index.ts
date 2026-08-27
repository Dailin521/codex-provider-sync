import { contextBridge, ipcRenderer, type IpcRendererEvent } from "electron";

import {
  assertCoreRequestEnvelope,
  assertCoreOperationEventEnvelope,
  createCoreFailureEnvelope,
  createPublicCoreErrorDto,
  type CoreRequestEnvelope,
  type CoreResponseEnvelope,
  type CoreOperationEventEnvelope
} from "@codex-provider-sync/contracts";
import {
  isDesktopMaintenanceMethod,
  isDesktopReadMethod,
  isDesktopRestoreMethod,
  isDesktopSyncSwitchMethod,
  type DesktopCancelOperationInput,
  type DesktopMaintenanceMethod,
  type DesktopReadMethod,
  type DesktopRestoreMethod,
  type DesktopSyncSwitchMethod
} from "@codex-provider-sync/core-client";

import type { DesktopBridgeApi } from "../shared/bridge.js";
import {
  DESKTOP_IPC_CHANNELS,
  MAX_DESKTOP_IPC_BYTES
} from "../shared/constants.js";
import type { DesktopProfileListResponse } from "../shared/profile-types.js";
import type {
  DesktopDiagnosticsExportInput,
  DesktopDiagnosticsExportResult
} from "../shared/diagnostics-types.js";
import type { DesktopUpdateStatus } from "../shared/update-types.js";

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const nested of Object.values(value as Record<string, unknown>)) deepFreeze(nested);
  }
  return value;
}

function requestFailure(
  request: CoreRequestEnvelope,
  code: "INVALID_INPUT" | "PERMISSION_DENIED"
): CoreResponseEnvelope {
  return createCoreFailureEnvelope(request, createPublicCoreErrorDto(code));
}

function validateProfileList(value: unknown): DesktopProfileListResponse {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Invalid desktop profile response.");
  }
  const source = value as Record<string, unknown>;
  if (Object.keys(source).sort().join(",") !== "profiles,schemaVersion"
      || source.schemaVersion !== 1
      || !Array.isArray(source.profiles)) {
    throw new TypeError("Invalid desktop profile response.");
  }
  for (const entry of source.profiles) {
    if (entry === null || typeof entry !== "object" || Array.isArray(entry)) {
      throw new TypeError("Invalid desktop profile response.");
    }
    const profile = entry as Record<string, unknown>;
    if (Object.keys(profile).sort().join(",") !== "codexHomeConfigured,id,name,revision,sqliteHomeConfigured"
        || typeof profile.id !== "string"
        || typeof profile.name !== "string"
        || typeof profile.revision !== "string"
        || typeof profile.codexHomeConfigured !== "boolean"
        || typeof profile.sqliteHomeConfigured !== "boolean") {
      throw new TypeError("Invalid desktop profile response.");
    }
  }
  return structuredClone(value) as DesktopProfileListResponse;
}

async function requestReadOnly<M extends DesktopReadMethod>(
  envelope: CoreRequestEnvelope<M>
): Promise<unknown> {
  try {
    assertCoreRequestEnvelope(envelope);
  } catch {
    return requestFailure(envelope, "INVALID_INPUT");
  }
  if (!isDesktopReadMethod(envelope.method)) {
    return requestFailure(envelope, "PERMISSION_DENIED");
  }
  let size = Number.POSITIVE_INFINITY;
  try {
    size = new TextEncoder().encode(JSON.stringify(envelope)).byteLength;
  } catch {}
  if (size > MAX_DESKTOP_IPC_BYTES) return requestFailure(envelope, "INVALID_INPUT");
  return ipcRenderer.invoke(
    DESKTOP_IPC_CHANNELS.coreRead,
    structuredClone(envelope)
  );
}

async function requestSyncSwitch<M extends DesktopSyncSwitchMethod>(
  envelope: CoreRequestEnvelope<M>
): Promise<unknown> {
  try {
    assertCoreRequestEnvelope(envelope);
  } catch {
    return requestFailure(envelope, "INVALID_INPUT");
  }
  if (!isDesktopSyncSwitchMethod(envelope.method) || envelope.operationId !== undefined) {
    return requestFailure(envelope, "PERMISSION_DENIED");
  }
  let size = Number.POSITIVE_INFINITY;
  try { size = new TextEncoder().encode(JSON.stringify(envelope)).byteLength; } catch {}
  if (size > MAX_DESKTOP_IPC_BYTES) return requestFailure(envelope, "INVALID_INPUT");
  return ipcRenderer.invoke(
    DESKTOP_IPC_CHANNELS.coreSyncSwitch,
    structuredClone(envelope)
  );
}

async function requestRestore<M extends DesktopRestoreMethod>(
  envelope: CoreRequestEnvelope<M>
): Promise<unknown> {
  try {
    assertCoreRequestEnvelope(envelope);
  } catch {
    return requestFailure(envelope, "INVALID_INPUT");
  }
  if (!isDesktopRestoreMethod(envelope.method) || envelope.operationId !== undefined) {
    return requestFailure(envelope, "PERMISSION_DENIED");
  }
  let size = Number.POSITIVE_INFINITY;
  try { size = new TextEncoder().encode(JSON.stringify(envelope)).byteLength; } catch {}
  if (size > MAX_DESKTOP_IPC_BYTES) return requestFailure(envelope, "INVALID_INPUT");
  return ipcRenderer.invoke(
    DESKTOP_IPC_CHANNELS.coreRestore,
    structuredClone(envelope)
  );
}

async function requestMaintenance<M extends DesktopMaintenanceMethod>(
  envelope: CoreRequestEnvelope<M>
): Promise<unknown> {
  try {
    assertCoreRequestEnvelope(envelope);
  } catch {
    return requestFailure(envelope, "INVALID_INPUT");
  }
  if (!isDesktopMaintenanceMethod(envelope.method) || envelope.operationId !== undefined) {
    return requestFailure(envelope, "PERMISSION_DENIED");
  }
  let size = Number.POSITIVE_INFINITY;
  try { size = new TextEncoder().encode(JSON.stringify(envelope)).byteLength; } catch {}
  if (size > MAX_DESKTOP_IPC_BYTES) return requestFailure(envelope, "INVALID_INPUT");
  return ipcRenderer.invoke(
    DESKTOP_IPC_CHANNELS.coreMaintenance,
    structuredClone(envelope)
  );
}

function validProfileSelector(value: unknown): boolean {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const profile = value as Record<string, unknown>;
  const allowed = profile.profileRevision === undefined
    ? ["profileId"]
    : ["profileId", "profileRevision"];
  return Object.keys(profile).sort().join(",") === allowed.sort().join(",")
    && typeof profile.profileId === "string"
    && /^[A-Za-z0-9._-]{1,80}$/.test(profile.profileId)
    && (profile.profileRevision === undefined
      || (typeof profile.profileRevision === "string"
        && profile.profileRevision.length > 0
        && profile.profileRevision.length <= 512));
}

function validateDiagnosticsExportInput(value: unknown): DesktopDiagnosticsExportInput {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Invalid diagnostics export request.");
  }
  const input = value as Record<string, unknown>;
  if (Object.keys(input).sort().join(",") !== "profile,schemaVersion"
      || input.schemaVersion !== 1
      || !validProfileSelector(input.profile)) {
    throw new TypeError("Invalid diagnostics export request.");
  }
  return structuredClone(value) as DesktopDiagnosticsExportInput;
}

function validateDiagnosticsExportResult(value: unknown): DesktopDiagnosticsExportResult {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Invalid diagnostics export response.");
  }
  const result = value as Record<string, unknown>;
  if (result.schemaVersion !== 1) throw new TypeError("Invalid diagnostics export response.");
  if (result.status === "cancelled"
      && Object.keys(result).sort().join(",") === "schemaVersion,status") {
    return structuredClone(value) as DesktopDiagnosticsExportResult;
  }
  if (result.status === "created"
      && Object.keys(result).sort().join(",") === "artifactId,createdAt,schemaVersion,status"
      && typeof result.artifactId === "string"
      && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(result.artifactId)
      && typeof result.createdAt === "string"
      && Number.isFinite(Date.parse(result.createdAt))) {
    return structuredClone(value) as DesktopDiagnosticsExportResult;
  }
  if (result.status === "failed"
      && Object.keys(result).sort().join(",") === "reason,schemaVersion,status"
      && ["runtime-unavailable", "invalid-snapshot", "write-failed"].includes(String(result.reason))) {
    return structuredClone(value) as DesktopDiagnosticsExportResult;
  }
  throw new TypeError("Invalid diagnostics export response.");
}

function validateUpdateStatus(value: unknown): DesktopUpdateStatus {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Invalid update status response.");
  }
  const status = value as Record<string, unknown>;
  const allowedKeys = new Set([
    "schemaVersion",
    "state",
    "installAllowed",
    "reason",
    "version",
    "progressPercent",
    "installBlockedReason"
  ]);
  const states = new Set([
    "disabled",
    "idle",
    "checking",
    "available",
    "downloading",
    "downloaded",
    "not-available",
    "error",
    "installing"
  ]);
  if (Object.keys(status).some((key) => !allowedKeys.has(key))
      || status.schemaVersion !== 2
      || typeof status.installAllowed !== "boolean"
      || !states.has(String(status.state))) {
    throw new TypeError("Invalid update status response.");
  }
  const state = String(status.state);
  const disabledReasons = new Set(["not-packaged", "not-configured", "unsupported-target"]);
  const errorReasons = new Set(["check-failed", "download-failed", "install-failed"]);
  if ((state === "disabled" && !disabledReasons.has(String(status.reason)))
      || (state === "error" && !errorReasons.has(String(status.reason)))
      || (!["disabled", "error"].includes(state) && status.reason !== undefined)) {
    throw new TypeError("Invalid update status response.");
  }
  const versioned = ["available", "downloading", "downloaded", "installing"].includes(state);
  if ((versioned
      && (typeof status.version !== "string"
        || !/^[0-9A-Za-z][0-9A-Za-z.+-]{0,63}$/.test(status.version)))
      || (!versioned && status.version !== undefined)) {
    throw new TypeError("Invalid update status response.");
  }
  if (status.progressPercent !== undefined
      && (!Number.isInteger(status.progressPercent)
        || Number(status.progressPercent) < 0
        || Number(status.progressPercent) > 100
        || !["downloading", "downloaded"].includes(state))) {
    throw new TypeError("Invalid update status response.");
  }
  const blockedReasons = new Set([
    "write-in-progress",
    "watch-active",
    "pending-recovery",
    "recovery-unverified"
  ]);
  if (state === "downloaded") {
    if ((status.installAllowed === true && status.installBlockedReason !== undefined)
        || (status.installAllowed === false
          && !blockedReasons.has(String(status.installBlockedReason)))) {
      throw new TypeError("Invalid update status response.");
    }
  } else if (status.installAllowed !== false || status.installBlockedReason !== undefined) {
    throw new TypeError("Invalid update status response.");
  }
  return structuredClone(value) as DesktopUpdateStatus;
}

function subscribeOperation(
  listener: (event: CoreOperationEventEnvelope) => void
): () => void {
  const receive = (_event: IpcRendererEvent, value: unknown) => {
    try {
      assertCoreOperationEventEnvelope(value);
      listener(structuredClone(value));
    } catch {
      // Main is trusted, but malformed lifecycle data must fail closed at the
      // observer boundary and never influence an in-flight transaction.
    }
  };
  ipcRenderer.on(DESKTOP_IPC_CHANNELS.operationEvent, receive);
  return () => ipcRenderer.removeListener(DESKTOP_IPC_CHANNELS.operationEvent, receive);
}

async function cancelOperation(input: DesktopCancelOperationInput): Promise<{ accepted: boolean }> {
  const allowed = input.operationId === undefined
    ? ["requestId"]
    : ["requestId", "operationId"];
  if (Object.keys(input).sort().join(",") !== allowed.sort().join(",")
      || typeof input.requestId !== "string"
      || input.requestId.length === 0
      || input.requestId.length > 512
      || (input.operationId !== undefined
        && !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(input.operationId))) {
    return { accepted: false };
  }
  const value: unknown = await ipcRenderer.invoke(
    DESKTOP_IPC_CHANNELS.operationCancel,
    structuredClone(input)
  );
  return value !== null
    && typeof value === "object"
    && !Array.isArray(value)
    && Object.keys(value).length === 1
    && typeof (value as { accepted?: unknown }).accepted === "boolean"
    ? { accepted: (value as { accepted: boolean }).accepted }
    : { accepted: false };
}

const api: DesktopBridgeApi = {
  version: 1,
  core: {
    requestReadOnly,
    requestSyncSwitch,
    requestRestore,
    requestMaintenance,
    subscribeOperation,
    cancelOperation
  },
  profiles: {
    async list() {
      return validateProfileList(await ipcRenderer.invoke(
        DESKTOP_IPC_CHANNELS.profilesList,
        null
      ));
    }
  },
  diagnostics: {
    async export(input) {
      const validated = validateDiagnosticsExportInput(input);
      return validateDiagnosticsExportResult(await ipcRenderer.invoke(
        DESKTOP_IPC_CHANNELS.diagnosticsExport,
        validated
      ));
    }
  },
  updates: {
    async getStatus() {
      return validateUpdateStatus(await ipcRenderer.invoke(
        DESKTOP_IPC_CHANNELS.updateStatus,
        null
      ));
    },
    async check() {
      return validateUpdateStatus(await ipcRenderer.invoke(
        DESKTOP_IPC_CHANNELS.updateCheck,
        null
      ));
    },
    async download() {
      return validateUpdateStatus(await ipcRenderer.invoke(
        DESKTOP_IPC_CHANNELS.updateDownload,
        null
      ));
    },
    async install() {
      return validateUpdateStatus(await ipcRenderer.invoke(
        DESKTOP_IPC_CHANNELS.updateInstall,
        null
      ));
    }
  },
  ...(__CPS_DESKTOP_TEST_BUILD__ && process.env.CPS_DESKTOP_E2E === "1" ? {
    test: {
      async crashRuntime() {
        const value: unknown = await ipcRenderer.invoke(
          "cps:v1:test:crash-runtime",
          null
        );
        return value !== null
          && typeof value === "object"
          && !Array.isArray(value)
          && typeof (value as { crashed?: unknown }).crashed === "boolean"
          ? { crashed: (value as { crashed: boolean }).crashed }
          : { crashed: false };
      },
      async requestRaw(envelope) {
        return ipcRenderer.invoke(
          DESKTOP_IPC_CHANNELS.coreRead,
          structuredClone(envelope)
        ) as Promise<CoreResponseEnvelope>;
      }
    }
  } : {})
};

contextBridge.exposeInMainWorld("codexProvider", deepFreeze(api));
