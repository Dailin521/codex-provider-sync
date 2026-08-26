import { contextBridge, ipcRenderer } from "electron";

import {
  assertCoreRequestEnvelope,
  createCoreFailureEnvelope,
  createPublicCoreErrorDto,
  type CoreRequestEnvelope,
  type CoreResponseEnvelope
} from "@codex-provider-sync/contracts";
import {
  isDesktopReadMethod,
  type DesktopReadMethod
} from "@codex-provider-sync/core-client";

import type { DesktopBridgeApi } from "../shared/bridge.js";
import {
  DESKTOP_IPC_CHANNELS,
  MAX_DESKTOP_IPC_BYTES
} from "../shared/constants.js";
import type { DesktopProfileListResponse } from "../shared/profile-types.js";

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

const api: DesktopBridgeApi = {
  version: 1,
  core: { requestReadOnly },
  profiles: {
    async list() {
      return validateProfileList(await ipcRenderer.invoke(
        DESKTOP_IPC_CHANNELS.profilesList,
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
