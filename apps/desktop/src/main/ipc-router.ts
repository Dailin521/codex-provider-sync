import type {
  BrowserWindow,
  IpcMain,
  IpcMainInvokeEvent
} from "electron";

import {
  CORE_PROTOCOL_VERSION,
  ContractValidationError,
  assertCoreRequestEnvelope,
  createPublicCoreErrorDto,
  type CoreRequestEnvelope,
  type CoreResponseEnvelope
} from "@codex-provider-sync/contracts";
import {
  isDesktopReadMethod,
  type DesktopReadMethod
} from "@codex-provider-sync/core-client";

import {
  DESKTOP_IPC_CHANNELS,
  MAX_DESKTOP_IPC_BYTES
} from "../shared/constants.js";
import type { DesktopProfileListResponse } from "../shared/profile-types.js";
import type { DesktopProfileRepository } from "../profiles/repository.js";
import type { CoreRuntimeSupervisor } from "./runtime-supervisor.js";

export interface DesktopIpcRouterOptions {
  ipcMain: IpcMain;
  getWindow(): BrowserWindow | null;
  rendererOrigin: string;
  profiles: DesktopProfileRepository;
  supervisor: CoreRuntimeSupervisor;
}

function isTrustedSender(
  event: IpcMainInvokeEvent,
  window: BrowserWindow | null,
  rendererOrigin: string
): boolean {
  if (!window || window.isDestroyed() || event.sender !== window.webContents) return false;
  const frame = event.senderFrame;
  if (!frame || frame !== event.sender.mainFrame) return false;
  try {
    const actual = new URL(frame.url);
    const expected = new URL(rendererOrigin);
    return actual.protocol === expected.protocol
      && actual.hostname === expected.hostname
      && actual.port === expected.port
      && actual.username === ""
      && actual.password === "";
  } catch {
    return false;
  }
}

function correlation(value: unknown): { requestId: string; operationId?: string } {
  const source = value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
  return {
    requestId: typeof source.requestId === "string" && source.requestId.length > 0
      ? source.requestId
      : "invalid-request",
    ...(typeof source.operationId === "string" && source.operationId.length > 0
      ? { operationId: source.operationId }
      : {})
  };
}

function failureEnvelope(
  value: unknown,
  code: "INVALID_INPUT" | "PERMISSION_DENIED" | "PROTOCOL_VERSION_MISMATCH"
): CoreResponseEnvelope {
  const ids = correlation(value);
  return {
    protocolVersion: CORE_PROTOCOL_VERSION,
    requestId: ids.requestId,
    ...(ids.operationId ? { operationId: ids.operationId } : {}),
    ok: false,
    error: createPublicCoreErrorDto(code)
  };
}

function encodedSize(value: unknown): number {
  try {
    return new TextEncoder().encode(JSON.stringify(value)).byteLength;
  } catch {
    return Number.POSITIVE_INFINITY;
  }
}

export function registerDesktopIpc(options: DesktopIpcRouterOptions): () => void {
  const registered: string[] = [];
  const register = (
    channel: string,
    handler: (event: IpcMainInvokeEvent, value: unknown) => unknown | Promise<unknown>
  ) => {
    options.ipcMain.handle(channel, handler);
    registered.push(channel);
  };

  register(DESKTOP_IPC_CHANNELS.coreRead, async (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)) {
      return failureEnvelope(value, "PERMISSION_DENIED");
    }
    if (encodedSize(value) > MAX_DESKTOP_IPC_BYTES) {
      return failureEnvelope(value, "INVALID_INPUT");
    }
    let request: CoreRequestEnvelope;
    try {
      assertCoreRequestEnvelope(value);
      request = value;
    } catch (error) {
      return failureEnvelope(
        value,
        error instanceof ContractValidationError && error.code === "PROTOCOL_VERSION_MISMATCH"
          ? "PROTOCOL_VERSION_MISMATCH"
          : "INVALID_INPUT"
      );
    }
    if (!isDesktopReadMethod(request.method)) {
      return failureEnvelope(request, "PERMISSION_DENIED");
    }
    return options.supervisor.request(
      request as CoreRequestEnvelope<DesktopReadMethod>
    );
  });

  register(DESKTOP_IPC_CHANNELS.profilesList, (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)
        || value !== null) {
      throw new Error("Desktop profile request rejected.");
    }
    const response: DesktopProfileListResponse = {
      schemaVersion: 1,
      profiles: options.profiles.list()
    };
    return response;
  });

  return () => {
    for (const channel of registered) options.ipcMain.removeHandler(channel);
  };
}

export { isTrustedSender };
