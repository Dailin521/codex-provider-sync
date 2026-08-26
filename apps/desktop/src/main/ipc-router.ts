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
  type CoreErrorCode,
  type CoreRequestEnvelope,
  type CoreResponseEnvelope,
  type PlanSummary,
  type ProfileSelector
} from "@codex-provider-sync/contracts";
import {
  isDesktopReadMethod,
  isDesktopSyncSwitchMethod,
  type DesktopReadMethod,
  type DesktopSyncSwitchMethod
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

interface PlanOwnership {
  senderId: number;
  applyMethod: "applySync" | "applySwitch";
  profile: ProfileSelector;
  generation: number;
  expiresAt: number;
  state: "prepared" | "applying";
}

interface ActiveRequestOwnership {
  senderId: number;
  planId: string;
}

const MAX_DESKTOP_OWNED_PLANS = 256;

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

function failureEnvelope(value: unknown, code: CoreErrorCode): CoreResponseEnvelope {
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

function requestProfile(request: CoreRequestEnvelope<DesktopSyncSwitchMethod>): ProfileSelector | null {
  if (request.method === "applySync" || request.method === "applySwitch") return null;
  const payload = request.payload as { profile?: ProfileSelector };
  return payload.profile ?? null;
}

function validPlanResult(
  request: CoreRequestEnvelope<DesktopSyncSwitchMethod>,
  value: unknown
): value is PlanSummary {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const result = value as PlanSummary;
  const expectedOperation = request.method === "prepareSync" ? "sync" : "switch";
  const profile = requestProfile(request);
  const expiresAt = Date.parse(result.expiresAt);
  return result.schemaVersion === 1
    && result.operation === expectedOperation
    && typeof result.planId === "string"
    && /^[A-Za-z0-9_-]{32,128}$/.test(result.planId)
    && Number.isFinite(expiresAt)
    && expiresAt > Date.now()
    && expiresAt <= Date.now() + 10 * 60_000 + 5_000
    && Boolean(profile)
    && result.profile.id === profile?.profileId
    && (profile?.profileRevision === undefined || result.profile.revision === profile.profileRevision);
}

function validCancelInput(value: unknown): value is { requestId: string; operationId?: string } {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const source = value as Record<string, unknown>;
  const allowed = source.operationId === undefined
    ? ["requestId"]
    : ["requestId", "operationId"];
  return Object.keys(source).sort().join(",") === allowed.sort().join(",")
    && typeof source.requestId === "string"
    && source.requestId.length > 0
    && source.requestId.length <= 512
    && (source.operationId === undefined
      || (typeof source.operationId === "string"
        && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(source.operationId)));
}

export function registerDesktopIpc(options: DesktopIpcRouterOptions): () => void {
  const registered: string[] = [];
  const plans = new Map<string, PlanOwnership>();
  const activeRequests = new Map<string, ActiveRequestOwnership>();
  const inFlightRequestIds = new Set<string>();
  const pruneExpiredPlans = (): void => {
    const now = Date.now();
    const generation = options.supervisor.snapshot.generation;
    for (const [planId, owner] of plans) {
      if (owner.state === "prepared"
          && (owner.expiresAt <= now || owner.generation !== generation)) {
        plans.delete(planId);
      }
    }
  };
  const makeRoomForPreparedPlan = (): boolean => {
    while (plans.size >= MAX_DESKTOP_OWNED_PLANS) {
      const victim = [...plans].find(([, owner]) => owner.state === "prepared");
      if (!victim) return false;
      plans.delete(victim[0]);
    }
    return true;
  };
  const register = (
    channel: string,
    handler: (event: IpcMainInvokeEvent, value: unknown) => unknown | Promise<unknown>
  ) => {
    options.ipcMain.handle(channel, handler);
    registered.push(channel);
  };

  const unsubscribeOperations = options.supervisor.subscribeOperation((event) => {
    const owner = activeRequests.get(event.requestId);
    const window = options.getWindow();
    if (!owner || !window || window.isDestroyed() || window.webContents.id !== owner.senderId) return;
    window.webContents.send(DESKTOP_IPC_CHANNELS.operationEvent, structuredClone(event));
  });

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
    if (!isDesktopReadMethod(request.method)) return failureEnvelope(request, "PERMISSION_DENIED");
    if (inFlightRequestIds.has(request.requestId)) return failureEnvelope(request, "INVALID_INPUT");
    inFlightRequestIds.add(request.requestId);
    try {
      return await options.supervisor.request(request as CoreRequestEnvelope<DesktopReadMethod>);
    } finally {
      inFlightRequestIds.delete(request.requestId);
    }
  });

  register(DESKTOP_IPC_CHANNELS.coreSyncSwitch, async (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)) {
      return failureEnvelope(value, "PERMISSION_DENIED");
    }
    if (encodedSize(value) > MAX_DESKTOP_IPC_BYTES) return failureEnvelope(value, "INVALID_INPUT");
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
    if (!isDesktopSyncSwitchMethod(request.method) || request.operationId !== undefined) {
      return failureEnvelope(request, "PERMISSION_DENIED");
    }
    const typed = request as CoreRequestEnvelope<DesktopSyncSwitchMethod>;
    if (inFlightRequestIds.has(request.requestId)) return failureEnvelope(request, "INVALID_INPUT");
    inFlightRequestIds.add(request.requestId);
    try {
      pruneExpiredPlans();
      if (typed.method === "prepareSync" || typed.method === "prepareSwitch") {
        const profile = requestProfile(typed);
        if (!profile) return failureEnvelope(request, "INVALID_INPUT");
        const response = await options.supervisor.requestWrite(typed, profile);
        if (!response.ok) return response;
        if (!validPlanResult(typed, response.result)
            || options.supervisor.snapshot.state !== "ready"
            || plans.has(response.result.planId)
            || !makeRoomForPreparedPlan()) {
          return failureEnvelope(request, "INTERNAL_ERROR");
        }
        plans.set(response.result.planId, {
          senderId: event.sender.id,
          applyMethod: typed.method === "prepareSync" ? "applySync" : "applySwitch",
          profile: {
            profileId: response.result.profile.id,
            profileRevision: response.result.profile.revision
          },
          generation: options.supervisor.snapshot.generation,
          expiresAt: Date.parse(response.result.expiresAt),
          state: "prepared"
        });
        return response;
      }

      const planId = (typed.payload as { planId: string }).planId;
      const owner = plans.get(planId);
      if (!owner
          || owner.senderId !== event.sender.id
          || owner.applyMethod !== typed.method
          || owner.state !== "prepared"
          || owner.expiresAt <= Date.now()
          || owner.generation !== options.supervisor.snapshot.generation
          || options.supervisor.snapshot.state !== "ready") {
        plans.delete(planId);
        return failureEnvelope(request, "PLAN_EXPIRED");
      }
      owner.state = "applying";
      activeRequests.set(request.requestId, { senderId: event.sender.id, planId });
      try {
        return await options.supervisor.requestWrite(typed, owner.profile);
      } finally {
        activeRequests.delete(request.requestId);
        plans.delete(planId);
      }
    } finally {
      inFlightRequestIds.delete(request.requestId);
    }
  });

  register(DESKTOP_IPC_CHANNELS.operationCancel, (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)
        || encodedSize(value) > MAX_DESKTOP_IPC_BYTES
        || !validCancelInput(value)) {
      return { accepted: false };
    }
    const owner = activeRequests.get(value.requestId);
    if (!owner || owner.senderId !== event.sender.id) return { accepted: false };
    return { accepted: options.supervisor.cancel(value.requestId, value.operationId) };
  });

  register(DESKTOP_IPC_CHANNELS.profilesList, (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin) || value !== null) {
      throw new Error("Desktop profile request rejected.");
    }
    const response: DesktopProfileListResponse = {
      schemaVersion: 1,
      profiles: options.profiles.list()
    };
    return response;
  });

  return () => {
    unsubscribeOperations();
    plans.clear();
    activeRequests.clear();
    for (const channel of registered) options.ipcMain.removeHandler(channel);
  };
}

export { isTrustedSender };
