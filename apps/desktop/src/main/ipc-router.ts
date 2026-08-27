import type {
  BrowserWindow,
  IpcMain,
  IpcMainInvokeEvent
} from "electron";
import { randomUUID } from "node:crypto";

import {
  CORE_PROTOCOL_VERSION,
  ContractValidationError,
  assertCoreRequestEnvelope,
  createCoreRequestEnvelope,
  createPublicCoreErrorDto,
  type CoreErrorCode,
  type CoreRequestEnvelope,
  type CoreResponseEnvelope,
  type PlanSummary,
  type ProfileSelector,
  type WatchSnapshot,
  type WatchStatusList
} from "@codex-provider-sync/contracts";
import {
  isDesktopMaintenanceMethod,
  isDesktopReadMethod,
  isDesktopRestoreMethod,
  isDesktopSyncSwitchMethod,
  type DesktopMaintenanceMethod,
  type DesktopReadMethod,
  type DesktopRestoreMethod,
  type DesktopSyncSwitchMethod
} from "@codex-provider-sync/core-client";

import {
  DESKTOP_IPC_CHANNELS,
  MAX_DESKTOP_IPC_BYTES
} from "../shared/constants.js";
import type { DesktopProfileListResponse } from "../shared/profile-types.js";
import type { DesktopProfileRepository } from "../profiles/repository.js";
import type { DesktopDiagnosticsExporter } from "./diagnostics-export.js";
import type { CoreRuntimeSupervisor } from "./runtime-supervisor.js";
import type { DesktopUpdateController } from "./updater.js";
import type {
  DesktopDiagnosticsExportInput,
  DesktopDiagnosticsExportResult
} from "../shared/diagnostics-types.js";
import type { DesktopUpdateStatus } from "../shared/update-types.js";

export interface DesktopIpcRouterOptions {
  ipcMain: IpcMain;
  getWindow(): BrowserWindow | null;
  rendererOrigin: string;
  profiles: DesktopProfileRepository;
  supervisor: CoreRuntimeSupervisor;
  diagnosticsExporter: DesktopDiagnosticsExporter;
  selectDiagnosticsTarget(): Promise<string | null>;
  updates: Pick<
    DesktopUpdateController,
    "status" | "restartPending" | "check" | "download" | "install"
  >;
  onActiveWatchCountChanged?(count: number): void;
}

interface PlanOwnership {
  senderId: number;
  applyMethod: "applySync" | "applySwitch" | "applyRestore";
  profile: ProfileSelector;
  generation: number;
  expiresAt: number;
  state: "prepared" | "applying";
}

interface WatchOwnership {
  senderId: number;
  profile: ProfileSelector;
  generation: number;
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

function failureEnvelope(
  value: unknown,
  code: CoreErrorCode,
  details?: unknown
): CoreResponseEnvelope {
  const ids = correlation(value);
  return {
    protocolVersion: CORE_PROTOCOL_VERSION,
    requestId: ids.requestId,
    ...(ids.operationId ? { operationId: ids.operationId } : {}),
    ok: false,
    error: createPublicCoreErrorDto(code, { details })
  };
}

function encodedSize(value: unknown): number {
  try {
    return new TextEncoder().encode(JSON.stringify(value)).byteLength;
  } catch {
    return Number.POSITIVE_INFINITY;
  }
}

function requestProfile(
  request: CoreRequestEnvelope<DesktopSyncSwitchMethod | DesktopRestoreMethod>
): ProfileSelector | null {
  if (request.method === "applySync"
      || request.method === "applySwitch"
      || request.method === "applyRestore") return null;
  const payload = request.payload as { profile?: ProfileSelector };
  return payload.profile ?? null;
}

function validPlanResult(
  request: CoreRequestEnvelope<DesktopSyncSwitchMethod | DesktopRestoreMethod>,
  value: unknown
): value is PlanSummary {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const result = value as PlanSummary;
  const expectedOperation = request.method === "prepareSync"
    ? "sync"
    : request.method === "prepareSwitch"
      ? "switch"
      : "restore";
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

function diagnosticsExportInput(value: unknown): DesktopDiagnosticsExportInput | null {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return null;
  const input = value as Record<string, unknown>;
  if (Object.keys(input).sort().join(",") !== "profile,schemaVersion"
      || input.schemaVersion !== 1
      || input.profile === null
      || typeof input.profile !== "object"
      || Array.isArray(input.profile)) return null;
  const profile = input.profile as Record<string, unknown>;
  const allowed = profile.profileRevision === undefined
    ? ["profileId"]
    : ["profileId", "profileRevision"];
  if (Object.keys(profile).sort().join(",") !== allowed.sort().join(",")
      || typeof profile.profileId !== "string"
      || !/^[A-Za-z0-9._-]{1,80}$/.test(profile.profileId)
      || (profile.profileRevision !== undefined
        && (typeof profile.profileRevision !== "string"
          || profile.profileRevision.length === 0
          || profile.profileRevision.length > 512))) return null;
  return structuredClone(value) as DesktopDiagnosticsExportInput;
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
  const watches = new Map<string, WatchOwnership>();
  const activeRequests = new Map<string, ActiveRequestOwnership>();
  const inFlightRequestIds = new Set<string>();
  const notifyWatchCount = (): void => options.onActiveWatchCountChanged?.(watches.size);
  const sameProfile = (left: ProfileSelector, right: ProfileSelector): boolean => (
    left.profileId === right.profileId
      && (left.profileRevision ?? null) === (right.profileRevision ?? null)
  );
  const reconcileWatchStatus = (
    senderId: number,
    profile: ProfileSelector,
    result: WatchSnapshot | WatchStatusList
  ): void => {
    let changed = false;
    if ("watches" in result) {
      const liveIds = new Set(
        result.watches
          .filter((watch) => watch.status !== "stopped")
          .map((watch) => watch.watchId)
      );
      for (const [ownedWatchId, owner] of watches) {
        if (owner.senderId === senderId
            && sameProfile(owner.profile, profile)
            && !liveIds.has(ownedWatchId)) {
          watches.delete(ownedWatchId);
          changed = true;
        }
      }
    } else if (result.status === "stopped" && watches.delete(result.watchId)) {
      changed = true;
    }
    if (changed) notifyWatchCount();
  };
  const updateBusy = (value: unknown): CoreResponseEnvelope => failureEnvelope(
    value,
    "OPERATION_BUSY",
    { busyScope: "codex-home" }
  );
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
    if (options.updates.restartPending) return updateBusy(value);
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

  register(DESKTOP_IPC_CHANNELS.coreRestore, async (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)) {
      return failureEnvelope(value, "PERMISSION_DENIED");
    }
    if (encodedSize(value) > MAX_DESKTOP_IPC_BYTES) return failureEnvelope(value, "INVALID_INPUT");
    if (options.updates.restartPending) return updateBusy(value);
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
    if (!isDesktopRestoreMethod(request.method) || request.operationId !== undefined) {
      return failureEnvelope(request, "PERMISSION_DENIED");
    }
    const typed = request as CoreRequestEnvelope<DesktopRestoreMethod>;
    if (inFlightRequestIds.has(request.requestId)) return failureEnvelope(request, "INVALID_INPUT");
    inFlightRequestIds.add(request.requestId);
    try {
      pruneExpiredPlans();
      if (typed.method === "prepareRestore") {
        const profile = requestProfile(typed);
        if (!profile) return failureEnvelope(request, "INVALID_INPUT");
        const response = await options.supervisor.requestManaged(typed, profile, {
          allowRecoveryBlocked: true
        });
        if (!response.ok) return response;
        if (!validPlanResult(typed, response.result)
            || options.supervisor.snapshot.state !== "ready"
            || plans.has(response.result.planId)
            || !makeRoomForPreparedPlan()) {
          return failureEnvelope(request, "INTERNAL_ERROR");
        }
        plans.set(response.result.planId, {
          senderId: event.sender.id,
          applyMethod: "applyRestore",
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
          || owner.applyMethod !== "applyRestore"
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
        return await options.supervisor.requestManaged(typed, owner.profile, {
          allowRecoveryBlocked: true
        });
      } finally {
        activeRequests.delete(request.requestId);
        plans.delete(planId);
      }
    } finally {
      inFlightRequestIds.delete(request.requestId);
    }
  });

  register(DESKTOP_IPC_CHANNELS.coreMaintenance, async (event, value) => {
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
    if (!isDesktopMaintenanceMethod(request.method) || request.operationId !== undefined) {
      return failureEnvelope(request, "PERMISSION_DENIED");
    }
    const typed = request as CoreRequestEnvelope<DesktopMaintenanceMethod>;
    if (inFlightRequestIds.has(request.requestId)) return failureEnvelope(request, "INVALID_INPUT");
    inFlightRequestIds.add(request.requestId);
    try {
      const generation = options.supervisor.snapshot.generation;
      let removedStaleWatch = false;
      for (const [watchId, owner] of watches) {
        if (owner.generation !== generation) {
          watches.delete(watchId);
          removedStaleWatch = true;
        }
      }
      if (removedStaleWatch) notifyWatchCount();
      if (typed.method === "pruneBackups" || typed.method === "startWatch") {
        if (options.updates.restartPending) return updateBusy(request);
        const profile = (typed.payload as { profile?: ProfileSelector }).profile;
        if (!profile) return failureEnvelope(request, "INVALID_INPUT");
        const response = await options.supervisor.requestManaged(typed, profile, {
          allowRecoveryBlocked: typed.method === "pruneBackups"
        });
        if (response.ok && typed.method === "startWatch") {
          const watch = response.result as WatchSnapshot;
          watches.set(watch.watchId, {
            senderId: event.sender.id,
            profile,
            generation: options.supervisor.snapshot.generation
          });
          notifyWatchCount();
        }
        return response;
      }
      const watchId = (typed.payload as { watchId?: string }).watchId;
      const owner = watchId ? watches.get(watchId) : undefined;
      if (watchId && (!owner || owner.senderId !== event.sender.id)) {
        return failureEnvelope(request, "INVALID_INPUT");
      }
      const fallback = owner ?? [...watches.values()].find(
        (candidate) => candidate.senderId === event.sender.id
      );
      const defaultProfile = options.profiles.list()[0];
      const profile = fallback?.profile ?? (defaultProfile ? {
        profileId: defaultProfile.id,
        profileRevision: defaultProfile.revision
      } : null);
      if (!profile) return failureEnvelope(request, "INTERNAL_ERROR");
      const response = await options.supervisor.requestManaged(typed, profile, {
        allowRecoveryBlocked: true
      });
      if (response.ok && typed.method === "getWatchStatus") {
        reconcileWatchStatus(
          event.sender.id,
          profile,
          response.result as WatchSnapshot | WatchStatusList
        );
      }
      if (response.ok && typed.method === "stopWatch") {
        if (watchId) {
          watches.delete(watchId);
        } else {
          for (const [ownedWatchId, candidate] of watches) {
            if (candidate.senderId === event.sender.id) watches.delete(ownedWatchId);
          }
        }
        notifyWatchCount();
      }
      return response;
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

  register(DESKTOP_IPC_CHANNELS.diagnosticsExport, async (event, value): Promise<DesktopDiagnosticsExportResult> => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)
        || encodedSize(value) > MAX_DESKTOP_IPC_BYTES) {
      return { schemaVersion: 1, status: "failed", reason: "runtime-unavailable" };
    }
    const input = diagnosticsExportInput(value);
    if (!input) return { schemaVersion: 1, status: "failed", reason: "runtime-unavailable" };
    let target: string | null;
    let token: string;
    try {
      target = await options.selectDiagnosticsTarget();
      if (!target) return { schemaVersion: 1, status: "cancelled" };
      token = options.diagnosticsExporter.authorizeTarget(target);
    } catch {
      return { schemaVersion: 1, status: "failed", reason: "write-failed" };
    }
    const request = createCoreRequestEnvelope(
      "getDiagnostics",
      { profile: input.profile },
      `desktop-diagnostics-${randomUUID()}`
    );
    const response = await options.supervisor.request(request);
    if (!response.ok) {
      return { schemaVersion: 1, status: "failed", reason: "runtime-unavailable" };
    }
    return options.diagnosticsExporter.export(token, response.result);
  });

  register(DESKTOP_IPC_CHANNELS.updateStatus, (event, value): DesktopUpdateStatus => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin) || value !== null) {
      return {
        schemaVersion: 2,
        state: "disabled",
        reason: "not-configured",
        installAllowed: false
      };
    }
    return options.updates.status;
  });

  const updateAction = (
    action: "check" | "download" | "install"
  ) => async (event: IpcMainInvokeEvent, value: unknown): Promise<DesktopUpdateStatus> => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin) || value !== null) {
      return {
        schemaVersion: 2,
        state: "disabled",
        reason: "not-configured",
        installAllowed: false
      };
    }
    return options.updates[action]();
  };
  register(DESKTOP_IPC_CHANNELS.updateCheck, updateAction("check"));
  register(DESKTOP_IPC_CHANNELS.updateDownload, updateAction("download"));
  register(DESKTOP_IPC_CHANNELS.updateInstall, updateAction("install"));

  return () => {
    unsubscribeOperations();
    plans.clear();
    watches.clear();
    notifyWatchCount();
    activeRequests.clear();
    for (const channel of registered) options.ipcMain.removeHandler(channel);
  };
}

export { isTrustedSender };
