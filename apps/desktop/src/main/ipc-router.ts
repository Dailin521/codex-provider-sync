import type {
  BrowserWindow,
  IpcMain,
  IpcMainInvokeEvent
} from "electron";
import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

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
import type {
  DesktopProfileDeleteInput,
  DesktopProfileDirectoryKind,
  DesktopProfileRevealInput,
  DesktopProfileSaveInput
} from "../shared/profile-types.js";
import type { DesktopProfileRepository } from "../profiles/repository.js";
import type { DirectorySelectionService } from "./directory-selection-service.js";
import type { DesktopDiagnosticsExporter } from "./diagnostics-export.js";
import type { OperationLogService } from "./operation-log-service.js";
import type { CoreRuntimeSupervisor } from "./runtime-supervisor.js";
import type { DesktopUpdateController } from "./updater.js";
import type {
  DesktopDiagnosticsExportInput,
  DesktopDiagnosticsExportResult
} from "../shared/diagnostics-types.js";
import type { DesktopUpdateStatus } from "../shared/update-types.js";
import { validateUpdateReminderInput } from "../shared/update-preferences.js";
import { validateDesktopHistoryRevealInput } from "../shared/history-reveal.js";
import { validateDesktopClipboardInput } from "../shared/clipboard.js";
import type { RuntimeWatchStopped } from "../shared/runtime-protocol.js";

export interface DesktopIpcRouterOptions {
  ipcMain: IpcMain;
  getWindow(): BrowserWindow | null;
  rendererOrigin: string;
  profiles: DesktopProfileRepository;
  directorySelections: DirectorySelectionService;
  operationLogs: OperationLogService;
  selectProfileDirectory(kind: DesktopProfileDirectoryKind): Promise<string | null>;
  revealProfileDirectory(path: string): Promise<boolean>;
  revealHistoryFile?(path: string): Promise<boolean>;
  writeClipboardText?(text: string): void;
  isProfileMutationBlocked(): boolean;
  supervisor: CoreRuntimeSupervisor;
  diagnosticsExporter: DesktopDiagnosticsExporter;
  selectDiagnosticsTarget(): Promise<string | null>;
  updates: Pick<
    DesktopUpdateController,
    "status" | "restartPending" | "check" | "download" | "install" | "setReminder"
  >;
  onActiveWatchCountChanged?(count: number): void;
}

export type DesktopWatchRestartVerification = "clear" | "active" | "unverifiable";

export interface DesktopIpcRegistration {
  (): void;
  verifyNoActiveWatchesForRestart(): Promise<DesktopWatchRestartVerification>;
}

interface PlanOwnership {
  senderId: number;
  applyMethod: "applySync" | "applySwitch" | "applyRepair" | "applyRestore";
  profile: ProfileSelector;
  generation: number;
  expiresAt: number;
  state: "prepared" | "applying";
  logId: string;
}

interface WatchOwnership {
  senderId: number;
  profile: ProfileSelector;
  generation: number;
}

interface ActiveRequestOwnership {
  senderId: number;
  planId?: string;
  logId: string;
}

const MAX_DESKTOP_OWNED_PLANS = 256;
const MAX_DESKTOP_TERMINAL_WATCHES = 256;

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

function isWithinRoot(root: string, candidate: string): boolean {
  const relative = path.relative(root, candidate);
  return relative === "" || (!path.isAbsolute(relative)
    && relative !== ".."
    && !relative.startsWith(`..${path.sep}`));
}

async function isRevealableHistoryRollout(codexHome: string, rolloutPath: string): Promise<boolean> {
  if (!path.isAbsolute(rolloutPath)) return false;
  try {
    const lexicalHome = path.resolve(codexHome);
    const homeStat = await fs.lstat(lexicalHome);
    if (homeStat.isSymbolicLink() || !homeStat.isDirectory()) return false;
    const physicalHome = path.resolve(await fs.realpath(lexicalHome));
    const candidate = path.resolve(rolloutPath);
    for (const directory of ["sessions", "archived_sessions"]) {
      const lexicalRoot = path.join(lexicalHome, directory);
      if (!isWithinRoot(lexicalRoot, candidate)) continue;
      const rootStat = await fs.lstat(lexicalRoot);
      if (rootStat.isSymbolicLink() || !rootStat.isDirectory()) continue;
      const physicalRoot = path.resolve(await fs.realpath(lexicalRoot));
      if (!isWithinRoot(physicalHome, physicalRoot)) continue;
      const candidateStat = await fs.lstat(candidate);
      if (candidateStat.isSymbolicLink() || !candidateStat.isFile()) return false;
      const physicalCandidate = path.resolve(await fs.realpath(candidate));
      return isWithinRoot(physicalRoot, physicalCandidate);
    }
  } catch {
    return false;
  }
  return false;
}

function historyRevealRolloutPath(response: CoreResponseEnvelope, sessionId: string): string | null {
  if (!response.ok || !response.result || typeof response.result !== "object" || Array.isArray(response.result)) return null;
  const detail = response.result as Record<string, unknown>;
  const session = detail.session;
  const storage = detail.storage;
  if (!session || typeof session !== "object" || Array.isArray(session)
      || (session as { id?: unknown }).id !== sessionId
      || !Array.isArray(detail.messages) || detail.messages.length !== 0
      || detail.truncated !== false || detail.returnedMessageCount !== 0
      || !storage || typeof storage !== "object" || Array.isArray(storage)
      || typeof (storage as { rolloutPath?: unknown }).rolloutPath !== "string") return null;
  return (storage as { rolloutPath: string }).rolloutPath;
}

function requestProfile(
  request: CoreRequestEnvelope<DesktopSyncSwitchMethod | DesktopRestoreMethod>
): ProfileSelector | null {
  if (request.method === "applySync"
      || request.method === "applySwitch"
      || request.method === "applyRepair"
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
      : request.method === "prepareRepair"
        ? "repair"
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

function profileSaveInput(value: unknown): DesktopProfileSaveInput | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const input = value as Record<string, unknown>;
  const allowed = new Set(["schemaVersion", "name", "profileId", "profileRevision", "codexHomeSelectionToken", "sqliteHomeMode", "sqliteHomeSelectionToken"]);
  if (Object.keys(input).some((key) => !allowed.has(key)) || input.schemaVersion !== 1
      || typeof input.name !== "string" || input.name.trim().length < 1 || input.name.trim().length > 120
      || (input.profileId !== undefined && (typeof input.profileId !== "string" || !/^[A-Za-z0-9._-]{1,80}$/.test(input.profileId)))
      || (input.profileRevision !== undefined && (typeof input.profileRevision !== "string" || input.profileRevision.length < 1 || input.profileRevision.length > 512))
      || (input.codexHomeSelectionToken !== undefined && (typeof input.codexHomeSelectionToken !== "string" || !/^[A-Za-z0-9_-]{32,128}$/.test(input.codexHomeSelectionToken)))
      || !["preserve", "inherit", "selected"].includes(String(input.sqliteHomeMode))
      || (input.sqliteHomeSelectionToken !== undefined && (typeof input.sqliteHomeSelectionToken !== "string" || !/^[A-Za-z0-9_-]{32,128}$/.test(input.sqliteHomeSelectionToken)))
      || (input.sqliteHomeMode === "selected") !== Boolean(input.sqliteHomeSelectionToken)) return null;
  return structuredClone(value) as DesktopProfileSaveInput;
}

function profileDeleteInput(value: unknown): DesktopProfileDeleteInput | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const input = value as Record<string, unknown>;
  if (Object.keys(input).sort().join(",") !== "profileId,profileRevision,schemaVersion"
      || input.schemaVersion !== 1 || typeof input.profileId !== "string"
      || !/^[A-Za-z0-9._-]{1,80}$/.test(input.profileId)
      || typeof input.profileRevision !== "string" || input.profileRevision.length < 1 || input.profileRevision.length > 512) return null;
  return structuredClone(value) as DesktopProfileDeleteInput;
}

function profileRevealInput(value: unknown): DesktopProfileRevealInput | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const input = value as Record<string, unknown>;
  const base = profileDeleteInput({ schemaVersion: input.schemaVersion, profileId: input.profileId, profileRevision: input.profileRevision });
  if (!base || Object.keys(input).sort().join(",") !== "profileId,profileRevision,schemaVersion,target"
      || (input.target !== "codex-home" && input.target !== "sqlite-home")) return null;
  return structuredClone(value) as DesktopProfileRevealInput;
}

function logListInput(value: unknown): import("../shared/operation-log-types.js").OperationLogListInput | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const input = value as Record<string, unknown>;
  const allowed = new Set(["schemaVersion", "page", "pageSize", "profileId", "profileRevision", "operation", "status"]);
  if (Object.keys(input).some((key) => !allowed.has(key)) || input.schemaVersion !== 1
      || !Number.isInteger(input.page) || Number(input.page) < 1
      || !Number.isInteger(input.pageSize) || Number(input.pageSize) < 1 || Number(input.pageSize) > 100
      || (input.profileId !== undefined && typeof input.profileId !== "string")
      || (input.profileRevision !== undefined && (typeof input.profileRevision !== "string" || input.profileRevision.length === 0 || input.profileRevision.length > 512))
      || (input.operation !== undefined && typeof input.operation !== "string")
      || (input.status !== undefined && !["running", "awaiting-confirmation", "completed", "partial", "failed", "cancelled", "dismissed", "interrupted"].includes(String(input.status)))) return null;
  return structuredClone(value) as import("../shared/operation-log-types.js").OperationLogListInput;
}

export function registerDesktopIpc(options: DesktopIpcRouterOptions): DesktopIpcRegistration {
  const registered: string[] = [];
  const plans = new Map<string, PlanOwnership>();
  const watches = new Map<string, WatchOwnership>();
  const terminalWatches = new Map<string, RuntimeWatchStopped & { generation: number }>();
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
  const removeStaleWatchOwnership = (): boolean => {
    const generation = options.supervisor.snapshot.generation;
    let changed = false;
    for (const [watchId, owner] of watches) {
      if (owner.generation !== generation) {
        watches.delete(watchId);
        changed = true;
      }
    }
    for (const [watchId, terminal] of terminalWatches) {
      if (terminal.generation !== generation) terminalWatches.delete(watchId);
    }
    if (changed) notifyWatchCount();
    return changed;
  };
  const rememberTerminalWatch = (terminal: RuntimeWatchStopped & { generation: number }): void => {
    while (terminalWatches.size >= MAX_DESKTOP_TERMINAL_WATCHES) {
      const first = terminalWatches.keys().next().value as string | undefined;
      if (!first) break;
      terminalWatches.delete(first);
    }
    terminalWatches.set(terminal.watch.watchId, terminal);
  };
  const sendWatchStopped = (
    senderId: number,
    terminal: RuntimeWatchStopped & { generation: number }
  ): void => {
    const window = options.getWindow();
    if (!window || window.isDestroyed() || window.webContents.id !== senderId) return;
    window.webContents.send(DESKTOP_IPC_CHANNELS.watchStoppedEvent, structuredClone(terminal));
  };
  const verifyNoActiveWatchesForRestart = async (): Promise<DesktopWatchRestartVerification> => {
    removeStaleWatchOwnership();
    const first = watches.values().next().value as WatchOwnership | undefined;
    if (!first) return "clear";
    const generation = options.supervisor.snapshot.generation;
    const request = createCoreRequestEnvelope(
      "getWatchStatus",
      {},
      `desktop-update-watch-${randomUUID()}`
    );
    try {
      const response = await options.supervisor.requestManaged(request, first.profile, {
        allowRecoveryBlocked: true
      });
      if (!response.ok || options.supervisor.snapshot.generation !== generation) {
        return "unverifiable";
      }
      const result = response.result as WatchSnapshot | WatchStatusList;
      if (!("watches" in result)) return "unverifiable";
      if (result.watches.some((watch) => watch.status !== "stopped")) return "active";
      if (watches.size > 0) {
        watches.clear();
        notifyWatchCount();
      }
      return "clear";
    } catch {
      return "unverifiable";
    }
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
        void options.operationLogs.finish(owner.logId, {
          status: "failed",
          outcome: "stale",
          errorCode: "PLAN_EXPIRED"
        });
      }
    }
  };
  const makeRoomForPreparedPlan = (): boolean => {
    while (plans.size >= MAX_DESKTOP_OWNED_PLANS) {
      const victim = [...plans].find(([, owner]) => owner.state === "prepared");
      if (!victim) return false;
      plans.delete(victim[0]);
      void options.operationLogs.finish(victim[1].logId, {
        status: "interrupted",
        outcome: "interrupted",
        errorCode: "PLAN_EXPIRED"
      });
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
    if (owner) {
      if (event.event === "operation-started") {
        void options.operationLogs.bindOperation(owner.logId, event.operationId);
      } else if (event.event === "request-progress") {
        void options.operationLogs.requestProgress(owner.logId, event.progress);
      } else {
        void options.operationLogs.progress(owner.logId, event.progress);
      }
    }
    const window = options.getWindow();
    if (!owner || !window || window.isDestroyed() || window.webContents.id !== owner.senderId) return;
    window.webContents.send(DESKTOP_IPC_CHANNELS.operationEvent, structuredClone(event));
  });
  const unsubscribeWatchStopped = options.supervisor.subscribeWatchStopped((terminal) => {
      if (terminal.generation !== options.supervisor.snapshot.generation) return;
      const watchId = terminal.watch.watchId;
      const previous = terminalWatches.get(watchId);
      if (previous?.generation === terminal.generation) return;
      rememberTerminalWatch(terminal);
      const owner = watches.get(watchId);
      if (!owner || owner.generation !== terminal.generation) return;
      watches.delete(watchId);
      notifyWatchCount();
      sendWatchStopped(owner.senderId, terminal);
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
    const diagnosticsProfile = request.method === "getDiagnostics"
      ? (request.payload as { profile?: ProfileSelector }).profile ?? null
      : null;
    const logId = diagnosticsProfile
      ? await options.operationLogs.begin({ operation: "diagnostics", profileId: diagnosticsProfile.profileId, profileRevision: diagnosticsProfile.profileRevision, requestId: request.requestId, stage: "scan" })
      : null;
    try {
      if (logId) activeRequests.set(request.requestId, { senderId: event.sender.id, logId });
      const response = await options.supervisor.request(request as CoreRequestEnvelope<DesktopReadMethod>);
      if (logId) await options.operationLogs.finishFromResponse(logId, response);
      return response;
    } catch (error) {
      if (logId) await options.operationLogs.finish(logId, { status: "failed", outcome: "failed", errorCode: "CORE_RUNTIME_CRASHED" });
      throw error;
    } finally {
      activeRequests.delete(request.requestId);
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
      if (typed.method === "prepareSync"
          || typed.method === "prepareSwitch"
          || typed.method === "prepareRepair") {
        const profile = requestProfile(typed);
        if (!profile) return failureEnvelope(request, "INVALID_INPUT");
        const operation = typed.method === "prepareSync" ? "sync" : typed.method === "prepareSwitch" ? "switch" : "repair";
        const logId = await options.operationLogs.begin({ operation, profileId: profile.profileId, profileRevision: profile.profileRevision, requestId: request.requestId });
        if (typed.method === "prepareRepair") {
          activeRequests.set(request.requestId, { senderId: event.sender.id, logId });
        }
        let response;
        try {
          response = await options.supervisor.requestWrite(typed, profile);
        } finally {
          activeRequests.delete(request.requestId);
        }
        if (!response.ok) {
          await options.operationLogs.finishFromResponse(logId, response);
          return response;
        }
        if (!validPlanResult(typed, response.result)
            || options.supervisor.snapshot.state !== "ready"
            || plans.has(response.result.planId)
            || !makeRoomForPreparedPlan()) {
          await options.operationLogs.finish(logId, { status: "failed", outcome: "failed", errorCode: "INTERNAL_ERROR" });
          return failureEnvelope(request, "INTERNAL_ERROR");
        }
        await options.operationLogs.prepared(logId, response.result.planId, {
          target: {
            provider: response.result.target.provider,
            model: response.result.target.model,
            modelMode: response.result.target.modelMode,
            previousProvider: response.result.target.previousProvider,
            previousRootModel: response.result.target.previousRootModel
          },
          impact: {
            rolloutFilesToChange: response.result.impact.rolloutFilesToChange,
            sqliteRowsToChange: response.result.impact.sqliteRowsToChange,
            lockedRolloutFiles: response.result.impact.lockedRolloutFiles
          }
        });
        plans.set(response.result.planId, {
          senderId: event.sender.id,
          applyMethod: typed.method === "prepareSync"
            ? "applySync"
            : typed.method === "prepareSwitch"
              ? "applySwitch"
              : "applyRepair",
          profile: {
            profileId: response.result.profile.id,
            profileRevision: response.result.profile.revision
          },
          generation: options.supervisor.snapshot.generation,
          expiresAt: Date.parse(response.result.expiresAt),
          state: "prepared",
          logId
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
        if (owner) await options.operationLogs.finish(owner.logId, { status: "failed", outcome: "stale", errorCode: "PLAN_EXPIRED" });
        return failureEnvelope(request, "PLAN_EXPIRED");
      }
      owner.state = "applying";
      await options.operationLogs.resume(owner.logId, request.requestId);
      activeRequests.set(request.requestId, { senderId: event.sender.id, planId, logId: owner.logId });
      try {
        const response = await options.supervisor.requestWrite(typed, owner.profile);
        await options.operationLogs.finishFromResponse(owner.logId, response);
        return response;
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
        const logId = await options.operationLogs.begin({ operation: "restore", profileId: profile.profileId, profileRevision: profile.profileRevision, requestId: request.requestId });
        const response = await options.supervisor.requestManaged(typed, profile, {
          allowRecoveryBlocked: true
        });
        if (!response.ok) {
          await options.operationLogs.finishFromResponse(logId, response);
          return response;
        }
        if (!validPlanResult(typed, response.result)
            || options.supervisor.snapshot.state !== "ready"
            || plans.has(response.result.planId)
            || !makeRoomForPreparedPlan()) {
          await options.operationLogs.finish(logId, { status: "failed", outcome: "failed", errorCode: "INTERNAL_ERROR" });
          return failureEnvelope(request, "INTERNAL_ERROR");
        }
        await options.operationLogs.prepared(logId, response.result.planId, {
          target: { provider: response.result.target.provider },
          impact: {
            rolloutFilesToChange: response.result.impact.rolloutFilesToChange,
            sqliteRowsToChange: response.result.impact.sqliteRowsToChange,
            lockedRolloutFiles: response.result.impact.lockedRolloutFiles
          }
        });
        plans.set(response.result.planId, {
          senderId: event.sender.id,
          applyMethod: "applyRestore",
          profile: {
            profileId: response.result.profile.id,
            profileRevision: response.result.profile.revision
          },
          generation: options.supervisor.snapshot.generation,
          expiresAt: Date.parse(response.result.expiresAt),
          state: "prepared",
          logId
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
        if (owner) await options.operationLogs.finish(owner.logId, { status: "failed", outcome: "stale", errorCode: "PLAN_EXPIRED" });
        return failureEnvelope(request, "PLAN_EXPIRED");
      }
      owner.state = "applying";
      await options.operationLogs.resume(owner.logId, request.requestId);
      activeRequests.set(request.requestId, { senderId: event.sender.id, planId, logId: owner.logId });
      try {
        const response = await options.supervisor.requestManaged(typed, owner.profile, {
          allowRecoveryBlocked: true
        });
        await options.operationLogs.finishFromResponse(owner.logId, response);
        return response;
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
      removeStaleWatchOwnership();
      if (typed.method === "pruneBackups" || typed.method === "startWatch") {
        if (options.updates.restartPending) return updateBusy(request);
        const profile = (typed.payload as { profile?: ProfileSelector }).profile;
        if (!profile) return failureEnvelope(request, "INVALID_INPUT");
        const logId = await options.operationLogs.begin({
          operation: typed.method === "pruneBackups" ? "pruneBackups" : "watch",
          profileId: profile.profileId,
          profileRevision: profile.profileRevision,
          requestId: request.requestId,
          stage: typed.method === "pruneBackups" ? "prune" : "start"
        });
        const response = await options.supervisor.requestManaged(typed, profile, {
          allowRecoveryBlocked: typed.method === "pruneBackups"
        });
        await options.operationLogs.finishFromResponse(logId, response);
        if (response.ok && typed.method === "startWatch") {
          const watch = response.result as WatchSnapshot;
          const terminal = terminalWatches.get(watch.watchId);
          if (terminal?.generation === options.supervisor.snapshot.generation) {
            // A terminal event can beat this response. Do not resurrect Main
            // ownership; deliver the already-validated snapshot to settle UI.
            sendWatchStopped(event.sender.id, terminal);
            return response;
          }
          const existingOwner = watches.get(watch.watchId);
          // A shared physical watcher keeps its first starter's options and log ownership.
          if (!existingOwner || existingOwner.generation !== options.supervisor.snapshot.generation) {
            watches.set(watch.watchId, {
              senderId: event.sender.id,
              profile,
              generation: options.supervisor.snapshot.generation
            });
          }
          notifyWatchCount();
        }
        return response;
      }
      const watchId = (typed.payload as { watchId?: string }).watchId;
      const requestedProfile = typed.method === "getWatchStatus"
        ? (typed.payload as { profile?: ProfileSelector }).profile
        : undefined;
      const owner = watchId ? watches.get(watchId) : undefined;
      if (watchId && (!owner || owner.senderId !== event.sender.id)) {
        return failureEnvelope(request, "INVALID_INPUT");
      }
      const fallback = owner ?? [...watches.values()].find(
        (candidate) => candidate.senderId === event.sender.id
      );
      const defaultProfile = options.profiles.list()[0];
      const profile = requestedProfile ?? fallback?.profile ?? (defaultProfile ? {
        profileId: defaultProfile.id,
        profileRevision: defaultProfile.revision
      } : null);
      if (!profile) return failureEnvelope(request, "INTERNAL_ERROR");
      const logId = typed.method === "stopWatch"
        ? await options.operationLogs.begin({ operation: "watch", profileId: profile.profileId, profileRevision: profile.profileRevision, requestId: request.requestId, stage: "stop" })
        : null;
      const response = await options.supervisor.requestManaged(typed, profile, {
        allowRecoveryBlocked: true
      });
      if (logId) await options.operationLogs.finishFromResponse(logId, response);
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

  register(DESKTOP_IPC_CHANNELS.profilesSelectDirectory, async (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)
        || (value !== "codex-home" && value !== "sqlite-home")
        || options.isProfileMutationBlocked()) {
      throw new Error("Desktop directory selection rejected.");
    }
    const selected = await options.selectProfileDirectory(value);
    if (!selected) return { schemaVersion: 1, status: "cancelled" };
    const authorized = await options.directorySelections.authorize(value, selected);
    return { schemaVersion: 1, status: "selected", ...authorized };
  });

  register(DESKTOP_IPC_CHANNELS.profilesSave, async (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)
        || encodedSize(value) > MAX_DESKTOP_IPC_BYTES || options.isProfileMutationBlocked()) {
      throw new Error("Desktop profile save rejected.");
    }
    const input = profileSaveInput(value);
    if (!input || input.profileId === "default") throw new Error("Invalid desktop profile save request.");
    const logId = await options.operationLogs.begin({ operation: "profile", profileId: input.profileId, stage: input.profileId ? "update" : "create" });
    try {
      const codexHome = input.codexHomeSelectionToken
        ? options.directorySelections.consume(input.codexHomeSelectionToken, "codex-home")
        : undefined;
      const sqliteHome = input.sqliteHomeMode === "selected"
        ? options.directorySelections.consume(input.sqliteHomeSelectionToken!, "sqlite-home")
        : input.sqliteHomeMode === "inherit" ? null : undefined;
      const saved = await options.profiles.save({
        name: input.name,
        ...(input.profileId ? { profileId: input.profileId } : {}),
        ...(input.profileRevision ? { profileRevision: input.profileRevision } : {}),
        ...(codexHome ? { codexHome } : {}),
        ...(sqliteHome !== undefined ? { sqliteHome } : {})
      });
      await options.operationLogs.finish(logId, { status: "completed", outcome: "completed", profileId: saved.id });
      return saved;
    } catch (error) {
      await options.operationLogs.finish(logId, {
        status: "failed",
        outcome: "failed",
        errorCode: typeof (error as { code?: unknown }).code === "string" ? (error as { code: string }).code : "INVALID_INPUT"
      });
      throw error;
    }
  });

  register(DESKTOP_IPC_CHANNELS.profilesDelete, async (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)
        || encodedSize(value) > MAX_DESKTOP_IPC_BYTES || options.isProfileMutationBlocked()) {
      throw new Error("Desktop profile delete rejected.");
    }
    const input = profileDeleteInput(value);
    if (!input || input.profileId === "default") throw new Error("Invalid desktop profile delete request.");
    const logId = await options.operationLogs.begin({ operation: "profile", profileId: input.profileId, stage: "delete" });
    try {
      await options.profiles.delete(input.profileId, input.profileRevision);
      await options.operationLogs.finish(logId, { status: "completed", outcome: "completed" });
      return { deleted: true };
    } catch (error) {
      await options.operationLogs.finish(logId, { status: "failed", outcome: "failed", errorCode: typeof (error as { code?: unknown }).code === "string" ? (error as { code: string }).code : "INVALID_INPUT" });
      throw error;
    }
  });

  register(DESKTOP_IPC_CHANNELS.profilesReveal, async (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)
        || encodedSize(value) > MAX_DESKTOP_IPC_BYTES) throw new Error("Desktop profile reveal rejected.");
    const input = profileRevealInput(value);
    if (!input) throw new Error("Invalid desktop profile reveal request.");
    const profile = options.profiles.resolve({ profileId: input.profileId, profileRevision: input.profileRevision });
    const target = input.target === "codex-home" ? profile.codexHome : profile.sqliteHome;
    return { revealed: target ? await options.revealProfileDirectory(target) : false };
  });

  register(DESKTOP_IPC_CHANNELS.clipboardWriteText, (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)) {
      throw new Error("Desktop clipboard request rejected.");
    }
    const input = validateDesktopClipboardInput(value);
    // This write-only Host action never enters Core, activity logs or diagnostics.
    if (!options.writeClipboardText) return { copied: false };
    try {
      options.writeClipboardText(input.text);
      return { copied: true };
    } catch {
      return { copied: false };
    }
  });

  register(DESKTOP_IPC_CHANNELS.historyReveal, async (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)
        || encodedSize(value) > MAX_DESKTOP_IPC_BYTES) {
      throw new Error("Desktop history reveal rejected.");
    }
    const input = validateDesktopHistoryRevealInput(value);
    const selected = options.profiles.resolve(input.profile);
    const profile = { profileId: selected.id, profileRevision: selected.revision };
    const request = createCoreRequestEnvelope(
      "getHistorySession",
      { profile, sessionId: input.sessionId, metadataOnly: true },
      `desktop-history-reveal-${randomUUID()}`
    );
    const response = await options.supervisor.request(request);
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)) return { revealed: false };
    let current;
    try {
      current = options.profiles.resolve(profile);
    } catch {
      return { revealed: false };
    }
    const rolloutPath = historyRevealRolloutPath(response, input.sessionId);
    if (!rolloutPath || !await isRevealableHistoryRollout(current.codexHome, rolloutPath)) {
      return { revealed: false };
    }
    return { revealed: options.revealHistoryFile ? await options.revealHistoryFile(rolloutPath) : false };
  });

  register(DESKTOP_IPC_CHANNELS.operationLogsList, (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)
        || encodedSize(value) > MAX_DESKTOP_IPC_BYTES) throw new Error("Operation log query rejected.");
    const input = logListInput(value);
    if (!input) throw new Error("Invalid operation log query.");
    return options.operationLogs.list(input);
  });

  register(DESKTOP_IPC_CHANNELS.operationLogsGet, (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)
        || !value || typeof value !== "object" || Array.isArray(value)
        || Object.keys(value).sort().join(",") !== "id,schemaVersion"
        || (value as { schemaVersion?: unknown }).schemaVersion !== 1
        || typeof (value as { id?: unknown }).id !== "string") throw new Error("Operation log detail rejected.");
    return options.operationLogs.get((value as { id: string }).id);
  });

  register(DESKTOP_IPC_CHANNELS.operationLogsDismissPlan, async (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)
        || !value || typeof value !== "object" || Array.isArray(value)
        || Object.keys(value).sort().join(",") !== "planId,schemaVersion"
        || (value as { schemaVersion?: unknown }).schemaVersion !== 1
        || typeof (value as { planId?: unknown }).planId !== "string") return { dismissed: false };
    const planId = (value as { planId: string }).planId;
    const owner = plans.get(planId);
    if (!owner || owner.senderId !== event.sender.id || owner.state !== "prepared") return { dismissed: false };
    plans.delete(planId);
    await options.operationLogs.dismiss(owner.logId);
    return { dismissed: true };
  });

  register(DESKTOP_IPC_CHANNELS.diagnosticsExport, async (event, value): Promise<DesktopDiagnosticsExportResult> => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)
        || encodedSize(value) > MAX_DESKTOP_IPC_BYTES) {
      return { schemaVersion: 1, status: "failed", reason: "runtime-unavailable" };
    }
    const input = diagnosticsExportInput(value);
    if (!input) return { schemaVersion: 1, status: "failed", reason: "runtime-unavailable" };
    const logId = await options.operationLogs.begin({ operation: "diagnostics", profileId: input.profile.profileId, profileRevision: input.profile.profileRevision, stage: "export" });
    let target: string | null;
    let token: string;
    try {
      target = await options.selectDiagnosticsTarget();
      if (!target) {
        await options.operationLogs.finish(logId, { status: "cancelled", outcome: "cancelled" });
        return { schemaVersion: 1, status: "cancelled" };
      }
      token = options.diagnosticsExporter.authorizeTarget(target);
    } catch {
      await options.operationLogs.finish(logId, { status: "failed", outcome: "failed", errorCode: "WRITE_FAILED" });
      return { schemaVersion: 1, status: "failed", reason: "write-failed" };
    }
    const request = createCoreRequestEnvelope(
      "getDiagnostics",
      { profile: input.profile },
      `desktop-diagnostics-${randomUUID()}`
    );
    const response = await options.supervisor.request(request);
    if (!response.ok) {
      options.diagnosticsExporter.revoke(token);
      await options.operationLogs.finishFromResponse(logId, response);
      return { schemaVersion: 1, status: "failed", reason: "runtime-unavailable" };
    }
    const result = await options.diagnosticsExporter.export(token, response.result);
    await options.operationLogs.finish(logId, {
      status: result.status === "created" ? "completed" : result.status === "cancelled" ? "cancelled" : "failed",
      outcome: result.status,
      ...(result.status === "failed" ? { errorCode: result.reason } : {})
    });
    return result;
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
    const logId = await options.operationLogs.begin({ operation: "update", stage: action });
    try {
      const status = await options.updates[action]();
      await options.operationLogs.finish(logId, {
        status: status.state === "error" || status.state === "disabled" ? "failed" : "completed",
        outcome: status.state,
        ...(status.reason ? { errorCode: status.reason } : {})
      });
      return status;
    } catch (error) {
      await options.operationLogs.finish(logId, { status: "failed", outcome: "failed", errorCode: "INTERNAL_ERROR" });
      throw error;
    }
  };
  register(DESKTOP_IPC_CHANNELS.updateCheck, updateAction("check"));
  register(DESKTOP_IPC_CHANNELS.updateDownload, updateAction("download"));
  register(DESKTOP_IPC_CHANNELS.updateInstall, updateAction("install"));
  register(DESKTOP_IPC_CHANNELS.updateReminder, async (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)
        || encodedSize(value) > MAX_DESKTOP_IPC_BYTES) throw new Error("Update reminder rejected.");
    return options.updates.setReminder(validateUpdateReminderInput(value));
  });

  const cleanup = (() => {
    unsubscribeOperations();
    unsubscribeWatchStopped();
    plans.clear();
    watches.clear();
    terminalWatches.clear();
    notifyWatchCount();
    activeRequests.clear();
    for (const channel of registered) options.ipcMain.removeHandler(channel);
  }) as DesktopIpcRegistration;
  cleanup.verifyNoActiveWatchesForRestart = verifyNoActiveWatchesForRestart;
  return cleanup;
}

export { isTrustedSender };
