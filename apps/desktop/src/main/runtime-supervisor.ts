import { randomBytes, randomUUID } from "node:crypto";

import {
  createCoreFailureEnvelope,
  createCoreRequestEnvelope,
  createPublicCoreErrorDto,
  type CoreErrorCode,
  type CoreOperationEventEnvelope,
  type CoreRequestEnvelope,
  type CoreResponseEnvelope,
  type ProfileSelector,
  type StatusSnapshot
} from "@codex-provider-sync/contracts";
import {
  isDesktopManagedMethod,
  isDesktopReadMethod,
  isDesktopSyncSwitchMethod,
  type DesktopManagedMethod,
  type DesktopReadMethod,
  type DesktopRuntimeMethod,
  type DesktopSyncSwitchMethod
} from "@codex-provider-sync/core-client";

import {
  DESKTOP_BUILD_ID,
  DESKTOP_CORE_VERSION,
  DESKTOP_RUNTIME_PROTOCOL_VERSION
} from "../shared/constants.js";
import {
  assertRuntimeHelloFrame,
  assertRuntimeOperationEventFrame,
  assertRuntimeResponseFrame,
  createRuntimeCancelFrame,
  createRuntimeRequestFrame,
  type ExpectedRuntimeIdentity,
  type RuntimeFrame,
  type RuntimeHelloFrame
} from "../shared/runtime-protocol.js";

export type RuntimeSupervisorState = "stopped" | "starting" | "ready" | "crashed" | "shutting-down";

export interface RuntimeUtilityHandle {
  postMessage(frame: RuntimeFrame): void;
  kill(): void;
  onMessage(listener: (frame: unknown) => void): () => void;
  onExit(listener: () => void): () => void;
}

export type RuntimeUtilitySpawner = (
  identity: ExpectedRuntimeIdentity
) => RuntimeUtilityHandle;

export interface RuntimeRestartInstallLease {
  waitForWrites(): Promise<void>;
  release(): void;
}

export interface CoreRuntimeSupervisorOptions {
  appVersion: string;
  spawnUtility: RuntimeUtilitySpawner;
  handshakeTimeoutMs?: number;
  requestTimeoutMs?: number;
  writeRequestTimeoutMs?: number;
}

interface PendingRuntimeRequest {
  dispatchId: string;
  request: CoreRequestEnvelope<DesktopRuntimeMethod>;
  generation: number;
  isWrite: boolean;
  operationId?: string;
  resolve(response: CoreResponseEnvelope<DesktopRuntimeMethod>): void;
  timer: ReturnType<typeof setTimeout>;
}

class RuntimeActivationError extends Error {
  readonly code: CoreErrorCode;

  constructor(code: CoreErrorCode) {
    super(code);
    this.name = "RuntimeActivationError";
    this.code = code;
  }
}

function profileFromReadRequest(request: CoreRequestEnvelope<DesktopReadMethod>): ProfileSelector {
  const payload = request.payload as { profile?: ProfileSelector };
  if (!payload.profile) throw new RuntimeActivationError("INVALID_INPUT");
  return payload.profile;
}

function profileKey(profile: ProfileSelector): string {
  return JSON.stringify([profile.profileId, profile.profileRevision]);
}

function isApplyMethod(method: DesktopRuntimeMethod): boolean {
  return method === "applySync" || method === "applySwitch" || method === "applyRestore";
}

export class CoreRuntimeSupervisor {
  readonly #appVersion: string;
  readonly #spawnUtility: RuntimeUtilitySpawner;
  readonly #handshakeTimeoutMs: number;
  readonly #requestTimeoutMs: number;
  readonly #writeRequestTimeoutMs: number;
  readonly #pending = new Map<string, PendingRuntimeRequest>();
  readonly #dispatchByRequestId = new Map<string, string>();
  readonly #profilePreflights = new Map<string, Promise<void>>();
  readonly #recoveryByProfile = new Map<string, boolean>();
  readonly #operationListeners = new Set<(event: CoreOperationEventEnvelope) => void>();
  #state: RuntimeSupervisorState = "stopped";
  #generation = 0;
  #child: RuntimeUtilityHandle | null = null;
  #detachChild: (() => void) | null = null;
  #activation: Promise<void> | null = null;
  #shutdownPromise: Promise<void> | null = null;
  #disposed = false;
  #helloResolve: ((frame: RuntimeHelloFrame) => void) | null = null;
  #helloReject: ((error: RuntimeActivationError) => void) | null = null;
  #preflightReadsAfterCrash = false;
  #lastHandshakeAt: string | null = null;
  #restartInstallGateClosed = false;
  #admittedWriteCount = 0;
  readonly #writeDrainWaiters = new Set<() => void>();

  constructor(options: CoreRuntimeSupervisorOptions) {
    this.#appVersion = options.appVersion;
    this.#spawnUtility = options.spawnUtility;
    this.#handshakeTimeoutMs = options.handshakeTimeoutMs ?? 10_000;
    this.#requestTimeoutMs = options.requestTimeoutMs ?? 30_000;
    this.#writeRequestTimeoutMs = options.writeRequestTimeoutMs ?? 15 * 60_000;
  }

  get snapshot(): Readonly<{
    state: RuntimeSupervisorState;
    generation: number;
    recoveryBlocked: boolean;
    writeInProgress: boolean;
    lastHandshakeAt: string | null;
  }> {
    return Object.freeze({
      state: this.#state,
      generation: this.#generation,
      recoveryBlocked: [...this.#recoveryByProfile.values()].some(Boolean),
      writeInProgress: this.#admittedWriteCount > 0
        || [...this.#pending.values()].some((pending) => pending.isWrite),
      lastHandshakeAt: this.#lastHandshakeAt
    });
  }

  tryBeginRestartInstall(): RuntimeRestartInstallLease | null {
    if (this.#disposed
        || this.#state === "shutting-down"
        || this.#shutdownPromise
        || this.#restartInstallGateClosed) return null;
    this.#restartInstallGateClosed = true;
    let released = false;
    return Object.freeze({
      waitForWrites: async () => {
        if (released || this.#admittedWriteCount === 0) return;
        await new Promise<void>((resolve) => this.#writeDrainWaiters.add(resolve));
      },
      release: () => {
        if (released) return;
        released = true;
        this.#restartInstallGateClosed = false;
      }
    });
  }

  subscribeOperation(listener: (event: CoreOperationEventEnvelope) => void): () => void {
    this.#operationListeners.add(listener);
    return () => this.#operationListeners.delete(listener);
  }

  async verifyProfilesSafeForRestart(
    profiles: readonly ProfileSelector[]
  ): Promise<"clear" | "blocked" | "unverifiable"> {
    if (profiles.length === 0 || this.snapshot.writeInProgress) return "unverifiable";
    try {
      for (const profile of profiles) {
        await this.#ensureReady(profile, false);
        this.#invalidateProfilePreflight(profile);
        await this.#ensureProfilePreflight(profile);
        if (this.#recoveryByProfile.get(profileKey(profile)) === true) return "blocked";
      }
      return this.snapshot.writeInProgress ? "unverifiable" : "clear";
    } catch {
      return "unverifiable";
    }
  }

  async request<M extends DesktopReadMethod>(
    request: CoreRequestEnvelope<M>
  ): Promise<CoreResponseEnvelope<M>> {
    if (!isDesktopReadMethod(request.method)) {
      return createCoreFailureEnvelope(request, createPublicCoreErrorDto("PERMISSION_DENIED"));
    }
    const profile = profileFromReadRequest(request as CoreRequestEnvelope<DesktopReadMethod>);
    try {
      await this.#ensureReady(profile, false);
    } catch (error) {
      const code = error instanceof RuntimeActivationError ? error.code : "INTERNAL_ERROR";
      return createCoreFailureEnvelope(request, createPublicCoreErrorDto(code));
    }
    return this.#dispatch(request, false) as Promise<CoreResponseEnvelope<M>>;
  }

  async requestWrite<M extends DesktopSyncSwitchMethod>(
    request: CoreRequestEnvelope<M>,
    profile: ProfileSelector
  ): Promise<CoreResponseEnvelope<M>> {
    if (!isDesktopSyncSwitchMethod(request.method) || request.operationId !== undefined) {
      return createCoreFailureEnvelope(request, createPublicCoreErrorDto("PERMISSION_DENIED"));
    }
    return this.requestManaged(request, profile);
  }

  async requestManaged<M extends DesktopManagedMethod>(
    request: CoreRequestEnvelope<M>,
    profile: ProfileSelector,
    options: {
      allowRecoveryBlocked?: boolean;
    } = {}
  ): Promise<CoreResponseEnvelope<M>> {
    if (!isDesktopManagedMethod(request.method) || request.operationId !== undefined) {
      return createCoreFailureEnvelope(request, createPublicCoreErrorDto("PERMISSION_DENIED"));
    }
    const isWrite = request.method !== "getWatchStatus";
    const releaseAdmission = isWrite ? this.#tryAdmitWrite() : null;
    if (isWrite && !releaseAdmission) {
      return createCoreFailureEnvelope(request, createPublicCoreErrorDto("OPERATION_BUSY", {
        details: { busyScope: "codex-home" }
      }));
    }
    try {
      try {
        await this.#ensureReady(profile, isWrite);
        if (!options.allowRecoveryBlocked
            && this.#recoveryByProfile.get(profileKey(profile)) === true) {
          throw new RuntimeActivationError("PENDING_TRANSACTION");
        }
      } catch (error) {
        const code = error instanceof RuntimeActivationError ? error.code : "INTERNAL_ERROR";
        return createCoreFailureEnvelope(request, createPublicCoreErrorDto(code));
      }
      const response = await this.#dispatch(request, isWrite) as CoreResponseEnvelope<M>;
      if (request.method === "applyRestore") {
        this.#invalidateProfilePreflight(profile);
        if (response.ok) {
          try {
            await this.#ensureProfilePreflight(profile);
          } catch {
            this.#recoveryByProfile.set(profileKey(profile), true);
          }
        }
      }
      return response;
    } finally {
      releaseAdmission?.();
    }
  }

  cancel(requestId: string, operationId?: string): boolean {
    const dispatchId = this.#dispatchByRequestId.get(requestId);
    const pending = dispatchId ? this.#pending.get(dispatchId) : undefined;
    if (!pending || !pending.isWrite || !isApplyMethod(pending.request.method)) return false;
    if (operationId !== undefined && operationId !== pending.operationId) return false;
    const child = this.#child;
    if (!child || pending.generation !== this.#generation || this.#state !== "ready") return false;
    try {
      child.postMessage(createRuntimeCancelFrame(
        pending.generation,
        pending.dispatchId,
        requestId,
        operationId
      ));
      return true;
    } catch {
      this.#failRuntime(child);
      return false;
    }
  }

  crashForTest(): boolean {
    const child = this.#child;
    if (!child || this.#state !== "ready") return false;
    this.#failRuntime(child);
    return true;
  }

  async shutdown(): Promise<void> {
    if (this.#shutdownPromise) return this.#shutdownPromise;
    this.#disposed = true;
    if (this.#state === "stopped") return;
    const shutdown = this.#performShutdown();
    this.#shutdownPromise = shutdown;
    try {
      await shutdown;
    } finally {
      if (this.#shutdownPromise === shutdown) this.#shutdownPromise = null;
    }
  }

  async #performShutdown(): Promise<void> {
    this.#state = "shutting-down";
    this.#helloReject?.(new RuntimeActivationError("INTERNAL_ERROR"));
    const child = this.#child;
    if (!child) {
      this.#failAllPending("INTERNAL_ERROR");
      this.#clearRuntimeCaches();
      this.#state = "stopped";
      return;
    }
    try {
      child.postMessage({
        kind: "shutdown",
        runtimeProtocolVersion: DESKTOP_RUNTIME_PROTOCOL_VERSION,
        generation: this.#generation
      });
    } catch {
      child.kill();
    }
    await new Promise<void>((resolve) => {
      const timeout = setTimeout(() => {
        child.kill();
        resolve();
      }, 30_000);
      const detach = child.onExit(() => {
        clearTimeout(timeout);
        detach();
        resolve();
      });
    });
    await this.#activation?.catch(() => undefined);
    this.#clearChild(child);
    this.#failAllPending("INTERNAL_ERROR");
    this.#clearRuntimeCaches();
    this.#state = "stopped";
  }

  #tryAdmitWrite(): (() => void) | null {
    if (this.#restartInstallGateClosed || this.#disposed || this.#state === "shutting-down") {
      return null;
    }
    this.#admittedWriteCount += 1;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.#admittedWriteCount -= 1;
      if (this.#admittedWriteCount !== 0) return;
      const waiters = [...this.#writeDrainWaiters];
      this.#writeDrainWaiters.clear();
      for (const resolve of waiters) resolve();
    };
  }

  async #ensureReady(profile: ProfileSelector, requireWritePreflight: boolean): Promise<void> {
    if (this.#disposed || this.#state === "shutting-down" || this.#shutdownPromise) {
      throw new RuntimeActivationError("INTERNAL_ERROR");
    }
    if (this.#state !== "ready") {
      if (!this.#activation) this.#activation = this.#startRuntime(this.#state === "crashed");
      const activation = this.#activation;
      try {
        await activation;
      } finally {
        if (this.#activation === activation) this.#activation = null;
      }
    }
    if (this.#shutdownPromise) throw new RuntimeActivationError("INTERNAL_ERROR");
    if (this.#state !== "ready") throw new RuntimeActivationError("CORE_RUNTIME_CRASHED");
    if (requireWritePreflight || this.#preflightReadsAfterCrash) {
      await this.#ensureProfilePreflight(profile);
    }
  }

  async #startRuntime(restartedAfterCrash: boolean): Promise<void> {
    this.#generation += 1;
    const identity: ExpectedRuntimeIdentity = {
      appVersion: this.#appVersion,
      coreVersion: DESKTOP_CORE_VERSION,
      buildId: DESKTOP_BUILD_ID,
      sessionNonce: randomBytes(32).toString("hex"),
      generation: this.#generation
    };
    this.#state = "starting";
    let child: RuntimeUtilityHandle;
    try {
      child = this.#spawnUtility(identity);
    } catch {
      this.#state = "crashed";
      throw new RuntimeActivationError("CORE_RUNTIME_CRASHED");
    }
    const hello = new Promise<RuntimeHelloFrame>((resolve, reject) => {
      this.#helloResolve = resolve;
      this.#helloReject = reject;
    });
    this.#child = child;
    const detachMessage = child.onMessage((frame) => this.#handleMessage(child, frame, identity));
    const detachExit = child.onExit(() => this.#handleExit(child));
    this.#detachChild = () => { detachMessage(); detachExit(); };
    const timeout = setTimeout(() => {
      this.#helloReject?.(new RuntimeActivationError("PROTOCOL_VERSION_MISMATCH"));
    }, this.#handshakeTimeoutMs);
    try {
      await hello;
    } catch (error) {
      this.#clearChild(child);
      child.kill();
      if (!this.#shutdownPromise) this.#state = "crashed";
      throw error;
    } finally {
      clearTimeout(timeout);
      this.#helloResolve = null;
      this.#helloReject = null;
    }
    if (this.#child !== child) throw new RuntimeActivationError("CORE_RUNTIME_CRASHED");
    this.#clearRuntimeCaches();
    this.#preflightReadsAfterCrash = restartedAfterCrash;
    this.#state = "ready";
    this.#lastHandshakeAt = new Date().toISOString();
  }

  async #ensureProfilePreflight(profile: ProfileSelector): Promise<void> {
    const key = profileKey(profile);
    const generationKey = `${this.#generation}:${key}`;
    let preflight = this.#profilePreflights.get(generationKey);
    if (!preflight) {
      preflight = this.#preflight(profile, key, this.#generation).catch((error) => {
        if (this.#profilePreflights.get(generationKey) === preflight) {
          this.#profilePreflights.delete(generationKey);
        }
        throw error;
      });
      this.#profilePreflights.set(generationKey, preflight);
    }
    await preflight;
  }

  #invalidateProfilePreflight(profile: ProfileSelector): void {
    const key = profileKey(profile);
    for (const cached of this.#profilePreflights.keys()) {
      if (cached.endsWith(`:${key}`)) this.#profilePreflights.delete(cached);
    }
    this.#recoveryByProfile.delete(key);
  }

  async #preflight(profile: ProfileSelector, key: string, generation: number): Promise<void> {
    const request = createCoreRequestEnvelope(
      "getStatus",
      { profile },
      `desktop-preflight-${randomUUID()}`
    );
    const response = await this.#dispatch(request, false);
    if (!response.ok) throw new RuntimeActivationError(response.error.code);
    if (generation !== this.#generation || this.#state !== "ready") {
      throw new RuntimeActivationError("CORE_RUNTIME_CRASHED");
    }
    const status = response.result as StatusSnapshot;
    if (status.profile.id !== profile.profileId
        || (profile.profileRevision !== undefined
          && status.profile.revision !== profile.profileRevision)) {
      const child = this.#child;
      if (child) this.#failRuntime(child);
      throw new RuntimeActivationError("CORE_RUNTIME_CRASHED");
    }
    this.#recoveryByProfile.set(
      key,
      status.pendingRecovery === true
        || (Array.isArray(status.pendingTransactions) && status.pendingTransactions.length > 0)
    );
  }

  #dispatch<M extends DesktopRuntimeMethod>(
    request: CoreRequestEnvelope<M>,
    isWrite: boolean
  ): Promise<CoreResponseEnvelope<M>> {
    const child = this.#child;
    const generation = this.#generation;
    if (!child || this.#state !== "ready") {
      return Promise.resolve(createCoreFailureEnvelope(
        request,
        createPublicCoreErrorDto("CORE_RUNTIME_CRASHED")
      ));
    }
    if (this.#dispatchByRequestId.has(request.requestId)) {
      return Promise.resolve(createCoreFailureEnvelope(
        request,
        createPublicCoreErrorDto("INVALID_INPUT")
      ));
    }
    const dispatchId = randomUUID();
    return new Promise<CoreResponseEnvelope<M>>((resolve) => {
      const timeoutMs = isWrite ? this.#writeRequestTimeoutMs : this.#requestTimeoutMs;
      const timer = setTimeout(() => {
        this.#removePending(dispatchId);
        resolve(createCoreFailureEnvelope(
          request,
          createPublicCoreErrorDto(isWrite ? "CORE_RUNTIME_CRASHED" : "INTERNAL_ERROR")
        ));
        this.#failRuntime(child);
      }, timeoutMs);
      const pending: PendingRuntimeRequest = {
        dispatchId,
        request: request as CoreRequestEnvelope<DesktopRuntimeMethod>,
        generation,
        isWrite,
        resolve: resolve as (response: CoreResponseEnvelope<DesktopRuntimeMethod>) => void,
        timer
      };
      this.#pending.set(dispatchId, pending);
      this.#dispatchByRequestId.set(request.requestId, dispatchId);
      try {
        child.postMessage(createRuntimeRequestFrame(generation, dispatchId, request));
      } catch {
        this.#removePending(dispatchId);
        resolve(createCoreFailureEnvelope(request, createPublicCoreErrorDto("CORE_RUNTIME_CRASHED")));
        this.#failRuntime(child);
      }
    });
  }

  #handleMessage(
    child: RuntimeUtilityHandle,
    frame: unknown,
    identity: ExpectedRuntimeIdentity
  ): void {
    if (this.#child !== child) return;
    if (this.#state === "starting") {
      try {
        assertRuntimeHelloFrame(frame, identity);
        this.#helloResolve?.(frame);
      } catch {
        this.#helloReject?.(new RuntimeActivationError("PROTOCOL_VERSION_MISMATCH"));
      }
      return;
    }
    if (this.#state !== "ready") return;
    try {
      const kind = frame !== null && typeof frame === "object" && !Array.isArray(frame)
        ? (frame as { kind?: unknown }).kind
        : undefined;
      if (kind === "operation-event") {
        this.#handleOperationEvent(frame);
        return;
      }
      assertRuntimeResponseFrame(frame);
      if (frame.generation !== this.#generation) throw new Error("Stale runtime response.");
      const pending = this.#pending.get(frame.dispatchId);
      if (!pending || pending.generation !== frame.generation) throw new Error("Unknown runtime response.");
      assertRuntimeResponseFrame(frame, {
        dispatchId: pending.dispatchId,
        requestId: pending.request.requestId
      });
      const responseOperationId = frame.envelope.operationId;
      if (pending.operationId !== undefined) {
        if (responseOperationId !== pending.operationId) {
          throw new Error("Runtime response operationId mismatch.");
        }
        if (frame.envelope.ok
            && (!frame.envelope.result
              || typeof frame.envelope.result !== "object"
              || !("operationId" in frame.envelope.result)
              || frame.envelope.result.operationId !== pending.operationId)) {
          throw new Error("Runtime result operationId mismatch.");
        }
        if (!frame.envelope.ok
            && frame.envelope.error.operationId !== undefined
            && frame.envelope.error.operationId !== pending.operationId) {
          throw new Error("Runtime error operationId mismatch.");
        }
      } else if (responseOperationId !== undefined) {
        throw new Error("Runtime response supplied operationId before operation-started.");
      }
      this.#removePending(pending.dispatchId);
      pending.resolve(frame.envelope);
    } catch {
      this.#failRuntime(child);
    }
  }

  #handleOperationEvent(frame: unknown): void {
    assertRuntimeOperationEventFrame(frame);
    if (frame.generation !== this.#generation) throw new Error("Stale runtime operation event.");
    const pending = this.#pending.get(frame.dispatchId);
    if (!pending || pending.generation !== frame.generation || !isApplyMethod(pending.request.method)) {
      throw new Error("Unknown runtime operation event.");
    }
    assertRuntimeOperationEventFrame(frame, {
      dispatchId: pending.dispatchId,
      requestId: pending.request.requestId,
      ...(pending.operationId ? { operationId: pending.operationId } : {})
    });
    if (frame.envelope.event === "operation-started") {
      if (pending.operationId !== undefined) throw new Error("Duplicate operation-started event.");
      const expectedOperation = pending.request.method === "applySync"
        ? "sync"
        : pending.request.method === "applySwitch"
          ? "switch"
          : "restore";
      if (frame.envelope.operation !== expectedOperation) {
        throw new Error("Runtime operation kind mismatch.");
      }
      pending.operationId = frame.envelope.operationId;
    } else if (pending.operationId === undefined) {
      throw new Error("Runtime progress preceded operation-started.");
    }
    for (const listener of this.#operationListeners) {
      try { listener(frame.envelope); } catch {}
    }
  }

  #removePending(dispatchId: string): PendingRuntimeRequest | undefined {
    const pending = this.#pending.get(dispatchId);
    if (!pending) return undefined;
    clearTimeout(pending.timer);
    this.#pending.delete(dispatchId);
    if (this.#dispatchByRequestId.get(pending.request.requestId) === dispatchId) {
      this.#dispatchByRequestId.delete(pending.request.requestId);
    }
    return pending;
  }

  #handleExit(child: RuntimeUtilityHandle): void {
    if (this.#child !== child) return;
    const shuttingDown = this.#state === "shutting-down";
    this.#clearChild(child);
    this.#helloReject?.(new RuntimeActivationError("CORE_RUNTIME_CRASHED"));
    this.#failAllPending(shuttingDown ? "INTERNAL_ERROR" : "CORE_RUNTIME_CRASHED");
    this.#clearRuntimeCaches();
    this.#state = shuttingDown ? "stopped" : "crashed";
  }

  #failRuntime(child: RuntimeUtilityHandle): void {
    if (this.#child !== child) return;
    this.#clearChild(child);
    child.kill();
    this.#failAllPending("CORE_RUNTIME_CRASHED");
    this.#clearRuntimeCaches();
    if (this.#state !== "shutting-down") this.#state = "crashed";
  }

  #failAllPending(code: CoreErrorCode): void {
    for (const pending of [...this.#pending.values()]) {
      this.#removePending(pending.dispatchId);
      pending.resolve(createCoreFailureEnvelope(
        pending.request,
        createPublicCoreErrorDto(code, { operationId: pending.operationId }),
        pending.operationId
      ));
    }
  }

  #clearChild(child: RuntimeUtilityHandle): void {
    if (this.#child !== child) return;
    this.#detachChild?.();
    this.#detachChild = null;
    this.#child = null;
  }

  #clearRuntimeCaches(): void {
    this.#preflightReadsAfterCrash = false;
    this.#profilePreflights.clear();
    this.#recoveryByProfile.clear();
  }
}
