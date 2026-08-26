import { randomBytes, randomUUID } from "node:crypto";

import {
  createCoreFailureEnvelope,
  createCoreRequestEnvelope,
  createPublicCoreErrorDto,
  type CoreErrorCode,
  type CoreRequestEnvelope,
  type CoreResponseEnvelope,
  type ProfileSelector,
  type StatusSnapshot
} from "@codex-provider-sync/contracts";
import {
  isDesktopReadMethod,
  type DesktopReadMethod
} from "@codex-provider-sync/core-client";

import {
  DESKTOP_BUILD_ID,
  DESKTOP_CORE_VERSION,
  DESKTOP_RUNTIME_PROTOCOL_VERSION
} from "../shared/constants.js";
import {
  assertRuntimeHelloFrame,
  assertRuntimeResponseFrame,
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

export interface CoreRuntimeSupervisorOptions {
  appVersion: string;
  spawnUtility: RuntimeUtilitySpawner;
  handshakeTimeoutMs?: number;
  requestTimeoutMs?: number;
}

interface PendingRuntimeRequest {
  request: CoreRequestEnvelope<DesktopReadMethod>;
  generation: number;
  resolve(response: CoreResponseEnvelope<DesktopReadMethod>): void;
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

function profileFromRequest(request: CoreRequestEnvelope<DesktopReadMethod>): ProfileSelector {
  const payload = request.payload as { profile?: ProfileSelector };
  if (!payload.profile) throw new RuntimeActivationError("INVALID_INPUT");
  return payload.profile;
}

export class CoreRuntimeSupervisor {
  readonly #appVersion: string;
  readonly #spawnUtility: RuntimeUtilitySpawner;
  readonly #handshakeTimeoutMs: number;
  readonly #requestTimeoutMs: number;
  readonly #pending = new Map<string, PendingRuntimeRequest>();
  readonly #profilePreflights = new Map<string, Promise<void>>();
  readonly #recoveryByProfile = new Map<string, boolean>();
  #state: RuntimeSupervisorState = "stopped";
  #generation = 0;
  #child: RuntimeUtilityHandle | null = null;
  #detachChild: (() => void) | null = null;
  #activation: Promise<void> | null = null;
  #shutdownPromise: Promise<void> | null = null;
  #disposed = false;
  #helloResolve: ((frame: RuntimeHelloFrame) => void) | null = null;
  #helloReject: ((error: RuntimeActivationError) => void) | null = null;
  #preflightGeneration = 0;
  #lastHandshakeAt: string | null = null;

  constructor(options: CoreRuntimeSupervisorOptions) {
    this.#appVersion = options.appVersion;
    this.#spawnUtility = options.spawnUtility;
    this.#handshakeTimeoutMs = options.handshakeTimeoutMs ?? 10_000;
    this.#requestTimeoutMs = options.requestTimeoutMs ?? 30_000;
  }

  get snapshot(): Readonly<{
    state: RuntimeSupervisorState;
    generation: number;
    recoveryBlocked: boolean;
    lastHandshakeAt: string | null;
  }> {
    return Object.freeze({
      state: this.#state,
      generation: this.#generation,
      recoveryBlocked: [...this.#recoveryByProfile.values()].some(Boolean),
      lastHandshakeAt: this.#lastHandshakeAt
    });
  }

  async request<M extends DesktopReadMethod>(
    request: CoreRequestEnvelope<M>
  ): Promise<CoreResponseEnvelope<M>> {
    if (!isDesktopReadMethod(request.method)) {
      return createCoreFailureEnvelope(request, createPublicCoreErrorDto("PERMISSION_DENIED"));
    }
    try {
      await this.#ensureReady(profileFromRequest(request));
    } catch (error) {
      const code = error instanceof RuntimeActivationError ? error.code : "INTERNAL_ERROR";
      return createCoreFailureEnvelope(request, createPublicCoreErrorDto(code));
    }
    return this.#dispatch(request) as Promise<CoreResponseEnvelope<M>>;
  }

  crashForTest(): boolean {
    if (!this.#child || this.#state === "shutting-down") return false;
    this.#child.kill();
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
    this.#failAllPending("INTERNAL_ERROR");
    if (!child) {
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
      }, 1_000);
      const detach = child.onExit(() => {
        clearTimeout(timeout);
        detach();
        resolve();
      });
    });
    await this.#activation?.catch(() => undefined);
    this.#clearChild(child);
    this.#clearRuntimeCaches();
    this.#state = "stopped";
  }

  async #ensureReady(profile: ProfileSelector): Promise<void> {
    if (this.#disposed || this.#state === "shutting-down" || this.#shutdownPromise) {
      throw new RuntimeActivationError("INTERNAL_ERROR");
    }
    if (this.#state !== "ready") {
      if (!this.#activation) {
        this.#activation = this.#startRuntime(this.#state === "crashed");
      }
      const activation = this.#activation;
      try {
        await activation;
      } finally {
        if (this.#activation === activation) this.#activation = null;
      }
    }
    if (this.#shutdownPromise) {
      throw new RuntimeActivationError("INTERNAL_ERROR");
    }
    if (this.#state !== "ready") throw new RuntimeActivationError("CORE_RUNTIME_CRASHED");
    if (this.#preflightGeneration === this.#generation) {
      await this.#ensureProfilePreflight(profile);
    }
  }

  async #startRuntime(requirePreflight: boolean): Promise<void> {
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
    this.#detachChild = () => {
      detachMessage();
      detachExit();
    };
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
    this.#state = "ready";
    this.#lastHandshakeAt = new Date().toISOString();
    this.#clearRuntimeCaches();
    this.#preflightGeneration = requirePreflight ? this.#generation : 0;
  }

  async #ensureProfilePreflight(profile: ProfileSelector): Promise<void> {
    const generation = this.#generation;
    const key = `${generation}:${JSON.stringify([profile.profileId, profile.profileRevision])}`;
    let preflight = this.#profilePreflights.get(key);
    if (!preflight) {
      preflight = this.#preflight(profile, key, generation).catch((error) => {
        if (this.#profilePreflights.get(key) === preflight) this.#profilePreflights.delete(key);
        throw error;
      });
      this.#profilePreflights.set(key, preflight);
    }
    await preflight;
  }

  async #preflight(profile: ProfileSelector, key: string, generation: number): Promise<void> {
    const request = createCoreRequestEnvelope(
      "getStatus",
      { profile },
      `desktop-preflight-${randomUUID()}`
    );
    const response = await this.#dispatch(request);
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

  #dispatch<M extends DesktopReadMethod>(
    request: CoreRequestEnvelope<M>
  ): Promise<CoreResponseEnvelope<M>> {
    const child = this.#child;
    const generation = this.#generation;
    if (!child || this.#state !== "ready") {
      return Promise.resolve(createCoreFailureEnvelope(
        request,
        createPublicCoreErrorDto("CORE_RUNTIME_CRASHED")
      ));
    }
    if (this.#pending.has(request.requestId)) {
      return Promise.resolve(createCoreFailureEnvelope(
        request,
        createPublicCoreErrorDto("INVALID_INPUT")
      ));
    }
    return new Promise<CoreResponseEnvelope<M>>((resolve) => {
      const timer = setTimeout(() => {
        this.#pending.delete(request.requestId);
        resolve(createCoreFailureEnvelope(request, createPublicCoreErrorDto("INTERNAL_ERROR")));
        this.#failRuntime(child);
      }, this.#requestTimeoutMs);
      this.#pending.set(request.requestId, {
        request: request as CoreRequestEnvelope<DesktopReadMethod>,
        generation,
        resolve: resolve as (response: CoreResponseEnvelope<DesktopReadMethod>) => void,
        timer
      });
      try {
        child.postMessage(createRuntimeRequestFrame(generation, request));
      } catch {
        clearTimeout(timer);
        this.#pending.delete(request.requestId);
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
      assertRuntimeResponseFrame(frame);
      if (frame.generation !== this.#generation) throw new Error("Stale runtime response.");
      const pending = this.#pending.get(frame.envelope.requestId);
      if (!pending || pending.generation !== frame.generation) throw new Error("Unknown runtime response.");
      assertRuntimeResponseFrame(frame, pending.request.requestId);
      if ((frame.envelope.operationId ?? null) !== (pending.request.operationId ?? null)) {
        throw new Error("Runtime response operationId mismatch.");
      }
      if (!frame.envelope.ok
          && frame.envelope.error.operationId !== undefined
          && frame.envelope.error.operationId !== frame.envelope.operationId) {
        throw new Error("Runtime error operationId mismatch.");
      }
      clearTimeout(pending.timer);
      this.#pending.delete(pending.request.requestId);
      pending.resolve(frame.envelope);
    } catch {
      this.#failRuntime(child);
    }
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
    for (const pending of this.#pending.values()) {
      clearTimeout(pending.timer);
      pending.resolve(createCoreFailureEnvelope(
        pending.request,
        createPublicCoreErrorDto(code)
      ));
    }
    this.#pending.clear();
  }

  #clearChild(child: RuntimeUtilityHandle): void {
    if (this.#child !== child) return;
    this.#detachChild?.();
    this.#detachChild = null;
    this.#child = null;
  }

  #clearRuntimeCaches(): void {
    this.#preflightGeneration = 0;
    this.#profilePreflights.clear();
    this.#recoveryByProfile.clear();
  }
}
