import process from "node:process";

import { DESKTOP_RUNTIME_METHODS } from "@codex-provider-sync/core-client";

import { DesktopProfileRepository } from "../profiles/repository.js";
import {
  DESKTOP_BUILD_ID,
  DESKTOP_CORE_PROTOCOL_VERSION,
  DESKTOP_CORE_VERSION,
  DESKTOP_RUNTIME_PROTOCOL_VERSION
} from "../shared/constants.js";
import {
  assertRuntimeCancelFrame,
  assertRuntimeRequestFrame,
  assertRuntimeShutdownFrame,
  createRuntimeOperationEventFrame,
  createRuntimeResponseFrame,
  type RuntimeHelloFrame
} from "../shared/runtime-protocol.js";
import { createDesktopRuntimeHost } from "./host.js";

interface UtilityParentPort {
  postMessage(value: unknown): void;
  on(event: "message", listener: (event: { data: unknown }) => void): void;
}

interface ActiveDispatch {
  requestId: string;
  controller: AbortController;
  operationId?: string;
  task?: Promise<void>;
}

function requiredEnvironment(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing desktop runtime bootstrap field: ${name}`);
  return value;
}

const parentPort = (process as NodeJS.Process & { parentPort?: UtilityParentPort }).parentPort;
if (!parentPort) throw new Error("Desktop Core Runtime requires an Electron Utility Process parent port.");

const generation = Number(requiredEnvironment("CPS_DESKTOP_RUNTIME_GENERATION"));
if (!Number.isSafeInteger(generation) || generation < 1) {
  throw new Error("Invalid desktop runtime generation.");
}

const profiles = new DesktopProfileRepository({
  filePath: requiredEnvironment("CPS_DESKTOP_PROFILE_FILE"),
  defaultCodexHome: requiredEnvironment("CPS_DESKTOP_DEFAULT_CODEX_HOME"),
  ...(process.env.CPS_DESKTOP_DEFAULT_SQLITE_HOME
    ? { defaultSqliteHome: process.env.CPS_DESKTOP_DEFAULT_SQLITE_HOME }
    : {})
});
await profiles.initialize();
const testApplyInvoker = __CPS_DESKTOP_TEST_BUILD__
  && process.env.CPS_DESKTOP_E2E === "1"
  ? (await import("./e2e-gate.js")).createDesktopTestApplyInvoker()
  : undefined;
const host = createDesktopRuntimeHost(profiles, testApplyInvoker);
const active = new Map<string, ActiveDispatch>();
let shuttingDown = false;

const hello: RuntimeHelloFrame = {
  kind: "hello",
  runtimeProtocolVersion: DESKTOP_RUNTIME_PROTOCOL_VERSION,
  coreProtocolVersion: DESKTOP_CORE_PROTOCOL_VERSION,
  appVersion: requiredEnvironment("CPS_DESKTOP_APP_VERSION"),
  coreVersion: DESKTOP_CORE_VERSION,
  buildId: DESKTOP_BUILD_ID,
  sessionNonce: requiredEnvironment("CPS_DESKTOP_RUNTIME_NONCE"),
  generation,
  capabilities: DESKTOP_RUNTIME_METHODS
};
parentPort.postMessage(hello);

async function beginShutdown(frame: unknown): Promise<void> {
  assertRuntimeShutdownFrame(frame);
  if (frame.generation !== generation) throw new Error("Stale desktop runtime shutdown frame.");
  shuttingDown = true;
  for (const dispatch of active.values()) dispatch.controller.abort();
  await Promise.allSettled(
    [...active.values()].map((dispatch) => dispatch.task).filter(Boolean) as Promise<void>[]
  );
  process.exit(0);
}

function cancelDispatch(frame: unknown): void {
  assertRuntimeCancelFrame(frame);
  if (frame.generation !== generation) throw new Error("Stale desktop runtime cancel frame.");
  const dispatch = active.get(frame.dispatchId);
  if (!dispatch || dispatch.requestId !== frame.requestId) {
    throw new Error("Unknown desktop runtime cancel target.");
  }
  if (frame.operationId !== undefined && dispatch.operationId !== frame.operationId) {
    throw new Error("Desktop runtime cancel operationId mismatch.");
  }
  dispatch.controller.abort();
}

function startDispatch(frame: unknown): void {
  assertRuntimeRequestFrame(frame);
  if (shuttingDown) throw new Error("Desktop runtime is shutting down.");
  if (frame.generation !== generation) throw new Error("Stale desktop runtime request frame.");
  if (active.has(frame.dispatchId)) throw new Error("Duplicate desktop runtime dispatchId.");
  const dispatch: ActiveDispatch = {
    requestId: frame.envelope.requestId,
    controller: new AbortController()
  };
  active.set(frame.dispatchId, dispatch);
  const task = host.dispatch(frame.envelope, {
    signal: dispatch.controller.signal,
    onOperationStarted(envelope) {
      if (dispatch.operationId !== undefined) {
        throw new Error("Duplicate desktop operation-started event.");
      }
      dispatch.operationId = envelope.operationId;
      parentPort.postMessage(createRuntimeOperationEventFrame(
        generation,
        frame.dispatchId,
        envelope
      ));
    },
    onProgress(envelope) {
      if (!dispatch.operationId || envelope.operationId !== dispatch.operationId) {
        throw new Error("Desktop progress event preceded operation-started.");
      }
      parentPort.postMessage(createRuntimeOperationEventFrame(
        generation,
        frame.dispatchId,
        envelope
      ));
    }
  }).then((response) => {
    parentPort.postMessage(createRuntimeResponseFrame(
      generation,
      frame.dispatchId,
      response
    ));
  }).finally(() => {
    active.delete(frame.dispatchId);
  });
  dispatch.task = task;
  void task.catch(() => process.exit(70));
}

parentPort.on("message", (event) => {
  try {
    const frame = event.data;
    const kind = frame !== null && typeof frame === "object" && !Array.isArray(frame)
      ? (frame as { kind?: unknown }).kind
      : undefined;
    if (kind === "shutdown") {
      void beginShutdown(frame).catch(() => process.exit(70));
      return;
    }
    if (kind === "cancel") {
      cancelDispatch(frame);
      return;
    }
    startDispatch(frame);
  } catch {
    process.exit(70);
  }
});
