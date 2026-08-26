import {
  assertCoreOperationEventEnvelope,
  assertCoreRequestEnvelope,
  assertCoreResponseEnvelope,
  type CoreMethodName,
  type CoreOperationEventEnvelope,
  type CoreRequestEnvelope,
  type CoreResponseEnvelope
} from "@codex-provider-sync/contracts";
import {
  DESKTOP_RUNTIME_METHODS,
  isDesktopRuntimeMethod,
  type DesktopRuntimeMethod
} from "@codex-provider-sync/core-client";

import {
  DESKTOP_CORE_PROTOCOL_VERSION,
  DESKTOP_RUNTIME_PROTOCOL_VERSION
} from "./constants.js";

export interface RuntimeHelloFrame {
  kind: "hello";
  runtimeProtocolVersion: typeof DESKTOP_RUNTIME_PROTOCOL_VERSION;
  coreProtocolVersion: typeof DESKTOP_CORE_PROTOCOL_VERSION;
  appVersion: string;
  coreVersion: string;
  buildId: string;
  sessionNonce: string;
  generation: number;
  capabilities: readonly DesktopRuntimeMethod[];
}

export interface RuntimeRequestFrame<M extends DesktopRuntimeMethod = DesktopRuntimeMethod> {
  kind: "request";
  runtimeProtocolVersion: typeof DESKTOP_RUNTIME_PROTOCOL_VERSION;
  generation: number;
  dispatchId: string;
  envelope: CoreRequestEnvelope<M>;
}

export interface RuntimeResponseFrame<M extends DesktopRuntimeMethod = DesktopRuntimeMethod> {
  kind: "response";
  runtimeProtocolVersion: typeof DESKTOP_RUNTIME_PROTOCOL_VERSION;
  generation: number;
  dispatchId: string;
  envelope: CoreResponseEnvelope<M>;
}

export interface RuntimeOperationEventFrame {
  kind: "operation-event";
  runtimeProtocolVersion: typeof DESKTOP_RUNTIME_PROTOCOL_VERSION;
  generation: number;
  dispatchId: string;
  envelope: CoreOperationEventEnvelope;
}

export interface RuntimeCancelFrame {
  kind: "cancel";
  runtimeProtocolVersion: typeof DESKTOP_RUNTIME_PROTOCOL_VERSION;
  generation: number;
  dispatchId: string;
  requestId: string;
  operationId?: string;
}

export interface RuntimeShutdownFrame {
  kind: "shutdown";
  runtimeProtocolVersion: typeof DESKTOP_RUNTIME_PROTOCOL_VERSION;
  generation: number;
}

export type RuntimeFrame =
  | RuntimeHelloFrame
  | RuntimeRequestFrame
  | RuntimeResponseFrame
  | RuntimeOperationEventFrame
  | RuntimeCancelFrame
  | RuntimeShutdownFrame;

export interface ExpectedRuntimeIdentity {
  appVersion: string;
  coreVersion: string;
  buildId: string;
  sessionNonce: string;
  generation: number;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, allowed: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const expected = [...allowed].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

function isBoundedString(value: unknown, maximum = 256): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= maximum;
}

function assertGeneration(value: unknown): asserts value is number {
  if (!Number.isSafeInteger(value) || Number(value) < 1) {
    throw new TypeError("Invalid desktop runtime generation.");
  }
}

function assertDispatchId(value: unknown): asserts value is string {
  if (typeof value !== "string"
      || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)) {
    throw new TypeError("Invalid desktop runtime dispatchId.");
  }
}

export function assertRuntimeHelloFrame(
  value: unknown,
  expected: ExpectedRuntimeIdentity
): asserts value is RuntimeHelloFrame {
  if (!isRecord(value)
      || !hasExactKeys(value, [
        "kind", "runtimeProtocolVersion", "coreProtocolVersion", "appVersion",
        "coreVersion", "buildId", "sessionNonce", "generation", "capabilities"
      ])
      || value.kind !== "hello"
      || value.runtimeProtocolVersion !== DESKTOP_RUNTIME_PROTOCOL_VERSION
      || value.coreProtocolVersion !== DESKTOP_CORE_PROTOCOL_VERSION
      || !isBoundedString(value.appVersion)
      || !isBoundedString(value.coreVersion)
      || !isBoundedString(value.buildId)
      || !/^[a-f0-9]{32,128}$/.test(String(value.sessionNonce))
      || !Array.isArray(value.capabilities)) {
    throw new TypeError("Invalid desktop runtime hello frame.");
  }
  assertGeneration(value.generation);
  if (value.appVersion !== expected.appVersion
      || value.coreVersion !== expected.coreVersion
      || value.buildId !== expected.buildId
      || value.sessionNonce !== expected.sessionNonce
      || value.generation !== expected.generation
      || value.capabilities.length !== DESKTOP_RUNTIME_METHODS.length
      || value.capabilities.some((method, index) => method !== DESKTOP_RUNTIME_METHODS[index])) {
    throw new TypeError("Desktop runtime identity is incompatible.");
  }
}

export function assertRuntimeRequestFrame(value: unknown): asserts value is RuntimeRequestFrame {
  if (!isRecord(value)
      || !hasExactKeys(value, ["kind", "runtimeProtocolVersion", "generation", "dispatchId", "envelope"])
      || value.kind !== "request"
      || value.runtimeProtocolVersion !== DESKTOP_RUNTIME_PROTOCOL_VERSION) {
    throw new TypeError("Invalid desktop runtime request frame.");
  }
  assertGeneration(value.generation);
  assertDispatchId(value.dispatchId);
  assertCoreRequestEnvelope(value.envelope);
  if (!isDesktopRuntimeMethod(value.envelope.method)) {
    throw new TypeError("Desktop runtime method is not allowed.");
  }
}

export function assertRuntimeResponseFrame(
  value: unknown,
  expected?: { dispatchId?: string; requestId?: string }
): asserts value is RuntimeResponseFrame {
  if (!isRecord(value)
      || !hasExactKeys(value, ["kind", "runtimeProtocolVersion", "generation", "dispatchId", "envelope"])
      || value.kind !== "response"
      || value.runtimeProtocolVersion !== DESKTOP_RUNTIME_PROTOCOL_VERSION) {
    throw new TypeError("Invalid desktop runtime response frame.");
  }
  assertGeneration(value.generation);
  assertDispatchId(value.dispatchId);
  if (expected?.dispatchId !== undefined && value.dispatchId !== expected.dispatchId) {
    throw new TypeError("Desktop runtime response dispatchId mismatch.");
  }
  assertCoreResponseEnvelope(value.envelope, expected?.requestId);
}

export function assertRuntimeOperationEventFrame(
  value: unknown,
  expected?: { dispatchId?: string; requestId?: string; operationId?: string }
): asserts value is RuntimeOperationEventFrame {
  if (!isRecord(value)
      || !hasExactKeys(value, ["kind", "runtimeProtocolVersion", "generation", "dispatchId", "envelope"])
      || value.kind !== "operation-event"
      || value.runtimeProtocolVersion !== DESKTOP_RUNTIME_PROTOCOL_VERSION) {
    throw new TypeError("Invalid desktop runtime operation event frame.");
  }
  assertGeneration(value.generation);
  assertDispatchId(value.dispatchId);
  if (expected?.dispatchId !== undefined && value.dispatchId !== expected.dispatchId) {
    throw new TypeError("Desktop runtime operation event dispatchId mismatch.");
  }
  assertCoreOperationEventEnvelope(value.envelope, expected?.requestId, expected?.operationId);
}

export function assertRuntimeCancelFrame(value: unknown): asserts value is RuntimeCancelFrame {
  if (!isRecord(value)
      || !hasExactKeys(value, [
        "kind", "runtimeProtocolVersion", "generation", "dispatchId", "requestId",
        ...(value.operationId === undefined ? [] : ["operationId"])
      ])
      || value.kind !== "cancel"
      || value.runtimeProtocolVersion !== DESKTOP_RUNTIME_PROTOCOL_VERSION
      || !isBoundedString(value.requestId, 512)
      || (value.operationId !== undefined
        && (typeof value.operationId !== "string"
          || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value.operationId)))) {
    throw new TypeError("Invalid desktop runtime cancel frame.");
  }
  assertGeneration(value.generation);
  assertDispatchId(value.dispatchId);
}

export function assertRuntimeShutdownFrame(value: unknown): asserts value is RuntimeShutdownFrame {
  if (!isRecord(value)
      || !hasExactKeys(value, ["kind", "runtimeProtocolVersion", "generation"])
      || value.kind !== "shutdown"
      || value.runtimeProtocolVersion !== DESKTOP_RUNTIME_PROTOCOL_VERSION) {
    throw new TypeError("Invalid desktop runtime shutdown frame.");
  }
  assertGeneration(value.generation);
}

export function createRuntimeRequestFrame<M extends DesktopRuntimeMethod>(
  generation: number,
  dispatchId: string,
  envelope: CoreRequestEnvelope<M>
): RuntimeRequestFrame<M> {
  const frame: RuntimeRequestFrame<M> = {
    kind: "request",
    runtimeProtocolVersion: DESKTOP_RUNTIME_PROTOCOL_VERSION,
    generation,
    dispatchId,
    envelope
  };
  assertRuntimeRequestFrame(frame);
  return frame;
}

export function createRuntimeResponseFrame<M extends DesktopRuntimeMethod>(
  generation: number,
  dispatchId: string,
  envelope: CoreResponseEnvelope<M>
): RuntimeResponseFrame<M> {
  const frame: RuntimeResponseFrame<M> = {
    kind: "response",
    runtimeProtocolVersion: DESKTOP_RUNTIME_PROTOCOL_VERSION,
    generation,
    dispatchId,
    envelope
  };
  assertRuntimeResponseFrame(frame);
  return frame;
}

export function createRuntimeOperationEventFrame(
  generation: number,
  dispatchId: string,
  envelope: CoreOperationEventEnvelope
): RuntimeOperationEventFrame {
  const frame: RuntimeOperationEventFrame = {
    kind: "operation-event",
    runtimeProtocolVersion: DESKTOP_RUNTIME_PROTOCOL_VERSION,
    generation,
    dispatchId,
    envelope
  };
  assertRuntimeOperationEventFrame(frame);
  return frame;
}

export function createRuntimeCancelFrame(
  generation: number,
  dispatchId: string,
  requestId: string,
  operationId?: string
): RuntimeCancelFrame {
  const frame: RuntimeCancelFrame = {
    kind: "cancel",
    runtimeProtocolVersion: DESKTOP_RUNTIME_PROTOCOL_VERSION,
    generation,
    dispatchId,
    requestId,
    ...(operationId ? { operationId } : {})
  };
  assertRuntimeCancelFrame(frame);
  return frame;
}

export function isRuntimeCoreMethod(value: CoreMethodName): value is DesktopRuntimeMethod {
  return isDesktopRuntimeMethod(value);
}
