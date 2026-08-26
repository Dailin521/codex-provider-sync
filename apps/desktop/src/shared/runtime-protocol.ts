import {
  assertCoreRequestEnvelope,
  assertCoreResponseEnvelope,
  type CoreMethodName,
  type CoreRequestEnvelope,
  type CoreResponseEnvelope
} from "@codex-provider-sync/contracts";
import {
  DESKTOP_READ_METHODS,
  type DesktopReadMethod
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
  capabilities: readonly DesktopReadMethod[];
}

export interface RuntimeRequestFrame<M extends DesktopReadMethod = DesktopReadMethod> {
  kind: "request";
  runtimeProtocolVersion: typeof DESKTOP_RUNTIME_PROTOCOL_VERSION;
  generation: number;
  envelope: CoreRequestEnvelope<M>;
}

export interface RuntimeResponseFrame<M extends DesktopReadMethod = DesktopReadMethod> {
  kind: "response";
  runtimeProtocolVersion: typeof DESKTOP_RUNTIME_PROTOCOL_VERSION;
  generation: number;
  envelope: CoreResponseEnvelope<M>;
}

export interface RuntimeShutdownFrame {
  kind: "shutdown";
  runtimeProtocolVersion: typeof DESKTOP_RUNTIME_PROTOCOL_VERSION;
  generation: number;
}

export type RuntimeFrame = RuntimeHelloFrame | RuntimeRequestFrame | RuntimeResponseFrame | RuntimeShutdownFrame;

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

export function assertRuntimeHelloFrame(
  value: unknown,
  expected: ExpectedRuntimeIdentity
): asserts value is RuntimeHelloFrame {
  if (!isRecord(value)
      || !hasExactKeys(value, [
        "kind",
        "runtimeProtocolVersion",
        "coreProtocolVersion",
        "appVersion",
        "coreVersion",
        "buildId",
        "sessionNonce",
        "generation",
        "capabilities"
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
      || value.capabilities.length !== DESKTOP_READ_METHODS.length
      || value.capabilities.some((method, index) => method !== DESKTOP_READ_METHODS[index])) {
    throw new TypeError("Desktop runtime identity is incompatible.");
  }
}

export function assertRuntimeRequestFrame(value: unknown): asserts value is RuntimeRequestFrame {
  if (!isRecord(value)
      || !hasExactKeys(value, ["kind", "runtimeProtocolVersion", "generation", "envelope"])
      || value.kind !== "request"
      || value.runtimeProtocolVersion !== DESKTOP_RUNTIME_PROTOCOL_VERSION) {
    throw new TypeError("Invalid desktop runtime request frame.");
  }
  assertGeneration(value.generation);
  assertCoreRequestEnvelope(value.envelope);
  if (!DESKTOP_READ_METHODS.includes(value.envelope.method as DesktopReadMethod)) {
    throw new TypeError("Desktop runtime method is not read-only.");
  }
}

export function assertRuntimeResponseFrame(
  value: unknown,
  requestId?: string
): asserts value is RuntimeResponseFrame {
  if (!isRecord(value)
      || !hasExactKeys(value, ["kind", "runtimeProtocolVersion", "generation", "envelope"])
      || value.kind !== "response"
      || value.runtimeProtocolVersion !== DESKTOP_RUNTIME_PROTOCOL_VERSION) {
    throw new TypeError("Invalid desktop runtime response frame.");
  }
  assertGeneration(value.generation);
  assertCoreResponseEnvelope(value.envelope, requestId);
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

export function createRuntimeRequestFrame<M extends DesktopReadMethod>(
  generation: number,
  envelope: CoreRequestEnvelope<M>
): RuntimeRequestFrame<M> {
  return {
    kind: "request",
    runtimeProtocolVersion: DESKTOP_RUNTIME_PROTOCOL_VERSION,
    generation,
    envelope
  };
}

export function createRuntimeResponseFrame<M extends DesktopReadMethod>(
  generation: number,
  envelope: CoreResponseEnvelope<M>
): RuntimeResponseFrame<M> {
  return {
    kind: "response",
    runtimeProtocolVersion: DESKTOP_RUNTIME_PROTOCOL_VERSION,
    generation,
    envelope
  };
}

export function isReadOnlyCoreMethod(value: CoreMethodName): value is DesktopReadMethod {
  return DESKTOP_READ_METHODS.includes(value as DesktopReadMethod);
}
