import assert from "node:assert/strict";
import test from "node:test";

import {
  createCoreOperationStartedEnvelope,
  createCoreProgressEnvelope,
  createCoreRequestEnvelope,
  createCoreSuccessEnvelope
} from "@codex-provider-sync/contracts";
import { DESKTOP_RUNTIME_METHODS } from "@codex-provider-sync/core-client";

import {
  DESKTOP_BUILD_ID,
  DESKTOP_CORE_PROTOCOL_VERSION,
  DESKTOP_CORE_VERSION,
  DESKTOP_RUNTIME_PROTOCOL_VERSION
} from "../dist/shared/constants.js";
import {
  assertRuntimeHelloFrame,
  assertRuntimeOperationEventFrame,
  assertRuntimeRequestFrame,
  assertRuntimeResponseFrame,
  createRuntimeOperationEventFrame,
  createRuntimeRequestFrame,
  createRuntimeResponseFrame
} from "../dist/shared/runtime-protocol.js";

const dispatchId = "22222222-2222-4222-8222-222222222222";
const operationId = "11111111-1111-4111-8111-111111111111";
const request = createCoreRequestEnvelope(
  "listBackups",
  { profile: { profileId: "default", profileRevision: "r1" } },
  "request-1"
);

test("runtime request and response frames require Main dispatch correlation", () => {
  const requestFrame = createRuntimeRequestFrame(1, dispatchId, request);
  const responseFrame = createRuntimeResponseFrame(
    1,
    dispatchId,
    createCoreSuccessEnvelope(request, { backups: [] })
  );
  assert.doesNotThrow(() => assertRuntimeRequestFrame(requestFrame));
  assert.doesNotThrow(() => assertRuntimeResponseFrame(responseFrame, {
    dispatchId,
    requestId: "request-1"
  }));
  assert.throws(() => assertRuntimeRequestFrame({ ...requestFrame, path: "C:/private" }));
  assert.throws(() => assertRuntimeResponseFrame(responseFrame, {
    dispatchId: "33333333-3333-4333-8333-333333333333",
    requestId: "request-1"
  }));
});

test("runtime operation events accept only strict pathless shared envelopes", () => {
  const started = createRuntimeOperationEventFrame(
    1,
    dispatchId,
    createCoreOperationStartedEnvelope("request-1", operationId, "sync")
  );
  const progress = createRuntimeOperationEventFrame(
    1,
    dispatchId,
    createCoreProgressEnvelope("request-1", operationId, {
      stage: "update_sqlite",
      status: "start",
      progress: 0.5,
      count: 2
    })
  );
  assert.doesNotThrow(() => assertRuntimeOperationEventFrame(started, {
    dispatchId,
    requestId: "request-1"
  }));
  assert.doesNotThrow(() => assertRuntimeOperationEventFrame(progress, {
    dispatchId,
    requestId: "request-1",
    operationId
  }));
  assert.throws(() => assertRuntimeOperationEventFrame({
    ...progress,
    envelope: {
      ...progress.envelope,
      progress: { ...progress.envelope.progress, backupDir: "C:/private" }
    }
  }));
});

test("runtime hello requires the exact C7 capability set and identity", () => {
  const identity = {
    appVersion: "0.5.0",
    coreVersion: DESKTOP_CORE_VERSION,
    buildId: DESKTOP_BUILD_ID,
    sessionNonce: "a".repeat(64),
    generation: 1
  };
  const hello = {
    kind: "hello",
    runtimeProtocolVersion: DESKTOP_RUNTIME_PROTOCOL_VERSION,
    coreProtocolVersion: DESKTOP_CORE_PROTOCOL_VERSION,
    ...identity,
    capabilities: DESKTOP_RUNTIME_METHODS
  };
  assert.doesNotThrow(() => assertRuntimeHelloFrame(hello, identity));
  assert.throws(() => assertRuntimeHelloFrame({
    ...hello,
    capabilities: DESKTOP_RUNTIME_METHODS.slice(0, 5)
  }, identity));
  assert.throws(() => assertRuntimeHelloFrame({
    ...hello,
    capabilities: [...DESKTOP_RUNTIME_METHODS, "prepareRestore"]
  }, identity));
});
