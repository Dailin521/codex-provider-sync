import assert from "node:assert/strict";
import test from "node:test";

import {
  CORE_METHODS,
  ContractValidationError,
  assertApplyPlanInput,
  assertCoreErrorDto,
  assertCoreMethodInput,
  assertCoreMethodOutput,
  assertCoreRequestEnvelope,
  assertCoreResponseEnvelope,
  assertProgressEvent,
  createPublicCoreErrorDto,
  createCoreRequestEnvelope
} from "../dist/index.js";

test("contract exposes the complete stable CoreClient method set", () => {
  assert.deepEqual(CORE_METHODS, [
    "getStatus",
    "prepareSync",
    "applySync",
    "prepareSwitch",
    "applySwitch",
    "listBackups",
    "prepareRestore",
    "applyRestore",
    "pruneBackups",
    "listHistory",
    "getHistorySession",
    "startWatch",
    "stopWatch",
    "getWatchStatus",
    "getDiagnostics"
  ]);
});

test("Apply input is opaque, versioned and exact", () => {
  assert.doesNotThrow(() => assertApplyPlanInput({ schemaVersion: 1, planId: "opaque" }));
  assert.throws(
    () => assertApplyPlanInput({ schemaVersion: 1, planId: "opaque", provider: "openai" }),
    ContractValidationError
  );
});

test("product inputs cannot carry paths or arbitrary apply/watch fields", () => {
  assert.doesNotThrow(() => assertCoreMethodInput("startWatch", {
    profile: { profileId: "default", profileRevision: "r1" },
    includeStateDb: true,
    debounceMs: 0,
    once: true
  }));
  assert.throws(() => assertCoreMethodInput("getStatus", {
    profile: { profileId: "default" },
    codexHome: "C:/private"
  }));
  assert.throws(() => assertCoreMethodInput("prepareRestore", {
    profile: { profileId: "default" },
    backupId: "managed",
    restoreConfig: true,
    restoreDatabase: true,
    restoreSessions: true,
    backupDir: "C:/private"
  }));
});

test("protocol mismatch fails before request business validation", () => {
  assert.throws(
    () => assertCoreRequestEnvelope({
      protocolVersion: 2,
      requestId: "request-1",
      method: "getStatus",
      payload: null
    }),
    (error) => error instanceof ContractValidationError
      && error.code === "PROTOCOL_VERSION_MISMATCH"
  );
});

test("request and response envelopes preserve request correlation", () => {
  const request = createCoreRequestEnvelope(
    "getStatus",
    { profile: { profileId: "default" } },
    "request-1"
  );
  assert.doesNotThrow(() => assertCoreRequestEnvelope(request));
  assert.doesNotThrow(() => assertCoreResponseEnvelope({
    protocolVersion: 1,
    requestId: "request-1",
    ok: true,
    result: {}
  }, "request-1"));
  assert.throws(() => assertCoreResponseEnvelope({
    protocolVersion: 1,
    requestId: "request-2",
    ok: true,
    result: {}
  }, "request-1"));
});

test("public errors use fixed messages and allowlisted details", () => {
  const busy = createPublicCoreErrorDto("OPERATION_BUSY", {
    details: {
      busyScope: "state-db",
      token: "must-not-cross",
      path: "C:/private"
    }
  });
  assert.deepEqual(busy, {
    code: "OPERATION_BUSY",
    message: "Another write operation is using the protected resource.",
    severity: "warning",
    retryable: true,
    recoveryRequired: false,
    details: { busyScope: "state-db" }
  });
  assert.doesNotThrow(() => assertCoreErrorDto(busy));
  assert.throws(() => assertCoreErrorDto({ ...busy, message: "private path" }));
  assert.throws(() => assertCoreErrorDto({ ...busy, suggestedAction: "token=secret" }));
  assert.throws(() => assertCoreErrorDto({ ...busy, details: { busyScope: "state-db", path: "private" } }));
});

test("method output guards reject structurally invalid successes", () => {
  const status = {
    schemaVersion: 1,
    snapshotAt: "2026-08-25T00:00:00.000Z",
    storageRevision: "storage",
    profile: { id: "default", revision: "r1" },
    currentProvider: "openai",
    rolloutCounts: { openai: 1 },
    sqliteCounts: { openai: 1 },
    codexHomeSource: "profile",
    sqliteHomeSource: "default",
    backupSummary: { count: 0, totalBytes: 0 },
    pendingRecovery: false,
    pendingTransactions: [],
    operationInProgress: null,
    rolloutScanComplete: true,
    lockedRolloutFiles: []
  };
  assert.throws(() => assertCoreMethodOutput("getStatus", null));
  assert.throws(() => assertCoreMethodOutput("getStatus", {
    schemaVersion: 1,
    snapshotAt: "2026-08-25T00:00:00.000Z",
    storageRevision: "storage",
    profile: { id: "default", revision: "r1" },
    currentProvider: "openai",
    lockedRolloutFiles: []
  }));
  assert.throws(() => assertCoreMethodOutput("prepareSync", { schemaVersion: 1 }));
  assert.throws(() => assertCoreMethodOutput("listBackups", { backups: [{ backupId: "b", sizeBytes: -1, metadata: {} }] }));
  assert.throws(() => assertCoreMethodOutput("getStatus", {
    ...status,
    pendingTransactions: [{ leak: () => "private" }]
  }));
  assert.throws(() => assertCoreMethodOutput("getStatus", {
    ...status,
    operationInProgress: { leak: () => "private" }
  }));
  assert.throws(() => assertCoreMethodOutput("getStatus", {
    ...status,
    codexHome: "C:/private"
  }));
  assert.throws(() => assertCoreMethodOutput("getDiagnostics", {
    schemaVersion: 1,
    generatedAt: "2026-08-25T00:00:00.000Z",
    runtime: { leak: () => "private" },
    storage: {},
    provider: {},
    safety: {}
  }));
  assert.doesNotThrow(() => assertCoreMethodOutput("getWatchStatus", {
    schemaVersion: 1,
    watches: []
  }));
});

test("ProgressEvent cannot carry messages, paths or diagnostics", () => {
  assert.doesNotThrow(() => assertProgressEvent({
    stage: "sqlite",
    status: "running",
    progress: 0.5,
    count: 2
  }));
  assert.throws(() => assertProgressEvent({
    stage: "history",
    status: "running",
    messageBody: "must not cross the progress channel"
  }));
});
