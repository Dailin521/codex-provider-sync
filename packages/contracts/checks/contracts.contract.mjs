import assert from "node:assert/strict";
import test from "node:test";

import {
  CORE_METHODS,
  ContractValidationError,
  assertApplyPlanInput,
  assertCoreErrorDto,
  assertCoreMethodInput,
  assertCoreMethodOutput,
  assertCoreOperationStartedEnvelope,
  assertCoreProgressEnvelope,
  assertCoreRequestEnvelope,
  assertCoreResponseEnvelope,
  assertProgressEvent,
  createPublicCoreErrorDto,
  createCoreOperationStartedEnvelope,
  createCoreProgressEnvelope,
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

test("DiagnosticsSnapshot is a recursive pathless allowlist", () => {
  const diagnostics = {
    schemaVersion: 1,
    generatedAt: "2026-08-27T00:00:00.000Z",
    runtime: { node: "v24.0.0", platform: "win32", arch: "x64" },
    storage: { sqliteHomeSource: "default", stateDbFound: true, sqliteSupported: true },
    provider: {
      current: "openai",
      implicit: false,
      configured: ["openai", "relay-v2"],
      rolloutCounts: { sessions: { openai: 2 }, archived_sessions: {} },
      sqliteCounts: { sessions: { openai: 2 }, archived_sessions: {}, unreadable: true }
    },
    safety: {
      storageRevision: "revision_1",
      pendingRecovery: true,
      pendingTransactions: [{
        operationId: "11111111-1111-4111-8111-111111111111",
        operationKind: "restore",
        state: "committed-pending-ack",
        sourceBackupId: "provider-sync-source",
        preRestoreSnapshotId: "restore-v2-snapshot"
      }],
      operationInProgress: {
        operationId: "22222222-2222-4222-8222-222222222222",
        operation: "restore",
        actor: "manual",
        startedAt: "2026-08-27T00:00:00.000Z",
        busyScope: "codex-home"
      },
      rolloutScanComplete: true,
      lockedRolloutCount: 0,
      projectThreadVisibilityAvailable: true
    }
  };
  assert.doesNotThrow(() => assertCoreMethodOutput("getDiagnostics", diagnostics));

  for (const [section, field] of [
    ["runtime", "path"],
    ["storage", "codexHome"],
    ["provider", "token"],
    ["safety", "message"]
  ]) {
    const mutated = structuredClone(diagnostics);
    mutated[section][field] = "C:/private/secret";
    assert.throws(() => assertCoreMethodOutput("getDiagnostics", mutated), `${section}.${field}`);
  }
  const pendingLeak = structuredClone(diagnostics);
  pendingLeak.safety.pendingTransactions[0].journalPath = "C:/private/journal";
  assert.throws(() => assertCoreMethodOutput("getDiagnostics", pendingLeak));
  const operationLeak = structuredClone(diagnostics);
  operationLeak.safety.operationInProgress.encrypted_content = "message-body";
  assert.throws(() => assertCoreMethodOutput("getDiagnostics", operationLeak));
  const providerPath = structuredClone(diagnostics);
  providerPath.provider.rolloutCounts.sessions["C:/private"] = 1;
  assert.throws(() => assertCoreMethodOutput("getDiagnostics", providerPath));
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

test("operation lifecycle envelopes are exact, correlated, and pathless", () => {
  const operationId = "11111111-1111-4111-8111-111111111111";
  const started = createCoreOperationStartedEnvelope("request-1", operationId, "sync");
  const progress = createCoreProgressEnvelope("request-1", operationId, {
    stage: "create_backup",
    status: "start",
    progress: 0.25,
    count: 1
  });
  assert.doesNotThrow(() => assertCoreOperationStartedEnvelope(
    started,
    "request-1",
    operationId
  ));
  assert.doesNotThrow(() => assertCoreProgressEnvelope(
    progress,
    "request-1",
    operationId
  ));
  assert.throws(() => assertCoreOperationStartedEnvelope(
    { ...started, path: "C:/private" }
  ));
  assert.throws(() => assertCoreProgressEnvelope({
    ...progress,
    progress: { ...progress.progress, backupDir: "C:/private" }
  }));
  assert.throws(() => assertCoreProgressEnvelope(progress, "request-2", operationId));
  assert.throws(() => assertCoreProgressEnvelope(
    progress,
    "request-1",
    "22222222-2222-4222-8222-222222222222"
  ));
});
