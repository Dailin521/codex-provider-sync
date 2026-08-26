import assert from "node:assert/strict";
import test from "node:test";

import {
  createCoreOperationStartedEnvelope,
  createCoreProgressEnvelope,
  createCoreRequestEnvelope,
  createCoreSuccessEnvelope
} from "@codex-provider-sync/contracts";

import { registerDesktopIpc } from "../dist/main/ipc-router.js";
import { DESKTOP_IPC_CHANNELS } from "../dist/shared/constants.js";

const profile = { profileId: "default", profileRevision: "r1" };
const operationId = "11111111-1111-4111-8111-111111111111";

function planResult(request) {
  const operation = request.method === "prepareSwitch" ? "switch" : "sync";
  const planId = `${operation}-${Buffer.from(request.requestId).toString("base64url")}`
    .padEnd(40, "p")
    .slice(0, 128);
  return {
    schemaVersion: 1,
    planId,
    operation,
    createdAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + 10 * 60_000).toISOString(),
    profile: { id: profile.profileId, revision: profile.profileRevision },
    storageRevision: "storage",
    configRevision: "config",
    rolloutRevision: "rollout",
    stateDbRevision: "db",
    target: { provider: operation === "sync" ? "openai" : request.payload.provider },
    impact: { backupExpected: true },
    warnings: [],
    requiresConfirmation: true
  };
}

function harness({ holdApply = false } = {}) {
  const handlers = new Map();
  const ipcMain = {
    handle(channel, handler) {
      assert.equal(handlers.has(channel), false);
      handlers.set(channel, handler);
    },
    removeHandler(channel) { handlers.delete(channel); }
  };
  const frame = { url: "cps-app://app/index.html" };
  const sent = [];
  const webContents = {
    id: 7,
    mainFrame: frame,
    send(channel, value) { sent.push({ channel, value }); }
  };
  const window = { webContents, isDestroyed: () => false };
  const event = { sender: webContents, senderFrame: frame };
  const calls = [];
  const cancellations = [];
  const pendingApplies = [];
  const listeners = new Set();
  const supervisor = {
    snapshot: { state: "ready", generation: 1, recoveryBlocked: false, lastHandshakeAt: null },
    subscribeOperation(listener) { listeners.add(listener); return () => listeners.delete(listener); },
    async request(request) {
      calls.push({ kind: "read", request });
      return createCoreSuccessEnvelope(request, {
        schemaVersion: 1,
        snapshotAt: "2026-08-26T00:00:00.000Z",
        storageRevision: "storage",
        profile: { id: "default", revision: "r1" },
        currentProvider: "openai",
        rolloutCounts: {},
        sqliteCounts: {},
        codexHomeSource: "profile",
        sqliteHomeSource: "default",
        backupSummary: { count: 0, totalBytes: 0 },
        pendingRecovery: false,
        pendingTransactions: [],
        operationInProgress: null,
        rolloutScanComplete: true,
        lockedRolloutFiles: []
      });
    },
    async requestWrite(request, selectedProfile) {
      calls.push({ kind: "write", request, profile: selectedProfile });
      if (request.method === "prepareSync" || request.method === "prepareSwitch") {
        return createCoreSuccessEnvelope(request, planResult(request));
      }
      const operation = request.method === "applySync" ? "sync" : "switch";
      for (const listener of listeners) listener(createCoreOperationStartedEnvelope(
        request.requestId,
        operationId,
        operation
      ));
      for (const listener of listeners) listener(createCoreProgressEnvelope(
        request.requestId,
        operationId,
        { stage: "create_backup", status: "start" }
      ));
      const complete = () => createCoreSuccessEnvelope(request, {
          schemaVersion: 1,
          operationId,
          operation,
          outcome: "completed",
          backup: { backupId: "managed-backup" },
          warnings: [],
          result: { targetProvider: "openai" }
        }, operationId);
      if (!holdApply) return complete();
      return new Promise((resolve) => pendingApplies.push(() => resolve(complete())));
    },
    cancel(requestId, selectedOperationId) {
      cancellations.push({ requestId, operationId: selectedOperationId });
      return true;
    }
  };
  const cleanup = registerDesktopIpc({
    ipcMain,
    getWindow: () => window,
    rendererOrigin: "cps-app://app",
    profiles: {
      list: () => [{
        id: "default",
        name: "Default",
        revision: "r1",
        codexHomeConfigured: true,
        sqliteHomeConfigured: false
      }]
    },
    supervisor
  });
  return { handlers, event, calls, cancellations, pendingApplies, sent, supervisor, cleanup };
}

test("read IPC accepts only a top-level local sender and a validated read method", async () => {
  const value = harness();
  try {
    const request = createCoreRequestEnvelope("getStatus", { profile }, "ipc-status");
    const response = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreRead)(value.event, request);
    assert.equal(response.ok, true);
    const evilFrame = { url: "cps-app://evil/index.html" };
    const denied = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreRead)(
      { sender: value.event.sender, senderFrame: evilFrame },
      request
    );
    assert.equal(denied.error.code, "PERMISSION_DENIED");
    assert.equal(value.calls.length, 1);
  } finally { value.cleanup(); }
});

test("Sync Prepare and same-method Apply use a one-time Main-owned plan", async () => {
  const value = harness();
  try {
    const prepare = createCoreRequestEnvelope(
      "prepareSync",
      { profile, keepCount: 5 },
      "ipc-prepare"
    );
    const prepared = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(value.event, prepare);
    assert.equal(prepared.ok, true);
    const apply = createCoreRequestEnvelope(
      "applySync",
      { schemaVersion: 1, planId: prepared.result.planId },
      "ipc-apply"
    );
    const applied = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(value.event, apply);
    assert.equal(applied.ok, true);
    assert.equal(applied.result.operationId, operationId);
    assert.deepEqual(value.sent.map((entry) => entry.channel), [
      DESKTOP_IPC_CHANNELS.operationEvent,
      DESKTOP_IPC_CHANNELS.operationEvent
    ]);
    const replay = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(value.event, apply);
    assert.equal(replay.ok, false);
    assert.equal(replay.error.code, "PLAN_EXPIRED");
    assert.deepEqual(value.calls.map((call) => call.request.method), ["prepareSync", "applySync"]);
  } finally { value.cleanup(); }
});

test("a Sync plan cannot be consumed by ApplySwitch", async () => {
  const value = harness();
  try {
    const prepared = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(
      value.event,
      createCoreRequestEnvelope("prepareSync", { profile, keepCount: 5 }, "prepare-cross")
    );
    const denied = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(
      value.event,
      createCoreRequestEnvelope(
        "applySwitch",
        { schemaVersion: 1, planId: prepared.result.planId },
        "apply-cross"
      )
    );
    assert.equal(denied.error.code, "PLAN_EXPIRED");
    assert.equal(value.calls.length, 1);
  } finally { value.cleanup(); }
});

test("Main bounds unconsumed prepared-plan ownership and expires the oldest plan", async () => {
  const value = harness();
  try {
    const prepared = [];
    for (let index = 0; index <= 256; index += 1) {
      prepared.push(await value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(
        value.event,
        createCoreRequestEnvelope(
          "prepareSync",
          { profile, keepCount: 5 },
          `bounded-plan-${index}`
        )
      ));
    }
    assert.equal(prepared.every((response) => response.ok), true);
    const expired = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(
      value.event,
      createCoreRequestEnvelope(
        "applySync",
        { schemaVersion: 1, planId: prepared[0].result.planId },
        "apply-evicted-plan"
      )
    );
    assert.equal(expired.ok, false);
    assert.equal(expired.error.code, "PLAN_EXPIRED");
    const latest = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(
      value.event,
      createCoreRequestEnvelope(
        "applySync",
        { schemaVersion: 1, planId: prepared.at(-1).result.planId },
        "apply-latest-plan"
      )
    );
    assert.equal(latest.ok, true);
  } finally { value.cleanup(); }
});

test("a concurrent Apply cannot reuse requestId or steal the first operation routing", async () => {
  const value = harness({ holdApply: true });
  try {
    const syncPlan = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(
      value.event,
      createCoreRequestEnvelope("prepareSync", { profile, keepCount: 5 }, "prepare-sync")
    );
    const switchPlan = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(
      value.event,
      createCoreRequestEnvelope("prepareSwitch", {
        profile,
        provider: "relay",
        modelMode: "keep-root-model",
        keepCount: 5
      }, "prepare-switch")
    );
    const first = value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(
      value.event,
      createCoreRequestEnvelope(
        "applySync",
        { schemaVersion: 1, planId: syncPlan.result.planId },
        "duplicate-apply"
      )
    );
    await new Promise((resolve) => setImmediate(resolve));
    const duplicate = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(
      value.event,
      createCoreRequestEnvelope(
        "applySwitch",
        { schemaVersion: 1, planId: switchPlan.result.planId },
        "duplicate-apply"
      )
    );
    assert.equal(duplicate.ok, false);
    assert.equal(duplicate.error.code, "INVALID_INPUT");
    assert.deepEqual(
      await value.handlers.get(DESKTOP_IPC_CHANNELS.operationCancel)(value.event, {
        requestId: "duplicate-apply",
        operationId
      }),
      { accepted: true }
    );
    assert.deepEqual(value.cancellations, [{ requestId: "duplicate-apply", operationId }]);
    value.pendingApplies[0]();
    assert.equal((await first).ok, true);
    assert.equal(value.sent.length, 2);
  } finally { value.cleanup(); }
});

test("channels reject cross-capability methods, protocol drift, operationId and oversized payloads", async () => {
  const value = harness();
  try {
    const write = createCoreRequestEnvelope("prepareSync", { profile, keepCount: 5 }, "write-on-read");
    assert.equal(
      (await value.handlers.get(DESKTOP_IPC_CHANNELS.coreRead)(value.event, write)).error.code,
      "PERMISSION_DENIED"
    );
    const restore = createCoreRequestEnvelope(
      "prepareRestore",
      { profile, backupId: "managed", restoreConfig: true, restoreDatabase: true, restoreSessions: true },
      "restore-on-write"
    );
    assert.equal(
      (await value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(value.event, restore)).error.code,
      "PERMISSION_DENIED"
    );
    const withOperationId = createCoreRequestEnvelope(
      "prepareSync",
      { profile, keepCount: 5 },
      "forged-operation",
      operationId
    );
    assert.equal(
      (await value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(value.event, withOperationId)).error.code,
      "PERMISSION_DENIED"
    );
    const mismatch = { ...createCoreRequestEnvelope("getStatus", { profile }, "bad-version"), protocolVersion: 99 };
    assert.equal(
      (await value.handlers.get(DESKTOP_IPC_CHANNELS.coreRead)(value.event, mismatch)).error.code,
      "PROTOCOL_VERSION_MISMATCH"
    );
    const oversized = {
      ...createCoreRequestEnvelope("prepareSwitch", {
        profile,
        provider: "relay",
        modelMode: "explicit",
        model: "x",
        keepCount: 5
      }, "oversized"),
      payload: {
        profile,
        provider: "relay",
        modelMode: "explicit",
        model: "x".repeat(70 * 1024),
        keepCount: 5
      }
    };
    assert.equal(
      (await value.handlers.get(DESKTOP_IPC_CHANNELS.coreSyncSwitch)(value.event, oversized)).error.code,
      "INVALID_INPUT"
    );
    assert.equal(value.calls.length, 0);
  } finally { value.cleanup(); }
});

test("cancel IPC is fixed-schema and sender-bound", async () => {
  const value = harness();
  try {
    const accepted = await value.handlers.get(DESKTOP_IPC_CHANNELS.operationCancel)(value.event, {
      requestId: "not-active",
      operationId
    });
    assert.deepEqual(accepted, { accepted: false });
    const malformed = await value.handlers.get(DESKTOP_IPC_CHANNELS.operationCancel)(value.event, {
      requestId: "not-active",
      operationId,
      path: "C:/private"
    });
    assert.deepEqual(malformed, { accepted: false });
    assert.equal(value.cancellations.length, 0);
  } finally { value.cleanup(); }
});

test("Profile IPC remains redacted and cleanup removes every registered channel", async () => {
  const value = harness();
  const response = await value.handlers.get(DESKTOP_IPC_CHANNELS.profilesList)(value.event, null);
  assert.equal("codexHome" in response.profiles[0], false);
  value.cleanup();
  assert.equal(value.handlers.size, 0);
});
