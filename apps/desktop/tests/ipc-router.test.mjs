import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  createCoreOperationStartedEnvelope,
  createCoreProgressEnvelope,
  createCoreRequestEnvelope,
  createCoreSuccessEnvelope
} from "@codex-provider-sync/contracts";

import { registerDesktopIpc } from "../dist/main/ipc-router.js";
import { DesktopDiagnosticsExporter } from "../dist/main/diagnostics-export.js";
import { DESKTOP_IPC_CHANNELS } from "../dist/shared/constants.js";

const profile = { profileId: "default", profileRevision: "r1" };
const operationId = "11111111-1111-4111-8111-111111111111";

function planResult(request) {
  const operation = request.method === "prepareSwitch"
    ? "switch"
    : request.method === "prepareRestore"
      ? "restore"
      : "sync";
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
    ...(operation === "restore" ? { backupRevision: "backup" } : {}),
    target: operation === "restore"
      ? { backupId: request.payload.backupId }
      : { provider: operation === "sync" ? "openai" : request.payload.provider },
    impact: { backupExpected: true },
    warnings: [],
    requiresConfirmation: true
  };
}

function harness({
  holdApply = false,
  diagnosticsSnapshot = null,
  diagnosticsExporter = null,
  diagnosticsTarget = "D:\\safe\\diagnostics.zip",
  updateRestartPending = false
} = {}) {
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
  const diagnosticExports = [];
  let selectedTargets = 0;
  let updateCalls = 0;
  const pendingApplies = [];
  const listeners = new Set();
  const activeWatchCounts = [];
  const supervisor = {
    snapshot: {
      state: "ready",
      generation: 1,
      recoveryBlocked: false,
      writeInProgress: false,
      lastHandshakeAt: null
    },
    subscribeOperation(listener) { listeners.add(listener); return () => listeners.delete(listener); },
    async request(request) {
      calls.push({ kind: "read", request });
      if (request.method === "getDiagnostics") {
        const validSnapshot = {
          schemaVersion: 1,
          generatedAt: "2026-08-26T00:00:00.000Z",
          runtime: { node: "v24", platform: "win32", arch: "x64" },
          storage: { sqliteHomeSource: "default", stateDbFound: true, sqliteSupported: true },
          provider: {
            current: "openai",
            implicit: false,
            configured: ["openai"],
            rolloutCounts: { sessions: {}, archived_sessions: {} },
            sqliteCounts: { sessions: {}, archived_sessions: {} }
          },
          safety: {
            pendingRecovery: false,
            pendingTransactions: [],
            operationInProgress: null,
            rolloutScanComplete: true,
            lockedRolloutCount: 0,
            projectThreadVisibilityAvailable: true
          }
        };
        const envelope = createCoreSuccessEnvelope(request, validSnapshot);
        return diagnosticsSnapshot ? { ...envelope, result: diagnosticsSnapshot } : envelope;
      }
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
    async requestManaged(request, selectedProfile, options) {
      calls.push({ kind: "managed", request, profile: selectedProfile, options });
      if (request.method === "prepareRestore") {
        return createCoreSuccessEnvelope(request, planResult(request));
      }
      if (request.method === "applyRestore") {
        for (const listener of listeners) listener(createCoreOperationStartedEnvelope(
          request.requestId,
          operationId,
          "restore"
        ));
        return createCoreSuccessEnvelope(request, {
          schemaVersion: 1,
          operationId,
          operation: "restore",
          outcome: "completed",
          backup: { backupId: "pre-restore-snapshot" },
          warnings: [],
          result: { sourceBackupId: "managed" }
        }, operationId);
      }
      if (request.method === "pruneBackups") {
        return createCoreSuccessEnvelope(request, {
          deletedCount: 1,
          remainingCount: request.payload.keepCount,
          freedBytes: 1024
        });
      }
      const watch = {
        schemaVersion: 1,
        watchId: request.payload.watchId ?? "22222222-2222-4222-8222-222222222222",
        status: request.method === "stopWatch" ? "stopped" : "running",
        startedAt: "2026-08-26T00:00:00.000Z",
        stoppedAt: request.method === "stopWatch" ? "2026-08-26T00:01:00.000Z" : null,
        stopReason: request.method === "stopWatch" ? "manual" : null,
        includeStateDb: true,
        once: false
      };
      return createCoreSuccessEnvelope(
        request,
        request.method === "getWatchStatus" && !request.payload.watchId
          ? { schemaVersion: 1, watches: [watch] }
          : watch
      );
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
    supervisor,
    diagnosticsExporter: diagnosticsExporter ?? {
      authorizeTarget(target) {
        assert.equal(target, diagnosticsTarget);
        return "main-only-token";
      },
      async export(token, snapshot) {
        diagnosticExports.push({ token, snapshot });
        return {
          schemaVersion: 1,
          status: "created",
          artifactId: "33333333-3333-4333-8333-333333333333",
          createdAt: "2026-08-26T00:00:00.000Z"
        };
      }
    },
    async selectDiagnosticsTarget() {
      selectedTargets += 1;
      return diagnosticsTarget;
    },
    updates: {
      restartPending: updateRestartPending,
      get status() {
        updateCalls += 1;
        return {
          schemaVersion: 2,
          state: "disabled",
          reason: "not-configured",
          installAllowed: false
        };
      },
      async check() {
        updateCalls += 1;
        return { schemaVersion: 2, state: "checking", installAllowed: false };
      },
      async download() {
        updateCalls += 1;
        return { schemaVersion: 2, state: "downloading", version: "1.0.1", progressPercent: 0, installAllowed: false };
      },
      async install() {
        updateCalls += 1;
        return { schemaVersion: 2, state: "installing", version: "1.0.1", installAllowed: false };
      }
    },
    onActiveWatchCountChanged(count) { activeWatchCounts.push(count); }
  });
  return {
    handlers,
    event,
    calls,
    cancellations,
    pendingApplies,
    sent,
    supervisor,
    diagnosticExports,
    activeWatchCounts,
    get selectedTargets() { return selectedTargets; },
    get updateCalls() { return updateCalls; },
    cleanup
  };
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

test("Restore Prepare and Apply use a one-time Main-owned recovery plan", async () => {
  const value = harness();
  try {
    const prepared = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreRestore)(
      value.event,
      createCoreRequestEnvelope("prepareRestore", {
        profile,
        backupId: "managed",
        restoreConfig: true,
        restoreDatabase: true,
        restoreSessions: true
      }, "restore-prepare")
    );
    assert.equal(prepared.ok, true);
    assert.equal(prepared.result.operation, "restore");
    const apply = createCoreRequestEnvelope(
      "applyRestore",
      { schemaVersion: 1, planId: prepared.result.planId },
      "restore-apply"
    );
    const applied = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreRestore)(value.event, apply);
    assert.equal(applied.ok, true);
    assert.equal(applied.result.operation, "restore");
    assert.equal(value.sent[0].value.operation, "restore");
    assert.equal(value.calls[0].options.allowRecoveryBlocked, true);
    assert.equal(value.calls[1].options.allowRecoveryBlocked, true);
    assert.equal(
      (await value.handlers.get(DESKTOP_IPC_CHANNELS.coreRestore)(value.event, apply)).error.code,
      "PLAN_EXPIRED"
    );
  } finally { value.cleanup(); }
});

test("Maintenance owns Watch IDs and keeps Prune available during recovery", async () => {
  const value = harness();
  try {
    const prune = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreMaintenance)(
      value.event,
      createCoreRequestEnvelope("pruneBackups", { profile, keepCount: 5 }, "prune")
    );
    assert.equal(prune.ok, true);
    assert.equal(value.calls[0].options.allowRecoveryBlocked, true);
    const started = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreMaintenance)(
      value.event,
      createCoreRequestEnvelope("startWatch", { profile, includeStateDb: true }, "watch-start")
    );
    assert.equal(started.result.status, "running");
    assert.equal(value.calls[1].options.allowRecoveryBlocked, false);
    const status = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreMaintenance)(
      value.event,
      createCoreRequestEnvelope("getWatchStatus", { watchId: started.result.watchId }, "watch-status")
    );
    assert.equal(status.ok, true);
    assert.equal("isWrite" in value.calls[2].options, false);
    const evilFrame = { url: "cps-app://evil/index.html" };
    const denied = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreMaintenance)(
      { sender: value.event.sender, senderFrame: evilFrame },
      createCoreRequestEnvelope("stopWatch", { watchId: started.result.watchId }, "watch-evil-stop")
    );
    assert.equal(denied.error.code, "PERMISSION_DENIED");
    const stopped = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreMaintenance)(
      value.event,
      createCoreRequestEnvelope("stopWatch", { watchId: started.result.watchId }, "watch-stop")
    );
    assert.equal(stopped.result.status, "stopped");
    assert.equal(value.calls[3].options.allowRecoveryBlocked, true);
  } finally { value.cleanup(); }
});

test("Watch status reconciliation removes an autonomously stopped Watch from Main ownership", async () => {
  const value = harness();
  try {
    const started = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreMaintenance)(
      value.event,
      createCoreRequestEnvelope("startWatch", { profile, includeStateDb: true }, "watch-auto-start")
    );
    assert.equal(started.ok, true);
    assert.equal(value.activeWatchCounts.at(-1), 1);
    const originalRequestManaged = value.supervisor.requestManaged;
    value.supervisor.requestManaged = async (request, selectedProfile, options) => {
      if (request.method !== "getWatchStatus") {
        return originalRequestManaged(request, selectedProfile, options);
      }
      value.calls.push({ kind: "managed", request, profile: selectedProfile, options });
      return createCoreSuccessEnvelope(request, {
        schemaVersion: 1,
        watchId: started.result.watchId,
        status: "stopped",
        startedAt: "2026-08-26T00:00:00.000Z",
        stoppedAt: "2026-08-26T00:01:00.000Z",
        stopReason: "recovery-required",
        includeStateDb: true,
        once: false
      });
    };
    const status = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreMaintenance)(
      value.event,
      createCoreRequestEnvelope("getWatchStatus", { watchId: started.result.watchId }, "watch-auto-status")
    );
    assert.equal(status.ok, true);
    assert.equal(status.result.status, "stopped");
    assert.equal(value.activeWatchCounts.at(-1), 0);
    const staleStop = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreMaintenance)(
      value.event,
      createCoreRequestEnvelope("stopWatch", { watchId: started.result.watchId }, "watch-auto-stale-stop")
    );
    assert.equal(staleStop.ok, false);
    assert.equal(staleStop.error.code, "INVALID_INPUT");
  } finally { value.cleanup(); }
});

test("Diagnostics and Update IPC accept only pathless trusted product input", async () => {
  const value = harness();
  try {
    const exported = await value.handlers.get(DESKTOP_IPC_CHANNELS.diagnosticsExport)(
      value.event,
      { schemaVersion: 1, profile }
    );
    assert.equal(exported.status, "created");
    assert.equal("path" in exported, false);
    assert.equal(value.selectedTargets, 1);
    assert.equal(value.diagnosticExports.length, 1);
    assert.equal(value.diagnosticExports[0].token, "main-only-token");
    const denied = await value.handlers.get(DESKTOP_IPC_CHANNELS.diagnosticsExport)(
      value.event,
      { schemaVersion: 1, profile, path: "D:\\attacker.zip", token: "forged" }
    );
    assert.equal(denied.status, "failed");
    assert.equal(value.selectedTargets, 1);
    const update = await value.handlers.get(DESKTOP_IPC_CHANNELS.updateStatus)(value.event, null);
    assert.equal(update.reason, "not-configured");
    assert.equal(update.installAllowed, false);
    const forged = await value.handlers.get(DESKTOP_IPC_CHANNELS.updateStatus)(
      value.event,
      { url: "https://example.invalid", install: true }
    );
    assert.equal(forged.installAllowed, false);
    for (const [channel, expectedState] of [
      [DESKTOP_IPC_CHANNELS.updateCheck, "checking"],
      [DESKTOP_IPC_CHANNELS.updateDownload, "downloading"],
      [DESKTOP_IPC_CHANNELS.updateInstall, "installing"]
    ]) {
      const result = await value.handlers.get(channel)(value.event, null);
      assert.equal(result.state, expectedState);
      const rejected = await value.handlers.get(channel)(value.event, {
        url: "https://example.invalid",
        channel: "attacker",
        path: "D:\\update.exe"
      });
      assert.equal(rejected.state, "disabled");
    }
    assert.equal(value.updateCalls, 4);
  } finally { value.cleanup(); }
});

test("Update restart intent rejects every new Desktop write before Core dispatch", async () => {
  const value = harness({ updateRestartPending: true });
  try {
    for (const [channel, request] of [
      [DESKTOP_IPC_CHANNELS.coreSyncSwitch, createCoreRequestEnvelope("prepareSync", { profile }, "update-block-sync")],
      [DESKTOP_IPC_CHANNELS.coreRestore, createCoreRequestEnvelope("prepareRestore", {
        profile,
        backupId: "managed",
        restoreConfig: true,
        restoreDatabase: true,
        restoreSessions: true
      }, "update-block-restore")],
      [DESKTOP_IPC_CHANNELS.coreMaintenance, createCoreRequestEnvelope("startWatch", { profile, includeStateDb: true }, "update-block-watch")],
      [DESKTOP_IPC_CHANNELS.coreMaintenance, createCoreRequestEnvelope("pruneBackups", { profile, keepCount: 5 }, "update-block-prune")]
    ]) {
      const response = await value.handlers.get(channel)(value.event, request);
      assert.equal(response.ok, false);
      assert.equal(response.error.code, "OPERATION_BUSY");
      assert.equal(response.error.details.busyScope, "codex-home");
    }
    assert.equal(value.calls.length, 0);
  } finally { value.cleanup(); }
});

test("Diagnostics IPC rejects a hostile Runtime snapshot before creating an archive", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-desktop-diagnostics-ipc-"));
  const target = path.join(root, "diagnostics.zip");
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const value = harness({
    diagnosticsTarget: target,
    diagnosticsExporter: new DesktopDiagnosticsExporter({ appVersion: "test", isPackaged: false }),
    diagnosticsSnapshot: {
      schemaVersion: 1,
      generatedAt: "2026-08-26T00:00:00.000Z",
      runtime: { node: "v24", platform: "win32", arch: "x64" },
      storage: {
        sqliteHomeSource: "default",
        stateDbFound: true,
        sqliteSupported: true,
        path: "C:\\secret\\state_5.sqlite"
      },
      provider: {
        current: "openai",
        implicit: false,
        configured: ["openai"],
        rolloutCounts: { sessions: {}, archived_sessions: {} },
        sqliteCounts: { sessions: {}, archived_sessions: {} },
        message: "private message body"
      },
      safety: {
        pendingRecovery: false,
        pendingTransactions: [],
        operationInProgress: null,
        rolloutScanComplete: true,
        lockedRolloutCount: 0,
        projectThreadVisibilityAvailable: true,
        encrypted_content: "ciphertext"
      }
    }
  });
  try {
    const exported = await value.handlers.get(DESKTOP_IPC_CHANNELS.diagnosticsExport)(
      value.event,
      { schemaVersion: 1, profile }
    );
    assert.deepEqual(exported, {
      schemaVersion: 1,
      status: "failed",
      reason: "invalid-snapshot"
    });
    await assert.rejects(fs.access(target), { code: "ENOENT" });
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
