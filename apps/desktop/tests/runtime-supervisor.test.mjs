import assert from "node:assert/strict";
import test from "node:test";

import {
  createCoreFailureEnvelope,
  createCoreOperationStartedEnvelope,
  createCoreProgressEnvelope,
  createCoreRequestEnvelope,
  createCoreSuccessEnvelope,
  createPublicCoreErrorDto
} from "@codex-provider-sync/contracts";
import { DESKTOP_RUNTIME_METHODS } from "@codex-provider-sync/core-client";

import { CoreRuntimeSupervisor } from "../dist/main/runtime-supervisor.js";
import {
  DESKTOP_CORE_PROTOCOL_VERSION,
  DESKTOP_RUNTIME_PROTOCOL_VERSION
} from "../dist/shared/constants.js";
import {
  createRuntimeOperationEventFrame,
  createRuntimeResponseFrame
} from "../dist/shared/runtime-protocol.js";

const profile = { profileId: "default", profileRevision: "profile-r1" };
const operationId = "11111111-1111-4111-8111-111111111111";

function statusResult({ pending = false, selectedProfile = profile } = {}) {
  return {
    schemaVersion: 1,
    snapshotAt: "2026-08-26T00:00:00.000Z",
    storageRevision: "storage-r1",
    profile: { id: selectedProfile.profileId, revision: selectedProfile.profileRevision },
    currentProvider: "openai",
    rolloutCounts: { sessions: { openai: 1 }, archived_sessions: {} },
    sqliteCounts: {},
    codexHomeSource: "profile",
    sqliteHomeSource: "default",
    backupSummary: { count: 0, totalBytes: 0 },
    pendingRecovery: pending,
    pendingTransactions: pending ? [{ operationId: "pending", state: "applying" }] : [],
    operationInProgress: null,
    rolloutScanComplete: true,
    lockedRolloutFiles: []
  };
}

function planResult(request) {
  const operation = request.method === "prepareSwitch"
    ? "switch"
    : request.method === "prepareRestore"
      ? "restore"
      : "sync";
  return {
    schemaVersion: 1,
    planId: "p".repeat(48),
    operation,
    createdAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + 10 * 60_000).toISOString(),
    profile: {
      id: request.payload.profile.profileId,
      revision: request.payload.profile.profileRevision
    },
    storageRevision: "storage-r1",
    configRevision: "config-r1",
    rolloutRevision: "rollout-r1",
    stateDbRevision: "db-r1",
    ...(operation === "restore" ? { backupRevision: "backup-r1" } : {}),
    target: operation === "restore"
      ? { backupId: request.payload.backupId }
      : { provider: operation === "sync" ? "openai" : request.payload.provider },
    impact: { backupExpected: true },
    warnings: [],
    requiresConfirmation: true
  };
}

class FakeUtility {
  constructor(identity, behavior = {}) {
    this.identity = identity;
    this.behavior = behavior;
    this.messages = [];
    this.messageListeners = new Set();
    this.exitListeners = new Set();
    this.exited = false;
    this.applyFrames = new Map();
    queueMicrotask(() => this.emitMessage({
      kind: "hello",
      runtimeProtocolVersion: DESKTOP_RUNTIME_PROTOCOL_VERSION,
      coreProtocolVersion: DESKTOP_CORE_PROTOCOL_VERSION,
      appVersion: behavior.badAppVersion ? "wrong-app" : identity.appVersion,
      coreVersion: identity.coreVersion,
      buildId: identity.buildId,
      sessionNonce: identity.sessionNonce,
      generation: identity.generation,
      capabilities: behavior.readOnlyHello
        ? DESKTOP_RUNTIME_METHODS.slice(0, 5)
        : DESKTOP_RUNTIME_METHODS
    }));
  }

  postMessage(frame) {
    this.messages.push(frame);
    if (frame.kind === "shutdown") {
      queueMicrotask(() => this.exit());
      return;
    }
    if (frame.kind === "cancel") {
      const requestFrame = this.applyFrames.get(frame.dispatchId);
      if (!requestFrame || this.behavior.ignoreCancel) return;
      const response = createCoreFailureEnvelope(
        requestFrame.envelope,
        createPublicCoreErrorDto("OPERATION_CANCELLED", { operationId }),
        operationId
      );
      queueMicrotask(() => this.emitMessage(createRuntimeResponseFrame(
        frame.generation,
        frame.dispatchId,
        response
      )));
      return;
    }
    if (frame.kind !== "request") return;
    if (this.behavior.crashOnRequest?.(frame.envelope)) {
      queueMicrotask(() => this.exit());
      return;
    }
    if (this.behavior.holdRequests) return;
    const respond = () => {
      const request = frame.envelope;
      if (request.method === "getStatus") {
        const response = createCoreSuccessEnvelope(request, statusResult({
          pending: this.behavior.pendingStatus === true
            || this.behavior.pendingProfiles?.includes(request.payload.profile.profileId),
          selectedProfile: this.behavior.wrongStatusProfile
            ? { profileId: "wrong", profileRevision: "wrong" }
            : request.payload.profile
        }));
        this.emitMessage(createRuntimeResponseFrame(frame.generation, frame.dispatchId, response));
        return;
      }
      if (request.method === "listBackups") {
        const response = createCoreSuccessEnvelope(request, { backups: [] });
        this.emitMessage(createRuntimeResponseFrame(frame.generation, frame.dispatchId, response));
        return;
      }
      if (request.method === "prepareSync"
          || request.method === "prepareSwitch"
          || request.method === "prepareRestore") {
        const response = createCoreSuccessEnvelope(request, planResult(request));
        this.emitMessage(createRuntimeResponseFrame(frame.generation, frame.dispatchId, response));
        return;
      }
      if (request.method === "applySync"
          || request.method === "applySwitch"
          || request.method === "applyRestore") {
        if (this.behavior.holdApplyBeforeStart) return;
        if (this.behavior.failApplyBeforeStart) {
          const response = createCoreFailureEnvelope(
            request,
            createPublicCoreErrorDto("PLAN_EXPIRED")
          );
          this.emitMessage(createRuntimeResponseFrame(frame.generation, frame.dispatchId, response));
          return;
        }
        this.applyFrames.set(frame.dispatchId, frame);
        const operation = request.method === "applySync"
          ? "sync"
          : request.method === "applySwitch"
            ? "switch"
            : "restore";
        this.emitMessage(createRuntimeOperationEventFrame(
          frame.generation,
          frame.dispatchId,
          createCoreOperationStartedEnvelope(request.requestId, operationId, operation)
        ));
        this.emitMessage(createRuntimeOperationEventFrame(
          frame.generation,
          frame.dispatchId,
          createCoreProgressEnvelope(request.requestId, operationId, {
            stage: "create_backup",
            status: "start"
          })
        ));
        if (this.behavior.holdApply) return;
        const response = createCoreSuccessEnvelope(request, {
          schemaVersion: 1,
          operationId,
          operation,
          outcome: "completed",
          backup: { backupId: "managed-backup" },
          warnings: [],
          result: { targetProvider: "openai" }
        }, operationId);
        this.emitMessage(createRuntimeResponseFrame(frame.generation, frame.dispatchId, response));
        return;
      }
      if (request.method === "pruneBackups") {
        this.emitMessage(createRuntimeResponseFrame(
          frame.generation,
          frame.dispatchId,
          createCoreSuccessEnvelope(request, {
            deletedCount: 0,
            remainingCount: request.payload.keepCount,
            freedBytes: 0
          })
        ));
        return;
      }
      if (request.method === "startWatch"
          || request.method === "stopWatch"
          || request.method === "getWatchStatus") {
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
        this.emitMessage(createRuntimeResponseFrame(
          frame.generation,
          frame.dispatchId,
          createCoreSuccessEnvelope(
            request,
            request.method === "getWatchStatus" && !request.payload.watchId
              ? { schemaVersion: 1, watches: [watch] }
              : watch
          )
        ));
      }
    };
    if (this.behavior.responseDelayMs) setTimeout(respond, this.behavior.responseDelayMs);
    else queueMicrotask(respond);
  }

  kill() { this.exit(); }
  onMessage(listener) { this.messageListeners.add(listener); return () => this.messageListeners.delete(listener); }
  onExit(listener) { this.exitListeners.add(listener); return () => this.exitListeners.delete(listener); }
  emitMessage(frame) { for (const listener of [...this.messageListeners]) listener(frame); }
  exit() {
    if (this.exited) return;
    this.exited = true;
    for (const listener of [...this.exitListeners]) listener();
  }
}

function readRequest(method, requestId, selectedProfile = profile) {
  return createCoreRequestEnvelope(method, { profile: selectedProfile }, requestId);
}

function prepareRequest(requestId = "prepare-1") {
  return createCoreRequestEnvelope(
    "prepareSync",
    { profile, keepCount: 5 },
    requestId
  );
}

function applyRequest(requestId = "apply-1") {
  return createCoreRequestEnvelope(
    "applySync",
    { schemaVersion: 1, planId: "p".repeat(48) },
    requestId
  );
}

function restorePrepareRequest(requestId = "restore-prepare") {
  return createCoreRequestEnvelope("prepareRestore", {
    profile,
    backupId: "managed",
    restoreConfig: true,
    restoreDatabase: true,
    restoreSessions: true
  }, requestId);
}

function restoreApplyRequest(requestId = "restore-apply") {
  return createCoreRequestEnvelope(
    "applyRestore",
    { schemaVersion: 1, planId: "p".repeat(48) },
    requestId
  );
}

test("runtime handshake completes before the first read", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) { const child = new FakeUtility(identity); children.push(child); return child; }
  });
  const response = await supervisor.request(readRequest("getStatus", "status-1"));
  assert.equal(response.ok, true);
  assert.equal(children.length, 1);
  assert.deepEqual(children[0].messages.map((frame) => frame.envelope?.method), ["getStatus"]);
  assert.equal(supervisor.snapshot.generation, 1);
});

test("first cold-start write preflights Status before Prepare", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) { const child = new FakeUtility(identity); children.push(child); return child; }
  });
  const response = await supervisor.requestWrite(prepareRequest(), profile);
  assert.equal(response.ok, true);
  assert.deepEqual(
    children[0].messages.filter((frame) => frame.kind === "request").map((frame) => frame.envelope.method),
    ["getStatus", "prepareSync"]
  );
});

test("restart safety force-preflights every known profile and fails closed", async () => {
  const second = { profileId: "secondary", profileRevision: "profile-r2" };
  for (const scenario of ["clear", "blocked", "unverifiable"]) {
    const children = [];
    const supervisor = new CoreRuntimeSupervisor({
      appVersion: "0.5.0",
      spawnUtility(identity) {
        const child = new FakeUtility(identity, scenario === "blocked"
          ? { pendingProfiles: [second.profileId] }
          : scenario === "unverifiable"
            ? { wrongStatusProfile: true }
            : {});
        children.push(child);
        return child;
      }
    });
    assert.equal(
      await supervisor.verifyProfilesSafeForRestart([profile, second]),
      scenario
    );
    const methods = children[0].messages
      .filter((frame) => frame.kind === "request")
      .map((frame) => frame.envelope.method);
    assert.equal(methods.every((method) => method === "getStatus"), true);
    assert.equal(methods.length, scenario === "unverifiable" ? 1 : 2);
    await supervisor.shutdown();
  }
});

test("pending recovery blocks cold-start writes but preserves reads", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) { const child = new FakeUtility(identity, { pendingStatus: true }); children.push(child); return child; }
  });
  const blocked = await supervisor.requestWrite(prepareRequest(), profile);
  assert.equal(blocked.ok, false);
  assert.equal(blocked.error.code, "PENDING_TRANSACTION");
  assert.equal((await supervisor.request(readRequest("listBackups", "read-after-block"))).ok, true);
  assert.deepEqual(
    children[0].messages.filter((frame) => frame.kind === "request").map((frame) => frame.envelope.method),
    ["getStatus", "listBackups"]
  );
});

test("Restore and Prune may converge recovery while starting Watch remains blocked", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity, { pendingStatus: true });
      children.push(child);
      return child;
    }
  });
  const restore = await supervisor.requestManaged(restorePrepareRequest(), profile, {
    allowRecoveryBlocked: true
  });
  assert.equal(restore.ok, true);
  const prune = await supervisor.requestManaged(
    createCoreRequestEnvelope("pruneBackups", { profile, keepCount: 5 }, "prune-recovery"),
    profile,
    { allowRecoveryBlocked: true }
  );
  assert.equal(prune.ok, true);
  const watch = await supervisor.requestManaged(
    createCoreRequestEnvelope("startWatch", { profile, includeStateDb: true }, "watch-recovery"),
    profile
  );
  assert.equal(watch.ok, false);
  assert.equal(watch.error.code, "PENDING_TRANSACTION");
  assert.equal(supervisor.snapshot.recoveryBlocked, true);
  assert.deepEqual(
    children[0].messages.filter((frame) => frame.kind === "request").map((frame) => frame.envelope.method),
    ["getStatus", "prepareRestore", "pruneBackups"]
  );
});

test("Restore Apply lifecycle is cancellable and blocks update installation while in flight", async () => {
  const children = [];
  const events = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity, { holdApply: true });
      children.push(child);
      return child;
    }
  });
  supervisor.subscribeOperation((event) => events.push(event));
  const applying = supervisor.requestManaged(restoreApplyRequest(), profile, {
    allowRecoveryBlocked: true
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(supervisor.snapshot.writeInProgress, true);
  assert.deepEqual(events.map((event) => [event.event, event.operation]), [
    ["operation-started", "restore"],
    ["progress", undefined]
  ]);
  assert.equal(supervisor.cancel("restore-apply", operationId), true);
  const response = await applying;
  assert.equal(response.ok, false);
  assert.equal(response.error.code, "OPERATION_CANCELLED");
  assert.equal(supervisor.snapshot.writeInProgress, false);
  assert.equal(children[0].messages.at(-1).kind, "cancel");
});

test("restart install gate drains an admitted Watch and rejects later managed writes", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity, { responseDelayMs: 25 });
      children.push(child);
      return child;
    }
  });
  const firstWatch = supervisor.requestManaged(
    createCoreRequestEnvelope("startWatch", { profile, includeStateDb: true }, "watch-before-restart"),
    profile
  );
  assert.equal(supervisor.snapshot.writeInProgress, true);
  const restartLease = supervisor.tryBeginRestartInstall();
  assert.ok(restartLease);
  let drained = false;
  const drain = restartLease.waitForWrites().then(() => { drained = true; });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(drained, false);
  const rejected = await supervisor.requestManaged(
    createCoreRequestEnvelope("startWatch", { profile, includeStateDb: true }, "watch-after-restart"),
    profile
  );
  assert.equal(rejected.ok, false);
  assert.equal(rejected.error.code, "OPERATION_BUSY");
  assert.equal(rejected.error.details.busyScope, "codex-home");
  assert.equal((await firstWatch).ok, true);
  await drain;
  assert.equal(drained, true);
  assert.equal(supervisor.snapshot.writeInProgress, false);
  restartLease.release();
  await supervisor.shutdown();
});

test("apply lifecycle is correlated and cancellation waits for the terminal response", async () => {
  const children = [];
  const events = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) { const child = new FakeUtility(identity, { holdApply: true }); children.push(child); return child; }
  });
  supervisor.subscribeOperation((event) => events.push(event));
  const applying = supervisor.requestWrite(applyRequest(), profile);
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(events.map((event) => event.event), ["operation-started", "progress"]);
  assert.equal(supervisor.cancel("apply-1", operationId), true);
  const response = await applying;
  assert.equal(response.ok, false);
  assert.equal(response.error.code, "OPERATION_CANCELLED");
  assert.equal(response.operationId, operationId);
  assert.equal(children[0].messages.at(-1).kind, "cancel");
});

test("cancel before operation-started rejects a forged operationId without killing Runtime", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity, { holdApplyBeforeStart: true });
      children.push(child);
      return child;
    }
  });
  const applying = supervisor.requestWrite(applyRequest("apply-before-start"), profile);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(supervisor.cancel("apply-before-start", operationId), false);
  assert.equal(children[0].messages.some((frame) => frame.kind === "cancel"), false);
  assert.equal(supervisor.snapshot.state, "ready");
  children[0].exit();
  assert.equal((await applying).error.code, "CORE_RUNTIME_CRASHED");
});

test("an Apply failure before operation-started remains a normal Core failure", async () => {
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) { return new FakeUtility(identity, { failApplyBeforeStart: true }); }
  });
  const response = await supervisor.requestWrite(applyRequest(), profile);
  assert.equal(response.ok, false);
  assert.equal(response.error.code, "PLAN_EXPIRED");
  assert.equal(response.operationId, undefined);
  assert.equal(supervisor.snapshot.state, "ready");
});

test("runtime crash rejects every pending request and next read restarts with preflight", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity, children.length === 0 ? { holdRequests: true } : {});
      children.push(child);
      return child;
    }
  });
  const first = supervisor.request(readRequest("getStatus", "pending-1"));
  const second = supervisor.request(readRequest("listBackups", "pending-2"));
  await new Promise((resolve) => setImmediate(resolve));
  children[0].exit();
  for (const response of await Promise.all([first, second])) {
    assert.equal(response.ok, false);
    assert.equal(response.error.code, "CORE_RUNTIME_CRASHED");
  }
  const recovered = await supervisor.request(readRequest("listBackups", "after-crash"));
  assert.equal(recovered.ok, true);
  assert.deepEqual(
    children[1].messages.filter((frame) => frame.kind === "request").map((frame) => frame.envelope.method),
    ["getStatus", "listBackups"]
  );
});

test("unknown dispatch operation event fails the generation closed", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) { const child = new FakeUtility(identity); children.push(child); return child; }
  });
  assert.equal((await supervisor.request(readRequest("getStatus", "activate"))).ok, true);
  children[0].emitMessage(createRuntimeOperationEventFrame(
    1,
    "22222222-2222-4222-8222-222222222222",
    createCoreOperationStartedEnvelope("unknown", operationId, "sync")
  ));
  assert.equal(supervisor.snapshot.state, "crashed");
});

test("incompatible read-only capability hello fails before C8 business dispatch", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    handshakeTimeoutMs: 100,
    spawnUtility(identity) { const child = new FakeUtility(identity, { readOnlyHello: true }); children.push(child); return child; }
  });
  const response = await supervisor.request(readRequest("getStatus", "bad-hello"));
  assert.equal(response.ok, false);
  assert.equal(response.error.code, "PROTOCOL_VERSION_MISMATCH");
  assert.equal(children[0].messages.length, 0);
});

test("read timeout kills its generation before a late response can alias", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    requestTimeoutMs: 5,
    spawnUtility(identity) {
      const child = new FakeUtility(identity, children.length === 0 ? { responseDelayMs: 25 } : {});
      children.push(child);
      return child;
    }
  });
  const timedOut = await supervisor.request(readRequest("getStatus", "late"));
  assert.equal(timedOut.ok, false);
  assert.equal(timedOut.error.code, "INTERNAL_ERROR");
  assert.equal(supervisor.snapshot.state, "crashed");
  await new Promise((resolve) => setTimeout(resolve, 35));
  assert.equal((await supervisor.request(readRequest("getStatus", "late"))).ok, true);
  assert.equal(children.length, 2);
});

test("write timeout is a Runtime crash, never a cancellation, and the restart preflights", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    writeRequestTimeoutMs: 5,
    spawnUtility(identity) {
      const child = new FakeUtility(
        identity,
        children.length === 0 ? { holdApplyBeforeStart: true } : {}
      );
      children.push(child);
      return child;
    }
  });
  const timedOut = await supervisor.requestWrite(applyRequest("write-timeout"), profile);
  assert.equal(timedOut.ok, false);
  assert.equal(timedOut.error.code, "CORE_RUNTIME_CRASHED");
  assert.equal(children[0].messages.some((frame) => frame.kind === "cancel"), false);
  assert.equal(supervisor.snapshot.state, "crashed");
  const recovered = await supervisor.request(readRequest("listBackups", "after-write-timeout"));
  assert.equal(recovered.ok, true);
  assert.equal(children.length, 2);
  assert.deepEqual(
    children[1].messages.filter((frame) => frame.kind === "request").map((frame) => frame.envelope.method),
    ["getStatus", "listBackups"]
  );
});

test("shutdown before activation permanently rejects later requests", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) { const child = new FakeUtility(identity); children.push(child); return child; }
  });
  await supervisor.shutdown();
  const response = await supervisor.request(readRequest("getStatus", "disposed"));
  assert.equal(response.ok, false);
  assert.equal(response.error.code, "INTERNAL_ERROR");
  assert.equal(children.length, 0);
});
