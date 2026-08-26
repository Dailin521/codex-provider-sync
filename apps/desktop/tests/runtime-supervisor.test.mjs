import assert from "node:assert/strict";
import test from "node:test";

import {
  createCoreFailureEnvelope,
  createCoreRequestEnvelope,
  createCoreSuccessEnvelope,
  createPublicCoreErrorDto
} from "@codex-provider-sync/contracts";
import { DESKTOP_READ_METHODS } from "@codex-provider-sync/core-client";

import { CoreRuntimeSupervisor } from "../dist/main/runtime-supervisor.js";
import {
  DESKTOP_CORE_PROTOCOL_VERSION,
  DESKTOP_RUNTIME_PROTOCOL_VERSION
} from "../dist/shared/constants.js";
import { createRuntimeResponseFrame } from "../dist/shared/runtime-protocol.js";

function statusResult({ pending = false, profile = { profileId: "default", profileRevision: "profile-r1" } } = {}) {
  return {
    schemaVersion: 1,
    snapshotAt: "2026-08-26T00:00:00.000Z",
    storageRevision: "storage-r1",
    profile: { id: profile.profileId, revision: profile.profileRevision ?? "resolved-revision" },
    currentProvider: "openai",
    rolloutCounts: { sessions: { openai: 1 }, archived_sessions: {} },
    sqliteCounts: {},
    codexHomeSource: "profile",
    sqliteHomeSource: "default",
    backupSummary: { count: 0, totalBytes: 0 },
    pendingRecovery: pending,
    pendingTransactions: pending ? [{ operationId: "op-pending", state: "applying" }] : [],
    operationInProgress: null,
    rolloutScanComplete: true,
    lockedRolloutFiles: []
  };
}

function outputFor(request, options) {
  if (request.method === "getStatus") return statusResult({
    ...options,
    profile: options.wrongStatusProfile
      ? { profileId: "wrong-profile", profileRevision: "wrong-revision" }
      : request.payload.profile
  });
  if (request.method === "listBackups") return { backups: [] };
  throw new Error(`Unexpected fake method: ${request.method}`);
}

class FakeUtility {
  constructor(identity, behavior = {}) {
    this.identity = identity;
    this.behavior = behavior;
    this.messages = [];
    this.messageListeners = new Set();
    this.exitListeners = new Set();
    this.exited = false;
    queueMicrotask(() => this.emitMessage({
      kind: "hello",
      runtimeProtocolVersion: DESKTOP_RUNTIME_PROTOCOL_VERSION,
      coreProtocolVersion: DESKTOP_CORE_PROTOCOL_VERSION,
      appVersion: behavior.badAppVersion ? "wrong-app" : identity.appVersion,
      coreVersion: identity.coreVersion,
      buildId: identity.buildId,
      sessionNonce: identity.sessionNonce,
      generation: identity.generation,
      capabilities: DESKTOP_READ_METHODS
    }));
  }

  postMessage(frame) {
    this.messages.push(frame);
    if (frame.kind === "shutdown") {
      if (!this.behavior.holdShutdown) queueMicrotask(() => this.exit());
      return;
    }
    if (frame.kind !== "request") return;
    if (this.behavior.crashOnRequest?.(frame.envelope)) {
      queueMicrotask(() => this.exit());
      return;
    }
    if (this.behavior.holdRequests) return;
    const respond = () => {
      let envelope;
      if (frame.envelope.method === "getStatus" && this.behavior.failStatusCount > 0) {
        this.behavior.failStatusCount -= 1;
        envelope = createCoreFailureEnvelope(
          frame.envelope,
          createPublicCoreErrorDto("INTERNAL_ERROR")
        );
      } else {
        envelope = createCoreSuccessEnvelope(
          frame.envelope,
          outputFor(frame.envelope, {
            pending: this.behavior.pendingStatus === true,
            wrongStatusProfile: this.behavior.wrongStatusProfile === true
          }),
          this.behavior.wrongOperationId ? "wrong-operation" : undefined
        );
      }
      this.emitMessage(createRuntimeResponseFrame(frame.generation, envelope));
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

function request(method, requestId, profileId = "default", profileRevision = "profile-r1", operationId) {
  const profile = { profileId, profileRevision };
  return method === "getStatus"
    ? createCoreRequestEnvelope(method, { profile }, requestId, operationId)
    : createCoreRequestEnvelope(method, { profile }, requestId, operationId);
}

test("runtime handshake completes before the first read-only business request", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity);
      children.push(child);
      return child;
    }
  });
  const response = await supervisor.request(request("getStatus", "status-1"));
  assert.equal(response.ok, true);
  assert.equal(children.length, 1);
  assert.deepEqual(children[0].messages.map((frame) => frame.envelope?.method), ["getStatus"]);
  assert.equal(supervisor.snapshot.state, "ready");
  assert.equal(supervisor.snapshot.generation, 1);
});

test("runtime crash rejects every pending request with CORE_RUNTIME_CRASHED", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity, { holdRequests: true });
      children.push(child);
      return child;
    }
  });
  const first = supervisor.request(request("getStatus", "pending-1"));
  const second = supervisor.request(request("listBackups", "pending-2"));
  await new Promise((resolve) => setImmediate(resolve));
  children[0].exit();
  for (const response of await Promise.all([first, second])) {
    assert.equal(response.ok, false);
    assert.equal(response.error.code, "CORE_RUNTIME_CRASHED");
  }
  assert.equal(supervisor.snapshot.state, "crashed");
});

test("next request restarts once, preflights pending journals, then serves reads", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity, { pendingStatus: children.length === 1 });
      children.push(child);
      return child;
    }
  });
  assert.equal((await supervisor.request(request("getStatus", "initial"))).ok, true);
  children[0].exit();
  const response = await supervisor.request(request("listBackups", "after-crash"));
  assert.equal(response.ok, true);
  assert.equal(children.length, 2);
  assert.deepEqual(
    children[1].messages.map((frame) => frame.envelope?.method),
    ["getStatus", "listBackups"]
  );
  assert.equal(supervisor.snapshot.generation, 2);
  assert.equal(supervisor.snapshot.recoveryBlocked, true);
});

test("incompatible hello fails before business dispatch and does not restart-loop", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity, { badAppVersion: true });
      children.push(child);
      return child;
    },
    handshakeTimeoutMs: 100
  });
  const response = await supervisor.request(request("getStatus", "bad-hello"));
  assert.equal(response.ok, false);
  assert.equal(response.error.code, "PROTOCOL_VERSION_MISMATCH");
  assert.equal(children.length, 1);
  assert.equal(children[0].messages.length, 0);
  assert.equal(supervisor.snapshot.state, "crashed");
});

test("a crash during restart preflight is not retried inside the same request", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const behavior = children.length === 1
        ? { crashOnRequest: (envelope) => envelope.method === "getStatus" }
        : {};
      const child = new FakeUtility(identity, behavior);
      children.push(child);
      return child;
    }
  });
  assert.equal((await supervisor.request(request("getStatus", "pre-crash"))).ok, true);
  children[0].exit();
  const response = await supervisor.request(request("listBackups", "restart-fails"));
  assert.equal(response.ok, false);
  assert.equal(response.error.code, "CORE_RUNTIME_CRASHED");
  assert.equal(children.length, 2);
});

test("shutdown is idempotent and rejects concurrent requests without spawning an orphan runtime", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity);
      children.push(child);
      return child;
    }
  });
  assert.equal((await supervisor.request(request("getStatus", "shutdown-ready"))).ok, true);
  const firstShutdown = supervisor.shutdown();
  const secondShutdown = supervisor.shutdown();
  const duringShutdown = await supervisor.request(request("getStatus", "shutdown-denied"));
  assert.equal(duringShutdown.ok, false);
  assert.equal(duringShutdown.error.code, "INTERNAL_ERROR");
  await Promise.all([firstShutdown, secondShutdown]);
  assert.equal(children.length, 1);
  assert.equal(children[0].exited, true);
  assert.equal(supervisor.snapshot.state, "stopped");
});

test("a timeout terminates its generation before a late response or reused requestId can alias", async () => {
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
  const timedOut = await supervisor.request(request("getStatus", "late-response"));
  assert.equal(timedOut.ok, false);
  assert.equal(timedOut.error.code, "INTERNAL_ERROR");
  assert.equal(supervisor.snapshot.state, "crashed");
  await new Promise((resolve) => setTimeout(resolve, 40));
  assert.equal(supervisor.snapshot.state, "crashed");
  assert.equal((await supervisor.request(request("getStatus", "late-response"))).ok, true);
  assert.equal(children.length, 2);
  assert.equal(supervisor.snapshot.generation, 2);
});

test("restart preflights every concurrently requested profile before dispatch", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity);
      children.push(child);
      return child;
    }
  });
  assert.equal((await supervisor.request(request("getStatus", "profiles-initial"))).ok, true);
  children[0].exit();
  const [profileA, profileB] = await Promise.all([
    supervisor.request(request("listBackups", "profile-a-read", "profile-a", "revision-a")),
    supervisor.request(request("listBackups", "profile-b-read", "profile-b", "revision-b"))
  ]);
  assert.equal(profileA.ok, true);
  assert.equal(profileB.ok, true);
  const statusProfiles = children[1].messages
    .filter((frame) => frame.envelope?.method === "getStatus")
    .map((frame) => frame.envelope.payload.profile.profileId)
    .sort();
  assert.deepEqual(statusProfiles, ["profile-a", "profile-b"]);
  assert.equal(children[1].messages.filter((frame) => frame.envelope?.method === "listBackups").length, 2);
});

test("a failed restart preflight remains required and is retried on the next request", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity, children.length === 1 ? { failStatusCount: 1 } : {});
      children.push(child);
      return child;
    }
  });
  assert.equal((await supervisor.request(request("getStatus", "preflight-retry-initial"))).ok, true);
  children[0].exit();
  const failed = await supervisor.request(request("listBackups", "preflight-first-fails"));
  assert.equal(failed.ok, false);
  assert.equal(failed.error.code, "INTERNAL_ERROR");
  const retried = await supervisor.request(request("listBackups", "preflight-second-succeeds"));
  assert.equal(retried.ok, true);
  assert.deepEqual(
    children[1].messages.map((frame) => frame.envelope?.method),
    ["getStatus", "getStatus", "listBackups"]
  );
});

test("a response operationId mismatch fails the runtime instead of resolving the request", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity, { wrongOperationId: true });
      children.push(child);
      return child;
    }
  });
  const response = await supervisor.request(request(
    "getStatus",
    "operation-mismatch",
    "default",
    "profile-r1",
    "expected-operation"
  ));
  assert.equal(response.ok, false);
  assert.equal(response.error.code, "CORE_RUNTIME_CRASHED");
  assert.equal(supervisor.snapshot.state, "crashed");
});

test("shutdown before first activation permanently rejects later requests", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity);
      children.push(child);
      return child;
    }
  });
  await supervisor.shutdown();
  const response = await supervisor.request(request("getStatus", "disposed-request"));
  assert.equal(response.ok, false);
  assert.equal(response.error.code, "INTERNAL_ERROR");
  assert.equal(children.length, 0);
  assert.equal(supervisor.snapshot.state, "stopped");
});

test("restart preflight rejects a status projected for the wrong profile", async () => {
  const children = [];
  const supervisor = new CoreRuntimeSupervisor({
    appVersion: "0.5.0",
    spawnUtility(identity) {
      const child = new FakeUtility(identity, children.length === 1 ? { wrongStatusProfile: true } : {});
      children.push(child);
      return child;
    }
  });
  assert.equal((await supervisor.request(request("getStatus", "wrong-profile-initial"))).ok, true);
  children[0].exit();
  const response = await supervisor.request(request("listBackups", "wrong-profile-preflight"));
  assert.equal(response.ok, false);
  assert.equal(response.error.code, "CORE_RUNTIME_CRASHED");
  assert.equal(supervisor.snapshot.state, "crashed");
});
