import assert from "node:assert/strict";
import test from "node:test";

import {
  CoreClientError,
  CoreTransportError,
  DESKTOP_MAINTENANCE_METHODS,
  DESKTOP_READ_METHODS,
  DESKTOP_RESTORE_METHODS,
  DESKTOP_SYNC_SWITCH_METHODS,
  DesktopCoreClient,
  HttpCoreClient,
  HttpCoreTransport,
  MockCoreClient,
  legacyErrorToDto
} from "../dist/index.js";

const profile = { profile: { profileId: "default", profileRevision: "r1" } };

test("MockCoreClient uses the same versioned request envelope", async () => {
  const client = new MockCoreClient({
    getStatus: async () => ({
      schemaVersion: 1,
      snapshotAt: "2026-08-25T00:00:00.000Z",
      storageRevision: "storage",
      profile: { id: "default", revision: "r1" },
      currentProvider: "openai",
      rolloutCounts: {},
      sqliteCounts: null,
      codexHomeSource: "profile",
      sqliteHomeSource: "default",
      backupSummary: { count: 0, totalBytes: 0 },
      pendingRecovery: false,
      pendingTransactions: [],
      operationInProgress: null,
      rolloutScanComplete: true,
      lockedRolloutFiles: []
    })
  }, { requestIdFactory: () => "mock-request" });

  const status = await client.getStatus(profile);
  assert.equal(status.currentProvider, "openai");
  assert.deepEqual(client.requests[0], {
    protocolVersion: 1,
    requestId: "mock-request",
    method: "getStatus",
    payload: profile
  });
});

test("HttpCoreClient validates response correlation and sends one envelope", async () => {
  let captured;
  const client = new HttpCoreClient({
    baseUrl: "http://127.0.0.1:31337/",
    requestIdFactory: () => "http-request",
    fetch: async (_url, init) => {
      captured = JSON.parse(String(init.body));
      return new Response(JSON.stringify({
        protocolVersion: 1,
        requestId: "http-request",
        ok: true,
        result: { schemaVersion: 1, watches: [] }
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    }
  });

  const result = await client.getWatchStatus({});
  assert.deepEqual(result, { schemaVersion: 1, watches: [] });
  assert.deepEqual(captured, {
    protocolVersion: 1,
    requestId: "http-request",
    method: "getWatchStatus",
    payload: {}
  });
});

test("HttpCoreClient streams lifecycle events and cancels an apply by request correlation", async () => {
  const operationId = "11111111-1111-4111-8111-111111111111";
  const cancellations = [];
  let stream;
  let mainSignal;
  const client = new HttpCoreClient({
    baseUrl: "http://127.0.0.1:31337/",
    requestIdFactory: () => "http-stream-apply",
    fetch: async (url, init) => {
      if (String(url).endsWith("/api/core/cancel")) {
        cancellations.push(JSON.parse(String(init.body)));
        stream.enqueue(new TextEncoder().encode(`${JSON.stringify({
          protocolVersion: 1,
          requestId: "http-stream-apply",
          operationId,
          ok: false,
          error: {
            code: "OPERATION_CANCELLED",
            message: "The operation was cancelled.",
            severity: "info",
            retryable: true,
            recoveryRequired: false,
            operationId
          }
        })}\n`));
        stream.close();
        return new Response(JSON.stringify({ accepted: true }), { status: 200 });
      }
      mainSignal = init.signal;
      assert.equal(init.headers.Accept, "application/x-ndjson");
      return new Response(new ReadableStream({
        start(controller) {
          stream = controller;
          for (const event of [{
            protocolVersion: 1,
            requestId: "http-stream-apply",
            operationId,
            event: "operation-started",
            operation: "sync"
          }, {
            protocolVersion: 1,
            requestId: "http-stream-apply",
            operationId,
            event: "progress",
            progress: { stage: "create_backup", status: "start" }
          }]) controller.enqueue(new TextEncoder().encode(`${JSON.stringify(event)}\n`));
        }
      }), { status: 200, headers: { "Content-Type": "application/x-ndjson; charset=utf-8" } });
    }
  });
  const controller = new AbortController();
  const started = [];
  const progress = [];
  let progressSeen;
  const sawProgress = new Promise((resolve) => { progressSeen = resolve; });
  const applying = client.applySync(
    { schemaVersion: 1, planId: "a".repeat(32) },
    {
      signal: controller.signal,
      onOperationStarted: (event) => started.push(event),
      onProgress: (event) => { progress.push(event); progressSeen(); }
    }
  );
  await sawProgress;
  controller.abort();
  await assert.rejects(applying, (error) => (
    error instanceof CoreClientError && error.code === "OPERATION_CANCELLED"
  ));
  assert.equal(mainSignal, undefined);
  assert.equal(started.length, 1);
  assert.equal(progress.length, 1);
  assert.deepEqual(cancellations, [{
    protocolVersion: 1,
    requestId: "http-stream-apply",
    operationId
  }]);
});

function ndjsonResponse(frames) {
  return new Response(
    frames.map((frame) => `${JSON.stringify(frame)}\n`).join(""),
    { status: 200, headers: { "Content-Type": "application/x-ndjson" } }
  );
}

function completedSyncResult(operationId) {
  return {
    schemaVersion: 1,
    operationId,
    operation: "sync",
    outcome: "completed",
    backup: { backupId: "managed" },
    warnings: [],
    result: {}
  };
}

test("HttpCoreClient rejects a terminal operationId that differs from the lifecycle", async () => {
  const startedOperationId = "11111111-1111-4111-8111-111111111113";
  const terminalOperationId = "11111111-1111-4111-8111-111111111114";
  const client = new HttpCoreClient({
    baseUrl: "http://127.0.0.1:31337/",
    requestIdFactory: () => "http-mismatched-terminal",
    fetch: async () => ndjsonResponse([{
      protocolVersion: 1,
      requestId: "http-mismatched-terminal",
      operationId: startedOperationId,
      event: "operation-started",
      operation: "sync"
    }, {
      protocolVersion: 1,
      requestId: "http-mismatched-terminal",
      operationId: terminalOperationId,
      ok: true,
      result: completedSyncResult(terminalOperationId)
    }])
  });

  await assert.rejects(
    client.applySync({ schemaVersion: 1, planId: "a".repeat(32) }),
    (error) => error instanceof CoreTransportError
      && error.message === "Core HTTP stream terminal operationId did not match its lifecycle."
  );
});

test("HttpCoreClient rejects result and error operationIds that differ from the lifecycle", async () => {
  const lifecycleOperationId = "11111111-1111-4111-8111-111111111119";
  const mismatchedOperationId = "11111111-1111-4111-8111-111111111120";
  for (const terminal of [{
    protocolVersion: 1,
    requestId: "http-inner-operation-mismatch",
    operationId: lifecycleOperationId,
    ok: true,
    result: completedSyncResult(mismatchedOperationId)
  }, {
    protocolVersion: 1,
    requestId: "http-inner-operation-mismatch",
    operationId: lifecycleOperationId,
    ok: false,
    error: {
      code: "OPERATION_CANCELLED",
      message: "The operation was cancelled.",
      severity: "info",
      retryable: true,
      recoveryRequired: false,
      operationId: mismatchedOperationId
    }
  }]) {
    const client = new HttpCoreClient({
      baseUrl: "http://127.0.0.1:31337/",
      requestIdFactory: () => "http-inner-operation-mismatch",
      fetch: async () => ndjsonResponse([{
        protocolVersion: 1,
        requestId: "http-inner-operation-mismatch",
        operationId: lifecycleOperationId,
        event: "operation-started",
        operation: "sync"
      }, terminal])
    });
    await assert.rejects(
      client.applySync({ schemaVersion: 1, planId: "a".repeat(32) }),
      (error) => error instanceof CoreTransportError
        && /operationId did not match its lifecycle/.test(error.message)
    );
  }
});

test("HttpCoreClient rejects progress before operation-started", async () => {
  const operationId = "11111111-1111-4111-8111-111111111115";
  const client = new HttpCoreClient({
    baseUrl: "http://127.0.0.1:31337/",
    requestIdFactory: () => "http-progress-before-start",
    fetch: async () => ndjsonResponse([{
      protocolVersion: 1,
      requestId: "http-progress-before-start",
      operationId,
      event: "progress",
      progress: { stage: "create_backup", status: "start" }
    }])
  });

  await assert.rejects(
    client.applySync({ schemaVersion: 1, planId: "a".repeat(32) }),
    (error) => error instanceof CoreTransportError
      && error.message === "Core HTTP stream emitted progress before operation-started."
  );
});

test("HttpCoreClient rejects lifecycle events on read methods", async () => {
  const client = new HttpCoreClient({
    baseUrl: "http://127.0.0.1:31337/",
    requestIdFactory: () => "http-read-lifecycle",
    fetch: async () => ndjsonResponse([{
      protocolVersion: 1,
      requestId: "http-read-lifecycle",
      operationId: "11111111-1111-4111-8111-111111111116",
      event: "operation-started",
      operation: "sync"
    }])
  });

  await assert.rejects(
    client.getWatchStatus({}),
    (error) => error instanceof CoreTransportError
      && error.message === "Core HTTP read stream contained an operation event."
  );
});

test("HttpCoreClient rejects successful apply streams without operation-started", async () => {
  const operationId = "11111111-1111-4111-8111-111111111117";
  const client = new HttpCoreClient({
    baseUrl: "http://127.0.0.1:31337/",
    requestIdFactory: () => "http-apply-without-start",
    fetch: async () => ndjsonResponse([{
      protocolVersion: 1,
      requestId: "http-apply-without-start",
      ok: true,
      result: completedSyncResult(operationId)
    }])
  });

  await assert.rejects(
    client.applySync({ schemaVersion: 1, planId: "a".repeat(32) }),
    (error) => error instanceof CoreTransportError
      && error.message === "Core HTTP apply stream ended without operation-started."
  );
});

test("MockCoreClient exposes observer-safe lifecycle controls to UI handlers", async () => {
  const operationId = "11111111-1111-4111-8111-111111111112";
  const controller = new AbortController();
  const progress = [];
  const client = new MockCoreClient({
    applySync: async (_payload, request, control) => {
      assert.equal(control.signal, controller.signal);
      control.onOperationStarted?.({
        protocolVersion: 1,
        requestId: request.requestId,
        operationId,
        event: "operation-started",
        operation: "sync"
      });
      control.onProgress?.({
        protocolVersion: 1,
        requestId: request.requestId,
        operationId,
        event: "progress",
        progress: { stage: "create_backup", status: "complete" }
      });
      return {
        schemaVersion: 1,
        operationId,
        operation: "sync",
        outcome: "completed",
        backup: { backupId: "managed" },
        warnings: [],
        result: {}
      };
    }
  }, { requestIdFactory: () => "mock-stream-apply" });
  const result = await client.applySync(
    { schemaVersion: 1, planId: "a".repeat(32) },
    {
      signal: controller.signal,
      onOperationStarted: () => { throw new Error("observer failure"); },
      onProgress: (event) => progress.push(event)
    }
  );
  assert.equal(result.outcome, "completed");
  assert.equal(progress.length, 1);
});

test("HttpCoreClient rejects an oversized envelope before calling fetch", async () => {
  let fetchCalls = 0;
  const transport = new HttpCoreTransport({
    baseUrl: "http://127.0.0.1:31337/",
    fetch: async () => {
      fetchCalls += 1;
      throw new Error("fetch must not be called");
    }
  });

  await assert.rejects(
    transport.request({
      protocolVersion: 1,
      requestId: "oversized-request",
      method: "getWatchStatus",
      payload: { oversized: "x".repeat(65 * 1024) }
    }),
    (error) => error instanceof CoreTransportError
      && error.status === null
      && error.message === "Core request exceeds the 64 KiB transport limit."
  );
  assert.equal(fetchCalls, 0);
});

test("canonical failed envelopes become CoreClientError", async () => {
  const client = new MockCoreClient({
    prepareSync: async () => {
      throw Object.assign(new Error("busy"), {
        code: "OPERATION_BUSY",
        severity: "warning",
        retryable: true,
        recoveryRequired: false,
        details: { busyScope: "codex-home" }
      });
    }
  });
  await assert.rejects(
    client.prepareSync(profile),
    (error) => error instanceof CoreClientError
      && error.code === "OPERATION_BUSY"
      && error.dto.details.busyScope === "codex-home"
  );
});

test("legacy error adapter classifies by code and never parses message text", () => {
  const dto = legacyErrorToDto(new Error("OPERATION_BUSY and RECOVERY_REQUIRED are only words"));
  assert.equal(dto.code, "INTERNAL_ERROR");
  assert.equal(dto.message, "An internal error occurred.");
});

test("legacy error adapter never leaks exception text or arbitrary details", () => {
  const dto = legacyErrorToDto(Object.assign(new Error("token=secret C:/private message body"), {
    code: "OPERATION_BUSY",
    details: {
      busyScope: "codex-home",
      token: "secret",
      path: "C:/private",
      messageBody: "private"
    },
    suggestedAction: "send token=secret"
  }));
  assert.deepEqual(dto, {
    code: "OPERATION_BUSY",
    message: "Another write operation is using the protected resource.",
    severity: "warning",
    retryable: true,
    recoveryRequired: false,
    details: { busyScope: "codex-home" }
  });
  assert.doesNotMatch(JSON.stringify(dto), /secret|private|message body/i);
});

test("malformed protocol and success payloads become canonical client errors", async () => {
  const protocolClient = new HttpCoreClient({
    baseUrl: "http://127.0.0.1:31337/",
    requestIdFactory: () => "protocol-request",
    fetch: async () => new Response(JSON.stringify({
      protocolVersion: 2,
      requestId: "protocol-request",
      ok: true,
      result: { schemaVersion: 1, watches: [] }
    }), { status: 200 })
  });
  await assert.rejects(
    protocolClient.getWatchStatus({}),
    (error) => error instanceof CoreClientError
      && error.code === "PROTOCOL_VERSION_MISMATCH"
      && error.message === "The client and Core protocol versions are incompatible."
  );

  const malformedClient = new MockCoreClient({
    getStatus: async () => null
  });
  await assert.rejects(
    malformedClient.getStatus(profile),
    (error) => error instanceof CoreClientError
      && error.code === "INTERNAL_ERROR"
      && error.message === "An internal error occurred."
  );
});

test("HTTP status cannot turn a failed request into a success envelope", async () => {
  const invalid = new HttpCoreClient({
    baseUrl: "http://127.0.0.1:31337/",
    requestIdFactory: () => "invalid-http",
    fetch: async () => new Response(JSON.stringify({
      protocolVersion: 1,
      requestId: "invalid-http",
      ok: true,
      result: { schemaVersion: 1, watches: [] }
    }), { status: 500 })
  });
  await assert.rejects(invalid.getWatchStatus({}), CoreTransportError);

  const invalidStream = new HttpCoreClient({
    baseUrl: "http://127.0.0.1:31337/",
    requestIdFactory: () => "invalid-http-stream",
    fetch: async () => new Response(`${JSON.stringify({
      protocolVersion: 1,
      requestId: "invalid-http-stream",
      ok: true,
      result: { schemaVersion: 1, watches: [] }
    })}\n`, {
      status: 500,
      headers: { "Content-Type": "application/x-ndjson" }
    })
  });
  await assert.rejects(invalidStream.getWatchStatus({}), CoreTransportError);

  const busy = new HttpCoreClient({
    baseUrl: "http://127.0.0.1:31337/",
    requestIdFactory: () => "busy-http",
    fetch: async () => new Response(JSON.stringify({
      protocolVersion: 1,
      requestId: "busy-http",
      ok: false,
      error: {
        code: "OPERATION_BUSY",
        message: "Another write operation is using the protected resource.",
        severity: "warning",
        retryable: true,
        recoveryRequired: false,
        details: { busyScope: "codex-home" }
      }
    }), { status: 409 })
  });
  await assert.rejects(
    busy.getWatchStatus({}),
    (error) => error instanceof CoreClientError && error.code === "OPERATION_BUSY"
  );
});

test("DesktopCoreClient reuses the Core envelope through one read-only bridge", async () => {
  const requests = [];
  const client = new DesktopCoreClient({
    async requestReadOnly(request) {
      requests.push(request);
      return {
        protocolVersion: 1,
        requestId: request.requestId,
        ok: true,
        result: {
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
        }
      };
    },
    async requestSyncSwitch() { throw new Error("unexpected write"); },
    async requestRestore() { throw new Error("unexpected restore"); },
    async requestMaintenance() { throw new Error("unexpected maintenance"); },
    subscribeOperation() { return () => {}; },
    async cancelOperation() { return { accepted: false }; }
  }, { requestIdFactory: () => "desktop-status" });
  assert.equal((await client.getStatus(profile)).currentProvider, "openai");
  assert.deepEqual(requests, [{
    protocolVersion: 1,
    requestId: "desktop-status",
    method: "getStatus",
    payload: profile
  }]);
  assert.deepEqual(DESKTOP_READ_METHODS, [
    "getStatus",
    "listBackups",
    "listHistory",
    "getHistorySession",
    "getDiagnostics"
  ]);
});

test("DesktopCoreClient routes the exact C8 surface and forwards lifecycle cancellation", async () => {
  let calls = 0;
  let listener = null;
  const cancellations = [];
  const routed = [];
  let finishApply;
  const client = new DesktopCoreClient({
    async requestReadOnly() {
      calls += 1;
      throw new Error("unexpected read");
    },
    async requestSyncSwitch(request) {
      calls += 1;
      routed.push(request.method);
      if (request.method === "prepareSync") {
        return {
          protocolVersion: 1,
          requestId: request.requestId,
          ok: true,
          result: {
            schemaVersion: 1,
            planId: "a".repeat(32),
            operation: "sync",
            createdAt: "2026-08-26T00:00:00.000Z",
            expiresAt: "2026-08-26T00:10:00.000Z",
            profile: { id: "default", revision: "r1" },
            storageRevision: "storage",
            configRevision: "config",
            rolloutRevision: "rollout",
            stateDbRevision: "state-db",
            target: { provider: "openai" },
            impact: { backupExpected: true },
            warnings: [],
            requiresConfirmation: true
          }
        };
      }
      return new Promise((resolve) => { finishApply = () => resolve({
        protocolVersion: 1,
        requestId: request.requestId,
        operationId: "11111111-1111-4111-8111-111111111111",
        ok: false,
        error: {
          code: "OPERATION_CANCELLED",
          message: "The operation was cancelled.",
          severity: "info",
          retryable: true,
          recoveryRequired: false,
          operationId: "11111111-1111-4111-8111-111111111111"
        }
      }); });
    },
    async requestRestore(request) {
      calls += 1;
      routed.push(request.method);
      return {
        protocolVersion: 1,
        requestId: request.requestId,
        ok: true,
        result: {
          schemaVersion: 1,
          planId: "r".repeat(32),
          operation: "restore",
          createdAt: "2026-08-26T00:00:00.000Z",
          expiresAt: "2026-08-26T00:10:00.000Z",
          profile: { id: "default", revision: "r1" },
          storageRevision: "storage",
          configRevision: "config",
          rolloutRevision: "rollout",
          stateDbRevision: "state-db",
          backupRevision: "backup",
          target: { backupId: "managed" },
          impact: { backupExpected: true },
          warnings: [],
          requiresConfirmation: true
        }
      };
    },
    async requestMaintenance(request) {
      calls += 1;
      routed.push(request.method);
      return {
        protocolVersion: 1,
        requestId: request.requestId,
        ok: true,
        result: request.method === "pruneBackups"
          ? { deletedCount: 0, remainingCount: 1, freedBytes: 0 }
          : {
              schemaVersion: 1,
              watchId: "11111111-1111-4111-8111-111111111112",
              status: "running",
              startedAt: "2026-08-26T00:00:00.000Z",
              stoppedAt: null,
              stopReason: null,
              includeStateDb: true,
              once: false
            }
      };
    },
    subscribeOperation(next) { listener = next; return () => { listener = null; }; },
    async cancelOperation(input) { cancellations.push(input); return { accepted: true }; }
  }, { requestIdFactory: (() => {
    const ids = ["desktop-prepare", "desktop-apply", "desktop-restore", "desktop-prune", "desktop-watch"];
    return () => ids.shift() ?? `desktop-denied-${calls}`;
  })() });
  const plan = await client.prepareSync({ ...profile, keepCount: 5 });
  assert.equal(plan.operation, "sync");
  const controller = new AbortController();
  const started = [];
  const progress = [];
  const applying = client.applySync(
    { schemaVersion: 1, planId: plan.planId },
    {
      signal: controller.signal,
      onOperationStarted: (event) => started.push(event),
      onProgress: (event) => progress.push(event)
    }
  );
  listener({
    protocolVersion: 1,
    requestId: "desktop-apply",
    operationId: "11111111-1111-4111-8111-111111111111",
    event: "operation-started",
    operation: "sync"
  });
  listener({
    protocolVersion: 1,
    requestId: "desktop-apply",
    operationId: "11111111-1111-4111-8111-111111111111",
    event: "progress",
    progress: { stage: "create_backup", status: "start" }
  });
  controller.abort();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(started.length, 1);
  assert.equal(progress.length, 1);
  assert.deepEqual(cancellations.at(-1), {
    requestId: "desktop-apply",
    operationId: "11111111-1111-4111-8111-111111111111"
  });
  finishApply();
  await assert.rejects(applying, (error) => (
    error instanceof CoreClientError && error.code === "OPERATION_CANCELLED"
  ));
  const restore = await client.prepareRestore({
    ...profile,
    backupId: "managed",
    restoreConfig: true,
    restoreDatabase: true,
    restoreSessions: true
  });
  assert.equal(restore.operation, "restore");
  assert.equal((await client.pruneBackups({ ...profile, keepCount: 5 })).remainingCount, 1);
  assert.equal((await client.startWatch({ ...profile, includeStateDb: true })).status, "running");
  assert.deepEqual(DESKTOP_SYNC_SWITCH_METHODS, [
    "prepareSync",
    "applySync",
    "prepareSwitch",
    "applySwitch"
  ]);
  assert.deepEqual(DESKTOP_RESTORE_METHODS, ["prepareRestore", "applyRestore"]);
  assert.deepEqual(DESKTOP_MAINTENANCE_METHODS, [
    "pruneBackups",
    "startWatch",
    "stopWatch",
    "getWatchStatus"
  ]);
  assert.deepEqual(routed, [
    "prepareSync",
    "applySync",
    "prepareRestore",
    "pruneBackups",
    "startWatch"
  ]);
  assert.equal(calls, 5);
});

test("DesktopCoreClient retries an unaccepted Apply cancellation until it is acknowledged", async () => {
  let listener = null;
  let finishApply;
  const cancellations = [];
  const operationId = "11111111-1111-4111-8111-111111111111";
  const client = new DesktopCoreClient({
    async requestReadOnly() { throw new Error("unexpected read"); },
    async requestSyncSwitch(request) {
      return new Promise((resolve) => {
        finishApply = () => resolve({
          protocolVersion: 1,
          requestId: request.requestId,
          operationId,
          ok: false,
          error: {
            code: "OPERATION_CANCELLED",
            message: "The operation was cancelled.",
            severity: "info",
            retryable: true,
            recoveryRequired: false,
            operationId
          }
        });
      });
    },
    async requestRestore() { throw new Error("unexpected restore"); },
    async requestMaintenance() { throw new Error("unexpected maintenance"); },
    subscribeOperation(next) { listener = next; return () => { listener = null; }; },
    async cancelOperation(input) {
      cancellations.push(input);
      const accepted = cancellations.length > 1;
      if (accepted) queueMicrotask(finishApply);
      return { accepted };
    }
  }, { requestIdFactory: () => "desktop-cancel-retry" });
  const controller = new AbortController();
  const applying = client.applySync(
    { schemaVersion: 1, planId: "p".repeat(48) },
    { signal: controller.signal }
  );
  listener({
    protocolVersion: 1,
    requestId: "desktop-cancel-retry",
    operationId,
    event: "operation-started",
    operation: "sync"
  });
  controller.abort();
  await assert.rejects(applying, (error) => (
    error instanceof CoreClientError && error.code === "OPERATION_CANCELLED"
  ));
  assert.deepEqual(cancellations, [
    { requestId: "desktop-cancel-retry", operationId },
    { requestId: "desktop-cancel-retry", operationId }
  ]);
});

test("DesktopCoreClient re-confirms an early accepted cancellation after operation-started", async () => {
  let listener = null;
  let finishApply;
  const cancellations = [];
  const operationId = "11111111-1111-4111-8111-111111111111";
  const client = new DesktopCoreClient({
    async requestReadOnly() { throw new Error("unexpected read"); },
    async requestSyncSwitch(request) {
      return new Promise((resolve) => {
        finishApply = () => resolve({
          protocolVersion: 1,
          requestId: request.requestId,
          operationId,
          ok: false,
          error: {
            code: "OPERATION_CANCELLED",
            message: "The operation was cancelled.",
            severity: "info",
            retryable: true,
            recoveryRequired: false,
            operationId
          }
        });
      });
    },
    async requestRestore() { throw new Error("unexpected restore"); },
    async requestMaintenance() { throw new Error("unexpected maintenance"); },
    subscribeOperation(next) { listener = next; return () => { listener = null; }; },
    async cancelOperation(input) {
      cancellations.push(input);
      if (input.operationId === operationId) queueMicrotask(finishApply);
      return { accepted: true };
    }
  }, { requestIdFactory: () => "desktop-early-cancel" });
  const controller = new AbortController();
  const applying = client.applySync(
    { schemaVersion: 1, planId: "p".repeat(48) },
    { signal: controller.signal }
  );
  controller.abort();
  await new Promise((resolve) => setImmediate(resolve));
  listener({
    protocolVersion: 1,
    requestId: "desktop-early-cancel",
    operationId,
    event: "operation-started",
    operation: "sync"
  });
  await assert.rejects(applying, (error) => (
    error instanceof CoreClientError && error.code === "OPERATION_CANCELLED"
  ));
  assert.deepEqual(cancellations, [
    { requestId: "desktop-early-cancel" },
    { requestId: "desktop-early-cancel", operationId }
  ]);
});

test("DesktopCoreClient backs off rejected cancellations and stops after the request settles", async () => {
  let listener = null;
  let finishApply;
  let activeCancellations = 0;
  let maximumActiveCancellations = 0;
  let cancellationCalls = 0;
  const operationId = "11111111-1111-4111-8111-111111111111";
  const client = new DesktopCoreClient({
    async requestReadOnly() { throw new Error("unexpected read"); },
    async requestSyncSwitch(request) {
      return new Promise((resolve) => {
        finishApply = () => resolve({
          protocolVersion: 1,
          requestId: request.requestId,
          operationId,
          ok: false,
          error: {
            code: "OPERATION_CANCELLED",
            message: "The operation was cancelled.",
            severity: "info",
            retryable: true,
            recoveryRequired: false,
            operationId
          }
        });
      });
    },
    async requestRestore() { throw new Error("unexpected restore"); },
    async requestMaintenance() { throw new Error("unexpected maintenance"); },
    subscribeOperation(next) { listener = next; return () => { listener = null; }; },
    async cancelOperation() {
      cancellationCalls += 1;
      activeCancellations += 1;
      maximumActiveCancellations = Math.max(maximumActiveCancellations, activeCancellations);
      await new Promise((resolve) => setTimeout(resolve, 40));
      activeCancellations -= 1;
      throw new Error("transient cancellation invoke failure");
    }
  }, { requestIdFactory: () => "desktop-cancel-backoff" });
  const controller = new AbortController();
  const applying = client.applySync(
    { schemaVersion: 1, planId: "p".repeat(48) },
    { signal: controller.signal }
  );
  listener({
    protocolVersion: 1,
    requestId: "desktop-cancel-backoff",
    operationId,
    event: "operation-started",
    operation: "sync"
  });
  controller.abort();
  setTimeout(finishApply, 110);
  await assert.rejects(applying, (error) => (
    error instanceof CoreClientError && error.code === "OPERATION_CANCELLED"
  ));
  const callsAtSettlement = cancellationCalls;
  assert.equal(maximumActiveCancellations, 1);
  assert.ok(callsAtSettlement >= 1 && callsAtSettlement <= 2);
  await new Promise((resolve) => setTimeout(resolve, 175));
  assert.equal(cancellationCalls, callsAtSettlement);
});
