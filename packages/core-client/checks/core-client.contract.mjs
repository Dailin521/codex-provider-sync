import assert from "node:assert/strict";
import test from "node:test";

import {
  CoreClientError,
  CoreTransportError,
  DESKTOP_READ_METHODS,
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
    }
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

test("DesktopCoreClient rejects every C6 write method before invoking the bridge", async () => {
  let calls = 0;
  const client = new DesktopCoreClient({
    async requestReadOnly() {
      calls += 1;
      throw new Error("write request escaped the desktop policy");
    }
  });
  await assert.rejects(
    client.prepareSync({ ...profile, keepCount: 5 }),
    (error) => error instanceof CoreClientError && error.code === "PERMISSION_DENIED"
  );
  await assert.rejects(
    client.applyRestore({ schemaVersion: 1, planId: "plan-denied" }),
    (error) => error instanceof CoreClientError && error.code === "PERMISSION_DENIED"
  );
  await assert.rejects(
    client.startWatch(profile),
    (error) => error instanceof CoreClientError && error.code === "PERMISSION_DENIED"
  );
  assert.equal(calls, 0);
});
