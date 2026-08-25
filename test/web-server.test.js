import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { CoreError } from "../src/public-api.js";
import { createWebUiServer, startWebUi } from "../src/web-server.js";
import { createMemoryWebUiState, WebUiStateStore } from "../src/web-state.js";

function request({ origin, pathname = "/", method = "GET", body, headers = {}, hostHeader }) {
  return new Promise((resolve, reject) => {
    const target = new URL(origin);
    const serialized = body === undefined ? null : JSON.stringify(body);
    const client = http.request({
      hostname: target.hostname,
      port: target.port,
      path: pathname,
      method,
      headers: {
        ...(hostHeader ? { Host: hostHeader } : {}),
        ...(serialized ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(serialized) } : {}),
        ...headers
      }
    }, (response) => {
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("end", () => {
        const text = Buffer.concat(chunks).toString("utf8");
        let payload = null;
        try { payload = JSON.parse(text); } catch {}
        resolve({ status: response.statusCode, text, payload, headers: response.headers });
      });
    });
    client.once("error", reject);
    if (serialized) client.write(serialized);
    client.end();
  });
}

async function startFixture(services = {}, options = {}) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-web-"));
  await fs.writeFile(
    path.join(root, "index.html"),
    '<!doctype html><title>fixture</title><script>window.boot=__CODEX_PROVIDER_SYNC_BOOTSTRAP__;</script>',
    "utf8"
  );
  const stateStore = options.stateStore ?? createMemoryWebUiState({
    codexHome: root,
    ...(options.defaultSqliteHome ? { sqliteHome: options.defaultSqliteHome } : {})
  });
  const handle = createWebUiServer({
    webRoot: root,
    services,
    stateStore,
    now: options.now,
    pairingTtlMs: options.pairingTtlMs,
    platform: options.platform,
    environment: options.environment
  });
  await new Promise((resolve, reject) => {
    handle.server.once("error", reject);
    handle.server.listen(0, "127.0.0.1", resolve);
  });
  const address = handle.server.address();
  const origin = `http://127.0.0.1:${address.port}`;
  handle.setBaseUrl(origin);
  return {
    ...handle,
    stateStore,
    root,
    origin,
    async pair(pairingToken = handle.issuePairing(), { originHeader = origin, hostHeader } = {}) {
      const response = await request({
        origin,
        pathname: "/api/pair",
        method: "POST",
        headers: { Origin: originHeader, "X-Codex-Provider-Pairing": pairingToken },
        hostHeader
      });
      return { response, credential: response.payload?.deviceCredential };
    },
    close: async () => {
      await new Promise((resolve, reject) => handle.server.close((error) => error ? reject(error) : resolve()));
      await fs.rm(root, { recursive: true, force: true });
    }
  };
}

async function api(handle, pathname, body = {}, credential, { originHeader = handle.origin, hostHeader } = {}) {
  const profileRevisionEndpoints = new Set([
    "/api/prune",
    "/api/sync/prepare", "/api/switch/prepare", "/api/restore/prepare"
  ]);
  const profile = profileRevisionEndpoints.has(pathname) && body.profileId && !Object.hasOwn(body, "profileRevision")
    ? handle.stateStore.getProfile(body.profileId)
    : null;
  const preparedBody = profile ? { ...body, profileRevision: profile.revision } : body;
  return request({
    origin: handle.origin,
    pathname,
    method: "POST",
    body: preparedBody,
    hostHeader,
    headers: {
      Origin: originHeader,
      "X-Codex-Provider-Device": credential ?? ""
    }
  });
}

function statusFixture(overrides = {}) {
  return {
    codexHome: "/tmp/.codex",
    sqliteHome: "/tmp/.codex/sqlite",
    sqliteHomeSource: "default",
    sqliteAccess: { supported: true, reason: null, message: null },
    checkedStateDbPaths: ["/tmp/.codex/sqlite/state_5.sqlite"],
    currentProvider: "openai",
    currentProviderImplicit: false,
    configuredProviders: ["openai", "relay"],
    rolloutCounts: { sessions: { relay: 2, openai: 3 }, archived_sessions: { openai: 1 } },
    lockedRolloutFiles: [],
    encryptedContentCounts: { sessions: {}, archived_sessions: {} },
    encryptedContentWarning: null,
    sqliteCounts: { sessions: { openai: 3, relay: 2 }, archived_sessions: { openai: 1 } },
    stateDbLocation: { path: "/tmp/.codex/sqlite/state_5.sqlite", source: "sqlite-dir" },
    sqliteRepairStats: { userEventRowsNeedingRepair: 0, cwdRowsNeedingRepair: 0 },
    projectThreadVisibility: [],
    backupRoot: "/tmp/.codex/backups_state/provider-sync",
    backupSummary: { count: 0, totalBytes: 0 },
    pathComparisonCaseInsensitive: process.platform === "win32",
    ...overrides
  };
}

test("status alignment follows the current provider without requiring equal inventory counts", async () => {
  let currentStatus = statusFixture({
    currentProvider: "dal",
    configuredProviders: ["dal", "openai"],
    rolloutCounts: { sessions: { dal: 949 }, archived_sessions: {} },
    sqliteCounts: { sessions: { dal: 948 }, archived_sessions: {} }
  });
  const handle = await startFixture({ getStatus: async () => currentStatus });
  try {
    const paired = await handle.pair();
    const readAlignment = async () => {
      const response = await api(handle, "/api/status", { profileId: "default" }, paired.credential);
      assert.equal(response.status, 200);
      return response.payload.status.alignment;
    };

    assert.equal((await readAlignment()).aligned, true);

    currentStatus = {
      ...currentStatus,
      sqliteCounts: { sessions: { dal: 947, openai: 1 }, archived_sessions: {} }
    };
    assert.equal((await readAlignment()).aligned, false);

    currentStatus = {
      ...currentStatus,
      rolloutCounts: { sessions: { openai: 948 }, archived_sessions: {} },
      sqliteCounts: { sessions: { openai: 948 }, archived_sessions: {} }
    };
    assert.equal((await readAlignment()).aligned, false);

    currentStatus = {
      ...currentStatus,
      rolloutCounts: { sessions: { dal: 949 }, archived_sessions: {} },
      sqliteCounts: { sessions: { dal: 948 }, archived_sessions: {} },
      lockedRolloutFiles: ["C:\\locked-rollout.jsonl"]
    };
    assert.equal((await readAlignment()).aligned, false);
  } finally {
    await handle.close();
  }
});

test("anonymous HTML contains no API or pairing credential and write APIs require pairing", async () => {
  const handle = await startFixture({ getStatus: async () => statusFixture() });
  try {
    const pairingToken = handle.issuePairing();
    const page = await request({ origin: handle.origin });
    assert.equal(page.status, 200);
    assert.doesNotMatch(page.text, new RegExp(pairingToken));
    assert.doesNotMatch(page.text, /apiToken|X-Codex-Provider-Token/);
    assert.doesNotMatch(page.text, /__CODEX_PROVIDER_SYNC_BOOTSTRAP__/);
    assert.match(page.headers["content-security-policy"], /script-src 'self';/);

    const denied = await api(handle, "/api/status", { profileId: "default" }, "wrong-token");
    assert.equal(denied.status, 403);
    assert.equal(denied.payload.code, "PAIRING_REQUIRED");

    const paired = await handle.pair(pairingToken);
    assert.equal(paired.response.status, 200);
    assert.ok(paired.credential.length >= 32);
    const replay = await handle.pair(pairingToken);
    assert.equal(replay.response.status, 403);

    const allowed = await api(handle, "/api/status", { profileId: "default" }, paired.credential);
    assert.equal(allowed.status, 200);
  } finally {
    await handle.close();
  }
});

test("pairing credentials expire and pairing material is never recorded as activity", async () => {
  let currentTime = 1000;
  const handle = await startFixture({}, { now: () => currentTime, pairingTtlMs: 300 });
  try {
    const pairingToken = handle.issuePairing();
    currentTime += 301;
    const expired = await handle.pair(pairingToken);
    assert.equal(expired.response.status, 403);
    assert.doesNotMatch(JSON.stringify(handle.getActivity()), new RegExp(pairingToken));
  } finally {
    await handle.close();
  }
});

test("internal pairing requires and consumes a server-issued authenticated challenge", async () => {
  const handle = await startFixture();
  try {
    const port = Number(new URL(handle.origin).port);
    const challenge = await request({
      origin: handle.origin,
      pathname: "/api/internal/challenge",
      method: "POST",
      body: { protocolVersion: 2, port, instanceId: handle.instanceId }
    });
    assert.equal(challenge.status, 200);
    const signed = {
      protocolVersion: 2,
      port,
      instanceId: handle.instanceId,
      nonce: challenge.payload.nonce,
      resetAccess: false
    };
    const proof = crypto.createHmac("sha256", handle.internalSecret)
      .update(`codex-provider-sync:web-ui:internal-pairing:v2:request\n${JSON.stringify(signed)}`, "utf8")
      .digest("base64url");
    const first = await request({
      origin: handle.origin,
      pathname: "/api/internal/new-pairing",
      method: "POST",
      body: signed,
      headers: { "X-Codex-Provider-Internal-Proof": proof }
    });
    assert.equal(first.status, 200);
    assert.equal(first.payload.nonce, challenge.payload.nonce);
    assert.equal(first.payload.resetAccess, false);

    const replay = await request({
      origin: handle.origin,
      pathname: "/api/internal/new-pairing",
      method: "POST",
      body: signed,
      headers: { "X-Codex-Provider-Internal-Proof": proof }
    });
    assert.equal(replay.status, 403);
    assert.equal(replay.payload.code, "INTERNAL_CHALLENGE_REQUIRED");
  } finally {
    await handle.close();
  }
});

test("Origin validation uses the actual loopback Host and supports forwarded ports", async () => {
  const handle = await startFixture({ getStatus: async () => statusFixture() });
  try {
    const { credential } = await handle.pair();
    const invalid = await api(handle, "/api/status", { profileId: "default" }, credential, { originHeader: "http://evil.example" });
    assert.equal(invalid.status, 403);
    assert.equal(invalid.payload.code, "INVALID_ORIGIN");

    const forwardedHost = "localhost:19091";
    const forwarded = await api(handle, "/api/status", { profileId: "default" }, credential, {
      originHeader: `http://${forwardedHost}`,
      hostHeader: forwardedHost
    });
    assert.equal(forwarded.status, 200);
  } finally {
    await handle.close();
  }
});

test("server-managed profiles reject per-operation paths and resolve profileId", async () => {
  const calls = [];
  const handle = await startFixture({ getStatus: async (storage) => { calls.push(storage); return statusFixture({ codexHome: storage.codexHome }); } });
  try {
    const { credential } = await handle.pair();
    const rawPath = await api(handle, "/api/status", { codexHome: "/tmp/other" }, credential);
    assert.equal(rawPath.status, 500);
    assert.match(rawPath.payload.error, /server-managed profileId/);

    const workCodexHome = path.join(handle.root, "work-codex");
    const workSqliteHome = path.join(handle.root, "work-sqlite");
    await fs.mkdir(workCodexHome);
    await fs.mkdir(workSqliteHome);
    const saved = await api(handle, "/api/profiles/save", {
      profileId: "work",
      name: "Work",
      codexHome: workCodexHome,
      sqliteHome: workSqliteHome
    }, credential);
    assert.equal(saved.status, 200);

    const response = await api(handle, "/api/status", { profileId: "work" }, credential);
    assert.equal(response.status, 200);
    assert.equal(response.payload.status.pathComparisonCaseInsensitive, process.platform === "win32");
    assert.equal(calls.at(-1).codexHome, path.resolve(workCodexHome));
    assert.equal(calls.at(-1).sqliteHome, path.resolve(workSqliteHome));
  } finally {
    await handle.close();
  }
});

test("legacy Web direct-write routes require Plan/Apply and never invoke a writer", async () => {
  let calls = 0;
  const handle = await startFixture({
    prepareSync: async () => { calls += 1; },
    applySync: async () => { calls += 1; },
    prepareSwitch: async () => { calls += 1; },
    applySwitch: async () => { calls += 1; },
    prepareRestore: async () => { calls += 1; },
    applyRestore: async () => { calls += 1; }
  });
  try {
    const configPath = path.join(handle.root, "config.toml");
    await fs.writeFile(configPath, 'model_provider = "openai"\n', "utf8");
    const before = await fs.readFile(configPath);
    const { credential } = await handle.pair();
    for (const [endpoint, body] of [
      ["/api/sync", { profileId: "default", provider: "openai", keepCount: 5 }],
      ["/api/switch", { profileId: "default", provider: "openai", keepCount: 5 }],
      ["/api/restore", { profileId: "default", backupId: "managed", restoreSessions: true }]
    ]) {
      const response = await api(handle, endpoint, body, credential);
      assert.equal(response.status, 410);
      assert.equal(response.payload.code, "PLAN_REQUIRED");
    }
    assert.equal(calls, 0);
    assert.deepEqual(await fs.readFile(configPath), before);
    await assert.rejects(() => fs.access(path.join(handle.root, "backups_state")), { code: "ENOENT" });
  } finally {
    await handle.close();
  }
});

test("Web Prepare/Apply keeps trusted profile paths server-side and Apply accepts only planId", async () => {
  const prepareCalls = [];
  const applyCalls = [];
  const handle = await startFixture({
    readConfigText: async () => 'model_provider = "openai"\nmodel = "gpt-5"\n',
    readRootModelFromConfigText: () => "gpt-5",
    prepareSync: async (options) => {
      prepareCalls.push(options);
      return {
        schemaVersion: 1,
        planId: "opaque-plan",
        operation: "sync",
        requiresConfirmation: true
      };
    },
    applySync: async (input) => {
      applyCalls.push(input);
      return {
        schemaVersion: 1,
        operationId: "11111111-1111-4111-8111-111111111111",
        operation: "sync",
        outcome: "completed",
        backup: null,
        warnings: [],
        result: { targetProvider: "openai" }
      };
    }
  });
  try {
    const { credential } = await handle.pair();
    const prepared = await api(
      handle,
      "/api/sync/prepare",
      { profileId: "default", provider: "openai", keepCount: 5 },
      credential
    );
    assert.equal(prepared.status, 200);
    assert.equal(prepared.payload.plan.planId, "opaque-plan");
    assert.equal(prepareCalls.length, 1);
    assert.equal(prepareCalls[0].codexHome, path.resolve(handle.root));
    assert.equal(prepareCalls[0].profile.id, "default");
    assert.equal(prepareCalls[0].model, "gpt-5");
    assert.equal(typeof prepareCalls[0].profileResolver, "function");

    const rejected = await api(
      handle,
      "/api/sync/apply",
      { schemaVersion: 1, planId: "opaque-plan", provider: "attacker" },
      credential
    );
    assert.equal(rejected.status, 400);
    assert.equal(rejected.payload.coreError.code, "INVALID_INPUT");
    assert.equal(applyCalls.length, 0);

    const applied = await api(
      handle,
      "/api/sync/apply",
      { schemaVersion: 1, planId: "opaque-plan" },
      credential
    );
    assert.equal(applied.status, 200);
    assert.deepEqual(applyCalls, [{ schemaVersion: 1, planId: "opaque-plan" }]);
    assert.equal(applied.payload.result.outcome, "completed");
  } finally {
    await handle.close();
  }
});

test("Web Prepare validates Switch model modes and passes Restore only a managed backupId", async () => {
  const switchCalls = [];
  const restoreCalls = [];
  const handle = await startFixture({
    prepareSwitch: async (options) => {
      switchCalls.push(options);
      return { schemaVersion: 1, planId: `switch-${switchCalls.length}`, operation: "switch" };
    },
    prepareRestore: async (options) => {
      restoreCalls.push(options);
      return { schemaVersion: 1, planId: "restore-1", operation: "restore" };
    }
  });
  try {
    const { credential } = await handle.pair();
    const explicit = await api(handle, "/api/switch/prepare", {
      profileId: "default",
      provider: "relay",
      keepCount: 5,
      modelMode: "explicit",
      model: "model-x"
    }, credential);
    assert.equal(explicit.status, 200);
    assert.equal(switchCalls[0].model, "model-x");
    assert.equal(switchCalls[0].keepRootModel, false);

    const invalid = await api(handle, "/api/switch/prepare", {
      profileId: "default",
      provider: "relay",
      keepCount: 5,
      modelMode: "keep-root-model",
      model: "must-not-pass"
    }, credential);
    assert.equal(invalid.status, 400);
    assert.equal(invalid.payload.coreError.code, "INVALID_INPUT");

    const restore = await api(handle, "/api/restore/prepare", {
      profileId: "default",
      backupId: "managed-backup-1",
      restoreConfig: true,
      restoreDatabase: false,
      restoreSessions: true
    }, credential);
    assert.equal(restore.status, 200);
    assert.equal(restoreCalls.length, 1);
    assert.equal(restoreCalls[0].backupId, "managed-backup-1");
    assert.equal(Object.hasOwn(restoreCalls[0], "backupDir"), false);
  } finally {
    await handle.close();
  }
});

test("Web Apply appends a safe CoreError DTO without leaking transport internals", async () => {
  const handle = await startFixture({
    applySync: async () => {
      throw new CoreError("SQLITE_BUSY", "The state database is busy.", {
        details: { causeCode: "SQLITE_BUSY" }
      });
    }
  });
  try {
    const { credential } = await handle.pair();
    const response = await api(
      handle,
      "/api/sync/apply",
      { schemaVersion: 1, planId: "opaque-plan" },
      credential
    );

    assert.equal(response.status, 400);
    assert.equal(response.payload.error, "The state database is busy.");
    assert.equal(response.payload.code, undefined);
    assert.deepEqual(response.payload.coreError, {
      code: "SQLITE_BUSY",
      message: "The state database is busy.",
      severity: "warning",
      retryable: true,
      recoveryRequired: false,
      details: { causeCode: "SQLITE_BUSY" }
    });
  } finally {
    await handle.close();
  }
});

test("Web status forwards the Core last-complete snapshot without reading or mixing live storage", async () => {
  let configReads = 0;
  const coreStatus = statusFixture({
    schemaVersion: 1,
    snapshotAt: "2026-08-25T00:00:00.000Z",
    storageRevision: "cached-storage-revision",
    profile: { id: "default", revision: "trusted-profile-revision" },
    profileId: "default",
    profileRevision: "server-profile-revision",
    currentProvider: "openai",
    operationInProgress: {
      operationId: "external-operation",
      operation: "sync",
      actor: "external",
      busyScope: "codex-home"
    }
  });
  const handle = await startFixture({
    readConfigText: async () => { configReads += 1; throw new Error("status must not read config in Web"); },
    getStatus: async () => structuredClone(coreStatus)
  });
  try {
    const { credential } = await handle.pair();
    const response = await api(handle, "/api/status", { profileId: "default" }, credential);
    assert.equal(response.status, 200);
    assert.equal(configReads, 0);
    assert.equal(response.payload.status.storageRevision, coreStatus.storageRevision);
    assert.deepEqual(response.payload.status.operationInProgress, coreStatus.operationInProgress);
    assert.equal(response.payload.status.currentProvider, "openai");
  } finally {
    await handle.close();
  }
});

test("Web Switch and Restore Apply reject every field beyond schemaVersion and planId", async () => {
  let switchCalls = 0;
  let restoreCalls = 0;
  const handle = await startFixture({
    applySwitch: async () => { switchCalls += 1; },
    applyRestore: async () => { restoreCalls += 1; }
  });
  try {
    const { credential } = await handle.pair();
    const switchResponse = await api(handle, "/api/switch/apply", {
      schemaVersion: 1,
      planId: "switch-plan",
      provider: "attacker"
    }, credential);
    const restoreResponse = await api(handle, "/api/restore/apply", {
      schemaVersion: 1,
      planId: "restore-plan",
      backupId: "attacker"
    }, credential);
    assert.equal(switchResponse.status, 400);
    assert.equal(restoreResponse.status, 400);
    assert.equal(switchResponse.payload.coreError.code, "INVALID_INPUT");
    assert.equal(restoreResponse.payload.coreError.code, "INVALID_INPUT");
    assert.equal(switchCalls, 0);
    assert.equal(restoreCalls, 0);
  } finally {
    await handle.close();
  }
});

test("Web Restore preparation delegates managed backup membership validation to Core", async () => {
  let applies = 0;
  const handle = await startFixture({
    prepareRestore: async ({ backupId }) => {
      assert.equal(backupId, "../../outside");
      throw new CoreError("RESTORE_VALIDATION_FAILED", "The selected backup is not managed by this Codex Home.");
    },
    applyRestore: async () => { applies += 1; }
  });
  try {
    const { credential } = await handle.pair();
    const response = await api(handle, "/api/restore/prepare", { profileId: "default", backupId: "../../outside", restoreDatabase: true }, credential);
    assert.equal(response.status, 400);
    assert.equal(response.payload.coreError.code, "RESTORE_VALIDATION_FAILED");
    assert.equal(applies, 0);
  } finally {
    await handle.close();
  }
});

test("Web UI history endpoints delegate through the selected profile", async () => {
  const calls = [];
  const handle = await startFixture({
    listHistory: async (codexHome, options) => { calls.push(["list", codexHome, options]); return { page: 1, pageSize: 50, total: 1, hasNextPage: false, sessions: [{ id: "thread", title: "safe" }] }; },
    getHistorySession: async (codexHome, sessionId) => { calls.push(["detail", codexHome, sessionId]); return { session: { id: sessionId }, messages: [], truncated: false, returnedMessageCount: 0 }; }
  });
  try {
    const { credential } = await handle.pair();
    assert.equal((await api(handle, "/api/history", { profileId: "default", query: "safe" }, credential)).status, 200);
    assert.equal((await api(handle, "/api/history/session", { profileId: "default", sessionId: "thread" }, credential)).status, 200);
    assert.deepEqual(calls.map((call) => call[0]), ["list", "detail"]);
  } finally {
    await handle.close();
  }
});

test("Web UI opens a no-thread-id history session from a rollout path longer than the API id limit", async (t) => {
  const handle = await startFixture();
  try {
    const deepSegments = Array.from({ length: 7 }, (_, index) => `segment-${index}-${"x".repeat(32)}`);
    const rolloutPath = path.join(handle.root, "sessions", ...deepSegments, "rollout-no-thread-id.jsonl");
    assert.ok(path.resolve(rolloutPath).length > 300);
    try {
      await fs.mkdir(path.dirname(rolloutPath), { recursive: true });
      await fs.writeFile(rolloutPath, [
        { type: "session_meta", timestamp: "2026-08-04T08:00:00.000Z", payload: { title: "Long fallback", cwd: "/work/long", model_provider: "openai" } },
        { type: "event_msg", timestamp: "2026-08-04T08:01:00.000Z", payload: { type: "user_message", message: "Open this session" } }
      ].map((line) => JSON.stringify(line)).join("\n") + "\n", "utf8");
    } catch (error) {
      if (process.platform === "win32" && ["ENAMETOOLONG", "ENOENT"].includes(error?.code)) {
        t.skip("Windows long-path support is unavailable on this host.");
        return;
      }
      throw error;
    }

    const { credential } = await handle.pair();
    const listed = await api(handle, "/api/history", { profileId: "default" }, credential);
    assert.equal(listed.status, 200);
    assert.equal(listed.payload.history.total, 1);
    const [session] = listed.payload.history.sessions;
    assert.match(session.id, /^rollout:[A-Za-z0-9_-]{43}$/);
    assert.equal(session.rolloutPath, path.resolve(rolloutPath));

    const detail = await api(handle, "/api/history/session", {
      profileId: "default",
      sessionId: session.id
    }, credential);
    assert.equal(detail.status, 200);
    assert.equal(detail.payload.history.session.id, session.id);
    assert.equal(detail.payload.history.messages[0].text, "Open this session");
  } finally {
    await handle.close();
  }
});

test("device credentials persist only as hashes and reset invalidates them", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-state-"));
  const filePath = path.join(root, "web-state.json");
  const credential = "device-secret-that-must-not-be-persisted-verbatim";
  try {
    const first = new WebUiStateStore({ filePath, defaultProfile: { codexHome: root } });
    await first.initialize();
    await first.addCredential(credential);
    const serialized = await fs.readFile(filePath, "utf8");
    assert.doesNotMatch(serialized, new RegExp(credential));

    const restarted = new WebUiStateStore({ filePath, defaultProfile: { codexHome: root } });
    await restarted.initialize();
    assert.equal(restarted.hasCredential(credential), true);
    await restarted.resetCredentials();
    assert.equal(restarted.hasCredential(credential), false);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("startWebUi listens only on IPv4 loopback and reuses its existing instance", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-start-"));
  const webRoot = path.join(root, "web");
  const stateFile = path.join(root, "state.json");
  const runtimeFile = path.join(root, "runtime.json");
  await fs.mkdir(webRoot);
  await fs.writeFile(path.join(webRoot, "index.html"), "<!doctype html><title>fixture</title>");
  let first;
  try {
    first = await startWebUi({ port: 0, openBrowser: false, codexHome: root, stateFile, runtimeFile, webRoot });
    assert.equal(first.server.address().address, "127.0.0.1");
    const initialToken = new URL(first.pairingUrl).hash.slice("#pair=".length);
    const paired = await request({
      origin: first.url,
      pathname: "/api/pair",
      method: "POST",
      headers: { Origin: first.url, "X-Codex-Provider-Pairing": initialToken }
    });
    const originalCredential = paired.payload.deviceCredential;
    const second = await startWebUi({ port: 0, openBrowser: false, resetAccess: true, codexHome: root, stateFile, runtimeFile, webRoot });
    assert.equal(second.reused, true);
    assert.equal(second.url, first.url);
    assert.match(second.pairingUrl, /#pair=/);
    const invalidated = await request({
      origin: first.url,
      pathname: "/api/status",
      method: "POST",
      body: { profileId: "default" },
      headers: { Origin: first.url, "X-Codex-Provider-Device": originalCredential }
    });
    assert.equal(invalidated.status, 403);
  } finally {
    await first?.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("startWebUi never discloses its runtime secret and preserves unverifiable descriptors", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-runtime-auth-"));
  const webRoot = path.join(root, "web");
  const stateFile = path.join(root, "state.json");
  const runtimeFile = path.join(root, "runtime.json");
  const internalSecret = "S".repeat(43);
  const instanceId = "I".repeat(43);
  const captured = [];
  let fakePort = null;
  let rejectChallenge = false;
  await fs.mkdir(webRoot);
  await fs.writeFile(path.join(webRoot, "index.html"), "<!doctype html><title>fixture</title>");
  const fake = http.createServer(async (incoming, response) => {
    const chunks = [];
    for await (const chunk of incoming) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString("utf8");
    captured.push({ headers: incoming.headers, text });
    if (rejectChallenge) {
      const payload = JSON.stringify({ error: "not found" });
      response.writeHead(404, { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) });
      response.end(payload);
      return;
    }
    const body = JSON.parse(text || "{}");
    const payload = JSON.stringify(incoming.url === "/api/internal/challenge"
      ? {
          protocolVersion: 2,
          port: fakePort,
          instanceId,
          nonce: "N".repeat(43)
        }
      : {
          protocolVersion: 2,
          port: fakePort,
          instanceId,
          nonce: body.nonce,
          resetAccess: body.resetAccess,
          pairingToken: "attacker-controlled-token",
          proof: "A".repeat(43)
        });
    response.writeHead(200, { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) });
    response.end(payload);
  });
  try {
    await new Promise((resolve, reject) => {
      fake.once("error", reject);
      fake.listen(0, "127.0.0.1", resolve);
    });
    fakePort = fake.address().port;
    await fs.writeFile(runtimeFile, `${JSON.stringify({
      protocolVersion: 2,
      instanceId,
      port: fakePort,
      internalSecret,
      codexHome: path.resolve(root),
      sqliteHome: path.join(path.resolve(root), "sqlite")
    })}\n`);

    await assert.rejects(
      startWebUi({ port: 0, openBrowser: false, codexHome: root, stateFile, runtimeFile, webRoot }),
      /authenticated Web UI pairing/
    );
    assert.equal(captured.length, 2);
    assert.ok(captured.every((entry) => entry.headers["x-codex-provider-internal"] === undefined));
    assert.doesNotMatch(JSON.stringify(captured), new RegExp(internalSecret));
    assert.equal(JSON.parse(await fs.readFile(runtimeFile, "utf8")).internalSecret, internalSecret);

    rejectChallenge = true;
    await assert.rejects(
      startWebUi({ port: 0, openBrowser: false, codexHome: root, stateFile, runtimeFile, webRoot }),
      /could not complete secure Web UI authentication/
    );
    assert.doesNotMatch(JSON.stringify(captured), new RegExp(internalSecret));
    assert.equal(JSON.parse(await fs.readFile(runtimeFile, "utf8")).internalSecret, internalSecret);
  } finally {
    await new Promise((resolve) => fake.close(resolve));
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("startWebUi refuses to reuse an instance launched with a different SQLite home", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-sqlite-home-"));
  const webRoot = path.join(root, "web");
  const stateFile = path.join(root, "state.json");
  const runtimeFile = path.join(root, "runtime.json");
  const homeA = path.join(root, "sqlite-a");
  const homeB = path.join(root, "sqlite-b");
  await fs.mkdir(webRoot);
  await fs.writeFile(path.join(webRoot, "index.html"), "<!doctype html><title>fixture</title>");
  let first;
  try {
    first = await startWebUi({ port: 0, openBrowser: false, codexHome: root, sqliteHome: homeA, stateFile, runtimeFile, webRoot });

    // Launching with a different SQLite home must not silently reuse instance A.
    await assert.rejects(
      startWebUi({ port: 0, openBrowser: false, codexHome: root, sqliteHome: homeB, stateFile, runtimeFile, webRoot }),
      /resolved Codex Home/
    );

    // The rejection must leave the original instance and its runtime descriptor intact.
    const health = await request({ origin: first.url, pathname: "/api/health" });
    assert.equal(health.status, 200);
    assert.equal(JSON.parse(await fs.readFile(runtimeFile, "utf8")).port, new URL(first.url).port * 1);

    // Relaunching with the same normalized SQLite home still reuses the instance.
    const relaunched = await startWebUi({
      port: 0,
      openBrowser: false,
      codexHome: root,
      sqliteHome: path.join(homeA, "."),
      stateFile,
      runtimeFile,
      webRoot
    });
    assert.equal(relaunched.reused, true);
    assert.equal(relaunched.url, first.url);
  } finally {
    await first?.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("startWebUi reports occupied ports clearly and handles unavailable or headless browser openers", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-port-"));
  const webRoot = path.join(root, "web");
  await fs.mkdir(webRoot);
  await fs.writeFile(path.join(webRoot, "index.html"), "<!doctype html><title>fixture</title>");
  const occupied = http.createServer();
  await new Promise((resolve, reject) => {
    occupied.once("error", reject);
    occupied.listen(0, "127.0.0.1", resolve);
  });
  let handle;
  try {
    const occupiedPort = occupied.address().port;
    await assert.rejects(
      startWebUi({
        port: occupiedPort,
        openBrowser: false,
        codexHome: root,
        stateFile: path.join(root, "occupied-state.json"),
        runtimeFile: path.join(root, "occupied-runtime.json"),
        webRoot
      }),
      /port .* already in use by another program/
    );
    handle = await startWebUi({
      port: 0,
      openBrowser: true,
      platform: "linux",
      environment: { DISPLAY: ":0" },
      openUrl: async () => false,
      codexHome: root,
      stateFile: path.join(root, "headless-state.json"),
      runtimeFile: path.join(root, "headless-runtime.json"),
      webRoot
    });
    assert.equal(handle.browserOpened, false);
    assert.match(handle.pairingUrl, /#pair=/);
    await handle.close();
    handle = null;

    let openAttempts = 0;
    handle = await startWebUi({
      port: 0,
      openBrowser: true,
      platform: "linux",
      environment: {},
      openUrl: async () => {
        openAttempts += 1;
        return true;
      },
      codexHome: root,
      stateFile: path.join(root, "no-display-state.json"),
      runtimeFile: path.join(root, "no-display-runtime.json"),
      webRoot
    });
    assert.equal(openAttempts, 0);
    assert.equal(handle.browserOpened, false);
    assert.match(handle.pairingUrl, /#pair=/);
  } finally {
    await handle?.close();
    await new Promise((resolve) => occupied.close(resolve));
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Web UI operations require a current profile revision and preserve a captured profile snapshot", async () => {
  let prepareCalls = 0;
  const handle = await startFixture({ prepareSync: async () => { prepareCalls += 1; return {}; } });
  try {
    const first = await handle.pair();
    const second = await handle.pair();
    await api(handle, "/api/profiles/save", {
      profileId: "work",
      name: "Work",
      codexHome: "/tmp/work-before"
    }, first.credential);
    const stale = handle.stateStore.getProfile("work");
    await api(handle, "/api/profiles/save", {
      profileId: "work",
      name: "Work updated",
      codexHome: "/tmp/work-after",
      profileRevision: stale.revision
    }, second.credential);

    const changed = await request({
      origin: handle.origin,
      pathname: "/api/sync/prepare",
      method: "POST",
      body: { profileId: "work", profileRevision: stale.revision, provider: "openai", keepCount: 5 },
      headers: { Origin: handle.origin, "X-Codex-Provider-Device": first.credential }
    });
    assert.equal(changed.status, 409);
    assert.equal(changed.payload.code, "PROFILE_CHANGED");
    assert.equal(changed.payload.profile.codexHome, path.resolve("/tmp/work-after"));
    assert.equal(prepareCalls, 0);

    const required = await request({
      origin: handle.origin,
      pathname: "/api/sync/prepare",
      method: "POST",
      body: { profileId: "work", provider: "openai", keepCount: 5 },
      headers: { Origin: handle.origin, "X-Codex-Provider-Device": first.credential }
    });
    assert.equal(required.status, 409);
    assert.equal(required.payload.code, "PROFILE_REVISION_REQUIRED");
    assert.equal(prepareCalls, 0);
  } finally {
    await handle.close();
  }
});

test("Web UI profile save and delete reject missing or stale revisions without changing the profile", async () => {
  const handle = await startFixture();
  try {
    const { credential } = await handle.pair();
    await api(handle, "/api/profiles/save", {
      profileId: "work",
      name: "Work",
      codexHome: "/tmp/work-before"
    }, credential);
    const initial = handle.stateStore.getProfile("work");

    const missingSaveRevision = await api(handle, "/api/profiles/save", {
      profileId: "work",
      name: "Should not overwrite",
      codexHome: "/tmp/work-missing"
    }, credential);
    assert.equal(missingSaveRevision.status, 409);
    assert.equal(missingSaveRevision.payload.code, "PROFILE_REVISION_REQUIRED");
    assert.equal(handle.stateStore.getProfile("work").codexHome, path.resolve("/tmp/work-before"));

    await api(handle, "/api/profiles/save", {
      profileId: "work",
      name: "Work updated",
      codexHome: "/tmp/work-after",
      profileRevision: initial.revision
    }, credential);
    const missingDeleteRevision = await api(handle, "/api/profiles/delete", {
      profileId: "work"
    }, credential);
    assert.equal(missingDeleteRevision.status, 409);
    assert.equal(missingDeleteRevision.payload.code, "PROFILE_REVISION_REQUIRED");
    assert.equal(handle.stateStore.hasProfile("work"), true);

    const staleDelete = await api(handle, "/api/profiles/delete", {
      profileId: "work",
      profileRevision: initial.revision
    }, credential);
    assert.equal(staleDelete.status, 409);
    assert.equal(staleDelete.payload.code, "PROFILE_CHANGED");
    assert.equal(handle.stateStore.hasProfile("work"), true);

    const current = handle.stateStore.getProfile("work");
    const deleted = await api(handle, "/api/profiles/delete", {
      profileId: "work",
      profileRevision: current.revision
    }, credential);
    assert.equal(deleted.status, 200);
    assert.equal(handle.stateStore.hasProfile("work"), false);
  } finally {
    await handle.close();
  }
});

test("Web UI state profile saves apply revision checks after asynchronous validation", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-profile-cas-"));
  let gateActive = false;
  let arrivals = 0;
  let releaseGate = () => {};
  let gate = Promise.resolve();
  const resetGate = () => {
    arrivals = 0;
    gate = new Promise((resolve) => { releaseGate = resolve; });
  };
  const store = new WebUiStateStore({
    filePath: path.join(root, "state.json"),
    defaultProfile: { codexHome: root },
    validateDirectory: async (_value, label) => {
      if (!gateActive || label !== "Codex Home") return;
      arrivals += 1;
      if (arrivals === 2) releaseGate();
      await gate;
    }
  });
  try {
    await store.initialize();
    await store.saveProfile({ id: "work", name: "Initial", codexHome: root });
    const revision = store.getProfile("work").revision;

    resetGate();
    gateActive = true;
    const updates = await Promise.allSettled([
      store.saveProfile({ id: "work", name: "First", codexHome: root }, { expectedRevision: revision }),
      store.saveProfile({ id: "work", name: "Second", codexHome: root }, { expectedRevision: revision })
    ]);
    assert.equal(updates.filter((result) => result.status === "fulfilled").length, 1);
    const rejectedUpdate = updates.find((result) => result.status === "rejected");
    assert.equal(rejectedUpdate.reason.code, "PROFILE_CHANGED");
    assert.ok(["First", "Second"].includes(store.getProfile("work").name));

    resetGate();
    const creates = await Promise.allSettled([
      store.saveProfile({ id: "new", name: "First create", codexHome: root }),
      store.saveProfile({ id: "new", name: "Second create", codexHome: root })
    ]);
    assert.equal(creates.filter((result) => result.status === "fulfilled").length, 1);
    const rejectedCreate = creates.find((result) => result.status === "rejected");
    assert.equal(rejectedCreate.reason.code, "PROFILE_REVISION_REQUIRED");
  } finally {
    gateActive = false;
    releaseGate();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Web UI marks skipped locked rollout files as a partial operation outcome", async () => {
  const handle = await startFixture({
    applySync: async () => ({
      schemaVersion: 1,
      operationId: "partial-operation",
      operation: "sync",
      outcome: "partial",
      backup: null,
      warnings: [],
      result: { skippedLockedRolloutFiles: ["rollout-active.jsonl"] }
    })
  });
  try {
    const { credential } = await handle.pair();
    const response = await api(handle, "/api/sync/apply", { schemaVersion: 1, planId: "partial-plan" }, credential);
    assert.equal(response.status, 200);
    assert.equal(response.payload.result.outcome, "partial");
  } finally {
    await handle.close();
  }
});

test("Web UI restore requires an explicit SQLite Home for relocation and rejects WSL UNC storage", async () => {
  let restorePrepareCalls = 0;
  const handle = await startFixture({ prepareRestore: async () => { restorePrepareCalls += 1; return {}; } });
  try {
    const { credential } = await handle.pair();
    const relocation = await api(handle, "/api/restore/prepare", {
      profileId: "default",
      backupId: "known",
      restoreDatabase: true,
      allowSqliteHomeRelocation: true
    }, credential);
    assert.equal(relocation.status, 400);
    assert.match(relocation.payload.error, /explicit SQLite Home target/);
    assert.equal(restorePrepareCalls, 0);
  } finally {
    await handle.close();
  }

  const rawWslUnc = "\\\\wsl.localhost\\Ubuntu\\home\\user\\.codex\\sqlite";
  const wslCodexHome = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-wsl-unc-"));
  const baseWslStore = createMemoryWebUiState({ codexHome: wslCodexHome });
  const wslStore = {
    ...baseWslStore,
    getProfile(profileId) {
      return { ...baseWslStore.getProfile(profileId), sqliteHome: rawWslUnc };
    }
  };
  assert.equal(wslStore.getProfile("default").sqliteHome, rawWslUnc);

  let wslPrepareCalls = 0;
  const wslHandle = await startFixture(
    {
      readConfigText: async () => 'model_provider = "openai"\n',
      prepareSync: async (options) => {
        wslPrepareCalls += 1;
        assert.equal(options.sqliteHome, rawWslUnc);
        throw new CoreError("SQLITE_UNSUPPORTED_PATH", "Windows cannot safely access SQLite through the WSL UNC path.", {
          details: { reason: "windows-wsl-unc" }
        });
      }
    },
    { platform: "win32", stateStore: wslStore }
  );
  try {
    const { credential } = await wslHandle.pair();
    const rejected = await api(wslHandle, "/api/sync/prepare", { profileId: "default", provider: "openai", keepCount: 5 }, credential);
    assert.equal(rejected.status, 400);
    assert.match(rejected.payload.error, /Windows cannot safely access SQLite through the WSL UNC path/);
    assert.equal(rejected.payload.coreError.code, "SQLITE_UNSUPPORTED_PATH");
    assert.deepEqual(rejected.payload.coreError.details, { reason: "windows-wsl-unc" });
    assert.equal(wslPrepareCalls, 1);
  } finally {
    await wslHandle.close();
    await fs.rm(wslCodexHome, { recursive: true, force: true });
  }
});

test("startWebUi reuses only a matching effective storage identity and replaces dead descriptors", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-runtime-identity-"));
  const webRoot = path.join(root, "web");
  const stateFile = path.join(root, "state.json");
  const runtimeFile = path.join(root, "runtime.json");
  const homeA = path.join(root, "sqlite-a");
  const homeB = path.join(root, "sqlite-b");
  await fs.mkdir(webRoot);
  await fs.writeFile(path.join(webRoot, "index.html"), "<!doctype html><title>fixture</title>");
  let first;
  try {
    first = await startWebUi({ port: 0, openBrowser: false, codexHome: root, stateFile, runtimeFile, webRoot, environment: { CODEX_SQLITE_HOME: homeA }, platform: "linux" });
    const descriptor = JSON.parse(await fs.readFile(runtimeFile, "utf8"));
    assert.equal(descriptor.codexHome, path.resolve(root));
    assert.equal(descriptor.sqliteHome, path.resolve(homeA));

    const reused = await startWebUi({ port: 0, openBrowser: false, codexHome: root, stateFile, runtimeFile, webRoot, environment: { CODEX_SQLITE_HOME: homeA }, platform: "linux" });
    assert.equal(reused.reused, true);
    await assert.rejects(
      startWebUi({ port: 0, openBrowser: false, codexHome: root, stateFile, runtimeFile, webRoot, environment: { CODEX_SQLITE_HOME: homeB }, platform: "linux" }),
      /resolved Codex Home/
    );
    await first.close();
    first = null;

    await fs.writeFile(runtimeFile, `${JSON.stringify({
      protocolVersion: 2,
      instanceId: "I".repeat(43),
      port: 9,
      internalSecret: "S".repeat(43),
      codexHome: path.resolve(root),
      sqliteHome: path.resolve(homeA)
    })}\n`);
    first = await startWebUi({ port: 0, openBrowser: false, codexHome: root, stateFile, runtimeFile, webRoot, environment: { CODEX_SQLITE_HOME: homeA }, platform: "linux" });
    assert.notEqual(JSON.parse(await fs.readFile(runtimeFile, "utf8")).port, 9);
  } finally {
    await first?.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("runtime identity ignores path case only for win32", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-runtime-case-"));
  const webRoot = path.join(root, "web");
  const stateFile = path.join(root, "state.json");
  const runtimeFile = path.join(root, "runtime.json");
  await fs.mkdir(webRoot);
  await fs.writeFile(path.join(webRoot, "index.html"), "<!doctype html><title>fixture</title>");
  let first;
  try {
    first = await startWebUi({ port: 0, openBrowser: false, codexHome: root, sqliteHome: path.join(root, "Sqlite"), stateFile, runtimeFile, webRoot, platform: "win32" });
    const win32Reuse = await startWebUi({ port: 0, openBrowser: false, codexHome: root, sqliteHome: path.join(root, "sqlite"), stateFile, runtimeFile, webRoot, platform: "win32" });
    assert.equal(win32Reuse.reused, true);
    await assert.rejects(
      startWebUi({ port: 0, openBrowser: false, codexHome: root, sqliteHome: path.join(root, "sqlite"), stateFile, runtimeFile, webRoot, platform: "linux" }),
      /resolved Codex Home/
    );
  } finally {
    await first?.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});
