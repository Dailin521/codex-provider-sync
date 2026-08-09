import assert from "node:assert/strict";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import test from "node:test";

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
  const stateStore = options.stateStore ?? createMemoryWebUiState({ codexHome: "/tmp/.codex" });
  const handle = createWebUiServer({ webRoot: root, services, stateStore, now: options.now, pairingTtlMs: options.pairingTtlMs });
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
  return request({
    origin: handle.origin,
    pathname,
    method: "POST",
    body,
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
    ...overrides
  };
}

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

    const saved = await api(handle, "/api/profiles/save", {
      profileId: "work",
      name: "Work",
      codexHome: "/tmp/work-codex",
      sqliteHome: "/tmp/work-sqlite"
    }, credential);
    assert.equal(saved.status, 200);

    const response = await api(handle, "/api/status", { profileId: "work" }, credential);
    assert.equal(response.status, 200);
    assert.equal(calls.at(-1).codexHome, path.resolve("/tmp/work-codex"));
    assert.equal(calls.at(-1).sqliteHome, path.resolve("/tmp/work-sqlite"));
  } finally {
    await handle.close();
  }
});

test("Web UI sync delegates only server-resolved storage to the shared service", async () => {
  const calls = [];
  const handle = await startFixture({
    readConfigText: async () => 'model_provider = "openai"\nmodel = "gpt-5"\n',
    readRootModelFromConfigText: () => "gpt-5",
    runSync: async (options) => { calls.push(options); options.onProgress({ stage: "create_backup", status: "start" }); return { targetProvider: options.provider, backupDir: "/tmp/backup" }; }
  });
  try {
    const { credential } = await handle.pair();
    const invalid = await api(handle, "/api/sync", { profileId: "default", provider: "bad provider", keepCount: 5 }, credential);
    assert.equal(invalid.status, 400);
    const response = await api(handle, "/api/sync", { profileId: "default", provider: "openai", keepCount: 5 }, credential);
    assert.equal(response.status, 200);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].codexHome, path.resolve("/tmp/.codex"));
    assert.equal(calls[0].model, "gpt-5");
    assert.ok(handle.getActivity().some((entry) => entry.message === "Creating backup"));
  } finally {
    await handle.close();
  }
});

test("Web UI restore only accepts managed backups for the selected profile", async () => {
  let restored = false;
  const handle = await startFixture({
    listBackups: async () => ({ backupRoot: "/tmp/.codex/backups_state/provider-sync", backups: [{ id: "known", path: "/tmp/.codex/backups_state/provider-sync/known", metadata: {} }] }),
    runRestore: async () => { restored = true; return { targetProvider: "openai" }; }
  });
  try {
    const { credential } = await handle.pair();
    const response = await api(handle, "/api/restore", { profileId: "default", backupId: "../../outside", restoreDatabase: true, restoreSessions: true }, credential);
    assert.equal(response.status, 400);
    assert.equal(restored, false);
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
