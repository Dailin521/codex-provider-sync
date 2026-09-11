import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import { appendFileSync } from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import test, { afterEach } from "node:test";

import { openDatabase } from "../src/sqlite.js";
import { createWebCoreFacade } from "../src/web-core-adapter.js";
import { createWebUiServer } from "../src/web-server.js";
import { createMemoryWebUiState } from "../src/web-state.js";
import { getStatus } from "../packages/core/src/application/status.js";
import { acquireLock } from "../src/locking.js";
import { getDiagnostics } from "../packages/core/src/application/diagnostics.js";
import { prepareSync, applySync } from "../src/service.js";

// Node 16 has afterEach, but no TestContext.mock. Keep these filesystem
// injections test-local and restore them even when a tested assertion fails.
const methodRestores = [];
const methodMocks = {
  method(object, key, implementation) {
    const original = object[key];
    methodRestores.push(() => { object[key] = original; });
    object[key] = implementation;
  },
  restoreAll() { for (const restore of methodRestores.splice(0).reverse()) restore(); }
};
afterEach(() => methodMocks.restoreAll());

async function request(origin, pathname, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const target = new URL(origin);
    const serialized = body === undefined ? null : JSON.stringify(body);
    const client = http.request({
      hostname: target.hostname,
      port: target.port,
      path: pathname,
      method: "POST",
      headers: {
        ...(serialized ? {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(serialized)
        } : {}),
        ...headers
      }
    }, (response) => {
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("end", () => {
        const text = Buffer.concat(chunks).toString("utf8");
        resolve({ status: response.statusCode, payload: JSON.parse(text) });
      });
    });
    client.once("error", reject);
    if (serialized) client.write(serialized);
    client.end();
  });
}

async function makeFixture() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-status-lock-"));
  const codexHome = path.join(root, ".codex");
  const sqliteHome = path.join(root, "shared-sqlite");
  const stateDbPath = path.join(sqliteHome, "state_5.sqlite");
  const rolloutPath = path.join(codexHome, "sessions", "2026", "08", "25", "rollout-status.jsonl");
  await fs.mkdir(path.dirname(rolloutPath), { recursive: true });
  await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
  await fs.mkdir(sqliteHome, { recursive: true });
  const configText = (provider) => [
    `model_provider = "${provider}"`,
    `sqlite_home = ${JSON.stringify(sqliteHome)}`,
    ""
  ].join("\n");
  await fs.writeFile(path.join(codexHome, "config.toml"), configText("openai"), "utf8");
  await fs.writeFile(rolloutPath, `${JSON.stringify({
    timestamp: "2026-08-25T00:00:00.000Z",
    type: "session_meta",
    payload: {
      id: "status-thread",
      model_provider: "openai",
      cwd: "C:\\AITemp"
    }
  })}\n`, "utf8");
  const database = await openDatabase(stateDbPath);
  try {
    database.exec(`
      CREATE TABLE threads (
        id TEXT PRIMARY KEY,
        model_provider TEXT,
        cwd TEXT NOT NULL DEFAULT '',
        archived INTEGER NOT NULL DEFAULT 0,
        first_user_message TEXT NOT NULL DEFAULT '',
        model TEXT
      )
    `);
    database.prepare("INSERT INTO threads (id, model_provider, cwd, archived, first_user_message) VALUES (?, ?, ?, ?, ?)")
      .run("status-thread", "openai", "C:\\AITemp", 0, "redacted");
  } finally {
    database.close();
  }
  return { root, codexHome, sqliteHome, stateDbPath, rolloutPath, configText };
}

async function startRealWeb(codexHome, webRoot) {
  await fs.mkdir(webRoot, { recursive: true });
  await fs.writeFile(path.join(webRoot, "index.html"), "<!doctype html><title>status</title>", "utf8");
  const stateStore = createMemoryWebUiState({ codexHome });
  const coreFacade = createWebCoreFacade(stateStore);
  const handle = createWebUiServer({ webRoot, stateStore, services: { coreFacade } });
  await new Promise((resolve, reject) => {
    handle.server.once("error", reject);
    handle.server.listen(0, "127.0.0.1", resolve);
  });
  const address = handle.server.address();
  const origin = `http://127.0.0.1:${address.port}`;
  handle.setBaseUrl(origin);
  const paired = await request(origin, "/api/pair", undefined, {
    Origin: origin,
    "X-Codex-Provider-Pairing": handle.issuePairing()
  });
  assert.equal(paired.status, 200);
  const credential = paired.payload.deviceCredential;
  return {
    handle,
    coreFacade,
    stateStore,
    origin,
    async status() {
      return request(origin, "/api/status", { profileId: "default" }, {
        Origin: origin,
        "X-Codex-Provider-Device": credential
      });
    },
    async close() {
      await new Promise((resolve, reject) => handle.server.close((error) => error ? reject(error) : resolve()));
    }
  };
}

async function startHolder({ mode, codexHome, stateDbPath, configText = "" }) {
  const lockingUrl = new URL("../src/locking.js", import.meta.url).href;
  const stateLockUrl = new URL("../src/state-db-lock.js", import.meta.url).href;
  const sqliteUrl = new URL("../src/sqlite.js", import.meta.url).href;
  const script = `
    import fs from "node:fs/promises";
    import path from "node:path";
    import { once } from "node:events";
    import { acquireLock } from ${JSON.stringify(lockingUrl)};
    import { acquireStateDbLock } from ${JSON.stringify(stateLockUrl)};
    import { openDatabase } from ${JSON.stringify(sqliteUrl)};
    const mode = process.env.STATUS_HOLDER_MODE;
    const codexHome = process.env.STATUS_CODEX_HOME;
    const stateDbPath = process.env.STATUS_STATE_DB;
    const releaseHome = mode === "home" ? await acquireLock(codexHome, "external-status-home") : null;
    const heldState = await acquireStateDbLock(stateDbPath, mode === "home" ? "external-status-home" : "external-status-db");
    try {
      if (mode === "home") {
        await fs.writeFile(path.join(codexHome, "config.toml"), process.env.STATUS_CONFIG_TEXT, "utf8");
      } else {
        const database = await openDatabase(stateDbPath);
        try {
          database.prepare("UPDATE threads SET model_provider = ? WHERE id = ?").run("external", "status-thread");
        } finally {
          database.close();
        }
      }
      process.stdout.write(JSON.stringify({ ready: true }) + "\\n");
      await once(process.stdin, "data");
    } finally {
      await heldState.release();
      if (releaseHome) await releaseHome();
    }
  `;
  const child = spawn(process.execPath, ["--input-type=module", "-e", script], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      STATUS_HOLDER_MODE: mode,
      STATUS_CODEX_HOME: codexHome,
      STATUS_STATE_DB: stateDbPath,
      STATUS_CONFIG_TEXT: configText
    },
    stdio: ["pipe", "pipe", "pipe"]
  });
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  const ready = await new Promise((resolve, reject) => {
    let stdout = "";
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
      const lineEnd = stdout.indexOf("\n");
      if (lineEnd < 0) return;
      try {
        resolve(JSON.parse(stdout.slice(0, lineEnd)));
      } catch (error) {
        reject(error);
      }
    });
    child.once("error", reject);
    child.once("exit", (code) => {
      if (code !== null) reject(new Error(`Status lock holder exited ${code} before ready: ${stderr}`));
    });
  });
  assert.equal(ready.ready, true);
  return async () => {
    child.stdin.end("release\n");
    const code = await new Promise((resolve) => child.once("exit", resolve));
    assert.equal(code, 0, stderr);
  };
}

function asLastComplete(status) {
  const value = JSON.parse(JSON.stringify(status));
  value.operationInProgress = null;
  delete value.statusReadBlocked;
  delete value.alignment;
  return value;
}

test("stale Home lock does not trap Status and is reclaimed through ordinary Plan/Apply", async () => {
  const fixture = await makeFixture();
  try {
    const lockDir = path.join(fixture.codexHome, "tmp", "provider-sync.lock");
    await fs.mkdir(lockDir, { recursive: true });
    const ownerPath = path.join(lockDir, "owner.json");
    const ownerText = JSON.stringify({ pid: 2147483647, processStartMarker: "windows:1" });
    await fs.writeFile(ownerPath, ownerText);
    const options = { codexHome: fixture.codexHome, sqliteHome: fixture.sqliteHome };
    for (let attempt = 0; attempt < 2; attempt++) {
      const status = await getStatus(options);
      assert.equal(status.operationInProgress, null);
      assert.equal(status.staleLockDetected, true);
      assert.equal(status.statusReadBlocked, undefined);
      assert.equal(await fs.readFile(ownerPath, "utf8"), ownerText);
    }
    const plan = await prepareSync(options);
    const result = await applySync({ schemaVersion: 1, planId: plan.planId });
    assert.equal(result.outcome, "completed");
    const status = await getStatus(options);
    assert.equal(status.operationInProgress, null);
    assert.equal(status.staleLockDetected, undefined);
    await assert.rejects(fs.stat(ownerPath), { code: "ENOENT" });
  } finally {
    await fs.rm(fixture.root, { recursive: true, force: true });
  }
});

test("Core and Web Status block on the Home lock and ignore legacy State DB resource locks", async () => {
  const fixture = await makeFixture();
  const web = await startRealWeb(fixture.codexHome, path.join(fixture.root, "web"));
  let releaseHolder = null;
  try {
    const profileRevision = web.stateStore.getProfile("default").revision;
    const statusOptions = {
      profile: { profileId: "default", profileRevision }
    };
    const readCoreStatus = () => web.coreFacade.getStatus(statusOptions);
    const baselineWebResponse = await web.status();
    assert.equal(baselineWebResponse.status, 200);
    const baselineCore = await readCoreStatus();

    releaseHolder = await startHolder({
      mode: "home",
      codexHome: fixture.codexHome,
      stateDbPath: fixture.stateDbPath,
      configText: fixture.configText("external")
    });
    const blockedCore = await readCoreStatus();
    const blockedWebResponse = await web.status();
    assert.equal(blockedCore.operationInProgress.actor, "external");
    assert.equal(blockedCore.operationInProgress.busyScope, "codex-home");
    assert.equal(blockedCore.statusReadBlocked.reason, "codex-home-lock");
    assert.deepEqual(asLastComplete(blockedCore), asLastComplete(baselineCore));
    assert.equal(blockedWebResponse.status, 200);
    assert.equal(blockedWebResponse.payload.status.alignment.aligned, false);
    assert.equal(blockedWebResponse.payload.status.operationInProgress.busyScope, "codex-home");
    assert.deepEqual(
      asLastComplete(blockedWebResponse.payload.status),
      asLastComplete(baselineCore)
    );
    await releaseHolder();
    releaseHolder = null;
    assert.equal((await readCoreStatus()).currentProvider, "external");

    await fs.writeFile(path.join(fixture.codexHome, "config.toml"), fixture.configText("openai"), "utf8");
    assert.equal((await web.status()).status, 200);
    const stateBaselineCore = await readCoreStatus();
    releaseHolder = await startHolder({
      mode: "state-db",
      codexHome: fixture.codexHome,
      stateDbPath: fixture.stateDbPath
    });
    const stateBlockedCore = await readCoreStatus();
    const stateBlockedWeb = (await web.status()).payload.status;
    assert.equal(stateBlockedCore.operationInProgress, null);
    assert.equal(stateBlockedCore.statusReadBlocked, undefined);
    assert.equal(stateBlockedCore.sqliteCounts.sessions.external, 1);
    assert.equal(stateBlockedWeb.operationInProgress, null);
    assert.equal(stateBlockedWeb.sqliteCounts.sessions.external, 1);
    assert.notDeepEqual(asLastComplete(stateBlockedCore), asLastComplete(stateBaselineCore));
    await releaseHolder();
    releaseHolder = null;
    const refreshed = await readCoreStatus();
    assert.equal(refreshed.sqliteCounts.sessions.external, 1);

    const lockDir = path.join(fixture.codexHome, "tmp", "provider-sync.lock");
    await fs.mkdir(lockDir, { recursive: true });
    await fs.writeFile(path.join(lockDir, "owner.json"), "{malformed", "utf8");
    const unverifiable = await readCoreStatus();
    assert.equal(unverifiable.operationInProgress.lockState, "unverifiable");
    assert.equal(unverifiable.rolloutScanComplete, false);
    assert.ok(unverifiable.statusReadBlocked);
    await fs.rm(lockDir, { recursive: true, force: true });
  } finally {
    await releaseHolder?.().catch(() => {});
    await web.close();
    await fs.rm(fixture.root, { recursive: true, force: true });
  }
});

// Model a writer appending just after the header is read, without timers,
// production hooks or any real Codex data. Revision capture itself uses stat.
function afterHeaderRead(t, fixture, callback) {
  const open = fs.open;
  let reads = 0;
  methodMocks.method(fs, "open", async (filePath, flags, ...args) => {
    const handle = await open(filePath, flags, ...args);
    if (filePath === fixture.rolloutPath && flags === "r") {
      const close = handle.close.bind(handle);
      handle.close = async () => { await close(); await callback(++reads); };
    }
    return handle;
  });
  return () => reads;
}

const syntheticAppend = (fixture) => fs.appendFile(fixture.rolloutPath,
  '{"type":"event_msg","payload":{"type":"user_message","message":"synthetic append"}}\n');

for (const lockState of ["active", "unverifiable"]) {
  test(`Diagnostics under ${lockState} lock exports safety without opening any rollout`, async () => {
    const fixture = await makeFixture();
    let release;
    try {
      if (lockState === "active") release = await acquireLock(fixture.codexHome, "sync");
      else {
        const lockDir = path.join(fixture.codexHome, "tmp", "provider-sync.lock");
        await fs.mkdir(lockDir, { recursive: true });
        await fs.writeFile(path.join(lockDir, "owner.json"), "{");
      }
      const open = fs.open;
      const readFile = fs.readFile;
      methodMocks.method(fs, "open", async (file, ...args) => {
        assert.notEqual(file, fixture.rolloutPath, "blocked diagnostic must not open rollout");
        return open(file, ...args);
      });
      methodMocks.method(fs, "readFile", async (file, ...args) => {
        assert.notEqual(file, fixture.rolloutPath);
        return readFile(file, ...args);
      });
      const result = await getDiagnostics({ codexHome: fixture.codexHome });
      assert.equal(result.safety.operationInProgress.lockState, lockState);
      assert.equal(result.safety.rolloutScanComplete, false);
      assert.equal(result.historyIntegrity, undefined);
    } finally {
      methodMocks.restoreAll();
      await release?.();
      await fs.rm(fixture.root, { recursive: true, force: true });
    }
  });
}

for (const locked of [false, true]) {
  test(`Diagnostics preserves current facts after revision failure, with Home lock priority (${locked})`, async (t) => {
    const fixture = await makeFixture();
    let release;
    try {
      await getStatus({ codexHome: fixture.codexHome, includeSessionActivity: false });
      let armed = false;
      let failed = false;
      const stat = fs.stat;
      methodMocks.method(fs, "stat", async (file, ...args) => {
        if (armed && !failed && file === fixture.rolloutPath) {
          failed = true;
          if (locked) release = await acquireLock(fixture.codexHome, "synthetic-sync");
          throw Object.assign(new Error("synthetic stat failure"), { code: "EIO" });
        }
        return stat(file, ...args);
      });
      const result = await getDiagnostics({ codexHome: fixture.codexHome, requestControl: { onProgress(event) {
        if (event.stage === "inspect_diagnostics_index") armed = true;
      } } });
      assert.equal(failed, true);
      assert.equal(result.safety.rolloutScanComplete, false);
      if (locked) assert.equal(result.safety.operationInProgress.lockState, "active");
      else {
        assert.equal(result.safety.operationInProgress, null);
        assert.equal(result.provider.rolloutCounts.sessions.openai, 1);
      }
    } finally {
      methodMocks.restoreAll();
      await release?.();
      await fs.rm(fixture.root, { recursive: true, force: true });
    }
  });
}

test("Explicit Diagnostics scans facts once and retains current findings on drift without content hashing", async (t) => {
  const fixture = await makeFixture();
  try {
    await getStatus({ codexHome: fixture.codexHome, includeSessionActivity: false });
    const readFile = fs.readFile;
    methodMocks.method(fs, "readFile", async (file, ...args) => {
      assert.notEqual(file, fixture.rolloutPath, "diagnostic revision must not re-read/hash the rollout body");
      return readFile(file, ...args);
    });
    let scans = 0;
    const diagnostics = await getDiagnostics({ codexHome: fixture.codexHome, includeSessionActivity: false,
      requestControl: { onProgress: (event) => {
        if (event.stage === "inspect_diagnostics_index") {
          scans += 1;
          appendFileSync(fixture.rolloutPath, '{"type":"event_msg","payload":{"type":"user_message","message":"synthetic"}}\n');
        }
      } }
    });
    assert.equal(scans, 1);
    assert.equal(diagnostics.safety.operationInProgress, null);
    assert.equal(diagnostics.safety.rolloutScanComplete, false);
    assert.equal(diagnostics.provider.rolloutCounts.sessions.openai, 1);
    assert.equal(diagnostics.historyIntegrity.counts.filesScanned, 1);
  } finally {
    methodMocks.restoreAll();
    await fs.rm(fixture.root, { recursive: true, force: true });
  }
});

test("Diagnostic drift preserves findings but a real Home lock still takes priority", async (t) => {
  const fixture = await makeFixture();
  let release;
  try {
    await getStatus({ codexHome: fixture.codexHome, includeSessionActivity: false });
    const readFile = fs.readFile;
    let configReads = 0;
    methodMocks.method(fs, "readFile", async (file, ...args) => {
      const value = await readFile(file, ...args);
      if (file === path.join(fixture.codexHome, "config.toml") && ++configReads === 2) {
        await syntheticAppend(fixture);
        release = await acquireLock(fixture.codexHome, "synthetic-sync");
      }
      return value;
    });
    const result = await getDiagnostics({ codexHome: fixture.codexHome });
    assert.equal(configReads, 2);
    assert.equal(result.safety.operationInProgress.lockState, "active");
    assert.equal(result.safety.rolloutScanComplete, false);
  } finally {
    methodMocks.restoreAll();
    await release?.();
    await fs.rm(fixture.root, { recursive: true, force: true });
  }
});

for (const revision of ["config", "state-db"]) {
  test(`Status ${revision} drift also stays separate from operation state`, async (t) => {
    const fixture = await makeFixture();
    try {
      afterHeaderRead(t, fixture, async (count) => {
        if (revision === "config") {
          await fs.writeFile(path.join(fixture.codexHome, "config.toml"), fixture.configText("new-provider"));
        } else {
          const database = await openDatabase(fixture.stateDbPath);
          try { database.prepare("UPDATE threads SET model_provider = ? WHERE id = ?").run(`writer-${count}`, "status-thread"); }
          finally { database.close(); }
        }
      });
      const status = await getStatus({ codexHome: fixture.codexHome, includeSessionActivity: false });
      assert.equal(status.operationInProgress, null);
      assert.equal(status.rolloutScanComplete, false);
      assert.deepEqual(status.statusReadBlocked, { reason: "state-changed-during-status", revision });
    } finally {
      methodMocks.restoreAll();
      await fs.rm(fixture.root, { recursive: true, force: true });
    }
  });
}

for (const cached of [false, true]) {
  test(`Status revision drift needs refresh, never a fabricated writer (cached=${cached})`, async (t) => {
    const fixture = await makeFixture();
    try {
      const options = { codexHome: fixture.codexHome, includeSessionActivity: false };
      const baseline = cached ? await getStatus(options) : null;
      const reads = afterHeaderRead(t, fixture, () => syntheticAppend(fixture));
      const blocked = await getStatus(options);
      assert.equal(reads(), 2, "exactly one bounded retry, no polling");
      assert.equal(blocked.operationInProgress, null);
      assert.deepEqual(blocked.statusReadBlocked, { reason: "state-changed-during-status", revision: "rollout" });
      assert.equal(blocked.rolloutScanComplete, false);
      assert.equal(blocked.pendingRecovery, false);
      if (baseline) {
        assert.equal(blocked.snapshotAt, baseline.snapshotAt);
        assert.deepEqual(blocked.rolloutCounts, baseline.rolloutCounts);
        assert.equal(blocked.storageRevision, baseline.storageRevision);
      } else {
        assert.equal(blocked.currentProvider, undefined, "no invented Provider before the first complete snapshot");
      }
      methodMocks.restoreAll();
      const refreshed = await getStatus(options);
      assert.equal(refreshed.operationInProgress, null);
      assert.equal(refreshed.statusReadBlocked, undefined);
      assert.equal(refreshed.rolloutScanComplete, true);
      assert.equal(refreshed.backupSummary.count, 0);
      assert.deepEqual(refreshed.rolloutCounts.sessions, { openai: 1 });
    } finally {
      methodMocks.restoreAll();
      await fs.rm(fixture.root, { recursive: true, force: true });
    }
  });
}

test("Status's one retry can settle an ordinary append", async (t) => {
  const fixture = await makeFixture();
  try {
    const reads = afterHeaderRead(t, fixture, (count) => count === 1 ? syntheticAppend(fixture) : undefined);
    const status = await getStatus({ codexHome: fixture.codexHome, includeSessionActivity: false });
    assert.equal(reads(), 2);
    assert.equal(status.operationInProgress, null);
    assert.equal(status.statusReadBlocked, undefined);
    assert.equal(status.rolloutScanComplete, true);
  } finally {
    methodMocks.restoreAll();
    await fs.rm(fixture.root, { recursive: true, force: true });
  }
});

for (const phase of [0, 1, 2]) {
  test(`Status revision capture failure at phase ${phase} stays unverified, not busy`, async (t) => {
    const fixture = await makeFixture();
    try {
      const reads = afterHeaderRead(t, fixture, () => syntheticAppend(fixture));
      const stat = fs.stat;
      methodMocks.method(fs, "stat", async (filePath, ...args) => {
        if (filePath === fixture.rolloutPath && reads() >= phase) {
          throw Object.assign(new Error("synthetic revision failure"), { code: "EIO" });
        }
        return stat(filePath, ...args);
      });
      const status = await getStatus({ codexHome: fixture.codexHome, includeSessionActivity: false });
      assert.equal(reads(), phase);
      assert.equal(status.operationInProgress, null);
      assert.equal(status.statusReadBlocked.reason, "revision-unverifiable");
      assert.equal(status.rolloutScanComplete, false);
      methodMocks.restoreAll();
      assert.equal((await getStatus({ codexHome: fixture.codexHome, includeSessionActivity: false })).statusReadBlocked, undefined);
    } finally {
      methodMocks.restoreAll();
      await fs.rm(fixture.root, { recursive: true, force: true });
    }
  });
}

test("A real Home lock acquired during the retry takes priority over revision drift", async (t) => {
  const fixture = await makeFixture();
  let release;
  try {
    afterHeaderRead(t, fixture, async (count) => {
      await syntheticAppend(fixture);
      if (count === 2) release = await acquireLock(fixture.codexHome, "synthetic-sync");
    });
    const status = await getStatus({ codexHome: fixture.codexHome, includeSessionActivity: false });
    assert.equal(status.statusReadBlocked.reason, "codex-home-lock");
    assert.equal(status.operationInProgress.lockState, "active");
    assert.equal(status.operationInProgress.busyScope, "codex-home");
  } finally {
    methodMocks.restoreAll();
    await release?.();
    await fs.rm(fixture.root, { recursive: true, force: true });
  }
});

test("Core facade and HTTP preserve the refresh-needed state without a busy operation", async (t) => {
  const fixture = await makeFixture();
  const web = await startRealWeb(fixture.codexHome, path.join(fixture.root, "web"));
  try {
    afterHeaderRead(t, fixture, () => syntheticAppend(fixture));
    const response = await web.status();
    assert.equal(response.status, 200);
    assert.equal(response.payload.status.operationInProgress, null);
    assert.deepEqual(response.payload.status.statusReadBlocked, { reason: "state-changed-during-status" });
    assert.equal(response.payload.status.rolloutScanComplete, false);
    assert.equal(response.payload.status.alignment.aligned, false);
    methodMocks.restoreAll();
    assert.equal((await web.status()).payload.status.statusReadBlocked, undefined);
  } finally {
    methodMocks.restoreAll();
    await web.close();
    await fs.rm(fixture.root, { recursive: true, force: true });
  }
});
