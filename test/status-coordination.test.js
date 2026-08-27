import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { openDatabase } from "../src/sqlite.js";
import { createWebCoreFacade } from "../src/web-core-adapter.js";
import { createWebUiServer } from "../src/web-server.js";
import { createMemoryWebUiState } from "../src/web-state.js";

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
  return { root, codexHome, sqliteHome, stateDbPath, configText };
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
  const value = structuredClone(status);
  value.operationInProgress = null;
  delete value.statusReadBlocked;
  delete value.alignment;
  return value;
}

test("Core and Web Status preserve the last complete snapshot under external Home and shared State DB locks", async () => {
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
    assert.equal(stateBlockedCore.operationInProgress.busyScope, "state-db");
    assert.equal(stateBlockedCore.statusReadBlocked.reason, "state-db-lock");
    assert.deepEqual(asLastComplete(stateBlockedCore), asLastComplete(stateBaselineCore));
    assert.equal(stateBlockedWeb.alignment.aligned, false);
    assert.equal(stateBlockedWeb.operationInProgress.busyScope, "state-db");
    assert.deepEqual(asLastComplete(stateBlockedWeb), asLastComplete(stateBaselineCore));
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
