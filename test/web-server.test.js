import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { createWebUiServer } from "../src/web-server.js";

async function startFixture(services = {}) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-web-"));
  await fs.writeFile(
    path.join(root, "index.html"),
    '<!doctype html><title>fixture</title><script>window.boot=__CODEX_PROVIDER_SYNC_BOOTSTRAP__;</script>',
    "utf8"
  );
  const handle = createWebUiServer({ token: "test-token", webRoot: root, services });
  await new Promise((resolve, reject) => {
    handle.server.once("error", reject);
    handle.server.listen(0, "127.0.0.1", resolve);
  });
  const address = handle.server.address();
  const origin = `http://127.0.0.1:${address.port}`;
  handle.setOrigin(origin);
  return {
    ...handle,
    root,
    origin,
    close: async () => {
      await new Promise((resolve, reject) => handle.server.close((error) => error ? reject(error) : resolve()));
      await fs.rm(root, { recursive: true, force: true });
    }
  };
}

async function api(handle, pathname, body = {}, token = "test-token") {
  return fetch(`${handle.origin}${pathname}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Codex-Provider-Token": token,
      Origin: handle.origin
    },
    body: JSON.stringify(body)
  });
}

function statusFixture() {
  return {
    codexHome: "/tmp/.codex",
    sqliteHome: "/tmp/.codex/sqlite",
    sqliteHomeSource: "default",
    sqliteAccess: { supported: true, reason: null, message: null },
    checkedStateDbPaths: ["/tmp/.codex/sqlite/state_5.sqlite"],
    currentProvider: "openai",
    currentProviderImplicit: false,
    configuredProviders: ["openai", "relay"],
    rolloutCounts: {
      sessions: { relay: 2, openai: 3 },
      archived_sessions: { openai: 1 }
    },
    lockedRolloutFiles: [],
    encryptedContentCounts: { sessions: {}, archived_sessions: {} },
    encryptedContentWarning: null,
    sqliteCounts: {
      sessions: { openai: 3, relay: 2 },
      archived_sessions: { openai: 1 }
    },
    stateDbLocation: { path: "/tmp/.codex/sqlite/state_5.sqlite", source: "sqlite-dir" },
    sqliteRepairStats: { userEventRowsNeedingRepair: 0, cwdRowsNeedingRepair: 0 },
    projectThreadVisibility: [],
    backupRoot: "/tmp/.codex/backups_state/provider-sync",
    backupSummary: { count: 0, totalBytes: 0 }
  };
}

test("Web UI injects a per-process API token and rejects unauthorized API calls", async () => {
  const handle = await startFixture({ getStatus: async () => statusFixture() });
  try {
    const page = await fetch(handle.origin);
    const html = await page.text();
    assert.equal(page.status, 200);
    assert.match(html, /window\.boot=\{"apiToken":"test-token"\}/);
    assert.doesNotMatch(html, /__CODEX_PROVIDER_SYNC_BOOTSTRAP__/);

    const denied = await api(handle, "/api/status", {}, "wrong-token");
    assert.equal(denied.status, 403);
  } finally {
    await handle.close();
  }
});

test("Web UI status comparison ignores provider key order", async () => {
  const handle = await startFixture({ getStatus: async () => statusFixture() });
  try {
    const response = await api(handle, "/api/status", { codexHome: "/tmp/.codex" });
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.status.alignment.aligned, true);
    assert.equal(payload.status.alignment.sqliteReadable, true);
  } finally {
    await handle.close();
  }
});

test("Web UI sync validates provider IDs and delegates to the shared service", async () => {
  const calls = [];
  const handle = await startFixture({
    readConfigText: async () => 'model_provider = "openai"\nmodel = "gpt-5"\n',
    readRootModelFromConfigText: () => "gpt-5",
    runSync: async (options) => {
      calls.push(options);
      options.onProgress({ stage: "create_backup", status: "start" });
      return { targetProvider: options.provider, backupDir: "/tmp/backup" };
    }
  });
  try {
    const invalid = await api(handle, "/api/sync", {
      codexHome: "/tmp/.codex",
      provider: "bad provider",
      keepCount: 5
    });
    assert.equal(invalid.status, 400);

    const response = await api(handle, "/api/sync", {
      codexHome: "/tmp/.codex",
      provider: "openai",
      keepCount: 5
    });
    assert.equal(response.status, 200);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].provider, "openai");
    assert.equal(calls[0].model, "gpt-5");
    assert.equal(calls[0].keepCount, 5);
    assert.ok(handle.getActivity().some((entry) => entry.message === "Creating backup"));
  } finally {
    await handle.close();
  }
});

test("Web UI restore only accepts managed backups returned for the selected Codex Home", async () => {
  let restored = false;
  const handle = await startFixture({
    listBackups: async () => ({
      backupRoot: "/tmp/.codex/backups_state/provider-sync",
      backups: [{ id: "known", path: "/tmp/.codex/backups_state/provider-sync/known", metadata: {} }]
    }),
    runRestore: async () => {
      restored = true;
      return { targetProvider: "openai" };
    }
  });
  try {
    const response = await api(handle, "/api/restore", {
      codexHome: "/tmp/.codex",
      backupId: "../../outside",
      restoreConfig: false,
      restoreDatabase: true,
      restoreSessions: true
    });
    assert.equal(response.status, 400);
    assert.equal(restored, false);
  } finally {
    await handle.close();
  }
});

test("Web UI history endpoints delegate to safe history services", async () => {
  const calls = [];
  const handle = await startFixture({
    listHistory: async (codexHome, options) => { calls.push(["list", codexHome, options]); return { page: 1, pageSize: 50, total: 1, hasNextPage: false, sessions: [{ id: "thread", title: "safe", cwd: "/tmp", provider: "openai", messageCount: 1 }] }; },
    getHistorySession: async (codexHome, sessionId) => { calls.push(["detail", codexHome, sessionId]); return { session: { id: sessionId }, messages: [{ role: "user", text: "hello", timestamp: null, sequence: 1 }], truncated: false, returnedMessageCount: 1 }; }
  });
  try {
    const listResponse = await api(handle, "/api/history", { codexHome: "/tmp/.codex", query: "safe", page: 1, pageSize: 50 });
    assert.equal(listResponse.status, 200);
    const detailResponse = await api(handle, "/api/history/session", { codexHome: "/tmp/.codex", sessionId: "thread" });
    assert.equal(detailResponse.status, 200);
    assert.deepEqual(calls.map((call) => call[0]), ["list", "detail"]);
    assert.equal(calls[1][2], "thread");
  } finally {
    await handle.close();
  }
});
