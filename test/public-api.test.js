import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { listBackups } from "../src/backup.js";
import { getHistorySession, listHistory } from "../src/history.js";
import * as publicApi from "../src/public-api.js";
import {
  SyncTransactionError,
  getStatus,
  runPruneBackups,
  runRestore,
  runSwitch,
  runSync
} from "../src/service.js";
import { runWatch } from "../src/watch.js";

const EXPECTED_EXPORTS = [
  "SyncTransactionError",
  "getHistorySession",
  "getStatus",
  "listBackups",
  "listHistory",
  "runPruneBackups",
  "runRestore",
  "runSwitch",
  "runSync",
  "runWatch"
];

test("public API exposes only the supported Core boundary", () => {
  assert.deepEqual(Object.keys(publicApi).sort(), EXPECTED_EXPORTS);
});

test("public API preserves the existing implementation identities", () => {
  assert.strictEqual(publicApi.SyncTransactionError, SyncTransactionError);
  assert.strictEqual(publicApi.getStatus, getStatus);
  assert.strictEqual(publicApi.runSync, runSync);
  assert.strictEqual(publicApi.runSwitch, runSwitch);
  assert.strictEqual(publicApi.runRestore, runRestore);
  assert.strictEqual(publicApi.runPruneBackups, runPruneBackups);
  assert.strictEqual(publicApi.listBackups, listBackups);
  assert.strictEqual(publicApi.listHistory, listHistory);
  assert.strictEqual(publicApi.getHistorySession, getHistorySession);
  assert.strictEqual(publicApi.runWatch, runWatch);
});

test("getStatus exposes the frozen public result fields", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-public-api-"));
  const codexHome = path.join(root, ".codex");
  try {
    await fs.mkdir(codexHome, { recursive: true });
    await fs.writeFile(path.join(codexHome, "config.toml"), 'model_provider = "openai"\n', "utf8");

    const status = await publicApi.getStatus({ codexHome });

    assert.deepEqual(Object.keys(status).sort(), [
      "backupRoot",
      "backupSummary",
      "checkedStateDbPaths",
      "codexHome",
      "configuredProviders",
      "currentProvider",
      "currentProviderImplicit",
      "encryptedContentCounts",
      "encryptedContentWarning",
      "lockedRolloutFiles",
      "pendingTransactions",
      "projectThreadVisibility",
      "rolloutCounts",
      "sqliteAccess",
      "sqliteCounts",
      "sqliteHome",
      "sqliteHomeSource",
      "sqliteRepairStats",
      "stateDbLocation"
    ]);
    assert.deepEqual(Object.keys(status.backupSummary).sort(), ["count", "totalBytes"]);
    assert.deepEqual(Object.keys(status.rolloutCounts).sort(), ["archived_sessions", "sessions"]);
    assert.deepEqual(Object.keys(status.sqliteAccess).sort(), ["message", "reason", "supported"]);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("production adapters use the public API instead of implementation modules", async () => {
  const adapterPaths = ["cli.js", "web-server.js"];
  const implementationImport = /(?:from\s+|import\s*\(\s*|import\s*)["']\.\/(?:service|backup|history|watch)\.js["']/;

  for (const adapterPath of adapterPaths) {
    const source = await fs.readFile(new URL(`../src/${adapterPath}`, import.meta.url), "utf8");
    assert.doesNotMatch(
      source,
      implementationImport,
      `${adapterPath} must import Core operations through public-api.js`
    );
  }
});
