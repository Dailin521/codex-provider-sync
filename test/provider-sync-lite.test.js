import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test, { afterEach } from "node:test";
import { fileURLToPath } from "node:url";

import { getDiagnostics } from "../src/diagnostics.js";
import {
  applySync,
  getStatus,
  prepareSync,
  runRepair,
  runRestore,
  runSwitch,
  runSync
} from "../src/service.js";
import { openDatabase } from "../src/sqlite.js";
import { TransactionJournal, findPendingTransactions } from "../src/transaction-journal.js";

delete process.env.CODEX_SQLITE_HOME;

const cli = fileURLToPath(new URL("../src/cli.js", import.meta.url));
const cleanups = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

async function fixture({ rolloutProvider = "openai", configProvider = "openai", includeRootModel = true } = {}) {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "provider-lite-"));
  cleanups.push(() => fs.rm(home, { recursive: true, force: true }));
  await fs.mkdir(path.join(home, "sessions"));
  await fs.mkdir(path.join(home, "sqlite"));
  const config = [
    `model_provider = "${configProvider}"`,
    ...(includeRootModel ? ['model = "root-model"'] : []),
    "[model_providers.prov_a]",
    'model = "provider-model"',
    "[model_providers.provider_long]",
    'model = "long-provider-model"',
    ""
  ].join("\n");
  await fs.writeFile(path.join(home, "config.toml"), config);
  const file = path.join(home, "sessions", "rollout-test.jsonl");
  const cwd = process.platform === "win32" ? "D:\\workspace\\test" : "/workspace/test";
  const header = JSON.stringify({
    type: "session_meta",
    payload: { id: "test", cwd, model_provider: rolloutProvider }
  });
  const body = [
    { type: "turn_context", payload: { model: "history-model" } },
    { type: "event_msg", payload: { type: "user_message", message: "fixture" } },
    { type: "response_item", payload: { encrypted_content: "fixture-not-a-secret" } }
  ].map(JSON.stringify).join("\n") + "\n";
  await fs.writeFile(file, `${header}\n${body}`);
  const dbPath = path.join(home, "sqlite", "state_5.sqlite");
  const db = await openDatabase(dbPath);
  db.exec(`CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT, model TEXT,
    cwd TEXT, archived INTEGER DEFAULT 0, has_user_event INTEGER DEFAULT 0,
    first_user_message TEXT DEFAULT '', updated_at INTEGER DEFAULT 123);
    INSERT INTO threads (id, model_provider, model, cwd)
    VALUES ('test', '${rolloutProvider}', 'history-model', '/old');`);
  db.close();
  return { home, file, header, body, config, dbPath, cwd };
}

async function row(value) {
  const db = await openDatabase(value.dbPath);
  try {
    return { ...db.prepare("SELECT * FROM threads WHERE id = 'test'").get() };
  } finally {
    db.close();
  }
}

async function bodyBytes(file) {
  const bytes = await fs.readFile(file);
  const newline = bytes.indexOf(0x0a);
  return bytes.subarray(newline + 1);
}

function hashBytes(value) {
  return createHash("sha256").update(value).digest("hex");
}

test("Provider Switch reads only session metadata and preserves every non-Provider field", async () => {
  const value = await fixture();
  const beforeRow = await row(value);
  const beforeBody = await bodyBytes(value.file);
  const beforeStat = await fs.stat(value.file, { bigint: true });
  const originalReadFile = fs.readFile;
  const originalCreateReadStream = fsSync.createReadStream;
  let guarded = true;
  fs.readFile = async function (file, ...args) {
    if (guarded) assert.notEqual(path.resolve(String(file)), path.resolve(value.file));
    return originalReadFile.call(this, file, ...args);
  };
  fsSync.createReadStream = function (file, ...args) {
    if (guarded) assert.notEqual(path.resolve(String(file)), path.resolve(value.file));
    return originalCreateReadStream.call(this, file, ...args);
  };
  cleanups.push(() => {
    fs.readFile = originalReadFile;
    fsSync.createReadStream = originalCreateReadStream;
  });

  const result = await runSwitch({ codexHome: value.home, provider: "prov_a", keepRootModel: true });
  guarded = false;
  const afterRow = await row(value);
  const afterStat = await fs.stat(value.file, { bigint: true });
  assert.equal(result.inPlaceSessionFiles, 1);
  assert.equal(afterStat.ino, beforeStat.ino);
  assert.equal(afterStat.size, beforeStat.size);
  assert.deepEqual(await bodyBytes(value.file), beforeBody);
  assert.deepEqual(afterRow, { ...beforeRow, model_provider: "prov_a" });
  const config = await fs.readFile(path.join(value.home, "config.toml"), "utf8");
  assert.match(config, /model_provider = "prov_a"/);
  assert.match(config, /model = "root-model"/);
});

test("32 MiB equal-length Provider update keeps file identity, size, and body hash", async () => {
  const value = await fixture();
  const handle = await fs.open(value.file, "a");
  const block = Buffer.alloc(64 * 1024, 0x78);
  try {
    for (let index = 0; index < 512; index += 1) await handle.write(block);
    await handle.sync();
  } finally {
    await handle.close();
  }
  const beforeBodyHash = hashBytes(await bodyBytes(value.file));
  const before = await fs.stat(value.file, { bigint: true });
  const result = await runSwitch({ codexHome: value.home, provider: "prov_a", keepRootModel: true });
  const after = await fs.stat(value.file, { bigint: true });
  assert.equal(result.inPlaceSessionFiles, 1);
  assert.equal(result.rewrittenSessionFiles, 0);
  assert.equal(after.ino, before.ino);
  assert.equal(after.size, before.size);
  assert.equal(hashBytes(await bodyBytes(value.file)), beforeBodyHash);
});

test("unequal-length Provider update streams a replacement while preserving body bytes", async () => {
  const value = await fixture();
  const beforeBody = await bodyBytes(value.file);
  const result = await runSwitch({
    codexHome: value.home,
    provider: "provider_long",
    keepRootModel: true
  });
  assert.equal(result.inPlaceSessionFiles, 0);
  assert.equal(result.rewrittenSessionFiles, 1);
  assert.deepEqual(await bodyBytes(value.file), beforeBody);
  assert.match((await fs.readFile(value.file, "utf8")).split(/\r?\n/, 1)[0], /"model_provider":"provider_long"/);
});

test("Diagnostics performs the explicit full read and remains read-only", async () => {
  const value = await fixture();
  const beforeFile = await fs.readFile(value.file);
  const beforeRow = await row(value);
  const originalCreateReadStream = fsSync.createReadStream;
  let rolloutStreams = 0;
  fsSync.createReadStream = function (file, ...args) {
    if (path.resolve(String(file)) === path.resolve(value.file)) rolloutStreams += 1;
    return originalCreateReadStream.call(this, file, ...args);
  };
  cleanups.push(() => { fsSync.createReadStream = originalCreateReadStream; });

  const snapshot = await getDiagnostics({ codexHome: value.home });
  assert.equal(rolloutStreams, 1);
  assert.equal(snapshot.issues.rootModelAvailable, true);
  assert.equal(snapshot.issues.rolloutModelFilesNeedingRepair, 1);
  assert.equal(snapshot.issues.sqliteModelRowsNeedingRepair, 1);
  assert.equal(snapshot.issues.cwdRowsNeedingRepair, 1);
  assert.equal(snapshot.issues.userEventRowsNeedingRepair, 1);
  assert.equal(snapshot.issues.encryptedContentFiles, 1);
  assert.deepEqual(await fs.readFile(value.file), beforeFile);
  assert.deepEqual(await row(value), beforeRow);
});

test("Repair updates only selected model, cwd, and user-event fields", async () => {
  const value = await fixture();
  const originalConfig = await fs.readFile(path.join(value.home, "config.toml"), "utf8");
  const result = await runRepair({
    codexHome: value.home,
    targets: ["models", "cwd", "userEvent"]
  });
  const afterRow = await row(value);
  assert.deepEqual(result.repairTargets, ["models", "cwd", "userEvent"]);
  assert.equal(afterRow.model_provider, "openai");
  assert.equal(afterRow.model, "root-model");
  assert.equal(afterRow.cwd, value.cwd);
  assert.equal(afterRow.has_user_event, 1);
  assert.equal(afterRow.updated_at, 123);
  assert.match(await fs.readFile(value.file, "utf8"), /"model":"root-model"/);
  assert.equal(await fs.readFile(path.join(value.home, "config.toml"), "utf8"), originalConfig);
});

test("workspaceRoots Repair automatically includes cwd and updates both global-state copies", async () => {
  const value = await fixture();
  const storedRoot = process.platform === "win32" ? `\\\\?\\${value.cwd}` : value.cwd;
  await fs.writeFile(path.join(value.home, ".codex-global-state.json"), `${JSON.stringify({
    "electron-saved-workspace-roots": [storedRoot],
    "project-order": [storedRoot],
    "active-workspace-roots": [storedRoot]
  })}\n`);
  const result = await runRepair({ codexHome: value.home, targets: ["workspaceRoots"] });
  assert.deepEqual(result.repairTargets, ["cwd", "workspaceRoots"]);
  assert.equal((await row(value)).cwd, value.cwd);
  for (const name of [".codex-global-state.json", ".codex-global-state.json.bak"]) {
    const state = JSON.parse(await fs.readFile(path.join(value.home, name), "utf8"));
    assert.ok(state["electron-saved-workspace-roots"].includes(value.cwd));
  }
});

test("model Repair fails during Prepare when config has no root model", async () => {
  const value = await fixture({ includeRootModel: false });
  const beforeFile = await fs.readFile(value.file);
  const beforeRow = await row(value);
  await assert.rejects(
    runRepair({ codexHome: value.home, targets: ["models"] }),
    (error) => error?.code === "INVALID_INPUT"
  );
  assert.deepEqual(await fs.readFile(value.file), beforeFile);
  assert.deepEqual(await row(value), beforeRow);
  await assert.rejects(fs.access(path.join(value.home, "backups_state")), { code: "ENOENT" });
});

test("Sync always uses config Provider and rejects removed mode/provider inputs", async () => {
  const value = await fixture({ configProvider: "prov_a", rolloutProvider: "openai" });
  await assert.rejects(runSync({ codexHome: value.home, provider: "openai" }), (error) => error?.code === "INVALID_INPUT");
  await assert.rejects(runSync({ codexHome: value.home, fast: true }), (error) => error?.code === "INVALID_INPUT");
  const result = await runSync({ codexHome: value.home });
  assert.equal(result.targetProvider, "prov_a");
  assert.equal((await row(value)).model_provider, "prov_a");
});

test("CLI removes --fast and sync --provider while exposing diagnostics and repair", async () => {
  for (const args of [["sync", "--fast"], ["sync", "--provider", "prov_a"]]) {
    const value = await fixture();
    const child = spawnSync(process.execPath, [cli, ...args, "--codex-home", value.home], {
      encoding: "utf8",
      env: { ...process.env, CODEX_SQLITE_HOME: "" }
    });
    assert.equal(child.status, 1);
    assert.match(child.stderr, /removed/);
  }

  const diagnostic = await fixture();
  const inspected = spawnSync(process.execPath, [cli, "diagnostics", "--json", "--codex-home", diagnostic.home], {
    encoding: "utf8",
    env: { ...process.env, CODEX_SQLITE_HOME: "" }
  });
  assert.equal(inspected.status, 0, inspected.stderr);
  assert.equal(JSON.parse(inspected.stdout).command, "diagnostics");

  const repaired = spawnSync(process.execPath, [cli, "repair", "models,cwd,userEvent", "--json", "--codex-home", diagnostic.home], {
    encoding: "utf8",
    env: { ...process.env, CODEX_SQLITE_HOME: "" }
  });
  assert.equal(repaired.status, 0, repaired.stderr);
  const document = JSON.parse(repaired.stdout);
  assert.equal(document.command, "repair");
  assert.deepEqual(document.result.repairTargets, ["models", "cwd", "userEvent"]);
});

test("ordinary Sync noop completes without creating an UndoBackup", async () => {
  const value = await fixture();
  const plan = await prepareSync({ codexHome: value.home });
  assert.equal(plan.impact.backupExpected, false);
  assert.equal(plan.impact.lockedRolloutFiles, 0);
  assert.equal(typeof plan.impact.lockedRolloutFiles, "number");
  const applied = await applySync({ schemaVersion: 1, planId: plan.planId });
  assert.equal(applied.outcome, "completed");
  assert.equal(applied.backup, null);
  assert.equal(applied.result.noop, true);
  await assert.rejects(fs.access(path.join(value.home, "backups_state")), { code: "ENOENT" });
});

test("ordinary Sync creates an UndoBackup but no transaction journal or State DB resource lock", async () => {
  const value = await fixture({ rolloutProvider: "prov_a", configProvider: "openai" });
  const result = await runSync({ codexHome: value.home });
  assert.ok(result.backupDir);
  await assert.rejects(
    fs.access(path.join(result.backupDir, "transaction-journal.jsonl")),
    { code: "ENOENT" }
  );
  await assert.rejects(
    fs.access(path.join(path.dirname(value.dbPath), ".codex-provider-sync", "locks")),
    { code: "ENOENT" }
  );
});

test("Provider Sync UndoBackup captures only the SQLite and rollout targets it can mutate", async () => {
  const value = await fixture({ rolloutProvider: "prov_a", configProvider: "openai" });
  await fs.writeFile(path.join(value.home, ".codex-global-state.json"), "before\n");
  const synced = await runSync({ codexHome: value.home });
  const metadata = JSON.parse(await fs.readFile(path.join(synced.backupDir, "metadata.json"), "utf8"));
  assert.deepEqual(metadata.undoTargets, {
    config: { captured: false },
    globalState: { captured: false },
    sqlite: { captured: true, present: true },
    rollout: { captured: true, entryCount: 1 }
  });
  await assert.rejects(fs.access(path.join(synced.backupDir, "config.toml")), { code: "ENOENT" });
  await assert.rejects(fs.access(path.join(synced.backupDir, ".codex-global-state.json")), { code: "ENOENT" });

  await fs.writeFile(path.join(value.home, "config.toml"), 'model_provider = "provider_long"\n');
  await fs.writeFile(path.join(value.home, ".codex-global-state.json"), "after\n");

  const restored = await runRestore({ codexHome: value.home, backupDir: synced.backupDir });
  assert.deepEqual(restored.skippedNotCapturedTargetKinds, ["config", "globalState"]);
  assert.equal(await fs.readFile(path.join(value.home, "config.toml"), "utf8"), 'model_provider = "provider_long"\n');
  assert.equal(await fs.readFile(path.join(value.home, ".codex-global-state.json"), "utf8"), "after\n");
  assert.equal((await row(value)).model_provider, "prov_a");
  assert.match(await fs.readFile(value.file, "utf8"), /"model_provider":"prov_a"/);
});

test("UndoBackup targetKinds follow the actual SQLite, config, and global-state mutation sets", async () => {
  const sqliteOnly = await fixture({ rolloutProvider: "openai", configProvider: "openai" });
  const sqlite = await openDatabase(sqliteOnly.dbPath);
  sqlite.prepare("UPDATE threads SET model_provider = 'prov_a' WHERE id = 'test'").run();
  sqlite.close();
  const sqliteResult = await runSync({ codexHome: sqliteOnly.home });
  const sqliteMetadata = JSON.parse(await fs.readFile(path.join(sqliteResult.backupDir, "metadata.json"), "utf8"));
  assert.deepEqual(sqliteMetadata.undoTargets, {
    config: { captured: false },
    globalState: { captured: false },
    sqlite: { captured: true, present: true },
    rollout: { captured: false }
  });

  const switchOnly = await fixture({ rolloutProvider: "prov_a", configProvider: "openai" });
  const switched = await runSwitch({ codexHome: switchOnly.home, provider: "prov_a", keepRootModel: true });
  const switchMetadata = JSON.parse(await fs.readFile(path.join(switched.backupDir, "metadata.json"), "utf8"));
  assert.deepEqual(switchMetadata.undoTargets, {
    config: { captured: true, present: true },
    globalState: { captured: false },
    sqlite: { captured: false },
    rollout: { captured: false }
  });

  const workspaceOnly = await fixture();
  const workspaceDb = await openDatabase(workspaceOnly.dbPath);
  workspaceDb.prepare("UPDATE threads SET cwd = ? WHERE id = 'test'").run(workspaceOnly.cwd);
  workspaceDb.close();
  await fs.writeFile(path.join(workspaceOnly.home, ".codex-global-state.json"), `${JSON.stringify({
    "electron-saved-workspace-roots": ["/old"],
    "project-order": ["/old"],
    "active-workspace-roots": ["/old"]
  })}\n`);
  const repaired = await runRepair({ codexHome: workspaceOnly.home, targets: ["workspaceRoots"] });
  const repairMetadata = JSON.parse(await fs.readFile(path.join(repaired.backupDir, "metadata.json"), "utf8"));
  assert.deepEqual(repairMetadata.undoTargets, {
    config: { captured: false },
    globalState: { captured: true },
    sqlite: { captured: false },
    rollout: { captured: false }
  });
});

test("reduced UndoBackup metadata fails closed when a target declaration is incomplete", async () => {
  for (const mutate of [
    (metadata) => { delete metadata.undoTargets.globalState; },
    (metadata) => { metadata.undoTargets.config.captured = "false"; }
  ]) {
    const value = await fixture({ rolloutProvider: "prov_a", configProvider: "openai" });
    const synced = await runSync({ codexHome: value.home });
    const metadataPath = path.join(synced.backupDir, "metadata.json");
    const metadata = JSON.parse(await fs.readFile(metadataPath, "utf8"));
    mutate(metadata);
    await fs.writeFile(metadataPath, `${JSON.stringify(metadata, null, 2)}\n`);

    await assert.rejects(
      () => runRestore({ codexHome: value.home, backupDir: synced.backupDir }),
      /invalid undo target/
    );
  }
});

test("legacy ordinary journal is diagnostic-only and does not block a new Sync", async () => {
  const value = await fixture({ rolloutProvider: "prov_a", configProvider: "openai" });
  const legacyBackup = path.join(value.home, "backups_state", "provider-sync", "legacy-pending");
  await fs.mkdir(legacyBackup, { recursive: true });
  const journal = await TransactionJournal.create(legacyBackup, {
    codexHome: value.home,
    targetProvider: "prov_a",
    potentialTargets: [value.file]
  });
  const journalBefore = await fs.readFile(journal.filePath);
  const before = await getStatus({ codexHome: value.home });
  assert.equal(before.pendingRecovery, false);
  assert.equal(before.pendingTransactions.length, 1);

  const result = await runSync({ codexHome: value.home });
  assert.equal(result.partial, false);
  assert.deepEqual(await fs.readFile(journal.filePath), journalBefore);
  assert.equal((await findPendingTransactions(value.home)).length, 1);
});

test("post-mutation failure returns partial with backup evidence and retry converges", async () => {
  const value = await fixture({ rolloutProvider: "prov_a", configProvider: "openai" });
  const plan = await prepareSync({
    codexHome: value.home,
    faultInjector({ point }) {
      if (point === "after_rollout_mutation_before_applied") throw new Error("injected write failure");
    }
  });
  const partial = await applySync({ schemaVersion: 1, planId: plan.planId });
  assert.equal(partial.outcome, "partial");
  assert.ok(partial.backup?.backupId);
  assert.equal(partial.result.partialReason, "mutation-failed");
  assert.equal(partial.result.failedStage, "rewrite_rollout_files");
  assert.equal(partial.result.retryRecommended, true);
  assert.equal((await row(value)).model_provider, "prov_a");

  const retryPlan = await prepareSync({ codexHome: value.home });
  const retry = await applySync({ schemaVersion: 1, planId: retryPlan.planId });
  assert.equal(retry.outcome, "completed");
  assert.equal((await row(value)).model_provider, "openai");
});

test("rollout changed during Apply is reported separately and a fresh retry converges", async () => {
  const value = await fixture({ rolloutProvider: "prov_a", configProvider: "openai" });
  const plan = await prepareSync({
    codexHome: value.home,
    async faultInjector({ point, path: targetPath }) {
      if (point === "before_rollout_apply" && targetPath === value.file) {
        await fs.appendFile(value.file, '{"type":"event_msg","payload":{"type":"assistant_message","message":"later"}}\n');
      }
    }
  });
  const partial = await applySync({ schemaVersion: 1, planId: plan.planId });
  assert.equal(partial.outcome, "partial");
  assert.equal(partial.result.partialReason, "rollout-changed");
  assert.equal(partial.result.retryRecommended, true);
  assert.deepEqual(partial.result.skippedLockedRolloutFiles, []);
  assert.deepEqual(partial.result.skippedChangedRolloutFiles, [value.file]);
  assert.equal((await row(value)).model_provider, "openai");

  const retryPlan = await prepareSync({ codexHome: value.home });
  const retry = await applySync({ schemaVersion: 1, planId: retryPlan.planId });
  assert.equal(retry.outcome, "completed");
  assert.match(await fs.readFile(value.file, "utf8"), /"model_provider":"openai"/);
});
