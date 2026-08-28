import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test, { afterEach } from "node:test";
import { fileURLToPath } from "node:url";
import { collectSessionChanges } from "../src/session-files.js";
import { runRestore, runSwitch, runSync } from "../src/service.js";
import { openDatabase } from "../src/sqlite.js";
import { findPendingTransactions } from "../src/transaction-journal.js";

const cli = fileURLToPath(new URL("../src/cli.js", import.meta.url));
const cleanups = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });

async function fixture(t, provider = "openai") {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "provider-fast-"));
  cleanups.push(() => fs.rm(home, { recursive: true, force: true }));
  await fs.mkdir(path.join(home, "sessions"));
  await fs.mkdir(path.join(home, "sqlite"));
  const config = 'model_provider = "openai"\nmodel = "root-model"\n[model_providers.prov_a]\nmodel = "provider-model"\n';
  await fs.writeFile(path.join(home, "config.toml"), config);
  const file = path.join(home, "sessions", "rollout-test.jsonl");
  const header = JSON.stringify({ type: "session_meta", payload: { id: "test", cwd: "/workspace/test", model_provider: provider } });
  const body = [
    { type: "turn_context", payload: { model: "history-model" } },
    { type: "event_msg", payload: { type: "user_message", message: "fixture" } },
    { type: "response_item", payload: { encrypted_content: "fixture-not-a-secret" } }
  ].map(JSON.stringify).join("\n") + "\n";
  await fs.writeFile(file, header + "\n" + body);
  const dbPath = path.join(home, "sqlite", "state_5.sqlite");
  const db = await openDatabase(dbPath);
  db.exec(`CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT, model TEXT,
    cwd TEXT, archived INTEGER DEFAULT 0, has_user_event INTEGER DEFAULT 0,
    first_user_message TEXT DEFAULT '', updated_at INTEGER DEFAULT 123);
    INSERT INTO threads (id, model_provider, model, cwd) VALUES ('test', 'openai', 'history-model', '/old');`);
  db.close();
  return { home, file, body, config, dbPath };
}

async function row(f) {
  const db = await openDatabase(f.dbPath);
  try { return { ...db.prepare("SELECT * FROM threads WHERE id = 'test'").get() }; }
  finally { db.close(); }
}

test("fast switch/restore never open a rollout body stream and preserve models", async (t) => {
  const f = await fixture(t);
  const before = await fs.stat(f.file);
  const original = await fs.readFile(f.file);
  const originalRow = await row(f);
  const createStream = fsSync.createReadStream;
  const readFile = fs.readFile;
  let guarded = true;
  fs.readFile = function (file, ...args) {
    if (guarded) assert.notEqual(String(file), f.file, "fast operation must not read the entire rollout");
    return readFile.call(this, file, ...args);
  };
  fsSync.createReadStream = function (file, ...args) {
    assert.notEqual(String(file), f.file, "fast operation must not scan the rollout body");
    return createStream.call(this, file, ...args);
  };
  cleanups.push(() => { fsSync.createReadStream = createStream; fs.readFile = readFile; });
  const result = await runSwitch({ codexHome: f.home, provider: "prov_a", fast: true });
  guarded = false;
  assert.equal(result.inPlaceSessionFiles, 1);
  assert.equal(result.scanScope, "metadata");
  assert.equal(result.encryptedContentCounts, null);
  assert.deepEqual(result.unchecked, ["historyModels", "userEventFlags", "encryptedContent"]);
  assert.match(result.encryptedContentWarning, /not checked/);
  assert.equal((await fs.stat(f.file)).ino, before.ino);
  assert.equal(await fs.readFile(f.file, "utf8"), original.toString().replace("openai", "prov_a"));
  assert.deepEqual(await row(f), { ...originalRow, model_provider: "prov_a", cwd: "/workspace/test" });
  assert.equal(await fs.readFile(path.join(f.home, "config.toml"), "utf8"), f.config.replace('"openai"', '"prov_a"'));
  const metadata = JSON.parse(await fs.readFile(path.join(result.backupDir, "metadata.json")));
  assert.equal(metadata.version, 3);
  assert.equal(metadata.scanScope, "metadata");
  guarded = true;
  await runRestore({ codexHome: f.home, backupDir: result.backupDir });
  guarded = false;
  assert.deepEqual(await fs.readFile(f.file), original);
  assert.deepEqual(await row(f), originalRow);
  assert.equal((await fs.stat(f.file)).ino, before.ino);
});

test("full scan reads each body once and retains all three diagnostics", async (t) => {
  const f = await fixture(t);
  const createStream = fsSync.createReadStream;
  let streams = 0;
  fsSync.createReadStream = function (file, ...args) {
    if (String(file) === f.file) streams++;
    return createStream.call(this, file, ...args);
  };
  cleanups.push(() => { fsSync.createReadStream = createStream; });
  const scan = await collectSessionChanges(f.home, "prov_a", { targetModel: "new-model" });
  assert.equal(streams, 1);
  assert.equal(scan.encryptedContentCounts.sessions.openai, 1);
  assert.ok(scan.userEventThreadIds.has("test"));
  assert.equal(scan.changes[0].modelRewriteRequired, true);
  assert.equal(scan.changes[0].originalTurnContextModels[0].originalModel, "history-model");
  assert.equal(scan.changes[0].inPlaceMutation, null);
});

test("full mode still repairs models and user-event flags", async (t) => {
  const f = await fixture(t);
  const result = await runSwitch({ codexHome: f.home, provider: "prov_a" });
  assert.equal(result.inPlaceSessionFiles, 0);
  assert.equal((await row(f)).model, "provider-model");
  assert.equal((await row(f)).has_user_event, 1);
  assert.equal((await row(f)).updated_at, 123);
  assert.match(await fs.readFile(f.file, "utf8"), /"model":"provider-model"/);
});

test("fast preflight rejects ineligible files and model intents before any mutation", async (t) => {
  for (const kind of ["length", "duplicate", "oversized", "model"]) {
    const f = await fixture(t, kind === "length" ? "provider_old" : "openai");
    if (kind === "duplicate") {
      const text = (await fs.readFile(f.file, "utf8")).replace('"model_provider":"openai"', '"model_provider":"openai","model_provider":"openai"');
      await fs.writeFile(f.file, text);
    }
    if (kind === "oversized") await fs.writeFile(f.file, "x".repeat(2 * 1024 * 1024));
    const original = await fs.readFile(f.file);
    const before = await row(f);
    await assert.rejects(runSwitch({ codexHome: f.home, provider: "prov_a", fast: true,
      ...(kind === "model" ? { model: "other" } : {}) }));
    assert.deepEqual(await fs.readFile(f.file), original);
    assert.deepEqual(await row(f), before);
    assert.equal(await fs.readFile(path.join(f.home, "config.toml"), "utf8"), f.config);
    assert.deepEqual(await findPendingTransactions(f.home), []);
    await assert.rejects(fs.access(path.join(f.home, "backups_state")), { code: "ENOENT" });
  }
});

test("fast transaction restores config, database and bytes after mutation-before-applied", async (t) => {
  const f = await fixture(t);
  const original = await fs.readFile(f.file);
  const before = await row(f);
  await assert.rejects(runSwitch({ codexHome: f.home, provider: "prov_a", fast: true,
    faultInjector({ point }) { if (point === "after_rollout_mutation_before_applied") throw new Error("fault"); }
  }), { code: "SYNC_FAILED_ROLLED_BACK" });
  assert.deepEqual(await fs.readFile(f.file), original);
  assert.deepEqual(await row(f), before);
  assert.equal(await fs.readFile(path.join(f.home, "config.toml"), "utf8"), f.config);
});

test("CLI --fast is a flag in either position and explicitly preserves models", async (t) => {
  for (const args of [["switch", "--fast", "prov_a"], ["switch", "prov_a", "--fast"], ["sync", "--provider", "prov_a", "--fast"]]) {
    const f = await fixture(t);
    const child = spawnSync(process.execPath, [cli, ...args, "--codex-home", f.home], {
      encoding: "utf8", env: { ...process.env, CODEX_SQLITE_HOME: "" }
    });
    assert.equal(child.status, 0, child.stderr);
    assert.match(child.stdout, /In-place rollout updates: 1/);
    assert.match(child.stdout, /not checked/);
    assert.equal((await row(f)).model, "history-model");
  }
  for (const args of [["status", "--fast"], ["sync", "--fast=false"], ["sync", "--fast", "--model", "other"]]) {
    const f = await fixture(t);
    const child = spawnSync(process.execPath, [cli, ...args, "--codex-home", f.home], { encoding: "utf8" });
    assert.equal(child.status, 1);
    assert.match(child.stderr, /--fast/);
  }
});

test("fast sync rejects a model at the service boundary", async (t) => {
  const f = await fixture(t);
  await assert.rejects(runSync({ codexHome: f.home, fast: true, model: "other" }), /preserves historical models/);
});

test("fast mode without a byte mutation keeps the compatible v2 backup format", async (t) => {
  const f = await fixture(t);
  const result = await runSync({ codexHome: f.home, fast: true });
  assert.equal(result.inPlaceSessionFiles, 0);
  assert.equal(result.changedSessionFiles, 0);
  for (const file of ["metadata.json", "session-meta-backup.json"]) {
    assert.equal(JSON.parse(await fs.readFile(path.join(result.backupDir, file))).version, 2);
  }
  await runRestore({ codexHome: f.home, backupDir: result.backupDir });
});

test("manual restore preflights unknown provider bytes before changing config or SQLite", async (t) => {
  const f = await fixture(t);
  const result = await runSwitch({ codexHome: f.home, provider: "prov_a", fast: true });
  const manifest = JSON.parse(await fs.readFile(path.join(result.backupDir, "session-meta-backup.json")));
  const h = await fs.open(f.file, "r+");
  try { await h.write(Buffer.from("!"), 0, 1, manifest.files[0].mutation.byteOffset + 1); }
  finally { await h.close(); }
  const config = await fs.readFile(path.join(f.home, "config.toml"));
  const before = await row(f);
  await assert.rejects(runRestore({ codexHome: f.home, backupDir: result.backupDir }), /Unknown rollout bytes/);
  assert.deepEqual(await fs.readFile(path.join(f.home, "config.toml")), config);
  assert.deepEqual(await row(f), before);
});
