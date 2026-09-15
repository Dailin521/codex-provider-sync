import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test, { afterEach } from "node:test";

const cleanups = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });
import { prepareSync, applySync, prepareSwitch, applySwitch, prepareRestore, applyRestore, getStatus } from "../src/service.js";
import { openDatabase } from "../src/sqlite.js";
import { collectStatusRolloutMetadata, collectSessionChanges } from "../src/session-files.js";

const line = id => JSON.stringify({ type: "session_meta", payload: { id, model_provider: "custom" } }) + '\n{"type":"event_msg","payload":{"message":"synthetic body"}}\n';
async function fixture(t, withPath = true) {
  const base = process.platform === "win32" ? "D:/Temp/" : os.tmpdir();
  await fs.mkdir(base, { recursive: true });
  const root = await fs.mkdtemp(path.join(base, "provider-skip-"));
  cleanups.push(() => fs.rm(root, { recursive: true, force: true }));
  const home = path.join(root, "home"), sessions = path.join(home, "sessions"), dbPath = path.join(home, "sqlite", "state_5.sqlite");
  await fs.mkdir(sessions, { recursive: true });
  await fs.mkdir(path.dirname(dbPath), { recursive: true });
  await fs.writeFile(path.join(home, "config.toml"), 'model_provider = "openai"\n[model_providers.custom]\nname = "Custom"\n');
  const good = path.join(sessions, "rollout-good.jsonl"), bad = path.join(sessions, "rollout-bad.jsonl");
  await fs.writeFile(good, line("good"));
  await fs.writeFile(bad, "invalid metadata\n");
  const db = await openDatabase(dbPath);
  try {
    db.exec(`CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT, archived INTEGER DEFAULT 0, updated_at INTEGER DEFAULT 42${withPath ? ", rollout_path TEXT" : ""})`);
    for (const [id, file] of [["good", good], ["bad", bad], ["sqlite-only", null]]) {
      if (withPath) db.prepare("INSERT INTO threads(id, model_provider, rollout_path) VALUES (?, 'custom', ?)").run(id, file);
      else db.prepare("INSERT INTO threads(id, model_provider) VALUES (?, 'custom')").run(id);
    }
  } finally { db.close(); }
  return { home, good, bad, dbPath, async rows() {
    const db = await openDatabase(dbPath);
    try { return Object.fromEntries(db.prepare("SELECT id, model_provider FROM threads").all().map(r => [r.id, r.model_provider])); } finally { db.close(); }
  } };
}
const apply = plan => applySync({ schemaVersion: 1, planId: plan.planId });

test("wrapped busy reads retain the path and retry reason in Status and Provider scan summaries", async t => {
  for (const code of ["EBUSY", "EPERM"]) {
    const f = await fixture(t);
    const archived = path.join(f.home, "archived_sessions", "rollout-healthy.jsonl");
    await fs.mkdir(path.dirname(archived), { recursive: true });
    await fs.writeFile(archived, line("archived"));
    const originalOpen = fs.open;
    fs.open = async (file, ...args) => {
      if (String(file) === f.good) throw Object.assign(new Error("synthetic busy read"), { code });
      return originalOpen(file, ...args);
    };
    try {
      const status = await collectStatusRolloutMetadata(f.home, { skipLockedReads: true });
      assert.deepEqual(status.lockedPaths, [f.good]);
      assert.ok(status.incompletePaths.includes(f.good));
      assert.equal(status.providerCounts.archived_sessions.get("custom"), 1);
      assert.equal(status.skipSummary.total, 2);
      assert.equal(status.skipSummary.retryRecommended, true);
      assert.deepEqual(status.skipSummary.items.filter(item => item.reason === "locked"), [
        { kind: "rollout", path: f.good, reason: "locked", stage: "scan", retryable: true }
      ]);
      const scan = await collectSessionChanges(f.home, "openai", {
        skipLockedReads: true, rejectInvalidMetadata: true,
        includeModels: false, includeUserEvent: false, includeEncryptedContent: false
      });
      assert.deepEqual(scan.lockedPaths, [f.good]);
      assert.equal(scan.skippedItems.filter(item => item.path === f.good && item.reason === "locked").length, 1);
      assert.deepEqual(scan.changes.map(change => change.path), [archived]);
      const plan = await prepareSync({ codexHome: f.home });
      assert.equal(plan.impact.rolloutFilesToChange, 1);
      assert.ok(plan.impact.skipSummary.items.some(item => item.path === f.good && item.reason === (code === "EPERM" ? "unreadable" : "locked")));
    } finally { fs.open = originalOpen; }
    assert.equal(await fs.readFile(f.good, "utf8"), line("good"));
  }
});

test("mixed metadata preserves a bad file and its index, updates healthy data, and restores only written files", async t => {
  const f = await fixture(t);
  const before = await fs.readFile(f.good, "utf8");
  const plan = await prepareSync({ codexHome: f.home });
  assert.equal(plan.impact.rolloutFilesToChange, 1);
  assert.equal(plan.impact.sqliteRowsToChange, 2);
  assert.ok(plan.impact.skipSummary.total >= 1);
  const result = await apply(plan);
  assert.equal(result.outcome, "partial");
  assert.equal(result.result.changedSessionFiles, 1);
  assert.equal(result.result.sqliteRowsUpdated, 2);
  assert.deepEqual(await f.rows(), { good: "openai", bad: "custom", "sqlite-only": "openai" });
  assert.equal(await fs.readFile(f.bad, "utf8"), "invalid metadata\n");
  const status = await getStatus({ codexHome: f.home });
  assert.equal(status.rolloutScanComplete, false);
  // A later repair must not be undone by restoring a backup which excluded it.
  await fs.writeFile(f.bad, line("bad"));
  const restore = await prepareRestore({ codexHome: f.home, backupId: result.backup.backupId });
  await applyRestore({ schemaVersion: 1, planId: restore.planId });
  assert.equal(await fs.readFile(f.bad, "utf8"), line("bad"));
  assert.equal(await fs.readFile(f.good, "utf8"), before);
});

test("unidentified bad metadata without path column preserves uncertain SQLite-only rows", async t => {
  const f = await fixture(t, false);
  const plan = await prepareSync({ codexHome: f.home });
  assert.equal(plan.impact.sqliteRowsToChange, 1);
  const result = await apply(plan);
  assert.equal(result.outcome, "partial");
  assert.deepEqual(await f.rows(), { good: "openai", bad: "custom", "sqlite-only": "custom" });
  assert.ok(result.result.skipSummary.unconfirmed > 0);
});

test("preview exclusions remain frozen after repair and new files are deferred", async t => {
  const f = await fixture(t);
  const plan = await prepareSync({ codexHome: f.home });
  await fs.writeFile(f.bad, line("bad"));
  const added = path.join(path.dirname(f.good), "rollout-new.jsonl");
  await fs.writeFile(added, line("new"));
  await apply(plan);
  assert.equal(await fs.readFile(f.bad, "utf8"), line("bad"));
  assert.equal(await fs.readFile(added, "utf8"), line("new"));
  assert.equal((await f.rows()).bad, "custom");
  const next = await prepareSync({ codexHome: f.home });
  assert.equal(next.impact.rolloutFilesToChange, 2);
  await apply(next);
  assert.equal((await f.rows()).bad, "openai");
});

test("deleted eligible file preserves its index while another eligible file continues", async t => {
  const f = await fixture(t);
  await fs.writeFile(f.bad, line("bad"));
  const plan = await prepareSync({ codexHome: f.home });
  await fs.unlink(f.good);
  const result = await apply(plan);
  assert.equal(result.outcome, "partial");
  assert.equal(result.result.changedSessionFiles, 1);
  assert.equal((await f.rows()).good, "custom");
  assert.equal((await f.rows()).bad, "openai");
});

test("all excluded yields no Sync backup but Switch backs up and changes config", async t => {
  const f = await fixture(t, false);
  await fs.writeFile(f.good, "invalid too\n");
  const sync = await apply(await prepareSync({ codexHome: f.home }));
  assert.equal(sync.outcome, "partial");
  assert.equal(sync.backup, null);
  assert.equal(sync.result.sqliteRowsUpdated, 0);
  const plan = await prepareSwitch({ codexHome: f.home, provider: "custom", keepRootModel: true });
  const switched = await applySwitch({ schemaVersion: 1, planId: plan.planId });
  assert.equal(switched.outcome, "partial");
  assert.ok(switched.backup);
  assert.equal(switched.result.configUpdated, true);
  assert.equal(switched.result.changedSessionFiles, 0);
  assert.match(await fs.readFile(path.join(f.home, "config.toml"), "utf8"), /model_provider = "custom"/);
});

test("a row whose rollout_path changes after preview is preserved", async t => {
  const f = await fixture(t);
  const plan = await prepareSync({ codexHome: f.home });
  const db = await openDatabase(f.dbPath);
  try { db.prepare("UPDATE threads SET rollout_path=? WHERE id='good'").run(f.bad); } finally { db.close(); }
  const result = await apply(plan);
  assert.equal(result.outcome, "partial");
  assert.equal((await f.rows()).good, "custom");
  assert.ok(result.result.skipSummary.items.some(item => item.kind === "sqlite" && item.id === "good"));
});

test("initially aligned SQLite rows that change or disappear make Apply partial without expanding the plan", async t => {
  for (const withPath of [false, true]) for (const mixed of [false, true]) for (const drift of ["provider", "missing"]) {
    const f = await fixture(t, withPath);
    await fs.writeFile(f.good, line("good").replace('"custom"', '"openai"'));
    await fs.writeFile(f.bad, line("bad").replace('"custom"', mixed ? '"custom"' : '"openai"'));
    const setupDb = await openDatabase(f.dbPath);
    try {
      setupDb.exec("UPDATE threads SET model_provider='openai'");
      if (mixed) setupDb.exec("UPDATE threads SET model_provider='custom' WHERE id='bad'");
    } finally { setupDb.close(); }
    const original = await fs.readFile(f.good);
    const plan = await prepareSync({ codexHome: f.home });
    assert.equal(plan.impact.rolloutFilesToChange, mixed ? 1 : 0);
    assert.equal(plan.impact.sqliteRowsToChange, mixed ? 1 : 0);
    const changedDb = await openDatabase(f.dbPath);
    try {
      changedDb.exec(drift === "missing" ? "DELETE FROM threads WHERE id='good'" : "UPDATE threads SET model_provider='custom' WHERE id='good'");
    } finally { changedDb.close(); }
    const result = await apply(plan);
    assert.equal(result.outcome, "partial");
    assert.equal(result.result.changedSessionFiles, mixed ? 1 : 0);
    assert.equal(result.result.sqliteRowsUpdated, mixed ? 1 : 0);
    assert.equal(Boolean(result.backup), mixed);
    assert.deepEqual(result.result.skipSummary.items.filter(item => item.id === "good"), [{
      kind: "sqlite", id: "good", reason: drift === "missing" ? "row-missing" : "row-changed", stage: "revalidate", retryable: true
    }]);
    assert.deepEqual(await fs.readFile(f.good), original);
    assert.deepEqual(await f.rows(), { ...(drift === "missing" ? {} : { good: "custom" }), bad: "openai", "sqlite-only": "openai" });
  }
});

test("SQLite transaction reports initially aligned row drift during rollout writes", async t => {
  for (const drift of ["provider", "missing"]) {
    const f = await fixture(t);
    await fs.writeFile(f.good, line("good").replace('"custom"', '"openai"'));
    await fs.writeFile(f.bad, line("bad"));
    const setupDb = await openDatabase(f.dbPath);
    try { setupDb.exec("UPDATE threads SET model_provider='openai' WHERE id <> 'bad'"); } finally { setupDb.close(); }
    const original = await fs.readFile(f.good);
    let changed = false;
    const plan = await prepareSync({ codexHome: f.home, faultInjector: async ({ point }) => {
      if (point !== "after_rollout_apply" || changed) return;
      changed = true;
      const db = await openDatabase(f.dbPath);
      try { db.exec(drift === "missing" ? "DELETE FROM threads WHERE id='good'" : "UPDATE threads SET model_provider='custom' WHERE id='good'"); }
      finally { db.close(); }
    } });
    const result = await apply(plan);
    assert.equal(changed, true);
    assert.equal(result.outcome, "partial");
    assert.equal(result.result.changedSessionFiles, 1);
    assert.equal(result.result.sqliteRowsUpdated, 1);
    assert.deepEqual(result.result.skipSummary.items.filter(item => item.id === "good"), [{
      kind: "sqlite", id: "good", reason: drift === "missing" ? "row-missing" : "row-changed", stage: "sqlite", retryable: true
    }]);
    assert.deepEqual(await fs.readFile(f.good), original);
    assert.deepEqual(await f.rows(), { ...(drift === "missing" ? {} : { good: "custom" }), bad: "openai", "sqlite-only": "openai" });
  }
});

test("a file disappearing after backup is excluded from physical Restore validation", async t => {
  const f = await fixture(t);
  await fs.writeFile(f.bad, line("bad"));
  const plan = await prepareSync({ codexHome: f.home, faultInjector: async ({ point, path: file }) => {
    if (point === "before_rollout_apply" && file === f.good) await fs.unlink(f.good);
  } });
  const result = await apply(plan);
  assert.equal(result.outcome, "partial");
  assert.equal(result.result.changedSessionFiles, 1);
  assert.equal((await f.rows()).good, "custom");
  const restore = await prepareRestore({ codexHome: f.home, backupId: result.backup.backupId });
  await applyRestore({ schemaVersion: 1, planId: restore.planId });
  await assert.rejects(fs.stat(f.good), { code: "ENOENT" });
  assert.equal(await fs.readFile(f.bad, "utf8"), line("bad"));
});

test("replacing the database after rollout mutation stops before writing the replacement", async t => {
  const f = await fixture(t);
  let swapped = false;
  const plan = await prepareSync({ codexHome: f.home, faultInjector: async ({ point }) => {
    if (point !== "after_rollout_apply" || swapped) return;
    swapped = true;
    await fs.rename(f.dbPath, f.dbPath + ".original");
    await fs.copyFile(f.dbPath + ".original", f.dbPath);
  } });
  const result = await apply(plan);
  assert.equal(swapped, true);
  assert.equal(result.outcome, "partial");
  assert.equal(result.result.changedSessionFiles, 1);
  assert.equal(result.result.sqliteRowsUpdated, 0);
  assert.deepEqual(await f.rows(), { good: "custom", bad: "custom", "sqlite-only": "custom" });
});

test("mixed invalid, oversized, unreadable and busy files leave healthy rollout and index writable", async t => {
  const f = await fixture(t, false);
  const large = path.join(path.dirname(f.good), "rollout-large.jsonl");
  const denied = path.join(path.dirname(f.good), "rollout-denied.jsonl");
  const busy = path.join(path.dirname(f.good), "rollout-busy.jsonl");
  await fs.writeFile(large, "x");
  await fs.truncate(large, 128 * 1024 * 1024 + 1);
  await fs.writeFile(denied, line("denied"));
  await fs.writeFile(busy, line("busy"));
  const originalOpen = fs.open;
  fs.open = async (file, ...args) => {
    if (String(file) === denied || String(file) === busy) throw Object.assign(new Error("synthetic read failure"), { code: String(file) === denied ? "EACCES" : "EBUSY" });
    return originalOpen(file, ...args);
  };
  try {
    const plan = await prepareSync({ codexHome: f.home });
    assert.equal(plan.impact.rolloutFilesToChange, 1);
    for (const reason of ["metadata-invalid", "metadata-too-large", "unreadable", "locked"]) {
      assert.ok(plan.impact.skipSummary.items.some(item => item.reason === reason), reason);
    }
    const result = await apply(plan);
    assert.equal(result.outcome, "partial");
    assert.equal(result.result.changedSessionFiles, 1);
    assert.equal(result.result.sqliteRowsUpdated, 1);
    assert.deepEqual(await f.rows(), { good: "openai", bad: "custom", "sqlite-only": "custom" });
  } finally { fs.open = originalOpen; }
  assert.equal(await fs.readFile(denied, "utf8"), line("denied"));
  assert.equal(await fs.readFile(busy, "utf8"), line("busy"));
  assert.equal((await fs.stat(large)).size, 128 * 1024 * 1024 + 1);
});
