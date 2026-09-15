import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { execFile } from "node:child_process";
import test, { afterEach } from "node:test";

const cleanups = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });
import { collectProviderChanges, collectSessionChanges, readProviderRevisionHeader, applySessionChanges } from "../src/session-files.js";
import { prepareSync, applySync, getStatus, prepareRestore, applyRestore, prepareSwitch, applySwitch } from "../src/service.js";
import { openDatabase } from "../src/sqlite.js";

const header = (id, provider = "custom", extra = {}) => JSON.stringify({ type: "session_meta", payload: { id, model_provider: provider, ...extra } });
const nested = (depth, id = "bad") => '{"type":"session_meta","payload":{"id":"' + id + '","model_provider":"custom","extra":' + '['.repeat(depth) + '0' + ']'.repeat(depth) + '}}';
const invalidUtf8 = Buffer.concat([Buffer.from('{"type":"session_meta","payload":{"id":"bad","model_provider":"custom","extra":"'), Buffer.from([0xff]), Buffer.from('"}}')]);

async function fixture(t, withPath = true) {
  const base = process.platform === "win32" ? "D:/Temp/" : os.tmpdir();
  await fs.mkdir(base, { recursive: true });
  const home = await fs.mkdtemp(path.join(base, "provider-header-"));
  cleanups.push(() => fs.rm(home, { recursive: true, force: true }));
  await fs.mkdir(path.join(home, "sessions"));
  await fs.mkdir(path.join(home, "archived_sessions"));
  await fs.mkdir(path.join(home, "sqlite"));
  await fs.writeFile(path.join(home, "config.toml"), 'model_provider="openai"\n[model_providers.custom]\nname="Custom"\n');
  const good = path.join(home, "sessions", "rollout-good.jsonl");
  const bad = path.join(home, "archived_sessions", "rollout-bad.jsonl");
  const originalGood = header("good") + '\r\n{"type":"event_msg","payload":{"message":"fixture body"}}\n';
  await fs.writeFile(good, originalGood);
  await fs.writeFile(bad, header("bad"));
  const dbPath = path.join(home, "sqlite", "state_5.sqlite");
  const db = await openDatabase(dbPath);
  try {
    db.exec(`CREATE TABLE threads(id TEXT PRIMARY KEY, model_provider TEXT, archived INTEGER DEFAULT 0, updated_at INTEGER DEFAULT 42${withPath ? ', rollout_path TEXT' : ''})`);
    for (const [id, file] of [["good", good], ["bad", bad], ["sqlite-only", null]]) {
      if (withPath) db.prepare("INSERT INTO threads(id, model_provider, rollout_path) VALUES (?, 'custom', ?)").run(id, file);
      else db.prepare("INSERT INTO threads(id, model_provider) VALUES (?, 'custom')").run(id);
    }
  } finally { db.close(); }
  return { home, good, bad, originalGood, async rows() {
    const db = await openDatabase(dbPath);
    try { return Object.fromEntries(db.prepare("SELECT id, model_provider FROM threads").all().map(row => [row.id, row.model_provider])); }
    finally { db.close(); }
  } };
}
const apply = plan => applySync({ schemaVersion: 1, planId: plan.planId });

for (const [name, content, reason] of [
  ["invalid UTF-8", invalidUtf8, "metadata-invalid-utf8"],
  ["array payload", Buffer.from('{"type":"session_meta","payload":[]}'), "metadata-invalid"],
  ["serialization capacity", Buffer.from(nested(20000)), "metadata-too-complex"]
]) test(`${name}: skip file and index, sync healthy data, freeze exclusion, restore only written files`, async t => {
  const f = await fixture(t);
  await fs.writeFile(f.bad, content);
  const plan = await prepareSync({ codexHome: f.home });
  assert.equal(plan.impact.rolloutFilesToChange, 1);
  assert.equal(plan.impact.sqliteRowsToChange, 2);
  assert.ok(plan.impact.skipSummary.items.some(item => item.kind === "rollout" && item.reason === reason));
  assert.equal(plan.impact.skipSummary.retryRecommended, false);
  const result = await apply(plan);
  assert.equal(result.outcome, "partial");
  assert.equal(result.result.changedSessionFiles, 1);
  assert.deepEqual(await fs.readFile(f.bad), content);
  assert.deepEqual(await f.rows(), { good: "openai", bad: "custom", "sqlite-only": "openai" });
  const repaired = header("bad") + '\n';
  const frozen = await prepareSync({ codexHome: f.home });
  await fs.writeFile(f.bad, repaired);
  await apply(frozen);
  assert.equal(await fs.readFile(f.bad, "utf8"), repaired);
  const restore = await prepareRestore({ codexHome: f.home, backupId: result.backup.backupId });
  await applyRestore({ schemaVersion: 1, planId: restore.planId });
  assert.equal(await fs.readFile(f.bad, "utf8"), repaired);
  assert.equal(await fs.readFile(f.good, "utf8"), f.originalGood);
  const next = await prepareSync({ codexHome: f.home });
  assert.equal(next.impact.rolloutFilesToChange, 2);
});

test("strict decoding covers both returns, chunk boundaries, BOM and valid replacement characters", async t => {
  const f = await fixture(t);
  for (const separator of ["\n", "\r\n", ""]) {
    for (const malformed of [invalidUtf8, Buffer.from([0xe4, 0xb8]), Buffer.from([0xc0, 0xaf])]) {
      await fs.writeFile(f.bad, Buffer.concat([malformed, Buffer.from(separator)]));
      await assert.rejects(readProviderRevisionHeader(f.bad), { name: "RolloutMetadataEncodingError" });
    }
    // Place a multibyte code point across a 64 KiB read boundary.
    const prefix = '{"type":"session_meta","payload":{"id":"bad","extra":"';
    const text = prefix + "x".repeat(65535 - Buffer.byteLength(prefix)) + '中文😀�"}}';
    await fs.writeFile(f.bad, text + separator);
    const record = await readProviderRevisionHeader(f.bad);
    assert.equal(record.firstLine, text);
    assert.equal(record.offset, Buffer.byteLength(text + separator));
    const scan = await collectProviderChanges(f.home, "openai");
    assert.equal(scan.changes.length, 2); // A missing Provider remains supported.
    assert.equal(scan.skippedItems.length, 0);
  }
  for (const bytes of [Buffer.concat([Buffer.from([239, 187, 191]), Buffer.from(header("bad"))]), Buffer.from(header("bad"), "utf16le")]) {
    await fs.writeFile(f.bad, bytes);
    const scan = await collectProviderChanges(f.home, "openai");
    assert.equal(scan.changes.length, 1);
    assert.equal(scan.skippedItems[0].reason, "metadata-invalid");
  }
});

test("Provider shape checks do not change shared legacy collection defaults", async t => {
  const f = await fixture(t);
  for (const payload of [[], null, "text", 1, false]) {
    await fs.writeFile(f.bad, JSON.stringify({ type: "session_meta", payload }));
    const scan = await collectProviderChanges(f.home, "openai");
    assert.equal(scan.changes.length, 1);
    assert.equal(scan.skippedItems[0].reason, "metadata-invalid");
  }
  await fs.writeFile(f.bad, '{"type":"session_meta","payload":[]}');
  const legacy = await collectSessionChanges(f.home, "openai", { includeModels: false, includeEncryptedContent: false });
  assert.equal(legacy.changes.length, 2);
  const status = await getStatus({ codexHome: f.home });
  assert.equal(status.rolloutScanComplete, false);
  assert.ok(status.skipSummary.items.some(item => item.reason === "metadata-invalid"));
});

test("semantic comparison capacity is skipped independently of serialization; supported nesting works", async t => {
  const f = await fixture(t, false);
  // JIT optimization changes which operation exhausts its stack first. Isolate
  // only this case without JIT, keeping the real error and business flow in the
  // same process. No product depth limit or mocked comparison is introduced.
  const script = `
    import assert from "node:assert/strict";
    import fs from "node:fs/promises";
    import { isDeepStrictEqual } from "node:util";
    import { prepareSync, applySync } from "./src/service.js";
    const [home, bad] = process.argv.slice(1);
    const nested = ${nested.toString()};
    let deep;
    for (let depth = 500; depth <= 6000; depth += 250) {
      const text = nested(depth);
      const parsed = JSON.parse(text);
      try { JSON.stringify(parsed); } catch { break; }
      try { isDeepStrictEqual(parsed, JSON.parse(text)); }
      catch (error) { assert.ok(error instanceof RangeError); deep = text; break; }
    }
    assert.ok(deep, "must exercise real equality overflow separately from stringify");
    await fs.writeFile(bad, deep);
    const plan = await prepareSync({ codexHome: home });
    assert.equal(plan.impact.rolloutFilesToChange, 1);
    assert.ok(plan.impact.skipSummary.items.some(item => item.reason === "metadata-too-complex"));
    const result = await applySync({ schemaVersion: 1, planId: plan.planId });
    assert.equal(result.outcome, "partial");
  `;
  await promisify(execFile)(process.execPath, ["--jitless", "--input-type=module", "-e", script, f.home, f.bad], {
    cwd: new URL("..", import.meta.url), timeout: 30000, windowsHide: true
  });
  assert.deepEqual(await f.rows(), { good: "openai", bad: "custom", "sqlite-only": "openai" });
  await fs.writeFile(f.bad, nested(100));
  assert.equal((await collectProviderChanges(f.home, "openai")).changes.length, 1);
});

test("unknown JSON processing exceptions still abort preparation", async t => {
  const f = await fixture(t);
  const stringify = JSON.stringify;
  const failure = new Error("injected unknown processing failure");
  JSON.stringify = function(value, ...args) {
    if (value?.payload?.id === "bad" && value.payload.model_provider === "openai") throw failure;
    return stringify(value, ...args);
  };
  try { await assert.rejects(collectProviderChanges(f.home, "openai"), error => error === failure); }
  finally { JSON.stringify = stringify; }
  assert.equal(await fs.readFile(f.good, "utf8"), f.originalGood);
});

test("write preflight detects invalid bytes replacing valid U+FFFD, for both write strategies", async t => {
  const f = await fixture(t);
  for (const provider of ["custom", "old"]) for (const separator of ["\n", ""]) {
    await fs.writeFile(f.bad, header("bad", provider, { extra: "�" }) + separator);
    const scan = await collectProviderChanges(f.home, "openai");
    const change = scan.changes.find(item => item.path === f.bad);
    const bytes = await fs.readFile(f.bad);
    bytes[bytes.indexOf(Buffer.from("�"))] = 0xff;
    await fs.writeFile(f.bad, bytes);
    // Keep identity/size stable and bind the fresh timestamp so the decoder or
    // in-place byte comparison, rather than a timestamp mismatch, rejects it.
    const stat = await fs.stat(f.bad);
    change.originalMtimeMs = stat.mtimeMs;
    if (change.inPlaceMutation) change.inPlaceMutation.originalMtimeMs = stat.mtimeMs;
    const result = await applySessionChanges([change]);
    assert.deepEqual(result.skippedChangedPaths, [f.bad]);
    assert.deepEqual(await fs.readFile(f.bad), bytes);
  }
});

test("invalid encoding introduced after backup preserves index and is excluded from Restore", async t => {
  const f = await fixture(t);
  await fs.writeFile(f.bad, header("bad", "old", { extra: "�" }) + '\n');
  let changed;
  const plan = await prepareSync({ codexHome: f.home, faultInjector: async ({ point, path: file }) => {
    if (point !== "before_rollout_apply" || file !== f.bad) return;
    changed = await fs.readFile(file);
    changed[changed.indexOf(Buffer.from("�"))] = 0xff;
    await fs.writeFile(file, changed);
  } });
  const result = await apply(plan);
  assert.equal(result.outcome, "partial");
  assert.equal(result.result.changedSessionFiles, 1);
  assert.equal((await f.rows()).bad, "custom");
  const restore = await prepareRestore({ codexHome: f.home, backupId: result.backup.backupId });
  await applyRestore({ schemaVersion: 1, planId: restore.planId });
  assert.deepEqual(await fs.readFile(f.bad), changed);
});

test("all invalid headers give zero-write Sync and config-only Switch", async t => {
  const f = await fixture(t, false);
  await fs.writeFile(f.bad, invalidUtf8);
  await fs.writeFile(f.good, '{"type":"session_meta","payload":[]}');
  const status = await getStatus({ codexHome: f.home });
  assert.equal(status.rolloutScanComplete, false);
  assert.ok(status.skipSummary.items.some(item => item.reason === "metadata-invalid-utf8"));
  const sync = await apply(await prepareSync({ codexHome: f.home }));
  assert.equal(sync.outcome, "partial");
  assert.equal(sync.backup, null);
  assert.equal(sync.result.changedSessionFiles, 0);
  assert.equal(sync.result.sqliteRowsUpdated, 0);
  const plan = await prepareSwitch({ codexHome: f.home, provider: "custom", keepRootModel: true });
  const switched = await applySwitch({ schemaVersion: 1, planId: plan.planId });
  assert.equal(switched.result.configUpdated, true);
  assert.equal(switched.result.changedSessionFiles, 0);
  assert.ok(switched.backup);
});
