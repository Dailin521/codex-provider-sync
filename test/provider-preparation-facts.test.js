import assert from "node:assert/strict";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test, { afterEach } from "node:test";
import { captureRolloutRevision, collectProviderPreparationFacts } from "../src/operation-revision.js";
import { collectProviderChanges, collectStatusRolloutMetadata } from "../src/session-files.js";
import { prepareSync, prepareSwitch, applySync } from "../src/service.js";

const cleanups = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });

async function fixture(t) {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "provider-prepare-facts-"));
  cleanups.push(() => fs.rm(home, { recursive: true, force: true }));
  await fs.mkdir(path.join(home, "sessions"));
  await fs.mkdir(path.join(home, "archived_sessions"));
  await fs.writeFile(path.join(home, "config.toml"), 'model_provider="openai"\n[model_providers.prov_a]\nmodel="fixture-model"\n');
  const file = path.join(home, "sessions", "rollout-fixture.jsonl");
  const line = JSON.stringify({ type: "session_meta", payload: { id: "fixture", model_provider: "custom" } });
  const body = Buffer.alloc(32 * 1024 * 1024, 120);
  await fs.writeFile(file, Buffer.concat([Buffer.from(line + "\r\n"), body]));
  const mtime = new Date("2026-01-02T03:04:05.789Z");
  await fs.utimes(file, mtime, mtime);
  return { home, file, line, body };
}

function headerReadProbe(file) {
  const originalOpen = fs.open;
  const originalReadFile = fs.readFile;
  const originalStream = fsSync.createReadStream;
  let opens = 0;
  let bytes = 0;
  fs.open = async (target, ...args) => {
    const handle = await originalOpen(target, ...args);
    if (path.resolve(String(target)) !== file || args[0] !== "r") return handle;
    opens += 1;
    const read = handle.read.bind(handle);
    handle.read = async (...readArgs) => {
      const result = await read(...readArgs);
      bytes += result.bytesRead;
      return result;
    };
    return handle;
  };
  fs.readFile = async (target, ...args) => {
    assert.notEqual(path.resolve(String(target)), file, "Prepare must not read a whole rollout.");
    return originalReadFile(target, ...args);
  };
  fsSync.createReadStream = (target, ...args) => {
    assert.notEqual(path.resolve(String(target)), file, "Prepare must not open a body stream.");
    return originalStream(target, ...args);
  };
  return { counts: () => ({ opens, bytes }), restore() { fs.open = originalOpen; fs.readFile = originalReadFile; fsSync.createReadStream = originalStream; } };
}

test("Provider facts reuse the exact revision and change algorithm with one bounded header read instead of three", async t => {
  const value = await fixture(t);
  const probe = headerReadProbe(value.file);
  try {
    const status = await collectStatusRolloutMetadata(value.home, { skipLockedReads: true });
    const revision = await captureRolloutRevision(value.home, { mode: "provider" });
    const changes = await collectProviderChanges(value.home, "openai", { skipLockedReads: true });
    assert.deepEqual(probe.counts(), { opens: 3, bytes: 3 * 64 * 1024 });
    const facts = await collectProviderPreparationFacts(value.home, "openai");
    assert.deepEqual(probe.counts(), { opens: 4, bytes: 4 * 64 * 1024 });
    assert.deepEqual(facts.rollout, revision);
    assert.deepEqual(facts.scan, changes);
    assert.deepEqual(facts.scan.providerCounts, status.providerCounts);
  } finally { probe.restore(); }
});

test("Sync and Switch Prepare each read one header, while Apply rereads fresh timestamps and preserves appended bytes", async t => {
  const value = await fixture(t);
  const probe = headerReadProbe(value.file);
  let plan;
  try {
    plan = await prepareSync({ codexHome: value.home });
    assert.deepEqual(probe.counts(), { opens: 1, bytes: 64 * 1024 });
    const switched = await prepareSwitch({ codexHome: value.home, provider: "prov_a" });
    assert.deepEqual(probe.counts(), { opens: 2, bytes: 2 * 64 * 1024 });
    assert.equal(switched.target.previousProvider, "openai");
    assert.equal(JSON.stringify(plan).includes("originalFirstLine"), false);
    assert.equal(JSON.stringify(plan).includes("providerScan"), false);
  } finally { probe.restore(); }
  await fs.appendFile(value.file, "\nnew-append\n");
  const appendedMtime = new Date("2026-01-03T03:04:05.456Z");
  await fs.utimes(value.file, appendedMtime, appendedMtime);
  const before = await fs.stat(value.file, { bigint: true });
  const result = await applySync({ schemaVersion: 1, planId: plan.planId });
  assert.equal(result.outcome, "completed");
  const after = await fs.stat(value.file, { bigint: true });
  assert.equal(after.mtimeNs, before.mtimeNs, "Apply must restore its own fresh mtime, not Prepare's mtime.");
  assert.equal(after.ino, before.ino);
  const contents = await fs.readFile(value.file);
  assert.deepEqual(contents.subarray(contents.indexOf(10) + 1), Buffer.concat([value.body, Buffer.from("\nnew-append\n")]));
});

test("merged Prepare retains locked cause/revision and never turns locked facts into write descriptors", async t => {
  const value = await fixture(t);
  for (const code of ["EBUSY", "EACCES", "EPERM", "ETXTBSY"]) {
    const fsImpl = { ...fs, async open(target, ...args) {
      if (path.resolve(String(target)) === value.file) throw Object.assign(new Error("fixture locked"), { code });
      return fs.open(target, ...args);
    } };
    const old = await captureRolloutRevision(value.home, { mode: "provider", fsImpl });
    const facts = await collectProviderPreparationFacts(value.home, "openai", { fsImpl });
    assert.deepEqual(facts.rollout, old);
    assert.deepEqual(facts.scan.lockedPaths, [value.file]);
    assert.equal(facts.scan.changes.length, 0);
    assert.equal(facts.rollout.rolloutScanComplete, false);
  }
});

test("shared facts keep nested active and archived inventories separate and revision-identical", async t => {
  const value = await fixture(t);
  for (const [relative, provider] of [
    ["sessions/nested/rollout-current.jsonl", "openai"],
    ["archived_sessions/nested/rollout-old.jsonl", "old"],
    ["archived_sessions/rollout-missing.jsonl", undefined]
  ]) {
    const file = path.join(value.home, relative);
    await fs.mkdir(path.dirname(file), { recursive: true });
    await fs.writeFile(file, JSON.stringify({ type: "session_meta", payload: { id: relative, model_provider: provider } }) + "\n");
  }
  const expected = await collectProviderChanges(value.home, "openai", { skipLockedReads: true });
  const expectedRevision = await captureRolloutRevision(value.home, { mode: "provider" });
  const facts = await collectProviderPreparationFacts(value.home, "openai");
  assert.deepEqual(facts.rollout, expectedRevision);
  assert.equal(facts.rollout.fileCount, 4);
  assert.deepEqual(facts.scan.providerCounts, expected.providerCounts);
  assert.deepEqual(facts.scan.changes.map(change => [change.path, change]).sort(), expected.changes.map(change => [change.path, change]).sort());
  assert.equal(facts.scan.changes.filter(change => change.directory === "sessions").length, 1);
  assert.equal(facts.scan.changes.filter(change => change.directory === "archived_sessions").length, 2);
});

test("merged Prepare rejects invalid, oversized and symlinked metadata without writing", async t => {
  const value = await fixture(t);
  for (const content of ["not-json\n", '{"type":"event_msg","payload":{}}\n', "x".repeat(1024 * 1024 + 1) + "\n"]) {
    await fs.writeFile(value.file, content);
    await assert.rejects(collectProviderPreparationFacts(value.home, "openai"), error => error.code === "ROLLOUT_CHANGED");
    assert.equal(await fs.readFile(value.file, "utf8"), content);
  }
  await fs.writeFile(value.file, value.line + "\n");
  const link = path.join(value.home, "sessions", "linked-directory");
  await fs.symlink(path.join(value.home, "archived_sessions"), link, process.platform === "win32" ? "junction" : "dir");
  for (const read of [() => captureRolloutRevision(value.home, { mode: "provider" }), () => collectProviderPreparationFacts(value.home, "openai")]) {
    await assert.rejects(read(), error => error.code === "STALE_STATE");
  }
});

test("a candidate appended during the shared header read remains skipped, never frozen into a stale descriptor", async t => {
  const value = await fixture(t);
  let appended = false;
  const fsImpl = { ...fs, async open(target, ...args) {
    const handle = await fs.open(target, ...args);
    if (path.resolve(String(target)) !== value.file) return handle;
    const read = handle.read.bind(handle);
    handle.read = async (...readArgs) => {
      const result = await read(...readArgs);
      if (!appended) { appended = true; await fs.appendFile(value.file, "new bytes"); }
      return result;
    };
    return handle;
  } };
  const facts = await collectProviderPreparationFacts(value.home, "openai", { fsImpl });
  assert.equal(facts.scan.changes.length, 0);
  assert.deepEqual(facts.scan.lockedPaths, [value.file]);
  assert.equal(facts.rollout.observedSizes["sessions/rollout-fixture.jsonl"], String((await fs.stat(value.file)).size));
});
