import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test, { afterEach } from "node:test";
import { readSessionActivity, readWindowsWriterOwners } from "../src/session-activity.js";
import { createCoreFacade } from "../packages/core/src/index.js";
import { createWriterSessionFixture } from "../test-support/writer-session-fixture.mjs";

const ids = ["11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"];
const cleanups = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });
async function fixture(t, { locks = true } = {}) {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "activity-中文 [1]-"));
  cleanups.push(() => fs.rm(home, { recursive: true, force: true }));
  const directory = path.join(home, "thread-writer-locks");
  if (locks) {
    await fs.mkdir(directory);
    await fs.writeFile(path.join(directory, ".coordination.lock"), "");
    for (const id of ids) await fs.writeFile(path.join(directory, `${id}.lock`), "");
  }
  return { home, directory };
}

test("activity counts owned UUID sessions, not processes, stale files or Provider differences", async (t) => {
  const { home, directory } = await fixture(t);
  const before = await fs.stat(directory);
  const result = await readSessionActivity(home, { platform: "win32", readOwners: async (files) => {
    assert.equal(files.length, 2);
    assert.ok(files.every((file) => !file.endsWith(".coordination.lock")));
    return [true, false];
  } });
  assert.deepEqual(result, { state: "checked", count: 1 });
  assert.equal((await fs.stat(directory)).mtimeMs, before.mtimeMs);
  assert.deepEqual(await readSessionActivity(home, { platform: "win32", readOwners: async () => [false, false] }), { state: "checked", count: 0 });
  assert.deepEqual((await fs.readdir(home)), ["thread-writer-locks"], "no plans, backups, DB or message reads needed");
});

test("activity isolates physical Homes, reports unsupported platforms and missing protocol as unknown", async (t) => {
  const a = await fixture(t);
  const b = await fixture(t, { locks: false });
  assert.deepEqual(await readSessionActivity(a.home, { platform: "win32", readOwners: async () => [true, true] }), { state: "checked", count: 2 });
  assert.deepEqual(await readSessionActivity(b.home, { platform: "win32", readOwners: async () => { throw new Error("must not query"); } }), { state: "unavailable", count: null });
  for (const platform of ["linux", "darwin"]) assert.deepEqual(await readSessionActivity(a.home, { platform }), { state: "unsupported", count: null });
  assert.deepEqual(await readSessionActivity("\\\\wsl.localhost\\Fixture\\home", { platform: "win32" }), { state: "unsupported", count: null });
});

test("unknown schema, nonempty records, changed inventory, failed or malformed native queries never become zero", async (t) => {
  for (const scenario of ["unknown-file", "nonempty", "removed-coordination", "changed", "failure", "malformed"]) {
    const { home, directory } = await fixture(t);
    if (scenario === "unknown-file") await fs.writeFile(path.join(directory, "future-protocol.json"), "");
    if (scenario === "nonempty") await fs.writeFile(path.join(directory, `${ids[0]}.lock`), "future-schema");
    if (scenario === "removed-coordination") await fs.unlink(path.join(directory, ".coordination.lock"));
    const result = await readSessionActivity(home, { platform: "win32", readOwners: async () => {
      if (scenario === "changed") await fs.unlink(path.join(directory, `${ids[0]}.lock`));
      if (scenario === "failure") throw new Error("private details must not escape");
      return scenario === "malformed" ? [false] : [false, false];
    } });
    assert.deepEqual(result, { state: "unavailable", count: null }, scenario);
  }
});

test("activity rejects redirected writer directories", async (t) => {
  const { home, directory } = await fixture(t, { locks: false });
  const target = await fixture(t);
  try { await fs.symlink(target.directory, directory, process.platform === "win32" ? "junction" : "dir"); }
  catch (error) { if (error.code === "EPERM") { t.skip("symlink permission unavailable"); return; } throw error; }
  assert.deepEqual(await readSessionActivity(home, { platform: "win32" }), { state: "unavailable", count: null });
});

test("activity rejects a writer directory swapped during enumeration", async (t) => {
  const { home, directory } = await fixture(t);
  const canonicalDirectory = await fs.realpath(directory);
  const foreign = await fixture(t);
  const originalReadDir = fs.readdir;
  let swapped = false;
  fs.readdir = async function (target, ...args) {
    if (String(target) === canonicalDirectory && !swapped) {
      swapped = true;
      await fs.rename(directory, path.join(home, "saved-writer-directory"));
      await fs.symlink(foreign.directory, directory, process.platform === "win32" ? "junction" : "dir");
    }
    return originalReadDir.call(this, target, ...args);
  };
  let queries = 0;
  try {
    assert.deepEqual(await readSessionActivity(home, { platform: "win32", readOwners: async () => { queries++; return [true, true]; } }), { state: "unavailable", count: null });
    assert.equal(queries, 0, "do not query foreign resource owners after a reparse swap");
  } finally { fs.readdir = originalReadDir; }
});

test("hidden owner query has a deadline, kills its child, and never exposes stderr", async () => {
  const child = Object.assign(new EventEmitter(), { stdin: new PassThrough(), stdout: new PassThrough(), stderr: new PassThrough(), kill() { this.killed = true; this.emit("close", null); } });
  const spawnProcess = (_exe, _args, options) => { assert.equal(options.windowsHide, true); return child; };
  const result = readWindowsWriterOwners(["fixture.lock"], { spawnProcess, timeoutMs: 10 });
  child.stderr.write("private path and process details");
  await assert.rejects(result, { message: "Writer ownership unavailable." });
  assert.equal(child.killed, true);
});

test("owner query spawn failure settles once even without a close event", async () => {
  const child = Object.assign(new EventEmitter(), { stdin: new PassThrough(), stdout: new PassThrough(), stderr: new PassThrough(), kill() { throw new Error("no process"); } });
  const result = readWindowsWriterOwners(["fixture.lock"], { spawnProcess: () => child, timeoutMs: 8000 });
  child.emit("error", new Error("private executable path"));
  await assert.rejects(result, { message: "Writer ownership unavailable." });
  child.stdin.emit("error", new Error("late EPIPE"));
});

test("Windows real writer ownership counts aligned sessions in Status and Preview, then excludes released stale locks", { skip: process.platform !== "win32" }, async (t) => {
  const { home } = await fixture(t, { locks: false });
  await fs.writeFile(path.join(home, "config.toml"), 'model_provider = "openai"\n');
  await fs.mkdir(path.join(home, "sessions"));
  const rollout = path.join(home, "sessions", "rollout-aligned.jsonl");
  const content = JSON.stringify({ type: "session_meta", payload: { id: ids[0], model_provider: "openai" } }) + "\nDO_NOT_PARSE_THIS_BODY\n";
  await fs.writeFile(rollout, content);
  const before = await fs.stat(rollout);
  const owner = await createWriterSessionFixture(home, ids);
  cleanups.push(() => owner.stop());
  const facade = createCoreFacade({ resolveProfile: async () => ({ id: "fixture", revision: "r1", codexHome: home }) });
  const selector = { profile: { profileId: "fixture", profileRevision: "r1" } };
  const activity = { state: "checked", count: 2 };
  assert.deepEqual(await readSessionActivity(home), activity, "same PID owns two waiting sessions; Unicode paths survive stdin");
  assert.deepEqual(await readWindowsWriterOwners([owner.files[0]]), [true], "single-owner PowerShell JSON remains an array");
  const status = await facade.getStatus(selector);
  assert.deepEqual(status.sessionActivity, activity);
  assert.equal(status.syncSessionUsage, undefined, "do not keep a second target-only probe in Status");
  const plan = await facade.prepareSync(selector);
  assert.deepEqual(plan.impact.sessionActivity, activity, "public Plan sanitizer keeps the pathless snapshot");
  assert.equal(plan.impact.lockedRolloutFiles, 0, "aligned sessions have no sync blockers even when in use");
  assert.equal(plan.impact.rolloutFilesToChange, 0);
  assert.equal(status.backupSummary.count, 0);
  await owner.stop();
  assert.deepEqual(await readSessionActivity(home), { state: "checked", count: 0 });
  // An unrelated process opening a stale lock does not represent a Codex writer.
  const unrelated = await fs.open(owner.files[0], "r");
  try { assert.deepEqual(await readSessionActivity(home), { state: "checked", count: 0 }); } finally { await unrelated.close(); }
  assert.equal(await fs.readFile(rollout, "utf8"), content);
  assert.equal((await fs.stat(rollout)).ino, before.ino);
  assert.equal((await fs.stat(rollout)).mtimeMs, before.mtimeMs);
});
