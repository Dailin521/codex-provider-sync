import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test, { afterEach } from "node:test";
import { createCoreFacade } from "../packages/core/src/index.js";
import { trackScanFiles } from "../src/scan-progress.js";

const cleanups = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });

async function fixture(t) {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "request-progress-"));
  cleanups.push(() => fs.rm(home, { recursive: true, force: true }));
  await fs.mkdir(path.join(home, "sessions"));
  await fs.mkdir(path.join(home, "archived_sessions"));
  await fs.writeFile(path.join(home, "config.toml"), 'model = "target-model"\n');
  const file = path.join(home, "sessions", "rollout-one.jsonl");
  const data = [
    { type: "session_meta", payload: { id: "one", model_provider: "openai", cwd: "C:/synthetic-private" } },
    { type: "turn_context", payload: { model: "old-model" } },
    { type: "event_msg", payload: { type: "user_message", message: "NEVER_IN_PROGRESS" } }
  ].map(JSON.stringify).join("\n") + "\n";
  await fs.writeFile(file, data);
  const facade = createCoreFacade({ resolveProfile: () => ({ id: "default", revision: "r1", codexHome: home }) });
  return { home, file, data, facade, profile: { profileId: "default", profileRevision: "r1" } };
}

test("explicit Diagnostics and Repair previews report real scoped scan progress without writes or operation-started", async (t) => {
  const { home, file, data, facade, profile } = await fixture(t);
  for (const [method, input] of [["getDiagnostics", { profile }], ["prepareRepair", { profile, targets: ["models"] }]]) {
    const events = [];
    const value = await facade[method](input, {
      onOperationStarted() { assert.fail("A read request must not start a write operation"); },
      onProgress(event) { events.push(event); }
    });
    assert.ok(value);
    assert.ok(events.length >= 6);
    assert.ok(events.some((event) => event.stage === "scan_sessions" && event.count === 1 && event.progress === 1));
    assert.ok(events.some((event) => event.stage === "scan_archived_sessions" && event.count === 0 && event.progress === 1));
    for (const stage of ["scan_sessions", "scan_archived_sessions"]) {
      assert.equal(events.filter((event) => event.stage === stage && event.status === "completed").length, 1);
    }
    for (const event of events) assert.ok(Object.keys(event).every((key) => ["stage", "status", "progress", "count"].includes(key)));
    assert.doesNotMatch(JSON.stringify(events), /synthetic-private|NEVER_IN_PROGRESS|rollout-one|operationId/);
    assert.equal(await fs.readFile(file, "utf8"), data);
    assert.equal((await facade.getStatus({ profile })).operationInProgress, null);
    assert.equal((await facade.listBackups({ profile })).backups.length, 0);
    assert.deepEqual((await fs.readdir(home)).sort(), ["archived_sessions", "config.toml", "sessions"]);
  }
});

test("throwing and rejecting scan observers cannot fail Diagnostics or persist into Apply", async (t) => {
  const { facade, profile } = await fixture(t);
  for (const onProgress of [() => { throw new Error("observer"); }, async () => { throw new Error("observer"); }]) {
    const result = await facade.getDiagnostics({ profile }, { onProgress });
    assert.equal(result.schemaVersion, 1);
  }
  let notifications = 0;
  const controller = new AbortController();
  const plan = await facade.prepareRepair({ profile, targets: ["models"] }, { signal: controller.signal, onProgress() { notifications++; } });
  const before = notifications;
  controller.abort(); // Completed preview lifetime must not cancel the later write.
  const result = await facade.applyRepair({ schemaVersion: 1, planId: plan.planId });
  assert.equal(result.outcome, "completed");
  assert.equal(notifications, before);
});

test("aborted read requests stop before issuing a preview or changing files", async (t) => {
  const { facade, profile, file, data } = await fixture(t);
  for (const method of ["prepareRepair", "getDiagnostics"]) {
    const controller = new AbortController();
    await assert.rejects(facade[method]({ profile, ...(method === "prepareRepair" ? { targets: ["models"] } : {}) }, {
      signal: controller.signal,
      onProgress(event) { if (event.stage === "scan_sessions" && event.count === 0) controller.abort(); }
    }), { name: "AbortError" });
    assert.equal(await fs.readFile(file, "utf8"), data);
  }
});

test("scan tracker preserves order, counts skipped work, emits empty completion and does not fake completion on errors", () => {
  const events = [];
  const options = { stage: "scan_sessions", onProgress: (event) => events.push(event) };
  const visited = [];
  for (const item of trackScanFiles(["a", "b", "c"], options)) {
    visited.push(item);
    if (item === "b") continue;
  }
  assert.deepEqual(visited, ["a", "b", "c"]);
  assert.deepEqual(events.at(-1), { stage: "scan_sessions", status: "completed", progress: 1, count: 3 });
  events.length = 0;
  assert.throws(() => { for (const item of trackScanFiles(["a", "b"], options)) throw new Error(item); });
  assert.equal(events.some((event) => event.status === "completed"), false);
});
