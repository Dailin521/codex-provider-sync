import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { OperationLogService } from "../dist/main/operation-log-service.js";
import { validateOperationLogEntry } from "../dist/shared/operation-log-validation.js";
import { assertRuntimeWatchActivityFrame, createRuntimeWatchActivityFrame } from "../dist/shared/runtime-protocol.js";
import { fileUpdateTimingFixture as timing } from "../../../test-support/file-update-timing-fixture.mjs";

test("file update detail survives completed/partial logs, restart and diagnostic export; invalid detail is absent", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-file-timing-log-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const logs = new OperationLogService({ directory: root });
  await logs.initialize();
  const ids = [];
  for (const [operation, outcome] of [["sync", "completed"], ["switch", "partial"], ["watch", "partial"]]) {
    const id = await logs.begin({ operation }); ids.push(id);
    if (operation === "watch") await logs.finish(id, { status: outcome, fileUpdateTiming: timing });
    else await logs.finishFromResponse(id, { protocolVersion: 1, requestId: "apply", ok: true, result: {
      schemaVersion: 1, operationId: "operation", operation, outcome, backup: { backupId: "backup" }, warnings: [],
      result: { fileUpdateTiming: timing }
    } });
    assert.deepEqual(validateOperationLogEntry(logs.get(id)).fileUpdateTiming, timing);
  }
  const interrupted = await logs.begin({ operation: "sync" });
  for (const patch of [{ rawPath: "PRIVATE_MARKER" }, { flushMs: -1 }, { measuredFiles: 4 }]) {
    const id = await logs.begin({ operation: "sync" });
    await logs.finish(id, { status: "completed", fileUpdateTiming: { ...timing, ...patch } });
    assert.equal(logs.get(id).status, "completed");
    assert.equal(logs.get(id).fileUpdateTiming, undefined);
    assert.throws(() => validateOperationLogEntry({ ...logs.get(id), fileUpdateTiming: { ...timing, ...patch } }));
  }
  assert.doesNotMatch(logs.recentJsonLines(), /PRIVATE_MARKER/);
  const restarted = new OperationLogService({ directory: root }); await restarted.initialize();
  for (const id of ids) assert.deepEqual(restarted.get(id).fileUpdateTiming, timing);
  assert.equal(restarted.get(interrupted).status, "interrupted");
  assert.equal(restarted.get(interrupted).fileUpdateTiming, undefined);
  assert.ok(JSON.parse(restarted.recentJsonLines().split("\n").find((line) => JSON.parse(line).id === ids[0])).fileUpdateTiming);
});

test("Watch accepts file timing only on finished activity and retains strict numeric boundary", () => {
  const base = { schemaVersion: 1, event: "finished", activityId: "activity", watchId: "watch", profileId: "default", finishedAt: "2026-09-08T00:00:00Z", outcome: "partial", fileUpdateTiming: timing };
  const frame = createRuntimeWatchActivityFrame(1, base);
  assert.doesNotThrow(() => assertRuntimeWatchActivityFrame(frame));
  assert.throws(() => assertRuntimeWatchActivityFrame({ ...frame, activity: { ...base, fileUpdateTiming: { ...timing, body: "PRIVATE_MARKER" } } }));
  assert.throws(() => createRuntimeWatchActivityFrame(1, { ...base, event: "started", startedAt: base.finishedAt, finishedAt: undefined, outcome: undefined }));
});
