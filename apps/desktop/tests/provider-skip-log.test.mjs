import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { OperationLogService } from "../dist/main/operation-log-service.js";
import { redactOperationLogs } from "../dist/main/diagnostics-export.js";
import { validateOperationLogEntry } from "../dist/shared/operation-log-validation.js";
import { isSkipSummary, publicSkipSummary } from "../../../packages/contracts/dist/index.js";

test("local skip details survive restart; diagnostic export removes paths and row identities", async t => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "skip-logs-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const logs = new OperationLogService({ directory: root });
  await logs.initialize();
  const skipSummary = { total: 205, rolloutFiles: 204, sqliteRows: 1, unconfirmed: 1, omitted: 5, retryRecommended: false, items: [
    { kind: "sqlite", id: "private-row-identity", reason: "association-unknown", stage: "plan", retryable: false },
    ...Array.from({ length: 199 }, (_, n) => ({ kind: "rollout", path: `D:\\private-user\\sessions\\file-${n}.jsonl`, reason: n === 0 ? "metadata-invalid-utf8" : n === 1 ? "metadata-too-complex" : "metadata-invalid", stage: "scan", retryable: false }))
  ] };
  assert.ok(isSkipSummary(skipSummary));
  assert.equal(publicSkipSummary({ ...skipSummary, body: "must not leak" }), undefined);
  assert.equal(publicSkipSummary({ ...skipSummary, items: [...skipSummary.items, skipSummary.items[0]] }), undefined);
  assert.equal(publicSkipSummary({ ...skipSummary, items: [{ ...skipSummary.items[0], error: "raw" }] }), undefined);
  const id = await logs.begin({ operation: "sync" });
  await logs.prepared(id, "plan", { impact: { skipSummary, rolloutFilesToChange: 1, sqliteRowsToChange: 1, lockedRolloutFiles: 0 } });
  assert.deepEqual(logs.get(id).skipSummary, skipSummary);
  await logs.finish(id, { status: "partial", outcome: "partial", partialReason: "skipped-data", skipSummary, counts: { changedSessionFiles: 1 } });
  const restarted = new OperationLogService({ directory: root });
  await restarted.initialize();
  const entry = validateOperationLogEntry(restarted.get(id));
  assert.deepEqual(entry.skipSummary, skipSummary);
  assert.equal(entry.partialReason, "skipped-data");
  assert.match(restarted.recentJsonLines(), /private-user/);
  for (const exported of [restarted.recentRedactedJsonLines(), redactOperationLogs(restarted.recentJsonLines())]) {
    assert.doesNotMatch(exported, /private-user|private-row-identity|file-0/);
    const parsed = JSON.parse(exported);
    assert.equal(parsed.skipSummary.total, 205);
    assert.equal(parsed.skipSummary.omitted, 5);
    assert.equal(parsed.skipSummary.items[0].reason, "association-unknown");
    assert.equal(parsed.skipSummary.items[1].reason, "metadata-invalid-utf8");
    assert.equal(parsed.skipSummary.items[2].reason, "metadata-too-complex");
  }
  assert.equal(redactOperationLogs('{"body":"private"}\nnot-json'), "");
});
