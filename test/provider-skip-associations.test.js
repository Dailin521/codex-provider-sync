import assert from "node:assert/strict";
import path from "node:path";
import test from "node:test";
import { selectProviderRows, rolloutSkip, summarizeSkips } from "../src/provider-skips.js";
import { isSkipSummary } from "../packages/contracts/dist/index.js";
const home = path.resolve("D:/synthetic-associations");
const good = path.join(home, "sessions", "rollout-good.jsonl");
const bad = path.join(home, "sessions", "rollout-bad.jsonl");
const row = (id, rollout_path) => ({ id, rollout_path, model_provider: "old" });
test("duplicate ID and path associations are excluded without guessing from filenames", () => {
  const scan = { files: [{ path: good, id: "same" }, { path: bad, id: "same" }], skippedItems: [] };
  const result = selectProviderRows(home, scan, { key: "id", rows: [row("same", good), row("other", null)] }, "openai");
  assert.deepEqual(result.rows.map(r => r.id), ["other"]);
  assert.equal(result.skippedItems[0].reason, "association-conflict");
  const sharedPath = selectProviderRows(home, { files: [{ path: good, id: null }], skippedItems: [] }, { key: "id", rows: [row("one", good), row("two", good)] }, "openai");
  assert.equal(sharedPath.rows.length, 0);
});
test("a skipped known ID without an index row does not block unrelated SQLite-only rows", () => {
  const result = selectProviderRows(home, { files: [{ path: bad, id: "known" }], skippedItems: [rolloutSkip(bad, "metadata-too-large", "scan", "known")] },
    { key: "id", rows: [row("index-only", null)] }, "openai");
  assert.deepEqual(result.rows.map(r => r.id), ["index-only"]);
});
test("unknown bad metadata restricts updates to positive healthy associations", () => {
  const scan = { files: [{ path: good, id: "good" }, { path: bad, id: null }], skippedItems: [rolloutSkip(bad, "metadata-invalid")] };
  for (const unknownPath of [null, path.join(home, "..", "outside.jsonl"), "../../sessions/rollout-bad.jsonl"]) {
    const result = selectProviderRows(home, scan, { key: "id", rows: [row("good", good), row("unknown", unknownPath)] }, "openai");
    assert.deepEqual(result.rows.map(r => r.id), ["good"]);
  }
});
test("skip summaries deduplicate and bound detail bytes without exposing internal identifiers", () => {
  const items = Array.from({ length: 205 }, (_, n) => rolloutSkip(path.join(home, "sessions", `rollout-${n}.jsonl`), "changed", "revalidate", "unsafe/private-id"));
  const summary = summarizeSkips([...items, ...items]);
  assert.equal(summary.total, 205);
  assert.equal(summary.omitted, 5);
  assert.ok(isSkipSummary(summary));
  assert.doesNotMatch(JSON.stringify(summary), /unsafe\/private-id/);
  const huge = summarizeSkips(Array.from({ length: 200 }, (_, n) => rolloutSkip(path.join(home, "sessions", `rollout-${n}-${"长".repeat(10000)}.jsonl`), "metadata-invalid")));
  assert.equal(huge.total, 200);
  assert.ok(huge.omitted > 0);
  assert.ok(isSkipSummary(huge));
  assert.ok(Buffer.byteLength(JSON.stringify(huge)) <= 1024 * 1024);
});
