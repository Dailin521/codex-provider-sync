import assert from "node:assert/strict";
import path from "node:path";
import test from "node:test";
import { selectProviderRows, rolloutSkip, summarizeSkips, providerPathKey } from "../src/provider-skips.js";
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

test("an anchored paginated group has one owner regardless of ordinal", () => {
  const files = [good, bad].map(filePath => ({ path: filePath, id: "same", historyMode: "paginated", ordinal: 0 }));
  const result = selectProviderRows(home, { files, skippedItems: [] }, { key: "id", rows: [row("same", bad)] }, "openai");
  assert.deepEqual(result.rows.map(r => [r.id, r.paths]), [["same", [good, bad]]]);
  assert.deepEqual(result.skippedItems, []);
  for (const state of [
    { key: "id", rows: [row("same", null)] },
    { key: "id", rows: [row("same", bad), row("other", good)] }
  ]) {
    assert.equal(selectProviderRows(home, { files, skippedItems: [] }, state, "openai").rows.length, 0);
  }
  const rowIdSelection = selectProviderRows(home, { files, skippedItems: [] }, { key: "rowid", rows: [row("same", bad)] }, "openai");
  assert.deepEqual(rowIdSelection.rows[0].paths, [bad], "A legacy rowid path association must not become a thread-ID group.");
  const mixed = files.map((file, index) => index ? { ...file, historyMode: null } : file);
  assert.equal(selectProviderRows(home, { files: mixed, skippedItems: [] }, { key: "id", rows: [row("same", bad)] }, "openai").rows.length, 0);
  const wrong = { path: path.join(home, "sessions", "rollout-other.jsonl"), id: "other", historyMode: "paginated" };
  assert.equal(selectProviderRows(home, { files: [...files, wrong], skippedItems: [] }, { key: "id", rows: [row("same", wrong.path)] }, "openai").rows.length, 0);
});

test("skipped paginated members protect the owner and unknown files protect even paginated singletons", () => {
  const files = [good, bad].map(filePath => ({ path: filePath, id: "same", historyMode: "paginated" }));
  const state = { key: "id", rows: [row("same", good)] };
  const result = selectProviderRows(home, { files, skippedItems: [rolloutSkip(bad, "locked", "write", "same")] }, state, "openai");
  assert.equal(result.rows.length, 0);
  assert.equal(result.skippedItems[0].reason, "locked");
  assert.equal(result.skippedItems[0].retryable, true);
  const legacy = { path: path.join(home, "sessions", "rollout-legacy.jsonl"), id: "legacy" };
  const unknown = rolloutSkip(bad, "deferred", "revalidate");
  const restricted = selectProviderRows(home, { files: [files[0], legacy], skippedItems: [unknown] },
    { key: "id", rows: [...state.rows, row("legacy", legacy.path), row("sqlite-only", null)] }, "openai");
  assert.deepEqual(restricted.rows.map(r => r.id), ["legacy"]);
  assert.deepEqual(restricted.skippedItems.map(r => [r.id, r.reason]), [["same", "association-unknown"], ["sqlite-only", "association-unknown"]]);
});

test("a deferred known ID that makes its own group ambiguous does not block another paginated group", () => {
  const other = { path: path.join(home, "sessions", "rollout-other.jsonl"), id: "other", historyMode: "paginated" };
  const files = [{ path: good, id: "same", historyMode: "paginated" }, { path: bad, id: "same", historyMode: null }, other];
  const result = selectProviderRows(home, { files, skippedItems: [rolloutSkip(bad, "deferred", "revalidate", "same")] },
    { key: "id", rows: [row("same", good), row("other", other.path)] }, "openai");
  assert.deepEqual(result.rows.map(r => r.id), ["other"]);
  assert.deepEqual(result.skippedItems.map(r => [r.id, r.reason]), [["same", "association-conflict"]]);
});

test("only an unidentified deferred file overrides its SQLite path owner's positive association", () => {
  const legacy = { path: path.join(home, "sessions", "rollout-legacy.jsonl"), id: "legacy", historyMode: null };
  const files = [{ path: good, id: "paged", historyMode: "paginated" }, { path: bad, id: null, historyMode: null }, legacy];
  for (const ownerProvider of ["old", "openai"]) for (const reason of ["deferred", "locked", "metadata-invalid"]) {
    const scan = { files, skippedItems: [rolloutSkip(bad, reason)] };
    const state = { key: "id", rows: [row("paged", good), { ...row("owner", bad), model_provider: ownerProvider }, row("legacy", legacy.path)] };
    const result = selectProviderRows(home, scan, state, "openai");
    assert.deepEqual(result.rows.map(r => r.id), reason === "deferred" ? ["legacy"] : ["paged", "legacy"], `${ownerProvider}: ${reason}`);
    if (reason === "deferred") assert.ok(result.skippedItems.some(item => item.id === "paged" && item.reason === "association-unknown"));
  }
});

test("Windows DOS and UNC namespace aliases associate without changing stored paths", { skip: process.platform !== "win32" }, () => {
  for (const testHome of [home, "\\\\server\\share\\fixture"]) {
    const file = path.join(testHome, "sessions", "rollout-fixture.jsonl");
    const alias = path.toNamespacedPath(file);
    assert.equal(providerPathKey(file), providerPathKey(alias));
    const scan = { files: [{ path: file, id: null }], skippedItems: [rolloutSkip(file, "locked")] };
    const result = selectProviderRows(path.toNamespacedPath(testHome), scan, { key: "id", rows: [row("test", alias)] }, "openai");
    assert.equal(result.rows.length, 0);
    assert.equal(result.skippedItems[0].reason, "locked");
    const paginated = { files: [file, path.join(testHome, "archived_sessions", "rollout-old.jsonl")]
      .map(p => ({ path: p, id: "test", historyMode: "paginated" })), skippedItems: [] };
    const selected = selectProviderRows(testHome, paginated, { key: "id", rows: [row("test", alias)] }, "openai");
    assert.equal(selected.rows.length, 1);
    assert.equal(selected.rows[0].rollout_path, alias);
    for (const invalid of [path.toNamespacedPath(path.join(testHome, "..", "outside.jsonl")), "\\\\?\\GLOBALROOT\\Device\\sessions\\rollout-fixture.jsonl", "\\\\.\\D:\\sessions\\rollout-fixture.jsonl", "sessions\\..\\..\\outside.jsonl", "sessions\\rollout\0-fixture.jsonl"]) {
      assert.equal(selectProviderRows(testHome, paginated, { key: "id", rows: [row("test", invalid)] }, "openai").rows.length, 0);
    }
  }
});
test("unknown bad metadata restricts updates to positive healthy associations", () => {
  const scan = { files: [{ path: good, id: "good" }, { path: bad, id: null }], skippedItems: [rolloutSkip(bad, "metadata-invalid")] };
  for (const unknownPath of [null, path.join(home, "..", "outside.jsonl"), "../../sessions/rollout-bad.jsonl"]) {
    const result = selectProviderRows(home, scan, { key: "id", rows: [row("good", good), row("unknown", unknownPath)] }, "openai");
    assert.deepEqual(result.rows.map(r => r.id), ["good"]);
  }
});
for (const [layout, testHome] of [["DOS", home], ["UNC", "\\\\server\\share\\fixture"]]) {
  test(`Windows ${layout} directory casing preserves anchors and foreign owner protection`, { skip: process.platform !== "win32" }, () => {
    const files = ["sessions", "archived_sessions"].map(directory => ({
      path: path.join(testHome, directory, "rollout-fixture.jsonl"), id: "same", historyMode: "paginated"
    }));
    for (const file of files) for (const alias of [file.path.toUpperCase(), path.toNamespacedPath(file.path).toUpperCase()]) {
      assert.equal(providerPathKey(file.path), providerPathKey(alias));
      const scan = { files, skippedItems: [] };
      const anchored = selectProviderRows(testHome, scan, { key: "id", rows: [row("same", alias)] }, "openai");
      assert.deepEqual(anchored.rows.map(item => item.id), ["same"]);
      assert.equal(anchored.rows[0].rollout_path, alias);
      const contested = selectProviderRows(testHome, scan, { key: "id", rows: [
        row("same", files[0].path), { ...row("other", alias), model_provider: "openai" }, row("healthy", null)
      ] }, "openai");
      assert.deepEqual(contested.rows.map(item => item.id), ["healthy"]);
      assert.deepEqual(contested.skippedItems.map(item => [item.id, item.reason]), [["same", "association-conflict"]]);
    }
  });
}
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
