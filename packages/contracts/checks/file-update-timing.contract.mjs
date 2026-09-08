import test from "node:test";
import assert from "node:assert/strict";
import { isFileUpdateTiming, publicFileUpdateTiming, assertCoreMethodOutput } from "../dist/index.js";
import { fileUpdateTimingFixture as timing } from "../../../test-support/file-update-timing-fixture.mjs";

test("file timing is closed, numeric and optional for older OperationResult values", () => {
  assert.equal(isFileUpdateTiming(timing), true);
  assert.deepEqual(publicFileUpdateTiming(timing), timing);
  assert.notEqual(publicFileUpdateTiming(timing), timing);
  const envelope = { schemaVersion: 1, operationId: "fixture-operation", operation: "sync", outcome: "partial", backup: null, warnings: [], result: { fileUpdateTiming: timing } };
  assert.doesNotThrow(() => assertCoreMethodOutput("applySync", envelope));
  assert.doesNotThrow(() => assertCoreMethodOutput("applySync", { ...envelope, result: {} }));
  for (const patch of [{ copyTailMs: -1 }, { flushMs: NaN }, { replaceMs: Infinity }, { cleanupMs: "4" }, { restoreMtimeMs: undefined }, { attemptedFiles: 1.5 }, { measuredFiles: 5 }, { skippedFiles: 4 }, { path: "PRIVATE_MARKER" }, { scope: "all" }, { schemaVersion: 2 }]) {
    const bad = { ...timing, ...patch };
    assert.equal(publicFileUpdateTiming(bad), undefined);
    assert.throws(() => assertCoreMethodOutput("applySync", { ...envelope, result: { fileUpdateTiming: bad } }));
  }
  assert.equal(isFileUpdateTiming({ ...timing, attemptedFiles: 4 }), true, "partial/crashed request may lack one measured file");
});
