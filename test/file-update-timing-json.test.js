import test from "node:test";
import assert from "node:assert/strict";
import { createCliSuccessEnvelope } from "../src/cli-json.js";
import { fileUpdateTimingFixture as timing } from "../test-support/file-update-timing-fixture.mjs";

test("CLI Sync/Switch JSON preserves optional numeric file timing without changing outcome/envelope", () => {
  for (const command of ["sync", "switch"]) {
    const result = createCliSuccessEnvelope(command, { partial: true, fileUpdateTiming: timing });
    assert.equal(result.outcome, "partial");
    assert.deepEqual(result.result.fileUpdateTiming, timing);
    assert.deepEqual(Object.keys(result), ["schemaVersion", "command", "ok", "outcome", "result", "warnings", "error"]);
    assert.equal(createCliSuccessEnvelope(command, {}).result.fileUpdateTiming, undefined);
    assert.equal(createCliSuccessEnvelope(command, { fileUpdateTiming: { ...timing, path: "PRIVATE_MARKER" } }).result.fileUpdateTiming, undefined);
  }
});
