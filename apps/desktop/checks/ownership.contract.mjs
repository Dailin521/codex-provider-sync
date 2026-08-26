import assert from "node:assert/strict";
import test from "node:test";

import { DESKTOP_RUNTIME_STATE } from "../dist/index.js";

test("desktop workspace records the C6 read-only runtime boundary", () => {
  assert.equal(DESKTOP_RUNTIME_STATE, "readonly-c6");
});
