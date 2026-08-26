import assert from "node:assert/strict";
import test from "node:test";

import { DESKTOP_RUNTIME_STATE } from "../dist/index.js";

test("desktop workspace records the C7 Sync/Switch runtime boundary", () => {
  assert.equal(DESKTOP_RUNTIME_STATE, "sync-switch-c7");
});
