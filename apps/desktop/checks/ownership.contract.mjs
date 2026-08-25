import assert from "node:assert/strict";
import test from "node:test";

import { DESKTOP_RUNTIME_STATE } from "../dist/index.js";

test("desktop workspace cannot be mistaken for an enabled Electron runtime", () => {
  assert.equal(DESKTOP_RUNTIME_STATE, "not-enabled-c4");
});
