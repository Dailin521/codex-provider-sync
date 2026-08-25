import assert from "node:assert/strict";
import test from "node:test";

import {
  DESIGN_SYSTEM_MIGRATION_STATE,
  SUPPORTED_LOCALES,
  THEME_MODES
} from "../dist/index.js";

test("design-system owns the frozen locale and theme vocabulary", () => {
  assert.deepEqual(THEME_MODES, ["system", "light", "dark"]);
  assert.deepEqual(SUPPORTED_LOCALES, ["zh-CN", "en"]);
  assert.equal(DESIGN_SYSTEM_MIGRATION_STATE, "tokens-only-c4");
});
