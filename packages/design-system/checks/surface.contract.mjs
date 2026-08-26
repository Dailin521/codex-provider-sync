import assert from "node:assert/strict";
import fs from "node:fs/promises";
import test from "node:test";

import {
  DESIGN_SYSTEM_MIGRATION_STATE,
  SUPPORTED_LOCALES,
  THEME_MODES
} from "../dist/index.js";

test("design-system owns the frozen locale and theme vocabulary", async () => {
  assert.deepEqual(THEME_MODES, ["system", "light", "dark"]);
  assert.deepEqual(SUPPORTED_LOCALES, ["zh-CN", "en"]);
  assert.equal(DESIGN_SYSTEM_MIGRATION_STATE, "tokens-and-primitives-c5");
  const tokens = await fs.readFile(new URL("../src/tokens.css", import.meta.url), "utf8");
  assert.match(tokens, /data-theme="dark"/);
  assert.match(tokens, /prefers-color-scheme:\s*dark/);
  assert.match(tokens, /prefers-reduced-motion:\s*reduce/);
  assert.match(tokens, /--focus:/);
});
