import assert from "node:assert/strict";
import test from "node:test";

import { APP_ROUTES, APP_UI_MIGRATION_STATE } from "../dist/index.js";

test("app-ui owns the complete target navigation vocabulary", () => {
  assert.deepEqual(APP_ROUTES, [
    "overview",
    "sync",
    "switch-provider",
    "backups-restore",
    "history",
    "profiles",
    "diagnostics",
    "settings"
  ]);
  assert.equal(APP_UI_MIGRATION_STATE, "contract-only-c4");
});
