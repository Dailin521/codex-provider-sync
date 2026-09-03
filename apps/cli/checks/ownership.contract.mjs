import assert from "node:assert/strict";
import test from "node:test";

import { CLI_MIGRATION_STATE } from "../src/ownership.js";

test("CLI workspace records root-package compatibility ownership", () => {
  assert.deepEqual(CLI_MIGRATION_STATE, {
    compatibilityEntrypoint: "src/cli.js",
    owner: "apps/cli",
    implementationMoved: false
  });
  assert.ok(Object.isFrozen(CLI_MIGRATION_STATE));
});
