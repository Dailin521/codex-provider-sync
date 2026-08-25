import assert from "node:assert/strict";
import path from "node:path";
import test from "node:test";

import * as publicApi from "../src/public-api.js";

const EXPECTED_EXPORTS = [
  "CORE_ERROR_CODES",
  "CoreError",
  "detectStateDb",
  "ensureCodexHome",
  "getHistorySession",
  "getStatus",
  "listBackups",
  "listHistory",
  "readConfigText",
  "readRootModelFromConfigText",
  "resolveStorageLayout",
  "runPruneBackups",
  "runRestore",
  "runSwitch",
  "runSync",
  "runWatch",
  "toCoreErrorDto",
  "withStateDbLocation"
];

test("public Core API has the exact C1 export surface", () => {
  assert.deepEqual(Object.keys(publicApi).sort(), EXPECTED_EXPORTS);
  for (const name of EXPECTED_EXPORTS) {
    if (name !== "CORE_ERROR_CODES") {
      assert.equal(typeof publicApi[name], "function", name + " must be callable");
    }
  }
  assert.ok(Object.isFrozen(publicApi.CORE_ERROR_CODES));
  assert.ok(publicApi.CORE_ERROR_CODES.includes("STALE_STATE"));
});

test("public Core helper adapters retain their current callable behavior", () => {
  assert.equal(
    publicApi.readRootModelFromConfigText(
      'model = "gpt-5"\n[model_providers.example]\nmodel = "ignored"\n'
    ),
    "gpt-5"
  );

  const layout = publicApi.resolveStorageLayout({
    codexHome: "/tmp/codex-provider-sync-public-api",
    configText: ""
  });
  assert.equal(layout.codexHome, path.resolve("/tmp/codex-provider-sync-public-api"));
  assert.equal(typeof publicApi.withStateDbLocation(layout, null), "object");
});
