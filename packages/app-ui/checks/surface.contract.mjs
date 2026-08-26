import assert from "node:assert/strict";
import fs from "node:fs/promises";
import test from "node:test";

import {
  APP_ROUTES,
  APP_UI_MIGRATION_STATE,
  profileSchema,
  resourcesHaveMatchingKeys,
  restoreSchema,
  switchSchema
} from "../dist/index.js";

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
  assert.equal(APP_UI_MIGRATION_STATE, "shared-ui-c5");
});

test("shared UI translations and write forms keep one strict schema", () => {
  assert.equal(resourcesHaveMatchingKeys(), true);
  assert.equal(switchSchema.safeParse({ provider: "relay", modelMode: "explicit", model: "gpt", keepCount: 5 }).success, true);
  assert.equal(switchSchema.safeParse({ provider: "relay", modelMode: "explicit", model: "", keepCount: 5 }).success, false);
  assert.equal(restoreSchema.safeParse({ backupId: "managed", restoreConfig: false, restoreDatabase: false, restoreSessions: false, allowSqliteHomeRelocation: false }).success, false);
  assert.equal(profileSchema.safeParse({ profileId: "safe", name: "Safe", codexHome: "../relative", sqliteHome: "" }).success, false);
});

test("shared UI has no transport, Node, Electron or persistent history access", async () => {
  const appSource = await fs.readFile(new URL("../src/App.tsx", import.meta.url), "utf8");
  const allSource = await Promise.all([
    "App.tsx", "i18n.ts", "routes.ts", "schemas.ts", "types.ts", "ui.tsx"
  ].map((name) => fs.readFile(new URL(`../src/${name}`, import.meta.url), "utf8")));
  const source = allSource.join("\n");
  assert.doesNotMatch(source, /\bfetch\s*\(/);
  assert.doesNotMatch(source, /localStorage|sessionStorage|from\s+["'](?:node:|electron)/);
  assert.doesNotMatch(source, /\/api\//);
  assert.match(appSource, /core\.getHistorySession/);
  assert.match(appSource, /messageLimit:\s*200/);
  assert.doesNotMatch(appSource, /queryKey:\s*\["history-detail"/);
  assert.match(appSource, /schemaVersion:\s*1 as const, planId: summary\.planId/);
});
