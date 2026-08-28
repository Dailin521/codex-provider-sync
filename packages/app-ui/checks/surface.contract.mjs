import assert from "node:assert/strict";
import fs from "node:fs/promises";
import test from "node:test";

import {
  APP_ROUTES,
  APP_UI_MIGRATION_STATE,
  DESKTOP_C8_APP_UI_CAPABILITIES,
  FULL_APP_UI_CAPABILITIES,
  READ_ONLY_APP_UI_CAPABILITIES,
  SYNC_SWITCH_APP_UI_CAPABILITIES,
  profileSchema,
  resourcesHaveMatchingKeys,
  restoreSchema,
  syncSchema,
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

test("shared UI exposes explicit read-only, C7, and C8 capability profiles", async () => {
  assert.deepEqual(READ_ONLY_APP_UI_CAPABILITIES, {
    sync: false,
    switchProvider: false,
    restore: false,
    pruneBackups: false,
    watch: false,
    manageProfiles: false,
    revealProfilePaths: false,
    forgetBrowser: false,
    exportDiagnostics: false,
    viewUpdateStatus: false
  });
  assert.equal(Object.values(FULL_APP_UI_CAPABILITIES).every(Boolean), true);
  assert.equal(Object.isFrozen(READ_ONLY_APP_UI_CAPABILITIES), true);
  assert.deepEqual(SYNC_SWITCH_APP_UI_CAPABILITIES, {
    sync: true,
    switchProvider: true,
    restore: false,
    pruneBackups: false,
    watch: false,
    manageProfiles: false,
    revealProfilePaths: false,
    forgetBrowser: false,
    exportDiagnostics: false,
    viewUpdateStatus: false
  });
  assert.equal(Object.isFrozen(SYNC_SWITCH_APP_UI_CAPABILITIES), true);
  assert.deepEqual(DESKTOP_C8_APP_UI_CAPABILITIES, {
    sync: true,
    switchProvider: true,
    restore: true,
    pruneBackups: true,
    watch: true,
    manageProfiles: false,
    revealProfilePaths: false,
    forgetBrowser: false,
    exportDiagnostics: true,
    viewUpdateStatus: true
  });
  assert.equal(Object.isFrozen(DESKTOP_C8_APP_UI_CAPABILITIES), true);
  const appContentSource = await fs.readFile(new URL("../src/app/AppContent.tsx", import.meta.url), "utf8");
  const settingsSource = await fs.readFile(new URL("../src/features/settings/SettingsPage.tsx", import.meta.url), "utf8");
  const typesSource = await fs.readFile(new URL("../src/types.ts", import.meta.url), "utf8");
  assert.match(appContentSource, /route === "sync" && capabilities\.sync/);
  assert.match(settingsSource, /enabled: capabilities\.watch/);
  assert.match(settingsSource, /recoveryBlocked \|\| writeBlocked/);
  assert.match(appContentSource, /applySubmissionPending\.current/);
  assert.match(appContentSource, /canRestore=\{capabilities\.restore\}/);
  assert.match(appContentSource, /canManage=\{capabilities\.manageProfiles\}/);
  assert.match(appContentSource, /capabilities\.exportDiagnostics/);
  assert.match(settingsSource, /capabilities\.viewUpdateStatus/);
  assert.match(settingsSource, /host\.checkForUpdates/);
  assert.match(settingsSource, /host\.downloadUpdate/);
  assert.match(settingsSource, /host\.installUpdate/);
  assert.match(appContentSource, /recoveryWriteDisabled/);
  assert.match(typesSource, /AppUiSurface = "desktop" \| "web"/);
  assert.match(appContentSource, /brand\.\$\{props\.surface\}/);
  assert.match(settingsSource, /settings\.subtitle\.\$\{props\.surface\}/);
});

test("shared UI translations and write forms keep one strict schema", () => {
  assert.equal(resourcesHaveMatchingKeys(), true);
  assert.equal(switchSchema.safeParse({ provider: "relay", modelMode: "explicit", model: "gpt", keepCount: 5 }).success, true);
  assert.equal(syncSchema.safeParse({ keepCount: 0 }).success, false);
  assert.equal(switchSchema.safeParse({ provider: "relay", modelMode: "provider-default", keepCount: 0 }).success, false);
  assert.equal(switchSchema.safeParse({ provider: "relay", modelMode: "explicit", model: "", keepCount: 5 }).success, false);
  assert.equal(restoreSchema.safeParse({ backupId: "managed", restoreConfig: false, restoreDatabase: false, restoreSessions: false, allowSqliteHomeRelocation: false }).success, false);
  assert.equal(profileSchema.safeParse({ profileId: "safe", name: "Safe", codexHome: "../relative", sqliteHome: "" }).success, false);
});

test("shared UI has no transport, Node, Electron or persistent history access", async () => {
  const sourceRoot = new URL("../src/", import.meta.url);
  const readTree = async (directory) => {
    const entries = await fs.readdir(directory, { withFileTypes: true });
    const chunks = [];
    for (const entry of entries) {
      const target = new URL(entry.name + (entry.isDirectory() ? "/" : ""), directory);
      if (entry.isDirectory()) chunks.push(...await readTree(target));
      else if (/\.tsx?$/.test(entry.name)) chunks.push(await fs.readFile(target, "utf8"));
    }
    return chunks;
  };
  const source = (await readTree(sourceRoot)).join("\n");
  const appSource = await fs.readFile(new URL("../src/app/AppContent.tsx", import.meta.url), "utf8");
  const historySource = await fs.readFile(new URL("../src/features/history/HistoryPage.tsx", import.meta.url), "utf8");
  assert.doesNotMatch(source, /\bfetch\s*\(/);
  assert.doesNotMatch(source, /localStorage|sessionStorage|from\s+["'](?:node:|electron)/);
  assert.doesNotMatch(source, /\/api\//);
  assert.match(historySource, /core\.getHistorySession/);
  assert.match(historySource, /messageLimit:\s*200/);
  assert.doesNotMatch(historySource, /queryKey:\s*\["history-detail"/);
  assert.match(appSource, /schemaVersion:\s*1 as const, planId: summary\.planId/);
  assert.match(appSource, /onOperationStarted/);
  assert.match(appSource, /onProgress/);
  assert.match(appSource, /applyController\.current\?\.abort\(\)/);
});
