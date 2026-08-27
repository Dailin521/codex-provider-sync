import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { _electron as electron, expect, test } from "@playwright/test";

import { createDesktopReadOnlyFixture } from "../../../test-support/desktop-readonly-fixture.mjs";
import { createDesktopSyncSwitchFixture } from "../../../test-support/desktop-sync-switch-fixture.mjs";

const require = createRequire(import.meta.url);
const desktopRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const packagedExecutable = process.env.CPS_DESKTOP_EXECUTABLE;
const electronExecutable = packagedExecutable || require("electron");

test("production desktop bundle has no test bridge and reads the real SQLite fixture", async () => {
  const fixture = await createDesktopReadOnlyFixture();
  let electronApp;
  try {
    electronApp = await electron.launch({
      executablePath: electronExecutable,
      args: [
        ...(packagedExecutable ? [] : [path.join(desktopRoot, "out", "main", "index.js")]),
        `--user-data-dir=${fixture.userData}`,
        "--lang=en-US"
      ],
      env: {
        ...process.env,
        CODEX_HOME: fixture.codexHome,
        CPS_DESKTOP_E2E: "1",
        CPS_DESKTOP_WINDOW_DISPLAY: "hidden",
        ELECTRON_ENABLE_SECURITY_WARNINGS: "true"
      }
    });
    const page = await electronApp.firstWindow();
    await expect(page).toHaveURL("cps-app://app/index.html");
    const boundary = await page.evaluate(() => ({
      bridgeKeys: Object.keys(window.codexProvider).sort(),
      coreKeys: Object.keys(window.codexProvider.core).sort(),
      updateKeys: Object.keys(window.codexProvider.updates).sort(),
      process: typeof globalThis.process,
      require: typeof globalThis.require
    }));
    expect(boundary).toEqual({
      bridgeKeys: ["core", "diagnostics", "profiles", "updates", "version"],
      coreKeys: [
        "cancelOperation",
        "requestMaintenance",
        "requestReadOnly",
        "requestRestore",
        "requestSyncSwitch",
        "subscribeOperation"
      ],
      updateKeys: ["check", "download", "getStatus", "install"],
      process: "undefined",
      require: "undefined"
    });

    const updateStatus = await page.evaluate(() => window.codexProvider.updates.getStatus());
    expect(updateStatus.schemaVersion).toBe(2);
    expect(updateStatus.installAllowed).toBe(false);
    expect(JSON.stringify(updateStatus)).not.toMatch(/url|path|releaseNotes|token/i);

    const profile = (await page.evaluate(() => window.codexProvider.profiles.list())).profiles[0];
    const status = await page.evaluate(async ({ profile }) => window.codexProvider.core.requestReadOnly({
      protocolVersion: 1,
      requestId: "c6-production-status",
      method: "getStatus",
      payload: { profile: { profileId: profile.id, profileRevision: profile.revision } }
    }), { profile });
    expect(status.ok).toBe(true);
    expect(status.result.sqliteCounts.sessions.openai).toBe(1);
    expect(status.result.pendingRecovery).toBe(true);

    const denied = await page.evaluate(async ({ profile }) => window.codexProvider.core.requestReadOnly({
      protocolVersion: 1,
      requestId: "c6-production-write-denied",
      method: "prepareSync",
      payload: { profile: { profileId: profile.id, profileRevision: profile.revision }, keepCount: 5 }
    }), { profile });
    expect(denied.ok).toBe(false);
    expect(denied.error.code).toBe("PERMISSION_DENIED");

    const recoveryBlocked = await page.evaluate(async ({ profile }) => window.codexProvider.core.requestSyncSwitch({
      protocolVersion: 1,
      requestId: "c7-production-recovery-blocked",
      method: "prepareSync",
      payload: { profile: { profileId: profile.id, profileRevision: profile.revision }, keepCount: 5 }
    }), { profile });
    expect(recoveryBlocked.ok).toBe(false);
    expect(recoveryBlocked.error.code).toBe("PENDING_TRANSACTION");

    await page.getByRole("button", { name: "History" }).click();
    await expect(page.getByText("Untitled session", { exact: true })).toBeVisible();
    await expect(page.locator("body")).not.toContainText("C6_DESKTOP_BODY_ONLY_MARKER");
  } finally {
    await electronApp?.close();
    await fixture.assertUnchanged();
    await fixture.close();
  }
});

test("production or unpacked desktop completes a real Sync through Utility Core", async () => {
  const fixture = await createDesktopSyncSwitchFixture();
  let electronApp;
  try {
    electronApp = await electron.launch({
      executablePath: electronExecutable,
      args: [
        ...(packagedExecutable ? [] : [path.join(desktopRoot, "out", "main", "index.js")]),
        `--user-data-dir=${fixture.userData}`,
        "--lang=en-US"
      ],
      env: {
        ...process.env,
        CODEX_HOME: fixture.codexHome,
        CPS_DESKTOP_E2E: "1",
        CPS_DESKTOP_WINDOW_DISPLAY: "hidden",
        ELECTRON_ENABLE_SECURITY_WARNINGS: "true"
      }
    });
    const page = await electronApp.firstWindow();
    await expect(page).toHaveURL("cps-app://app/index.html");
    await page.evaluate(() => {
      globalThis.__productionOperationEvents = [];
      globalThis.__productionUnsubscribe = window.codexProvider.core.subscribeOperation((event) => {
        globalThis.__productionOperationEvents.push(event);
      });
    });
    await page.getByRole("button", { name: "Sync" }).click();
    await page.getByRole("button", { name: "Prepare sync" }).click();
    const dialog = page.getByRole("dialog", { name: "Review plan" });
    await expect(dialog).toBeVisible();
    await dialog.getByRole("button", { name: "Confirm and apply" }).click();
    await expect(page.getByText("Operation completed.", { exact: true })).toBeVisible();
    await expect.poll(async () => (await fixture.inspect()).sqlite.provider).toBe("openai");
    const state = await fixture.inspect();
    expect(state.rollout.model_provider).toBe("openai");
    expect(state.backupIds).toHaveLength(1);
    const events = await page.evaluate(() => globalThis.__productionOperationEvents);
    expect(events.some((event) => event.event === "operation-started")).toBe(true);
    expect(JSON.stringify(events)).not.toMatch(/codex-home|state_5\.sqlite|backupDir|messageBody/i);
  } finally {
    await electronApp?.close();
    await fixture.close();
  }
});
