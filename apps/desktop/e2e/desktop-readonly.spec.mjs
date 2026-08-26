import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { _electron as electron, expect, test } from "@playwright/test";

import { createDesktopReadOnlyFixture } from "../../../test-support/desktop-readonly-fixture.mjs";

const require = createRequire(import.meta.url);
const desktopRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const packagedExecutable = process.env.CPS_DESKTOP_EXECUTABLE;
const electronExecutable = packagedExecutable || require("electron");

test("secure desktop exposes C7 Sync/Switch narrowly and blocks writes during recovery", async () => {
  const fixture = await createDesktopReadOnlyFixture();
  let electronApp;
  try {
    electronApp = await electron.launch({
      executablePath: electronExecutable,
      args: packagedExecutable
        ? ["--lang=en-US"]
        : [path.join(desktopRoot, "out", "main", "index.js"), "--lang=en-US"],
      env: {
        ...process.env,
        CPS_DESKTOP_E2E: "1",
        CPS_DESKTOP_CODEX_HOME: fixture.codexHome,
        CPS_DESKTOP_USER_DATA: fixture.userData,
        CPS_DESKTOP_WINDOW_DISPLAY: "hidden",
        ELECTRON_ENABLE_SECURITY_WARNINGS: "true"
      }
    });
    const page = await electronApp.firstWindow();
    await expect(page).toHaveURL("cps-app://app/index.html");
    await expect(page.getByText("Codex Provider Sync", { exact: true })).toBeVisible();
    await expect(page.getByText("openai", { exact: true }).first()).toBeVisible();

    const preferences = await electronApp.evaluate(({ BrowserWindow }) => {
      const window = BrowserWindow.getAllWindows()[0];
      return window.webContents.getLastWebPreferences();
    });
    expect(preferences.nodeIntegration).toBe(false);
    expect(preferences.nodeIntegrationInWorker).toBe(false);
    expect(preferences.contextIsolation).toBe(true);
    expect(preferences.sandbox).toBe(true);
    expect(preferences.webSecurity).toBe(true);
    expect(preferences.allowRunningInsecureContent).toBe(false);
    expect(preferences.experimentalFeatures).toBe(false);
    expect(preferences.webviewTag).toBe(false);

    const rendererBoundary = await page.evaluate(() => ({
      process: typeof globalThis.process,
      require: typeof globalThis.require,
      buffer: typeof globalThis.Buffer,
      bridgeKeys: Object.keys(window.codexProvider).sort(),
      coreKeys: Object.keys(window.codexProvider.core).sort(),
      frozen: Object.isFrozen(window.codexProvider) && Object.isFrozen(window.codexProvider.core),
      csp: document.querySelector('meta[http-equiv="Content-Security-Policy"]')?.getAttribute("content")
    }));
    expect(rendererBoundary).toMatchObject({
      process: "undefined",
      require: "undefined",
      buffer: "undefined",
      bridgeKeys: ["core", "profiles", "test", "version"],
      coreKeys: ["cancelOperation", "requestReadOnly", "requestSyncSwitch", "subscribeOperation"],
      frozen: true
    });
    expect(rendererBoundary.csp).toContain("script-src 'self'");
    expect(rendererBoundary.csp).not.toContain("unsafe-inline");
    expect(rendererBoundary.csp).not.toContain("unsafe-eval");

    const navigation = page.getByRole("navigation").getByRole("button");
    await expect(navigation).toHaveCount(8);
    await expect(page.getByRole("button", { name: "Sync" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Switch Provider" })).toBeVisible();

    await page.getByRole("button", { name: "Backups / Restore" }).click();
    await expect(page.getByText(/read-only/i)).toBeVisible();
    await expect(page.getByRole("main").getByRole("button", { name: /restore/i })).toHaveCount(0);
    await expect(page.getByRole("main").getByRole("button", { name: /prune/i })).toHaveCount(0);

    await page.getByRole("button", { name: "Profiles" }).click();
    await expect(page.getByText(/profile IDs and revisions only/i)).toBeVisible();
    await expect(page.getByText(fixture.codexHome)).toHaveCount(0);

    await page.getByRole("button", { name: "Diagnostics" }).click();
    await expect(page.getByText(/runtime/i).first()).toBeVisible();
    await expect(page.locator("body")).not.toContainText(fixture.codexHome);

    await page.getByRole("button", { name: "History" }).click();
    await expect(page.locator("body")).not.toContainText("C6_DESKTOP_BODY_ONLY_MARKER");
    await expect(page.getByText("Untitled session", { exact: true })).toBeVisible();
    await page.getByRole("button", { name: "Open session" }).click();
    await expect(page.getByText("C6_DESKTOP_BODY_ONLY_MARKER")).toBeVisible();
    await page.getByRole("button", { name: "Back to sessions" }).click();
    await expect(page.locator("body")).not.toContainText("C6_DESKTOP_BODY_ONLY_MARKER");

    const profile = (await page.evaluate(() => window.codexProvider.profiles.list())).profiles[0];
    const statusBeforeCrash = await page.evaluate(async ({ profile }) => window.codexProvider.test.requestRaw({
      protocolVersion: 1,
      requestId: "c6-real-sqlite-status",
      method: "getStatus",
      payload: { profile: { profileId: profile.id, profileRevision: profile.revision } }
    }), { profile });
    expect(statusBeforeCrash.ok).toBe(true);
    expect(statusBeforeCrash.result.sqliteCounts.sessions.openai).toBe(1);
    expect(statusBeforeCrash.result.pendingRecovery).toBe(true);
    const writeAttempt = await page.evaluate(async ({ profile }) => window.codexProvider.test.requestRaw({
      protocolVersion: 1,
      requestId: "c6-write-denied",
      method: "prepareSync",
      payload: { profile: { profileId: profile.id, profileRevision: profile.revision }, keepCount: 5 }
    }), { profile });
    expect(writeAttempt.ok).toBe(false);
    expect(writeAttempt.error.code).toBe("PERMISSION_DENIED");
    const recoveryBlocked = await page.evaluate(async ({ profile }) => window.codexProvider.core.requestSyncSwitch({
      protocolVersion: 1,
      requestId: "c7-write-recovery-blocked",
      method: "prepareSync",
      payload: { profile: { profileId: profile.id, profileRevision: profile.revision }, keepCount: 5 }
    }), { profile });
    expect(recoveryBlocked.ok).toBe(false);
    expect(recoveryBlocked.error.code).toBe("PENDING_TRANSACTION");

    const beforeCrash = await electronApp.evaluate(() => globalThis.__CPS_DESKTOP_TEST__.runtime());
    expect(beforeCrash.state).toBe("ready");
    expect(beforeCrash.lastHandshakeAt).not.toBeNull();
    expect((await page.evaluate(() => window.codexProvider.test.crashRuntime())).crashed).toBe(true);
    await expect.poll(() => electronApp.evaluate(() => globalThis.__CPS_DESKTOP_TEST__.runtime().state)).toBe("crashed");
    const afterRestart = await page.evaluate(async ({ profile }) => window.codexProvider.test.requestRaw({
      protocolVersion: 1,
      requestId: "c6-restart-status",
      method: "getStatus",
      payload: { profile: { profileId: profile.id, profileRevision: profile.revision } }
    }), { profile });
    expect(afterRestart.ok).toBe(true);
    const restarted = await electronApp.evaluate(() => globalThis.__CPS_DESKTOP_TEST__.runtime());
    expect(restarted.state).toBe("ready");
    expect(restarted.generation).toBe(beforeCrash.generation + 1);
    expect(restarted.recoveryBlocked).toBe(true);

    const originalUrl = page.url();
    await page.evaluate(() => { globalThis.location.href = "https://example.com/"; });
    await page.waitForTimeout(250);
    expect(page.url()).toBe(originalUrl);
    expect(await page.evaluate(() => globalThis.open("https://example.com/"))).toBeNull();
  } finally {
    await electronApp?.close();
    await fixture.assertUnchanged();
    await fixture.close();
  }
});
