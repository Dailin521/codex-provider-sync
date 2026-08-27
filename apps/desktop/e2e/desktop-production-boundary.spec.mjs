import { createRequire } from "node:module";
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { _electron as electron, chromium, expect, test } from "@playwright/test";

import { createDesktopReadOnlyFixture } from "../../../test-support/desktop-readonly-fixture.mjs";
import { createDesktopSyncSwitchFixture } from "../../../test-support/desktop-sync-switch-fixture.mjs";

const require = createRequire(import.meta.url);
const desktopRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const packagedExecutable = process.env.CPS_DESKTOP_EXECUTABLE;
const electronExecutable = packagedExecutable || require("electron");

function waitForExit(child, timeoutMs) {
  if (child.exitCode !== null) return Promise.resolve(true);
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      child.off("exit", onExit);
      resolve(false);
    }, timeoutMs);
    const onExit = () => {
      clearTimeout(timer);
      resolve(true);
    };
    child.once("exit", onExit);
  });
}

async function launchPackagedDesktop({ args, env }) {
  const child = spawn(packagedExecutable, [...args, "--remote-debugging-port=0"], {
    env,
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true
  });
  let launchOutput = "";
  const endpoint = await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => finish(new Error(`Timed out waiting for packaged DevTools endpoint. ${launchOutput}`)), 15_000);
    const finish = (error, value) => {
      clearTimeout(timeout);
      child.stdout?.off("data", onData);
      child.stderr?.off("data", onData);
      child.off("error", onError);
      child.off("exit", onExit);
      if (error) reject(error);
      else resolve(value);
    };
    const onData = (chunk) => {
      launchOutput = `${launchOutput}${chunk}`.slice(-16_384);
      const match = launchOutput.match(/DevTools listening on (ws:\/\/[^\s]+)/);
      if (match) finish(null, match[1]);
    };
    const onError = (error) => finish(error);
    const onExit = (code) => finish(new Error(`Packaged desktop exited before CDP was ready (${code}). ${launchOutput}`));
    child.stdout?.on("data", onData);
    child.stderr?.on("data", onData);
    child.once("error", onError);
    child.once("exit", onExit);
  });
  const browser = await chromium.connectOverCDP(endpoint);
  let page;
  for (let attempt = 0; attempt < 100 && !page; attempt += 1) {
    page = browser.contexts().flatMap((context) => context.pages())[0];
    if (!page) await new Promise((resolve) => setTimeout(resolve, 50));
  }
  if (!page) throw new Error("Packaged desktop did not create a renderer page.");
  return {
    async firstWindow() {
      return page;
    },
    async close() {
      try {
        if (!page.isClosed()) await page.close({ runBeforeUnload: true });
      } catch {
        // A graceful application shutdown may close CDP before Playwright receives acknowledgement.
      }
      let exited = await waitForExit(child, process.platform === "darwin" ? 500 : 10_000);
      if (!exited && process.platform === "darwin") {
        child.kill("SIGTERM");
        exited = await waitForExit(child, 10_000);
      }
      await browser.close().catch(() => {});
      if (!exited) {
        child.kill("SIGKILL");
        await waitForExit(child, 5_000);
        throw new Error("Packaged desktop did not complete a graceful shutdown.");
      }
    }
  };
}

function launchProductionDesktop(options) {
  return packagedExecutable
    ? launchPackagedDesktop(options)
    : electron.launch({ executablePath: electronExecutable, ...options });
}

test("production desktop bundle has no test bridge and reads the real SQLite fixture", async () => {
  const fixture = await createDesktopReadOnlyFixture();
  let electronApp;
  try {
    electronApp = await launchProductionDesktop({
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

test("production or unpacked desktop completes real Sync and Restore through Utility Core", async () => {
  test.setTimeout(60_000);
  const fixture = await createDesktopSyncSwitchFixture();
  const baseline = await fixture.snapshotTargets();
  let electronApp;
  try {
    electronApp = await launchProductionDesktop({
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
    await page.getByRole("dialog", { name: "Operation result" }).getByRole("button", { name: "Close" }).last().click();
    await expect.poll(async () => (await fixture.inspect()).sqlite.provider).toBe("openai");
    const state = await fixture.inspect();
    expect(state.rollout.model_provider).toBe("openai");
    expect(state.backupIds).toHaveLength(1);
    const syncBackupId = state.backupIds[0];

    await page.getByRole("button", { name: "Backups / Restore" }).click();
    await page.getByRole("button", { name: new RegExp(syncBackupId) }).click();
    await page.getByRole("button", { name: "Prepare restore" }).click();
    const restoreDialog = page.getByRole("dialog", { name: "Review plan" });
    await expect(restoreDialog).toBeVisible();
    await restoreDialog.getByRole("button", { name: "Confirm and apply" }).click();
    await expect(page.getByText("Operation completed.", { exact: true }).last()).toBeVisible();
    await expect.poll(async () => (await fixture.snapshotTargets()).hash).toBe(baseline.hash);

    const events = await page.evaluate(() => globalThis.__productionOperationEvents);
    expect(events.filter((event) => event.event === "operation-started")).toHaveLength(2);
    expect(JSON.stringify(events)).not.toMatch(/codex-home|state_5\.sqlite|backupDir|messageBody/i);
  } finally {
    await electronApp?.close();
    await fixture.close();
  }
});
