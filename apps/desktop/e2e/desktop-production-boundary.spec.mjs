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
const PRODUCTION_SMOKE_TIMEOUT_MS = 150_000;
const PRODUCTION_OPERATION_TIMEOUT_MS = 30_000;
const PRODUCTION_READY_TIMEOUT_MS = 20_000;
const PRODUCTION_BRIDGE_TIMEOUT_MS = 30_000;
const PRODUCTION_CDP_TIMEOUT_MS = 60_000;
const PRODUCTION_CLOSE_TIMEOUT_MS = 10_000;

async function withDeadline(label, task, timeoutMs) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms.`)), timeoutMs);
    timer.unref?.();
  });
  try {
    return await Promise.race([task, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

async function waitForProductionReady(page) {
  await expect(page).toHaveURL("cps-app://app/index.html", {
    timeout: PRODUCTION_READY_TIMEOUT_MS
  });
  await page.waitForLoadState("load", { timeout: PRODUCTION_READY_TIMEOUT_MS });
  await expect(page.getByText("Codex Provider Sync", { exact: true })).toBeVisible({
    timeout: PRODUCTION_READY_TIMEOUT_MS
  });
  // The provider distribution is populated only after the Renderer has
  // completed its first real Core Status request.
  await expect(page.getByText("openai", { exact: true }).first()).toBeVisible({
    timeout: PRODUCTION_READY_TIMEOUT_MS
  });
}

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

async function forceStopPackagedChild(child) {
  if (child.exitCode !== null) return;
  child.kill("SIGTERM");
  if (await waitForExit(child, 5_000)) return;
  child.kill("SIGKILL");
  if (!await waitForExit(child, 5_000)) {
    throw new Error("Packaged desktop process could not be terminated after activation failure.");
  }
}

async function launchPackagedDesktop({ args, env }) {
  const child = spawn(packagedExecutable, [...args, "--remote-debugging-port=0"], {
    env,
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true
  });
  let launchOutput = "";
  let browser;
  let page;
  try {
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
    browser = await chromium.connectOverCDP(endpoint, {
      timeout: PRODUCTION_CDP_TIMEOUT_MS
    });
    for (let attempt = 0; attempt < 100 && !page; attempt += 1) {
      page = browser.contexts().flatMap((context) => context.pages())[0];
      if (!page) await new Promise((resolve) => setTimeout(resolve, 50));
    }
    if (!page) throw new Error("Packaged desktop did not create a renderer page.");
  } catch (activationError) {
    let cleanupError;
    try {
      if (browser) {
        await withDeadline("Failed packaged desktop CDP close", browser.close(), 5_000).catch(() => {});
      }
      await forceStopPackagedChild(child);
    } catch (error) {
      cleanupError = error;
    }
    if (cleanupError) {
      throw new AggregateError(
        [activationError, cleanupError],
        "Packaged desktop activation failed and its process could not be cleaned up."
      );
    }
    throw activationError;
  }
  return {
    async firstWindow() {
      return page;
    },
    async close() {
      let pageCloseError;
      try {
        if (!page.isClosed()) {
          await withDeadline(
            "Packaged desktop page close",
            page.close({ runBeforeUnload: true }),
            PRODUCTION_CLOSE_TIMEOUT_MS
          );
        }
      } catch (error) {
        // A graceful application shutdown may close CDP before Playwright receives acknowledgement.
        pageCloseError = error;
      }
      let exited = await waitForExit(child, process.platform === "darwin" ? 500 : 10_000);
      if (!exited && process.platform === "darwin") {
        child.kill("SIGTERM");
        exited = await waitForExit(child, 10_000);
      }
      await withDeadline(
        "Packaged desktop CDP close",
        browser.close(),
        5_000
      ).catch(() => {});
      if (!exited) {
        child.kill("SIGKILL");
        await waitForExit(child, 5_000);
        const detail = pageCloseError instanceof Error ? ` ${pageCloseError.message}` : "";
        throw new Error(`Packaged desktop did not complete a graceful shutdown.${detail}`);
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
  test.setTimeout(PRODUCTION_SMOKE_TIMEOUT_MS);
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
    await test.step("wait for the production UI to finish its first Status request", async () => {
      await waitForProductionReady(page);
    });
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

    const updateStatus = await test.step("read the production update status", () => withDeadline(
      "Production update status",
      page.evaluate(() => window.codexProvider.updates.getStatus()),
      PRODUCTION_BRIDGE_TIMEOUT_MS
    ));
    expect(updateStatus.schemaVersion).toBe(2);
    expect(updateStatus.installAllowed).toBe(false);
    expect(JSON.stringify(updateStatus)).not.toMatch(/url|path|releaseNotes|token/i);

    const profile = (await test.step("read the production profile list", () => withDeadline(
      "Production profile list",
      page.evaluate(() => window.codexProvider.profiles.list()),
      PRODUCTION_BRIDGE_TIMEOUT_MS
    ))).profiles[0];
    const status = await test.step("read production Core Status", () => withDeadline(
      "Production Core Status",
      page.evaluate(async ({ profile }) => window.codexProvider.core.requestReadOnly({
        protocolVersion: 1,
        requestId: "c6-production-status",
        method: "getStatus",
        payload: { profile: { profileId: profile.id, profileRevision: profile.revision } }
      }), { profile }),
      PRODUCTION_BRIDGE_TIMEOUT_MS
    ));
    expect(status.ok).toBe(true);
    expect(status.result.sqliteCounts.sessions.openai).toBe(1);
    expect(status.result.pendingRecovery).toBe(true);

    const denied = await test.step("reject a write over the read-only bridge", () => withDeadline(
      "Production read-only permission check",
      page.evaluate(async ({ profile }) => window.codexProvider.core.requestReadOnly({
        protocolVersion: 1,
        requestId: "c6-production-write-denied",
        method: "prepareSync",
        payload: { profile: { profileId: profile.id, profileRevision: profile.revision }, keepCount: 5 }
      }), { profile }),
      PRODUCTION_BRIDGE_TIMEOUT_MS
    ));
    expect(denied.ok).toBe(false);
    expect(denied.error.code).toBe("PERMISSION_DENIED");

    const recoveryBlocked = await test.step("block a real write while recovery is pending", () => withDeadline(
      "Production recovery write gate",
      page.evaluate(async ({ profile }) => window.codexProvider.core.requestSyncSwitch({
        protocolVersion: 1,
        requestId: "c7-production-recovery-blocked",
        method: "prepareSync",
        payload: { profile: { profileId: profile.id, profileRevision: profile.revision }, keepCount: 5 }
      }), { profile }),
      PRODUCTION_BRIDGE_TIMEOUT_MS
    ));
    expect(recoveryBlocked.ok).toBe(false);
    expect(recoveryBlocked.error.code).toBe("PENDING_TRANSACTION");

    await page.getByRole("button", { name: "History" }).click({
      timeout: PRODUCTION_READY_TIMEOUT_MS
    });
    await expect(page.getByText("Untitled session", { exact: true })).toBeVisible();
    await expect(page.locator("body")).not.toContainText("C6_DESKTOP_BODY_ONLY_MARKER");
  } finally {
    let closeError;
    try {
      await electronApp?.close();
    } catch (error) {
      closeError = error;
    }
    try {
      await fixture.assertUnchanged();
    } finally {
      await fixture.close();
    }
    if (closeError) throw closeError;
  }
});

test("production or unpacked desktop completes real Sync and Restore through Utility Core", async () => {
  test.setTimeout(PRODUCTION_SMOKE_TIMEOUT_MS);
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
    await test.step("wait for the production UI to finish its first Status request", async () => {
      await waitForProductionReady(page);
    });
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
    await expect(page.getByText("Operation completed.", { exact: true })).toBeVisible({
      timeout: PRODUCTION_OPERATION_TIMEOUT_MS
    });
    await page.getByRole("dialog", { name: "Operation result" }).getByRole("button", { name: "Close" }).last().click();
    await expect.poll(
      async () => (await fixture.inspect()).sqlite.provider,
      { timeout: PRODUCTION_OPERATION_TIMEOUT_MS }
    ).toBe("openai");
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
    await expect(page.getByText("Operation completed.", { exact: true }).last()).toBeVisible({
      timeout: PRODUCTION_OPERATION_TIMEOUT_MS
    });
    await expect.poll(
      async () => (await fixture.snapshotTargets()).hash,
      { timeout: PRODUCTION_OPERATION_TIMEOUT_MS }
    ).toBe(baseline.hash);

    const events = await page.evaluate(() => globalThis.__productionOperationEvents);
    expect(events.filter((event) => event.event === "operation-started")).toHaveLength(2);
    expect(JSON.stringify(events)).not.toMatch(/codex-home|state_5\.sqlite|backupDir|messageBody/i);
  } finally {
    let closeError;
    try {
      await electronApp?.close();
    } catch (error) {
      closeError = error;
    }
    await fixture.close();
    if (closeError) throw closeError;
  }
});
