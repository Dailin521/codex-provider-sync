import { createRequire } from "node:module";
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { _electron as electron, chromium, expect, test } from "@playwright/test";

import { createDesktopReadOnlyFixture } from "../../../test-support/desktop-readonly-fixture.mjs";
import { createDesktopSyncSwitchFixture } from "../../../test-support/desktop-sync-switch-fixture.mjs";
import { shouldRetryPackagedCdpActivation } from "./packaged-cdp-retry.mjs";
import { captureViewport } from "./viewport-screenshot.mjs";
import { claimDailyUpdateCheck } from "../dist/main/daily-update-check.js";
import { OperationLogService } from "../dist/main/operation-log-service.js";

const require = createRequire(import.meta.url);
const desktopRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const packagedExecutable = process.env.CPS_DESKTOP_EXECUTABLE;
const electronExecutable = packagedExecutable || require("electron");
const PRODUCTION_SMOKE_TIMEOUT_MS = 180_000;
const PRODUCTION_OPERATION_TIMEOUT_MS = 30_000;
const PRODUCTION_READY_TIMEOUT_MS = 20_000;
const PRODUCTION_BRIDGE_TIMEOUT_MS = 30_000;
const PRODUCTION_CDP_TIMEOUT_MS = 20_000;
const PRODUCTION_CLOSE_TIMEOUT_MS = 10_000;

async function seedLayoutLogs(userData) {
  const logs = new OperationLogService({ directory: path.join(userData, "logs", "operations") });
  await logs.initialize();
  for (let index = 0; index < 35; index++) {
    const id = await logs.begin({ operation: "sync", profileId: "default", requestId: `fixture-prepare-${index}` });
    await logs.prepared(id, `fixture-plan-${index}`, { target: { provider: "openai" }, impact: { rolloutFilesToChange: 100, sqliteRowsToChange: 100, lockedRolloutFiles: 0 } });
    await logs.resume(id, `fixture-apply-${index}`);
    await logs.requestProgress(id, { stage: "rewrite_rollout_files", status: "start" });
    await logs.requestProgress(id, { stage: "rewrite_rollout_files", status: "complete", count: 100 });
    await logs.finish(id, { status: "completed", outcome: "completed", counts: { changedSessionFiles: 100, rewrittenSessionFiles: 100, inPlaceSessionFiles: 0, sqliteRowsUpdated: 100 } });
  }
}

async function verifyLogPanes(page) {
  await page.getByRole("button", { name: "Operation logs", exact: true }).click();
  const list = page.getByRole("region", { name: "Operation list" });
  const detail = page.getByRole("region", { name: "Operation details" });
  // Only completed synthetic Sync records exercise this presentation-only scenario.
  const syncRows = list.getByRole("button").filter({ has: page.getByText("Sync", { exact: true }) }).filter({ has: page.getByText("Completed", { exact: true }) });
  await expect(syncRows).toHaveCount(35);
  for (const width of [1366, 1024]) {
    await page.setViewportSize({ width, height: 768 });
    await syncRows.first().click();
    await expect(detail.getByText("View speed-up tips", { exact: true })).toBeVisible();
    const [left, right] = await Promise.all([list.boundingBox(), detail.boundingBox()]);
    expect(left.x + left.width).toBeLessThan(right.x);
    expect(Math.abs(left.y - right.y)).toBeLessThan(70); // The detail header stays outside its scroller.
    await expect(page.getByRole("button", { name: "Next", exact: true })).toBeInViewport({ ratio: 1 });
    expect(await page.evaluate(() => document.documentElement.scrollHeight <= innerHeight + 1)).toBe(true);
    await list.evaluate((element) => { element.scrollTop = 200; });
    await detail.evaluate((element) => { element.scrollTop = 250; });
    expect(await list.evaluate((element) => element.scrollTop)).toBe(200);
    expect(await detail.evaluate((element) => element.scrollTop)).toBeGreaterThan(0);
    await detail.hover();
    await page.mouse.wheel(0, 800);
    expect(await list.evaluate((element) => element.scrollTop)).toBe(200);
    expect(await page.evaluate(() => window.scrollY)).toBe(0);
    await list.evaluate((element) => { element.scrollTop = 0; });
    await syncRows.nth(1).click();
    await expect.poll(() => detail.evaluate((element) => element.scrollTop)).toBe(0);
    await captureViewport(page, { path: test.info().outputPath(`operation-logs-split-${width}.png`) });
  }
  for (const viewport of [{ width: 683, height: 384 }, { width: 390, height: 700 }]) {
    await page.setViewportSize(viewport);
    await expect(list).toBeHidden();
    await expect(detail).toBeVisible();
    await expect(page.getByRole("button", { name: "Back to operations" })).toBeInViewport({ ratio: 1 });
    await expect(page.getByRole("button", { name: "Refresh operation details" })).toBeInViewport({ ratio: 1 });
    expect((await detail.boundingBox()).height).toBeGreaterThan(140);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth && document.documentElement.scrollHeight <= innerHeight + 1)).toBe(true);
    await page.getByRole("button", { name: "Back to operations" }).click();
    await expect(list).toBeVisible();
    await expect(detail).toBeHidden();
    await syncRows.first().click();
    await expect(detail).toBeFocused();
    await captureViewport(page, { path: test.info().outputPath(`operation-logs-narrow-${viewport.width}.png`) });
  }
  await page.setViewportSize({ width: 1366, height: 768 });
}

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
  let firstRetryableFailure;
  const maxAttempts = process.platform === "win32" ? 2 : 1;

  for (let activationAttempt = 1; activationAttempt <= maxAttempts; activationAttempt += 1) {
    const child = spawn(packagedExecutable, [...args, "--remote-debugging-port=0"], {
      env,
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true
    });
    let launchOutput = "";
    let endpointReady = false;
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
      endpointReady = true;
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

      if (shouldRetryPackagedCdpActivation({
        platform: process.platform,
        attempt: activationAttempt,
        endpointReady,
        browserConnected: Boolean(browser),
        cleanupCompleted: true,
        error: activationError
      })) {
        firstRetryableFailure = { error: activationError, launchOutput };
        process.stderr.write("Packaged desktop CDP handshake timed out; retrying once with a fresh process.\n");
        continue;
      }

      if (firstRetryableFailure) {
        throw new AggregateError(
          [firstRetryableFailure.error, activationError],
          `Packaged desktop activation failed after one bounded CDP retry. `
            + `First output: ${firstRetryableFailure.launchOutput || "(none)"} `
            + `Final output: ${launchOutput || "(none)"}`
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

  throw new Error("Packaged desktop activation exhausted its bounded attempts.");
}

function launchProductionDesktop(options) {
  return packagedExecutable
    ? launchPackagedDesktop(options)
    : electron.launch({ executablePath: electronExecutable, ...options });
}

test("production desktop bundle has no test bridge and reads the real SQLite fixture", async () => {
  test.setTimeout(PRODUCTION_SMOKE_TIMEOUT_MS);
  const fixture = await createDesktopReadOnlyFixture({ includeUntitled: true, repairPreview: true, currentProvider: "dal",
    writerSessionIds: process.platform === "win32" ? ["11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"] : [] });
  await claimDailyUpdateCheck(fixture.userData); // No real network checks in fixture tests.
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
    await page.setViewportSize({ width: 1366, height: 768 });
    await page.evaluate(() => window.scrollTo(0, 0));
    const overviewCells = page.getByTestId("overview-storage-sync").locator(":scope > div");
    const [storageBox, syncBox] = await Promise.all([overviewCells.nth(0).boundingBox(), overviewCells.nth(1).boundingBox()]);
    expect(Math.abs(storageBox.y - syncBox.y)).toBeLessThan(2);
    expect(storageBox.x + storageBox.width).toBeLessThan(syncBox.x);
    await expect(page.getByRole("button", { name: "Preview sync", exact: true })).toBeInViewport({ ratio: 1 });
    await expect(page.getByRole("button", { name: "Sync now", exact: true })).toBeInViewport({ ratio: 1 });
    await captureViewport(page, { path: test.info().outputPath("overview-first-screen-1366.png") });
    await expect(page.getByRole("heading", { name: "Switch Provider separately" })).toBeVisible();
    const usageCard = page.getByText("Sessions currently in use", { exact: true }).locator("..");
    await expect(usageCard.getByText(process.platform === "win32" ? "2" : "Unknown", { exact: true })).toBeVisible();
    await expect(usageCard.locator("p")).toHaveCount(0);
    await expect(page.getByLabel("Provider ID", { exact: true })).toHaveValue("dal");
    await expect(page.locator("#configured-providers option[value=dal]")).toHaveCount(1);
    await expect(page.getByText(fixture.codexHome, { exact: true })).toBeVisible();
    await expect(page.getByText(path.join(fixture.codexHome, "sqlite"), { exact: true })).toBeVisible();
    await expect(page.getByText(path.join(fixture.codexHome, "sqlite", "state_5.sqlite"), { exact: true })).toBeVisible();
    const boundary = await page.evaluate(() => ({
      bridgeKeys: Object.keys(window.codexProvider).sort(),
      coreKeys: Object.keys(window.codexProvider.core).sort(),
      updateKeys: Object.keys(window.codexProvider.updates).sort(),
      watchKeys: Object.keys(window.codexProvider.watch).sort(),
      process: typeof globalThis.process,
      require: typeof globalThis.require
    }));
    expect(boundary).toEqual({
      bridgeKeys: ["clipboard", "core", "diagnostics", "history", "operationLogs", "profiles", "updates", "version", "watch"],
      coreKeys: [
        "cancelOperation",
        "requestMaintenance",
        "requestReadOnly",
        "requestRestore",
        "requestSyncSwitch",
        "subscribeOperation"
      ],
      updateKeys: ["check", "download", "getStatus", "install", "subscribe"],
      watchKeys: ["subscribeStopped"],
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
    expect(updateStatus.currentVersion).toBeTruthy();
    if (packagedExecutable) {
      expect(updateStatus.mode).toBe("manual");
      expect(updateStatus.state).toBe("idle");
      await page.getByRole("button", { name: "Settings", exact: true }).click();
      await expect(page.getByRole("button", { name: "Check for updates" })).toBeVisible();
      await expect(page.getByText(/Current version:/)).toBeVisible();
      await expect(page.getByText(/This portable or local build opens/)).toBeVisible();
      // No network check or download is launched by opening Settings.
      expect((await page.evaluate(() => window.codexProvider.updates.getStatus())).state).toBe("idle");
      await page.getByRole("button", { name: "Overview", exact: true }).click();
    }
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
    expect(status.result.currentProvider).toBe("dal");
    expect(status.result.sqliteCounts.sessions.openai).toBe(1);
    expect(status.result.pendingRecovery).toBe(false);
    expect(status.result.pendingTransactions).toHaveLength(1);

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

    const ordinaryPlan = await test.step("allow a real write plan beside a legacy ordinary journal", () => withDeadline(
      "Production legacy-journal write gate",
      page.evaluate(async ({ profile }) => window.codexProvider.core.requestSyncSwitch({
        protocolVersion: 1,
        requestId: "c7-production-legacy-journal-plan",
        method: "prepareSync",
        payload: { profile: { profileId: profile.id, profileRevision: profile.revision }, keepCount: 5 }
      }), { profile }),
      PRODUCTION_BRIDGE_TIMEOUT_MS
    ));
    expect(ordinaryPlan.ok).toBe(true);
    expect(ordinaryPlan.result.operation).toBe("sync");
    expect(ordinaryPlan.result.impact.sessionActivity).toEqual(process.platform === "win32"
      ? { state: "checked", count: 2 } : { state: "unsupported", count: null });
    expect(ordinaryPlan.result.impact.lockedRolloutFiles).toBe(0);
    await page.getByRole("button", { name: "Preview sync", exact: true }).click();
    const preview = page.getByRole("dialog", { name: "Confirm sync", exact: true });
    await expect(preview.getByText("Sessions currently in use", { exact: true })).toBeVisible();
    await expect(preview.getByText("Sessions to skip this time", { exact: true })).toBeVisible();
    const activityRow = preview.getByText("Sessions currently in use", { exact: true }).locator("..");
    await expect(activityRow.getByText(process.platform === "win32" ? "2" : "Unknown", { exact: true })).toBeVisible();
    await preview.getByRole("button", { name: "Close", exact: true }).last().click();
    if (process.platform === "win32") {
      await fixture.stopWriter();
      // Releasing the writer does not push or poll a new snapshot.
      await expect(usageCard.getByText("2", { exact: true })).toBeVisible();
      await page.getByRole("button", { name: "Refresh", exact: true }).click();
      await expect(usageCard.getByText("0", { exact: true })).toBeVisible();
    }

    await page.getByRole("button", { name: "History" }).click({
      timeout: PRODUCTION_READY_TIMEOUT_MS
    });
    await expect(page.getByText("Saved desktop chat", { exact: true })).toBeVisible();
    const subtask = page.getByRole("button", { name: /View chat: Subtask · fixture_worker/ });
    await expect(subtask).toHaveCount(0);
    await page.getByRole("button", { name: "Subtasks of Saved desktop chat (1)", exact: true }).click();
    await expect(subtask).toBeVisible();
    await page.getByRole("region", { name: "Other chats", exact: true }).getByRole("button", { expanded: false }).click();
    await expect(page.getByRole("button", { name: /View chat: Untitled chat.*2026.*abcdef12/ })).toBeVisible();
    await subtask.click();
    await expect(page.getByRole("heading", { name: "Subtask · fixture_worker" })).toBeVisible();
    await expect(page.locator("body")).not.toContainText("C6_DESKTOP_BODY_ONLY_MARKER");
    await page.getByText("Search options and filters", { exact: true }).click();
    await expect(page.getByRole("combobox", { name: "Search scope" })).toHaveValue("metadata");
    await expect(page.getByRole("combobox", { name: "Session type" })).toHaveValue("all");
    await subtask.click({ button: "right" });
    await page.getByRole("menuitem", { name: "Session information", exact: true }).click();
    const actions = page.getByRole("region", { name: "Session actions" });
    await expect(actions.getByRole("button", { name: "Copy session ID", exact: true })).toBeEnabled();
    await expect(actions.getByText("fixture-subtask-11112222", { exact: true })).toBeVisible();
    await expect(actions.getByRole("button", { name: "Copy file path" })).toBeEnabled();
    await actions.getByRole("button", { name: "View parent session" }).click();
    await expect(page.getByRole("heading", { name: "Saved desktop chat" })).toBeVisible();
    await expect(page.locator("body")).toContainText("C6_DESKTOP_BODY_ONLY_MARKER");
    await page.getByRole("combobox", { name: "Session type" }).selectOption("main");
    await expect(subtask).toHaveCount(0);
    await page.getByRole("region", { name: "Other chats", exact: true }).getByRole("button", { expanded: false }).click();
    await page.getByRole("button", { name: /View chat: Untitled chat.*2026.*abcdef12/ }).click();
    await page.getByRole("button", { name: /View chat: Untitled chat.*2026.*abcdef12/ }).click({ button: "right" });
    await expect(page.getByRole("menuitem", { name: "Copy resume command" })).toBeEnabled();
    await page.keyboard.press("Escape");
    await page.getByRole("button", { name: "Advanced features", exact: true }).click();
    await expect(page.getByRole("heading", { name: "Advanced features" })).toBeVisible();
    await expect(page.getByText("Diagnostics have not been run", { exact: true })).toBeVisible();
    await expect(page.getByRole("button", { name: "Preview repair" })).toBeHidden();
    await page.getByText("Targeted repair", { exact: true }).click();
    await expect(page.getByRole("button", { name: "Preview repair" })).toBeVisible();
    await expect(page.getByRole("checkbox")).toHaveCount(3);
    await expect(page.getByRole("checkbox", { name: "Unify historical model names" })).toBeHidden();
    await page.getByText("Advanced adjustments", { exact: true }).click();
    await expect(page.getByRole("checkbox", { name: "Unify historical model names" })).toBeVisible();
    await expect(page.getByText(/Optional changes, not fault repairs/)).toBeVisible();
    const repairControls = page.getByText("Targeted repair", { exact: true }).locator("..").locator("..").locator("..");
    await repairControls.scrollIntoViewIfNeeded();
    await captureViewport(page, { path: test.info().outputPath("repair-controls-en.png"), style: "header { visibility: hidden !important; }" });
    await page.evaluate(() => { document.documentElement.style.zoom = "2"; });
    const repairLayout = await page.evaluate(() => ({ width: document.documentElement.clientWidth, scrollWidth: document.documentElement.scrollWidth }));
    expect(repairLayout.scrollWidth).toBeLessThanOrEqual(repairLayout.width);
    await expect(page.getByRole("button", { name: "Preview adjustment", exact: true })).toBeVisible();
    await captureViewport(page, { path: test.info().outputPath("repair-controls-200-percent.png"), style: "header { visibility: hidden !important; }" });
    await page.evaluate(() => { document.documentElement.style.zoom = ""; });
    for (const checkbox of await page.getByRole("checkbox").all()) await expect(checkbox).not.toBeChecked();
    await expect(page.getByText(/These repairs do not fix session record numbering/)).toBeVisible();
    await page.evaluate(() => {
      window.__requestProgressFrames = [];
      window.__stopRequestProgress = window.codexProvider.core.subscribeOperation((event) => {
        if (event.event === "request-progress") window.__requestProgressFrames.push(event);
      });
    });
    await page.getByRole("button", { name: "Start diagnostics" }).click();
    await expect(page.getByText(/Diagnostics completed ·/)).toBeVisible();
    const scanFrames = await page.evaluate(() => { window.__stopRequestProgress(); return window.__requestProgressFrames; });
    expect(scanFrames.length).toBeGreaterThan(5);
    expect(scanFrames.some((event) => event.progress.stage === "scan_sessions" && event.progress.count >= 1)).toBe(true);
    expect(scanFrames.some((event) => event.progress.stage === "integrity_sessions")).toBe(true);
    for (const stage of ["scan_sessions", "scan_archived_sessions"]) {
      expect(scanFrames.filter((event) => event.progress.stage === stage && event.progress.status === "completed")).toHaveLength(1);
    }
    expect(scanFrames.every((event) => !("operationId" in event))).toBe(true);
    await expect(page.getByTestId("request-progress")).toHaveCount(0);
    await expect(page.getByRole("heading", { name: "Check results", exact: true })).toBeVisible();
    await page.getByRole("button", { name: "Overview", exact: true }).click();
    await page.getByRole("button", { name: "Advanced features", exact: true }).click();
    await expect(page.getByText(/Diagnostics completed ·/)).toBeVisible();
    await page.getByText("Targeted repair", { exact: true }).click();
    await page.locator('input[data-repair-target="workspaceRoots"]').check();
    await page.locator('input[data-repair-target="userEvent"]').check();
    await page.getByRole("button", { name: "Preview repair", exact: true }).click();
    const previewDialog = page.getByRole("dialog", { name: "Confirm repair" });
    await expect(previewDialog).toBeVisible();
    await expect(previewDialog.getByRole("checkbox")).toHaveCount(0);
    await expect(previewDialog.getByRole("radio")).toHaveCount(0);
    await expect(previewDialog.getByRole("region", { name: "Affected chat preview" }).getByText("c6-desktop-session", { exact: true })).toBeVisible();
    await expect(previewDialog.getByText("Affected chats (unique)", { exact: true }).locator("..")).toContainText("1");
    await expect(previewDialog.getByText("Index field changes (total)", { exact: true }).locator("..")).toContainText("1");
    await expect(previewDialog.getByText("Create the missing settings backup", { exact: true })).toBeVisible();
    await captureViewport(page, { path: test.info().outputPath("repair-global-preview.png") });
    await previewDialog.getByText("Create the missing settings backup", { exact: true }).scrollIntoViewIfNeeded();
    await captureViewport(page, { path: test.info().outputPath("repair-count-breakdown.png") });
    // Browser 200% zoom halves the CSS viewport. Root style.zoom leaves vw and
    // media queries unchanged and is not equivalent for a fixed-position dialog.
    await page.setViewportSize({ width: 683, height: 384 });
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
    await previewDialog.getByRole("button", { name: "Confirm repair", exact: true }).scrollIntoViewIfNeeded();
    await expect(previewDialog.getByRole("button", { name: "Confirm repair", exact: true })).toBeInViewport({ ratio: 1 });
    // Capture the verified viewport directly: an element screenshot may try
    // to resize the hidden Electron surface after CDP viewport emulation.
    await captureViewport(page, { path: test.info().outputPath("repair-preview-200-percent-equivalent.png") });
    await page.setViewportSize({ width: 1366, height: 768 });
    await previewDialog.getByRole("button", { name: "Close", exact: true }).last().click();
  } finally {
    let closeError;
    try {
      await electronApp?.close();
    } catch (error) {
      closeError = error;
    }
    try {
      await fixture.stopWriter();
      await fixture.assertUnchanged();
    } finally {
      await fixture.close();
    }
    if (closeError) throw closeError;
  }
});

test("production operation logs keep independent panes and accessible narrow details", async () => {
  test.setTimeout(PRODUCTION_SMOKE_TIMEOUT_MS);
  const fixture = await createDesktopReadOnlyFixture();
  await claimDailyUpdateCheck(fixture.userData);
  await seedLayoutLogs(fixture.userData); // Synthetic UI records only; no Core writes.
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
    await waitForProductionReady(page);
    await page.setViewportSize({ width: 1366, height: 768 });
    const performanceHelp = page.getByText("How to speed up sync", { exact: true });
    await expect(performanceHelp.locator("..")).not.toHaveAttribute("open");
    await performanceHelp.click();
    await expect(performanceHelp.locator("..")).toContainText("openai and prov_a");
    await performanceHelp.click();
    await verifyLogPanes(page);
  } finally {
    try {
      await electronApp?.close();
    } finally {
      try { await fixture.assertUnchanged(); }
      finally { await fixture.close(); }
    }
  }
});

test("production Provider switch records model history and refreshes recent choices without another write", async () => {
  test.setTimeout(PRODUCTION_SMOKE_TIMEOUT_MS);
  const fixture = await createDesktopSyncSwitchFixture();
  await claimDailyUpdateCheck(fixture.userData);
  const baseline = await fixture.inspect();
  let electronApp;
  try {
    electronApp = await launchProductionDesktop({
      args: [...(packagedExecutable ? [] : [path.join(desktopRoot, "out", "main", "index.js")]), `--user-data-dir=${fixture.userData}`, "--lang=en-US"],
      env: { ...process.env, CODEX_HOME: fixture.codexHome, CPS_DESKTOP_E2E: "1", CPS_DESKTOP_WINDOW_DISPLAY: "hidden", ELECTRON_ENABLE_SECURITY_WARNINGS: "true" }
    });
    const page = await electronApp.firstWindow();
    await waitForProductionReady(page);
    await page.getByLabel("Provider ID", { exact: true }).fill("relay");
    await page.getByRole("button", { name: "Preview switch", exact: true }).click();
    const confirm = page.getByRole("dialog", { name: "Confirm Provider switch" });
    await expect(confirm).toBeVisible();
    await confirm.getByRole("button", { name: "Confirm switch", exact: true }).click();
    const result = page.getByRole("dialog", { name: "Operation result" });
    await expect(result.getByRole("heading", { name: "Completed", exact: true })).toBeVisible({ timeout: PRODUCTION_OPERATION_TIMEOUT_MS });
    await result.getByRole("button", { name: "Close", exact: true }).last().click();
    // No manual refresh: successful Apply must update the current Profile's recent list.
    const recent = page.getByRole("button", { name: "relay", exact: true });
    await expect(recent).toBeVisible();
    const state = await fixture.inspect();
    expect(state.configText).toContain('model_provider = "relay"');
    expect(state.configText).toContain('model = "relay-model"');
    expect(state.rollout.model_provider).toBe("relay");
    expect(state.sqlite.provider).toBe("relay");
    expect(state.sqlite.model).toBe(baseline.sqlite.model);
    expect(state.sqlite.updatedAt).toBe(baseline.sqlite.updatedAt);
    expect(state.turnContext).toEqual(baseline.turnContext);
    await page.getByLabel("Provider ID", { exact: true }).fill("openai");
    await recent.click();
    await expect(page.getByLabel("Provider ID", { exact: true })).toHaveValue("relay");
    await expect(confirm).toBeHidden();
    const logs = await page.evaluate(() => window.codexProvider.operationLogs.list({ schemaVersion: 1, page: 1, pageSize: 100, operation: "switch" }));
    expect(logs.entries).toHaveLength(1);
    expect(logs.entries[0].status).toBe("completed");
    if (process.platform === "win32") {
      const timing = logs.entries[0].fileUpdateTiming;
      expect(timing.scope).toBe("windows-first-line");
      expect(timing.measuredFiles).toBe(timing.attemptedFiles);
      expect(timing.rewrittenFiles).toBeGreaterThan(0);
      for (const key of ["copyTailMs", "flushMs", "replaceMs", "cleanupMs", "restoreMtimeMs"]) expect(timing[key]).toBeGreaterThanOrEqual(0);
    }
    expect(logs.entries[0].switchPlan).toEqual({ previousProvider: "openai", targetProvider: "relay", previousRootModel: "gpt-5", targetRootModel: "relay-model", modelMode: "provider-default" });
    await page.getByRole("button", { name: "Operation logs", exact: true }).click();
    await page.getByRole("region", { name: "Operation list" }).getByRole("button").filter({ has: page.getByText("Switch Provider", { exact: true }) }).click();
    const detail = page.getByRole("region", { name: "Operation details" });
    await expect(detail.getByText("openai → relay", { exact: true })).toBeVisible();
    await expect(detail.getByText("gpt-5 → relay-model", { exact: true })).toBeVisible();
    if (process.platform === "win32") {
      await expect(detail.getByRole("heading", { name: "File update timing" })).toBeVisible();
      await expect(detail.getByText("Restore file timestamps", { exact: true })).toBeVisible();
    }
    await page.screenshot({ path: test.info().outputPath("switch-history-details.png") });
    expect((await fixture.inspect()).backupIds).toHaveLength(1);
  } finally {
    try { await electronApp?.close(); } finally { await fixture.close(); }
  }
});

test("production or unpacked desktop completes real Sync and Restore through Utility Core", async () => {
  test.setTimeout(PRODUCTION_SMOKE_TIMEOUT_MS);
  const fixture = await createDesktopSyncSwitchFixture();
  await claimDailyUpdateCheck(fixture.userData);
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
    await page.getByRole("button", { name: "Preview sync" }).click();
    const dialog = page.getByRole("dialog", { name: "Confirm sync" });
    await expect(dialog).toBeVisible();
    await dialog.getByRole("button", { name: "Close" }).last().click();
    await page.getByRole("button", { name: "Sync now" }).click();
    await expect(dialog).toBeHidden();
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
    await page.getByRole("button", { name: "Preview restore" }).click();
    const restoreDialog = page.getByRole("dialog", { name: "Confirm restore" });
    await expect(restoreDialog).toBeVisible();
    await restoreDialog.getByRole("button", { name: "Confirm restore" }).click();
    await expect(page.getByText("Operation completed.", { exact: true }).last()).toBeVisible({
      timeout: PRODUCTION_OPERATION_TIMEOUT_MS
    });
    await expect.poll(
      async () => (await fixture.snapshotTargets()).hash,
      { timeout: PRODUCTION_OPERATION_TIMEOUT_MS }
    ).toBe(baseline.hash);

    await page.getByRole("dialog", { name: "Operation result" }).getByRole("button", { name: "Close" }).last().click();
    await page.getByRole("button", { name: "Advanced features", exact: true }).click();
    await page.getByRole("button", { name: "Start diagnostics" }).click();
    await expect(page.getByText(/Diagnostics completed ·/)).toBeVisible();
    await page.getByText("Advanced adjustments", { exact: true }).click();
    await page.getByRole("checkbox", { name: "Unify historical model names", exact: true }).check();
    await page.getByRole("button", { name: "Preview adjustment", exact: true }).click();
    const repairDialog = page.getByRole("dialog", { name: "Confirm repair" });
    await expect(repairDialog).toBeVisible();
    await repairDialog.getByRole("radio", { name: "Selected chats", exact: true }).check();
    await repairDialog.getByRole("checkbox", { name: "Select chat c7-desktop-session", exact: true }).check();
    await expect(repairDialog.getByRole("button", { name: "Confirm repair", exact: true })).toBeDisabled();
    await repairDialog.getByRole("button", { name: "Update repair preview", exact: true }).click();
    await expect(repairDialog.getByRole("button", { name: "Confirm repair", exact: true })).toBeEnabled();
    await repairDialog.getByRole("button", { name: "Confirm repair", exact: true }).click();
    const repairResult = page.getByRole("dialog", { name: "Operation result" });
    await expect(repairResult.getByText("The selected repair targets were verified after the write.", { exact: true })).toBeVisible({ timeout: PRODUCTION_OPERATION_TIMEOUT_MS });
    await repairResult.getByRole("button", { name: "Open restore preview", exact: true }).click();
    await page.getByRole("button", { name: "Preview restore", exact: true }).click();
    await restoreDialog.getByRole("button", { name: "Confirm restore", exact: true }).click();
    await expect(page.getByText("Operation completed.", { exact: true }).last()).toBeVisible({ timeout: PRODUCTION_OPERATION_TIMEOUT_MS });
    await expect.poll(async () => (await fixture.snapshotTargets()).hash, { timeout: PRODUCTION_OPERATION_TIMEOUT_MS }).toBe(baseline.hash);

    const events = await page.evaluate(() => globalThis.__productionOperationEvents);
    expect(events.filter((event) => event.event === "operation-started")).toHaveLength(4);
    expect(JSON.stringify(events)).not.toMatch(/codex-home|state_5\.sqlite|backupDir|messageBody/i);
    const logs = await page.evaluate(() => window.codexProvider.operationLogs.list({ schemaVersion: 1, page: 1, pageSize: 50 }));
    const completedSync = logs.entries.find((entry) => entry.operation === "sync" && entry.status === "completed");
    expect(completedSync.profileRevision).toBeTruthy();
    expect(completedSync.backupId).toBe(syncBackupId);
    const logDetail = await page.evaluate((id) => window.codexProvider.operationLogs.get({ schemaVersion: 1, id }), completedSync.id);
    expect(logDetail.planId).toBeTruthy();
    expect(logDetail.stages.length).toBeGreaterThan(0);
    expect(JSON.stringify(logs)).not.toContain(fixture.codexHome);
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
