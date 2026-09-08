import { expect, test } from "@playwright/test";
import { mkdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { createWebUiFixture } from "../../../scripts/run-web-ui-fixture.js";

let fixture;

async function assertOverviewOrder(page) {
  const elements = [
    page.getByRole("heading", { name: "Session files", exact: true }),
    page.getByRole("heading", { name: "Local chat index", exact: true }),
    page.getByText("Storage profile", { exact: true }),
    page.getByRole("button", { name: "Preview sync", exact: true }),
    page.getByRole("heading", { name: "Switch Provider separately", exact: true })
  ];
  await expect(elements[0]).toBeVisible();
  const boxes = await Promise.all(elements.map((element) => element.boundingBox()));
  for (const box of boxes) expect(box).not.toBeNull();
  expect(boxes[0].y + boxes[0].height).toBeLessThan(boxes[2].y);
  expect(boxes[1].y + boxes[1].height).toBeLessThan(boxes[2].y);
  expect(boxes[2].y + boxes[2].height).toBeLessThan(boxes[3].y);
  expect(boxes[3].y + boxes[3].height).toBeLessThan(boxes[4].y);
  const cells = page.getByTestId("overview-storage-sync").locator(":scope > div");
  const [storage, sync] = await Promise.all([cells.nth(0).boundingBox(), cells.nth(1).boundingBox()]);
  if (page.viewportSize().width >= 1024) {
    expect(Math.abs(storage.y - sync.y)).toBeLessThan(2);
    expect(storage.x + storage.width).toBeLessThan(sync.x);
    await page.evaluate(() => window.scrollTo(0, 0));
    await expect(page.getByRole("button", { name: "Preview sync", exact: true })).toBeInViewport({ ratio: 1 });
    await expect(page.getByRole("button", { name: "Sync now", exact: true })).toBeInViewport({ ratio: 1 });
  } else {
    expect(storage.y + storage.height).toBeLessThan(sync.y);
  }
  const artifactRoot = new URL("../../../output/playwright/", import.meta.url);
  await mkdir(artifactRoot, { recursive: true });
  await page.screenshot({ path: fileURLToPath(new URL(`overview-usage-${page.viewportSize().width}.png`, artifactRoot)), fullPage: true });
}

test.beforeAll(async () => {
  fixture = await createWebUiFixture();
});

test.afterAll(async () => {
  await fixture?.close();
});

test("paired production UI keeps history lazy and Apply opaque", async ({ page }) => {
  const consoleErrors = [];
  const coreRequests = [];
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("request", (request) => {
    if (request.url() !== `${fixture.origin}/api/core` || request.method() !== "POST") return;
    const payload = request.postDataJSON();
    coreRequests.push(payload);
  });

  const response = await page.goto(fixture.pairingUrl);
  expect(response?.status()).toBe(200);
  const contentSecurityPolicy = response?.headers()["content-security-policy"] ?? "";
  expect(contentSecurityPolicy).toContain("style-src 'self'");
  expect(contentSecurityPolicy).not.toContain("unsafe-inline");
  const nonce = /script-src 'self' 'nonce-([^']+)'/.exec(contentSecurityPolicy)?.[1];
  expect(nonce).toBeTruthy();
  expect(await page.locator("script[nonce]").evaluate((element) => element.nonce)).toBe(nonce);
  await expect(page).toHaveURL(`${fixture.origin}/`);
  await expect(page.getByRole("heading", { name: "Provider sync overview" })).toBeVisible();
  await expect(page.locator("html")).toHaveAttribute("lang", "en");
  await page.keyboard.press("Tab");
  await expect(page.getByRole("link", { name: "Skip to content" })).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.locator("#main-content")).toBeFocused();
  expect(coreRequests.some((entry) => entry.method === "listHistory")).toBe(false);
  expect(coreRequests.some((entry) => entry.method === "getHistorySession")).toBe(false);
  expect(JSON.stringify(coreRequests)).not.toContain("C5_BODY_ONLY_MARKER");
  expect(JSON.stringify(coreRequests)).not.toMatch(/codexHome|sqliteHome|cwd/i);
  await assertOverviewOrder(page);

  const pages = [
    ["Backups / Restore", "Backups and Restore"],
    ["Profiles", "Profiles"],
    ["Advanced features", "Advanced features"],
    ["Settings", "Settings"],
    ["Overview", "Provider sync overview"]
  ];
  for (const [navigation, heading] of [...pages, ["History", "Chats"]]) {
    await page.getByRole("button", { name: navigation, exact: true }).click();
    await expect(page.getByRole("heading", { name: heading, level: 1 })).toBeVisible();
  }

  await page.getByRole("button", { name: "History", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Chats", level: 1 })).toBeVisible();
  await expect(page.getByText("Synthetic History")).toBeVisible();
  expect(coreRequests.filter((entry) => entry.method === "listHistory")).toHaveLength(1);
  expect(coreRequests.some((entry) => entry.method === "getHistorySession")).toBe(false);
  await expect(page.getByText("C5_BODY_ONLY_MARKER")).toHaveCount(0);

  await page.setViewportSize({ width: 640, height: 900 });
  const sessionRow = page.getByRole("button", { name: "View chat: Synthetic History" });
  await sessionRow.click();
  await expect(page.getByText("C5_BODY_ONLY_MARKER")).toBeVisible();
  expect(coreRequests.filter((entry) => entry.method === "getHistorySession")).toHaveLength(1);
  await page.getByRole("button", { name: "Back to chats" }).click();
  await expect(sessionRow).toBeFocused();
  await expect(page.getByText("C5_BODY_ONLY_MARKER")).toHaveCount(0);
  await page.getByRole("button", { name: "Overview", exact: true }).click();
  await page.getByRole("button", { name: "History", exact: true }).click();
  await expect(page.getByText("C5_BODY_ONLY_MARKER")).toHaveCount(0);

  await page.getByRole("button", { name: "Settings", exact: true }).click();
  await page.getByLabel("Language").selectOption("zh-CN");
  await expect(page.getByRole("heading", { name: "设置", level: 1 })).toBeVisible();
  await expect(page.locator("html")).toHaveAttribute("lang", "zh-CN");
  await expect(page.getByLabel("存储配置")).toBeVisible();
  await expect(page.locator('nav[aria-label="主导航"]')).toBeVisible();
  await expect(page.getByText("修改后立即生效。")).toBeVisible();
  await page.getByLabel("语言").selectOption("en");
  await expect(page.getByRole("heading", { name: "Settings", level: 1 })).toBeVisible();
  await expect(page.locator("html")).toHaveAttribute("lang", "en");
  await page.getByRole("button", { name: "Dark", exact: true }).click();
  await expect.poll(() => page.evaluate(() => document.documentElement.dataset.theme)).toBe("dark");
  const coldPage = await page.context().newPage();
  let releaseBundle = () => {};
  const bundleGate = new Promise((resolve) => { releaseBundle = resolve; });
  let observeBundle = () => {};
  const bundleObserved = new Promise((resolve) => { observeBundle = resolve; });
  await coldPage.route(/\/assets\/index-[^/]+\.js$/, async (route) => {
    observeBundle();
    await bundleGate;
    await route.continue();
  });
  try {
    await coldPage.goto(fixture.origin, { waitUntil: "commit" });
    await bundleObserved;
    await expect.poll(() => coldPage.evaluate(() => document.documentElement.dataset.theme)).toBe("dark");
    expect(await coldPage.locator("#root").textContent()).toBe("");
  } finally {
    releaseBundle();
  }
  await expect(coldPage.getByRole("heading", { name: "Provider sync overview" })).toBeVisible();
  await coldPage.close();
  await page.emulateMedia({ reducedMotion: "reduce" });
  expect(parseFloat(await page.getByRole("button", { name: "Overview", exact: true }).evaluate((element) => getComputedStyle(element).transitionDuration))).toBeLessThanOrEqual(0.001);

  await page.setViewportSize({ width: 640, height: 900 });
  for (const [navigation, heading] of [...pages, ["History", "Chats"]]) {
    await page.getByRole("button", { name: navigation, exact: true }).click();
    await expect(page.getByRole("heading", { name: heading, level: 1 })).toBeVisible();
    await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    const layout = await page.evaluate(() => ({
      clientWidth: document.documentElement.clientWidth,
      scrollWidth: document.documentElement.scrollWidth
    }));
    expect(layout.scrollWidth, `${navigation} overflowed the 640px/200% equivalent viewport`).toBeLessThanOrEqual(layout.clientWidth);
    if (navigation === "Overview") await assertOverviewOrder(page);
  }

  await page.setViewportSize({ width: 380, height: 700 });
  for (const [navigation, heading] of [...pages, ["History", "Chats"]]) {
    await page.getByRole("button", { name: navigation, exact: true }).click();
    const pageHeading = page.getByRole("heading", { name: heading, level: 1 });
    await expect(pageHeading).toBeVisible();
    await expect(pageHeading).toBeInViewport();
    const layout = await page.evaluate(() => ({
      clientWidth: document.documentElement.clientWidth,
      scrollWidth: document.documentElement.scrollWidth
    }));
    expect(layout.scrollWidth, `${navigation} overflowed the 760px window at 200% zoom`).toBeLessThanOrEqual(layout.clientWidth);
    if (navigation === "Overview") await assertOverviewOrder(page);
  }
  await expect(page.getByLabel("Profile")).toBeVisible();
  await expect(page.locator("header").getByText("Ready", { exact: true })).toBeVisible();

  await page.getByRole("button", { name: "Overview", exact: true }).click();
  const prepare = page.getByRole("button", { name: "Preview sync" });
  await prepare.click();
  const planDialog = page.getByRole("dialog", { name: "Confirm sync" });
  await expect(planDialog).toBeVisible();
  await expect(planDialog.getByText("Session files to update")).toBeVisible();
  await expect(planDialog.getByText("A backup will be created before writes.")).toHaveCount(0);
  await expect(planDialog.getByText("Expected changes")).toBeVisible();
  await expect(planDialog.getByText("The app checks the data again before applying these changes. If anything changed, you will be asked to review again.")).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(prepare).toBeFocused();

  await prepare.click();
  await page.getByRole("button", { name: "Confirm sync" }).click();
  await expect(page.getByRole("dialog", { name: "Confirm sync" })).toHaveCount(0);
  const resultDialog = page.getByRole("dialog", { name: "Operation result" });
  await expect(resultDialog).toBeVisible();
  const resultLayout = await page.evaluate(() => ({
    clientWidth: document.documentElement.clientWidth,
    scrollWidth: document.documentElement.scrollWidth
  }));
  expect(resultLayout.scrollWidth).toBeLessThanOrEqual(resultLayout.clientWidth);
  await resultDialog.getByRole("button", { name: "Close" }).last().click();
  await expect(prepare).toBeFocused();
  const applyRequest = coreRequests.findLast((entry) => entry.method === "applySync");
  expect(applyRequest).toBeDefined();
  expect(Object.keys(applyRequest.payload).sort()).toEqual(["planId", "schemaVersion"]);
  expect(applyRequest.payload.schemaVersion).toBe(1);
  expect(typeof applyRequest.payload.planId).toBe("string");
  expect(consoleErrors).toEqual([]);
});

test("global partial, recovery, operation and error states are visible", async ({ page }) => {
  let recovery = false;
  let failStatus = false;
  let needsRefresh = true;
  let statusRequests = 0;
  await page.route(`${fixture.origin}/api/core`, async (route) => {
    const envelope = route.request().postDataJSON();
    const success = (result) => route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        protocolVersion: 1,
        requestId: envelope.requestId,
        ok: true,
        result
      })
    });
    if (envelope.method === "getStatus") {
      statusRequests += 1;
      if (failStatus) {
        await route.fulfill({
          status: 500,
          contentType: "application/json",
          body: JSON.stringify({
            protocolVersion: 1,
            requestId: envelope.requestId,
            ok: false,
            error: {
              code: "INTERNAL_ERROR",
              message: "An internal error occurred.",
              severity: "fatal",
              retryable: false,
              recoveryRequired: false
            }
          })
        });
        return;
      }
      await success({
        schemaVersion: 1,
        snapshotAt: "2026-08-26T00:00:00.000Z",
        storageRevision: "storage-r1",
        profile: {
          id: envelope.payload.profile.profileId,
          revision: envelope.payload.profile.profileRevision
        },
        currentProvider: "openai",
        rolloutCounts: { sessions: { openai: 1 }, archived_sessions: {} },
        sqliteCounts: { sessions: { openai: 1 }, archived_sessions: {} },
        codexHomeSource: "profile",
        sqliteHomeSource: "default",
        backupSummary: { count: 0, totalBytes: 0 },
        pendingRecovery: recovery,
        pendingTransactions: recovery ? [{ operationId: "recovery-operation", state: "recovery-required" }] : [],
        operationInProgress: recovery ? { operationId: "active-operation", operation: "restore", busyScope: "state-db" } : null,
        ...(needsRefresh ? { statusReadBlocked: { reason: "state-changed-during-status" } } : {}),
        rolloutScanComplete: !needsRefresh,
        lockedRolloutFiles: []
      });
      return;
    }
    if (envelope.method === "prepareSync") {
      await success({
        schemaVersion: 1,
        planId: "synthetic-plan-id",
        operation: "sync",
        createdAt: "2026-08-26T00:00:00.000Z",
        expiresAt: "2026-08-26T00:10:00.000Z",
        profile: {
          id: envelope.payload.profile.profileId,
          revision: envelope.payload.profile.profileRevision
        },
        storageRevision: "storage-r1",
        configRevision: "config-r1",
        rolloutRevision: "rollout-r1",
        stateDbRevision: "state-db-r1",
        target: { provider: "openai", model: null },
        impact: { rolloutFilesToChange: 1, sqliteRowsToChange: 0, backupExpected: true },
        warnings: [],
        requiresConfirmation: true
      });
      return;
    }
    if (envelope.method === "applySync") {
      await success({
        schemaVersion: 1,
        operationId: "synthetic-operation",
        operation: "sync",
        outcome: "partial",
        backup: { backupId: "synthetic-backup" },
        warnings: ["One or more rollout files are locked and may be skipped."],
        result: { skippedLockedRolloutCount: 1 }
      });
      return;
    }
    await route.continue();
  });

  await page.goto(fixture.issuePairingUrl());
  const recheck = page.getByRole("button", { name: "Check status again" });
  await expect(recheck).toBeVisible();
  await expect(page.locator("header").getByText("Refresh needed", { exact: true })).toBeVisible();
  await expect(page.getByText("Operation in progress", { exact: true })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Sync now" })).toBeDisabled();
  await expect(page.getByRole("button", { name: "Preview sync" })).toBeDisabled();
  await page.screenshot({ path: test.info().outputPath("status-refresh-needed.png"), fullPage: true });
  expect(statusRequests).toBe(1);
  needsRefresh = false;
  await recheck.click();
  await expect(recheck).toHaveCount(0);
  await expect(page.locator("header").getByText("Ready", { exact: true })).toBeVisible();
  expect(statusRequests).toBe(2);
  await page.getByRole("button", { name: "Preview sync" }).click();
  await page.getByRole("button", { name: "Confirm sync" }).click();
  await expect(page.getByRole("dialog", { name: "Operation result" })).toContainText("Partially completed");
  await expect(page.getByText("Some changes were not completed. Follow the guidance below to retry, or restore from a backup.", { exact: true })).toBeVisible();

  recovery = true;
  await page.reload();
  await expect(page.locator("#main-content").getByText("Recovery required", { exact: true })).toBeVisible();
  await expect(page.locator("#main-content").getByText("Operation in progress", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Preview sync" })).toBeDisabled();

  recovery = false;
  failStatus = true;
  await page.reload();
  await expect(page.getByRole("alert")).toContainText("An internal error occurred.");
});
