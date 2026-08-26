import { expect, test } from "@playwright/test";

import { createWebUiFixture } from "../../../scripts/run-web-ui-fixture.js";

let fixture;

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
  await expect(page.getByRole("heading", { name: "Provider metadata overview" })).toBeVisible();
  await expect(page.locator("html")).toHaveAttribute("lang", "en");
  await page.keyboard.press("Tab");
  await expect(page.getByRole("link", { name: "Skip to content" })).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.locator("#main-content")).toBeFocused();
  expect(coreRequests.some((entry) => entry.method === "listHistory")).toBe(false);
  expect(coreRequests.some((entry) => entry.method === "getHistorySession")).toBe(false);
  expect(JSON.stringify(coreRequests)).not.toContain("C5_BODY_ONLY_MARKER");
  expect(JSON.stringify(coreRequests)).not.toMatch(/codexHome|sqliteHome|cwd/i);

  const pages = [
    ["Sync", "Sync current Provider"],
    ["Switch Provider", "Switch Provider"],
    ["Backups / Restore", "Backups and Restore"],
    ["Profiles", "Profiles"],
    ["Diagnostics", "Diagnostics"],
    ["Settings", "Settings"],
    ["Overview", "Provider metadata overview"]
  ];
  for (const [navigation, heading] of pages) {
    await page.getByRole("button", { name: navigation, exact: true }).click();
    await expect(page.getByRole("heading", { name: heading, level: 1 })).toBeVisible();
  }

  await page.getByRole("button", { name: "History", exact: true }).click();
  await expect(page.getByRole("heading", { name: "History", level: 1 })).toBeVisible();
  await expect(page.getByText("Synthetic History")).toBeVisible();
  expect(coreRequests.filter((entry) => entry.method === "listHistory")).toHaveLength(1);
  expect(coreRequests.some((entry) => entry.method === "getHistorySession")).toBe(false);
  await expect(page.getByText("C5_BODY_ONLY_MARKER")).toHaveCount(0);

  await page.getByRole("button", { name: "Open session" }).click();
  await expect(page.getByText("C5_BODY_ONLY_MARKER")).toBeVisible();
  expect(coreRequests.filter((entry) => entry.method === "getHistorySession")).toHaveLength(1);
  await page.getByRole("button", { name: "Back to sessions" }).click();
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
  await expect(page.getByText("英文为兜底语言")).toBeVisible();
  await page.getByLabel("语言").selectOption("en");
  await expect(page.getByRole("heading", { name: "Settings", level: 1 })).toBeVisible();
  await expect(page.locator("html")).toHaveAttribute("lang", "en");
  await page.getByRole("button", { name: "Dark", exact: true }).click();
  await expect.poll(() => page.evaluate(() => document.documentElement.dataset.theme)).toBe("dark");
  await page.emulateMedia({ reducedMotion: "reduce" });
  expect(parseFloat(await page.getByRole("button", { name: "Overview", exact: true }).evaluate((element) => getComputedStyle(element).transitionDuration))).toBeLessThanOrEqual(0.001);

  await page.setViewportSize({ width: 640, height: 900 });
  for (const [navigation, heading] of [...pages, ["History", "History"]]) {
    await page.getByRole("button", { name: navigation, exact: true }).click();
    await expect(page.getByRole("heading", { name: heading, level: 1 })).toBeVisible();
    await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    const layout = await page.evaluate(() => ({
      clientWidth: document.documentElement.clientWidth,
      scrollWidth: document.documentElement.scrollWidth
    }));
    expect(layout.scrollWidth, `${navigation} overflowed the 640px/200% equivalent viewport`).toBeLessThanOrEqual(layout.clientWidth);
  }

  await page.getByRole("button", { name: "Sync", exact: true }).click();
  const prepare = page.getByRole("button", { name: "Prepare sync" });
  await prepare.click();
  await expect(page.getByRole("dialog", { name: "Review plan" })).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(prepare).toBeFocused();

  await prepare.click();
  await page.getByRole("button", { name: "Confirm and apply" }).click();
  await expect(page.getByRole("dialog", { name: "Review plan" })).toHaveCount(0);
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
        rolloutScanComplete: true,
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
  await page.getByRole("button", { name: "Sync", exact: true }).click();
  await page.getByRole("button", { name: "Prepare sync" }).click();
  await page.getByRole("button", { name: "Confirm and apply" }).click();
  await expect(page.getByText("Completed with locked rollout files skipped.")).toBeVisible();

  recovery = true;
  await page.reload();
  await expect(page.getByText("RECOVERY_REQUIRED")).toBeVisible();
  await expect(page.locator("#main-content").getByText("Operation in progress", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Sync", exact: true }).click();
  await expect(page.getByRole("button", { name: "Prepare sync" })).toBeDisabled();

  recovery = false;
  failStatus = true;
  await page.reload();
  await expect(page.getByRole("alert")).toContainText("An internal error occurred.");
});
