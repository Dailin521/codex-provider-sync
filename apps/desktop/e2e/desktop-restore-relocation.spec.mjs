import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { _electron as electron, expect, test } from "@playwright/test";

import { createDesktopSyncSwitchFixture } from "../../../test-support/desktop-sync-switch-fixture.mjs";

const require = createRequire(import.meta.url);
const desktopRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const electronExecutable = require("electron");

async function launchDesktop(fixture) {
  return electron.launch({
    executablePath: electronExecutable,
    args: [path.join(desktopRoot, "out", "main", "index.js"), "--lang=en-US"],
    env: {
      ...process.env,
      CPS_DESKTOP_E2E: "1",
      CPS_DESKTOP_CODEX_HOME: fixture.codexHome,
      CPS_DESKTOP_USER_DATA: fixture.userData,
      CPS_DESKTOP_WINDOW_DISPLAY: "hidden",
      ELECTRON_ENABLE_SECURITY_WARNINGS: "true"
    }
  });
}

async function confirmPlan(page) {
  const dialog = page.getByRole("dialog", { name: "Review plan" });
  await expect(dialog).toBeVisible();
  await dialog.getByRole("button", { name: "Confirm and apply" }).click();
  await expect(dialog).toHaveCount(0);
  const resultDialog = page.getByRole("dialog", { name: "Operation result" });
  await expect(resultDialog).toBeVisible();
  await expect(resultDialog.getByRole("heading", { name: "Completed", exact: true })).toBeVisible();
  await resultDialog.getByRole("button", { name: "Close" })
    .last()
    .click();
}

test("hidden Electron restores only the State DB into an explicit relocation target", async () => {
  test.setTimeout(120_000);
  const fixture = await createDesktopSyncSwitchFixture();
  const originalSourceSqlite = await fixture.snapshotSqlite();
  const originalTargetSqlite = await fixture.snapshotSqlite(fixture.targetStateDbPath);
  let electronApp;
  try {
    electronApp = await launchDesktop(fixture);
    const page = await electronApp.firstWindow();
    await expect(page.getByText("openai", { exact: true }).first()).toBeVisible();

    await page.getByRole("button", { name: "Sync" }).click();
    await page.getByRole("button", { name: "Prepare sync" }).click();
    await confirmPlan(page);
    await expect.poll(async () => (await fixture.inspect()).sqlite.provider).toBe("openai");
    const sourceAfterSync = await fixture.snapshotTargets();
    const backupId = (await fixture.inspect()).backupIds[0];
    expect(backupId).toBeTruthy();

    await page.getByRole("button", { name: "Backups / Restore" }).click();
    await page.getByRole("button", { name: new RegExp(backupId) }).click();
    await page.getByLabel("Restore config.toml").uncheck();
    await page.getByLabel("Restore rollout files").uncheck();
    await expect(page.getByLabel("Restore State DB")).toBeChecked();
    await page.getByLabel("Confirm SQLite Home relocation").check();
    await page.getByLabel("Relocation target profile").selectOption("relocation-target");
    await page.getByRole("button", { name: "Prepare restore" }).click();

    const dialog = page.getByRole("dialog", { name: "Review plan" });
    await expect(dialog).toContainText("Restore config.toml");
    await expect(dialog).toContainText("Restore State DB");
    await expect(dialog).toContainText("Restore rollout files");
    await expect(dialog).toContainText("SQLite Home relocation");
    await confirmPlan(page);

    expect((await fixture.snapshotTargets()).hash).toBe(sourceAfterSync.hash);
    const relocated = await fixture.snapshotSqlite(fixture.targetStateDbPath);
    expect(relocated.hash).toBe(originalSourceSqlite.hash);
    expect(relocated.hash).not.toBe(originalTargetSqlite.hash);
    expect((await fixture.inspect()).sqlite.provider).toBe("openai");
    expect(relocated.threads[0].model_provider).toBe("legacy-provider");

    const defaultProfile = (await page.evaluate(
      () => window.codexProvider.profiles.list()
    )).profiles.find((profile) => profile.id === "default");
    const invalid = await page.evaluate(async ({ profile, backupId: selectedBackupId }) => (
      window.codexProvider.core.requestRestore({
        protocolVersion: 1,
        requestId: "restore-relocation-no-sqlite-target",
        method: "prepareRestore",
        payload: {
          profile: { profileId: profile.id, profileRevision: profile.revision },
          backupId: selectedBackupId,
          restoreConfig: false,
          restoreDatabase: true,
          restoreSessions: false,
          allowSqliteHomeRelocation: true,
          relocationTargetProfileId: "no-sqlite-target"
        }
      })
    ), { profile: defaultProfile, backupId });
    expect(invalid.ok).toBe(false);
    expect(invalid.error.code).toBe("INVALID_INPUT");
    expect((await fixture.snapshotTargets()).hash).toBe(sourceAfterSync.hash);
    expect((await fixture.snapshotSqlite(fixture.targetStateDbPath)).hash).toBe(relocated.hash);
  } finally {
    await electronApp?.close();
    await fixture.close();
  }
});
