import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { _electron as electron, expect, test } from "@playwright/test";

import { createDesktopReadOnlyFixture } from "../../../test-support/desktop-readonly-fixture.mjs";

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
        ELECTRON_ENABLE_SECURITY_WARNINGS: "true"
      }
    });
    const page = await electronApp.firstWindow();
    await expect(page).toHaveURL("cps-app://app/index.html");
    const boundary = await page.evaluate(() => ({
      bridgeKeys: Object.keys(window.codexProvider).sort(),
      coreKeys: Object.keys(window.codexProvider.core).sort(),
      process: typeof globalThis.process,
      require: typeof globalThis.require
    }));
    expect(boundary).toEqual({
      bridgeKeys: ["core", "profiles", "version"],
      coreKeys: ["requestReadOnly"],
      process: "undefined",
      require: "undefined"
    });

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

    await page.getByRole("button", { name: "History" }).click();
    await expect(page.getByText("Untitled session", { exact: true })).toBeVisible();
    await expect(page.locator("body")).not.toContainText("C6_DESKTOP_BODY_ONLY_MARKER");
  } finally {
    await electronApp?.close();
    await fixture.assertUnchanged();
    await fixture.close();
  }
});
