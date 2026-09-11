import fs from "node:fs/promises";
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { _electron as electron, expect, test } from "@playwright/test";

import { createDesktopSyncSwitchFixture } from "../../../test-support/desktop-sync-switch-fixture.mjs";

const require = createRequire(import.meta.url);
const desktopRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const electronExecutable = require("electron");

async function launchDesktop(fixture, extraEnv = {}) {
  return electron.launch({
    executablePath: electronExecutable,
    args: [path.join(desktopRoot, "out", "main", "index.js"), "--lang=en-US"],
    env: {
      ...process.env,
      CPS_DESKTOP_E2E: "1",
      CPS_DESKTOP_CODEX_HOME: fixture.codexHome,
      CPS_DESKTOP_USER_DATA: fixture.userData,
      CPS_DESKTOP_WINDOW_DISPLAY: "hidden",
      ELECTRON_ENABLE_SECURITY_WARNINGS: "true",
      ...extraEnv
    }
  });
}

async function openSyncPlan(page) {
  await page.getByRole("button", { name: "Preview sync" }).click();
  const dialog = page.getByRole("dialog", { name: "Confirm sync" });
  await expect(dialog).toBeVisible();
  return dialog;
}

async function openSwitchPlan(page, { provider = "relay", mode = "provider-default", model } = {}) {
  await page.getByLabel("Provider ID").fill(provider);
  await page.getByLabel("Model handling").selectOption(mode);
  if (mode === "explicit") await page.getByLabel("Model name").fill(model);
  await page.getByRole("button", { name: "Preview switch" }).click();
  const dialog = page.getByRole("dialog", { name: "Confirm Provider switch" });
  await expect(dialog).toBeVisible();
  return dialog;
}

async function waitForGate(markerPath, expectedPoint, timeout = 10_000) {
  await expect.poll(async () => {
    try {
      return JSON.parse(await fs.readFile(markerPath, "utf8")).point;
    } catch (error) {
      if (error?.code === "ENOENT") return null;
      throw error;
    }
  }, { timeout }).toBe(expectedPoint);
}

function latestNotification(page) {
  return page.getByRole("listitem").last();
}

async function prepareSyncDirect(page, requestId) {
  const profile = (await page.evaluate(() => window.codexProvider.profiles.list())).profiles[0];
  return page.evaluate(async ({ profile, requestId }) => window.codexProvider.core.requestSyncSwitch({
    protocolVersion: 1,
    requestId,
    method: "prepareSync",
    payload: {
      profile: { profileId: profile.id, profileRevision: profile.revision },
      keepCount: 5
    }
  }), { profile, requestId });
}

async function applySyncDirect(page, planId, requestId) {
  return page.evaluate(async ({ planId, requestId }) => window.codexProvider.core.requestSyncSwitch({
    protocolVersion: 1,
    requestId,
    method: "applySync",
    payload: { schemaVersion: 1, planId }
  }), { planId, requestId });
}

async function fixtureStep(name, run) {
  // These names identify synthetic test phases only; never dump payloads.
  process.stderr.write(`[desktop-fixture] ${name}\n`);
  return test.step(name, run);
}

async function lockRolloutFile(filePath) {
  const script = `
& {
  param([string]$path)
  $ErrorActionPreference = 'Stop'
  $stream = [System.IO.File]::Open($path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
  try {
    Write-Output 'locked'
    [Console]::Out.Flush()
    [void][Console]::In.ReadLine()
  } finally {
    $stream.Close()
  }
}
`.trim();
  const child = spawn("powershell.exe", [
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-Command",
    script,
    filePath
  ], { stdio: ["pipe", "pipe", "pipe"], windowsHide: true });
  child.stdin.on("error", () => {}); // Exit/readiness below owns fixture failure reporting.
  await new Promise((resolve, reject) => {
    let output = "";
    let settled = false;
    const deadline = setTimeout(() => {
      if (settled) return;
      settled = true;
      child.kill();
      reject(new Error("Synthetic rollout lock was not ready within 20 seconds."));
    }, 20_000);
    child.stdout.on("data", (chunk) => {
      output += chunk.toString("utf8");
      if (!settled && output.includes("locked")) {
        settled = true;
        clearTimeout(deadline);
        resolve();
      }
    });
    child.once("error", (error) => {
      if (!settled) { settled = true; clearTimeout(deadline); reject(error); }
    });
    child.once("exit", (code, signal) => {
      if (!settled) {
        settled = true;
        clearTimeout(deadline);
        reject(new Error(`Rollout lock exited before ready (${code ?? "null"}/${signal ?? "null"}).`));
      }
    });
  });
  return child;
}

async function releaseChild(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  await new Promise((resolve, reject) => {
    let terminationDeadline;
    let releaseTimedOut = false;
    const deadline = setTimeout(() => {
      releaseTimedOut = true;
      child.kill();
      terminationDeadline = setTimeout(() => reject(new Error("Synthetic rollout lock did not exit after termination.")), 2_000);
    }, 5_000);
    child.once("exit", (code) => {
      clearTimeout(deadline);
      clearTimeout(terminationDeadline);
      if (releaseTimedOut) reject(new Error("Synthetic rollout lock did not release within 5 seconds."));
      else if (code === 0) resolve();
      else reject(new Error(`Synthetic rollout lock exited abnormally (${code ?? "null"}).`));
    });
    child.stdin.end("release\n");
  });
}

async function runProcess(command, args) {
  const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });
  const stdout = [];
  const stderr = [];
  child.stdout.on("data", (chunk) => stdout.push(chunk));
  child.stderr.on("data", (chunk) => stderr.push(chunk));
  const result = await new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve({ code, signal }));
  });
  if (result.code !== 0) {
    throw new Error(`${command} failed (${result.code ?? "null"}/${result.signal ?? "null"}): ${Buffer.concat(stderr).toString("utf8")}`);
  }
  return Buffer.concat(stdout).toString("utf8").trim();
}

async function findWslDistro() {
  const candidates = [...new Set([process.env.CPS_WSL_DISTRO, "Ubuntu"].filter(Boolean))];
  for (const candidate of candidates) {
    try {
      if (await runProcess("wsl.exe", ["-d", candidate, "--", "printf", "cps-ready"]) === "cps-ready") {
        return candidate;
      }
    } catch {}
  }
  return null;
}

async function confirmPlan(page, returnFocus, { dialogName = "Confirm sync", confirmName = "Confirm sync" } = {}) {
  const dialog = page.getByRole("dialog", { name: dialogName });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByText("Selected changes", { exact: true })).toBeVisible();
  await expect(dialog.getByText("Expected changes", { exact: true })).toBeVisible();
  await dialog.getByRole("button", { name: confirmName }).click();
  try {
    await expect(dialog).toHaveCount(0);
  } catch (error) {
    // Record only structured synthetic operation state on a failed wait. This
    // distinguishes a slow running Restore from a terminal Core error in CI.
    const logs = await page.evaluate(async () => {
      const result = await window.codexProvider.operationLogs.list({ schemaVersion: 1, page: 1, pageSize: 10 });
      return result.entries.map(({ operation, status, outcome, error: failure, stages }) => ({
        operation, status, outcome, code: failure?.code,
        stages: stages?.map(({ stage, durationMs }) => ({ stage, durationMs }))
      }));
    }).catch(() => null);
    process.stderr.write(`[desktop-fixture] confirmation wait failed: ${JSON.stringify({ dialogName, logs })}\n`);
    throw error;
  }
  const resultDialog = page.getByRole("dialog", { name: "Operation result" });
  await expect(resultDialog).toBeVisible();
  await expect(resultDialog.getByRole("heading", { name: "Completed", exact: true })).toBeVisible();
  await resultDialog.getByRole("button", { name: "Close" }).last().click();
  if (returnFocus) await expect(returnFocus).toBeFocused();
}

async function switchProvider(page, { provider, mode, model }) {
  await page.getByLabel("Provider ID").fill(provider);
  await page.getByLabel("Model handling").selectOption(mode);
  if (mode === "explicit") await page.getByLabel("Model name").fill(model);
  const prepare = page.getByRole("button", { name: "Preview switch" });
  await prepare.click();
  await confirmPlan(page, prepare, { dialogName: "Confirm Provider switch", confirmName: "Confirm switch" });
}

test("hidden Electron test build forces the native fallback through Status, Sync, Restore, and the narrow C8 bridge", async () => {
  test.setTimeout(120_000);
  const fixture = await createDesktopSyncSwitchFixture();
  const baseline = await fixture.snapshotTargets();
  let electronApp;
  let syncBackupId;
  try {
    const diagnosticsTarget = path.join(fixture.fixtureRoot, "diagnostics.zip");
    electronApp = await launchDesktop(fixture, {
      CPS_DESKTOP_DIAGNOSTICS_TARGET: diagnosticsTarget
    });
    const page = await electronApp.firstWindow();
    await expect(page).toHaveURL("cps-app://app/index.html");
    await expect(page.getByText("openai", { exact: true }).first()).toBeVisible();
    const profile = (await page.evaluate(() => window.codexProvider.profiles.list())).profiles[0];
    const fallbackStatus = await page.evaluate(async ({ profile }) => window.codexProvider.core.requestReadOnly({
      protocolVersion: 1,
      requestId: "c9-test-fallback-status",
      method: "getStatus",
      payload: { profile: { profileId: profile.id, profileRevision: profile.revision } }
    }), { profile });
    expect(fallbackStatus.ok).toBe(true);
    expect(fallbackStatus.result.sqliteCounts.sessions["legacy-provider"]).toBe(1);
    await page.evaluate(() => {
      globalThis.__c7OperationEvents = [];
      globalThis.__c7Unsubscribe = window.codexProvider.core.subscribeOperation((event) => {
        globalThis.__c7OperationEvents.push(event);
      });
    });

    await page.getByRole("button", { name: "Backups / Restore" }).click();
    await page.getByLabel("Backups to retain").fill("5");
    await page.getByRole("button", { name: "Save backup settings" }).click();
    await expect(page.getByText("Backup settings saved. No backups were deleted.")).toBeVisible();
    expect((await fixture.inspect()).backupIds).toHaveLength(0);
    await page.getByRole("button", { name: "Overview" }).click();
    await expect(page.getByRole("spinbutton")).toHaveCount(0);

    const prepareSync = page.getByRole("button", { name: "Preview sync" });
    await prepareSync.click();
    await confirmPlan(page, prepareSync);
    await expect.poll(async () => (await fixture.inspect()).sqlite.provider).toBe("openai");
    syncBackupId = (await fixture.inspect()).backupIds[0];

    await switchProvider(page, { provider: "relay", mode: "provider-default" });
    let state = await fixture.inspect();
    expect(state.configText).toMatch(/^model_provider = "relay"/m);
    expect(state.configText).toMatch(/^model = "relay-model"/m);

    await switchProvider(page, { provider: "openai", mode: "keep-root-model" });
    state = await fixture.inspect();
    expect(state.configText).toMatch(/^model_provider = "openai"/m);
    expect(state.configText).toMatch(/^model = "relay-model"/m);

    await switchProvider(page, { provider: "relay", mode: "explicit", model: "explicit-model" });
    state = await fixture.inspect();
    expect(state.configText).toMatch(/^model_provider = "relay"/m);
    expect(state.configText).toMatch(/^model = "explicit-model"/m);
    expect(state.rollout.model_provider).toBe("relay");
    expect(state.turnContext.model).toBe("legacy-model");
    expect(state.turnContext.collaboration_mode.settings.model).toBe("legacy-model");
    expect(state.sqlite.provider).toBe("relay");
    expect(state.sqlite.model).toBe("legacy-model");
    expect(state.sqlite.updatedAt).toBe(1787702400);
    expect(state.sqlite.updatedAtMs).toBe(1787702400000);
    expect(state.backupIds).toHaveLength(4);

    const events = await page.evaluate(() => globalThis.__c7OperationEvents);
    expect(events.filter((event) => event.event === "operation-started")).toHaveLength(4);
    expect(events.some((event) => event.event === "progress"
      && event.progress.stage === "create_backup")).toBe(true);
    expect(JSON.stringify(events)).not.toMatch(/codex-home|state_5\.sqlite|backupDir|messageBody/i);

    await page.getByRole("button", { name: "Backups / Restore" }).click();
    await page.getByRole("button", { name: new RegExp(syncBackupId) }).click();
    const beforeRestore = await fixture.snapshotTargets();
    const prepareRestore = page.getByRole("button", { name: "Preview restore" });
    await prepareRestore.click();
    await confirmPlan(page, prepareRestore, { dialogName: "Confirm restore", confirmName: "Confirm restore" });
    const restored = await fixture.snapshotTargets();
    // The selected backup came from Sync, so it intentionally captured only
    // rollout and SQLite mutations. Restore must not rewind the config later
    // changed by explicit Switch operations.
    expect(restored.config).toBe(beforeRestore.config);
    expect(restored.globalState).toBe(baseline.globalState);
    expect(restored.globalStateBackup).toBe(baseline.globalStateBackup);
    expect(restored.sessions).toEqual(baseline.sessions);
    expect(restored.archivedSessions).toEqual(baseline.archivedSessions);
    expect(restored.sqlite).toEqual(baseline.sqlite);

    await page.getByLabel("Backups to retain").fill("2");
    await page.getByRole("button", { name: "Save backup settings" }).click();
    await expect(page.getByText("Backup settings saved. No backups were deleted.")).toBeVisible();
    const backupsBeforeCleanup = (await fixture.inspect()).backupIds;
    await page.getByRole("button", { name: "Delete older backups" }).click();
    const cleanup = page.getByRole("dialog", { name: "Confirm backup cleanup" });
    await expect(cleanup).toBeVisible();
    expect((await fixture.inspect()).backupIds).toEqual(backupsBeforeCleanup);
    await cleanup.getByRole("button", { name: "Confirm cleanup" }).click();
    await expect(page.getByText("Operation completed.", { exact: true }).last()).toBeVisible();
    await expect.poll(async () => (await fixture.inspect()).backupIds.length).toBe(2);

    await page.getByRole("button", { name: "Settings", exact: true }).click();
    await page.getByRole("button", { name: "Enable automatic sync" }).click();
    await expect(page.getByRole("button", { name: "Disable automatic sync" })).toBeVisible();
    await page.getByRole("button", { name: "Disable automatic sync" }).click();
    await expect(page.getByRole("button", { name: "Enable automatic sync" })).toBeVisible();
    await expect(page.getByText("Updates", { exact: true })).toBeVisible();
    await expect(page.getByText("This installation does not support in-app updates.", { exact: true })).toBeVisible();

    await page.getByRole("button", { name: "Advanced features" }).click();
    await page.getByRole("button", { name: "Start diagnostics" }).click();
    await expect(page.getByText("Check results", { exact: true })).toBeVisible();
    await page.getByRole("button", { name: "Export redacted bundle" }).click();
    await expect(page.getByText("Redacted diagnostics bundle created.", { exact: true })).toBeVisible();
    const diagnostics = await fs.readFile(diagnosticsTarget);
    expect(diagnostics.toString("utf8")).not.toContain(fixture.codexHome);
    expect(diagnostics.toString("utf8")).not.toMatch(/auth\.json|encrypted_content|message body/i);

    const eventsAfterRestore = await page.evaluate(() => globalThis.__c7OperationEvents);
    expect(eventsAfterRestore.filter((event) => event.event === "operation-started")).toHaveLength(5);

    // Exercise the real bundled Preload DTO validator, not only the Main service.
    const logs = await page.evaluate(() => window.codexProvider.operationLogs.list({ schemaVersion: 1, page: 1, pageSize: 50 }));
    const syncLog = logs.entries.find((entry) => entry.operation === "sync" && entry.status === "completed");
    expect(syncLog.profileRevision).toBe(profile.revision);
    expect(syncLog.backupId).toBe(syncBackupId);
    expect(syncLog.activeDurationMs).toBeGreaterThanOrEqual(0);
    expect(syncLog.wallDurationMs).toBeGreaterThanOrEqual(syncLog.activeDurationMs);
    const detail = await page.evaluate((id) => window.codexProvider.operationLogs.get({ schemaVersion: 1, id }), syncLog.id);
    expect(detail.planId).toBeTruthy();
    expect(detail.stages.length).toBeGreaterThan(0);
    await page.getByRole("button", { name: "Operation logs", exact: true }).click();
    await expect(page.getByRole("heading", { name: "Operation logs", exact: true })).toBeVisible();
    await page.getByRole("button", { name: /^Sync Completed/ }).first().click();
    await expect(page.getByText("Processing time", { exact: true })).toBeVisible();
    await expect(page.getByText(syncLog.planId, { exact: true })).toBeVisible();
    await expect(page.getByRole("alert")).toHaveCount(0);
  } finally {
    await electronApp?.close();
    await fixture.close();
  }
});

test("hidden Electron runs diagnostics explicitly and applies a targeted model Repair", async () => {
  test.setTimeout(90_000);
  const fixture = await createDesktopSyncSwitchFixture();
  let electronApp;
  try {
    electronApp = await launchDesktop(fixture);
    const page = await electronApp.firstWindow();
    await expect(page.getByText("openai", { exact: true }).first()).toBeVisible();

    const beforeState = await fixture.inspect();
    const beforeBytes = await fs.readFile(fixture.rolloutPath);

    await page.getByRole("button", { name: "Advanced features" }).click();
    await expect(page.getByText("Diagnostics have not been run", { exact: true })).toBeVisible();
    await page.getByRole("button", { name: "Start diagnostics" }).click();
    await expect(page.getByText("Check results", { exact: true })).toBeVisible();
    await page.getByText("Advanced adjustments", { exact: true }).click();
    await page.getByLabel("Unify historical model names").check();
    await page.getByRole("button", { name: "Preview adjustment" }).click();

    const plan = page.getByRole("dialog", { name: "Confirm repair" });
    await expect(plan.getByText("Repair chat information", { exact: false })).toBeVisible();
    await expect(plan.getByText("Unify historical model names", { exact: true })).toBeVisible();
    await plan.getByRole("button", { name: "Confirm repair" }).click();
    await expect(page.getByText("Operation completed.", { exact: true })).toBeVisible();

    const result = page.getByRole("dialog", { name: "Operation result" });
    await expect(result.getByText("Repair targets", { exact: true })).toBeVisible();
    await expect(result.getByText("Unify historical model names", { exact: true })).toBeVisible();
    await expect(result.getByText("Model records updated", { exact: true })).toBeVisible();
    const afterState = await fixture.inspect();
    const afterBytes = await fs.readFile(fixture.rolloutPath);
    expect(afterState.configText).toBe(beforeState.configText);
    expect(afterState.rollout.model_provider).toBe(beforeState.rollout.model_provider);
    expect(afterState.sqlite.provider).toBe(beforeState.sqlite.provider);
    expect(afterState.turnContext.model).toBe("gpt-5");
    expect(afterState.sqlite.model).toBe("gpt-5");
    expect(afterBytes.toString("utf8")).toContain("C7_DESKTOP_BODY_ONLY_MARKER");
    expect(beforeBytes.toString("utf8")).toContain("C7_DESKTOP_BODY_ONLY_MARKER");
  } finally {
    await electronApp?.close();
    await fixture.close();
  }
});

test("Electron rejects a stale confirmed plan before backup or mutation", async () => {
  const fixture = await createDesktopSyncSwitchFixture();
  let electronApp;
  try {
    electronApp = await launchDesktop(fixture);
    const page = await electronApp.firstWindow();
    const dialog = await openSyncPlan(page);
    await fixture.appendConfigDrift();
    const expected = await fixture.snapshotProtected();
    await dialog.getByRole("button", { name: "Confirm sync" }).click();
    await expect(latestNotification(page)).toContainText(
      "The data changed. Review the operation again before continuing."
    );
    await expect(dialog).toHaveCount(0);
    expect((await fixture.snapshotProtected()).hash).toBe(expected.hash);
    expect((await fixture.inspect()).backupIds).toHaveLength(0);
  } finally {
    await electronApp?.close();
    await fixture.close();
  }
});

test("Electron rejects an unconfigured custom Provider before plan or backup creation", async () => {
  const fixture = await createDesktopSyncSwitchFixture();
  const baseline = await fixture.snapshotProtected();
  let electronApp;
  try {
    electronApp = await launchDesktop(fixture);
    const page = await electronApp.firstWindow();
    await expect(page.getByText("openai", { exact: true }).first()).toBeVisible();
    await page.getByLabel("Provider ID").fill("missing-provider");
    await page.getByLabel("Model handling").selectOption("provider-default");
    const prepareSwitch = page.getByRole("button", { name: "Preview switch" });
    await expect(prepareSwitch).toBeEnabled();
    await prepareSwitch.click();
    await expect(latestNotification(page)).toContainText("The selected Provider is not defined in config.toml.");
    await expect(latestNotification(page)).toContainText("No data was changed.");
    await expect(page.getByRole("dialog", { name: "Confirm Provider switch" })).toHaveCount(0);
    expect((await fixture.snapshotProtected()).hash).toBe(baseline.hash);
    expect((await fixture.inspect()).backupIds).toHaveLength(0);
  } finally {
    await electronApp?.close();
    await fixture.close();
  }
});

test("Electron Main rejects tampered and replayed plan IDs without a second backup", async () => {
  const fixture = await createDesktopSyncSwitchFixture();
  let electronApp;
  try {
    electronApp = await launchDesktop(fixture);
    const page = await electronApp.firstWindow();
    await expect(page.getByText("openai", { exact: true }).first()).toBeVisible();
    const prepared = await prepareSyncDirect(page, "plan-ownership-prepare");
    expect(prepared.ok, JSON.stringify(prepared)).toBe(true);

    const tampered = await applySyncDirect(
      page,
      "tampered-plan-id".padEnd(40, "x"),
      "plan-ownership-tampered"
    );
    expect(tampered.ok).toBe(false);
    expect(tampered.error.code).toBe("PLAN_EXPIRED");
    expect((await fixture.inspect()).backupIds).toHaveLength(0);

    const applied = await applySyncDirect(
      page,
      prepared.result.planId,
      "plan-ownership-apply"
    );
    expect(applied.ok, JSON.stringify(applied)).toBe(true);
    expect((await fixture.inspect()).backupIds).toHaveLength(1);

    const replay = await applySyncDirect(
      page,
      prepared.result.planId,
      "plan-ownership-replay"
    );
    expect(replay.ok).toBe(false);
    expect(replay.error.code).toBe("PLAN_EXPIRED");
    expect((await fixture.inspect()).backupIds).toHaveLength(1);
  } finally {
    await electronApp?.close();
    await fixture.close();
  }
});

test("Electron reports a real SQLite writer as busy before creating a backup", async () => {
  const fixture = await createDesktopSyncSwitchFixture();
  let electronApp;
  let sqliteLock;
  try {
    const baseline = await fixture.snapshotProtected();
    electronApp = await launchDesktop(fixture);
    const page = await electronApp.firstWindow();
    const dialog = await openSyncPlan(page);
    sqliteLock = fixture.holdSqliteWriteLock();
    await dialog.getByRole("button", { name: "Confirm sync" }).click();
    await expect(latestNotification(page)).toContainText(
      "The local chat index is in use. Close Codex and try again."
    );
    sqliteLock.release();
    sqliteLock = undefined;
    expect((await fixture.snapshotProtected()).hash).toBe(baseline.hash);
    expect((await fixture.inspect()).backupIds).toHaveLength(0);
  } finally {
    sqliteLock?.release();
    await electronApp?.close();
    await fixture.close();
  }
});

test("Electron reports a locked rollout as partial without rewriting the locked file", async () => {
  test.skip(process.platform !== "win32", "Real FileShare.None rollout locks are Windows-specific.");
  const fixture = await createDesktopSyncSwitchFixture();
  const rolloutBefore = await fs.readFile(fixture.rolloutPath);
  let electronApp;
  let lockProcess;
  try {
    lockProcess = await fixtureStep("Acquire synthetic rollout lock", () => lockRolloutFile(fixture.rolloutPath));
    electronApp = await fixtureStep("Launch locked-rollout Electron fixture", () => launchDesktop(fixture));
    const page = await fixtureStep("Open locked-rollout window", () => electronApp.firstWindow());
    const dialog = await fixtureStep("Preview locked-rollout sync", () => openSyncPlan(page));
    await fixtureStep("Confirm locked-rollout sync", () => dialog.getByRole("button", { name: "Confirm sync" }).click());
    await fixtureStep("Observe locked-rollout partial result", () => expect(page.getByRole("dialog", { name: "Operation result" }).getByRole("heading", { name: "Partially completed" })).toBeVisible());
    await fixtureStep("Release synthetic rollout lock", () => releaseChild(lockProcess));
    lockProcess = undefined;
    const state = await fixture.inspect();
    expect(await fs.readFile(fixture.rolloutPath)).toEqual(rolloutBefore);
    expect(state.rollout.model_provider).toBe("legacy-provider");
    expect(state.sqlite.provider).toBe("openai");
    expect(state.backupIds).toHaveLength(1);
  } finally {
    try {
      await releaseChild(lockProcess);
    } finally {
      try { await electronApp?.close(); }
      finally { await fixture.close(); }
    }
  }
});

test("Electron Cancel before backup leaves every protected target unchanged", async () => {
  test.setTimeout(90_000);
  const fixture = await createDesktopSyncSwitchFixture();
  const baseline = await fixture.snapshotProtected();
  let electronApp;
  try {
    electronApp = await launchDesktop(fixture, {
      CPS_DESKTOP_TEST_GATE: "before_backup",
      CPS_DESKTOP_TEST_GATE_FILE: fixture.gateMarkerPath
    });
    const page = await electronApp.firstWindow();
    const dialog = await openSyncPlan(page);
    await dialog.getByRole("button", { name: "Confirm sync" }).click();
    await waitForGate(fixture.gateMarkerPath, "before_backup");
    await dialog.getByRole("button", { name: "Cancel operation" }).click();
    await expect(page.getByText("Operation cancelled.", { exact: true })).toBeVisible();
    await expect(dialog).toHaveCount(0);
    expect((await fixture.snapshotProtected()).hash).toBe(baseline.hash);
    expect(await fixture.readJournals()).toEqual([]);
  } finally {
    await electronApp?.close();
    await fixture.close();
  }
});

test("Electron Cancel after config mutation returns partial and a retry converges", async () => {
  const fixture = await createDesktopSyncSwitchFixture();
  let electronApp;
  try {
    electronApp = await launchDesktop(fixture, {
      CPS_DESKTOP_TEST_GATE: "after_config_mutation_before_applied",
      CPS_DESKTOP_TEST_GATE_FILE: fixture.gateMarkerPath
    });
    const page = await electronApp.firstWindow();
    const dialog = await openSwitchPlan(page, { provider: "relay", mode: "provider-default" });
    await dialog.getByRole("button", { name: "Confirm switch" }).click();
    await waitForGate(fixture.gateMarkerPath, "after_config_mutation_before_applied");
    expect((await fixture.inspect()).configText).toMatch(/^model_provider = "relay"/m);
    await dialog.getByRole("button", { name: "Cancel operation" }).click();
    const partial = page.getByRole("dialog", { name: "Operation result" });
    await expect(partial.getByText("Partially completed", { exact: true })).toBeVisible();
    await expect(partial.getByText("Review the operation again and retry.", { exact: true })).toBeVisible();
    await expect(dialog).toHaveCount(0);
    let state = await fixture.inspect();
    expect(state.configText).toMatch(/^model_provider = "relay"/m);
    expect(state.rollout.model_provider).toBe("legacy-provider");
    expect(state.sqlite.provider).toBe("legacy-provider");
    expect(await fixture.readJournals()).toEqual([]);

    const retryPlan = await prepareSyncDirect(page, "retry-after-partial-prepare");
    expect(retryPlan.ok, JSON.stringify(retryPlan)).toBe(true);
    const retry = await applySyncDirect(page, retryPlan.result.planId, "retry-after-partial-apply");
    expect(retry.ok, JSON.stringify(retry)).toBe(true);
    expect(retry.result.outcome).toBe("completed");
    state = await fixture.inspect();
    expect(state.rollout.model_provider).toBe("relay");
    expect(state.sqlite.provider).toBe("relay");
    expect(await fixture.readJournals()).toEqual([]);
  } finally {
    await electronApp?.close();
    await fixture.close();
  }
});

for (const scenario of [
  { point: "before_backup", operation: "sync", targetProvider: "openai", mutationExpected: false },
  { point: "after_config_mutation_before_applied", operation: "switch", targetProvider: "relay", mutationExpected: true },
  { point: "after_rollout_mutation_before_applied", operation: "sync", targetProvider: "openai", mutationExpected: true },
  { point: "after_sqlite_commit_before_ack", operation: "sync", targetProvider: "openai", mutationExpected: true }
]) {
  test(`Utility crash leaves no ordinary journal and retry converges: ${scenario.point}`, async () => {
    test.setTimeout(90_000);
    const fixture = await createDesktopSyncSwitchFixture();
    const baseline = await fixture.snapshotProtected();
    let electronApp;
    try {
      electronApp = await fixtureStep(`Launch crash fixture: ${scenario.point}`, () => launchDesktop(fixture, {
        CPS_DESKTOP_TEST_GATE: scenario.point,
        CPS_DESKTOP_TEST_GATE_FILE: fixture.gateMarkerPath
      }));
      const page = await fixtureStep("Open crash fixture window", () => electronApp.firstWindow());
      const dialog = await fixtureStep("Preview operation before crash", () => scenario.operation === "switch"
        ? openSwitchPlan(page, { provider: "relay", mode: "provider-default" })
        : openSyncPlan(page));
      await fixtureStep("Confirm operation before crash", () => dialog.getByRole("button", { name: scenario.operation === "switch" ? "Confirm switch" : "Confirm sync" }).click());
      await fixtureStep(`Reach crash gate: ${scenario.point}`, () => waitForGate(fixture.gateMarkerPath, scenario.point));
      const beforeCrash = await fixtureStep("Read runtime before crash", () => electronApp.evaluate(
        () => globalThis.__CPS_DESKTOP_TEST__.runtime()
      ));
      const crash = await fixtureStep("Crash Utility through test IPC", () => page.evaluate(() => window.codexProvider.test.crashRuntime()));
      expect(crash.crashed).toBe(true);
      await fixtureStep("Observe crash notification", () => expect(latestNotification(page)).toContainText(
        "The background service stopped unexpectedly. It will restart automatically when possible."
      ));
      // The renderer refreshes Status after the failed write and the query
      // layer may retry a transient first recovery probe. Assert the safety
      // boundary (the crashed generation is abandoned and a ready Runtime
      // preflights the journal), not the UI's exact number of read attempts.
      await fixtureStep("Wait for replacement runtime readiness", () => expect.poll(() => electronApp.evaluate(
        () => globalThis.__CPS_DESKTOP_TEST__.runtime()
      )).toMatchObject({ state: "ready" }));

      const recovered = await fixtureStep("Read replacement runtime", () => electronApp.evaluate(
        () => globalThis.__CPS_DESKTOP_TEST__.runtime()
      ));
      expect(recovered.generation).toBeGreaterThan(beforeCrash.generation);
      if (!scenario.mutationExpected) {
        expect((await fixture.snapshotProtected()).hash).toBe(baseline.hash);
      }

      const nextWrite = await fixtureStep("Prepare retry after crash", () => prepareSyncDirect(page, `crash-${scenario.point}`));
      expect(nextWrite.ok, JSON.stringify(nextWrite)).toBe(true);
      const converged = await fixtureStep("Apply retry after crash", () => applySyncDirect(
        page,
        nextWrite.result.planId,
        `crash-retry-${scenario.point}`
      ));
      expect(converged.ok, JSON.stringify(converged)).toBe(true);
      expect(converged.result.outcome).toBe("completed");
      const afterRecoveryProbe = await electronApp.evaluate(
        () => globalThis.__CPS_DESKTOP_TEST__.runtime()
      );
      expect(afterRecoveryProbe.state).toBe("ready");
      expect(afterRecoveryProbe.generation).toBeGreaterThanOrEqual(recovered.generation);
      expect(afterRecoveryProbe.recoveryBlocked).toBe(false);
      expect(await fixture.readJournals()).toEqual([]);
      const finalState = await fixture.inspect();
      expect(finalState.rollout.model_provider).toBe(scenario.targetProvider);
      expect(finalState.sqlite.provider).toBe(scenario.targetProvider);
    } finally {
      await electronApp?.close();
      await fixture.close();
    }
  });
}

test("Windows WSL UNC storage is rejected with every protected hash unchanged", async () => {
  const requireRealWsl = process.env.CPS_REQUIRE_REAL_WSL === "1";
  if (process.platform !== "win32" && requireRealWsl) {
    throw new Error("CPS_REQUIRE_REAL_WSL=1 requires a Windows test process with a real WSL distribution.");
  }
  test.skip(process.platform !== "win32", "WSL UNC is a Windows-only safety boundary.");
  const distro = await findWslDistro();
  if (!distro && requireRealWsl) {
    throw new Error("CPS_REQUIRE_REAL_WSL=1 but no runnable WSL distribution is available.");
  }
  test.skip(!distro, "No runnable WSL distribution is available on this machine.");
  const fixture = await createDesktopSyncSwitchFixture();
  let electronApp;
  let linuxRoot;
  try {
    linuxRoot = await runProcess("wsl.exe", [
      "-d",
      distro,
      "--",
      "mktemp",
      "-d",
      "/tmp/cps-c7-wsl-XXXXXX"
    ]);
    if (!/^\/tmp\/cps-c7-wsl-[A-Za-z0-9]+$/.test(linuxRoot)) {
      throw new Error("WSL fixture returned an unsafe temporary path.");
    }
    const uncRoot = `\\\\wsl.localhost\\${distro}${linuxRoot.replaceAll("/", "\\")}`;
    const wslStateDb = path.join(uncRoot, "state_5.sqlite");
    await fs.writeFile(wslStateDb, "C7 real WSL UNC unchanged marker\n", "utf8");
    const wslBefore = await fs.readFile(wslStateDb);
    const baseline = await fixture.snapshotProtected();
    electronApp = await launchDesktop(fixture, { CPS_DESKTOP_SQLITE_HOME: uncRoot });
    const page = await electronApp.firstWindow();

    await page.getByRole("button", { name: "Preview sync" }).click();
    await expect(latestNotification(page)).toContainText(
      "This chat index location cannot be used on the current platform."
    );
    await expect(page.getByRole("dialog", { name: "Confirm sync" })).toHaveCount(0);

    await page.getByLabel("Provider ID").fill("relay");
    await page.getByLabel("Model handling").selectOption("provider-default");
    await page.getByRole("button", { name: "Preview switch" }).click();
    await expect(latestNotification(page)).toContainText(
      "This chat index location cannot be used on the current platform."
    );

    expect((await fixture.snapshotProtected()).hash).toBe(baseline.hash);
    expect(await fs.readFile(wslStateDb)).toEqual(wslBefore);
    expect((await fixture.inspect()).backupIds).toHaveLength(0);
  } finally {
    await electronApp?.close();
    await fixture.close();
    if (linuxRoot && /^\/tmp\/cps-c7-wsl-[A-Za-z0-9]+$/.test(linuxRoot) && distro) {
      await runProcess("wsl.exe", ["-d", distro, "--", "rm", "-rf", "--", linuxRoot]);
    }
  }
});
