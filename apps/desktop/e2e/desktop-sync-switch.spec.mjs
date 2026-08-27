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
  await page.getByRole("button", { name: "Sync" }).click();
  await page.getByRole("button", { name: "Prepare sync" }).click();
  const dialog = page.getByRole("dialog", { name: "Review plan" });
  await expect(dialog).toBeVisible();
  return dialog;
}

async function openSwitchPlan(page, { provider = "relay", mode = "provider-default", model } = {}) {
  await page.getByRole("button", { name: "Switch Provider" }).click();
  await page.getByLabel("Provider ID").fill(provider);
  await page.getByLabel("Model handling").selectOption(mode);
  if (mode === "explicit") await page.getByLabel("Model name").fill(model);
  await page.getByRole("button", { name: "Prepare switch" }).click();
  const dialog = page.getByRole("dialog", { name: "Review plan" });
  await expect(dialog).toBeVisible();
  return dialog;
}

async function waitForGate(markerPath, expectedPoint) {
  await expect.poll(async () => {
    try {
      return JSON.parse(await fs.readFile(markerPath, "utf8")).point;
    } catch (error) {
      if (error?.code === "ENOENT") return null;
      throw error;
    }
  }).toBe(expectedPoint);
}

function notificationWithCode(page, code) {
  return page.getByRole("listitem").filter({ hasText: `(${code})` }).last();
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

async function lockRolloutFile(filePath) {
  const script = `
& {
  param([string]$path)
  $stream = [System.IO.File]::Open($path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
  try {
    Write-Output 'locked'
    [Console]::Out.Flush()
    Start-Sleep -Seconds 30
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
  ], { stdio: ["ignore", "pipe", "pipe"] });
  await new Promise((resolve, reject) => {
    let output = "";
    let settled = false;
    child.stdout.on("data", (chunk) => {
      output += chunk.toString("utf8");
      if (!settled && output.includes("locked")) {
        settled = true;
        resolve();
      }
    });
    child.once("error", (error) => {
      if (!settled) { settled = true; reject(error); }
    });
    child.once("exit", (code, signal) => {
      if (!settled) {
        settled = true;
        reject(new Error(`Rollout lock exited before ready (${code ?? "null"}/${signal ?? "null"}).`));
      }
    });
  });
  return child;
}

async function releaseChild(child) {
  if (!child || child.exitCode !== null) return;
  const exited = new Promise((resolve) => child.once("exit", resolve));
  child.kill();
  await exited;
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

async function confirmPlan(page, returnFocus) {
  const dialog = page.getByRole("dialog", { name: "Review plan" });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByText("Target", { exact: true })).toBeVisible();
  await expect(dialog.getByText("Impact", { exact: true })).toBeVisible();
  await dialog.getByRole("button", { name: "Confirm and apply" }).click();
  await expect(page.getByText("Operation completed.", { exact: true })).toBeVisible();
  await expect(dialog).toHaveCount(0);
  await page.getByRole("dialog", { name: "Operation result" }).getByRole("button", { name: "Close" }).last().click();
  if (returnFocus) await expect(returnFocus).toBeFocused();
}

async function switchProvider(page, { provider, mode, model }) {
  await page.getByRole("button", { name: "Switch Provider" }).click();
  await page.getByLabel("Provider ID").fill(provider);
  await page.getByLabel("Model handling").selectOption(mode);
  if (mode === "explicit") await page.getByLabel("Model name").fill(model);
  const prepare = page.getByRole("button", { name: "Prepare switch" });
  await prepare.click();
  await confirmPlan(page, prepare);
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

    await page.getByRole("button", { name: "Sync" }).click();
    const prepareSync = page.getByRole("button", { name: "Prepare sync" });
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
    expect(state.turnContext.model).toBe("explicit-model");
    expect(state.turnContext.collaboration_mode.settings.model).toBe("explicit-model");
    expect(state.sqlite.provider).toBe("relay");
    expect(state.sqlite.model).toBe("explicit-model");
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
    const prepareRestore = page.getByRole("button", { name: "Prepare restore" });
    await prepareRestore.click();
    await confirmPlan(page, prepareRestore);
    expect((await fixture.snapshotTargets()).hash).toBe(baseline.hash);

    await page.getByLabel("Keep newest backups").fill("2");
    await page.getByRole("button", { name: "Prune older backups" }).click();
    await expect(page.getByText("Operation completed.", { exact: true }).last()).toBeVisible();

    await page.getByRole("button", { name: "Settings" }).click();
    await page.getByRole("button", { name: "Start watch" }).click();
    await expect(page.getByRole("button", { name: "Stop watch" })).toBeVisible();
    await page.getByRole("button", { name: "Stop watch" }).click();
    await expect(page.getByRole("button", { name: "Start watch" })).toBeVisible();
    await expect(page.getByText("Updates", { exact: true })).toBeVisible();
    await expect(page.getByText("Update checks are available only in a packaged build.", { exact: true })).toBeVisible();

    await page.getByRole("button", { name: "Diagnostics" }).click();
    await page.getByRole("button", { name: "Export redacted bundle" }).click();
    await expect(page.getByText("Redacted diagnostics bundle created.", { exact: true })).toBeVisible();
    const diagnostics = await fs.readFile(diagnosticsTarget);
    expect(diagnostics.toString("utf8")).not.toContain(fixture.codexHome);
    expect(diagnostics.toString("utf8")).not.toMatch(/auth\.json|encrypted_content|message body/i);

    const eventsAfterRestore = await page.evaluate(() => globalThis.__c7OperationEvents);
    expect(eventsAfterRestore.filter((event) => event.event === "operation-started")).toHaveLength(5);
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
    await dialog.getByRole("button", { name: "Confirm and apply" }).click();
    await expect(notificationWithCode(page, "STALE_STATE")).toContainText(
      "The protected state changed. Prepare the operation again."
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
    await page.getByRole("button", { name: "Switch Provider" }).click();
    await page.getByLabel("Provider ID").fill("missing-provider");
    await page.getByLabel("Model handling").selectOption("provider-default");
    const prepareSwitch = page.getByRole("button", { name: "Prepare switch" });
    await expect(prepareSwitch).toBeEnabled();
    await prepareSwitch.click();
    await expect(notificationWithCode(page, "INVALID_INPUT")).toBeVisible();
    await expect(page.getByRole("dialog", { name: "Review plan" })).toHaveCount(0);
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
    await dialog.getByRole("button", { name: "Confirm and apply" }).click();
    await expect(notificationWithCode(page, "SQLITE_BUSY")).toContainText(
      "The state database is busy. Close Codex processes and retry."
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
    lockProcess = await lockRolloutFile(fixture.rolloutPath);
    electronApp = await launchDesktop(fixture);
    const page = await electronApp.firstWindow();
    const dialog = await openSyncPlan(page);
    await dialog.getByRole("button", { name: "Confirm and apply" }).click();
    await expect(page.getByText(
      "Completed with locked rollout files skipped.",
      { exact: true }
    )).toBeVisible();
    await releaseChild(lockProcess);
    lockProcess = undefined;
    const state = await fixture.inspect();
    expect(await fs.readFile(fixture.rolloutPath)).toEqual(rolloutBefore);
    expect(state.rollout.model_provider).toBe("legacy-provider");
    expect(state.sqlite.provider).toBe("openai");
    expect(state.backupIds).toHaveLength(1);
  } finally {
    await releaseChild(lockProcess);
    await electronApp?.close();
    await fixture.close();
  }
});

test("Electron Cancel before backup leaves every protected target unchanged", async () => {
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
    await dialog.getByRole("button", { name: "Confirm and apply" }).click();
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

test("Electron Cancel after config mutation waits for a durable rollback terminal", async () => {
  const fixture = await createDesktopSyncSwitchFixture();
  const baseline = await fixture.snapshotTargets();
  let electronApp;
  try {
    electronApp = await launchDesktop(fixture, {
      CPS_DESKTOP_TEST_GATE: "after_config_mutation_before_applied",
      CPS_DESKTOP_TEST_GATE_FILE: fixture.gateMarkerPath
    });
    const page = await electronApp.firstWindow();
    const dialog = await openSwitchPlan(page, { provider: "relay", mode: "provider-default" });
    await dialog.getByRole("button", { name: "Confirm and apply" }).click();
    await waitForGate(fixture.gateMarkerPath, "after_config_mutation_before_applied");
    expect((await fixture.inspect()).configText).toMatch(/^model_provider = "relay"/m);
    await dialog.getByRole("button", { name: "Cancel operation" }).click();
    await expect(notificationWithCode(page, "SYNC_FAILED_ROLLED_BACK")).toContainText(
      "The operation failed and its changes were rolled back."
    );
    await expect(dialog).toHaveCount(0);
    expect((await fixture.snapshotTargets()).hash).toBe(baseline.hash);
    expect(await fixture.readJournals()).toEqual([
      expect.objectContaining({ state: "rolledBack", terminal: true, invalidTail: false })
    ]);
  } finally {
    await electronApp?.close();
    await fixture.close();
  }
});

for (const scenario of [
  { point: "before_backup", operation: "sync", recoveryBlocked: false, journalState: null },
  { point: "after_config_mutation_before_applied", operation: "switch", recoveryBlocked: true, journalState: "applying" },
  { point: "after_rollout_mutation_before_applied", operation: "sync", recoveryBlocked: true, journalState: "applying" },
  { point: "after_sqlite_commit_before_ack", operation: "sync", recoveryBlocked: true, journalState: "applied" },
  { point: "after_transaction_journal_commit_before_ack", operation: "sync", recoveryBlocked: false, journalState: "committed" },
  { point: "after_transaction_commit", operation: "sync", recoveryBlocked: false, journalState: "committed" }
]) {
  test(`Utility crash matrix: ${scenario.point}`, async () => {
    const fixture = await createDesktopSyncSwitchFixture();
    const baseline = await fixture.snapshotProtected();
    let electronApp;
    try {
      electronApp = await launchDesktop(fixture, {
        CPS_DESKTOP_TEST_GATE: scenario.point,
        CPS_DESKTOP_TEST_GATE_FILE: fixture.gateMarkerPath
      });
      const page = await electronApp.firstWindow();
      const dialog = scenario.operation === "switch"
        ? await openSwitchPlan(page, { provider: "relay", mode: "provider-default" })
        : await openSyncPlan(page);
      await dialog.getByRole("button", { name: "Confirm and apply" }).click();
      await waitForGate(fixture.gateMarkerPath, scenario.point);
      const beforeCrash = await electronApp.evaluate(
        () => globalThis.__CPS_DESKTOP_TEST__.runtime()
      );
      expect((await page.evaluate(() => window.codexProvider.test.crashRuntime())).crashed).toBe(true);
      await expect(notificationWithCode(page, "CORE_RUNTIME_CRASHED")).toContainText(
        "The Core runtime stopped unexpectedly."
      );
      // The renderer's post-write status refresh is itself the next request,
      // so it legitimately performs the supervisor's one allowed restart.
      // Assert that exact generation transition instead of racing the brief
      // intermediate `crashed` state.
      await expect.poll(() => electronApp.evaluate(
        () => globalThis.__CPS_DESKTOP_TEST__.runtime()
      )).toMatchObject({ state: "ready", generation: beforeCrash.generation + 1 });

      const nextWrite = await prepareSyncDirect(page, `crash-${scenario.point}`);
      expect(nextWrite.ok, JSON.stringify(nextWrite)).toBe(!scenario.recoveryBlocked);
      if (scenario.recoveryBlocked) expect(nextWrite.error.code).toBe("PENDING_TRANSACTION");
      const afterRestart = await electronApp.evaluate(
        () => globalThis.__CPS_DESKTOP_TEST__.runtime()
      );
      expect(afterRestart.generation).toBe(beforeCrash.generation + 1);
      expect(afterRestart.recoveryBlocked).toBe(scenario.recoveryBlocked);
      const journals = await fixture.readJournals();
      if (scenario.journalState === null) {
        expect(journals).toEqual([]);
        expect((await fixture.snapshotProtected()).hash).toBe(baseline.hash);
      } else {
        expect(journals).toEqual([
          expect.objectContaining({
            state: scenario.journalState,
            terminal: !scenario.recoveryBlocked,
            invalidTail: false
          })
        ]);
      }
    } finally {
      await electronApp?.close();
      await fixture.close();
    }
  });
}

test("Windows WSL UNC storage is rejected with every protected hash unchanged", async () => {
  test.skip(process.platform !== "win32", "WSL UNC is a Windows-only safety boundary.");
  const distro = await findWslDistro();
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

    await page.getByRole("button", { name: "Sync" }).click();
    await page.getByRole("button", { name: "Prepare sync" }).click();
    await expect(notificationWithCode(page, "SQLITE_UNSUPPORTED_PATH")).toContainText(
      "The selected SQLite path is not supported by this runtime."
    );
    await expect(page.getByRole("dialog", { name: "Review plan" })).toHaveCount(0);

    await page.getByRole("button", { name: "Switch Provider" }).click();
    await page.getByLabel("Provider ID").fill("relay");
    await page.getByLabel("Model handling").selectOption("provider-default");
    await page.getByRole("button", { name: "Prepare switch" }).click();
    await expect(notificationWithCode(page, "SQLITE_UNSUPPORTED_PATH")).toContainText(
      "The selected SQLite path is not supported by this runtime."
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
