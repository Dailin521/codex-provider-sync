import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";

import { DesktopUpdateController } from "../dist/main/updater.js";

class FakeUpdaterPort extends EventEmitter {
  checks = 0;
  downloads = 0;
  installs = 0;
  checkResult = { updateInfo: { version: "1.0.1" } };
  checkError = null;
  downloadError = null;
  installError = null;

  async checkForUpdates() {
    this.checks += 1;
    this.emit("checking-for-update");
    if (this.checkError) throw this.checkError;
    if (this.checkResult?.updateInfo?.version) {
      this.emit("update-available", this.checkResult.updateInfo);
    } else {
      this.emit("update-not-available", {});
    }
    return this.checkResult;
  }

  async downloadUpdate() {
    this.downloads += 1;
    if (this.downloadError) throw this.downloadError;
    this.emit("download-progress", { percent: 51.4, transferred: 123, total: 240 });
    this.emit("update-downloaded", { version: "1.0.1", files: [{ url: "secret" }] });
    return ["C:\\private\\update.exe"];
  }

  quitAndInstall() {
    if (this.installError) throw this.installError;
    this.installs += 1;
  }
}

function fixture(overrides = {}) {
  const port = new FakeUpdaterPort();
  const snapshot = { recoveryBlocked: false, writeInProgress: false };
  const state = {
    watches: false,
    verification: "clear",
    beforeInstall: 0,
    gateClosed: false,
    gateReleases: 0,
    watchVerification: "active",
    watchVerificationCalls: 0,
    waitForWrites: async () => {}
  };
  const controller = new DesktopUpdateController({
    isPackaged: true,
    platform: "win32",
    arch: "x64",
    appVersion: "1.0.0",
    releaseAuthorized: true,
    configured: true,
    supervisor: {
      snapshot,
      tryBeginRestartInstall() {
        if (state.gateClosed) return null;
        state.gateClosed = true;
        let released = false;
        return {
          waitForWrites: () => state.waitForWrites(),
          release() {
            if (released) return;
            released = true;
            state.gateClosed = false;
            state.gateReleases += 1;
          }
        };
      }
    },
    hasActiveWatches: () => state.watches,
    verifyNoActiveWatches: async () => {
      state.watchVerificationCalls += 1;
      if (state.watchVerification !== "clear") return false;
      state.watches = false;
      return true;
    },
    verifyRecoveryState: async () => state.verification,
    beforeInstall: async () => { state.beforeInstall += 1; },
    createPort: async () => port,
    ...overrides
  });
  return { controller, port, snapshot, state };
}

test("updater exposes a redacted Main-only check, download and install state machine", async () => {
  const { controller, port, state } = fixture();
  assert.deepEqual(controller.status, {
    schemaVersion: 2,
    currentVersion: "1.0.0",
    state: "idle",
    installAllowed: false
  });
  assert.deepEqual(await controller.check(), {
    schemaVersion: 2,
    currentVersion: "1.0.0",
    state: "available",
    installAllowed: false,
    version: "1.0.1"
  });
  const downloaded = await controller.download();
  assert.deepEqual(downloaded, {
    schemaVersion: 2,
    currentVersion: "1.0.0",
    state: "downloaded",
    installAllowed: true,
    version: "1.0.1",
    progressPercent: 100
  });
  assert.equal(JSON.stringify(downloaded).includes("private"), false);
  assert.equal(JSON.stringify(downloaded).includes("url"), false);
  const installing = await controller.install();
  assert.equal(installing.state, "installing");
  assert.equal(installing.installAllowed, false);
  assert.equal(controller.restartPending, true);
  assert.equal(port.installs, 1);
  assert.equal(state.beforeInstall, 1);
  controller.dispose();
  assert.equal(port.listenerCount("update-available"), 0);
});

test("updater blocks install for writes, Watch, recovery and unverifiable preflight", async () => {
  for (const scenario of ["write", "watch", "blocked", "unverifiable"]) {
    const { controller, port, snapshot, state } = fixture();
    await controller.check();
    await controller.download();
    if (scenario === "write") snapshot.writeInProgress = true;
    if (scenario === "watch") state.watches = true;
    if (scenario === "blocked") state.verification = "blocked";
    if (scenario === "unverifiable") state.verification = "unverifiable";
    const result = await controller.install();
    assert.equal(result.state, "downloaded", scenario);
    assert.equal(result.installAllowed, false, scenario);
    assert.equal(result.installBlockedReason, scenario === "write"
      ? "write-in-progress"
      : scenario === "watch"
        ? "watch-active"
        : scenario === "blocked"
          ? "pending-recovery"
          : "recovery-unverified");
    assert.equal(controller.restartPending, false, scenario);
    assert.equal(port.installs, 0, scenario);
    controller.dispose();
  }
});

test("updater closes admission, drains an already admitted Watch, and reopens without installing", async () => {
  const { controller, port, snapshot, state } = fixture();
  await controller.check();
  await controller.download();
  let releaseWrite;
  state.waitForWrites = () => new Promise((resolve) => { releaseWrite = resolve; });
  snapshot.writeInProgress = true;
  const installing = controller.install();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(controller.restartPending, true);
  assert.equal(state.gateClosed, true);
  assert.equal(port.installs, 0);
  state.watches = true;
  snapshot.writeInProgress = false;
  releaseWrite();
  const result = await installing;
  assert.equal(result.state, "downloaded");
  assert.equal(result.installBlockedReason, "watch-active");
  assert.equal(port.installs, 0);
  assert.equal(controller.restartPending, false);
  assert.equal(state.gateClosed, false);
  assert.equal(state.gateReleases, 1);
});

test("updater rechecks an autonomously stopped Watch after closing restart admission", async () => {
  const { controller, port, state } = fixture();
  await controller.check();
  await controller.download();
  state.watches = true;
  state.watchVerification = "clear";
  const result = await controller.install();
  assert.equal(result.state, "installing");
  assert.equal(state.watchVerificationCalls, 1);
  assert.equal(state.watches, false);
  assert.equal(port.installs, 1);
});

test("updater reopens write admission when the installer fails synchronously", async () => {
  const { controller, port, state } = fixture();
  await controller.check();
  await controller.download();
  port.installError = new Error("installer failed");
  const result = await controller.install();
  assert.equal(result.state, "error");
  assert.equal(result.reason, "install-failed");
  assert.equal(controller.restartPending, false);
  assert.equal(state.gateClosed, false);
  assert.equal(state.gateReleases, 1);
});

test("updater fails closed without leaking raw errors or allowing invalid event data", async () => {
  const checkFailure = fixture();
  checkFailure.port.checkError = Object.assign(new Error("https://token.example/private"), {
    path: "C:\\secret"
  });
  assert.deepEqual(await checkFailure.controller.check(), {
    schemaVersion: 2,
    currentVersion: "1.0.0",
    state: "error",
    installAllowed: false,
    reason: "check-failed"
  });

  const invalid = fixture();
  invalid.port.checkForUpdates = async function () {
    this.emit("update-available", { version: "bad version", releaseNotes: "secret" });
    return { updateInfo: { version: "bad version" } };
  };
  assert.deepEqual(await invalid.controller.check(), {
    schemaVersion: 2,
    currentVersion: "1.0.0",
    state: "error",
    installAllowed: false,
    reason: "check-failed"
  });
  assert.equal(invalid.port.downloads, 0);
});

test("updater stays disabled before a packaged, authorized and configured release", async () => {
  const created = [];
  const controller = new DesktopUpdateController({
    isPackaged: false,
    platform: "win32",
    arch: "x64",
    appVersion: "0.0.0",
    releaseAuthorized: false,
    configured: false,
    supervisor: {
      snapshot: { recoveryBlocked: false, writeInProgress: false },
      tryBeginRestartInstall: () => null
    },
    hasActiveWatches: () => false,
    verifyNoActiveWatches: async () => true,
    verifyRecoveryState: async () => "clear",
    createPort: async () => { created.push(true); return new FakeUpdaterPort(); }
  });
  assert.deepEqual(await controller.check(), {
    schemaVersion: 2,
    currentVersion: "0.0.0",
    state: "disabled",
    installAllowed: false,
    reason: "not-packaged"
  });
  assert.equal(created.length, 0);
});

test("unsigned candidate never creates an updater port or schedules network work", async () => {
  const created = [];
  const controller = new DesktopUpdateController({
    isPackaged: true,
    platform: "win32",
    arch: "x64",
    appVersion: "1.0.0-rc.205",
    releaseAuthorized: false,
    configured: true,
    supervisor: {
      snapshot: { recoveryBlocked: false, writeInProgress: false },
      tryBeginRestartInstall: () => null
    },
    hasActiveWatches: () => false,
    verifyNoActiveWatches: async () => true,
    verifyRecoveryState: async () => "clear",
    createPort: async () => { created.push(true); return new FakeUpdaterPort(); }
  });
  assert.deepEqual(controller.status, {
    schemaVersion: 2,
    currentVersion: "1.0.0-rc.205",
    state: "disabled",
    installAllowed: false,
    reason: "not-authorized"
  });
  controller.scheduleInitialCheck(0);
  await new Promise((resolve) => setTimeout(resolve, 10));
  await controller.check();
  await controller.download();
  await controller.install();
  assert.equal(created.length, 0);
  controller.dispose();
});

test("portable manual updates check on demand, open official downloads and never install", async () => {
  let checks = 0;
  let pages = 0;
  const published = [];
  const { controller, port, state } = fixture({
    releaseAuthorized: false,
    manualUpdates: {
      check: async () => { checks++; return { version: "1.1.0" }; },
      openDownloadPage: async () => { pages++; }
    },
    onStatus: status => published.push(status)
  });
  assert.equal(controller.status.mode, "manual");
  assert.equal(controller.status.state, "idle");
  controller.scheduleInitialCheck(0);
  await new Promise(resolve => setTimeout(resolve, 10));
  assert.equal(checks, 0);
  assert.equal((await controller.check()).version, "1.1.0");
  assert.equal(checks, 1);
  assert.equal((await controller.download()).state, "available");
  assert.equal(pages, 1);
  assert.equal((await controller.install()).installAllowed, false);
  assert.equal(port.checks + port.downloads + port.installs, 0);
  assert.equal(state.gateClosed, false);
  assert.ok(published.some(status => status.state === "checking"));
  controller.dispose();
});

test("manual check failure is retryable and status observers cannot break updates", async () => {
  let attempts = 0;
  const { controller, port } = fixture({
    manualUpdates: {
      force: true,
      check: async () => { if (++attempts === 1) throw new Error("private URL"); return null; },
      openDownloadPage: async () => { throw new Error("must not open"); }
    },
    onStatus: () => { throw new Error("broken renderer"); }
  });
  assert.equal((await controller.check()).reason, "check-failed");
  assert.equal((await controller.check()).state, "not-available");
  await controller.download();
  assert.equal(port.checks, 0);
  controller.dispose();
});

for (const [platform, arch] of [["win32", "x64"], ["darwin", "x64"], ["darwin", "arm64"], ["linux", "x64"]]) {
  test(`unreleased packaged ${platform}/${arch} falls back to manual checks without force`, async () => {
    let checks = 0;
    const { controller, port } = fixture({
      platform, arch, releaseAuthorized: false, configured: true,
      manualUpdates: { force: false, check: async () => { checks++; return null; }, openDownloadPage: async () => {} }
    });
    assert.equal(controller.status.mode, "manual");
    await controller.check();
    assert.equal(checks, 1);
    await controller.install();
    assert.equal(controller.status.installAllowed, false);
    assert.equal(port.checks + port.downloads + port.installs, 0);
    controller.dispose();
  });
}

test("download failures clear version and report valid error state; progress is pushed", async () => {
  const statuses = [];
  const { controller, port } = fixture({ onStatus: status => statuses.push(status) });
  await controller.check();
  await controller.download();
  assert.ok(statuses.some(status => status.state === "downloading" && status.progressPercent === 51));
  assert.ok(statuses.some(status => status.state === "downloaded" && status.installAllowed));
  controller.dispose();
  const failed = fixture();
  await failed.controller.check();
  failed.port.downloadError = new Error("network offline");
  const result = await failed.controller.download();
  assert.equal(result.reason, "download-failed");
  assert.equal(result.version, undefined);
  assert.equal(result.progressPercent, undefined);
  failed.controller.dispose();
});

test("daily startup check runs once, notifies only for a newer version and leaves manual check available", async () => {
  let claimed = false;
  let checks = 0;
  const notices = [];
  const callbacks = [];
  const options = {
    releaseAuthorized: false,
    claimStartupCheck: async () => { if (claimed) return false; claimed = true; return true; },
    onStartupUpdateAvailable: async status => { notices.push(status.version); },
    manualUpdates: { check: async () => { checks++; return { version: "1.1.0" }; }, openDownloadPage: async () => {} },
    setTimeoutImpl: callback => { callbacks.push(callback); return { unref() {} }; }
  };
  const first = fixture(options).controller;
  first.scheduleInitialCheck();
  first.scheduleInitialCheck();
  assert.equal(callbacks.length, 1);
  callbacks.shift()();
  for (let i = 0; i < 5; i++) await new Promise(resolve => setImmediate(resolve));
  assert.equal(checks, 1);
  assert.deepEqual(notices, ["1.1.0"]);
  first.scheduleInitialCheck();
  assert.equal(callbacks.length, 0);
  const second = fixture(options).controller;
  second.scheduleInitialCheck();
  callbacks.shift()();
  for (let i = 0; i < 5; i++) await new Promise(resolve => setImmediate(resolve));
  assert.equal(checks, 1);
  await second.check();
  assert.equal(checks, 2);
  assert.deepEqual(notices, ["1.1.0"], "manual check must not raise a startup popup");
  first.dispose(); second.dispose();
});

test("startup checks remain silent on failure/no update and do not retry during the run", async () => {
  for (const fail of [false, true]) {
    let run;
    let notices = 0;
    let checks = 0;
    const { controller } = fixture({
      releaseAuthorized: false,
      claimStartupCheck: async () => true,
      onStartupUpdateAvailable: async () => { notices++; },
      manualUpdates: { check: async () => { checks++; if (fail) throw new Error("offline"); return null; }, openDownloadPage: async () => {} },
      setTimeoutImpl: callback => { run = callback; return { unref() {} }; }
    });
    controller.scheduleInitialCheck();
    run();
    for (let i = 0; i < 5; i++) await new Promise(resolve => setImmediate(resolve));
    controller.scheduleInitialCheck();
    assert.equal(checks, 1);
    assert.equal(notices, 0);
    assert.equal(controller.status.state, fail ? "error" : "not-available");
    controller.dispose();
  }
});

test("ignored version suppresses only startup notice, still allows manual download; next version notifies", async () => {
  let ignoredVersion = null;
  let offeredVersion = "1.0.1";
  const notices = [];
  const timers = [];
  const options = () => ({
    ignoredVersion,
    saveIgnoredVersion: async version => { ignoredVersion = version; },
    claimStartupCheck: async () => true,
    onStartupUpdateAvailable: async status => { notices.push(status.version); },
    setTimeoutImpl: callback => { timers.push(callback); return { unref() {} }; }
  });
  const first = fixture(options());
  await first.controller.check();
  assert.equal((await first.controller.setReminder({ schemaVersion: 1, version: "1.0.1", ignored: true })).reminderIgnored, true);
  first.controller.dispose();
  const second = fixture(options());
  second.controller.scheduleInitialCheck(); timers.shift()();
  for (let i = 0; i < 8; i++) await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(notices, []);
  assert.equal((await second.controller.check()).reminderIgnored, true);
  assert.equal((await second.controller.download()).state, "downloaded");
  assert.equal(second.port.installs, 0);
  assert.equal((await second.controller.setReminder({ schemaVersion: 1, version: "1.0.1", ignored: false })).reminderIgnored, undefined);
  assert.equal(ignoredVersion, null);
  second.controller.dispose();
  ignoredVersion = "1.0.1";
  offeredVersion = "1.0.2";
  const third = fixture(options());
  third.port.checkResult = { updateInfo: { version: offeredVersion } };
  third.controller.scheduleInitialCheck(); timers.shift()();
  for (let i = 0; i < 8; i++) await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(notices, ["1.0.2"]);
  assert.equal(third.controller.status.reminderIgnored, undefined);
  third.controller.dispose();
});

test("reminder writes reject stale version and failures do not mute or poison subsequent requests", async () => {
  let fail = true;
  const saved = [];
  const { controller, port } = fixture({ saveIgnoredVersion: async version => {
    if (fail) throw new Error("fixture persistence failure");
    saved.push(version);
  } });
  const input = { schemaVersion: 1, version: "1.0.1", ignored: true };
  await assert.rejects(controller.setReminder(input));
  await controller.check();
  await assert.rejects(controller.setReminder({ ...input, version: "1.0.2" }));
  await assert.rejects(controller.setReminder(input));
  assert.equal(controller.status.reminderIgnored, undefined);
  fail = false;
  await Promise.all([controller.setReminder(input), controller.setReminder({ ...input, ignored: false })]);
  assert.deepEqual(saved, ["1.0.1", null]);
  assert.equal(controller.status.reminderIgnored, undefined);
  assert.equal(port.downloads + port.installs, 0);
  controller.dispose();
});

for (const asynchronous of [false, true]) {
  test(`installer error event without throw releases restart gate (async=${asynchronous})`, async () => {
    const { controller, port, state } = fixture();
    await controller.check();
    await controller.download();
    port.quitAndInstall = () => {
      const fail = () => port.emit("error", new Error("fixture installer failure"));
      if (asynchronous) setImmediate(fail);
      else fail();
    };
    await controller.install();
    if (asynchronous) await new Promise(resolve => setImmediate(resolve));
    assert.equal(controller.status.state, "error");
    assert.equal(controller.status.reason, "install-failed");
    assert.equal(controller.restartPending, false);
    assert.equal(state.gateClosed, false);
    assert.equal((await controller.check()).state, "available");
    controller.dispose();
  });
}
