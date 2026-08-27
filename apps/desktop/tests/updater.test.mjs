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
    waitForWrites: async () => {}
  };
  const controller = new DesktopUpdateController({
    isPackaged: true,
    platform: "win32",
    arch: "x64",
    appVersion: "1.0.0",
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
    state: "idle",
    installAllowed: false
  });
  assert.deepEqual(await controller.check(), {
    schemaVersion: 2,
    state: "available",
    installAllowed: false,
    version: "1.0.1"
  });
  const downloaded = await controller.download();
  assert.deepEqual(downloaded, {
    schemaVersion: 2,
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
    state: "error",
    installAllowed: false,
    reason: "check-failed"
  });
  assert.equal(invalid.port.downloads, 0);
});

test("updater stays disabled before a packaged, supported and configured release", async () => {
  const created = [];
  const controller = new DesktopUpdateController({
    isPackaged: false,
    platform: "win32",
    arch: "x64",
    appVersion: "0.0.0",
    configured: false,
    supervisor: {
      snapshot: { recoveryBlocked: false, writeInProgress: false },
      tryBeginRestartInstall: () => null
    },
    hasActiveWatches: () => false,
    verifyRecoveryState: async () => "clear",
    createPort: async () => { created.push(true); return new FakeUpdaterPort(); }
  });
  assert.deepEqual(await controller.check(), {
    schemaVersion: 2,
    state: "disabled",
    installAllowed: false,
    reason: "not-packaged"
  });
  assert.equal(created.length, 0);
});
