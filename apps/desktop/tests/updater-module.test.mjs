import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import test from "node:test";

test("production updater loads the real CommonJS package with automatic actions disabled", () => {
  // A fresh process keeps the Electron host mock and the dependency singleton
  // out of other tests. The electron-updater module itself is never mocked.
  const probe = String.raw`
    import assert from "node:assert/strict";
    import { EventEmitter } from "node:events";
    import { createRequire } from "node:module";
    import { fileURLToPath } from "node:url";

    const require = createRequire(process.argv[1]);
    const forbidden = () => { throw new Error("Unexpected host I/O in updater module test"); };
    const app = Object.assign(new EventEmitter(), {
      isPackaged: true,
      getVersion: () => "1.0.0",
      getName: () => "Updater module fixture",
      whenReady: async () => {},
      getPath: forbidden,
      getAppPath: forbidden,
      quit: forbidden,
      relaunch: forbidden
    });
    const electronId = require.resolve("electron");
    require.cache[electronId] = {
      id: electronId, filename: electronId, loaded: true,
      exports: { app, autoUpdater: new EventEmitter(), net: { request: forbidden }, session: { fromPartition: forbidden } }
    };
    // Linux package-type detection stays inside the repository test directory.
    process.resourcesPath = fileURLToPath(new URL("./fixtures/", process.argv[1]));
    const { createProductionUpdaterPort } = await import(new URL("../dist/main/updater.js", process.argv[1]));
    const updaterId = require.resolve("electron-updater");
    assert.equal(require.cache[updaterId], undefined, "updater must remain lazy before port creation");
    const port = await createProductionUpdaterPort({ allowPrerelease: false });
    const { autoUpdater, AppUpdater, NoOpLogger } = require("electron-updater");
    assert.ok(autoUpdater instanceof AppUpdater, "the actual dependency must supply the updater");
    assert.equal(autoUpdater.autoDownload, false);
    assert.equal(autoUpdater.autoInstallOnAppQuit, false);
    assert.equal(autoUpdater.allowPrerelease, false);
    assert.ok(autoUpdater.logger instanceof NoOpLogger);
    for (const method of ["checkForUpdates", "downloadUpdate", "quitAndInstall"]) {
      assert.equal(typeof port[method], "function");
    }
    for (const event of ["checking-for-update", "update-available", "update-not-available", "download-progress", "update-downloaded", "error"]) {
      const received = [];
      const listener = value => received.push(value);
      const baseline = autoUpdater.listenerCount(event);
      const payload = { version: "1.0.1", percent: 50 };
      port.on(event, listener);
      assert.equal(autoUpdater.listenerCount(event), baseline + 1);
      autoUpdater.emit(event, payload);
      assert.deepEqual(received, [payload]);
      port.off(event, listener);
      assert.equal(autoUpdater.listenerCount(event), baseline);
      autoUpdater.emit(event, payload);
      assert.deepEqual(received, [payload]);
    }
    await createProductionUpdaterPort({ allowPrerelease: true });
    assert.equal(autoUpdater.allowPrerelease, true);
    assert.equal(autoUpdater.autoDownload, false);
    assert.equal(autoUpdater.autoInstallOnAppQuit, false);
    process.stdout.write("real updater module passed\n");
  `;
  const result = spawnSync(process.execPath, ["--input-type=module", "--eval", probe, import.meta.url], {
    encoding: "utf8",
    timeout: 15_000,
    windowsHide: true
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr || result.stdout);
  assert.match(result.stdout, /real updater module passed/);
});
