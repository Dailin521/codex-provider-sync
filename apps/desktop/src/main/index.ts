import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  app,
  BrowserWindow,
  ipcMain,
  protocol,
  session
} from "electron";

import {
  DESKTOP_APP_ORIGIN
} from "../shared/constants.js";
import { createElectronUtilitySpawner } from "./electron-utility.js";
import { registerDesktopIpc } from "./ipc-router.js";
import { DesktopProfileRepository } from "../profiles/repository.js";
import { CoreRuntimeSupervisor } from "./runtime-supervisor.js";
import { createSecureWebPreferences } from "./security-policy.js";
import {
  registerDesktopProtocol,
  registerDesktopScheme,
  installDesktopSecurity
} from "./security.js";

const currentDirectory = path.dirname(fileURLToPath(import.meta.url));
const preloadPath = path.resolve(currentDirectory, "../preload/index.cjs");
const runtimePath = path.resolve(currentDirectory, "runtime.js");
const rendererRoot = path.resolve(currentDirectory, "../renderer");
const e2eEnabled = __CPS_DESKTOP_TEST_BUILD__ && process.env.CPS_DESKTOP_E2E === "1";

if (e2eEnabled && process.env.CPS_DESKTOP_USER_DATA) {
  app.setPath("userData", path.resolve(process.env.CPS_DESKTOP_USER_DATA));
}

registerDesktopScheme(protocol);

if (!app.requestSingleInstanceLock()) {
  app.quit();
} else {
  let mainWindow: BrowserWindow | null = null;
  let supervisor: CoreRuntimeSupervisor | null = null;
  let removeIpc: (() => void) | null = null;
  let removeTestIpc: (() => void) | null = null;
  let removeSecurity: (() => void) | null = null;
  let quitting = false;

  const defaultCodexHome = path.resolve(
    (e2eEnabled ? process.env.CPS_DESKTOP_CODEX_HOME : undefined)
      ?? process.env.CODEX_HOME
      ?? path.join(os.homedir(), ".codex")
  );
  const defaultSqliteHome = e2eEnabled && process.env.CPS_DESKTOP_SQLITE_HOME
    ? path.resolve(process.env.CPS_DESKTOP_SQLITE_HOME)
    : undefined;
  const profileFile = path.join(app.getPath("userData"), "profiles.v1.json");
  const profiles = new DesktopProfileRepository({
    filePath: profileFile,
    defaultCodexHome,
    ...(defaultSqliteHome ? { defaultSqliteHome } : {})
  });

  const createWindow = async (): Promise<BrowserWindow> => {
    const window = new BrowserWindow({
      width: 1280,
      height: 840,
      minWidth: 760,
      minHeight: 560,
      show: false,
      title: "Codex Provider Sync",
      backgroundColor: "#11141b",
      webPreferences: createSecureWebPreferences(preloadPath)
    });
    window.once("ready-to-show", () => window.show());
    window.on("closed", () => {
      if (mainWindow === window) mainWindow = null;
    });
    await window.loadURL(`${DESKTOP_APP_ORIGIN}/index.html`);
    return window;
  };

  void app.whenReady().then(async () => {
    await profiles.initialize();
    await registerDesktopProtocol(protocol, rendererRoot);
    removeSecurity = installDesktopSecurity(app, session.defaultSession);
    supervisor = new CoreRuntimeSupervisor({
      appVersion: app.getVersion(),
      spawnUtility: createElectronUtilitySpawner({
        runtimePath,
        profileFile,
        defaultCodexHome,
        ...(defaultSqliteHome ? { defaultSqliteHome } : {})
      })
    });
    removeIpc = registerDesktopIpc({
      ipcMain,
      getWindow: () => mainWindow,
      rendererOrigin: DESKTOP_APP_ORIGIN,
      profiles,
      supervisor
    });
    if (e2eEnabled) {
      const { registerDesktopTestHooks } = await import("./e2e-hooks.js");
      removeTestIpc = registerDesktopTestHooks({
        ipcMain,
        getWindow: () => mainWindow,
        rendererOrigin: DESKTOP_APP_ORIGIN,
        supervisor
      });
    }
    mainWindow = await createWindow();

    if (e2eEnabled) {
      Object.defineProperty(globalThis, "__CPS_DESKTOP_TEST__", {
        configurable: true,
        value: Object.freeze({
          runtime: () => supervisor?.snapshot ?? null,
          window: () => mainWindow
        })
      });
    }
  }).catch(() => {
    app.exit(1);
  });

  app.on("second-instance", () => {
    if (!mainWindow) return;
    if (mainWindow.isMinimized()) mainWindow.restore();
    mainWindow.focus();
  });

  app.on("activate", () => {
    if (!mainWindow && app.isReady()) void createWindow().then((window) => {
      mainWindow = window;
    });
  });

  app.on("window-all-closed", () => {
    if (process.platform !== "darwin" || e2eEnabled) app.quit();
  });

  app.on("before-quit", (event) => {
    if (quitting) return;
    event.preventDefault();
    quitting = true;
    void (async () => {
      await supervisor?.shutdown();
      removeTestIpc?.();
      removeIpc?.();
      removeSecurity?.();
      app.quit();
    })();
  });
}
