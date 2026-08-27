import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  app,
  BrowserWindow,
  dialog,
  ipcMain,
  protocol,
  screen,
  session
} from "electron";

import {
  DESKTOP_APP_ORIGIN
} from "../shared/constants.js";
import { createElectronUtilitySpawner } from "./electron-utility.js";
import { DesktopDiagnosticsExporter } from "./diagnostics-export.js";
import { registerDesktopIpc } from "./ipc-router.js";
import { DesktopProfileRepository } from "../profiles/repository.js";
import { CoreRuntimeSupervisor } from "./runtime-supervisor.js";
import { createSecureWebPreferences } from "./security-policy.js";
import { DesktopUpdateController } from "./updater.js";
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
  let updates: DesktopUpdateController | null = null;
  let activeWatchCount = 0;
  let quitting = false;
  if (process.platform !== "win32") {
    const requestGracefulQuit = () => app.quit();
    process.once("SIGINT", requestGracefulQuit);
    process.once("SIGTERM", requestGracefulQuit);
  }

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
    const windowDisplay = process.env.CPS_DESKTOP_WINDOW_DISPLAY;
    const preferredDisplay = windowDisplay === "secondary"
      ? screen.getAllDisplays().find((display) => display.id !== screen.getPrimaryDisplay().id)
      : undefined;
    const workArea = preferredDisplay?.workArea;
    const width = workArea ? Math.min(1280, workArea.width) : 1280;
    const height = workArea ? Math.min(840, workArea.height) : 840;
    const window = new BrowserWindow({
      width,
      height,
      ...(workArea ? {
        x: workArea.x + Math.max(0, Math.floor((workArea.width - width) / 2)),
        y: workArea.y + Math.max(0, Math.floor((workArea.height - height) / 2))
      } : {}),
      minWidth: 760,
      minHeight: 560,
      show: false,
      title: "Codex Provider Sync",
      backgroundColor: "#11141b",
      webPreferences: createSecureWebPreferences(preloadPath)
    });
    if (windowDisplay !== "hidden") window.once("ready-to-show", () => window.show());
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
    const diagnosticsExporter = new DesktopDiagnosticsExporter({
      appVersion: app.getVersion(),
      isPackaged: app.isPackaged
    });
    updates = new DesktopUpdateController({
      isPackaged: app.isPackaged,
      platform: process.platform,
      arch: process.arch,
      appVersion: app.getVersion(),
      configured: app.getVersion() !== "0.0.0",
      supervisor,
      hasActiveWatches: () => activeWatchCount > 0,
      verifyRecoveryState: () => supervisor!.verifyProfilesSafeForRestart(
        profiles.list().map((profile) => ({
          profileId: profile.id,
          profileRevision: profile.revision
        }))
      )
    });
    removeIpc = registerDesktopIpc({
      ipcMain,
      getWindow: () => mainWindow,
      rendererOrigin: DESKTOP_APP_ORIGIN,
      profiles,
      supervisor,
      updates,
      onActiveWatchCountChanged(count) {
        activeWatchCount = count;
      },
      diagnosticsExporter,
      async selectDiagnosticsTarget() {
        if (e2eEnabled && process.env.CPS_DESKTOP_DIAGNOSTICS_TARGET) {
          return path.resolve(process.env.CPS_DESKTOP_DIAGNOSTICS_TARGET);
        }
        const options = {
          title: "Export redacted diagnostics",
          defaultPath: path.join(
            app.getPath("downloads"),
            `codex-provider-diagnostics-${new Date().toISOString().slice(0, 10)}.zip`
          ),
          filters: [{ name: "ZIP archive", extensions: ["zip"] }],
          properties: ["showOverwriteConfirmation" as const]
        };
        const result = mainWindow
          ? await dialog.showSaveDialog(mainWindow, options)
          : await dialog.showSaveDialog(options);
        return result.canceled || !result.filePath ? null : result.filePath;
      }
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
    updates.scheduleInitialCheck();

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
      updates?.dispose();
      await supervisor?.shutdown();
      removeTestIpc?.();
      removeIpc?.();
      removeSecurity?.();
      app.quit();
    })();
  });
}
