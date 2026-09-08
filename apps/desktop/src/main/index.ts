import os from "node:os";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  app,
  BrowserWindow,
  clipboard,
  dialog,
  ipcMain,
  nativeTheme,
  protocol,
  screen,
  session,
  shell
} from "electron";

import {
  DESKTOP_APP_ORIGIN, DESKTOP_IPC_CHANNELS
} from "../shared/constants.js";
import { createElectronUtilitySpawner } from "./electron-utility.js";
import { DesktopDiagnosticsExporter } from "./diagnostics-export.js";
import { DirectorySelectionService } from "./directory-selection-service.js";
import { registerDesktopIpc, type DesktopIpcRegistration } from "./ipc-router.js";
import { OperationLogService } from "./operation-log-service.js";
import { DesktopProfileRepository } from "../profiles/repository.js";
import { CoreRuntimeSupervisor } from "./runtime-supervisor.js";
import { createSecureWebPreferences } from "./security-policy.js";
import { DesktopUpdateController } from "./updater.js";
import { checkPublicDesktopRelease, PUBLIC_RELEASES_URL } from "./public-release-checker.js";
import { claimDailyUpdateCheck } from "./daily-update-check.js";
import { fitWindowToDisplays, restoreWindowState, WindowStateController, WindowStateStore } from "./window-state.js";
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
  let ipcRegistration: DesktopIpcRegistration | null = null;
  let removeTestIpc: (() => void) | null = null;
  let removeSecurity: (() => void) | null = null;
  let updates: DesktopUpdateController | null = null;
  let unsubscribeRuntimeState: (() => void) | null = null;
  let unsubscribeWatchActivity: (() => void) | null = null;
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
  const windowStatePersistenceEnabled = !["hidden", "secondary"].includes(process.env.CPS_DESKTOP_WINDOW_DISPLAY ?? "");
  const windowStateStore = new WindowStateStore({
    filePath: path.join(app.getPath("userData"), "window-state.json"),
    enabled: windowStatePersistenceEnabled
  });
  const windowState = new WindowStateController({
    store: windowStateStore,
    screen
  });
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
    const workArea = preferredDisplay?.workArea ?? screen.getPrimaryDisplay().workArea;
    const width = Math.min(1280, workArea.width);
    const height = Math.min(840, workArea.height);
    const initialBounds = {
      width,
      height,
      x: workArea.x + Math.max(0, Math.floor((workArea.width - width) / 2)),
      y: workArea.y + Math.max(0, Math.floor((workArea.height - height) / 2))
    };
    const restored = restoreWindowState(
      await windowStateStore.read(),
      screen,
      initialBounds,
      { width: 760, height: 560 }
    );
    const restoredWorkArea = screen.getDisplayMatching(restored.bounds).workArea;
    const window = new BrowserWindow({
      ...restored.bounds,
      minWidth: Math.min(760, restoredWorkArea.width),
      minHeight: Math.min(560, restoredWorkArea.height),
      show: false,
      title: "Codex Provider Sync",
      backgroundColor: nativeTheme.shouldUseDarkColors ? "#11141b" : "#f6f7fb",
      webPreferences: createSecureWebPreferences(preloadPath)
    });
    if (restored.maximized) window.maximize();
    const detachWindowState = windowState.attach(window);
    const refit = () => { if (!window.isDestroyed()) fitWindowToDisplays(window, screen); };
    if (windowStatePersistenceEnabled) {
      screen.on("display-removed", refit);
      screen.on("display-metrics-changed", refit);
    }
    if (windowDisplay !== "hidden") window.once("ready-to-show", () => windowDisplay === "secondary" ? window.showInactive() : window.show());
    window.on("close", () => { void windowState.flush(window); });
    window.on("closed", () => {
      detachWindowState();
      screen.removeListener("display-removed", refit);
      screen.removeListener("display-metrics-changed", refit);
      if (mainWindow === window) mainWindow = null;
    });
    await window.loadURL(`${DESKTOP_APP_ORIGIN}/index.html`);
    return window;
  };

  void app.whenReady().then(async () => {
    const chinese = app.getLocale().toLowerCase().startsWith("zh");
    await profiles.initialize();
    const operationLogs = new OperationLogService({
      directory: path.join(app.getPath("userData"), "logs", "operations")
    });
    await operationLogs.initialize();
    const directorySelections = new DirectorySelectionService();
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
    let runtimePreviouslyCrashed = false;
    unsubscribeRuntimeState = supervisor.subscribeState((event) => {
      if (event.current === "crashed") {
        runtimePreviouslyCrashed = true;
        void operationLogs.begin({ operation: "runtime" }).then((id) => operationLogs.finish(id, {
          status: "failed",
          outcome: "crashed",
          errorCode: "CORE_RUNTIME_CRASHED"
        }));
      } else if (event.current === "ready" && runtimePreviouslyCrashed) {
        runtimePreviouslyCrashed = false;
        void operationLogs.begin({ operation: "runtime" }).then((id) => operationLogs.finish(id, {
          status: "completed",
          outcome: "restarted"
        }));
      }
    });
    const watchActivityLogs = new Map<string, Promise<string>>();
    unsubscribeWatchActivity = supervisor.subscribeWatchActivity((activity) => {
      if (activity.event === "started") {
        watchActivityLogs.set(activity.activityId, operationLogs.begin({ operation: "watch", profileId: activity.profileId, profileRevision: activity.profileRevision, stage: "automatic-sync" }));
        return;
      }
      const finish = async () => {
        const pendingLog = watchActivityLogs.get(activity.activityId);
        const logId = pendingLog
          ? await pendingLog
          : await operationLogs.begin({ operation: "watch", profileId: activity.profileId, profileRevision: activity.profileRevision, stage: "automatic-sync" });
        watchActivityLogs.delete(activity.activityId);
        await operationLogs.progress(logId, {
          stage: "automatic-sync",
          status: activity.outcome === "failed" || activity.failedStage ? "error" : "complete",
          count: (activity.changedSessionFiles ?? 0) + (activity.sqliteRowsUpdated ?? 0)
        });
        await operationLogs.finish(logId, {
          status: activity.outcome === "partial" ? "partial" : activity.outcome === "failed" ? "failed" : "completed",
          outcome: activity.outcome,
          backupId: activity.backupId,
          fileUpdateTiming: activity.fileUpdateTiming,
          failedStage: activity.failedStage,
          failureCode: activity.failureCode,
          partialReason: activity.partialReason,
          retryRecommended: activity.retryRecommended,
          counts: {
            changedSessionFiles: activity.changedSessionFiles ?? 0,
            sqliteRowsUpdated: activity.sqliteRowsUpdated ?? 0,
            skippedLockedRolloutFiles: activity.skippedLockedRolloutFiles ?? 0
          },
          ...(activity.errorCode ? { errorCode: activity.errorCode } : {}),
          ...(activity.skippedLockedRolloutFiles ? { warnings: [`skippedLockedRolloutFiles:${activity.skippedLockedRolloutFiles}`] } : {})
        });
      };
      void finish();
    });
    const diagnosticsExporter = new DesktopDiagnosticsExporter({
      appVersion: app.getVersion(),
      isPackaged: app.isPackaged,
      recentLogs: () => operationLogs.recentJsonLines()
    });
    updates = new DesktopUpdateController({
      claimStartupCheck: () => claimDailyUpdateCheck(app.getPath("userData")),
      async runStartupCheck(check) {
        const logId = await operationLogs.begin({ operation: "update", stage: "startup-check" });
        try {
          const status = await check();
          await operationLogs.finish(logId, {
            status: status.state === "error" || status.state === "disabled" ? "failed" : "completed",
            outcome: status.state,
            ...(status.reason ? { errorCode: status.reason } : {})
          });
          return status;
        } catch (error) {
          await operationLogs.finish(logId, { status: "failed", errorCode: "INTERNAL_ERROR" });
          throw error;
        }
      },
      async onStartupUpdateAvailable(status) {
        if (!mainWindow || mainWindow.isDestroyed()) return;
        await dialog.showMessageBox(mainWindow, {
          type: "info",
          title: chinese ? "发现新版本" : "Update available",
          message: chinese ? `Codex Provider Sync ${status.version} 已可用` : `Codex Provider Sync ${status.version} is available`,
          detail: chinese ? "请前往“设置 → 更新”查看和下载。不会自动下载或退出软件。" : "Go to Settings → Updates to review and download. Nothing is downloaded or installed automatically.",
          buttons: [chinese ? "知道了" : "OK"],
          noLink: true
        });
      },
      isPackaged: app.isPackaged,
      platform: process.platform,
      arch: process.arch,
      appVersion: app.getVersion(),
      configured: existsSync(path.join(process.resourcesPath, "app-update.yml")),
      releaseAuthorized: __CPS_DESKTOP_RELEASE_AUTHORIZED__,
      manualUpdates: {
        force: process.platform === "win32"
          ? !existsSync(path.join(path.dirname(process.execPath), "Uninstall Codex Provider Sync.exe"))
          : process.platform === "linux" && !process.env.APPIMAGE,
        check: () => checkPublicDesktopRelease({ appVersion: app.getVersion(), platform: process.platform, arch: process.arch }),
        openDownloadPage: () => shell.openExternal(PUBLIC_RELEASES_URL)
      },
      onStatus: (status) => {
        if (mainWindow && !mainWindow.isDestroyed()) mainWindow.webContents.send(DESKTOP_IPC_CHANNELS.updateEvent, status);
      },
      supervisor,
      hasActiveWatches: () => activeWatchCount > 0,
      verifyNoActiveWatches: async () => (
        (await ipcRegistration?.verifyNoActiveWatchesForRestart()) === "clear"
      ),
      verifyRecoveryState: () => supervisor!.verifyProfilesSafeForRestart(
        profiles.list().map((profile) => ({
          profileId: profile.id,
          profileRevision: profile.revision
        }))
      )
    });
    ipcRegistration = registerDesktopIpc({
      ipcMain,
      getWindow: () => mainWindow,
      rendererOrigin: DESKTOP_APP_ORIGIN,
      profiles,
      directorySelections,
      operationLogs,
      supervisor,
      updates,
      isProfileMutationBlocked: () => Boolean(supervisor?.snapshot.writeInProgress || activeWatchCount > 0),
      async selectProfileDirectory(kind) {
        const dialogOptions = {
          title: kind === "codex-home"
            ? (chinese ? "选择 Codex 数据目录" : "Choose Codex data folder")
            : (chinese ? "选择聊天索引目录" : "Choose chat index folder"),
          properties: ["openDirectory" as const, "createDirectory" as const]
        };
        const result = mainWindow
          ? await dialog.showOpenDialog(mainWindow, dialogOptions)
          : await dialog.showOpenDialog(dialogOptions);
        return result.canceled || result.filePaths.length !== 1 ? null : result.filePaths[0]!;
      },
      async revealProfileDirectory(directory) {
        return (await shell.openPath(directory)) === "";
      },
      async revealHistoryFile(filePath) {
        shell.showItemInFolder(filePath);
        return true;
      },
      writeClipboardText(text) {
        clipboard.writeText(text);
      },
      onActiveWatchCountChanged(count) {
        activeWatchCount = count;
      },
      diagnosticsExporter,
      async selectDiagnosticsTarget() {
        if (e2eEnabled && process.env.CPS_DESKTOP_DIAGNOSTICS_TARGET) {
          return path.resolve(process.env.CPS_DESKTOP_DIAGNOSTICS_TARGET);
        }
        const options = {
          title: chinese ? "导出诊断信息" : "Export diagnostics",
          defaultPath: path.join(
            app.getPath("downloads"),
            `codex-provider-diagnostics-${new Date().toISOString().slice(0, 10)}.zip`
          ),
          filters: [{ name: chinese ? "ZIP 压缩包" : "ZIP archive", extensions: ["zip"] }],
          properties: ["showOverwriteConfirmation" as const]
        };
        const result = mainWindow
          ? await dialog.showSaveDialog(mainWindow, options)
          : await dialog.showSaveDialog(options);
        return result.canceled || !result.filePath ? null : result.filePath;
      }
    });
    removeIpc = ipcRegistration;
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
      unsubscribeRuntimeState?.();
      unsubscribeWatchActivity?.();
      await windowState.flush(mainWindow ?? undefined);
      await supervisor?.shutdown();
      removeTestIpc?.();
      removeIpc?.();
      removeSecurity?.();
      app.quit();
    })();
  });
}
