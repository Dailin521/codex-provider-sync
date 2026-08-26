import type { BrowserWindow, IpcMain } from "electron";

import type { CoreRuntimeSupervisor } from "./runtime-supervisor.js";
import { isTrustedSender } from "./ipc-router.js";

const TEST_CRASH_RUNTIME_CHANNEL = "cps:v1:test:crash-runtime";

export function registerDesktopTestHooks(options: {
  ipcMain: IpcMain;
  getWindow(): BrowserWindow | null;
  rendererOrigin: string;
  supervisor: CoreRuntimeSupervisor;
}): () => void {
  options.ipcMain.handle(TEST_CRASH_RUNTIME_CHANNEL, (event, value) => {
    if (!isTrustedSender(event, options.getWindow(), options.rendererOrigin)
        || value !== null) {
      throw new Error("Desktop test request rejected.");
    }
    return { crashed: options.supervisor.crashForTest() };
  });
  return () => options.ipcMain.removeHandler(TEST_CRASH_RUNTIME_CHANNEL);
}
