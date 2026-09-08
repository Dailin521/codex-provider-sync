import path from "node:path";

import { utilityProcess, type UtilityProcess } from "electron";

import type { ExpectedRuntimeIdentity, RuntimeFrame } from "../shared/runtime-protocol.js";
import type { RuntimeUtilityHandle, RuntimeUtilitySpawner } from "./runtime-supervisor.js";

export interface ElectronUtilitySpawnerOptions {
  runtimePath: string;
  profileFile: string;
  defaultCodexHome: string;
  defaultSqliteHome?: string;
}

function wrapUtility(child: UtilityProcess): RuntimeUtilityHandle {
  return {
    postMessage(frame: RuntimeFrame) {
      child.postMessage(frame);
    },
    kill() {
      child.kill();
    },
    onMessage(listener) {
      const wrapped = (message: unknown) => listener(message);
      child.on("message", wrapped);
      return () => child.removeListener("message", wrapped);
    },
    onExit(listener) {
      const wrapped = () => listener();
      child.on("exit", wrapped);
      return () => child.removeListener("exit", wrapped);
    }
  };
}

export function createElectronUtilitySpawner(
  options: ElectronUtilitySpawnerOptions
): RuntimeUtilitySpawner {
  const runtimePath = path.resolve(options.runtimePath);
  return (identity: ExpectedRuntimeIdentity) => {
    const child = utilityProcess.fork(runtimePath, [], {
      serviceName: "Codex Provider Sync Core",
      stdio: "ignore",
      env: {
        ...process.env,
        CPS_DESKTOP_APP_VERSION: identity.appVersion,
        CPS_DESKTOP_RUNTIME_NONCE: identity.sessionNonce,
        CPS_DESKTOP_RUNTIME_GENERATION: String(identity.generation),
        CPS_DESKTOP_PROFILE_FILE: path.resolve(options.profileFile),
        CPS_DESKTOP_DEFAULT_CODEX_HOME: path.resolve(options.defaultCodexHome),
        ...(options.defaultSqliteHome
          ? { CPS_DESKTOP_DEFAULT_SQLITE_HOME: path.resolve(options.defaultSqliteHome) }
          : {})
      }
    });
    return wrapUtility(child);
  };
}
