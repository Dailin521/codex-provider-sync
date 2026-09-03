// @ts-check

import { createConfigStore } from "./config-store.js";
import { createGlobalStateStore } from "./global-state-store.js";
import { createSessionStore } from "./session-store.js";
import { createStateDbStore } from "./state-db-store.js";

/** @typedef {Record<string, Function>} StoragePort */
/**
 * @typedef {{
 *   config: StoragePort,
 *   sessions: StoragePort,
 *   stateDb: StoragePort,
 *   globalState: StoragePort
 * }} CodexStorage
 */

/**
 * Compose the four low-level Codex storage ports without adding orchestration
 * or business rules. Concrete adapters are injected by the application host.
 *
 * @param {CodexStorage} ports
 * @returns {Readonly<CodexStorage>}
 */
export function createCodexStorage(ports) {
  if (!ports || typeof ports !== "object") {
    throw new TypeError("CodexStorage requires four storage ports.");
  }
  for (const name of ["config", "sessions", "stateDb", "globalState"]) {
    const port = ports[/** @type {keyof CodexStorage} */ (name)];
    if (!port || typeof port !== "object" || Array.isArray(port)) {
      throw new TypeError(`CodexStorage ${name} port is missing.`);
    }
  }
  return Object.freeze({
    config: createConfigStore(ports.config),
    sessions: createSessionStore(ports.sessions),
    stateDb: createStateDbStore(ports.stateDb),
    globalState: createGlobalStateStore(ports.globalState)
  });
}
