import process from "node:process";

import { DESKTOP_READ_METHODS } from "@codex-provider-sync/core-client";

import { DesktopProfileRepository } from "../profiles/repository.js";
import {
  DESKTOP_BUILD_ID,
  DESKTOP_CORE_PROTOCOL_VERSION,
  DESKTOP_CORE_VERSION,
  DESKTOP_RUNTIME_PROTOCOL_VERSION
} from "../shared/constants.js";
import {
  assertRuntimeRequestFrame,
  assertRuntimeShutdownFrame,
  createRuntimeResponseFrame,
  type RuntimeHelloFrame
} from "../shared/runtime-protocol.js";
import { createDesktopRuntimeHost } from "./host.js";

interface UtilityParentPort {
  postMessage(value: unknown): void;
  on(event: "message", listener: (event: { data: unknown }) => void): void;
}

function requiredEnvironment(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing desktop runtime bootstrap field: ${name}`);
  return value;
}

const parentPort = (process as NodeJS.Process & { parentPort?: UtilityParentPort }).parentPort;
if (!parentPort) throw new Error("Desktop Core Runtime requires an Electron Utility Process parent port.");

const generation = Number(requiredEnvironment("CPS_DESKTOP_RUNTIME_GENERATION"));
if (!Number.isSafeInteger(generation) || generation < 1) {
  throw new Error("Invalid desktop runtime generation.");
}

const profiles = new DesktopProfileRepository({
  filePath: requiredEnvironment("CPS_DESKTOP_PROFILE_FILE"),
  defaultCodexHome: requiredEnvironment("CPS_DESKTOP_DEFAULT_CODEX_HOME"),
  ...(process.env.CPS_DESKTOP_DEFAULT_SQLITE_HOME
    ? { defaultSqliteHome: process.env.CPS_DESKTOP_DEFAULT_SQLITE_HOME }
    : {})
});
await profiles.initialize();
const host = createDesktopRuntimeHost(profiles);

const hello: RuntimeHelloFrame = {
  kind: "hello",
  runtimeProtocolVersion: DESKTOP_RUNTIME_PROTOCOL_VERSION,
  coreProtocolVersion: DESKTOP_CORE_PROTOCOL_VERSION,
  appVersion: requiredEnvironment("CPS_DESKTOP_APP_VERSION"),
  coreVersion: DESKTOP_CORE_VERSION,
  buildId: DESKTOP_BUILD_ID,
  sessionNonce: requiredEnvironment("CPS_DESKTOP_RUNTIME_NONCE"),
  generation,
  capabilities: DESKTOP_READ_METHODS
};
parentPort.postMessage(hello);

parentPort.on("message", (event) => {
  void (async () => {
    const frame = event.data;
    if (frame !== null && typeof frame === "object" && !Array.isArray(frame)
        && (frame as { kind?: unknown }).kind === "shutdown") {
      assertRuntimeShutdownFrame(frame);
      if (frame.generation !== generation) throw new Error("Stale desktop runtime shutdown frame.");
      process.exit(0);
    }
    assertRuntimeRequestFrame(frame);
    if (frame.generation !== generation) throw new Error("Stale desktop runtime request frame.");
    const response = await host.dispatch(frame.envelope);
    parentPort.postMessage(createRuntimeResponseFrame(generation, response));
  })().catch(() => {
    process.exit(70);
  });
});
