import type { CoreClient, CoreTransport } from "@codex-provider-sync/core-client";
import type { CoreProtocolVersion } from "@codex-provider-sync/contracts";

export const DESKTOP_RUNTIME_STATE = "restore-watch-c8" as const;

export interface DesktopHandshakeContract {
  appVersion: string;
  coreVersion: string;
  protocolVersion: CoreProtocolVersion;
}

export interface DesktopHostBoundary {
  createClient(transport: CoreTransport): CoreClient;
}

// Electron process internals remain private to this workspace. Consumers only
// receive the stable state marker and handshake boundary types above.
