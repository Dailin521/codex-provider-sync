import type { CoreClient, CoreTransport } from "@codex-provider-sync/core-client";
import type { CoreProtocolVersion } from "@codex-provider-sync/contracts";

export const DESKTOP_RUNTIME_STATE = "not-enabled-c4" as const;

export interface DesktopHandshakeContract {
  appVersion: string;
  coreVersion: string;
  protocolVersion: CoreProtocolVersion;
}

export interface DesktopHostBoundary {
  createClient(transport: CoreTransport): CoreClient;
}

// Electron, BrowserWindow, Preload, IPC and Utility Process are deliberately
// absent until C6 establishes and tests those security boundaries.
