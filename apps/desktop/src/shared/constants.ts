import { CORE_PROTOCOL_VERSION } from "@codex-provider-sync/contracts";

export const DESKTOP_RUNTIME_PROTOCOL_VERSION = 1 as const;
export const DESKTOP_CORE_PROTOCOL_VERSION = CORE_PROTOCOL_VERSION;
export const DESKTOP_CORE_VERSION = "0.0.0" as const;
export const DESKTOP_BUILD_ID = "c6-readonly-1" as const;
export const DESKTOP_APP_SCHEME = "cps-app" as const;
export const DESKTOP_APP_HOST = "app" as const;
export const DESKTOP_APP_ORIGIN = `${DESKTOP_APP_SCHEME}://${DESKTOP_APP_HOST}` as const;
export const MAX_DESKTOP_IPC_BYTES = 64 * 1024;

export const DESKTOP_IPC_CHANNELS = Object.freeze({
  coreRead: "cps:v1:core:read",
  profilesList: "cps:v1:profiles:list"
});

export const DESKTOP_CSP = [
  "default-src 'self'",
  "script-src 'self'",
  "style-src 'self'",
  "img-src 'self' data:",
  "font-src 'self'",
  "connect-src 'self'",
  "object-src 'none'",
  "frame-src 'none'",
  "base-uri 'none'",
  "form-action 'none'",
  "frame-ancestors 'none'"
].join("; ");
