import type { CoreClient } from "@codex-provider-sync/core-client";
import type { SupportedLocale, ThemeMode } from "@codex-provider-sync/design-system";

export interface HostProfile {
  id: string;
  name: string;
  revision: string;
  codexHome?: string;
  sqliteHome?: string | null;
  codexHomeConfigured?: boolean;
  sqliteHomeConfigured?: boolean;
}

export interface SaveProfileInput {
  profileId: string;
  profileRevision?: string;
  name: string;
  codexHome: string;
  sqliteHome?: string;
}

export interface HostClient {
  listProfiles(signal?: AbortSignal): Promise<HostProfile[]>;
  saveProfile?(input: SaveProfileInput, signal?: AbortSignal): Promise<HostProfile>;
  deleteProfile?(profileId: string, profileRevision: string, signal?: AbortSignal): Promise<void>;
  forgetBrowser?(): Promise<void>;
  exportDiagnostics?(
    profile: { profileId: string; profileRevision?: string },
    signal?: AbortSignal
  ): Promise<HostDiagnosticsExportResult>;
  getUpdateStatus?(signal?: AbortSignal): Promise<HostUpdateStatus>;
  checkForUpdates?(signal?: AbortSignal): Promise<HostUpdateStatus>;
  downloadUpdate?(signal?: AbortSignal): Promise<HostUpdateStatus>;
  installUpdate?(signal?: AbortSignal): Promise<HostUpdateStatus>;
}

export type HostDiagnosticsExportResult =
  | { status: "created" }
  | { status: "cancelled" }
  | { status: "failed" };

export interface HostUpdateStatus {
  state:
    | "disabled"
    | "idle"
    | "checking"
    | "available"
    | "downloading"
    | "downloaded"
    | "not-available"
    | "error"
    | "installing";
  reason?:
    | "not-packaged"
    | "not-authorized"
    | "not-configured"
    | "unsupported-target"
    | "check-failed"
    | "download-failed"
    | "install-failed";
  version?: string;
  progressPercent?: number;
  installBlockedReason?:
    | "write-in-progress"
    | "watch-active"
    | "pending-recovery"
    | "recovery-unverified";
  installAllowed: boolean;
}

export interface AppUiCapabilities {
  sync: boolean;
  switchProvider: boolean;
  repair: boolean;
  restore: boolean;
  pruneBackups: boolean;
  watch: boolean;
  manageProfiles: boolean;
  revealProfilePaths: boolean;
  forgetBrowser: boolean;
  exportDiagnostics: boolean;
  viewUpdateStatus: boolean;
}

export const FULL_APP_UI_CAPABILITIES: Readonly<AppUiCapabilities> = Object.freeze({
  sync: true,
  switchProvider: true,
  repair: true,
  restore: true,
  pruneBackups: true,
  watch: true,
  manageProfiles: true,
  revealProfilePaths: true,
  forgetBrowser: true,
  exportDiagnostics: true,
  viewUpdateStatus: true
});

export const READ_ONLY_APP_UI_CAPABILITIES: Readonly<AppUiCapabilities> = Object.freeze({
  sync: false,
  switchProvider: false,
  repair: false,
  restore: false,
  pruneBackups: false,
  watch: false,
  manageProfiles: false,
  revealProfilePaths: false,
  forgetBrowser: false,
  exportDiagnostics: false,
  viewUpdateStatus: false
});

export const SYNC_SWITCH_APP_UI_CAPABILITIES: Readonly<AppUiCapabilities> = Object.freeze({
  sync: true,
  switchProvider: true,
  repair: true,
  restore: false,
  pruneBackups: false,
  watch: false,
  manageProfiles: false,
  revealProfilePaths: false,
  forgetBrowser: false,
  exportDiagnostics: false,
  viewUpdateStatus: false
});

export const DESKTOP_C8_APP_UI_CAPABILITIES: Readonly<AppUiCapabilities> = Object.freeze({
  sync: true,
  switchProvider: true,
  repair: true,
  restore: true,
  pruneBackups: true,
  watch: true,
  manageProfiles: false,
  revealProfilePaths: false,
  forgetBrowser: false,
  exportDiagnostics: true,
  viewUpdateStatus: true
});

export interface PreferenceStore {
  getLocale(): SupportedLocale | null;
  setLocale(locale: SupportedLocale): void;
  getTheme(): ThemeMode | null;
  setTheme(theme: ThemeMode): void;
}

export type AppUiSurface = "desktop" | "web";

export interface AppUiProps {
  core: CoreClient;
  host: HostClient;
  surface: AppUiSurface;
  capabilities?: Partial<AppUiCapabilities>;
  preferences: PreferenceStore;
  initialLocale: SupportedLocale;
  initialTheme: ThemeMode;
  onForgetBrowser?: () => void | Promise<void>;
}
