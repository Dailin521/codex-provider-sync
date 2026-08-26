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
}

export interface AppUiCapabilities {
  sync: boolean;
  switchProvider: boolean;
  restore: boolean;
  pruneBackups: boolean;
  watch: boolean;
  manageProfiles: boolean;
  revealProfilePaths: boolean;
  forgetBrowser: boolean;
}

export const FULL_APP_UI_CAPABILITIES: Readonly<AppUiCapabilities> = Object.freeze({
  sync: true,
  switchProvider: true,
  restore: true,
  pruneBackups: true,
  watch: true,
  manageProfiles: true,
  revealProfilePaths: true,
  forgetBrowser: true
});

export const READ_ONLY_APP_UI_CAPABILITIES: Readonly<AppUiCapabilities> = Object.freeze({
  sync: false,
  switchProvider: false,
  restore: false,
  pruneBackups: false,
  watch: false,
  manageProfiles: false,
  revealProfilePaths: false,
  forgetBrowser: false
});

export const SYNC_SWITCH_APP_UI_CAPABILITIES: Readonly<AppUiCapabilities> = Object.freeze({
  sync: true,
  switchProvider: true,
  restore: false,
  pruneBackups: false,
  watch: false,
  manageProfiles: false,
  revealProfilePaths: false,
  forgetBrowser: false
});

export interface PreferenceStore {
  getLocale(): SupportedLocale | null;
  setLocale(locale: SupportedLocale): void;
  getTheme(): ThemeMode | null;
  setTheme(theme: ThemeMode): void;
}

export interface AppUiProps {
  core: CoreClient;
  host: HostClient;
  capabilities?: Partial<AppUiCapabilities>;
  preferences: PreferenceStore;
  initialLocale: SupportedLocale;
  initialTheme: ThemeMode;
  onForgetBrowser?: () => void | Promise<void>;
}
