import type { CoreClient } from "@codex-provider-sync/core-client";
import type { SupportedLocale, ThemeMode } from "@codex-provider-sync/design-system";

export interface HostProfile {
  id: string;
  name: string;
  codexHome: string;
  sqliteHome: string | null;
  revision: string;
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
  saveProfile(input: SaveProfileInput, signal?: AbortSignal): Promise<HostProfile>;
  deleteProfile(profileId: string, profileRevision: string, signal?: AbortSignal): Promise<void>;
  forgetBrowser?(): Promise<void>;
}

export interface PreferenceStore {
  getLocale(): SupportedLocale | null;
  setLocale(locale: SupportedLocale): void;
  getTheme(): ThemeMode | null;
  setTheme(theme: ThemeMode): void;
}

export interface AppUiProps {
  core: CoreClient;
  host: HostClient;
  preferences: PreferenceStore;
  initialLocale: SupportedLocale;
  initialTheme: ThemeMode;
  onForgetBrowser?: () => void | Promise<void>;
}
