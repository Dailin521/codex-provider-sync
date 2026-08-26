import {
  AppUi,
  SYNC_SWITCH_APP_UI_CAPABILITIES,
  type HostClient,
  type HostProfile,
  type PreferenceStore
} from "@codex-provider-sync/app-ui";
import { DesktopCoreClient } from "@codex-provider-sync/core-client";
import type { SupportedLocale, ThemeMode } from "@codex-provider-sync/design-system";
import React from "react";
import { createRoot } from "react-dom/client";

import type { DesktopBridgeApi } from "../shared/bridge.js";
import "./styles.css";

declare global {
  interface Window {
    readonly codexProvider: DesktopBridgeApi;
  }
}

const bridge = window.codexProvider;
if (!bridge || bridge.version !== 1) throw new Error("Desktop preload bridge is unavailable.");

const core = new DesktopCoreClient(bridge.core);
const host: HostClient = Object.freeze({
  async listProfiles(): Promise<HostProfile[]> {
    const value = await bridge.profiles.list();
    return value.profiles.map((profile) => ({ ...profile }));
  }
});

const preferences: PreferenceStore = Object.freeze({
  getLocale(): SupportedLocale | null {
    const value = localStorage.getItem("cps.desktop.locale");
    return value === "zh-CN" || value === "en" ? value : null;
  },
  setLocale(locale: SupportedLocale): void {
    localStorage.setItem("cps.desktop.locale", locale);
  },
  getTheme(): ThemeMode | null {
    const value = localStorage.getItem("cps.desktop.theme");
    return value === "system" || value === "light" || value === "dark" ? value : null;
  },
  setTheme(theme: ThemeMode): void {
    localStorage.setItem("cps.desktop.theme", theme);
  }
});

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <AppUi
      capabilities={SYNC_SWITCH_APP_UI_CAPABILITIES}
      core={core}
      host={host}
      initialLocale={navigator.language.toLowerCase().startsWith("zh") ? "zh-CN" : "en"}
      initialTheme="system"
      preferences={preferences}
    />
  </React.StrictMode>
);
