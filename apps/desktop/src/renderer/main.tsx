import {
  AppUi,
  createBackupPreferences,
  createProjectAliasPreferences,
  DESKTOP_C8_APP_UI_CAPABILITIES,
  type HostClient,
  type HostProfile,
  type OperationLogStatus,
  type SaveProfileInput,
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
  async copyText(text: string) {
    const result = await bridge.clipboard.writeText({ schemaVersion: 1, text });
    if (!result.copied) throw new Error("Copy failed.");
  },
  async listProfiles(): Promise<HostProfile[]> {
    const value = await bridge.profiles.list();
    return value.profiles.map((profile) => ({ ...profile }));
  },
  async selectProfileDirectory(kind: "codex-home" | "sqlite-home") {
    const result = await bridge.profiles.selectDirectory(kind);
    return result.status === "cancelled"
      ? { status: "cancelled" as const }
      : { status: "selected" as const, token: result.token, displayName: result.displayName };
  },
  async saveProfile(input: SaveProfileInput) {
    const result = await bridge.profiles.save({
      schemaVersion: 1,
      name: input.name,
      ...(input.profileId ? { profileId: input.profileId } : {}),
      ...(input.profileRevision ? { profileRevision: input.profileRevision } : {}),
      ...(input.codexHomeSelectionToken ? { codexHomeSelectionToken: input.codexHomeSelectionToken } : {}),
      sqliteHomeMode: input.sqliteHomeMode ?? "inherit",
      ...(input.sqliteHomeSelectionToken ? { sqliteHomeSelectionToken: input.sqliteHomeSelectionToken } : {})
    });
    return { ...result };
  },
  async deleteProfile(profileId: string, profileRevision: string) {
    await bridge.profiles.delete({ schemaVersion: 1, profileId, profileRevision });
  },
  async revealProfileDirectory(profileId: string, profileRevision: string, target: "codex-home" | "sqlite-home") {
    await bridge.profiles.reveal({ schemaVersion: 1, profileId, profileRevision, target });
  },
  async revealHistoryFile(profile: { profileId: string; profileRevision?: string }, sessionId: string) {
    return bridge.history.reveal({ schemaVersion: 1, profile, sessionId });
  },
  async listOperationLogs(input: { page: number; pageSize: number; profileId?: string; profileRevision?: string; operation?: string; status?: OperationLogStatus }) {
    return { ...await bridge.operationLogs.list({ schemaVersion: 1, ...input }) };
  },
  async getOperationLog(id: string) {
    return bridge.operationLogs.get({ schemaVersion: 1, id });
  },
  async dismissOperationPlan(planId: string) {
    await bridge.operationLogs.dismissPlan(planId);
  },
  async exportDiagnostics(profile: { profileId: string; profileRevision?: string }) {
    const result = await bridge.diagnostics.export({ schemaVersion: 1, profile });
    return { status: result.status };
  },
  async getUpdateStatus() {
    const status = await bridge.updates.getStatus();
    return { ...status };
  },
  subscribeUpdateStatus(listener: Parameters<NonNullable<HostClient["subscribeUpdateStatus"]>>[0]) {
    return bridge.updates.subscribe(listener);
  },
  subscribeWatchStopped(listener: Parameters<NonNullable<HostClient["subscribeWatchStopped"]>>[0]) {
    return bridge.watch.subscribeStopped(listener);
  },
  async checkForUpdates() {
    return { ...await bridge.updates.check() };
  },
  async downloadUpdate() {
    return { ...await bridge.updates.download() };
  },
  async installUpdate() {
    return { ...await bridge.updates.install() };
  },
  async setUpdateReminder(version: string, ignored: boolean) {
    return { ...await bridge.updates.setReminder({ schemaVersion: 1, version, ignored }) };
  }
});

const preferences: PreferenceStore = Object.freeze({
  ...createBackupPreferences(localStorage, "cps.desktop"),
  ...createProjectAliasPreferences(localStorage, "cps.desktop"),
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
      capabilities={DESKTOP_C8_APP_UI_CAPABILITIES}
      core={core}
      host={host}
      initialLocale={navigator.language.toLowerCase().startsWith("zh") ? "zh-CN" : "en"}
      initialTheme="system"
      preferences={preferences}
      surface="desktop"
    />
  </React.StrictMode>
);
