import type { FileUpdateTiming, WatchSnapshot } from "@codex-provider-sync/contracts";
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
  profileId?: string;
  profileRevision?: string;
  name: string;
  codexHome?: string;
  sqliteHome?: string;
  codexHomeSelectionToken?: string;
  sqliteHomeMode?: "preserve" | "inherit" | "selected";
  sqliteHomeSelectionToken?: string;
}

export type HostDirectorySelection =
  | { status: "cancelled" }
  | { status: "selected"; token: string; displayName: string };

export type OperationLogStatus = "running" | "awaiting-confirmation" | "completed" | "partial" | "failed" | "cancelled" | "dismissed" | "interrupted";
export interface OperationLogStage { stage: string; status: "running" | "completed" | "failed"; startedAt: string; completedAt?: string; durationMs?: number; progress?: number; count?: number; }
export interface OperationLogEntry { fileUpdateTiming?: FileUpdateTiming; }
export interface OperationLogEntry { schemaVersion: 1; id: string; operation: string; profileId?: string; profileRevision?: string; startedAt: string; completedAt?: string; activeDurationMs: number; wallDurationMs?: number; status: OperationLogStatus; outcome?: string; errorCode?: string; errorReason?: "profile" | "config" | "storage" | "rollout" | "state-db" | "backup" | "provider-not-configured"; failedStage?: string; failureCode?: string; partialReason?: "locked-session" | "rollout-changed" | "mutation-failed"; retryRecommended?: boolean; requestIds: string[]; planId?: string; operationId?: string; backupId?: string; targetProvider?: string; previewCounts?: { rolloutFilesToChange: number; sqliteRowsToChange: number; lockedRolloutFiles: number; }; switchPlan?: { previousProvider: string; targetProvider: string; previousRootModel: string | null; targetRootModel: string | null; modelMode: "provider-default" | "keep-root-model" | "explicit"; }; counts: Record<string, number>; warnings: string[]; stages: OperationLogStage[]; }
export interface OperationLogPage { schemaVersion: 1; page: number; pageSize: number; total: number; hasNextPage: boolean; entries: OperationLogEntry[]; }

export interface HostClient {
  copyText?(text: string): Promise<void>;
  revealHistoryFile?(profile: { profileId: string; profileRevision?: string }, sessionId: string): Promise<{ revealed: boolean }>;
  listProfiles(signal?: AbortSignal): Promise<HostProfile[]>;
  saveProfile?(input: SaveProfileInput, signal?: AbortSignal): Promise<HostProfile>;
  deleteProfile?(profileId: string, profileRevision: string, signal?: AbortSignal): Promise<void>;
  selectProfileDirectory?(kind: "codex-home" | "sqlite-home"): Promise<HostDirectorySelection>;
  revealProfileDirectory?(profileId: string, profileRevision: string, target: "codex-home" | "sqlite-home"): Promise<void>;
  listOperationLogs?(input: { page: number; pageSize: number; profileId?: string; profileRevision?: string; operation?: string; status?: OperationLogStatus }, signal?: AbortSignal): Promise<OperationLogPage>;
  getOperationLog?(id: string, signal?: AbortSignal): Promise<OperationLogEntry | null>;
  dismissOperationPlan?(planId: string): Promise<void>;
  forgetBrowser?(): Promise<void>;
  exportDiagnostics?(
    profile: { profileId: string; profileRevision?: string },
    signal?: AbortSignal
  ): Promise<HostDiagnosticsExportResult>;
  getUpdateStatus?(signal?: AbortSignal): Promise<HostUpdateStatus>;
  subscribeUpdateStatus?(listener: (status: HostUpdateStatus) => void): () => void;
  subscribeWatchStopped?(listener: (event: HostWatchStoppedEvent) => void): () => void;
  checkForUpdates?(signal?: AbortSignal): Promise<HostUpdateStatus>;
  downloadUpdate?(signal?: AbortSignal): Promise<HostUpdateStatus>;
  installUpdate?(signal?: AbortSignal): Promise<HostUpdateStatus>;
}

/** Host-only desktop lifecycle hint; it is never Core method input. */
export interface HostWatchStoppedEvent {
  generation: number;
  profileId: string;
  profileRevision: string;
  watch: WatchSnapshot & { status: "stopped"; stoppedAt: string; stopReason: string };
}

export type HostDiagnosticsExportResult =
  | { status: "created" }
  | { status: "cancelled" }
  | { status: "failed" };

export interface HostUpdateStatus {
  currentVersion?: string;
  mode?: "manual";
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
  operationLogs: boolean;
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
  viewUpdateStatus: true,
  operationLogs: false
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
  viewUpdateStatus: false,
  operationLogs: false
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
  viewUpdateStatus: false,
  operationLogs: false
});

export const DESKTOP_C8_APP_UI_CAPABILITIES: Readonly<AppUiCapabilities> = Object.freeze({
  sync: true,
  switchProvider: true,
  repair: true,
  restore: true,
  pruneBackups: true,
  watch: true,
  manageProfiles: true,
  revealProfilePaths: false,
  forgetBrowser: false,
  exportDiagnostics: true,
  viewUpdateStatus: true,
  operationLogs: true
});

export interface PreferenceStore {
  getBackupRetention?(): number | null;
  setBackupRetention?(count: number): void;
  getLocale(): SupportedLocale | null;
  setLocale(locale: SupportedLocale): void;
  getTheme(): ThemeMode | null;
  setTheme(theme: ThemeMode): void;
  getHistoryProjectAlias?(scope: string, projectId: string): string | null;
  setHistoryProjectAlias?(scope: string, projectId: string, alias: string): void;
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
