import type { JsonObject, JsonValue } from "./json.js";

export const CONTRACT_SCHEMA_VERSION = 1 as const;
export const CORE_PROTOCOL_VERSION = 1 as const;

export type ContractSchemaVersion = typeof CONTRACT_SCHEMA_VERSION;
export type CoreProtocolVersion = typeof CORE_PROTOCOL_VERSION;
export type OperationKind = "sync" | "switch" | "restore" | "prune" | "watch";
export type OperationOutcome =
  | "completed"
  | "partial"
  | "failed_rolled_back"
  | "recovery_required"
  | "cancelled"
  | "stale";

export interface ProfileSelector {
  profileId: string;
  profileRevision?: string;
}

export interface GetStatusInput {
  profile: ProfileSelector;
}

export interface PrepareSyncInput extends GetStatusInput {
  keepCount?: number;
}

export type SwitchModelMode = "provider-default" | "keep-root-model" | "explicit";

export interface PrepareSwitchInput extends GetStatusInput {
  provider: string;
  modelMode: SwitchModelMode;
  model?: string;
  keepCount?: number;
}

export interface ApplyPlanInput {
  schemaVersion: ContractSchemaVersion;
  planId: string;
}

export interface ListBackupsInput extends GetStatusInput {}

export interface PrepareRestoreInput extends GetStatusInput {
  backupId: string;
  restoreConfig: boolean;
  restoreDatabase: boolean;
  restoreSessions: boolean;
  allowSqliteHomeRelocation?: boolean;
  relocationTargetProfileId?: string;
}

export interface PruneBackupsInput extends GetStatusInput {
  keepCount: number;
}

export interface ListHistoryInput extends GetStatusInput {
  page?: number;
  pageSize?: number;
  query?: string;
  project?: string;
  provider?: string;
  archived?: "all" | "active" | "archived";
}

export interface GetHistorySessionInput extends GetStatusInput {
  sessionId: string;
  messageLimit?: number;
}

export interface StartWatchInput extends GetStatusInput {
  includeStateDb?: boolean;
  debounceMs?: number;
  once?: boolean;
}

export interface WatchReferenceInput {
  watchId: string;
}

export interface GetWatchStatusInput {
  watchId?: string;
}

export interface GetDiagnosticsInput extends GetStatusInput {}

export type ProviderDistribution = Record<string, Record<string, number>>;

export interface StatusSnapshot {
  schemaVersion: ContractSchemaVersion;
  snapshotAt: string;
  storageRevision: string;
  profile: {
    id: string;
    revision: string;
  };
  currentProvider: string;
  currentModel?: string | null;
  rolloutCounts: ProviderDistribution;
  modelCounts?: ProviderDistribution;
  sqliteCounts: JsonValue;
  codexHomeSource: string;
  sqliteHomeSource: string;
  backupSummary: {
    count: number;
    totalBytes: number;
  };
  pendingRecovery: boolean;
  pendingTransactions: JsonObject[];
  operationInProgress: JsonObject | null;
  rolloutScanComplete: boolean;
  lockedRolloutFiles: string[];
  [extension: string]: JsonValue | undefined;
}

export interface PlanSummary {
  schemaVersion: ContractSchemaVersion;
  planId: string;
  operation: "sync" | "switch" | "restore";
  createdAt: string;
  expiresAt: string;
  profile: {
    id: string;
    revision: string;
  };
  storageRevision: string;
  configRevision: string;
  rolloutRevision: string;
  stateDbRevision: string;
  backupRevision?: string;
  target: JsonObject;
  impact: JsonObject;
  warnings: string[];
  requiresConfirmation: boolean;
}

export interface ManagedBackup {
  backupId: string;
  createdAt?: string;
  sizeBytes: number;
  metadata: JsonObject;
}

export interface BackupList {
  backups: ManagedBackup[];
}

export interface OperationResult<Result extends JsonValue = JsonObject> {
  schemaVersion: ContractSchemaVersion;
  operationId: string;
  operation: "sync" | "switch" | "restore";
  outcome: OperationOutcome;
  backup: { backupId: string } | null;
  warnings: string[];
  result: Result;
}

export interface PruneBackupsResult {
  deletedCount: number;
  remainingCount: number;
  freedBytes: number;
}

export interface HistorySessionSummary {
  id: string;
  title: string;
  provider: string;
  model?: string | null;
  archived: boolean;
  createdAt?: string;
  updatedAt: string;
  messageCount: number;
}

export interface HistoryPage {
  page: number;
  pageSize: number;
  total: number;
  hasNextPage: boolean;
  sessions: HistorySessionSummary[];
}

export interface HistoryMessage {
  role: string;
  text: string;
  timestamp?: string;
  sequence: number;
}

export interface HistorySessionDetail {
  session: HistorySessionSummary;
  messages: HistoryMessage[];
  truncated: boolean;
  returnedMessageCount: number;
}

export interface WatchSnapshot {
  schemaVersion: ContractSchemaVersion;
  watchId: string;
  status: "running" | "stopping" | "stopped";
  startedAt: string;
  stoppedAt: string | null;
  stopReason: string | null;
  includeStateDb: boolean;
  once: boolean;
}

export interface WatchStatusList {
  schemaVersion: ContractSchemaVersion;
  watches: WatchSnapshot[];
}

export interface DiagnosticsSnapshot {
  schemaVersion: ContractSchemaVersion;
  generatedAt: string;
  runtime: JsonObject;
  storage: JsonObject;
  provider: JsonObject;
  safety: JsonObject;
}

export interface ProgressEvent {
  stage: string;
  status: string;
  progress?: number;
  count?: number;
}

export const CORE_METHODS = [
  "getStatus",
  "prepareSync",
  "applySync",
  "prepareSwitch",
  "applySwitch",
  "listBackups",
  "prepareRestore",
  "applyRestore",
  "pruneBackups",
  "listHistory",
  "getHistorySession",
  "startWatch",
  "stopWatch",
  "getWatchStatus",
  "getDiagnostics"
] as const;

export type CoreMethodName = typeof CORE_METHODS[number];

export interface CoreMethodMap {
  getStatus: { input: GetStatusInput; output: StatusSnapshot };
  prepareSync: { input: PrepareSyncInput; output: PlanSummary };
  applySync: { input: ApplyPlanInput; output: OperationResult };
  prepareSwitch: { input: PrepareSwitchInput; output: PlanSummary };
  applySwitch: { input: ApplyPlanInput; output: OperationResult };
  listBackups: { input: ListBackupsInput; output: BackupList };
  prepareRestore: { input: PrepareRestoreInput; output: PlanSummary };
  applyRestore: { input: ApplyPlanInput; output: OperationResult };
  pruneBackups: { input: PruneBackupsInput; output: PruneBackupsResult };
  listHistory: { input: ListHistoryInput; output: HistoryPage };
  getHistorySession: { input: GetHistorySessionInput; output: HistorySessionDetail };
  startWatch: { input: StartWatchInput; output: WatchSnapshot };
  stopWatch: { input: WatchReferenceInput; output: WatchSnapshot };
  getWatchStatus: { input: GetWatchStatusInput; output: WatchSnapshot | WatchStatusList };
  getDiagnostics: { input: GetDiagnosticsInput; output: DiagnosticsSnapshot };
}
