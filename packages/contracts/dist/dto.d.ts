import type { JsonObject, JsonValue } from "./json.js";
export declare const CONTRACT_SCHEMA_VERSION: 1;
export declare const CORE_PROTOCOL_VERSION: 1;
export type ContractSchemaVersion = typeof CONTRACT_SCHEMA_VERSION;
export type CoreProtocolVersion = typeof CORE_PROTOCOL_VERSION;
export type OperationKind = "sync" | "switch" | "repair" | "restore" | "prune" | "watch";
export type OperationOutcome = "completed" | "partial" | "failed_rolled_back" | "recovery_required" | "cancelled" | "stale";
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
export type RepairTarget = "models" | "cwd" | "userEvent" | "workspaceRoots";
export interface PrepareRepairInput extends GetStatusInput {
    targets: RepairTarget[];
    keepCount?: number;
}
export interface ApplyPlanInput {
    schemaVersion: ContractSchemaVersion;
    planId: string;
}
export interface ListBackupsInput extends GetStatusInput {
}
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
export interface GetDiagnosticsInput extends GetStatusInput {
}
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
    operation: "sync" | "switch" | "repair" | "restore";
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
    operation: "sync" | "switch" | "repair" | "restore";
    outcome: OperationOutcome;
    backup: {
        backupId: string;
    } | null;
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
    messageCountKnown?: boolean;
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
export interface DiagnosticsRuntime {
    node: string;
    platform: string;
    arch: string;
}
export type DiagnosticsSqliteHomeSource = "cli" | "config" | "env" | "default" | "unknown";
export interface DiagnosticsStorage {
    sqliteHomeSource: DiagnosticsSqliteHomeSource;
    stateDbFound: boolean;
    sqliteSupported: boolean;
}
export interface DiagnosticsProviderDistribution {
    sessions: Record<string, number>;
    archived_sessions: Record<string, number>;
}
export interface DiagnosticsSqliteDistribution extends DiagnosticsProviderDistribution {
    unreadable?: true;
}
export interface DiagnosticsProvider {
    current: string;
    implicit: boolean;
    configured: string[];
    rolloutCounts: DiagnosticsProviderDistribution;
    sqliteCounts: DiagnosticsSqliteDistribution | null;
}
export type DiagnosticsTransactionState = "prepared" | "applying" | "applied" | "skipped" | "committing" | "committed-pending-ack" | "rollback-pending" | "rollingBack" | "recovery-required" | "recoveryRequired" | "unknown";
export interface DiagnosticsPendingTransaction {
    operationId: string | null;
    operationKind: "sync" | "switch" | "restore";
    state: DiagnosticsTransactionState;
    sourceBackupId: string | null;
    preRestoreSnapshotId: string | null;
}
export interface DiagnosticsOperationState {
    operationId?: string;
    operation?: "sync" | "switch" | "repair" | "restore" | "prune" | "watch" | "unknown";
    actor?: "manual" | "watch" | "external";
    startedAt?: string;
    busyScope?: "codex-home" | "state-db";
    lockState?: string;
    errorCode?: string;
}
export interface DiagnosticsSafety {
    storageRevision?: string;
    pendingRecovery: boolean;
    pendingTransactions: DiagnosticsPendingTransaction[];
    operationInProgress: DiagnosticsOperationState | null;
    rolloutScanComplete: boolean;
    lockedRolloutCount: number;
    projectThreadVisibilityAvailable: boolean;
}
export interface DiagnosticsIssues {
    rootModelAvailable: boolean;
    rolloutModelFilesNeedingRepair: number;
    sqliteModelRowsNeedingRepair: number;
    cwdRowsNeedingRepair: number;
    userEventRowsNeedingRepair: number;
    workspaceRootsNeedingRepair: number;
    encryptedContentFiles: number;
}
export interface DiagnosticsSnapshot {
    schemaVersion: ContractSchemaVersion;
    generatedAt: string;
    runtime: DiagnosticsRuntime;
    storage: DiagnosticsStorage;
    provider: DiagnosticsProvider;
    issues: DiagnosticsIssues;
    safety: DiagnosticsSafety;
}
export interface ProgressEvent {
    stage: string;
    status: string;
    progress?: number;
    count?: number;
}
export declare const CORE_METHODS: readonly ["getStatus", "prepareSync", "applySync", "prepareSwitch", "applySwitch", "prepareRepair", "applyRepair", "listBackups", "prepareRestore", "applyRestore", "pruneBackups", "listHistory", "getHistorySession", "startWatch", "stopWatch", "getWatchStatus", "getDiagnostics"];
export type CoreMethodName = typeof CORE_METHODS[number];
export interface CoreMethodMap {
    getStatus: {
        input: GetStatusInput;
        output: StatusSnapshot;
    };
    prepareSync: {
        input: PrepareSyncInput;
        output: PlanSummary;
    };
    applySync: {
        input: ApplyPlanInput;
        output: OperationResult;
    };
    prepareSwitch: {
        input: PrepareSwitchInput;
        output: PlanSummary;
    };
    applySwitch: {
        input: ApplyPlanInput;
        output: OperationResult;
    };
    prepareRepair: {
        input: PrepareRepairInput;
        output: PlanSummary;
    };
    applyRepair: {
        input: ApplyPlanInput;
        output: OperationResult;
    };
    listBackups: {
        input: ListBackupsInput;
        output: BackupList;
    };
    prepareRestore: {
        input: PrepareRestoreInput;
        output: PlanSummary;
    };
    applyRestore: {
        input: ApplyPlanInput;
        output: OperationResult;
    };
    pruneBackups: {
        input: PruneBackupsInput;
        output: PruneBackupsResult;
    };
    listHistory: {
        input: ListHistoryInput;
        output: HistoryPage;
    };
    getHistorySession: {
        input: GetHistorySessionInput;
        output: HistorySessionDetail;
    };
    startWatch: {
        input: StartWatchInput;
        output: WatchSnapshot;
    };
    stopWatch: {
        input: WatchReferenceInput;
        output: WatchSnapshot;
    };
    getWatchStatus: {
        input: GetWatchStatusInput;
        output: WatchSnapshot | WatchStatusList;
    };
    getDiagnostics: {
        input: GetDiagnosticsInput;
        output: DiagnosticsSnapshot;
    };
}
