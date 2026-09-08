import type {
  ApplyPlanInput,
  BackupList,
  DiagnosticsSnapshot,
  FileUpdateTiming,
  GetDiagnosticsInput,
  GetHistorySessionInput,
  GetStatusInput,
  GetWatchStatusInput,
  HistoryPage,
  HistorySessionDetail,
  ListBackupsInput,
  ListHistoryInput,
  OperationResult,
  PlanSummary,
  ProgressEvent,
  PrepareRepairInput,
  PrepareRestoreInput,
  PrepareSwitchInput,
  PrepareSyncInput,
  ProfileSelector,
  PruneBackupsInput,
  PruneBackupsResult,
  StartWatchInput,
  StatusSnapshot,
  WatchReferenceInput,
  WatchSnapshot,
  WatchStatusList
} from "@codex-provider-sync/contracts";

export interface ResolvedProfile {
  id: string;
  revision: string;
  codexHome: string;
  sqliteHome?: string;
}

export type ProfileResolver = (
  selector: ProfileSelector
) => ResolvedProfile | Promise<ResolvedProfile>;

export interface WatchActivityEvent {
  schemaVersion: 1;
  event: "started" | "finished";
  activityId: string;
  watchId: string;
  profileId: string;
  profileRevision?: string;
  backupId?: string;
  failedStage?: string;
  failureCode?: string;
  partialReason?: string;
  retryRecommended?: boolean;
  startedAt?: string;
  finishedAt?: string;
  reason?: string;
  outcome?: "completed" | "partial" | "failed";
  errorCode?: string;
  changedSessionFiles?: number;
  sqliteRowsUpdated?: number;
  skippedLockedRolloutFiles?: number;
  fileUpdateTiming?: FileUpdateTiming;
}

/** @internal Trusted host lifecycle event. Never accepted from product input. */
export interface WatchStoppedEvent {
  profileId: string;
  profileRevision: string;
  watch: WatchSnapshot & { status: "stopped"; stoppedAt: string; stopReason: string };
}

/** @internal Trusted host control. Never expose this object to HTTP, IPC, or Renderer input. */
export interface CoreHostOperationControl {
  signal?: AbortSignal;
  onOperationStarted?(value: {
    operationId: string;
    operation: "sync" | "switch" | "repair" | "restore";
  }): void | Promise<void>;
  onProgress?(event: ProgressEvent): void | Promise<void>;
}

export interface CoreFacade {
  getStatus(input: GetStatusInput): Promise<StatusSnapshot>;
  prepareSync(input: PrepareSyncInput): Promise<PlanSummary>;
  applySync(input: ApplyPlanInput, control?: CoreHostOperationControl): Promise<OperationResult>;
  prepareSwitch(input: PrepareSwitchInput): Promise<PlanSummary>;
  applySwitch(input: ApplyPlanInput, control?: CoreHostOperationControl): Promise<OperationResult>;
  prepareRepair(input: PrepareRepairInput, control?: CoreHostOperationControl): Promise<PlanSummary>;
  applyRepair(input: ApplyPlanInput, control?: CoreHostOperationControl): Promise<OperationResult>;
  listBackups(input: ListBackupsInput): Promise<BackupList>;
  prepareRestore(input: PrepareRestoreInput): Promise<PlanSummary>;
  applyRestore(input: ApplyPlanInput, control?: CoreHostOperationControl): Promise<OperationResult>;
  pruneBackups(input: PruneBackupsInput): Promise<PruneBackupsResult>;
  listHistory(input: ListHistoryInput): Promise<HistoryPage>;
  getHistorySession(input: GetHistorySessionInput): Promise<HistorySessionDetail>;
  startWatch(input: StartWatchInput): Promise<WatchSnapshot>;
  stopWatch(input: WatchReferenceInput): Promise<WatchSnapshot>;
  getWatchStatus(input?: GetWatchStatusInput): Promise<WatchSnapshot | WatchStatusList>;
  getDiagnostics(input: GetDiagnosticsInput, control?: CoreHostOperationControl): Promise<DiagnosticsSnapshot>;
}

export function createCoreFacade(options: { resolveProfile: ProfileResolver; /** Trusted local Desktop host only; never accepted from product input. */ includeLocalDisplayPaths?: boolean; onWatchActivity?(event: WatchActivityEvent): void | Promise<void>; onWatchStopped?(event: WatchStoppedEvent): void | Promise<void> }): CoreFacade;
