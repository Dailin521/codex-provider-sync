import type {
  ApplyPlanInput,
  BackupList,
  DiagnosticsSnapshot,
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

/** @internal Trusted host control. Never expose this object to HTTP, IPC, or Renderer input. */
export interface CoreHostOperationControl {
  signal?: AbortSignal;
  onOperationStarted?(value: {
    operationId: string;
    operation: "sync" | "switch" | "restore";
  }): void | Promise<void>;
  onProgress?(event: ProgressEvent): void | Promise<void>;
}

export interface CoreFacade {
  getStatus(input: GetStatusInput): Promise<StatusSnapshot>;
  prepareSync(input: PrepareSyncInput): Promise<PlanSummary>;
  applySync(input: ApplyPlanInput, control?: CoreHostOperationControl): Promise<OperationResult>;
  prepareSwitch(input: PrepareSwitchInput): Promise<PlanSummary>;
  applySwitch(input: ApplyPlanInput, control?: CoreHostOperationControl): Promise<OperationResult>;
  listBackups(input: ListBackupsInput): Promise<BackupList>;
  prepareRestore(input: PrepareRestoreInput): Promise<PlanSummary>;
  applyRestore(input: ApplyPlanInput, control?: CoreHostOperationControl): Promise<OperationResult>;
  pruneBackups(input: PruneBackupsInput): Promise<PruneBackupsResult>;
  listHistory(input: ListHistoryInput): Promise<HistoryPage>;
  getHistorySession(input: GetHistorySessionInput): Promise<HistorySessionDetail>;
  startWatch(input: StartWatchInput): Promise<WatchSnapshot>;
  stopWatch(input: WatchReferenceInput): Promise<WatchSnapshot>;
  getWatchStatus(input?: GetWatchStatusInput): Promise<WatchSnapshot | WatchStatusList>;
  getDiagnostics(input: GetDiagnosticsInput): Promise<DiagnosticsSnapshot>;
}

export function createCoreFacade(options: { resolveProfile: ProfileResolver }): CoreFacade;
