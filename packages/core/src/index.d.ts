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

export interface CoreFacade {
  getStatus(input: GetStatusInput): Promise<StatusSnapshot>;
  prepareSync(input: PrepareSyncInput): Promise<PlanSummary>;
  applySync(input: ApplyPlanInput): Promise<OperationResult>;
  prepareSwitch(input: PrepareSwitchInput): Promise<PlanSummary>;
  applySwitch(input: ApplyPlanInput): Promise<OperationResult>;
  listBackups(input: ListBackupsInput): Promise<BackupList>;
  prepareRestore(input: PrepareRestoreInput): Promise<PlanSummary>;
  applyRestore(input: ApplyPlanInput): Promise<OperationResult>;
  pruneBackups(input: PruneBackupsInput): Promise<PruneBackupsResult>;
  listHistory(input: ListHistoryInput): Promise<HistoryPage>;
  getHistorySession(input: GetHistorySessionInput): Promise<HistorySessionDetail>;
  startWatch(input: StartWatchInput): Promise<WatchSnapshot>;
  stopWatch(input: WatchReferenceInput): Promise<WatchSnapshot>;
  getWatchStatus(input?: GetWatchStatusInput): Promise<WatchSnapshot | WatchStatusList>;
  getDiagnostics(input: GetDiagnosticsInput): Promise<DiagnosticsSnapshot>;
}

export function createCoreFacade(options: { resolveProfile: ProfileResolver }): CoreFacade;
