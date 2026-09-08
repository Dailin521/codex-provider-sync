import type { FileUpdateTiming } from "@codex-provider-sync/contracts";

export type OperationLogStatus =
  | "running"
  | "awaiting-confirmation"
  | "completed"
  | "partial"
  | "failed"
  | "cancelled"
  | "dismissed"
  | "interrupted";

export interface OperationLogStage {
  stage: string;
  status: "running" | "completed" | "failed";
  startedAt: string;
  completedAt?: string;
  durationMs?: number;
  progress?: number;
  count?: number;
}

export interface OperationLogEntry {
  schemaVersion: 1;
  id: string;
  operation: string;
  profileId?: string;
  profileRevision?: string;
  startedAt: string;
  completedAt?: string;
  activeDurationMs: number;
  wallDurationMs?: number;
  status: OperationLogStatus;
  outcome?: string;
  errorCode?: string;
  /** A bounded public Core error-detail reason; it never contains raw errors or paths. */
  errorReason?: "profile" | "config" | "storage" | "rollout" | "state-db" | "backup" | "provider-not-configured";
  failedStage?: string;
  failureCode?: string;
  partialReason?: "locked-session" | "rollout-changed" | "mutation-failed";
  retryRecommended?: boolean;
  requestIds: string[];
  planId?: string;
  operationId?: string;
  backupId?: string;
  /** Safe, display-only Provider identifier captured from a validated Prepare result. */
  targetProvider?: string;
  /** Prepare-time impact summary. It is deliberately distinct from post-Apply counts. */
  previewCounts?: {
    rolloutFilesToChange: number;
    sqliteRowsToChange: number;
    lockedRolloutFiles: number;
  };
  /** Switch Prepare intent. Older records intentionally omit this rather than reconstruct it. */
  switchPlan?: {
    previousProvider: string;
    targetProvider: string;
    previousRootModel: string | null;
    targetRootModel: string | null;
    modelMode: "provider-default" | "keep-root-model" | "explicit";
  };
  counts: Record<string, number>;
  fileUpdateTiming?: FileUpdateTiming;
  warnings: string[];
  stages: OperationLogStage[];
}

export interface OperationLogListInput {
  schemaVersion: 1;
  page: number;
  pageSize: number;
  profileId?: string;
  profileRevision?: string;
  operation?: string;
  status?: OperationLogStatus;
}

export interface OperationLogListResponse {
  schemaVersion: 1;
  page: number;
  pageSize: number;
  total: number;
  hasNextPage: boolean;
  entries: OperationLogEntry[];
}

export interface OperationLogDetailInput {
  schemaVersion: 1;
  id: string;
}
