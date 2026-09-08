import type { StatusSnapshot } from "@codex-provider-sync/contracts";

export function hasStatusSnapshot(status?: StatusSnapshot): status is StatusSnapshot {
  return Boolean(status && typeof status.snapshotAt === "string" && Number.isFinite(Date.parse(status.snapshotAt)));
}

/** Presentation only: use Core's alignment verdict, never infer it from counts or Apply outcome. */
export function statusAlignment(status?: StatusSnapshot): "aligned" | "notAligned" | "unknown" {
  if (!hasStatusSnapshot(status) || status.operationInProgress || status.pendingRecovery
    || status.statusReadBlocked || !status.rolloutScanComplete || status.lockedRolloutFiles.length > 0) return "unknown";
  const alignment = status.alignment;
  if (!alignment || typeof alignment !== "object" || Array.isArray(alignment)
    || alignment.sqliteReadable !== true || typeof alignment.aligned !== "boolean") return "unknown";
  return alignment.aligned ? "aligned" : "notAligned";
}

export type PostWriteStatus = {
  operationId: string;
  state: "checking" | "unverified" | "received";
  snapshot?: StatusSnapshot;
};
