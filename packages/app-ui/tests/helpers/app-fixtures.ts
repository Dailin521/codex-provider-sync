import type { PlanSummary, StatusSnapshot } from "@codex-provider-sync/contracts";

export function statusFor(profileRevision = "profile-r1"): StatusSnapshot {
  return {
    schemaVersion: 1,
    snapshotAt: "2026-08-27T00:00:00.000Z",
    storageRevision: `storage-${profileRevision}`,
    profile: { id: "default", revision: profileRevision },
    currentProvider: "openai",
    configuredProviders: ["openai", "relay"],
    rolloutCounts: { sessions: { openai: 1 }, archived_sessions: {} },
    sqliteCounts: { sessions: { openai: 1 }, archived_sessions: {} },
    codexHomeSource: "profile",
    sqliteHomeSource: "default",
    backupSummary: { count: 0, totalBytes: 0 },
    pendingRecovery: false,
    pendingTransactions: [],
    operationInProgress: null,
    rolloutScanComplete: true,
    lockedRolloutFiles: []
  };
}

export function syncPlanFor(profileRevision = "profile-r1", planId = "plan-sync-1"): PlanSummary {
  return {
    schemaVersion: 1,
    planId,
    operation: "sync",
    createdAt: "2026-08-27T00:00:00.000Z",
    expiresAt: "2026-08-27T00:10:00.000Z",
    profile: { id: "default", revision: profileRevision },
    storageRevision: `storage-${profileRevision}`,
    configRevision: "config-r1",
    rolloutRevision: "rollout-r1",
    stateDbRevision: "state-db-r1",
    target: { provider: "openai" },
    impact: {
      rolloutFilesToChange: 1,
      sqliteRowsToChange: 1,
      backupExpected: true
    },
    warnings: [],
    requiresConfirmation: true
  };
}
