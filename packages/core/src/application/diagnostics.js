// @ts-check

import { getDiagnosticSnapshot } from "./status.js";
import { codexStorage } from "../infrastructure/node-core-ports.js";
import { emitProgress, throwIfAborted } from "./runtime-support.js";

/** @param {Record<string, Record<string, number>> | null | undefined} distribution */
function countDistribution(distribution) {
  return Object.fromEntries(Object.entries(distribution ?? {}).map(([scope, counts]) => [
    scope,
    Object.values(counts ?? {}).reduce((total, count) => total + (Number.isSafeInteger(count) ? count : 0), 0)
  ]));
}

/**
 * Explicit, one-shot full diagnostic scan. It is deliberately separate from
 * Status so UI refreshes never open rollout bodies in the background.
 * @param {Record<string, unknown>} [options]
 */
export async function getDiagnostics(options = {}) {
  const control = /** @type {import("../index.js").CoreHostOperationControl | undefined} */ (options.requestControl);
  const { onProgress, signal } = control ?? {};
  throwIfAborted(signal);
  emitProgress(onProgress, { stage: "prepare_diagnostics", status: "running" });
  const status = await getDiagnosticSnapshot(options);
  throwIfAborted(signal);
  emitProgress(onProgress, { stage: "inspect_history_integrity", status: "running" });
  const historyIntegrity = await codexStorage.sessions.inspectHistoryIntegrity(status.codexHome, { onProgress, signal });
  throwIfAborted(signal);
  emitProgress(onProgress, { stage: "finish_diagnostics", status: "completed" });
  return {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    historyIntegrity,
    runtime: {
      node: process.version,
      platform: process.platform,
      arch: process.arch
    },
    storage: {
      codexHome: status.codexHome,
      sqliteHome: status.sqliteHome,
      sqliteHomeSource: status.sqliteHomeSource,
      stateDbLocation: status.stateDbLocation,
      sqliteAccess: status.sqliteAccess
    },
    provider: {
      current: status.currentProvider,
      implicit: status.currentProviderImplicit,
      configured: status.configuredProviders,
      rolloutCounts: status.rolloutCounts,
      sqliteCounts: status.sqliteCounts,
      rolloutTotals: countDistribution(status.rolloutCounts),
      sqliteTotals: countDistribution(status.sqliteCounts)
    },
    issues: status.diagnosticIssues ?? {
      rootModelAvailable: false,
      rolloutModelFilesNeedingRepair: 0,
      sqliteModelRowsNeedingRepair: 0,
      cwdRowsNeedingRepair: 0,
      userEventRowsNeedingRepair: 0,
      workspaceRootsNeedingRepair: 0,
      encryptedContentFiles: 0
    },
    safety: {
      storageRevision: status.storageRevision,
      pendingRecovery: status.pendingRecovery,
      pendingTransactions: (/** @type {Array<Record<string, unknown>>} */ (status.pendingTransactions ?? [])).map((transaction) => ({
        operationId: transaction.operationId,
        operationKind: transaction.operationKind,
        state: transaction.state,
        sourceBackupId: transaction.sourceBackupId,
        preRestoreSnapshotId: transaction.preRestoreSnapshotId
      })),
      operationInProgress: status.operationInProgress,
      rolloutScanComplete: Boolean(status.diagnosticIssues) && status.rolloutScanComplete
        && !status.statusReadBlocked && !status.operationInProgress,
      lockedRolloutCount: status.lockedRolloutFiles?.length ?? 0,
      projectThreadVisibilityAvailable: status.projectThreadVisibilityAvailable
    }
  };
}

export function createDiagnosticsUseCase() {
  return Object.freeze({ getDiagnostics });
}
