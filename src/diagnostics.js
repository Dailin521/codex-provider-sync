import { getStatus } from "./service.js";

function countDistribution(distribution) {
  return Object.fromEntries(Object.entries(distribution ?? {}).map(([scope, counts]) => [
    scope,
    Object.values(counts ?? {}).reduce((total, count) => total + (Number.isSafeInteger(count) ? count : 0), 0)
  ]));
}

export async function getDiagnostics(options = {}) {
  const status = await getStatus(options);
  return {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
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
    safety: {
      storageRevision: status.storageRevision,
      pendingRecovery: status.pendingRecovery,
      pendingTransactions: (status.pendingTransactions ?? []).map((transaction) => ({
        operationId: transaction.operationId,
        state: transaction.state
      })),
      operationInProgress: status.operationInProgress,
      rolloutScanComplete: status.rolloutScanComplete,
      lockedRolloutCount: status.lockedRolloutFiles?.length ?? 0,
      projectThreadVisibilityAvailable: status.projectThreadVisibilityAvailable
    }
  };
}
