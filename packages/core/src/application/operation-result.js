// @ts-nocheck

import { path } from "../infrastructure/node-core-ports.js";

export function operationWarnings(result) {
  return [
    result?.partialWarning,
    result?.autoPruneWarning,
    result?.backupInventoryWarning,
    result?.modelSync?.warning
  ].filter((warning) => typeof warning === "string" && warning.trim());
}

export function operationResult(operation, operationId, result, sourceBackup = null) {
  const partial = result?.partial === true
    || (Array.isArray(result?.skippedLockedRolloutFiles)
      && result.skippedLockedRolloutFiles.length > 0);
  const backupDir = result?.backupDir ?? sourceBackup?.backupDir ?? null;
  return {
    schemaVersion: 1,
    operationId,
    operation,
    outcome: partial ? "partial" : "completed",
    backup: backupDir
      ? {
          backupId: operation === "restore" ? sourceBackup?.backupId : path.basename(backupDir),
          path: backupDir
        }
      : null,
    warnings: operationWarnings(result),
    result
  };
}
