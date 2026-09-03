// @ts-nocheck

import { createConcurrencyGuard } from "./concurrency-guard.js";
import { createOperationRuntime } from "./operation-runtime.js";
import { createPlanApplyGuard } from "./plan-apply-guard.js";
import { operationResult } from "./operation-result.js";
import { emitProgress } from "./runtime-support.js";
import { getStatus } from "./status.js";
import { createRestoreRecovery } from "../infrastructure/restore-recovery.js";
import { createSqliteTransaction } from "../infrastructure/sqlite-transaction.js";
import { createUndoBackup } from "../infrastructure/undo-backup.js";
import {
  CoreError,
  PlanLedger,
  acknowledgePendingRestore,
  createBackup,
  codexStorage,
  executeRestoreV2,
  normalizeCodexHome,
  operationCoordinator,
  refreshBackupInventory,
  resolveStateDbLockResource
} from "../infrastructure/node-core-ports.js";

const { applySqliteRepairs, updateSqliteProvider } = codexStorage.stateDb;

const planLedger = new PlanLedger();
const concurrencyGuard = createConcurrencyGuard({ coordinator: operationCoordinator });
const planApplyGuard = createPlanApplyGuard({ planLedger, concurrencyGuard });

export const operationRuntime = createOperationRuntime({
  planApplyGuard,
  concurrencyGuard,
  CoreError,
  getStatus,
  normalizeCodexHome,
  toOperationResult: operationResult,
  emitProgress
});

export const sqliteTransaction = createSqliteTransaction({
  updateProvider: updateSqliteProvider,
  repair: applySqliteRepairs
});

export const undoBackup = createUndoBackup({
  capture: createBackup,
  refreshInventory: refreshBackupInventory
});

export const restoreRecovery = createRestoreRecovery({
  resolveResource: resolveStateDbLockResource,
  acknowledge: acknowledgePendingRestore,
  execute: executeRestoreV2
});

export function waitForManualOperationEnd({ codexHome, platform } = {}) {
  return operationRuntime.waitForManualOperationEnd({ codexHome, platform });
}
