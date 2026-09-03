// @ts-check

import path from "node:path";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import { randomUUID } from "node:crypto";

import {
  DEFAULT_BACKUP_RETENTION_COUNT,
  DEFAULT_LOCK_NAME,
  DEFAULT_PROVIDER,
  defaultBackupRoot
} from "../../../../src/constants.js";
import { CoreError } from "../../../../src/core-error.js";
import {
  configDeclaresProvider,
  listConfiguredProviderIds,
  readConfigText,
  readCurrentProviderFromConfigText,
  readProviderModel,
  readRootModelFromConfigText,
  setRootModelInConfigText,
  setRootProviderInConfigText,
  writeConfigText
} from "../../../../src/config-file.js";
import {
  createBackup,
  getBackupRecoveryCoverage,
  getBackupSummary,
  listBackups,
  pruneBackups as pruneManagedBackups,
  refreshBackupInventory,
  resolveRestoreStateDbTargetPath,
  restoreBackup
} from "../../../../src/backup.js";
import { acquireLock, inspectPathLock } from "../../../../src/locking.js";
import { getHistorySession, listHistory } from "../../../../src/history.js";
import { resolveStateDbLockResource } from "../../../../src/state-db-lock.js";
import { PlanLedger } from "../../../../src/plan-ledger.js";
import { sharedOperationCoordinator as operationCoordinator } from "../../../../src/operation-coordinator.js";
import {
  captureBackupRevision,
  captureOperationRevisions,
  captureStorageRevision,
  revisionMismatch,
  sha256Revision,
  stableStringify
} from "../../../../src/operation-revision.js";
import {
  applySessionChanges,
  collectDiagnosticsFacts,
  collectProviderChanges,
  collectRepairChanges,
  collectSessionChanges,
  collectStatusRolloutMetadata,
  splitLockedSessionChanges,
  summarizeProviderCounts
} from "../../../../src/session-files.js";
import {
  applySqliteRepairs,
  assertSqliteWritable,
  detectStateDb,
  readSqliteProviderCounts,
  readSqliteRepairStats,
  updateSqliteProvider
} from "../../../../src/sqlite-state.js";
import {
  cwdStatsFromThreadCwdMap,
  readProjectThreadVisibility,
  readThreadCwdStats,
  readWorkspaceRootRepairStats,
  syncWorkspaceRoots
} from "../../../../src/workspace-roots.js";
import {
  assertSqliteAccessSupported,
  ensureCodexHome,
  isConfiguredSqliteHome,
  missingConfiguredStateDbError,
  normalizeCodexHome,
  resolveStorageLayout,
  withStateDbLocation
} from "../../../../src/storage-layout.js";
import {
  findPendingTransactions,
  getStartedJournalTargets,
  readTransactionJournal
} from "../../../../src/transaction-journal.js";
import {
  acknowledgePendingRestore,
  captureStableRestoreSource,
  executeRestoreV2,
  restoreJournalCoverageIsComplete,
  restoreJournalMatchesPhysicalHome,
  restoreJournalMatchesSource
} from "../../../../src/restore-v2.js";

import { createCodexStorage } from "./codex-storage.js";

// These adapters are the migration seam from the Core use cases to the proven
// Node storage implementation. Business orchestration consumes the four
// bounded ports instead of importing root storage modules directly.
export const codexStorage = createCodexStorage({
  config: {
    configDeclaresProvider,
    listConfiguredProviderIds,
    readConfigText,
    readCurrentProviderFromConfigText,
    readProviderModel,
    readRootModelFromConfigText,
    setRootModelInConfigText,
    setRootProviderInConfigText,
    writeConfigText
  },
  sessions: {
    applySessionChanges,
    collectDiagnosticsFacts,
    collectProviderChanges,
    collectRepairChanges,
    collectSessionChanges,
    collectStatusRolloutMetadata,
    getHistorySession,
    listHistory,
    splitLockedSessionChanges,
    summarizeProviderCounts
  },
  stateDb: {
    applySqliteRepairs,
    assertSqliteWritable,
    detectStateDb,
    readSqliteProviderCounts,
    readSqliteRepairStats,
    updateSqliteProvider
  },
  globalState: {
    cwdStatsFromThreadCwdMap,
    readProjectThreadVisibility,
    readThreadCwdStats,
    readWorkspaceRootRepairStats,
    syncWorkspaceRoots
  }
});

export {
  DEFAULT_BACKUP_RETENTION_COUNT,
  DEFAULT_LOCK_NAME,
  DEFAULT_PROVIDER,
  defaultBackupRoot,
  CoreError,
  createBackup,
  getBackupRecoveryCoverage,
  getBackupSummary,
  listBackups,
  pruneManagedBackups,
  refreshBackupInventory,
  resolveRestoreStateDbTargetPath,
  restoreBackup,
  acquireLock,
  inspectPathLock,
  resolveStateDbLockResource,
  PlanLedger,
  operationCoordinator,
  captureBackupRevision,
  captureOperationRevisions,
  captureStorageRevision,
  revisionMismatch,
  sha256Revision,
  stableStringify,
  assertSqliteAccessSupported,
  ensureCodexHome,
  isConfiguredSqliteHome,
  missingConfiguredStateDbError,
  normalizeCodexHome,
  resolveStorageLayout,
  withStateDbLocation,
  findPendingTransactions,
  getStartedJournalTargets,
  readTransactionJournal,
  acknowledgePendingRestore,
  captureStableRestoreSource,
  executeRestoreV2,
  restoreJournalCoverageIsComplete,
  restoreJournalMatchesPhysicalHome,
  restoreJournalMatchesSource,
  path,
  fs,
  fsSync,
  randomUUID
};
