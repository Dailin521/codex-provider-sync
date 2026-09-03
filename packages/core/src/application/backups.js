// @ts-nocheck

import {
  DEFAULT_BACKUP_RETENTION_COUNT,
  CoreError,
  acquireLock,
  ensureCodexHome,
  listBackups as listManagedBackups,
  normalizeCodexHome,
  pruneManagedBackups,
  resolveStorageLayout
} from "../infrastructure/node-core-ports.js";

export async function runPruneBackups({
  codexHome: explicitCodexHome,
  keepCount = DEFAULT_BACKUP_RETENTION_COUNT
} = {}) {
  if (!Number.isInteger(keepCount) || keepCount < 0) {
    throw new CoreError(
      "INVALID_INPUT",
      `Invalid keep count: ${keepCount}. Expected a non-negative integer.`
    );
  }
  const codexHome = normalizeCodexHome(explicitCodexHome);
  await ensureCodexHome(resolveStorageLayout({ codexHome, env: {} }));
  const releaseLock = await acquireLock(codexHome, "prune-backups");
  try {
    return await pruneManagedBackups(codexHome, keepCount);
  } finally {
    await releaseLock();
  }
}

export async function pruneBackups(options = {}) {
  return runPruneBackups(options);
}

export function createBackupsUseCase() {
  return Object.freeze({ listBackups: listManagedBackups, pruneBackups });
}
