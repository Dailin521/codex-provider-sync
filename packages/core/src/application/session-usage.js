// @ts-check

import { codexStorage } from "../infrastructure/node-core-ports.js";

/**
 * Same target-scoped, non-mutating occupancy check for Status, Preview and Apply.
 * This is not a count of all open/running Codex threads.
 * @template {{path: string}} T
 * @param {{changes: T[], lockedPaths: string[]}} scan
 */
export async function inspectSessionUsage(scan) {
  /** @type {{writableChanges: T[], lockedChanges: T[]}} */
  const partition = await codexStorage.sessions.splitLockedSessionChanges(scan.changes);
  const lockedPaths = [...new Set([
    ...scan.lockedPaths,
    ...partition.lockedChanges.map((change) => change.path)
  ])].sort((left, right) => left.localeCompare(right));
  return { ...partition, lockedPaths };
}
