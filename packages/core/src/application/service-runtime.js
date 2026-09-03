// @ts-check

// Transitional compatibility only. Product use cases own their orchestration
// in sibling modules; root src/service.js still forwards here for Node 16
// consumers that use the established direct-import surface.
export { getStatus } from "./status.js";
export { applySync, prepareSync } from "./provider-sync.js";
export { applySwitch, prepareSwitch } from "./provider-switch.js";
export { applyRepair, prepareRepair } from "./repair.js";
export { applyRestore, prepareRestore } from "./restore.js";
export { pruneBackups, runPruneBackups } from "./backups.js";
export { waitForManualOperationEnd } from "./runtime-context.js";

import { applyRepair, prepareRepair } from "./repair.js";
import { applyRestore, prepareRestore } from "./restore.js";
import { applySwitch, prepareSwitch } from "./provider-switch.js";
import { applySync, prepareSync } from "./provider-sync.js";

/** @deprecated Compatibility adapter. New transports must use prepareSync/applySync. */
export async function runSync(options = {}) {
  const plan = await prepareSync(options);
  return (await applySync({ schemaVersion: 1, planId: plan.planId })).result;
}

/** @deprecated Compatibility adapter. New transports must use prepareSwitch/applySwitch. */
export async function runSwitch(options = {}) {
  const plan = await prepareSwitch(options);
  return (await applySwitch({ schemaVersion: 1, planId: plan.planId })).result;
}

/** @deprecated Compatibility adapter. New transports must use prepareRepair/applyRepair. */
export async function runRepair(options = {}) {
  const plan = await prepareRepair(options);
  return (await applyRepair({ schemaVersion: 1, planId: plan.planId })).result;
}

/** @deprecated Compatibility adapter. New transports must use prepareRestore/applyRestore. */
export async function runRestore(options = {}) {
  const plan = await prepareRestore(options);
  return (await applyRestore({ schemaVersion: 1, planId: plan.planId })).result;
}
