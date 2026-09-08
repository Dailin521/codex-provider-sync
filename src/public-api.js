// The only supported Node Core import surface for product entry points.
//
// This remains the root-package compatibility facade for CLI and Local Web.
// Business orchestration now lives under packages/core; root service, Watch,
// and Diagnostics modules forward to that implementation so published Node 16
// consumers keep the established package surface.
//
// The runSync/runSwitch/runRestore/runWatch exports are migration adapters.
// New transports must use the prepare/apply APIs once those land in C3; the
// adapters are retained here only for CLI/Web compatibility during migration.

export { CORE_ERROR_CODES, CoreError, toCoreErrorDto } from "./core-error.js";

export {
  applyRepair,
  applyRestore,
  applySwitch,
  applySync,
  getStatus,
  pruneBackups,
  prepareRestore,
  prepareRepair,
  prepareSwitch,
  prepareSync,
  runPruneBackups,
  runRepair,
  runRestore,
  runSwitch,
  runSync
} from "./service.js";

export { getDiagnostics } from "./diagnostics.js";

export { listBackups } from "./backup.js";
export { getHistorySession, listHistory } from "./history.js";
export { readConfigText, readRootModelFromConfigText } from "./config-file.js";
export { detectStateDb } from "./sqlite-state.js";
export {
  ensureCodexHome,
  resolveStorageLayout,
  withStateDbLocation
} from "./storage-layout.js";
export { getWatchStatus, runWatch, startWatch, stopWatch } from "./watch.js";
