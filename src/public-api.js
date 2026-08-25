// The only supported Node Core import surface for product entry points.
//
// This is intentionally a thin facade during the staged vNext migration: the
// current implementation stays in its existing modules, while CLI, Web, and
// future desktop transports depend on this stable boundary rather than those
// implementation details.
//
// The runSync/runSwitch/runRestore/runWatch exports are migration adapters.
// New transports must use the prepare/apply APIs once those land in C3; the
// adapters are retained here only for CLI/Web compatibility during migration.

export { CORE_ERROR_CODES, CoreError, toCoreErrorDto } from "./core-error.js";

export {
  applyRestore,
  applySwitch,
  applySync,
  getStatus,
  pruneBackups,
  prepareRestore,
  prepareSwitch,
  prepareSync,
  runPruneBackups,
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
