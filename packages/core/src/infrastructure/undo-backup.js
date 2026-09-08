// @ts-check

/** @param {{capture: Function, refreshInventory: Function}} handlers */
export function createUndoBackup(handlers) {
  if (typeof handlers?.capture !== "function" || typeof handlers?.refreshInventory !== "function") {
    throw new TypeError("UndoBackup requires capture and refreshInventory.");
  }
  return Object.freeze({ capture: handlers.capture, refreshInventory: handlers.refreshInventory });
}
