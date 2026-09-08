// @ts-check

/** @param {{resolveResource: Function, acknowledge: Function, execute: Function}} handlers */
export function createRestoreRecovery(handlers) {
  if (typeof handlers?.resolveResource !== "function"
      || typeof handlers?.acknowledge !== "function"
      || typeof handlers?.execute !== "function") {
    throw new TypeError("RestoreRecovery requires resource, acknowledgement, and execution handlers.");
  }
  return Object.freeze({
    resolveResource: handlers.resolveResource,
    acknowledge: handlers.acknowledge,
    execute: handlers.execute
  });
}
