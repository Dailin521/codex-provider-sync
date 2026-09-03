// @ts-check

/** @param {{updateProvider: Function, repair: Function}} handlers */
export function createSqliteTransaction(handlers) {
  if (typeof handlers?.updateProvider !== "function" || typeof handlers?.repair !== "function") {
    throw new TypeError("SqliteTransaction requires provider and repair writers.");
  }
  return Object.freeze({ updateProvider: handlers.updateProvider, repair: handlers.repair });
}
