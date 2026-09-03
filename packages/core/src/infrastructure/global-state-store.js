// @ts-check

/** @param {Record<string, Function>} port */
export function createGlobalStateStore(port) {
  if (!port || typeof port !== "object" || Array.isArray(port)) throw new TypeError("GlobalStateStore port is missing.");
  return Object.freeze({ ...port });
}
