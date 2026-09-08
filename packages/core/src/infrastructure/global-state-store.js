// @ts-check

/**
 * @template {Record<string, Function>} T
 * @param {T} port
 * @returns {Readonly<T>}
 */
export function createGlobalStateStore(port) {
  if (!port || typeof port !== "object" || Array.isArray(port)) throw new TypeError("GlobalStateStore port is missing.");
  return Object.freeze({ ...port });
}
