// @ts-check

/**
 * @template {Record<string, Function>} T
 * @param {T} port
 * @returns {Readonly<T>}
 */
export function createStateDbStore(port) {
  if (!port || typeof port !== "object" || Array.isArray(port)) throw new TypeError("StateDbStore port is missing.");
  return Object.freeze({ ...port });
}
