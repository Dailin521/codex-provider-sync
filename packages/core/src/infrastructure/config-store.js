// @ts-check

/**
 * @template {Record<string, Function>} T
 * @param {T} port
 * @returns {Readonly<T>}
 */
export function createConfigStore(port) {
  if (!port || typeof port !== "object" || Array.isArray(port)) throw new TypeError("ConfigStore port is missing.");
  return Object.freeze({ ...port });
}
