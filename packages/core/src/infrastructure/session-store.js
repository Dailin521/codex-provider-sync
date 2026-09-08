// @ts-check

/**
 * @template {Record<string, Function>} T
 * @param {T} port
 * @returns {Readonly<T>}
 */
export function createSessionStore(port) {
  if (!port || typeof port !== "object" || Array.isArray(port)) throw new TypeError("SessionStore port is missing.");
  return Object.freeze({ ...port });
}
