// @ts-check

/** @param {Record<string, Function>} port */
export function createSessionStore(port) {
  if (!port || typeof port !== "object" || Array.isArray(port)) throw new TypeError("SessionStore port is missing.");
  return Object.freeze({ ...port });
}
