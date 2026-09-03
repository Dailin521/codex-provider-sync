// @ts-check

/** @param {Record<string, Function>} port */
export function createStateDbStore(port) {
  if (!port || typeof port !== "object" || Array.isArray(port)) throw new TypeError("StateDbStore port is missing.");
  return Object.freeze({ ...port });
}
