// @ts-check

/** @param {Record<string, Function>} port */
export function createConfigStore(port) {
  if (!port || typeof port !== "object" || Array.isArray(port)) throw new TypeError("ConfigStore port is missing.");
  return Object.freeze({ ...port });
}
