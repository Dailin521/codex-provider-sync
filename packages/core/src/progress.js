/**
 * Project an internal observer event onto the public, pathless ProgressEvent DTO.
 * Invalid optional numeric fields are omitted instead of causing the trusted host
 * observer to fail and silently lose the whole progress event.
 *
 * @param {unknown} event
 * @returns {{stage: string, status: string, progress?: number, count?: number} | null}
 */
export function toPublicProgress(event) {
  if (!event
      || typeof event !== "object"
      || Array.isArray(event)) {
    return null;
  }
  const source = /** @type {Record<string, unknown>} */ (event);
  if (typeof source.stage !== "string"
      || !source.stage
      || source.stage.length > 80
      || typeof source.status !== "string"
      || !source.status
      || source.status.length > 40) return null;
  return {
    stage: source.stage,
    status: source.status,
    ...(typeof source.progress === "number"
        && Number.isFinite(source.progress)
        && source.progress >= 0
        && source.progress <= 1
      ? { progress: source.progress }
      : {}),
    ...(typeof source.count === "number"
        && Number.isSafeInteger(source.count)
        && source.count >= 0
      ? { count: source.count }
      : {})
  };
}
