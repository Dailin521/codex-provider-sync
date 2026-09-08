/** Aggregate observations, not a transaction receipt. Durations are nested,
 * not additive; missing timing must never be reconstructed from wall time. */
export interface FileUpdateTiming {
  schemaVersion: 1;
  scope: "windows-first-line";
  attemptedFiles: number;
  /** Complete worker-result measurements, including skips; error responses can
   * contribute partial durations without incrementing this coverage count. */
  measuredFiles: number;
  inPlaceFiles: number;
  rewrittenFiles: number;
  skippedFiles: number;
  totalMs: number;
  workerStartupMs: number;
  workerCloseMs: number;
  requestRoundTripMs: number;
  workerMs: number;
  sourceOpenMs: number;
  readHeaderMs: number;
  tempCreateMs: number;
  copyTailMs: number;
  flushMs: number;
  replaceMs: number;
  cleanupMs: number;
  restoreMtimeMs: number;
}

const COUNTS = ["attemptedFiles", "measuredFiles", "inPlaceFiles", "rewrittenFiles", "skippedFiles"] as const;
const DURATIONS = ["totalMs", "workerStartupMs", "workerCloseMs", "requestRoundTripMs", "workerMs", "sourceOpenMs", "readHeaderMs", "tempCreateMs", "copyTailMs", "flushMs", "replaceMs", "cleanupMs", "restoreMtimeMs"] as const;
const KEYS = new Set<string>(["schemaVersion", "scope", ...COUNTS, ...DURATIONS]);

/** Closed numeric schema: no paths, raw errors, per-file identifiers or bodies. */
export function isFileUpdateTiming(value: unknown): value is FileUpdateTiming {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const entry = value as Record<string, unknown>;
  if (entry.schemaVersion !== 1 || entry.scope !== "windows-first-line"
      || Object.keys(entry).some((key) => !KEYS.has(key))
      || !COUNTS.every((key) => Number.isSafeInteger(entry[key]) && Number(entry[key]) >= 0)
      || !DURATIONS.every((key) => typeof entry[key] === "number" && Number.isFinite(entry[key]) && Number(entry[key]) >= 0)) return false;
  return Number(entry.measuredFiles) <= Number(entry.attemptedFiles)
    && Number(entry.inPlaceFiles) + Number(entry.rewrittenFiles) + Number(entry.skippedFiles) <= Number(entry.attemptedFiles);
}

/** Copy only validated, known primitive fields. Invalid observations are absent,
 * never allowed to change the actual operation's success/partial outcome. */
export function publicFileUpdateTiming(value: unknown): FileUpdateTiming | undefined {
  if (!isFileUpdateTiming(value)) return undefined;
  return { ...value };
}
