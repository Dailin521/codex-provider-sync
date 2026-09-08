const COUNTS = ["attemptedFiles", "measuredFiles", "inPlaceFiles", "rewrittenFiles", "skippedFiles"];
const DURATIONS = ["totalMs", "workerStartupMs", "workerCloseMs", "requestRoundTripMs", "workerMs", "sourceOpenMs", "readHeaderMs", "tempCreateMs", "copyTailMs", "flushMs", "replaceMs", "cleanupMs", "restoreMtimeMs"];
const KEYS = new Set(["schemaVersion", "scope", ...COUNTS, ...DURATIONS]);
/** Closed numeric schema: no paths, raw errors, per-file identifiers or bodies. */
export function isFileUpdateTiming(value) {
    if (!value || typeof value !== "object" || Array.isArray(value))
        return false;
    const entry = value;
    if (entry.schemaVersion !== 1 || entry.scope !== "windows-first-line"
        || Object.keys(entry).some((key) => !KEYS.has(key))
        || !COUNTS.every((key) => Number.isSafeInteger(entry[key]) && Number(entry[key]) >= 0)
        || !DURATIONS.every((key) => typeof entry[key] === "number" && Number.isFinite(entry[key]) && Number(entry[key]) >= 0))
        return false;
    return Number(entry.measuredFiles) <= Number(entry.attemptedFiles)
        && Number(entry.inPlaceFiles) + Number(entry.rewrittenFiles) + Number(entry.skippedFiles) <= Number(entry.attemptedFiles);
}
/** Copy only validated, known primitive fields. Invalid observations are absent,
 * never allowed to change the actual operation's success/partial outcome. */
export function publicFileUpdateTiming(value) {
    if (!isFileUpdateTiming(value))
        return undefined;
    return { ...value };
}
