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
/** Closed numeric schema: no paths, raw errors, per-file identifiers or bodies. */
export declare function isFileUpdateTiming(value: unknown): value is FileUpdateTiming;
/** Copy only validated, known primitive fields. Invalid observations are absent,
 * never allowed to change the actual operation's success/partial outcome. */
export declare function publicFileUpdateTiming(value: unknown): FileUpdateTiming | undefined;
