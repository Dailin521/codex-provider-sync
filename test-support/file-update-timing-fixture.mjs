/** Synthetic, deliberately fractional monotonic durations; never real user data. */
export const fileUpdateTimingFixture = Object.freeze({
  schemaVersion: 1, scope: "windows-first-line",
  attemptedFiles: 3, measuredFiles: 3, inPlaceFiles: 1, rewrittenFiles: 1, skippedFiles: 1,
  totalMs: 21465.25, workerStartupMs: 251.5, workerCloseMs: 15.75,
  requestRoundTripMs: 20200.25, workerMs: 19500.5,
  sourceOpenMs: 160.25, readHeaderMs: 300.5, tempCreateMs: 240.75,
  copyTailMs: 3100.5, flushMs: 6200.25, replaceMs: 1200.5, cleanupMs: 775.25, restoreMtimeMs: 427.5
});
