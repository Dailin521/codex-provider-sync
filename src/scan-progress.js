import { performance } from "node:perf_hooks";

// Read-only scan telemetry. Observers cannot affect storage results, and events
// never contain filenames, session IDs or message content.
export function scanProgress(observer, stage, fields = {}) {
  if (typeof observer !== "function") return;
  try {
    const result = observer({ stage, status: "running", ...fields });
    if (result && typeof result.then === "function") result.catch(() => {});
  } catch { /* Telemetry is best effort. */ }
}

export function checkScanCancelled(signal) {
  if (!signal?.aborted) return;
  const error = new Error("The scan was cancelled.");
  error.name = "AbortError";
  error.code = "ABORT_ERR";
  throw error;
}

// The denominator is the enumerated files in this scope, not the entire request.
// Keep original enumeration/read order, including skipped files, unchanged.
export function* trackScanFiles(files, { onProgress, signal, stage }) {
  checkScanCancelled(signal);
  scanProgress(onProgress, stage, { progress: 0, count: 0 });
  let count = 0;
  let lastEmission = performance.now();
  for (const file of files) {
    checkScanCancelled(signal);
    yield file;
    count += 1;
    const now = performance.now();
    if (now - lastEmission >= 150 && count < files.length) {
      scanProgress(onProgress, stage, { progress: count / files.length, count });
      lastEmission = now;
    }
  }
  checkScanCancelled(signal);
  scanProgress(onProgress, stage, { status: "completed", progress: 1, count });
}
