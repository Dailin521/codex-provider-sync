import { createLatestRequestGate } from "./history-requests.js";

export function storagePayload(profileId) {
  return { profileId: profileId || "default" };
}

// Loads status and backups for a storage profile while guaranteeing that only
// the most recently started refresh may touch the UI. A stale request — for
// example one started for profile A before the user switched to profile B —
// is aborted and its results, errors, and loading transitions are discarded,
// even when it finishes after the newer request.
export function createProfileRefresh({ fetchStatus, fetchBackups, gate = createLatestRequestGate() }) {
  const refresh = async ({ profileId, showLoading = true, onLoading, onResult, onError }) => {
    const { controller, sequence } = gate.begin();
    if (showLoading) onLoading?.(true);
    try {
      const storage = storagePayload(profileId);
      const [statusPayload, backupPayload] = await Promise.all([
        fetchStatus(storage, { signal: controller.signal }),
        fetchBackups(storage, { signal: controller.signal })
      ]);
      if (!gate.isLatest(sequence)) return false;
      onResult?.({ profileId, status: statusPayload.status, backups: backupPayload });
      return true;
    } catch (error) {
      if (error?.name === "AbortError" || !gate.isLatest(sequence)) return false;
      onError?.(error);
      return false;
    } finally {
      if (gate.isLatest(sequence)) onLoading?.(false);
    }
  };
  refresh.cancel = () => gate.cancel();
  return refresh;
}
