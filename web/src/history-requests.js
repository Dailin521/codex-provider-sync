export function createLatestRequestGate() {
  let sequence = 0;
  let controller = null;
  return {
    begin() {
      controller?.abort();
      controller = new AbortController();
      sequence += 1;
      return { sequence, controller, signal: controller.signal };
    },
    isLatest(candidate) {
      return candidate === sequence;
    },
    cancel() {
      controller?.abort();
      sequence += 1;
    }
  };
}

export function scheduleDebounced(callback, delay = 300, timers = globalThis) {
  const id = timers.setTimeout(callback, delay);
  return () => timers.clearTimeout(id);
}
