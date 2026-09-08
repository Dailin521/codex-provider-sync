// @ts-check

import { getWatchStatus, startWatch, stopWatch } from "./watch-runtime.js";

export function createWatchUseCase() {
  return Object.freeze({ startWatch, stopWatch, getWatchStatus });
}
