// @ts-check

import { applySync, prepareSync } from "./service-runtime.js";

export function createProviderSyncUseCase() {
  return Object.freeze({ prepareSync, applySync });
}
