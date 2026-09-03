// @ts-check

import { applyRestore, prepareRestore } from "./service-runtime.js";

export function createRestoreUseCase() {
  return Object.freeze({ prepareRestore, applyRestore });
}
