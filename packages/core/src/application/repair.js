// @ts-check

import { applyRepair, prepareRepair } from "./service-runtime.js";

export function createRepairUseCase() {
  return Object.freeze({ prepareRepair, applyRepair });
}
