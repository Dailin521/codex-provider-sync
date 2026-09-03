// @ts-check

import { applySwitch, prepareSwitch } from "./service-runtime.js";

export function createProviderSwitchUseCase() {
  return Object.freeze({ prepareSwitch, applySwitch });
}
