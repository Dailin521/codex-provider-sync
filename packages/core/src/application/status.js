// @ts-check

import { getStatus } from "./service-runtime.js";

export function createStatusUseCase() {
  return Object.freeze({ getStatus });
}
