// @ts-check

import { pruneBackups } from "./service-runtime.js";
import { listBackups } from "../infrastructure/node-core-ports.js";

export function createBackupsUseCase() {
  return Object.freeze({ listBackups, pruneBackups });
}
