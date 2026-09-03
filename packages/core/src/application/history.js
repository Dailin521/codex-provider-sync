// @ts-check

import { codexStorage } from "../infrastructure/node-core-ports.js";

const { getHistorySession, listHistory } = codexStorage.sessions;

export function createHistoryUseCase() {
  return Object.freeze({ listHistory, getHistorySession });
}
