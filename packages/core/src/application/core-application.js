// @ts-check

import { createBackupsUseCase } from "./backups.js";
import { createDiagnosticsUseCase } from "./diagnostics.js";
import { createHistoryUseCase } from "./history.js";
import { createProviderSwitchUseCase } from "./provider-switch.js";
import { createProviderSyncUseCase } from "./provider-sync.js";
import { createRepairUseCase } from "./repair.js";
import { createRestoreUseCase } from "./restore.js";
import { createStatusUseCase } from "./status.js";
import { createWatchUseCase } from "./watch.js";

/**
 * Compose the business use-case surface consumed by CoreFacade. Use cases own
 * their runtime dependencies and cannot call back into the facade.
 */
export function createCoreApplication() {
  return Object.freeze({
    ...createStatusUseCase(),
    ...createProviderSyncUseCase(),
    ...createProviderSwitchUseCase(),
    ...createDiagnosticsUseCase(),
    ...createRepairUseCase(),
    ...createBackupsUseCase(),
    ...createRestoreUseCase(),
    ...createHistoryUseCase(),
    ...createWatchUseCase()
  });
}
