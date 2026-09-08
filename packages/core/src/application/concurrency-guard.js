// @ts-check

/**
 * @typedef {{
 *   registerManualIntent(codexHome: string, planId: string, expiresAt: string, platform?: string): void,
 *   begin(codexHome: string, operation: string, options?: Record<string, unknown>): {operationId: string},
 *   end(codexHome: string, operationId: string, platform?: string): void,
 *   waitForManualOperation(codexHome: string, platform?: string): unknown
 * }} OperationCoordinatorLike
 */
/**
 * @typedef {{
 *   registerManualIntent(codexHome: string, planId: string, expiresAt: string, platform?: string): void,
 *   begin(codexHome: string, operation: string, options?: Record<string, unknown>): {operationId: string},
 *   end(codexHome: string, operationId: string, platform?: string): void,
 *   waitForManualOperation(codexHome: string, platform?: string): unknown
 * }} ConcurrencyGuard
 */

/**
 * Keep in-process admission and Watch/manual priority behind one application
 * boundary. Filesystem locks remain storage infrastructure owned by the use
 * cases that mutate Codex data.
 *
 * @param {{coordinator: OperationCoordinatorLike}} dependencies
 */
export function createConcurrencyGuard({ coordinator }) {
  if (!coordinator) throw new TypeError("ConcurrencyGuard requires an operation coordinator.");
  /** @type {ConcurrencyGuard} */
  const guard = {
    registerManualIntent(codexHome, planId, expiresAt, platform) {
      coordinator.registerManualIntent(codexHome, planId, expiresAt, platform);
    },
    begin(codexHome, operation, options = {}) {
      return coordinator.begin(codexHome, operation, options);
    },
    end(codexHome, operationId, platform) {
      coordinator.end(codexHome, operationId, platform);
    },
    waitForManualOperation(codexHome, platform) {
      return coordinator.waitForManualOperation(codexHome, platform);
    }
  };
  return Object.freeze(guard);
}
