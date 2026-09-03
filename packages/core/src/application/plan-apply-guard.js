// @ts-check

/**
 * @typedef {{
 *   issue(operation: string, summary: Record<string, unknown>, internal: Record<string, any>): Record<string, any>,
 *   consume(input: Record<string, unknown>, operation: string): {internal: Record<string, any>}
 * }} PlanLedgerLike
 */

/**
 * @typedef {{
 *   registerManualIntent(codexHome: string, planId: string, expiresAt: string, platform?: string): void
 * }} ManualIntentGuard
 */
/**
 * @typedef {{
 *   issue(operation: string, summary: Record<string, unknown>, internal: Record<string, any>): Record<string, any>,
 *   consume(input: Record<string, unknown>, operation: string): {internal: Record<string, any>}
 * }} PlanApplyGuard
 */

/**
 * Own the Plan ledger and confirmation lifetime without knowing any storage or
 * mutation details.
 *
 * @param {{planLedger: PlanLedgerLike, concurrencyGuard: ManualIntentGuard}} dependencies
 */
export function createPlanApplyGuard({ planLedger, concurrencyGuard }) {
  if (!planLedger || !concurrencyGuard) {
    throw new TypeError("PlanApplyGuard requires a plan ledger and concurrency guard.");
  }
  /** @type {PlanApplyGuard} */
  const guard = {
    issue(operation, summary, internal) {
      const plan = planLedger.issue(operation, summary, internal);
      if (internal.actor === "manual") {
        concurrencyGuard.registerManualIntent(
          internal.codexHome,
          plan.planId,
          plan.expiresAt,
          internal.platform
        );
      }
      return plan;
    },
    consume(input, operation) {
      return planLedger.consume(input, operation);
    }
  };
  return Object.freeze(guard);
}
