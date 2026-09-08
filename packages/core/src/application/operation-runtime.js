// @ts-check

/** @typedef {Record<string, any>} AnyRecord */
/**
 * @typedef {{
 *   issue(operation: string, summary: Record<string, unknown>, internal: AnyRecord): AnyRecord,
 *   consume(input: Record<string, unknown>, operation: string): {internal: AnyRecord}
 * }} PlanApplyGuard
 */
/**
 * @typedef {{
 *   begin(codexHome: string, operation: string, options?: Record<string, unknown>): {operationId: string},
 *   end(codexHome: string, operationId: string, platform?: string): void,
 *   waitForManualOperation(codexHome: string, platform?: string): unknown
 * }} ConcurrencyGuard
 */
/**
 * @typedef {{
 *   signal?: AbortSignal,
 *   faultInjector?: (event: AnyRecord) => unknown,
 *   onOperationStarted?: (event: AnyRecord) => unknown,
 *   onProgress?: (event: AnyRecord) => unknown
 * }} OperationControl
 */
/**
 * @typedef {{
 *   issuePreparedPlan(operation: string, summary: Record<string, unknown>, internal: AnyRecord): AnyRecord,
 *   applyPrepared(input: Record<string, unknown>, operation: string, execute: (options: AnyRecord) => Promise<AnyRecord>, control?: OperationControl): Promise<AnyRecord>,
 *   waitForManualOperationEnd(options?: {codexHome?: string, platform?: string}): unknown
 * }} OperationRuntime
 */

/**
 * @param {{
 *   planApplyGuard: PlanApplyGuard,
 *   concurrencyGuard: ConcurrencyGuard,
 *   CoreError: new (code: string, message: string, options?: AnyRecord) => Error,
 *   getStatus(options?: AnyRecord): Promise<unknown>,
 *   normalizeCodexHome(value?: string): string,
 *   toOperationResult(operation: string, operationId: string, result: AnyRecord, sourceBackup?: AnyRecord | null): AnyRecord,
 *   emitProgress(observer: ((event: AnyRecord) => unknown) | undefined, event: AnyRecord): void
 * }} dependencies
 */
export function createOperationRuntime({
  planApplyGuard,
  concurrencyGuard,
  CoreError,
  getStatus,
  normalizeCodexHome,
  toOperationResult,
  emitProgress
}) {
  if (!planApplyGuard || !concurrencyGuard || !CoreError
      || typeof getStatus !== "function"
      || typeof normalizeCodexHome !== "function"
      || typeof toOperationResult !== "function"
      || typeof emitProgress !== "function") {
    throw new TypeError("OperationRuntime dependencies are incomplete.");
  }

  /** @param {unknown} observer @param {AnyRecord} value */
  function notifyOperationStarted(observer, value) {
    if (typeof observer !== "function") return;
    try {
      const result = observer(value);
      if (result && typeof result.then === "function") result.catch(() => {});
    } catch {
      // Lifecycle observers are non-authoritative.
    }
  }

  /** @param {unknown} error @param {string} operationId */
  function attachOperationId(error, operationId) {
    if (!error || (typeof error !== "object" && typeof error !== "function")) return;
    const candidate = /** @type {AnyRecord} */ (error);
    if (typeof candidate.operationId === "string" && candidate.operationId) return;
    try {
      Object.defineProperty(error, "operationId", {
        configurable: true,
        enumerable: true,
        value: operationId,
        writable: false
      });
    } catch {
      // Preserve a frozen third-party error unchanged.
    }
  }

  /**
   * @param {unknown} primary
   * @param {unknown} secondary
   * @returns {((event: AnyRecord) => void) | undefined}
   */
  function composeProgressObservers(primary, secondary) {
    if (typeof primary !== "function") {
      return typeof secondary === "function"
        ? /** @type {(event: AnyRecord) => void} */ (secondary)
        : undefined;
    }
    if (typeof secondary !== "function" || primary === secondary) {
      return /** @type {(event: AnyRecord) => void} */ (primary);
    }
    const primaryObserver = /** @type {(event: AnyRecord) => unknown} */ (primary);
    const secondaryObserver = /** @type {(event: AnyRecord) => unknown} */ (secondary);
    return (event) => {
      emitProgress(primaryObserver, event);
      emitProgress(secondaryObserver, event);
    };
  }

  /** @type {OperationRuntime} */
  const runtime = {
    issuePreparedPlan(operation, summary, internal) {
      return planApplyGuard.issue(operation, summary, internal);
    },

    /**
     * @param {Record<string, unknown>} input
     * @param {string} operation
     * @param {(options: AnyRecord) => Promise<AnyRecord>} execute
     * @param {OperationControl} [control]
     */
    async applyPrepared(input, operation, execute, control = {}) {
      const entry = planApplyGuard.consume(input, operation);
      const internal = entry.internal;
      const active = concurrencyGuard.begin(internal.codexHome, operation, {
        actor: internal.actor,
        planId: input.planId,
        platform: internal.platform
      });
      notifyOperationStarted(control.onOperationStarted, {
        operationId: active.operationId,
        operation
      });
      try {
        const result = await execute({
          ...internal.executionOptions,
          ...(control.signal ? { signal: control.signal } : {}),
          ...(typeof control.faultInjector === "function"
            ? { faultInjector: control.faultInjector }
            : {}),
          onProgress: composeProgressObservers(
            internal.executionOptions.onProgress,
            control.onProgress
          ),
          expectedPlanState: internal.expectedPlanState
        });
        return toOperationResult(operation, active.operationId, result, internal.sourceBackup);
      } catch (error) {
        attachOperationId(error, active.operationId);
        const candidate = /** @type {AnyRecord} */ (error ?? {});
        if (candidate.name === "AbortError" && candidate.code === "ABORT_ERR") {
          throw new CoreError(
            "OPERATION_CANCELLED",
            "The provider-sync operation was cancelled before commit.",
            { operationId: active.operationId, cause: error }
          );
        }
        throw error;
      } finally {
        concurrencyGuard.end(internal.codexHome, active.operationId, internal.platform);
        try {
          await getStatus(internal.statusOptions);
        } catch {
          // Status refresh is observational and cannot replace the operation result.
        }
      }
    },

    waitForManualOperationEnd({ codexHome, platform } = {}) {
      return concurrencyGuard.waitForManualOperation(
        normalizeCodexHome(codexHome),
        platform ?? process.platform
      );
    }
  };
  return Object.freeze(runtime);
}
