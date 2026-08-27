import {
  createCoreFailureEnvelope,
  createPublicCoreErrorDto,
  type CoreOperationEventEnvelope,
  type CoreMethodName,
  type CoreRequestEnvelope
} from "@codex-provider-sync/contracts";

import {
  TransportCoreClient,
  type CoreTransportCallOptions,
  type CoreTransport,
  type RequestIdFactory
} from "./client.js";

export const DESKTOP_READ_METHODS = Object.freeze([
  "getStatus",
  "listBackups",
  "listHistory",
  "getHistorySession",
  "getDiagnostics"
] as const satisfies readonly CoreMethodName[]);

export type DesktopReadMethod = typeof DESKTOP_READ_METHODS[number];

export const DESKTOP_SYNC_SWITCH_METHODS = Object.freeze([
  "prepareSync",
  "applySync",
  "prepareSwitch",
  "applySwitch"
] as const satisfies readonly CoreMethodName[]);

export type DesktopSyncSwitchMethod = typeof DESKTOP_SYNC_SWITCH_METHODS[number];

export const DESKTOP_RESTORE_METHODS = Object.freeze([
  "prepareRestore",
  "applyRestore"
] as const satisfies readonly CoreMethodName[]);

export type DesktopRestoreMethod = typeof DESKTOP_RESTORE_METHODS[number];

export const DESKTOP_MAINTENANCE_METHODS = Object.freeze([
  "pruneBackups",
  "startWatch",
  "stopWatch",
  "getWatchStatus"
] as const satisfies readonly CoreMethodName[]);

export type DesktopMaintenanceMethod = typeof DESKTOP_MAINTENANCE_METHODS[number];
export type DesktopManagedMethod =
  | DesktopSyncSwitchMethod
  | DesktopRestoreMethod
  | DesktopMaintenanceMethod;
export type DesktopRuntimeMethod = DesktopReadMethod | DesktopManagedMethod;
export const DESKTOP_RUNTIME_METHODS = Object.freeze([
  ...DESKTOP_READ_METHODS,
  ...DESKTOP_SYNC_SWITCH_METHODS,
  ...DESKTOP_RESTORE_METHODS,
  ...DESKTOP_MAINTENANCE_METHODS
] as const satisfies readonly DesktopRuntimeMethod[]);

const DESKTOP_READ_METHOD_SET = new Set<CoreMethodName>(DESKTOP_READ_METHODS);
const DESKTOP_SYNC_SWITCH_METHOD_SET = new Set<CoreMethodName>(DESKTOP_SYNC_SWITCH_METHODS);
const DESKTOP_RESTORE_METHOD_SET = new Set<CoreMethodName>(DESKTOP_RESTORE_METHODS);
const DESKTOP_MAINTENANCE_METHOD_SET = new Set<CoreMethodName>(DESKTOP_MAINTENANCE_METHODS);

export function isDesktopReadMethod(method: CoreMethodName): method is DesktopReadMethod {
  return DESKTOP_READ_METHOD_SET.has(method);
}

export function isDesktopSyncSwitchMethod(
  method: CoreMethodName
): method is DesktopSyncSwitchMethod {
  return DESKTOP_SYNC_SWITCH_METHOD_SET.has(method);
}

export function isDesktopRestoreMethod(method: CoreMethodName): method is DesktopRestoreMethod {
  return DESKTOP_RESTORE_METHOD_SET.has(method);
}

export function isDesktopMaintenanceMethod(
  method: CoreMethodName
): method is DesktopMaintenanceMethod {
  return DESKTOP_MAINTENANCE_METHOD_SET.has(method);
}

export function isDesktopManagedMethod(method: CoreMethodName): method is DesktopManagedMethod {
  return isDesktopSyncSwitchMethod(method)
    || isDesktopRestoreMethod(method)
    || isDesktopMaintenanceMethod(method);
}

export function isDesktopRuntimeMethod(method: CoreMethodName): method is DesktopRuntimeMethod {
  return isDesktopReadMethod(method) || isDesktopManagedMethod(method);
}

export interface DesktopCancelOperationInput {
  requestId: string;
  operationId?: string;
}

export interface DesktopCancelOperationResult {
  accepted: boolean;
}

export interface DesktopCoreBridge {
  requestReadOnly<M extends DesktopReadMethod>(
    envelope: CoreRequestEnvelope<M>
  ): Promise<unknown>;
  requestSyncSwitch<M extends DesktopSyncSwitchMethod>(
    envelope: CoreRequestEnvelope<M>
  ): Promise<unknown>;
  requestRestore<M extends DesktopRestoreMethod>(
    envelope: CoreRequestEnvelope<M>
  ): Promise<unknown>;
  requestMaintenance<M extends DesktopMaintenanceMethod>(
    envelope: CoreRequestEnvelope<M>
  ): Promise<unknown>;
  subscribeOperation(listener: (event: CoreOperationEventEnvelope) => void): () => void;
  cancelOperation(input: DesktopCancelOperationInput): Promise<DesktopCancelOperationResult>;
}

function abortError(): DOMException {
  return new DOMException("The desktop Core request was cancelled.", "AbortError");
}

function safeNotify<T>(observer: ((event: T) => void) | undefined, event: T): void {
  if (!observer) return;
  try { observer(event); } catch {}
}

class DesktopCoreTransport implements CoreTransport {
  readonly #bridge: DesktopCoreBridge;

  constructor(bridge: DesktopCoreBridge) {
    this.#bridge = bridge;
  }

  async request<M extends CoreMethodName>(
    envelope: CoreRequestEnvelope<M>,
    options: CoreTransportCallOptions = {}
  ): Promise<unknown> {
    if (!isDesktopRuntimeMethod(envelope.method)) {
      return createCoreFailureEnvelope(
        envelope,
        createPublicCoreErrorDto("PERMISSION_DENIED")
      );
    }
    if (options.signal?.aborted) {
      throw abortError();
    }
    if (isDesktopReadMethod(envelope.method)) {
      const request = this.#bridge.requestReadOnly(
        envelope as CoreRequestEnvelope<DesktopReadMethod>
      );
      if (!options.signal) return request;
      return new Promise((resolve, reject) => {
        const onAbort = () => reject(abortError());
        options.signal?.addEventListener("abort", onAbort, { once: true });
        void request.then(resolve, reject).finally(() => {
          options.signal?.removeEventListener("abort", onAbort);
        });
      });
    }

    const requestManaged = (): Promise<unknown> => {
      if (isDesktopSyncSwitchMethod(envelope.method)) {
        return this.#bridge.requestSyncSwitch(
          envelope as CoreRequestEnvelope<DesktopSyncSwitchMethod>
        );
      }
      if (isDesktopRestoreMethod(envelope.method)) {
        return this.#bridge.requestRestore(
          envelope as CoreRequestEnvelope<DesktopRestoreMethod>
        );
      }
      return this.#bridge.requestMaintenance(
        envelope as CoreRequestEnvelope<DesktopMaintenanceMethod>
      );
    };
    const isApply = envelope.method === "applySync"
      || envelope.method === "applySwitch"
      || envelope.method === "applyRestore";
    if (!isApply) {
      const request = requestManaged();
      if (!options.signal) return request;
      return new Promise((resolve, reject) => {
        const onAbort = () => reject(abortError());
        options.signal?.addEventListener("abort", onAbort, { once: true });
        void request.then(resolve, reject).finally(() => {
          options.signal?.removeEventListener("abort", onAbort);
        });
      });
    }

    let operationId: string | undefined;
    let cancelRequested = false;
    const unsubscribe = this.#bridge.subscribeOperation((event) => {
      if (event.requestId !== envelope.requestId) return;
      if (event.event === "operation-started") {
        operationId = event.operationId;
        safeNotify(options.onOperationStarted, event);
      } else {
        safeNotify(options.onProgress, event);
      }
      if (cancelRequested) {
        void this.#bridge.cancelOperation({ requestId: envelope.requestId, operationId });
      }
    });
    const onAbort = () => {
      cancelRequested = true;
      void this.#bridge.cancelOperation({ requestId: envelope.requestId, operationId });
    };
    options.signal?.addEventListener("abort", onAbort, { once: true });
    const request = requestManaged();
    return new Promise((resolve, reject) => {
      void request.then(resolve, reject).finally(() => {
        options.signal?.removeEventListener("abort", onAbort);
        unsubscribe();
      });
    });
  }
}

export class DesktopCoreClient extends TransportCoreClient {
  constructor(
    bridge: DesktopCoreBridge,
    { requestIdFactory }: { requestIdFactory?: RequestIdFactory } = {}
  ) {
    super(new DesktopCoreTransport(bridge), { requestIdFactory });
  }
}
