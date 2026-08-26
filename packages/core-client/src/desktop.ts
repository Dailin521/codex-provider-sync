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
export type DesktopRuntimeMethod = DesktopReadMethod | DesktopSyncSwitchMethod;
export const DESKTOP_RUNTIME_METHODS = Object.freeze([
  ...DESKTOP_READ_METHODS,
  ...DESKTOP_SYNC_SWITCH_METHODS
] as const satisfies readonly DesktopRuntimeMethod[]);

const DESKTOP_READ_METHOD_SET = new Set<CoreMethodName>(DESKTOP_READ_METHODS);
const DESKTOP_SYNC_SWITCH_METHOD_SET = new Set<CoreMethodName>(DESKTOP_SYNC_SWITCH_METHODS);

export function isDesktopReadMethod(method: CoreMethodName): method is DesktopReadMethod {
  return DESKTOP_READ_METHOD_SET.has(method);
}

export function isDesktopSyncSwitchMethod(
  method: CoreMethodName
): method is DesktopSyncSwitchMethod {
  return DESKTOP_SYNC_SWITCH_METHOD_SET.has(method);
}

export function isDesktopRuntimeMethod(method: CoreMethodName): method is DesktopRuntimeMethod {
  return isDesktopReadMethod(method) || isDesktopSyncSwitchMethod(method);
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

    const isApply = envelope.method === "applySync" || envelope.method === "applySwitch";
    if (!isApply) {
      const request = this.#bridge.requestSyncSwitch(
        envelope as CoreRequestEnvelope<DesktopSyncSwitchMethod>
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
    const request = this.#bridge.requestSyncSwitch(
      envelope as CoreRequestEnvelope<DesktopSyncSwitchMethod>
    );
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
