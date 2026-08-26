import {
  createCoreFailureEnvelope,
  createPublicCoreErrorDto,
  type CoreMethodName,
  type CoreRequestEnvelope
} from "@codex-provider-sync/contracts";

import {
  TransportCoreClient,
  type CoreCallOptions,
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

const DESKTOP_READ_METHOD_SET = new Set<CoreMethodName>(DESKTOP_READ_METHODS);

export function isDesktopReadMethod(method: CoreMethodName): method is DesktopReadMethod {
  return DESKTOP_READ_METHOD_SET.has(method);
}

export interface DesktopCoreBridge {
  requestReadOnly<M extends DesktopReadMethod>(
    envelope: CoreRequestEnvelope<M>
  ): Promise<unknown>;
}

class DesktopCoreTransport implements CoreTransport {
  readonly #bridge: DesktopCoreBridge;

  constructor(bridge: DesktopCoreBridge) {
    this.#bridge = bridge;
  }

  async request<M extends CoreMethodName>(
    envelope: CoreRequestEnvelope<M>,
    options: Pick<CoreCallOptions, "signal"> = {}
  ): Promise<unknown> {
    if (!isDesktopReadMethod(envelope.method)) {
      return createCoreFailureEnvelope(
        envelope,
        createPublicCoreErrorDto("PERMISSION_DENIED")
      );
    }
    if (options.signal?.aborted) {
      throw new DOMException("The desktop Core request was cancelled.", "AbortError");
    }
    const request = this.#bridge.requestReadOnly(
      envelope as CoreRequestEnvelope<DesktopReadMethod>
    );
    if (!options.signal) return request;
    return new Promise((resolve, reject) => {
      const onAbort = () => reject(
        new DOMException("The desktop Core request was cancelled.", "AbortError")
      );
      options.signal?.addEventListener("abort", onAbort, { once: true });
      void request.then(resolve, reject).finally(() => {
        options.signal?.removeEventListener("abort", onAbort);
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
