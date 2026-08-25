import {
  createCoreFailureEnvelope,
  createPublicCoreErrorDto,
  createCoreSuccessEnvelope,
  sanitizePublicCoreErrorDto,
  type CoreErrorDto,
  type CoreMethodMap,
  type CoreMethodName,
  type CoreRequestEnvelope
} from "@codex-provider-sync/contracts";

import {
  TransportCoreClient,
  type CoreCallOptions,
  type CoreTransport,
  type RequestIdFactory
} from "./client.js";

type MaybePromise<T> = T | Promise<T>;

export type MockCoreHandler<M extends CoreMethodName> = (
  payload: CoreMethodMap[M]["input"],
  request: CoreRequestEnvelope<M>
) => MaybePromise<CoreMethodMap[M]["output"]>;

export type MockCoreHandlers = {
  [M in CoreMethodName]?: MockCoreHandler<M>;
};

export function legacyErrorToDto(error: unknown): CoreErrorDto {
  return sanitizePublicCoreErrorDto(error);
}

class MockCoreTransport implements CoreTransport {
  readonly #handlers: MockCoreHandlers;
  readonly requests: CoreRequestEnvelope[] = [];

  constructor(handlers: MockCoreHandlers) {
    this.#handlers = { ...handlers };
  }

  async request<M extends CoreMethodName>(
    request: CoreRequestEnvelope<M>,
    _options: Pick<CoreCallOptions, "signal"> = {}
  ): Promise<unknown> {
    this.requests.push(request);
    const handler = this.#handlers[request.method] as MockCoreHandler<M> | undefined;
    if (!handler) {
      return createCoreFailureEnvelope(request, createPublicCoreErrorDto("INTERNAL_ERROR"));
    }
    let result: CoreMethodMap[M]["output"];
    try {
      result = await handler(request.payload, request);
    } catch (error) {
      return createCoreFailureEnvelope(request, legacyErrorToDto(error));
    }
    const operationId = result !== null
      && typeof result === "object"
      && "operationId" in result
      && typeof result.operationId === "string"
      ? result.operationId
      : undefined;
    try {
      return createCoreSuccessEnvelope(request, result, operationId);
    } catch {
      return createCoreFailureEnvelope(request, createPublicCoreErrorDto("INTERNAL_ERROR"));
    }
  }
}

export class MockCoreClient extends TransportCoreClient {
  readonly requests: readonly CoreRequestEnvelope[];

  constructor(
    handlers: MockCoreHandlers,
    { requestIdFactory }: { requestIdFactory?: RequestIdFactory } = {}
  ) {
    const transport = new MockCoreTransport(handlers);
    super(transport, { requestIdFactory });
    this.requests = transport.requests;
  }
}
