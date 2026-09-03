import type {
  CoreOperationEventEnvelope,
  CoreMethodName,
  CoreRequestEnvelope,
  CoreResponseEnvelope
} from "@codex-provider-sync/contracts";
import {
  assertCoreOperationEventEnvelope,
  assertCoreMethodOutput,
  assertCoreResponseEnvelope,
  createCoreFailureEnvelope,
  createPublicCoreErrorDto
} from "@codex-provider-sync/contracts";

import {
  TransportCoreClient,
  type CoreTransportCallOptions,
  type CoreTransport,
  type RequestIdFactory
} from "./client.js";

export const MAX_CORE_REQUEST_BYTES = 64 * 1024;
export const MAX_CORE_STREAM_BYTES = 16 * 1024 * 1024;
const CORE_STREAM_CONTENT_TYPE = "application/x-ndjson";

function safeNotify<T>(observer: ((event: T) => void) | undefined, event: T): void {
  if (!observer) return;
  try { observer(event); } catch {}
}

export class CoreTransportError extends Error {
  readonly status: number | null;

  constructor(message: string, status: number | null = null) {
    super(message);
    this.name = "CoreTransportError";
    this.status = status;
  }
}

export interface HttpCoreTransportOptions {
  baseUrl: string;
  endpoint?: string;
  fetch?: typeof globalThis.fetch;
  headers?: Readonly<Record<string, string>>;
}

export class HttpCoreTransport implements CoreTransport {
  readonly #url: URL;
  readonly #cancelUrl: URL;
  readonly #fetch: typeof globalThis.fetch;
  readonly #headers: Readonly<Record<string, string>>;

  constructor({
    baseUrl,
    endpoint = "/api/core",
    fetch: fetchImplementation = globalThis.fetch,
    headers = {}
  }: HttpCoreTransportOptions) {
    if (typeof fetchImplementation !== "function") {
      throw new TypeError("HttpCoreTransport requires a Fetch implementation.");
    }
    this.#url = new URL(endpoint, baseUrl);
    this.#cancelUrl = new URL(`${endpoint.replace(/\/$/, "")}/cancel`, baseUrl);
    this.#fetch = fetchImplementation;
    this.#headers = Object.freeze({ ...headers });
  }

  async request<M extends CoreMethodName>(
    envelope: CoreRequestEnvelope<M>,
    options: CoreTransportCallOptions = {}
  ): Promise<unknown> {
    const body = JSON.stringify(envelope);
    if (new TextEncoder().encode(body).byteLength > MAX_CORE_REQUEST_BYTES) {
      throw new CoreTransportError("Core request exceeds the 64 KiB transport limit.");
    }
    const isApply = envelope.method === "applySync"
      || envelope.method === "applySwitch"
      || envelope.method === "applyRepair"
      || envelope.method === "applyRestore";
    if (options.signal?.aborted) {
      if (isApply) {
        return createCoreFailureEnvelope(
          envelope,
          createPublicCoreErrorDto("OPERATION_CANCELLED")
        );
      }
      throw new DOMException("The Core HTTP request was cancelled.", "AbortError");
    }
    let operationId: string | undefined;
    let cancellationRequested = false;
    const requestCancellation = () => {
      cancellationRequested = true;
      void this.#fetch(this.#cancelUrl, {
        method: "POST",
        credentials: "same-origin",
        redirect: "error",
        headers: {
          "Content-Type": "application/json",
          ...this.#headers
        },
        body: JSON.stringify({
          protocolVersion: envelope.protocolVersion,
          requestId: envelope.requestId,
          ...(operationId ? { operationId } : {})
        })
      }).catch(() => undefined);
    };
    const onAbort = isApply ? requestCancellation : undefined;
    if (onAbort) options.signal?.addEventListener("abort", onAbort, { once: true });
    let response: Response;
    try {
      response = await this.#fetch(this.#url, {
        method: "POST",
        credentials: "same-origin",
        redirect: "error",
        headers: {
          "Content-Type": "application/json",
          "Accept": CORE_STREAM_CONTENT_TYPE,
          ...this.#headers
        },
        body,
        signal: isApply ? undefined : options.signal
      });
    } catch {
      if (onAbort) options.signal?.removeEventListener("abort", onAbort);
      throw new CoreTransportError("Core HTTP request failed.");
    }
    const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
    if (contentType.startsWith(CORE_STREAM_CONTENT_TYPE)) {
      try {
        const payload = await this.#readStream(response, envelope, options, {
          get operationId() { return operationId; },
          set operationId(value: string | undefined) { operationId = value; },
          get cancellationRequested() { return cancellationRequested; },
          requestCancellation
        });
        if (!response.ok
            && payload !== null
            && typeof payload === "object"
            && !Array.isArray(payload)
            && "ok" in payload
            && payload.ok === true) {
          throw new CoreTransportError("Core HTTP request failed.", response.status);
        }
        return payload;
      } finally {
        if (onAbort) options.signal?.removeEventListener("abort", onAbort);
      }
    }
    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      if (onAbort) options.signal?.removeEventListener("abort", onAbort);
      throw new CoreTransportError(
        "Core HTTP response was not valid JSON.",
        response.status
      );
    }
    if (onAbort) options.signal?.removeEventListener("abort", onAbort);
    if (!response.ok) {
      // A valid Core failure envelope still carries the canonical error DTO;
      // TransportCoreClient performs the protocol and DTO checks.
      if (payload === null
          || typeof payload !== "object"
          || Array.isArray(payload)
          || !("ok" in payload)
          || payload.ok !== false) {
        throw new CoreTransportError("Core HTTP request failed.", response.status);
      }
    }
    return payload;
  }

  async #readStream<M extends CoreMethodName>(
    response: Response,
    request: CoreRequestEnvelope<M>,
    options: CoreTransportCallOptions,
    cancellation: {
      operationId?: string;
      cancellationRequested: boolean;
      requestCancellation(): void;
    }
  ): Promise<unknown> {
    if (!response.body) throw new CoreTransportError("Core HTTP stream has no body.", response.status);
    const isApply = request.method === "applySync"
      || request.method === "applySwitch"
      || request.method === "applyRepair"
      || request.method === "applyRestore";
    const expectedOperation = request.method === "applySync"
      ? "sync"
      : request.method === "applySwitch"
        ? "switch"
        : request.method === "applyRepair"
          ? "repair"
        : request.method === "applyRestore"
          ? "restore"
          : null;
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffered = "";
    let received = 0;
    let finalEnvelope: unknown;
    const consume = (line: string) => {
      if (!line.trim()) return;
      if (finalEnvelope !== undefined) {
        throw new CoreTransportError("Core HTTP stream contained data after its terminal envelope.", response.status);
      }
      let value: unknown;
      try { value = JSON.parse(line); }
      catch { throw new CoreTransportError("Core HTTP stream contained invalid JSON.", response.status); }
      if (value !== null && typeof value === "object" && !Array.isArray(value) && "event" in value) {
        if (!isApply) {
          throw new CoreTransportError("Core HTTP read stream contained an operation event.", response.status);
        }
        const eventKind = "event" in value ? value.event : undefined;
        if (cancellation.operationId === undefined && eventKind !== "operation-started") {
          throw new CoreTransportError("Core HTTP stream emitted progress before operation-started.", response.status);
        }
        if (cancellation.operationId !== undefined && eventKind === "operation-started") {
          throw new CoreTransportError("Core HTTP stream emitted multiple operation-started events.", response.status);
        }
        try {
          assertCoreOperationEventEnvelope(value, request.requestId, cancellation.operationId);
        } catch {
          throw new CoreTransportError("Core HTTP stream contained an invalid operation event.", response.status);
        }
        const event = value as CoreOperationEventEnvelope;
        if (event.event === "operation-started" && event.operation !== expectedOperation) {
          throw new CoreTransportError("Core HTTP stream started the wrong operation.", response.status);
        }
        cancellation.operationId = event.operationId;
        if (event.event === "operation-started") safeNotify(options.onOperationStarted, event);
        else safeNotify(options.onProgress, event);
        if (cancellation.cancellationRequested) cancellation.requestCancellation();
        return;
      }
      try {
        assertCoreResponseEnvelope(value, request.requestId);
      } catch {
        throw new CoreTransportError("Core HTTP stream contained an invalid terminal envelope.", response.status);
      }
      const terminal = value as CoreResponseEnvelope<M>;
      const boundOperationId = cancellation.operationId;
      if (boundOperationId !== undefined) {
        if (terminal.operationId !== boundOperationId
            || (!terminal.ok
              && terminal.error.operationId !== undefined
              && terminal.error.operationId !== boundOperationId)) {
          throw new CoreTransportError("Core HTTP stream terminal operationId did not match its lifecycle.", response.status);
        }
      } else if (isApply && terminal.operationId !== undefined) {
        throw new CoreTransportError("Core HTTP stream ended an unannounced operation.", response.status);
      }
      if (terminal.ok) {
        try {
          assertCoreMethodOutput(request.method, terminal.result);
        } catch {
          throw new CoreTransportError("Core HTTP stream contained an invalid terminal result.", response.status);
        }
        if (isApply) {
          if (boundOperationId === undefined) {
            throw new CoreTransportError("Core HTTP apply stream ended without operation-started.", response.status);
          }
          const operationResult = terminal.result as { operationId?: unknown };
          if (operationResult.operationId !== boundOperationId) {
            throw new CoreTransportError("Core HTTP stream result operationId did not match its lifecycle.", response.status);
          }
        }
      }
      finalEnvelope = terminal;
    };
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      received += value.byteLength;
      if (received > MAX_CORE_STREAM_BYTES) {
        await reader.cancel();
        throw new CoreTransportError("Core HTTP stream exceeded its response limit.", response.status);
      }
      buffered += decoder.decode(value, { stream: true });
      let newline;
      while ((newline = buffered.indexOf("\n")) >= 0) {
        consume(buffered.slice(0, newline));
        buffered = buffered.slice(newline + 1);
      }
    }
    buffered += decoder.decode();
    consume(buffered);
    if (finalEnvelope === undefined) {
      throw new CoreTransportError("Core HTTP stream ended without a terminal envelope.", response.status);
    }
    return finalEnvelope;
  }
}

export interface HttpCoreClientOptions extends HttpCoreTransportOptions {
  requestIdFactory?: RequestIdFactory;
}

export class HttpCoreClient extends TransportCoreClient {
  constructor(options: HttpCoreClientOptions) {
    super(new HttpCoreTransport(options), {
      requestIdFactory: options.requestIdFactory
    });
  }
}
