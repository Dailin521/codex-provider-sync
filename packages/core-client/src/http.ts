import type {
  CoreMethodName,
  CoreRequestEnvelope
} from "@codex-provider-sync/contracts";

import {
  TransportCoreClient,
  type CoreTransportCallOptions,
  type CoreTransport,
  type RequestIdFactory
} from "./client.js";

export const MAX_CORE_REQUEST_BYTES = 64 * 1024;

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
    let response: Response;
    try {
      response = await this.#fetch(this.#url, {
        method: "POST",
        credentials: "same-origin",
        redirect: "error",
        headers: {
          "Content-Type": "application/json",
          ...this.#headers
        },
        body,
        signal: options.signal
      });
    } catch {
      throw new CoreTransportError("Core HTTP request failed.");
    }
    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      throw new CoreTransportError(
        "Core HTTP response was not valid JSON.",
        response.status
      );
    }
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
