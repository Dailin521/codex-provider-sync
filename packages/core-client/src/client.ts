import {
  ContractValidationError,
  assertCoreErrorDto,
  assertCoreMethodOutput,
  assertCoreResponseEnvelope,
  createPublicCoreErrorDto,
  createCoreRequestEnvelope,
  type ApplyPlanInput,
  type BackupList,
  type CoreErrorDto,
  type CoreMethodMap,
  type CoreMethodName,
  type CoreRequestEnvelope,
  type CoreOperationStartedEnvelope,
  type CoreProgressEnvelope,
  type DiagnosticsSnapshot,
  type GetDiagnosticsInput,
  type GetHistorySessionInput,
  type GetStatusInput,
  type GetWatchStatusInput,
  type HistoryPage,
  type HistorySessionDetail,
  type ListBackupsInput,
  type ListHistoryInput,
  type OperationResult,
  type PlanSummary,
  type PrepareRestoreInput,
  type PrepareSwitchInput,
  type PrepareSyncInput,
  type PruneBackupsInput,
  type PruneBackupsResult,
  type StartWatchInput,
  type StatusSnapshot,
  type WatchReferenceInput,
  type WatchSnapshot,
  type WatchStatusList
} from "@codex-provider-sync/contracts";

export interface CoreCallOptions {
  signal?: AbortSignal;
  operationId?: string;
  requestId?: string;
  onOperationStarted?(event: CoreOperationStartedEnvelope): void;
  onProgress?(event: CoreProgressEnvelope): void;
}

export type CoreTransportCallOptions = Pick<
  CoreCallOptions,
  "signal" | "onOperationStarted" | "onProgress"
>;

export interface CoreTransport {
  request<M extends CoreMethodName>(
    envelope: CoreRequestEnvelope<M>,
    options?: CoreTransportCallOptions
  ): Promise<unknown>;
}

export interface CoreClient {
  getStatus(input: GetStatusInput, options?: CoreCallOptions): Promise<StatusSnapshot>;
  prepareSync(input: PrepareSyncInput, options?: CoreCallOptions): Promise<PlanSummary>;
  applySync(input: ApplyPlanInput, options?: CoreCallOptions): Promise<OperationResult>;
  prepareSwitch(input: PrepareSwitchInput, options?: CoreCallOptions): Promise<PlanSummary>;
  applySwitch(input: ApplyPlanInput, options?: CoreCallOptions): Promise<OperationResult>;
  listBackups(input: ListBackupsInput, options?: CoreCallOptions): Promise<BackupList>;
  prepareRestore(input: PrepareRestoreInput, options?: CoreCallOptions): Promise<PlanSummary>;
  applyRestore(input: ApplyPlanInput, options?: CoreCallOptions): Promise<OperationResult>;
  pruneBackups(input: PruneBackupsInput, options?: CoreCallOptions): Promise<PruneBackupsResult>;
  listHistory(input: ListHistoryInput, options?: CoreCallOptions): Promise<HistoryPage>;
  getHistorySession(input: GetHistorySessionInput, options?: CoreCallOptions): Promise<HistorySessionDetail>;
  startWatch(input: StartWatchInput, options?: CoreCallOptions): Promise<WatchSnapshot>;
  stopWatch(input: WatchReferenceInput, options?: CoreCallOptions): Promise<WatchSnapshot>;
  getWatchStatus(input: GetWatchStatusInput, options?: CoreCallOptions): Promise<WatchSnapshot | WatchStatusList>;
  getDiagnostics(input: GetDiagnosticsInput, options?: CoreCallOptions): Promise<DiagnosticsSnapshot>;
}

export class CoreClientError extends Error {
  readonly dto: CoreErrorDto;
  readonly code: CoreErrorDto["code"];

  constructor(dto: CoreErrorDto) {
    assertCoreErrorDto(dto);
    super(dto.message);
    this.name = "CoreClientError";
    this.dto = dto;
    this.code = dto.code;
  }
}

export type RequestIdFactory = () => string;

function defaultRequestIdFactory(): string {
  return globalThis.crypto?.randomUUID?.()
    ?? `request-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export class TransportCoreClient implements CoreClient {
  readonly #transport: CoreTransport;
  readonly #requestIdFactory: RequestIdFactory;

  constructor(
    transport: CoreTransport,
    { requestIdFactory = defaultRequestIdFactory }: { requestIdFactory?: RequestIdFactory } = {}
  ) {
    this.#transport = transport;
    this.#requestIdFactory = requestIdFactory;
  }

  async #invoke<M extends CoreMethodName>(
    method: M,
    payload: CoreMethodMap[M]["input"],
    options: CoreCallOptions = {}
  ): Promise<CoreMethodMap[M]["output"]> {
    const requestId = options.requestId ?? this.#requestIdFactory();
    const request = createCoreRequestEnvelope(
      method,
      payload,
      requestId,
      options.operationId
    );
    const response = await this.#transport.request(request, {
      signal: options.signal,
      onOperationStarted: options.onOperationStarted,
      onProgress: options.onProgress
    });
    try {
      assertCoreResponseEnvelope<M>(response, requestId);
      if (response.ok) assertCoreMethodOutput(method, response.result);
    } catch (error) {
      if (error instanceof ContractValidationError) {
        const code = error.code === "PROTOCOL_VERSION_MISMATCH"
          ? "PROTOCOL_VERSION_MISMATCH"
          : "INTERNAL_ERROR";
        throw new CoreClientError(createPublicCoreErrorDto(code));
      }
      throw error;
    }
    if (!response.ok) throw new CoreClientError(response.error);
    return response.result;
  }

  getStatus(input: GetStatusInput, options?: CoreCallOptions): Promise<StatusSnapshot> {
    return this.#invoke("getStatus", input, options);
  }

  prepareSync(input: PrepareSyncInput, options?: CoreCallOptions): Promise<PlanSummary> {
    return this.#invoke("prepareSync", input, options);
  }

  applySync(input: ApplyPlanInput, options?: CoreCallOptions): Promise<OperationResult> {
    return this.#invoke("applySync", input, options);
  }

  prepareSwitch(input: PrepareSwitchInput, options?: CoreCallOptions): Promise<PlanSummary> {
    return this.#invoke("prepareSwitch", input, options);
  }

  applySwitch(input: ApplyPlanInput, options?: CoreCallOptions): Promise<OperationResult> {
    return this.#invoke("applySwitch", input, options);
  }

  listBackups(input: ListBackupsInput, options?: CoreCallOptions): Promise<BackupList> {
    return this.#invoke("listBackups", input, options);
  }

  prepareRestore(input: PrepareRestoreInput, options?: CoreCallOptions): Promise<PlanSummary> {
    return this.#invoke("prepareRestore", input, options);
  }

  applyRestore(input: ApplyPlanInput, options?: CoreCallOptions): Promise<OperationResult> {
    return this.#invoke("applyRestore", input, options);
  }

  pruneBackups(input: PruneBackupsInput, options?: CoreCallOptions): Promise<PruneBackupsResult> {
    return this.#invoke("pruneBackups", input, options);
  }

  listHistory(input: ListHistoryInput, options?: CoreCallOptions): Promise<HistoryPage> {
    return this.#invoke("listHistory", input, options);
  }

  getHistorySession(
    input: GetHistorySessionInput,
    options?: CoreCallOptions
  ): Promise<HistorySessionDetail> {
    return this.#invoke("getHistorySession", input, options);
  }

  startWatch(input: StartWatchInput, options?: CoreCallOptions): Promise<WatchSnapshot> {
    return this.#invoke("startWatch", input, options);
  }

  stopWatch(input: WatchReferenceInput, options?: CoreCallOptions): Promise<WatchSnapshot> {
    return this.#invoke("stopWatch", input, options);
  }

  getWatchStatus(
    input: GetWatchStatusInput,
    options?: CoreCallOptions
  ): Promise<WatchSnapshot | WatchStatusList> {
    return this.#invoke("getWatchStatus", input, options);
  }

  getDiagnostics(
    input: GetDiagnosticsInput,
    options?: CoreCallOptions
  ): Promise<DiagnosticsSnapshot> {
    return this.#invoke("getDiagnostics", input, options);
  }
}
