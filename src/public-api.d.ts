/**
 * @internal Transitional declarations for the legacy root package boundary.
 * New vNext hosts use @codex-provider-sync/core and its trusted profile facade.
 */
export const CORE_ERROR_CODES: readonly string[];

export class CoreError extends Error {
  readonly code: string;
  readonly severity: string;
  readonly retryable: boolean;
  readonly recoveryRequired: boolean;
  readonly operationId?: string;
  readonly details?: Record<string, unknown>;
  readonly suggestedAction?: string;
  constructor(code: string, message: string, options?: Record<string, unknown>);
  toDto(): Record<string, unknown>;
}

export function toCoreErrorDto(error: unknown, options?: Record<string, unknown>): Record<string, unknown>;
export function getStatus(options?: Record<string, unknown>): Promise<unknown>;
export function prepareSync(options?: Record<string, unknown>): Promise<unknown>;
/** @internal Trusted host control. Never expose this object to HTTP, IPC, or Renderer input. */
export interface CoreHostOperationControl {
  signal?: AbortSignal;
  onOperationStarted?(value: { operationId: string; operation: "sync" | "switch" | "repair" | "restore" }): void | Promise<void>;
  onProgress?(event: Record<string, unknown>): void | Promise<void>;
}

export function applySync(input: Record<string, unknown>, control?: CoreHostOperationControl): Promise<unknown>;
export function prepareSwitch(options?: Record<string, unknown>): Promise<unknown>;
export function applySwitch(input: Record<string, unknown>, control?: CoreHostOperationControl): Promise<unknown>;
export function prepareRepair(options?: Record<string, unknown>): Promise<unknown>;
export function applyRepair(input: Record<string, unknown>, control?: CoreHostOperationControl): Promise<unknown>;
export function prepareRestore(options?: Record<string, unknown>): Promise<unknown>;
export function applyRestore(input: Record<string, unknown>, control?: CoreHostOperationControl): Promise<unknown>;
export function pruneBackups(options?: Record<string, unknown>): Promise<unknown>;
export function listBackups(codexHome: string): Promise<unknown>;
export function listHistory(codexHome: string, options?: Record<string, unknown>): Promise<unknown>;
export function getHistorySession(codexHome: string, sessionId: string, options?: Record<string, unknown>): Promise<unknown>;
export function startWatch(options?: Record<string, unknown>): Promise<unknown>;
export function stopWatch(input: Record<string, unknown>): Promise<unknown>;
export function getWatchStatus(input?: Record<string, unknown> | null): unknown;
export function getDiagnostics(options?: Record<string, unknown>): Promise<unknown>;

/** @deprecated Use prepareSync/applySync through the trusted Core facade. */
export function runSync(options?: Record<string, unknown>): Promise<unknown>;
/** @deprecated Use prepareSwitch/applySwitch through the trusted Core facade. */
export function runSwitch(options?: Record<string, unknown>): Promise<unknown>;
/** @deprecated Use prepareRepair/applyRepair through the trusted Core facade. */
export function runRepair(options?: Record<string, unknown>): Promise<unknown>;
/** @deprecated Use prepareRestore/applyRestore through the trusted Core facade. */
export function runRestore(options?: Record<string, unknown>): Promise<unknown>;
/** @deprecated Use pruneBackups through the trusted Core facade. */
export function runPruneBackups(options?: Record<string, unknown>): Promise<unknown>;
/** @deprecated Use startWatch/stopWatch through the trusted Core facade. */
export function runWatch(options?: Record<string, unknown>): Promise<unknown>;

/** @internal Legacy root helper; not part of the vNext Core facade. */
export function readConfigText(filePath: string): Promise<string>;
/** @internal Legacy root helper; not part of the vNext Core facade. */
export function readRootModelFromConfigText(text: string): string | null;
/** @internal Legacy root helper; not part of the vNext Core facade. */
export function detectStateDb(input: unknown): Promise<unknown>;
/** @internal Legacy root helper; not part of the vNext Core facade. */
export function ensureCodexHome(input: unknown): Promise<unknown>;
/** @internal Legacy root helper; not part of the vNext Core facade. */
export function resolveStorageLayout(input: unknown): unknown;
/** @internal Legacy root helper; not part of the vNext Core facade. */
export function withStateDbLocation(input: unknown, location: unknown): unknown;
