import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";

import {
  CORE_ERROR_CODES,
  OPERATION_FAILURE_STAGES,
  SAFE_CAUSE_CODES,
  publicFileUpdateTiming,
  type CoreResponseEnvelope,
  type ProgressEvent
} from "@codex-provider-sync/contracts";
import { validateOperationLogEntry } from "../shared/operation-log-validation.js";

import type {
  OperationLogEntry,
  OperationLogListInput,
  OperationLogListResponse,
  OperationLogStatus
} from "../shared/operation-log-types.js";

const DEFAULT_MAX_FILE_BYTES = 5 * 1024 * 1024;
const DEFAULT_MAX_FILES = 5;
const MAX_WARNINGS = 64;
const MAX_STAGES = 256;
const FAILURE_STAGES = new Set([...OPERATION_FAILURE_STAGES, "mutation", "repair_workspace_roots", "verify_repair"]);
const FAILURE_CODES = new Set<string>([
  ...CORE_ERROR_CODES,
  "WRITE_FAILED",
  ...SAFE_CAUSE_CODES,
  // Existing partial-result codes are not DTO cause codes, but remain valid
  // result fields and must not be downgraded while projecting old records.
  "SQLITE_READONLY",
  "SQLITE_FULL"
]);
const PARTIAL_REASONS = new Set(["locked-session", "rollout-changed", "mutation-failed"]);
const ERROR_REASONS = new Set(["profile", "config", "storage", "rollout", "state-db", "backup", "provider-not-configured"]);
const COUNT_FIELDS = new Set([
  "changedSessionFiles", "inPlaceSessionFiles", "rewrittenSessionFiles", "sqliteRowsUpdated",
  "sqliteProviderRowsUpdated", "sqliteModelRowsUpdated", "sqliteUserEventRowsUpdated", "sqliteCwdRowsUpdated",
  "skippedLockedRolloutFiles", "skippedChangedRolloutFiles", "updatedWorkspaceRoots", "savedWorkspaceRootCount",
  "resolvedOperationCount", "deletedCount", "remainingCount", "freedBytes"
]);

interface ActiveTiming {
  resumedAt: number | null;
  wallStartedAt: number;
  stageStartedAt: Map<string, number>;
}

interface BeginInput {
  operation: string;
  profileId?: string;
  profileRevision?: string;
  requestId?: string;
  stage?: string;
}

interface PreparedSummary {
  target?: { provider?: unknown; model?: unknown; modelMode?: unknown; previousProvider?: unknown; previousRootModel?: unknown };
  impact?: {
    rolloutFilesToChange?: unknown;
    sqliteRowsToChange?: unknown;
    lockedRolloutFiles?: unknown;
  };
}

function safeText(value: unknown, max = 256): string | undefined {
  if (typeof value !== "string") return undefined;
  const text = value.trim();
  return text && text.length <= max && !/[\\/]/.test(text) ? text : undefined;
}

function safeProviderId(value: unknown): string | undefined {
  return typeof value === "string" && /^[A-Za-z0-9._-]{1,128}$/.test(value) ? value : undefined;
}

function safeErrorReason(value: unknown): OperationLogEntry["errorReason"] | undefined {
  return typeof value === "string" && ERROR_REASONS.has(value) ? value as OperationLogEntry["errorReason"] : undefined;
}

function previewCounts(value: PreparedSummary | undefined): OperationLogEntry["previewCounts"] | undefined {
  const impact = value?.impact;
  if (!impact) return undefined;
  const fields = [impact.rolloutFilesToChange, impact.sqliteRowsToChange, impact.lockedRolloutFiles];
  if (!fields.every((count) => Number.isSafeInteger(count) && Number(count) >= 0)) return undefined;
  return {
    rolloutFilesToChange: Number(impact.rolloutFilesToChange),
    sqliteRowsToChange: Number(impact.sqliteRowsToChange),
    lockedRolloutFiles: Number(impact.lockedRolloutFiles)
  };
}

function safeModelName(value: unknown): string | null | undefined {
  if (value === null) return null;
  return typeof value === "string" && value.trim().length > 0 && value.length <= 256
    && !/[\u0000-\u001f\\]/.test(value)
    && !/^(?:[A-Za-z]:[\\/]|[\\/]|[A-Za-z][A-Za-z0-9+.-]*:\/\/)/.test(value)
    && !/^(?:sk-|pk-|bearer\s|(?:api[_-]?key|token|secret|password)\s*[=:])/i.test(value)
    ? value
    : undefined;
}

function switchPlan(value: PreparedSummary | undefined): OperationLogEntry["switchPlan"] | undefined {
  const target = value?.target;
  const previousProvider = safeProviderId(target?.previousProvider);
  const targetProvider = safeProviderId(target?.provider);
  const previousRootModel = safeModelName(target?.previousRootModel);
  const targetRootModel = safeModelName(target?.model);
  const modelMode = target?.modelMode;
  if (!previousProvider || !targetProvider || previousRootModel === undefined || targetRootModel === undefined
      || !["provider-default", "keep-root-model", "explicit"].includes(String(modelMode))) return undefined;
  return { previousProvider, targetProvider, previousRootModel, targetRootModel, modelMode: modelMode as "provider-default" | "keep-root-model" | "explicit" };
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function resultCounts(value: unknown): Record<string, number> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  const counts: Record<string, number> = {};
  for (const [key, raw] of Object.entries(value)) {
    if (!COUNT_FIELDS.has(key)) continue;
    if (Number.isSafeInteger(raw) && Number(raw) >= 0) counts[key] = Number(raw);
    else if (Array.isArray(raw) && /(?:count|files|rows|sessions|backups|warnings|skipped)/i.test(key)) counts[key] = raw.length;
  }
  return counts;
}

function findLastStage(
  stages: readonly OperationLogEntry["stages"][number][],
  predicate: (stage: OperationLogEntry["stages"][number]) => boolean
): OperationLogEntry["stages"][number] | undefined {
  for (let index = stages.length - 1; index >= 0; index -= 1) {
    const stage = stages[index];
    if (stage && predicate(stage)) return stage;
  }
  return undefined;
}

export class OperationLogService {
  readonly #root: string;
  readonly #activeRoot: string;
  readonly #maxFileBytes: number;
  readonly #maxFiles: number;
  readonly #entries = new Map<string, OperationLogEntry>();
  readonly #timings = new Map<string, ActiveTiming>();
  readonly #archiveIds: Set<string>[];
  #queue: Promise<void> = Promise.resolve();

  constructor(options: { directory: string; maxFileBytes?: number; maxFiles?: number }) {
    this.#root = path.resolve(options.directory);
    this.#activeRoot = path.join(this.#root, "active");
    this.#maxFileBytes = options.maxFileBytes ?? DEFAULT_MAX_FILE_BYTES;
    this.#maxFiles = options.maxFiles ?? DEFAULT_MAX_FILES;
    this.#archiveIds = Array.from({ length: this.#maxFiles }, () => new Set<string>());
  }

  async initialize(): Promise<void> {
    await fs.mkdir(this.#activeRoot, { recursive: true });
    for (let index = this.#maxFiles - 1; index >= 0; index -= 1) {
      const text = await fs.readFile(this.#archivePath(index), "utf8").catch(() => "");
      for (const line of text.split(/\r?\n/)) {
        if (!line) continue;
        try {
          const entry = JSON.parse(line) as OperationLogEntry;
          if (entry.schemaVersion === 1 && typeof entry.id === "string") {
            entry.counts = entry.counts && typeof entry.counts === "object" ? entry.counts : {};
            this.#entries.set(entry.id, validateOperationLogEntry(entry));
            this.#archiveIds[index]!.add(entry.id);
          }
        } catch {}
      }
    }
    const activeFiles = await fs.readdir(this.#activeRoot).catch(() => []);
    for (const name of activeFiles.filter((value) => value.endsWith(".json"))) {
      try {
        const filePath = path.join(this.#activeRoot, name);
        const entry = JSON.parse(await fs.readFile(filePath, "utf8")) as OperationLogEntry;
        if (entry.schemaVersion !== 1 || typeof entry.id !== "string") continue;
        entry.counts = entry.counts && typeof entry.counts === "object" ? entry.counts : {};
        entry.status = "interrupted";
        entry.completedAt = new Date().toISOString();
        entry.wallDurationMs = Math.max(0, Date.parse(entry.completedAt) - Date.parse(entry.startedAt));
        validateOperationLogEntry(entry);
        await this.#append(entry);
        await fs.rm(filePath, { force: true });
        this.#entries.set(entry.id, entry);
      } catch {}
    }
  }

  async begin(input: BeginInput): Promise<string> {
    const id = randomUUID();
    const now = new Date().toISOString();
    const entry: OperationLogEntry = {
      schemaVersion: 1,
      id,
      operation: safeText(input.operation, 80) ?? "operation",
      ...(safeText(input.profileId, 80) ? { profileId: safeText(input.profileId, 80) } : {}),
      ...(safeText(input.profileRevision, 256) ? { profileRevision: safeText(input.profileRevision, 256) } : {}),
      startedAt: now,
      activeDurationMs: 0,
      status: "running",
      requestIds: safeText(input.requestId, 512) ? [input.requestId!] : [],
      counts: {},
      warnings: [],
      stages: [{ stage: safeText(input.stage, 120) ?? "prepare", status: "running", startedAt: now }]
    };
    this.#entries.set(id, entry);
    const monotonicNow = performance.now();
    this.#timings.set(id, {
      resumedAt: monotonicNow,
      wallStartedAt: monotonicNow,
      stageStartedAt: new Map([[entry.stages[0]!.stage, monotonicNow]])
    });
    await this.#persistActive(entry);
    return id;
  }

  async prepared(id: string, planId: string, summary?: PreparedSummary): Promise<void> {
    const entry = this.#require(id);
    this.#pause(entry);
    this.#completeStage(entry, "prepare", "completed");
    entry.planId = safeText(planId, 128);
    const targetProvider = safeProviderId(summary?.target?.provider);
    const plannedCounts = previewCounts(summary);
    if (targetProvider) entry.targetProvider = targetProvider;
    if (plannedCounts) entry.previewCounts = plannedCounts;
    const plannedSwitch = switchPlan(summary);
    if (plannedSwitch) entry.switchPlan = plannedSwitch;
    entry.status = "awaiting-confirmation";
    await this.#persistActive(entry);
  }

  async resume(id: string, requestId?: string): Promise<void> {
    const entry = this.#require(id);
    if (safeText(requestId, 512) && !entry.requestIds.includes(requestId!)) entry.requestIds.push(requestId!);
    entry.status = "running";
    const timing = this.#timings.get(id);
    if (timing) timing.resumedAt = performance.now();
    else {
      const monotonicNow = performance.now();
      this.#timings.set(id, { resumedAt: monotonicNow, wallStartedAt: monotonicNow, stageStartedAt: new Map() });
    }
    await this.progress(id, { stage: "validate_plan", status: "start" });
  }

  async bindOperation(id: string, operationId: string): Promise<void> {
    const entry = this.#require(id);
    entry.operationId = safeText(operationId, 128);
    await this.#persistActive(entry);
  }

  async progress(id: string, event: ProgressEvent): Promise<void> {
    const entry = this.#require(id);
    const stage = safeText(event.stage, 120);
    if (!stage) return;
    if (stage !== "validate_plan") this.#completeStage(entry, "validate_plan", "completed");
    if (event.status === "start") {
      const existing = findLastStage(entry.stages, (candidate) => candidate.stage === stage && candidate.status === "running");
      if (!existing && entry.stages.length < MAX_STAGES) {
        entry.stages.push({ stage, status: "running", startedAt: new Date().toISOString(), ...(event.progress === undefined ? {} : { progress: event.progress }), ...(event.count === undefined ? {} : { count: event.count }) });
        this.#timings.get(id)?.stageStartedAt.set(stage, performance.now());
      }
    } else {
      this.#completeStage(entry, stage, event.status === "complete" ? "completed" : "failed", event);
    }
    await this.#persistActive(entry);
  }

  async requestProgress(id: string, event: ProgressEvent): Promise<void> {
    const entry = this.#require(id);
    const stage = safeText(event.stage, 120);
    if (!stage) return;

    const active = findLastStage(entry.stages, (candidate) => candidate.status === "running");
    if (active && active.stage !== stage) {
      this.#completeStage(entry, active.stage, "completed");
    }
    const current = findLastStage(
      entry.stages,
      (candidate) => candidate.stage === stage && candidate.status === "running"
    );
    if (event.status === "running") {
      if (current) {
        if (event.progress !== undefined) current.progress = event.progress;
        if (event.count !== undefined) current.count = event.count;
      } else if (entry.stages.length < MAX_STAGES) {
        entry.stages.push({
          stage,
          status: "running",
          startedAt: new Date().toISOString(),
          ...(event.progress === undefined ? {} : { progress: event.progress }),
          ...(event.count === undefined ? {} : { count: event.count })
        });
        this.#timings.get(id)?.stageStartedAt.set(stage, performance.now());
      }
    } else if (event.status === "completed") {
      if (!current && entry.stages.length < MAX_STAGES) {
        entry.stages.push({ stage, status: "running", startedAt: new Date().toISOString() });
        this.#timings.get(id)?.stageStartedAt.set(stage, performance.now());
      }
      this.#completeStage(entry, stage, "completed", event);
    } else {
      this.#completeStage(entry, stage, "failed", event);
    }
    await this.#persistActive(entry);
  }

  async dismiss(id: string): Promise<void> {
    await this.finish(id, { status: "dismissed", outcome: "dismissed" });
  }

  async finishFromResponse(id: string, response: CoreResponseEnvelope): Promise<void> {
    if (response.ok) {
      const result = response.result as Record<string, unknown>;
      const outcome = safeText(result?.outcome, 80) ?? "completed";
      const status: OperationLogStatus = outcome === "partial" ? "partial" : outcome === "cancelled" ? "cancelled" : ["failed_rolled_back", "recovery_required", "stale"].includes(outcome) ? "failed" : "completed";
      const backup = result?.backup && typeof result.backup === "object" ? result.backup as Record<string, unknown> : null;
      const details = result?.result && typeof result.result === "object" && !Array.isArray(result.result) ? result.result as Record<string, unknown> : {};
      const targetProvider = safeProviderId(details.targetProvider);
      if (targetProvider) this.#require(id).targetProvider = targetProvider;
      await this.finish(id, {
        status,
        outcome,
        backupId: safeText(backup?.backupId, 180),
        counts: { ...resultCounts(result), ...resultCounts(details) },
        fileUpdateTiming: details.fileUpdateTiming,
        failedStage: typeof details.failedStage === "string" ? details.failedStage : undefined,
        failureCode: typeof details.failureCode === "string" ? details.failureCode : undefined,
        partialReason: typeof details.partialReason === "string" ? details.partialReason : undefined,
        retryRecommended: typeof details.retryRecommended === "boolean" ? details.retryRecommended : undefined,
        warnings: Array.isArray(result?.warnings) ? result.warnings : []
      });
      return;
    }
    await this.finish(id, {
      status: response.error.code === "OPERATION_CANCELLED" ? "cancelled" : "failed",
      errorCode: response.error.code,
      errorReason: safeErrorReason(response.error.details?.reason),
      failedStage: typeof response.error.details?.failureStage === "string"
        ? response.error.details.failureStage
        : undefined,
      failureCode: typeof response.error.details?.causeCode === "string"
        ? response.error.details.causeCode
        : undefined,
      outcome: response.error.code === "OPERATION_CANCELLED" ? "cancelled" : "failed"
    });
  }

  async finish(id: string, fields: { status: OperationLogStatus; outcome?: string; errorCode?: string; errorReason?: string; backupId?: string; profileId?: string; counts?: Record<string, number>; fileUpdateTiming?: unknown; warnings?: unknown[]; failedStage?: string; failureCode?: string; partialReason?: string; retryRecommended?: boolean }): Promise<void> {
    const entry = this.#require(id);
    this.#pause(entry);
    for (const running of entry.stages.filter((stage) => stage.status === "running")) {
      this.#completeStage(entry, running.stage, running.stage === fields.failedStage || (fields.status !== "completed" && fields.status !== "partial") ? "failed" : "completed");
    }
    entry.status = fields.status;
    const fileUpdateTiming = publicFileUpdateTiming(fields.fileUpdateTiming);
    if (fileUpdateTiming) entry.fileUpdateTiming = fileUpdateTiming;
    if (safeText(fields.profileId, 80)) entry.profileId = safeText(fields.profileId, 80);
    entry.outcome = safeText(fields.outcome, 80);
    entry.errorCode = safeText(fields.errorCode, 120);
    const errorReason = safeErrorReason(fields.errorReason);
    if (errorReason) entry.errorReason = errorReason;
    entry.backupId = safeText(fields.backupId, 180);
    if (fields.failedStage && FAILURE_STAGES.has(fields.failedStage)) entry.failedStage = fields.failedStage;
    if (fields.failureCode) entry.failureCode = FAILURE_CODES.has(fields.failureCode) ? fields.failureCode : "INTERNAL_ERROR";
    if (fields.partialReason && PARTIAL_REASONS.has(fields.partialReason)) entry.partialReason = fields.partialReason as OperationLogEntry["partialReason"];
    if (typeof fields.retryRecommended === "boolean") entry.retryRecommended = fields.retryRecommended;
    entry.counts = Object.fromEntries(Object.entries(fields.counts ?? entry.counts ?? {}).filter(([key, value]) => Boolean(safeText(key, 80)) && Number.isSafeInteger(value) && value >= 0).slice(0, 64));
    entry.warnings = (fields.warnings ?? []).map((value) => safeText(value, 512)).filter((value): value is string => Boolean(value)).slice(0, MAX_WARNINGS);
    entry.completedAt = new Date().toISOString();
    const timing = this.#timings.get(id);
    entry.wallDurationMs = timing
      ? Math.max(0, Math.round(performance.now() - timing.wallStartedAt))
      : Math.max(0, Date.parse(entry.completedAt) - Date.parse(entry.startedAt));
    await this.#enqueue(async () => {
      await this.#append(entry);
      await fs.rm(this.#activePath(entry.id), { force: true });
    });
    this.#timings.delete(id);
  }

  list(input: OperationLogListInput): OperationLogListResponse {
    const entries = [...this.#entries.values()]
      .filter((entry) => !input.profileId || entry.profileId === input.profileId)
      .filter((entry) => !input.profileRevision || entry.profileRevision === input.profileRevision)
      .filter((entry) => !input.operation || entry.operation === input.operation)
      .filter((entry) => !input.status || entry.status === input.status)
      .sort((left, right) => Date.parse(right.startedAt) - Date.parse(left.startedAt));
    const start = (input.page - 1) * input.pageSize;
    return { schemaVersion: 1, page: input.page, pageSize: input.pageSize, total: entries.length, hasNextPage: start + input.pageSize < entries.length, entries: clone(entries.slice(start, start + input.pageSize)) };
  }

  get(id: string): OperationLogEntry | null {
    const entry = this.#entries.get(id);
    return entry ? clone(entry) : null;
  }

  recentJsonLines(limit = 200): string {
    return [...this.#entries.values()].sort((left, right) => Date.parse(right.startedAt) - Date.parse(left.startedAt)).slice(0, limit).map((entry) => JSON.stringify(entry)).join("\n");
  }

  #require(id: string): OperationLogEntry {
    const entry = this.#entries.get(id);
    if (!entry) throw new Error("Unknown operation log entry.");
    return entry;
  }

  #pause(entry: OperationLogEntry): void {
    const timing = this.#timings.get(entry.id);
    if (timing?.resumedAt !== null && timing?.resumedAt !== undefined) entry.activeDurationMs += Math.max(0, Math.round(performance.now() - timing.resumedAt));
    if (timing) timing.resumedAt = null;
  }

  #completeStage(entry: OperationLogEntry, name: string, status: "completed" | "failed", event?: ProgressEvent): void {
    const stage = findLastStage(entry.stages, (candidate) => candidate.stage === name && candidate.status === "running");
    if (!stage) return;
    stage.status = status;
    stage.completedAt = new Date().toISOString();
    const timing = this.#timings.get(entry.id);
    const monotonicStartedAt = timing?.stageStartedAt.get(name);
    stage.durationMs = monotonicStartedAt === undefined
      ? Math.max(0, Date.parse(stage.completedAt) - Date.parse(stage.startedAt))
      : Math.max(0, Math.round(performance.now() - monotonicStartedAt));
    timing?.stageStartedAt.delete(name);
    if (event?.progress !== undefined) stage.progress = event.progress;
    if (event?.count !== undefined) stage.count = event.count;
  }

  async #persistActive(entry: OperationLogEntry): Promise<void> {
    const target = this.#activePath(entry.id);
    const temporary = `${target}.tmp-${process.pid}-${randomUUID()}`;
    await this.#enqueue(async () => {
      await fs.mkdir(this.#activeRoot, { recursive: true });
      await fs.writeFile(temporary, `${JSON.stringify(entry)}\n`, { encoding: "utf8", mode: 0o600 });
      await fs.rename(temporary, target);
    });
  }

  async #append(entry: OperationLogEntry): Promise<void> {
    const line = `${JSON.stringify(entry)}\n`;
    const current = this.#archivePath(0);
    const size = await fs.stat(current).then((value) => value.size).catch(() => 0);
    let rotated = false;
    if (size + Buffer.byteLength(line) > this.#maxFileBytes) {
      rotated = true;
      await fs.rm(this.#archivePath(this.#maxFiles - 1), { force: true });
      this.#archiveIds[this.#maxFiles - 1] = new Set();
      for (let index = this.#maxFiles - 2; index >= 0; index -= 1) {
        await fs.rename(this.#archivePath(index), this.#archivePath(index + 1)).catch((error: NodeJS.ErrnoException) => {
          if (error.code !== "ENOENT") throw error;
        });
        this.#archiveIds[index + 1] = this.#archiveIds[index]!;
        this.#archiveIds[index] = new Set();
      }
    }
    await fs.appendFile(current, line, { encoding: "utf8", mode: 0o600 });
    this.#archiveIds[0]!.add(entry.id);
    if (rotated) {
      const retained = new Set(this.#archiveIds.flatMap((ids) => [...ids]));
      for (const id of this.#entries.keys()) {
        if (!retained.has(id) && !this.#timings.has(id)) this.#entries.delete(id);
      }
    }
  }

  #enqueue(task: () => Promise<void>): Promise<void> {
    const next = this.#queue.then(task, task);
    this.#queue = next.catch(() => {});
    return next;
  }

  #activePath(id: string): string { return path.join(this.#activeRoot, `${id}.json`); }
  #archivePath(index: number): string { return path.join(this.#root, `operations-${index}.jsonl`); }
}
