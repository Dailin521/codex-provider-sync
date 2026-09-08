import type { OperationLogEntry, OperationLogStage, OperationLogStatus } from "./operation-log-types.js";
import { isFileUpdateTiming } from "@codex-provider-sync/contracts";

export const operationLogStatuses = new Set<OperationLogStatus>(["running", "awaiting-confirmation", "completed", "partial", "failed", "cancelled", "dismissed", "interrupted"]);
// A new DTO field must also be consciously admitted at the transport boundary.
const entryFields = {
  schemaVersion: true, id: true, operation: true, profileId: true, profileRevision: true,
  startedAt: true, completedAt: true, activeDurationMs: true, wallDurationMs: true,
  status: true, outcome: true, errorCode: true, failedStage: true, failureCode: true,
  partialReason: true, retryRecommended: true, requestIds: true, planId: true,
  operationId: true, backupId: true, targetProvider: true, previewCounts: true,
  errorReason: true, switchPlan: true, fileUpdateTiming: true, counts: true, warnings: true, stages: true
} satisfies Record<keyof OperationLogEntry, true>;
const stageFields = { stage: true, status: true, startedAt: true, completedAt: true, durationMs: true, progress: true, count: true } satisfies Record<keyof OperationLogStage, true>;
const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value);
const text = (value: unknown, max: number) => typeof value === "string" && value.length > 0 && value.length <= max;
const number = (value: unknown) => typeof value === "number" && Number.isFinite(value) && value >= 0;
const date = (value: unknown) => text(value, 64) && Number.isFinite(Date.parse(value as string));
const modelName = (value: unknown) => value === null || (text(value, 256)
  && !/[\u0000-\u001f\\]/.test(value as string)
  && !/^(?:[A-Za-z]:[\\/]|[\\/]|[A-Za-z][A-Za-z0-9+.-]*:\/\/)/.test(value as string)
  && !/^(?:sk-|pk-|bearer\s|(?:api[_-]?key|token|secret|password)\s*[=:])/i.test(value as string));

export function validateOperationLogEntry(value: unknown): OperationLogEntry {
  const fail = () => { throw new TypeError("Invalid operation log entry."); };
  if (!record(value)) return fail();
  if (Object.keys(value).some((key) => !Object.hasOwn(entryFields, key))
      || value.schemaVersion !== 1 || !text(value.id, 128) || !text(value.operation, 80)
      || !operationLogStatuses.has(value.status as OperationLogStatus) || !date(value.startedAt)
      || !number(value.activeDurationMs) || !Array.isArray(value.requestIds) || value.requestIds.length > 128
      || !value.requestIds.every((id) => text(id, 512)) || !record(value.counts)
      || Object.keys(value.counts).length > 64 || Object.entries(value.counts).some(([key, count]) => !text(key, 80) || !Number.isSafeInteger(count) || Number(count) < 0)
      || !Array.isArray(value.warnings) || value.warnings.length > 64 || !value.warnings.every((warning) => text(warning, 512))
      || !Array.isArray(value.stages) || value.stages.length > 256) return fail();
  for (const [key, max] of Object.entries({ profileId: 80, profileRevision: 512, outcome: 80, errorCode: 120, failedStage: 120, failureCode: 120, planId: 128, operationId: 128, backupId: 180 })) {
    if (value[key] !== undefined && !text(value[key], max)) return fail();
  }
  if ((value.targetProvider !== undefined && (typeof value.targetProvider !== "string" || !/^[A-Za-z0-9._-]{1,128}$/.test(value.targetProvider)))
      || (value.errorReason !== undefined && !["profile", "config", "storage", "rollout", "state-db", "backup", "provider-not-configured"].includes(String(value.errorReason)))) return fail();
  const planned = value.previewCounts;
  if (value.fileUpdateTiming !== undefined && !isFileUpdateTiming(value.fileUpdateTiming)) return fail();
  if (planned !== undefined
      && (!record(planned)
        || Object.keys(planned).sort().join(",") !== "lockedRolloutFiles,rolloutFilesToChange,sqliteRowsToChange"
        || !["rolloutFilesToChange", "sqliteRowsToChange", "lockedRolloutFiles"].every((key) => Number.isSafeInteger(planned[key]) && Number(planned[key]) >= 0))) return fail();
  const switchPlan = value.switchPlan;
  if (switchPlan !== undefined
      && (!record(switchPlan)
        || Object.keys(switchPlan).sort().join(",") !== "modelMode,previousProvider,previousRootModel,targetProvider,targetRootModel"
        || typeof switchPlan.previousProvider !== "string" || !/^[A-Za-z0-9._-]{1,128}$/.test(switchPlan.previousProvider)
        || typeof switchPlan.targetProvider !== "string" || !/^[A-Za-z0-9._-]{1,128}$/.test(switchPlan.targetProvider)
        || !["provider-default", "keep-root-model", "explicit"].includes(String(switchPlan.modelMode))
        || ![switchPlan.previousRootModel, switchPlan.targetRootModel].every(modelName))) return fail();
  if ((value.completedAt !== undefined && !date(value.completedAt))
      || (value.wallDurationMs !== undefined && !number(value.wallDurationMs))
      || (value.retryRecommended !== undefined && typeof value.retryRecommended !== "boolean")
      || (value.partialReason !== undefined && !["locked-session", "rollout-changed", "mutation-failed"].includes(String(value.partialReason)))) return fail();
  for (const stage of value.stages) {
    if (!record(stage) || Object.keys(stage).some((key) => !Object.hasOwn(stageFields, key))
        || !text(stage.stage, 120) || !["running", "completed", "failed"].includes(String(stage.status)) || !date(stage.startedAt)
        || (stage.completedAt !== undefined && !date(stage.completedAt))
        || ["durationMs", "progress"].some((key) => stage[key] !== undefined && !number(stage[key]))
        || (stage.count !== undefined && (!Number.isSafeInteger(stage.count) || Number(stage.count) < 0))) return fail();
  }
  return structuredClone(value) as unknown as OperationLogEntry;
}
