import {
  CORE_METHODS,
  CORE_PROTOCOL_VERSION,
  type ApplyPlanInput,
  type CoreMethodMap,
  type CoreMethodName,
  type CoreProtocolVersion,
  type ProgressEvent
} from "./dto.js";
import {
  CORE_ERROR_CODES,
  isCanonicalPublicCoreErrorDto,
  type CoreErrorCode,
  type CoreErrorDto,
  type CoreErrorSeverity
} from "./errors.js";

export interface CoreRequestEnvelope<M extends CoreMethodName = CoreMethodName> {
  protocolVersion: CoreProtocolVersion;
  requestId: string;
  operationId?: string;
  method: M;
  payload: CoreMethodMap[M]["input"];
}

export type CoreResponseEnvelope<M extends CoreMethodName = CoreMethodName> =
  | {
      protocolVersion: CoreProtocolVersion;
      requestId: string;
      operationId?: string;
      ok: true;
      result: CoreMethodMap[M]["output"];
    }
  | {
      protocolVersion: CoreProtocolVersion;
      requestId: string;
      operationId?: string;
      ok: false;
      error: CoreErrorDto;
    };

export interface CoreProgressEnvelope {
  protocolVersion: CoreProtocolVersion;
  requestId: string;
  operationId: string;
  event: "progress";
  progress: ProgressEvent;
}

export interface CoreOperationStartedEnvelope {
  protocolVersion: CoreProtocolVersion;
  requestId: string;
  operationId: string;
  event: "operation-started";
  operation: "sync" | "switch" | "restore";
}

export type CoreOperationEventEnvelope = CoreOperationStartedEnvelope | CoreProgressEnvelope;

const METHOD_SET = new Set<string>(CORE_METHODS);
const ERROR_CODE_SET = new Set<string>(CORE_ERROR_CODES);
const SEVERITY_SET = new Set<string>(["info", "warning", "error", "fatal"]);
const PROVIDER_SYNC_MODES = new Set<string>(["full", "fast"]);
const PROVIDER_SYNC_UNCHECKED = ["historyModels", "userEventFlags", "encryptedContent"] as const;
const PROVIDER_SYNC_UNCHECKED_SET = new Set<string>(PROVIDER_SYNC_UNCHECKED);

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function exactObjectKeys(value: Record<string, unknown>, allowed: readonly string[]): boolean {
  const allowedSet = new Set(allowed);
  return Object.keys(value).every((key) => allowedSet.has(key));
}

function assertProfileSelector(value: unknown): void {
  if (!isRecord(value)
      || !exactObjectKeys(value, ["profileId", "profileRevision"])
      || typeof value.profileId !== "string"
      || !/^[A-Za-z0-9._-]{1,80}$/.test(value.profileId)
      || (value.profileRevision !== undefined
        && (!isNonEmptyString(value.profileRevision) || value.profileRevision.length > 512))) {
    throw new ContractValidationError("INVALID_INPUT", "Invalid profile selector.");
  }
}

function assertProfileInput(
  value: unknown,
  allowed: readonly string[]
): asserts value is Record<string, unknown> {
  if (!isRecord(value)
      || !exactObjectKeys(value, ["profile", ...allowed])) {
    throw new ContractValidationError("INVALID_INPUT", "Invalid Core method input.");
  }
  assertProfileSelector(value.profile);
}

export class ContractValidationError extends Error {
  readonly code: "INVALID_INPUT" | "PROTOCOL_VERSION_MISMATCH";

  constructor(
    code: "INVALID_INPUT" | "PROTOCOL_VERSION_MISMATCH",
    message: string
  ) {
    super(message);
    this.name = "ContractValidationError";
    this.code = code;
  }
}

export function assertProtocolVersion(value: unknown): asserts value is CoreProtocolVersion {
  if (value !== CORE_PROTOCOL_VERSION) {
    throw new ContractValidationError(
      "PROTOCOL_VERSION_MISMATCH",
      `Unsupported Core protocol version: ${String(value)}.`
    );
  }
}

export function assertApplyPlanInput(value: unknown): asserts value is ApplyPlanInput {
  if (!isRecord(value)
      || Object.keys(value).sort().join(",") !== "planId,schemaVersion"
      || value.schemaVersion !== 1
      || !isNonEmptyString(value.planId)) {
    throw new ContractValidationError(
      "INVALID_INPUT",
      "Apply accepts exactly { schemaVersion: 1, planId }."
    );
  }
}

export function assertCoreMethodInput<M extends CoreMethodName>(
  method: M,
  value: unknown
): asserts value is CoreMethodMap[M]["input"] {
  switch (method) {
    case "applySync":
    case "applySwitch":
    case "applyRestore":
      assertApplyPlanInput(value);
      return;
    case "getStatus":
    case "listBackups":
    case "getDiagnostics":
      assertProfileInput(value, []);
      return;
    case "prepareSync":
      assertProfileInput(value, ["keepCount", "syncMode"]);
      if (value.keepCount !== undefined
          && (!Number.isSafeInteger(value.keepCount) || Number(value.keepCount) < 1)
          || (value.syncMode !== undefined && !PROVIDER_SYNC_MODES.has(String(value.syncMode)))) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid Sync retention count.");
      }
      return;
    case "prepareSwitch":
      assertProfileInput(value, ["provider", "modelMode", "model", "keepCount", "syncMode"]);
      if (!isNonEmptyString(value.provider)
          || !["provider-default", "keep-root-model", "explicit"].includes(String(value.modelMode))
          || (value.modelMode === "explicit" && !isNonEmptyString(value.model))
          || (value.modelMode !== "explicit" && value.model !== undefined)
          || (value.syncMode !== undefined && !PROVIDER_SYNC_MODES.has(String(value.syncMode)))
          || (value.syncMode === "fast" && value.modelMode !== "keep-root-model")
          || (value.keepCount !== undefined
            && (!Number.isSafeInteger(value.keepCount) || Number(value.keepCount) < 1))) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid Switch Provider input.");
      }
      return;
    case "prepareRestore":
      assertProfileInput(value, [
        "backupId",
        "restoreConfig",
        "restoreDatabase",
        "restoreSessions",
        "allowSqliteHomeRelocation",
        "relocationTargetProfileId"
      ]);
      if (!isNonEmptyString(value.backupId)
          || typeof value.restoreConfig !== "boolean"
          || typeof value.restoreDatabase !== "boolean"
          || typeof value.restoreSessions !== "boolean"
          || (value.allowSqliteHomeRelocation !== undefined
            && typeof value.allowSqliteHomeRelocation !== "boolean")
          || (value.relocationTargetProfileId !== undefined
            && (typeof value.relocationTargetProfileId !== "string"
              || !/^[A-Za-z0-9._-]{1,80}$/.test(value.relocationTargetProfileId)))
          || (value.allowSqliteHomeRelocation === true
            && (value.restoreConfig !== false || value.relocationTargetProfileId === undefined))
          || (value.relocationTargetProfileId !== undefined
            && value.allowSqliteHomeRelocation !== true)) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid Restore input.");
      }
      return;
    case "pruneBackups":
      assertProfileInput(value, ["keepCount"]);
      if (!Number.isSafeInteger(value.keepCount) || Number(value.keepCount) < 0) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid Prune retention count.");
      }
      return;
    case "listHistory":
      assertProfileInput(value, ["page", "pageSize", "query", "project", "provider", "archived"]);
      if ((value.page !== undefined && (!Number.isSafeInteger(value.page) || Number(value.page) < 1))
          || (value.pageSize !== undefined
            && (!Number.isSafeInteger(value.pageSize)
              || Number(value.pageSize) < 10
              || Number(value.pageSize) > 100))
          || ["query", "project", "provider"].some((key) => (
            value[key] !== undefined && typeof value[key] !== "string"
          ))
          || (value.archived !== undefined
            && !["all", "active", "archived"].includes(String(value.archived)))) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid History list input.");
      }
      return;
    case "getHistorySession":
      assertProfileInput(value, ["sessionId", "messageLimit"]);
      if (!isNonEmptyString(value.sessionId)
          || (value.messageLimit !== undefined
            && (!Number.isSafeInteger(value.messageLimit)
              || Number(value.messageLimit) < 1
              || Number(value.messageLimit) > 200))) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid History detail input.");
      }
      return;
    case "startWatch":
      assertProfileInput(value, ["includeStateDb", "debounceMs", "once"]);
      if ((value.includeStateDb !== undefined && typeof value.includeStateDb !== "boolean")
          || (value.once !== undefined && typeof value.once !== "boolean")
          || (value.debounceMs !== undefined
            && (!Number.isSafeInteger(value.debounceMs) || Number(value.debounceMs) < 0))) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid Watch input.");
      }
      return;
    case "stopWatch":
      if (!isRecord(value)
          || !exactObjectKeys(value, ["watchId"])
          || !isNonEmptyString(value.watchId)) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid Watch reference.");
      }
      return;
    case "getWatchStatus":
      if (!isRecord(value)
          || !exactObjectKeys(value, ["watchId"])
          || (value.watchId !== undefined && !isNonEmptyString(value.watchId))) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid Watch status input.");
      }
      return;
    default:
      throw new ContractValidationError("INVALID_INPUT", "Unknown Core method input.");
  }
}

export function assertCoreErrorDto(value: unknown): asserts value is CoreErrorDto {
  if (!isCanonicalPublicCoreErrorDto(value)) {
    throw new ContractValidationError("INVALID_INPUT", "Invalid public CoreErrorDto.");
  }
}

export function assertCoreRequestEnvelope<M extends CoreMethodName = CoreMethodName>(
  value: unknown
): asserts value is CoreRequestEnvelope<M> {
  if (!isRecord(value)) {
    throw new ContractValidationError("INVALID_INPUT", "Core request envelope must be an object.");
  }
  const allowedKeys = new Set(["protocolVersion", "requestId", "operationId", "method", "payload"]);
  if (Object.keys(value).some((key) => !allowedKeys.has(key))) {
    throw new ContractValidationError("INVALID_INPUT", "Core request envelope has unknown fields.");
  }
  assertProtocolVersion(value.protocolVersion);
  if (!isNonEmptyString(value.requestId)
      || !isNonEmptyString(value.method)
      || !METHOD_SET.has(value.method)
      || !isRecord(value.payload)) {
    throw new ContractValidationError("INVALID_INPUT", "Invalid Core request envelope.");
  }
  if (value.operationId !== undefined && !isNonEmptyString(value.operationId)) {
    throw new ContractValidationError("INVALID_INPUT", "Invalid Core request operationId.");
  }
  assertCoreMethodInput(value.method as M, value.payload);
}

export function assertCoreResponseEnvelope<M extends CoreMethodName = CoreMethodName>(
  value: unknown,
  expectedRequestId?: string
): asserts value is CoreResponseEnvelope<M> {
  if (!isRecord(value)) {
    throw new ContractValidationError("INVALID_INPUT", "Core response envelope must be an object.");
  }
  const allowedKeys = value.ok === true
    ? new Set(["protocolVersion", "requestId", "operationId", "ok", "result"])
    : new Set(["protocolVersion", "requestId", "operationId", "ok", "error"]);
  if (Object.keys(value).some((key) => !allowedKeys.has(key))) {
    throw new ContractValidationError("INVALID_INPUT", "Core response envelope has unknown fields.");
  }
  assertProtocolVersion(value.protocolVersion);
  if (!isNonEmptyString(value.requestId)
      || (expectedRequestId !== undefined && value.requestId !== expectedRequestId)
      || typeof value.ok !== "boolean") {
    throw new ContractValidationError("INVALID_INPUT", "Invalid Core response envelope.");
  }
  if (value.operationId !== undefined && !isNonEmptyString(value.operationId)) {
    throw new ContractValidationError("INVALID_INPUT", "Invalid Core response operationId.");
  }
  if (value.ok) {
    if (!("result" in value) || "error" in value) {
      throw new ContractValidationError("INVALID_INPUT", "Invalid successful Core response.");
    }
  } else {
    if (!("error" in value) || "result" in value) {
      throw new ContractValidationError("INVALID_INPUT", "Invalid failed Core response.");
    }
    assertCoreErrorDto(value.error);
  }
}

function requireSchemaObject(value: unknown, label: string): Record<string, unknown> {
  if (!isRecord(value) || value.schemaVersion !== 1) {
    throw new ContractValidationError("INVALID_INPUT", `Invalid ${label}.`);
  }
  return value;
}

function requireStringArray(value: unknown, label: string): asserts value is string[] {
  if (!Array.isArray(value) || value.some((entry) => typeof entry !== "string")) {
    throw new ContractValidationError("INVALID_INPUT", `Invalid ${label}.`);
  }
}

function isNonNegativeInteger(value: unknown): boolean {
  return Number.isSafeInteger(value) && Number(value) >= 0;
}

function hasCanonicalUnchecked(value: unknown, fast: boolean): boolean {
  if (!Array.isArray(value)
      || value.some((entry) => typeof entry !== "string" || !PROVIDER_SYNC_UNCHECKED_SET.has(entry))) {
    return false;
  }
  const actual = [...new Set(value)].sort();
  const expected = fast ? [...PROVIDER_SYNC_UNCHECKED].sort() : [];
  return actual.length === expected.length
    && actual.every((entry, index) => entry === expected[index]);
}

function assertProviderSyncPlanDetails(value: unknown): void {
  const details = isRecord(value) ? value : null;
  const fast = details?.mode === "fast";
  if (!details
      || !exactObjectKeys(details, [
        "mode",
        "rolloutScanScope",
        "providerWritePolicy",
        "historicalModelSync",
        "unchecked",
        "inPlaceEligibleSessionFiles",
        "rewriteRequiredSessionFiles"
      ])
      || !PROVIDER_SYNC_MODES.has(String(details.mode))
      || details.rolloutScanScope !== (fast ? "metadata" : "full")
      || details.providerWritePolicy !== (fast ? "require-in-place" : "prefer-in-place")
      || details.historicalModelSync !== (fast ? "preserved" : "enabled")
      || !hasCanonicalUnchecked(details.unchecked, fast)
      || !isNonNegativeInteger(details.inPlaceEligibleSessionFiles)
      || !isNonNegativeInteger(details.rewriteRequiredSessionFiles)
      || (fast && details.rewriteRequiredSessionFiles !== 0)) {
    throw new ContractValidationError("INVALID_INPUT", "Invalid Provider Sync plan details.");
  }
}

function assertProviderSyncResultDetails(value: unknown): void {
  const details = isRecord(value) ? value : null;
  const fast = details?.mode === "fast";
  if (!details
      || !exactObjectKeys(details, [
        "mode",
        "rolloutScanScope",
        "inPlaceSessionFiles",
        "rewrittenSessionFiles",
        "unchecked"
      ])
      || !PROVIDER_SYNC_MODES.has(String(details.mode))
      || details.rolloutScanScope !== (fast ? "metadata" : "full")
      || !hasCanonicalUnchecked(details.unchecked, fast)
      || !isNonNegativeInteger(details.inPlaceSessionFiles)
      || !isNonNegativeInteger(details.rewrittenSessionFiles)
      || (fast && details.rewrittenSessionFiles !== 0)) {
    throw new ContractValidationError("INVALID_INPUT", "Invalid Provider Sync result details.");
  }
}

function isNullableString(value: unknown): boolean {
  return value === null || typeof value === "string";
}

function isJsonValue(value: unknown, depth = 0): boolean {
  if (depth > 16) return false;
  if (value === null || typeof value === "string" || typeof value === "boolean") return true;
  if (typeof value === "number") return Number.isFinite(value);
  if (Array.isArray(value)) return value.every((entry) => isJsonValue(entry, depth + 1));
  if (!isRecord(value)) return false;
  return Object.values(value).every((entry) => isJsonValue(entry, depth + 1));
}

function isProviderDistribution(value: unknown): boolean {
  if (!isRecord(value)) return false;
  return Object.values(value).every((counts) => (
    isRecord(counts) && Object.values(counts).every(isNonNegativeInteger)
  ));
}

const DIAGNOSTIC_TRANSACTION_STATES = new Set([
  "prepared",
  "applying",
  "applied",
  "skipped",
  "committing",
  "committed-pending-ack",
  "rollback-pending",
  "rollingBack",
  "recovery-required",
  "recoveryRequired",
  "unknown"
]);

function isDiagnosticIdentifier(value: unknown): value is string {
  return typeof value === "string"
    && /^[A-Za-z0-9._()-]{1,200}$/.test(value);
}

function isUuid(value: unknown): value is string {
  return typeof value === "string"
    && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function isDiagnosticCountMap(value: unknown): boolean {
  if (!isRecord(value) || Object.keys(value).length > 512) return false;
  return Object.entries(value).every(([provider, count]) =>
    isDiagnosticIdentifier(provider) && isNonNegativeInteger(count)
  );
}

function isDiagnosticDistribution(value: unknown, allowUnreadable = false): boolean {
  if (!isRecord(value)
      || !exactObjectKeys(value, allowUnreadable
        ? ["sessions", "archived_sessions", "unreadable"]
        : ["sessions", "archived_sessions"])
      || !("sessions" in value)
      || !("archived_sessions" in value)
      || !isDiagnosticCountMap(value.sessions)
      || !isDiagnosticCountMap(value.archived_sessions)) {
    return false;
  }
  return !allowUnreadable || value.unreadable === undefined || value.unreadable === true;
}

function isDiagnosticPendingTransaction(value: unknown): boolean {
  return isRecord(value)
    && Object.keys(value).sort().join(",")
      === "operationId,operationKind,preRestoreSnapshotId,sourceBackupId,state"
    && (value.operationId === null || isUuid(value.operationId))
    && ["sync", "switch", "restore"].includes(String(value.operationKind))
    && DIAGNOSTIC_TRANSACTION_STATES.has(String(value.state))
    && (value.sourceBackupId === null || isDiagnosticIdentifier(value.sourceBackupId))
    && (value.preRestoreSnapshotId === null
      || isDiagnosticIdentifier(value.preRestoreSnapshotId));
}

function isDiagnosticOperationState(value: unknown): boolean {
  if (value === null) return true;
  if (!isRecord(value)
      || !exactObjectKeys(value, [
        "operationId",
        "operation",
        "actor",
        "startedAt",
        "busyScope",
        "lockState",
        "errorCode"
      ])) {
    return false;
  }
  return (value.operationId === undefined || isUuid(value.operationId))
    && (value.operation === undefined
      || ["sync", "switch", "restore", "prune", "watch", "unknown"].includes(String(value.operation)))
    && (value.actor === undefined || ["manual", "watch", "external"].includes(String(value.actor)))
    && (value.startedAt === undefined
      || (isNonEmptyString(value.startedAt) && value.startedAt.length <= 64))
    && (value.busyScope === undefined || ["codex-home", "state-db"].includes(String(value.busyScope)))
    && (value.lockState === undefined
      || (isDiagnosticIdentifier(value.lockState) && value.lockState.length <= 80))
    && (value.errorCode === undefined
      || (typeof value.errorCode === "string" && /^[A-Z0-9_]{1,80}$/.test(value.errorCode)));
}

function assertDiagnosticsSnapshot(value: unknown): void {
  const diagnostics = requireSchemaObject(value, "DiagnosticsSnapshot");
  const runtime = isRecord(diagnostics.runtime) ? diagnostics.runtime : null;
  const storage = isRecord(diagnostics.storage) ? diagnostics.storage : null;
  const provider = isRecord(diagnostics.provider) ? diagnostics.provider : null;
  const safety = isRecord(diagnostics.safety) ? diagnostics.safety : null;
  const valid = exactObjectKeys(diagnostics, [
    "schemaVersion",
    "generatedAt",
    "runtime",
    "storage",
    "provider",
    "safety"
  ])
    && isNonEmptyString(diagnostics.generatedAt)
    && diagnostics.generatedAt.length <= 64
    && runtime !== null
    && Object.keys(runtime).sort().join(",") === "arch,node,platform"
    && [runtime.node, runtime.platform, runtime.arch].every((entry) =>
      typeof entry === "string" && /^[A-Za-z0-9._-]{1,80}$/.test(entry)
    )
    && storage !== null
    && Object.keys(storage).sort().join(",")
      === "sqliteHomeSource,sqliteSupported,stateDbFound"
    && ["cli", "config", "env", "default", "unknown"].includes(String(storage.sqliteHomeSource))
    && typeof storage.stateDbFound === "boolean"
    && typeof storage.sqliteSupported === "boolean"
    && provider !== null
    && Object.keys(provider).sort().join(",")
      === "configured,current,implicit,rolloutCounts,sqliteCounts"
    && isDiagnosticIdentifier(provider.current)
    && typeof provider.implicit === "boolean"
    && Array.isArray(provider.configured)
    && provider.configured.length <= 256
    && provider.configured.every(isDiagnosticIdentifier)
    && isDiagnosticDistribution(provider.rolloutCounts)
    && (provider.sqliteCounts === null
      || isDiagnosticDistribution(provider.sqliteCounts, true))
    && safety !== null
    && exactObjectKeys(safety, [
      "storageRevision",
      "pendingRecovery",
      "pendingTransactions",
      "operationInProgress",
      "rolloutScanComplete",
      "lockedRolloutCount",
      "projectThreadVisibilityAvailable"
    ])
    && (safety.storageRevision === undefined
      || (typeof safety.storageRevision === "string"
        && /^[A-Za-z0-9_-]{1,256}$/.test(safety.storageRevision)))
    && typeof safety.pendingRecovery === "boolean"
    && Array.isArray(safety.pendingTransactions)
    && safety.pendingTransactions.length <= 256
    && safety.pendingTransactions.every(isDiagnosticPendingTransaction)
    && isDiagnosticOperationState(safety.operationInProgress)
    && typeof safety.rolloutScanComplete === "boolean"
    && isNonNegativeInteger(safety.lockedRolloutCount)
    && typeof safety.projectThreadVisibilityAvailable === "boolean";
  if (!valid) {
    throw new ContractValidationError("INVALID_INPUT", "Invalid DiagnosticsSnapshot.");
  }
}

function isHistorySummary(value: unknown): boolean {
  if (!isRecord(value)) return false;
  return isNonEmptyString(value.id)
    && typeof value.title === "string"
    && !("cwd" in value)
    && isNonEmptyString(value.provider)
    && typeof value.archived === "boolean"
    && isNonEmptyString(value.updatedAt)
    && isNonNegativeInteger(value.messageCount)
    && (value.messageCountKnown === undefined || typeof value.messageCountKnown === "boolean")
    && (value.model === undefined || isNullableString(value.model))
    && (value.createdAt === undefined || isNonEmptyString(value.createdAt));
}

function assertWatchSnapshot(value: unknown): void {
  const snapshot = requireSchemaObject(value, "WatchSnapshot");
  if (!isNonEmptyString(snapshot.watchId)
      || !["running", "stopping", "stopped"].includes(String(snapshot.status))
      || !isNonEmptyString(snapshot.startedAt)
      || !isNullableString(snapshot.stoppedAt)
      || !isNullableString(snapshot.stopReason)
      || typeof snapshot.includeStateDb !== "boolean"
      || typeof snapshot.once !== "boolean") {
    throw new ContractValidationError("INVALID_INPUT", "Invalid WatchSnapshot.");
  }
}

export function assertCoreMethodOutput<M extends CoreMethodName>(
  method: M,
  value: unknown
): asserts value is CoreMethodMap[M]["output"] {
  switch (method) {
    case "getStatus": {
      const status = requireSchemaObject(value, "StatusSnapshot");
      const profile = isRecord(status.profile) ? status.profile : null;
      if (!isNonEmptyString(status.snapshotAt)
          || !isNonEmptyString(status.storageRevision)
          || !profile
          || !isNonEmptyString(profile.id)
          || !isNonEmptyString(profile.revision)
          || !isNonEmptyString(status.currentProvider)
          || !isProviderDistribution(status.rolloutCounts)
          || (status.modelCounts !== undefined && !isProviderDistribution(status.modelCounts))
          || !("sqliteCounts" in status)
          || !isJsonValue(status.sqliteCounts)
          || "codexHome" in status
          || "sqliteHome" in status
          || !isNonEmptyString(status.codexHomeSource)
          || !isNonEmptyString(status.sqliteHomeSource)
          || !isRecord(status.backupSummary)
          || !isNonNegativeInteger(status.backupSummary.count)
          || !isNonNegativeInteger(status.backupSummary.totalBytes)
          || typeof status.pendingRecovery !== "boolean"
          || !Array.isArray(status.pendingTransactions)
          || status.pendingTransactions.some((entry) => !isRecord(entry) || !isJsonValue(entry))
          || !(status.operationInProgress === null
            || (isRecord(status.operationInProgress) && isJsonValue(status.operationInProgress)))
          || typeof status.rolloutScanComplete !== "boolean"
          || !Array.isArray(status.lockedRolloutFiles)
          || status.lockedRolloutFiles.some((entry) => typeof entry !== "string")
          || (status.currentModel !== undefined && !isNullableString(status.currentModel))) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid StatusSnapshot.");
      }
      return;
    }
    case "prepareSync":
    case "prepareSwitch":
    case "prepareRestore": {
      const plan = requireSchemaObject(value, "PlanSummary");
      if (!isNonEmptyString(plan.planId)
          || !["sync", "switch", "restore"].includes(String(plan.operation))
          || !isNonEmptyString(plan.createdAt)
          || !isNonEmptyString(plan.expiresAt)
          || !isRecord(plan.profile)
          || !isNonEmptyString(plan.profile.id)
          || !isNonEmptyString(plan.profile.revision)
          || !isNonEmptyString(plan.storageRevision)
          || !isNonEmptyString(plan.configRevision)
          || !isNonEmptyString(plan.rolloutRevision)
          || !isNonEmptyString(plan.stateDbRevision)
          || (plan.backupRevision !== undefined && !isNonEmptyString(plan.backupRevision))
          || !isRecord(plan.target)
          || !isJsonValue(plan.target)
          || !isRecord(plan.impact)
          || !isJsonValue(plan.impact)
          || (plan.providerSync !== undefined && !isRecord(plan.providerSync))
          || !Array.isArray(plan.warnings)
          || plan.warnings.some((entry) => typeof entry !== "string")
          || typeof plan.requiresConfirmation !== "boolean") {
        throw new ContractValidationError("INVALID_INPUT", "Invalid PlanSummary.");
      }
      if (plan.providerSync !== undefined) assertProviderSyncPlanDetails(plan.providerSync);
      return;
    }
    case "applySync":
    case "applySwitch":
    case "applyRestore": {
      const result = requireSchemaObject(value, "OperationResult");
      if (!isNonEmptyString(result.operationId)
          || !["sync", "switch", "restore"].includes(String(result.operation))
          || !["completed", "partial", "failed_rolled_back", "recovery_required", "cancelled", "stale"].includes(String(result.outcome))
          || !(result.backup === null
            || (isRecord(result.backup) && isNonEmptyString(result.backup.backupId)))
          || (result.providerSync !== undefined && !isRecord(result.providerSync))) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid OperationResult.");
      }
      if (result.providerSync !== undefined) assertProviderSyncResultDetails(result.providerSync);
      requireStringArray(result.warnings, "OperationResult warnings");
      if (!("result" in result) || !isJsonValue(result.result)) {
        throw new ContractValidationError("INVALID_INPUT", "OperationResult result is required.");
      }
      return;
    }
    case "listBackups": {
      if (!isRecord(value) || !Array.isArray(value.backups)
          || value.backups.some((entry) => {
            const backup = isRecord(entry) ? entry : null;
            return !backup
              || !isNonEmptyString(backup.backupId)
              || !isNonNegativeInteger(backup.sizeBytes)
              || !isRecord(backup.metadata)
              || !isJsonValue(backup.metadata)
              || (backup.createdAt !== undefined && !isNonEmptyString(backup.createdAt));
          })) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid BackupList.");
      }
      return;
    }
    case "pruneBackups": {
      if (!isRecord(value)
          || !isNonNegativeInteger(value.deletedCount)
          || !isNonNegativeInteger(value.remainingCount)
          || !isNonNegativeInteger(value.freedBytes)) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid PruneBackupsResult.");
      }
      return;
    }
    case "listHistory": {
      if (!isRecord(value)
          || !Number.isSafeInteger(value.page)
          || Number(value.page) < 1
          || !Number.isSafeInteger(value.pageSize)
          || Number(value.pageSize) < 1
          || !isNonNegativeInteger(value.total)
          || typeof value.hasNextPage !== "boolean"
          || !Array.isArray(value.sessions)
          || value.sessions.some((entry) => !isHistorySummary(entry))) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid HistoryPage.");
      }
      return;
    }
    case "getHistorySession": {
      if (!isRecord(value)
          || !isHistorySummary(value.session)
          || !Array.isArray(value.messages)
          || value.messages.some((entry) => {
            const message = isRecord(entry) ? entry : null;
            return !message
              || !isNonEmptyString(message.role)
              || typeof message.text !== "string"
              || !isNonNegativeInteger(message.sequence)
              || (message.timestamp !== undefined && !isNonEmptyString(message.timestamp));
          })
          || typeof value.truncated !== "boolean"
          || !isNonNegativeInteger(value.returnedMessageCount)
          || Number(value.returnedMessageCount) !== value.messages.length) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid HistorySessionDetail.");
      }
      return;
    }
    case "startWatch":
    case "stopWatch":
      assertWatchSnapshot(value);
      return;
    case "getWatchStatus": {
      if (isRecord(value) && Array.isArray(value.watches)) {
        requireSchemaObject(value, "WatchStatusList");
        value.watches.forEach(assertWatchSnapshot);
        return;
      }
      assertWatchSnapshot(value);
      return;
    }
    case "getDiagnostics": {
      assertDiagnosticsSnapshot(value);
      return;
    }
    default:
      throw new ContractValidationError("INVALID_INPUT", "Unknown Core method output.");
  }
}

export function assertProgressEvent(value: unknown): asserts value is ProgressEvent {
  if (!isRecord(value)) {
    throw new ContractValidationError("INVALID_INPUT", "Progress event must be an object.");
  }
  const allowed = new Set(["stage", "status", "progress", "count"]);
  if (Object.keys(value).some((key) => !allowed.has(key))
      || !isNonEmptyString(value.stage)
      || !isNonEmptyString(value.status)
      || value.stage.length > 80
      || value.status.length > 40
      || (value.progress !== undefined
        && (typeof value.progress !== "number"
          || !Number.isFinite(value.progress)
          || value.progress < 0
          || value.progress > 1))
      || (value.count !== undefined
        && (!Number.isSafeInteger(value.count) || Number(value.count) < 0))) {
    throw new ContractValidationError("INVALID_INPUT", "Invalid ProgressEvent.");
  }
}

export function assertCoreOperationStartedEnvelope(
  value: unknown,
  expectedRequestId?: string,
  expectedOperationId?: string
): asserts value is CoreOperationStartedEnvelope {
  if (!isRecord(value)
      || !exactObjectKeys(value, [
        "protocolVersion",
        "requestId",
        "operationId",
        "event",
        "operation"
      ])) {
    throw new ContractValidationError("INVALID_INPUT", "Invalid operation-started envelope.");
  }
  assertProtocolVersion(value.protocolVersion);
  if (!isNonEmptyString(value.requestId)
      || value.requestId.length > 512
      || (expectedRequestId !== undefined && value.requestId !== expectedRequestId)
      || !isNonEmptyString(value.operationId)
      || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value.operationId)
      || (expectedOperationId !== undefined && value.operationId !== expectedOperationId)
      || value.event !== "operation-started"
      || !["sync", "switch", "restore"].includes(String(value.operation))) {
    throw new ContractValidationError("INVALID_INPUT", "Invalid operation-started envelope.");
  }
}

export function assertCoreProgressEnvelope(
  value: unknown,
  expectedRequestId?: string,
  expectedOperationId?: string
): asserts value is CoreProgressEnvelope {
  if (!isRecord(value)
      || !exactObjectKeys(value, [
        "protocolVersion",
        "requestId",
        "operationId",
        "event",
        "progress"
      ])) {
    throw new ContractValidationError("INVALID_INPUT", "Invalid Core progress envelope.");
  }
  assertProtocolVersion(value.protocolVersion);
  if (!isNonEmptyString(value.requestId)
      || value.requestId.length > 512
      || (expectedRequestId !== undefined && value.requestId !== expectedRequestId)
      || !isNonEmptyString(value.operationId)
      || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value.operationId)
      || (expectedOperationId !== undefined && value.operationId !== expectedOperationId)
      || value.event !== "progress") {
    throw new ContractValidationError("INVALID_INPUT", "Invalid Core progress envelope.");
  }
  assertProgressEvent(value.progress);
}

export function assertCoreOperationEventEnvelope(
  value: unknown,
  expectedRequestId?: string,
  expectedOperationId?: string
): asserts value is CoreOperationEventEnvelope {
  if (isRecord(value) && value.event === "operation-started") {
    assertCoreOperationStartedEnvelope(value, expectedRequestId, expectedOperationId);
    return;
  }
  assertCoreProgressEnvelope(value, expectedRequestId, expectedOperationId);
}

export function createCoreOperationStartedEnvelope(
  requestId: string,
  operationId: string,
  operation: CoreOperationStartedEnvelope["operation"]
): CoreOperationStartedEnvelope {
  const envelope: CoreOperationStartedEnvelope = {
    protocolVersion: CORE_PROTOCOL_VERSION,
    requestId,
    operationId,
    event: "operation-started",
    operation
  };
  assertCoreOperationStartedEnvelope(envelope);
  return envelope;
}

export function createCoreProgressEnvelope(
  requestId: string,
  operationId: string,
  progress: ProgressEvent
): CoreProgressEnvelope {
  const envelope: CoreProgressEnvelope = {
    protocolVersion: CORE_PROTOCOL_VERSION,
    requestId,
    operationId,
    event: "progress",
    progress
  };
  assertCoreProgressEnvelope(envelope);
  return envelope;
}

export function createCoreRequestEnvelope<M extends CoreMethodName>(
  method: M,
  payload: CoreMethodMap[M]["input"],
  requestId: string,
  operationId?: string
): CoreRequestEnvelope<M> {
  const envelope = {
    protocolVersion: CORE_PROTOCOL_VERSION,
    requestId,
    ...(operationId ? { operationId } : {}),
    method,
    payload
  } satisfies CoreRequestEnvelope<M>;
  assertCoreRequestEnvelope<M>(envelope);
  return envelope;
}

export function createCoreSuccessEnvelope<M extends CoreMethodName>(
  request: CoreRequestEnvelope<M>,
  result: CoreMethodMap[M]["output"],
  operationId?: string
): CoreResponseEnvelope<M> {
  assertCoreMethodOutput(request.method, result);
  return {
    protocolVersion: CORE_PROTOCOL_VERSION,
    requestId: request.requestId,
    ...(operationId ?? request.operationId
      ? { operationId: operationId ?? request.operationId }
      : {}),
    ok: true,
    result
  };
}

export function createCoreFailureEnvelope<M extends CoreMethodName>(
  request: CoreRequestEnvelope<M>,
  error: CoreErrorDto,
  operationId?: string
): CoreResponseEnvelope<M> {
  assertCoreErrorDto(error);
  return {
    protocolVersion: CORE_PROTOCOL_VERSION,
    requestId: request.requestId,
    ...(operationId ?? error.operationId ?? request.operationId
      ? { operationId: operationId ?? error.operationId ?? request.operationId }
      : {}),
    ok: false,
    error
  };
}

export function isCoreErrorCode(value: unknown): value is CoreErrorCode {
  return typeof value === "string" && ERROR_CODE_SET.has(value);
}

export function isCoreErrorSeverity(value: unknown): value is CoreErrorSeverity {
  return typeof value === "string" && SEVERITY_SET.has(value);
}
