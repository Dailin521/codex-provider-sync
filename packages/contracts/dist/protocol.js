import { CORE_METHODS, CORE_PROTOCOL_VERSION } from "./dto.js";
import { CORE_ERROR_CODES, isCanonicalPublicCoreErrorDto } from "./errors.js";
import { isFileUpdateTiming } from "./file-update-timing.js";
const METHOD_SET = new Set(CORE_METHODS);
const ERROR_CODE_SET = new Set(CORE_ERROR_CODES);
const SEVERITY_SET = new Set(["info", "warning", "error", "fatal"]);
const REPAIR_TARGETS = ["models", "cwd", "userEvent", "workspaceRoots"];
const REPAIR_TARGET_SET = new Set(REPAIR_TARGETS);
function isRecord(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}
function isNonEmptyString(value) {
    return typeof value === "string" && value.length > 0;
}
function exactObjectKeys(value, allowed) {
    const allowedSet = new Set(allowed);
    return Object.keys(value).every((key) => allowedSet.has(key));
}
function assertProfileSelector(value) {
    if (!isRecord(value)
        || !exactObjectKeys(value, ["profileId", "profileRevision"])
        || typeof value.profileId !== "string"
        || !/^[A-Za-z0-9._-]{1,80}$/.test(value.profileId)
        || (value.profileRevision !== undefined
            && (!isNonEmptyString(value.profileRevision) || value.profileRevision.length > 512))) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid profile selector.");
    }
}
function assertProfileInput(value, allowed) {
    if (!isRecord(value)
        || !exactObjectKeys(value, ["profile", ...allowed])) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid Core method input.");
    }
    assertProfileSelector(value.profile);
}
export class ContractValidationError extends Error {
    code;
    constructor(code, message) {
        super(message);
        this.name = "ContractValidationError";
        this.code = code;
    }
}
export function assertProtocolVersion(value) {
    if (value !== CORE_PROTOCOL_VERSION) {
        throw new ContractValidationError("PROTOCOL_VERSION_MISMATCH", `Unsupported Core protocol version: ${String(value)}.`);
    }
}
export function assertApplyPlanInput(value) {
    if (!isRecord(value)
        || Object.keys(value).sort().join(",") !== "planId,schemaVersion"
        || value.schemaVersion !== 1
        || !isNonEmptyString(value.planId)) {
        throw new ContractValidationError("INVALID_INPUT", "Apply accepts exactly { schemaVersion: 1, planId }.");
    }
}
export function assertCoreMethodInput(method, value) {
    switch (method) {
        case "applySync":
        case "applySwitch":
        case "applyRepair":
        case "applyRestore":
            assertApplyPlanInput(value);
            return;
        case "getStatus":
        case "listBackups":
        case "getDiagnostics":
            assertProfileInput(value, []);
            return;
        case "prepareSync":
            assertProfileInput(value, ["keepCount"]);
            if (value.keepCount !== undefined
                && (!Number.isSafeInteger(value.keepCount) || Number(value.keepCount) < 1)) {
                throw new ContractValidationError("INVALID_INPUT", "Invalid Sync retention count.");
            }
            return;
        case "prepareSwitch":
            assertProfileInput(value, ["provider", "modelMode", "model", "keepCount"]);
            if (!isNonEmptyString(value.provider)
                || !["provider-default", "keep-root-model", "explicit"].includes(String(value.modelMode))
                || (value.modelMode === "explicit" && !isNonEmptyString(value.model))
                || (value.modelMode !== "explicit" && value.model !== undefined)
                || (value.keepCount !== undefined
                    && (!Number.isSafeInteger(value.keepCount) || Number(value.keepCount) < 1))) {
                throw new ContractValidationError("INVALID_INPUT", "Invalid Switch Provider input.");
            }
            return;
        case "prepareRepair": {
            assertProfileInput(value, ["targets", "keepCount", "sessionIds"]);
            const targets = Array.isArray(value.targets) ? value.targets : [];
            if (targets.length < 1
                || targets.length > REPAIR_TARGETS.length
                || targets.some((target) => typeof target !== "string" || !REPAIR_TARGET_SET.has(target))
                || new Set(targets).size !== targets.length
                || (value.sessionIds !== undefined && (!Array.isArray(value.sessionIds)
                    || value.sessionIds.length < 1 || value.sessionIds.length > 100
                    || value.sessionIds.some((id) => typeof id !== "string" || !/^[A-Za-z0-9_-]{1,128}$/.test(id))
                    || new Set(value.sessionIds).size !== value.sessionIds.length
                    || targets.includes("workspaceRoots")))
                || (value.keepCount !== undefined
                    && (!Number.isSafeInteger(value.keepCount) || Number(value.keepCount) < 1))) {
                throw new ContractValidationError("INVALID_INPUT", "Invalid Repair input.");
            }
            return;
        }
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
            assertProfileInput(value, ["page", "pageSize", "query", "project", "provider", "archived", "searchScope", "sessionKind", "view", "projectId", "parentId"]);
            if ((value.page !== undefined && (!Number.isSafeInteger(value.page) || Number(value.page) < 1))
                || (value.pageSize !== undefined
                    && (!Number.isSafeInteger(value.pageSize)
                        || Number(value.pageSize) < 10
                        || Number(value.pageSize) > 100))
                || ["query", "project", "provider", "projectId", "parentId"].some((key) => (value[key] !== undefined && typeof value[key] !== "string"))
                || (value.view !== undefined && !["flat", "projects"].includes(String(value.view)))
                || (value.projectId !== undefined && !(/^[a-f0-9]{64}$/.test(String(value.projectId)) || ["unassigned", "orphans"].includes(String(value.projectId))))
                || (value.parentId !== undefined && (!isNonEmptyString(value.parentId) || value.parentId.length > 512))
                || (value.searchScope !== undefined && !["metadata", "content"].includes(String(value.searchScope)))
                || (value.sessionKind !== undefined && !["all", "main", "subagent"].includes(String(value.sessionKind)))
                || (value.archived !== undefined
                    && !["all", "active", "archived"].includes(String(value.archived)))) {
                throw new ContractValidationError("INVALID_INPUT", "Invalid History list input.");
            }
            return;
        case "getHistorySession":
            assertProfileInput(value, ["sessionId", "messageLimit", "metadataOnly"]);
            if (!isNonEmptyString(value.sessionId)
                || (value.metadataOnly !== undefined && typeof value.metadataOnly !== "boolean")
                || (value.messageLimit !== undefined
                    && (!Number.isSafeInteger(value.messageLimit)
                        || Number(value.messageLimit) < 1
                        || Number(value.messageLimit) > 200))) {
                throw new ContractValidationError("INVALID_INPUT", "Invalid History detail input.");
            }
            return;
        case "startWatch":
            assertProfileInput(value, ["includeStateDb", "debounceMs", "once", "keepCount"]);
            if ((value.includeStateDb !== undefined && typeof value.includeStateDb !== "boolean")
                || (value.once !== undefined && typeof value.once !== "boolean")
                || (value.debounceMs !== undefined
                    && (!Number.isSafeInteger(value.debounceMs) || Number(value.debounceMs) < 0))
                || (value.keepCount !== undefined
                    && (!Number.isSafeInteger(value.keepCount) || Number(value.keepCount) < 1))) {
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
                || !exactObjectKeys(value, ["profile", "watchId"])
                || (value.watchId !== undefined && !isNonEmptyString(value.watchId))
                || (value.profile !== undefined && value.watchId !== undefined)) {
                throw new ContractValidationError("INVALID_INPUT", "Invalid Watch status input.");
            }
            if (value.profile !== undefined)
                assertProfileSelector(value.profile);
            return;
        default:
            throw new ContractValidationError("INVALID_INPUT", "Unknown Core method input.");
    }
}
export function assertCoreErrorDto(value) {
    if (!isCanonicalPublicCoreErrorDto(value)) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid public CoreErrorDto.");
    }
}
export function assertCoreRequestEnvelope(value) {
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
    assertCoreMethodInput(value.method, value.payload);
}
export function assertCoreResponseEnvelope(value, expectedRequestId) {
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
    }
    else {
        if (!("error" in value) || "result" in value) {
            throw new ContractValidationError("INVALID_INPUT", "Invalid failed Core response.");
        }
        assertCoreErrorDto(value.error);
    }
}
function requireSchemaObject(value, label) {
    if (!isRecord(value) || value.schemaVersion !== 1) {
        throw new ContractValidationError("INVALID_INPUT", `Invalid ${label}.`);
    }
    return value;
}
function requireStringArray(value, label) {
    if (!Array.isArray(value) || value.some((entry) => typeof entry !== "string")) {
        throw new ContractValidationError("INVALID_INPUT", `Invalid ${label}.`);
    }
}
function isNonNegativeInteger(value) {
    return Number.isSafeInteger(value) && Number(value) >= 0;
}
function isNullableString(value) {
    return value === null || typeof value === "string";
}
function isJsonValue(value, depth = 0) {
    if (depth > 16)
        return false;
    if (value === null || typeof value === "string" || typeof value === "boolean")
        return true;
    if (typeof value === "number")
        return Number.isFinite(value);
    if (Array.isArray(value))
        return value.every((entry) => isJsonValue(entry, depth + 1));
    if (!isRecord(value))
        return false;
    return Object.values(value).every((entry) => isJsonValue(entry, depth + 1));
}
function isProviderDistribution(value) {
    if (!isRecord(value))
        return false;
    return Object.values(value).every((counts) => (isRecord(counts) && Object.values(counts).every(isNonNegativeInteger)));
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
function isDiagnosticIdentifier(value) {
    return typeof value === "string"
        && /^[A-Za-z0-9._()-]{1,200}$/.test(value);
}
function isUuid(value) {
    return typeof value === "string"
        && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}
function isDiagnosticCountMap(value) {
    if (!isRecord(value) || Object.keys(value).length > 512)
        return false;
    return Object.entries(value).every(([provider, count]) => isDiagnosticIdentifier(provider) && isNonNegativeInteger(count));
}
function isDiagnosticDistribution(value, allowUnreadable = false) {
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
function isDiagnosticPendingTransaction(value) {
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
function isDiagnosticOperationState(value) {
    if (value === null)
        return true;
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
            || ["sync", "switch", "repair", "restore", "prune", "watch", "unknown"].includes(String(value.operation)))
        && (value.actor === undefined || ["manual", "watch", "external"].includes(String(value.actor)))
        && (value.startedAt === undefined
            || (isNonEmptyString(value.startedAt) && value.startedAt.length <= 64))
        && (value.busyScope === undefined || ["codex-home", "state-db"].includes(String(value.busyScope)))
        && (value.lockState === undefined
            || (isDiagnosticIdentifier(value.lockState) && value.lockState.length <= 80))
        && (value.errorCode === undefined
            || (typeof value.errorCode === "string" && /^[A-Z0-9_]{1,80}$/.test(value.errorCode)));
}
function isHistoryIntegrity(value) {
    if (!isRecord(value) || !exactObjectKeys(value, ["version", "outcome", "issuesTruncated", "counts", "skipped", "displayIndex", "issues", "limits"]))
        return false;
    const fields = [
        ["counts", ["filesDiscovered", "filesScanned", "recordsRead", "sessionsWithId", "jsonCorruptRecords", "oversizedRecords", "duplicateOrdinals", "outOfOrderOrdinals", "changedFiles", "truncatedFiles", "unsupportedFiles"]],
        ["skipped", ["symlinkOrReparse", "outOfRoot", "notRegular", "unreadable", "scanLimit"]],
        ["limits", ["maxFiles", "maxRecordsPerFile", "maxLineBytes", "maxIssues"]]
    ];
    const codes = ["json-corrupt", "record-too-large", "ordinal-duplicate-observed", "ordinal-out-of-order-observed", "record-limit-reached", "changed-during-scan", "unterminated-record", "unsupported-format", "invalid-utf8", "unverified"];
    return value.version === 1
        && ["no-findings", "findings", "inconclusive", "findings-and-inconclusive"].includes(String(value.outcome))
        && typeof value.issuesTruncated === "boolean"
        && fields.every(([section, keys]) => {
            const item = value[section];
            return isRecord(item) && exactObjectKeys(item, [...keys]) && keys.every((key) => isNonNegativeInteger(item[key]));
        })
        && isRecord(value.displayIndex)
        && exactObjectKeys(value.displayIndex, ["status", "reason"])
        && value.displayIndex.status === "unsupported" && value.displayIndex.reason === "no-known-display-index-schema"
        && Array.isArray(value.issues) && value.issues.length <= 100
        && value.issues.every((issue) => isRecord(issue)
            && exactObjectKeys(issue, ["code", "sessionId", "scope", "line"])
            && codes.includes(String(issue.code))
            && (issue.sessionId === null || (typeof issue.sessionId === "string" && /^[A-Za-z0-9_-]{1,128}$/.test(issue.sessionId)))
            && ["sessions", "archived_sessions"].includes(String(issue.scope))
            && (issue.line === null || (isNonNegativeInteger(issue.line) && Number(issue.line) > 0)));
}
function assertDiagnosticsSnapshot(value) {
    const diagnostics = requireSchemaObject(value, "DiagnosticsSnapshot");
    const runtime = isRecord(diagnostics.runtime) ? diagnostics.runtime : null;
    const storage = isRecord(diagnostics.storage) ? diagnostics.storage : null;
    const provider = isRecord(diagnostics.provider) ? diagnostics.provider : null;
    const issues = isRecord(diagnostics.issues) ? diagnostics.issues : null;
    const safety = isRecord(diagnostics.safety) ? diagnostics.safety : null;
    const valid = exactObjectKeys(diagnostics, [
        "schemaVersion",
        "generatedAt",
        "runtime",
        "storage",
        "provider",
        "issues",
        "safety",
        "historyIntegrity"
    ])
        && (diagnostics.historyIntegrity === undefined || isHistoryIntegrity(diagnostics.historyIntegrity))
        && isNonEmptyString(diagnostics.generatedAt)
        && diagnostics.generatedAt.length <= 64
        && runtime !== null
        && Object.keys(runtime).sort().join(",") === "arch,node,platform"
        && [runtime.node, runtime.platform, runtime.arch].every((entry) => typeof entry === "string" && /^[A-Za-z0-9._-]{1,80}$/.test(entry))
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
        && issues !== null
        && Object.keys(issues).sort().join(",")
            === "cwdRowsNeedingRepair,encryptedContentFiles,rolloutModelFilesNeedingRepair,rootModelAvailable,sqliteModelRowsNeedingRepair,userEventRowsNeedingRepair,workspaceRootsNeedingRepair"
        && typeof issues.rootModelAvailable === "boolean"
        && [
            issues.rolloutModelFilesNeedingRepair,
            issues.sqliteModelRowsNeedingRepair,
            issues.cwdRowsNeedingRepair,
            issues.userEventRowsNeedingRepair,
            issues.workspaceRootsNeedingRepair,
            issues.encryptedContentFiles
        ].every(isNonNegativeInteger)
        && safety !== null
        && exactObjectKeys(safety, [
            "storageRevision",
            "pendingRecovery",
            "pendingTransactions",
            "operationInProgress",
            "rolloutScanComplete",
            "lockedRolloutCount",
            "projectThreadVisibilityAvailable",
            ...(safety.staleLockDetected === undefined ? [] : ["staleLockDetected"])
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
        && typeof safety.projectThreadVisibilityAvailable === "boolean"
        && (safety.staleLockDetected === undefined || typeof safety.staleLockDetected === "boolean");
    if (!valid) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid DiagnosticsSnapshot.");
    }
}
function isHistorySummary(value) {
    if (!isRecord(value))
        return false;
    return isNonEmptyString(value.id)
        && typeof value.title === "string"
        && (value.project === undefined || value.project === null || (isRecord(value.project)
            && Object.keys(value.project).every((key) => ["id", "name"].includes(key))
            && typeof value.project.id === "string" && (/^[a-f0-9]{64}$/.test(value.project.id) || ["unassigned", "orphans"].includes(value.project.id))
            && isNonEmptyString(value.project.name) && value.project.name.length <= 160
            && !/[\x00-\x1f\x7f]/.test(value.project.name)))
        && (value.nativeSessionId === undefined || value.nativeSessionId === null || (isNonEmptyString(value.nativeSessionId) && value.nativeSessionId.length <= 512))
        && (value.parentSessionId === undefined || value.parentSessionId === null || (isNonEmptyString(value.parentSessionId) && value.parentSessionId.length <= 512))
        && (value.sessionKind === undefined || ["main", "subagent"].includes(String(value.sessionKind)))
        && (value.childCount === undefined || isNonNegativeInteger(value.childCount))
        && (value.fileModifiedAt === undefined || isNonEmptyString(value.fileModifiedAt))
        && (value.subagentName === undefined || (isNonEmptyString(value.subagentName)
            && value.subagentName.length <= 160 && !/[\\/\x00-\x1f]/.test(value.subagentName)))
        && !("cwd" in value)
        && isNonEmptyString(value.provider)
        && typeof value.archived === "boolean"
        && isNonEmptyString(value.updatedAt)
        && isNonNegativeInteger(value.messageCount)
        && (value.messageCountKnown === undefined || typeof value.messageCountKnown === "boolean")
        && (value.model === undefined || isNullableString(value.model))
        && (value.createdAt === undefined || isNonEmptyString(value.createdAt));
}
function assertWatchSnapshot(value) {
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
function isSessionActivity(value) {
    return isRecord(value) && Object.keys(value).sort().join(",") === "count,state"
        && (value.state === "checked" ? isNonNegativeInteger(value.count)
            : (value.state === "unavailable" || value.state === "unsupported") && value.count === null);
}
export function assertCoreMethodOutput(method, value) {
    switch (method) {
        case "getStatus": {
            const status = requireSchemaObject(value, "StatusSnapshot");
            const profile = isRecord(status.profile) ? status.profile : null;
            const displayPath = (value) => typeof value === "string"
                && value.length > 0 && value.length <= 32768 && !value.includes("\0")
                && /^(?:\/|[A-Za-z]:[\\/]|\\\\)/.test(value);
            const paths = status.displayPaths;
            const usage = status.syncSessionUsage;
            if (!isNonEmptyString(status.snapshotAt)
                || !isNonEmptyString(status.storageRevision)
                || !profile
                || !isNonEmptyString(profile.id)
                || !isNonEmptyString(profile.revision)
                || !isNonEmptyString(status.currentProvider)
                || (status.sessionActivity !== undefined && !isSessionActivity(status.sessionActivity))
                || (usage !== undefined && (!isRecord(usage)
                    || Object.keys(usage).sort().join(",") !== "count,state"
                    || !(usage.state === "checked" ? isNonNegativeInteger(usage.count)
                        : (usage.state === "unavailable" || usage.state === "unsupported") && usage.count === null)))
                || !isProviderDistribution(status.rolloutCounts)
                || (status.modelCounts !== undefined && !isProviderDistribution(status.modelCounts))
                || !("sqliteCounts" in status)
                || !isJsonValue(status.sqliteCounts)
                || "codexHome" in status
                || "sqliteHome" in status
                || (paths !== undefined && (!isRecord(paths)
                    || Object.keys(paths).sort().join(",") !== "codexHome,sqliteHome,stateDbPath"
                    || !displayPath(paths.codexHome) || !displayPath(paths.sqliteHome)
                    || !(paths.stateDbPath === null || displayPath(paths.stateDbPath))))
                || !isNonEmptyString(status.codexHomeSource)
                || !isNonEmptyString(status.sqliteHomeSource)
                || !isRecord(status.backupSummary)
                || !isNonNegativeInteger(status.backupSummary.count)
                || !isNonNegativeInteger(status.backupSummary.totalBytes)
                || typeof status.pendingRecovery !== "boolean"
                || (status.staleLockDetected !== undefined && typeof status.staleLockDetected !== "boolean")
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
        case "prepareRepair":
        case "prepareRestore": {
            const plan = requireSchemaObject(value, "PlanSummary");
            const expectedOperation = method === "prepareSync"
                ? "sync"
                : method === "prepareSwitch"
                    ? "switch"
                    : method === "prepareRepair"
                        ? "repair"
                        : "restore";
            const allowedKeys = [
                "schemaVersion",
                "planId",
                "operation",
                "createdAt",
                "expiresAt",
                "profile",
                "storageRevision",
                "configRevision",
                "rolloutRevision",
                "stateDbRevision",
                "target",
                "impact",
                "warnings",
                "requiresConfirmation",
                ...(plan.backupRevision === undefined ? [] : ["backupRevision"])
            ];
            if (!exactObjectKeys(plan, allowedKeys)
                || !isNonEmptyString(plan.planId)
                || plan.operation !== expectedOperation
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
                || (plan.impact.sessionActivity !== undefined && !isSessionActivity(plan.impact.sessionActivity))
                || !Array.isArray(plan.warnings)
                || plan.warnings.some((entry) => typeof entry !== "string")
                || typeof plan.requiresConfirmation !== "boolean") {
                throw new ContractValidationError("INVALID_INPUT", "Invalid PlanSummary.");
            }
            return;
        }
        case "applySync":
        case "applySwitch":
        case "applyRepair":
        case "applyRestore": {
            const result = requireSchemaObject(value, "OperationResult");
            const expectedOperation = method === "applySync"
                ? "sync"
                : method === "applySwitch"
                    ? "switch"
                    : method === "applyRepair"
                        ? "repair"
                        : "restore";
            if (!exactObjectKeys(result, [
                "schemaVersion",
                "operationId",
                "operation",
                "outcome",
                "backup",
                "warnings",
                "result"
            ])
                || !isNonEmptyString(result.operationId)
                || result.operation !== expectedOperation
                || !["completed", "partial", "failed_rolled_back", "recovery_required", "cancelled", "stale"].includes(String(result.outcome))
                || !(result.backup === null
                    || (isRecord(result.backup) && isNonEmptyString(result.backup.backupId)))) {
                throw new ContractValidationError("INVALID_INPUT", "Invalid OperationResult.");
            }
            requireStringArray(result.warnings, "OperationResult warnings");
            if (!("result" in result) || !isJsonValue(result.result)) {
                throw new ContractValidationError("INVALID_INPUT", "OperationResult result is required.");
            }
            if (isRecord(result.result) && result.result.fileUpdateTiming !== undefined && !isFileUpdateTiming(result.result.fileUpdateTiming)) {
                throw new ContractValidationError("INVALID_INPUT", "Invalid file update timing.");
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
                || value.sessions.some((entry) => !isHistorySummary(entry))
                || (value.view !== undefined && value.view !== "projects")
                || (value.projects !== undefined && (!Array.isArray(value.projects) || value.projects.some((entry) => !isRecord(entry)
                    || Object.keys(entry).sort().join(",") !== "id,kind,name,total"
                    || typeof entry.id !== "string" || !(/^[a-f0-9]{64}$/.test(entry.id) || ["unassigned", "orphans"].includes(entry.id))
                    || !isNonEmptyString(entry.name) || entry.name.length > 160 || !["workspace", "directory", "unassigned", "orphans"].includes(String(entry.kind))
                    || !isNonNegativeInteger(entry.total))))
                || (value.projectId !== undefined && !(value.projectId === null || typeof value.projectId === "string"))) {
                throw new ContractValidationError("INVALID_INPUT", "Invalid HistoryPage.");
            }
            return;
        }
        case "getHistorySession": {
            if (!isRecord(value)
                || !isHistorySummary(value.session)
                || (value.storage !== undefined && (!isRecord(value.storage)
                    || Object.keys(value.storage).some((key) => !["cwd", "rolloutPath"].includes(key))
                    || typeof value.storage.cwd !== "string" || value.storage.cwd.length > 32768
                    || !isNonEmptyString(value.storage.rolloutPath) || value.storage.rolloutPath.length > 32768
                    || /[\x00]/.test(value.storage.cwd + value.storage.rolloutPath)))
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
export function assertProgressEvent(value) {
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
export function assertCoreOperationStartedEnvelope(value, expectedRequestId, expectedOperationId) {
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
        || !["sync", "switch", "repair", "restore"].includes(String(value.operation))) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid operation-started envelope.");
    }
}
export function assertCoreProgressEnvelope(value, expectedRequestId, expectedOperationId) {
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
export function assertCoreRequestProgressEnvelope(value, expectedRequestId) {
    if (!isRecord(value)
        || !exactObjectKeys(value, ["protocolVersion", "requestId", "event", "progress"])) {
        throw new ContractValidationError("INVALID_INPUT", "Invalid Core request progress envelope.");
    }
    assertProtocolVersion(value.protocolVersion);
    if (!isNonEmptyString(value.requestId)
        || value.requestId.length > 512
        || (expectedRequestId !== undefined && value.requestId !== expectedRequestId)
        || value.event !== "request-progress") {
        throw new ContractValidationError("INVALID_INPUT", "Invalid Core request progress envelope.");
    }
    assertProgressEvent(value.progress);
}
export function assertCoreOperationEventEnvelope(value, expectedRequestId, expectedOperationId) {
    if (isRecord(value) && value.event === "operation-started") {
        assertCoreOperationStartedEnvelope(value, expectedRequestId, expectedOperationId);
        return;
    }
    assertCoreProgressEnvelope(value, expectedRequestId, expectedOperationId);
}
export function createCoreOperationStartedEnvelope(requestId, operationId, operation) {
    const envelope = {
        protocolVersion: CORE_PROTOCOL_VERSION,
        requestId,
        operationId,
        event: "operation-started",
        operation
    };
    assertCoreOperationStartedEnvelope(envelope);
    return envelope;
}
export function createCoreProgressEnvelope(requestId, operationId, progress) {
    const envelope = {
        protocolVersion: CORE_PROTOCOL_VERSION,
        requestId,
        operationId,
        event: "progress",
        progress
    };
    assertCoreProgressEnvelope(envelope);
    return envelope;
}
export function createCoreRequestProgressEnvelope(requestId, progress) {
    const envelope = {
        protocolVersion: CORE_PROTOCOL_VERSION,
        requestId,
        event: "request-progress",
        progress
    };
    assertCoreRequestProgressEnvelope(envelope);
    return envelope;
}
export function createCoreRequestEnvelope(method, payload, requestId, operationId) {
    const envelope = {
        protocolVersion: CORE_PROTOCOL_VERSION,
        requestId,
        ...(operationId ? { operationId } : {}),
        method,
        payload
    };
    assertCoreRequestEnvelope(envelope);
    return envelope;
}
export function createCoreSuccessEnvelope(request, result, operationId) {
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
export function createCoreFailureEnvelope(request, error, operationId) {
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
export function isCoreErrorCode(value) {
    return typeof value === "string" && ERROR_CODE_SET.has(value);
}
export function isCoreErrorSeverity(value) {
    return typeof value === "string" && SEVERITY_SET.has(value);
}
