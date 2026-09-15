/** Local operation detail. Diagnostic exports must use redactSkipSummary. */
export const SKIP_REASONS = ["metadata-invalid", "metadata-invalid-utf8", "metadata-too-complex", "metadata-too-large", "locked", "unreadable", "missing", "changed", "write-not-applied", "association-unknown", "association-conflict", "row-changed", "row-missing", "deferred"];
export const SKIP_STAGES = ["scan", "plan", "revalidate", "write", "sqlite"];
const record = (value) => !!value && typeof value === "object" && !Array.isArray(value);
const count = (value) => Number.isSafeInteger(value) && Number(value) >= 0;
const fields = new Set(["total", "rolloutFiles", "sqliteRows", "unconfirmed", "omitted", "retryRecommended", "items"]);
const itemFields = new Set(["kind", "path", "id", "reason", "stage", "retryable"]);
export function isSkipSummary(value) {
    if (!record(value) || Object.keys(value).some(key => !fields.has(key))
        || !["total", "rolloutFiles", "sqliteRows", "unconfirmed", "omitted"].every(key => count(value[key]))
        || typeof value.retryRecommended !== "boolean" || !Array.isArray(value.items) || value.items.length > 200
        || Number(value.total) !== Number(value.rolloutFiles) + Number(value.sqliteRows)
        || Number(value.total) !== value.items.length + Number(value.omitted))
        return false;
    if (new TextEncoder().encode(JSON.stringify(value)).length > 1024 * 1024)
        return false;
    return value.items.every(item => record(item) && Object.keys(item).every(key => itemFields.has(key))
        && ["rollout", "sqlite"].includes(String(item.kind))
        && SKIP_REASONS.includes(item.reason)
        && SKIP_STAGES.includes(item.stage)
        && typeof item.retryable === "boolean"
        && (item.path === undefined || (item.kind === "rollout" && typeof item.path === "string" && item.path.length > 0 && item.path.length <= 32768 && !/[\u0000-\u001f]/.test(item.path)))
        && (item.id === undefined || (item.kind === "sqlite" && typeof item.id === "string" && /^[A-Za-z0-9_.:-]{1,128}$/.test(item.id))));
}
export function publicSkipSummary(value) {
    return isSkipSummary(value) ? { ...value, items: value.items.map(item => ({ ...item })) } : undefined;
}
/** Preserve counts/reasons while removing every local file path and row ID. */
export function redactSkipSummary(value) {
    const summary = publicSkipSummary(value);
    return summary ? { ...summary, items: summary.items.map(({ kind, reason, stage, retryable }) => ({ kind, reason, stage, retryable })) } : undefined;
}
