// Explicit public projection. No path, source record, title or message text is
// allowed through Diagnostics, CLI JSON or diagnostic exports.
const COUNTS = ["filesDiscovered", "filesScanned", "recordsRead", "sessionsWithId", "jsonCorruptRecords", "oversizedRecords", "duplicateOrdinals", "outOfOrderOrdinals", "changedFiles", "truncatedFiles", "unsupportedFiles"];
const SKIPPED = ["symlinkOrReparse", "outOfRoot", "notRegular", "unreadable", "scanLimit"];
const LIMITS = ["maxFiles", "maxRecordsPerFile", "maxLineBytes", "maxIssues"];
const OUTCOMES = ["no-findings", "findings", "inconclusive", "findings-and-inconclusive"];
const ISSUE_CODES = ["json-corrupt", "record-too-large", "ordinal-duplicate-observed", "ordinal-out-of-order-observed", "record-limit-reached", "changed-during-scan", "unterminated-record", "unsupported-format", "invalid-utf8", "unverified"];
const record = (value) => value && typeof value === "object" && !Array.isArray(value);
const number = (value) => Number.isSafeInteger(value) && value >= 0 ? value : 0;
const counts = (source, keys) => Object.fromEntries(keys.map((key) => [key, number(source?.[key])]));

export function publicHistoryIntegrity(value) {
  if (!record(value)) return undefined;
  return {
    version: 1,
    outcome: OUTCOMES.includes(value.outcome) ? value.outcome : "inconclusive",
    issuesTruncated: value.issuesTruncated === true || (Array.isArray(value.issues) && value.issues.length > 100),
    counts: counts(value.counts, COUNTS),
    skipped: counts(value.skipped, SKIPPED),
    displayIndex: { status: "unsupported", reason: "no-known-display-index-schema" },
    issues: (Array.isArray(value.issues) ? value.issues : []).filter(record).slice(0, 100).map((issue) => ({
      code: ISSUE_CODES.includes(issue.code) ? issue.code : "unverified",
      sessionId: typeof issue.sessionId === "string" && /^[A-Za-z0-9_-]{1,128}$/.test(issue.sessionId) ? issue.sessionId : null,
      scope: issue.scope === "archived_sessions" ? "archived_sessions" : "sessions",
      line: Number.isSafeInteger(issue.line) && issue.line > 0 ? issue.line : null
    })),
    limits: counts(value.limits, LIMITS)
  };
}
