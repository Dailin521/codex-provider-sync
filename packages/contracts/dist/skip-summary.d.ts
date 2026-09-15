/** Local operation detail. Diagnostic exports must use redactSkipSummary. */
export declare const SKIP_REASONS: readonly ["metadata-invalid", "metadata-invalid-utf8", "metadata-too-complex", "metadata-too-large", "locked", "unreadable", "missing", "changed", "write-not-applied", "association-unknown", "association-conflict", "row-changed", "row-missing", "deferred"];
export declare const SKIP_STAGES: readonly ["scan", "plan", "revalidate", "write", "sqlite"];
export interface SkipItem {
    kind: "rollout" | "sqlite";
    path?: string;
    id?: string;
    reason: typeof SKIP_REASONS[number];
    stage: typeof SKIP_STAGES[number];
    retryable: boolean;
}
export interface SkipSummary {
    total: number;
    rolloutFiles: number;
    sqliteRows: number;
    unconfirmed: number;
    omitted: number;
    retryRecommended: boolean;
    items: SkipItem[];
}
export declare function isSkipSummary(value: unknown): value is SkipSummary;
export declare function publicSkipSummary(value: unknown): SkipSummary | undefined;
/** Preserve counts/reasons while removing every local file path and row ID. */
export declare function redactSkipSummary(value: unknown): SkipSummary | undefined;
