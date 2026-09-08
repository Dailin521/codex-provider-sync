import type { HistoryPage, HistorySessionSummary } from "@codex-provider-sync/contracts";

/** Single-collection fixture for tests unrelated to project classification. */
export function historyProjectPage(sessions: HistorySessionSummary[], overrides: Partial<HistoryPage> = {}): HistoryPage {
  return { view: "projects", projectId: "unassigned", projects: [{ id: "unassigned", name: "Unassigned", kind: "unassigned", total: sessions.length }], page: 1, pageSize: 10, total: sessions.length, hasNextPage: false, sessions, ...overrides };
}
