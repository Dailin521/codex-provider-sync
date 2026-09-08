import type { HistorySessionSummary } from "@codex-provider-sync/contracts";
import { formatDate } from "../../shared/presentation.js";

type Translate = (key: string, options?: Record<string, unknown>) => string;

export function historyDisplayTitle(session: HistorySessionSummary, t: Translate, locale: string): string {
  if (session.title.trim()) return session.title;
  if (session.subagentName) return t("history.subagentTitle", { name: session.subagentName });
  // Creation time stays the same between lightweight lists and deep-read details.
  const date = formatDate(session.createdAt || session.updatedAt, locale);
  return t("history.untitledIdentity", { date, id: session.id.slice(-8) });
}
