import { useState } from "react";
import { useTranslation } from "react-i18next";

import { useCopyText } from "../../shared/clipboard.js";
import { Badge, Button, Card } from "../../ui.js";

type HistoryIntegrity = {
  version: 1;
  outcome: "no-findings" | "findings" | "inconclusive" | "findings-and-inconclusive";
  counts: Record<string, number>;
  skipped: Record<string, number>;
  displayIndex: { status: "unsupported"; reason: "no-known-display-index-schema" };
  issues: Array<{ code: string; sessionId: string | null; scope: "sessions" | "archived_sessions"; line: number | null }>;
  issuesTruncated?: boolean;
  limits: Record<string, number>;
};

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function asHistoryIntegrity(value: unknown): HistoryIntegrity | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const candidate = value as Record<string, unknown>;
  if (candidate.version !== 1
      || !["no-findings", "findings", "inconclusive", "findings-and-inconclusive"].includes(String(candidate.outcome))
      || !candidate.counts || typeof candidate.counts !== "object" || Array.isArray(candidate.counts)
      || !candidate.skipped || typeof candidate.skipped !== "object" || Array.isArray(candidate.skipped)
      || !candidate.displayIndex || typeof candidate.displayIndex !== "object" || Array.isArray(candidate.displayIndex)
      || !Array.isArray(candidate.issues)
      || !candidate.limits || typeof candidate.limits !== "object" || Array.isArray(candidate.limits)) return null;
  if (!Object.values(candidate.counts).every(isNonNegativeInteger)
      || !Object.values(candidate.skipped).every(isNonNegativeInteger)
      || !Object.values(candidate.limits).every(isNonNegativeInteger)) return null;
  const index = candidate.displayIndex as Record<string, unknown>;
  if (index.status !== "unsupported" || index.reason !== "no-known-display-index-schema") return null;
  if (!candidate.issues.every((issue) => issue && typeof issue === "object" && !Array.isArray(issue)
      && typeof (issue as Record<string, unknown>).code === "string")) return null;
  return candidate as unknown as HistoryIntegrity;
}

export function HistoryIntegrityPanel({ historyIntegrity }: { historyIntegrity?: unknown }) {
  const { t } = useTranslation();
  const copyText = useCopyText();
  const [copiedSessionId, setCopiedSessionId] = useState<string | null>(null);
  const integrity = asHistoryIntegrity(historyIntegrity);
  if (!integrity) return null;
  const findings = integrity.issues.slice(0, 20);
  const hasMoreFindings = integrity.issuesTruncated === true || integrity.issues.length > findings.length;
  const tone = integrity.outcome === "no-findings" ? "success" : integrity.outcome === "findings" ? "warning" : "neutral";
  return (
    <Card className="mt-4 max-w-3xl">
      <div className="flex flex-wrap items-center justify-between gap-2"><h2 className="font-semibold">{t("diagnostics.historyIntegrity.title")}</h2><Badge tone={tone}>{t(`diagnostics.historyIntegrity.outcomes.${integrity.outcome}`)}</Badge></div>
      <p className="mt-2 text-sm text-[var(--muted)]">{t("diagnostics.historyIntegrity.scope")}</p>
      <p className="mt-2 rounded-[var(--radius-control)] border border-[var(--border)] bg-[var(--surface)] p-3 text-sm">{t("diagnostics.historyIntegrity.displayIndexUnsupported")}</p>
      <dl className="mt-3 grid gap-2 text-sm sm:grid-cols-2">
        {Object.entries(integrity.counts).map(([key, value]) => <div key={key}><dt className="text-[var(--muted)]">{t(`diagnostics.historyIntegrity.counts.${key}`, { defaultValue: key })}</dt><dd>{value}</dd></div>)}
      </dl>
      {Object.values(integrity.skipped).some((value) => value > 0) ? <p className="mt-3 text-sm text-[var(--warning)]">{t("diagnostics.historyIntegrity.skipped")}</p> : null}
      {findings.length ? <div className="mt-3"><h3 className="text-sm font-semibold">{t("diagnostics.historyIntegrity.findings")}</h3><ul className="mt-2 list-disc space-y-2 pl-5 text-sm">{findings.map((issue, index) => <li key={`${index}-${issue.code}`}><span>{t(`diagnostics.historyIntegrity.issueCodes.${issue.code}`, { defaultValue: t("diagnostics.historyIntegrity.manualReview") })}</span>{issue.sessionId ? <span className="ml-1 text-[var(--muted)]">· {t("diagnostics.historyIntegrity.session", { sessionId: issue.sessionId })}{issue.line ? ` · ${t("diagnostics.historyIntegrity.line", { line: issue.line })}` : ""}</span> : issue.line ? <span className="ml-1 text-[var(--muted)]">· {t("diagnostics.historyIntegrity.line", { line: issue.line })}</span> : null}{issue.sessionId ? <Button aria-label={t("diagnostics.historyIntegrity.copySessionId")} className="ml-2 align-middle" onClick={() => { void copyText(issue.sessionId!).then(() => setCopiedSessionId(issue.sessionId!)); }} size="compact" type="button" variant="ghost">{copiedSessionId === issue.sessionId ? t("diagnostics.historyIntegrity.copiedSessionId") : t("diagnostics.historyIntegrity.copySessionId")}</Button> : null}</li>)}</ul>{hasMoreFindings ? <p className="mt-2 text-sm text-[var(--muted)]">{t("diagnostics.historyIntegrity.moreFindings")}</p> : null}</div> : null}
      <details className="mt-3 rounded-[var(--radius-control)] border border-[var(--border)] bg-[var(--surface)] p-3 text-sm"><summary className="cursor-pointer font-medium">{t("diagnostics.technicalDetails")}</summary><pre className="mt-3 max-h-72 overflow-auto whitespace-pre-wrap break-words text-xs leading-5 text-[var(--muted)]">{JSON.stringify(integrity, null, 2)}</pre></details>
    </Card>
  );
}
