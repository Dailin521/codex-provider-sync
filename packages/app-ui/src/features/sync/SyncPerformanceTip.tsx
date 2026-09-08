import { useTranslation } from "react-i18next";

// Presentation only: use actual Core counters, never scan files or infer a write strategy.
export function hasManyRewrittenSessions(operation: string, counts: unknown): boolean {
  if (!["sync", "switch"].includes(operation) || !counts || typeof counts !== "object" || Array.isArray(counts)) return false;
  const rewritten = (counts as Record<string, unknown>).rewrittenSessionFiles;
  return typeof rewritten === "number" && Number.isSafeInteger(rewritten) && rewritten >= 100;
}

export function SyncPerformanceTip({ afterOperation = false }: { afterOperation?: boolean }) {
  const { t } = useTranslation();
  return (
    <details className="rounded-lg border border-[var(--border)] px-3 py-2 text-sm">
      <summary className="cursor-pointer rounded py-1 font-medium text-[var(--accent-strong)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--focus)]">
        {t(afterOperation ? "sync.performance.resultLink" : "sync.performance.title")}
      </summary>
      <div className="grid gap-2 pb-1 pt-2 text-[var(--muted)]">
        <p>{t("sync.performance.equalLength")}</p>
        <p>{t("sync.performance.differentLength")}</p>
        <p>{t("sync.performance.configuration")}</p>
      </div>
    </details>
  );
}
