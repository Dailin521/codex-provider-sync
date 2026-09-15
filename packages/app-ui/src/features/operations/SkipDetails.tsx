import { publicSkipSummary } from "@codex-provider-sync/contracts";
import { useTranslation } from "react-i18next";

export function SkipDetails({ value }: { value: unknown }) {
  const { t } = useTranslation();
  const summary = publicSkipSummary(value);
  if (!summary || (!summary.total && !summary.unconfirmed)) return null;
  return <section className="grid gap-2 rounded-lg border border-[var(--warning)] bg-[var(--warning-soft)] p-3 text-sm" aria-label={t("skips.title")}>
    <h3 className="font-semibold">{t("skips.title")}</h3>
    <p>{t("skips.counts", { total: summary.total, files: summary.rolloutFiles, rows: summary.sqliteRows, unknown: summary.unconfirmed })}</p>
    <p>{t(summary.retryRecommended ? "skips.retry" : "skips.fix")}</p>
    <details><summary className="cursor-pointer">{t("skips.details")}</summary>
      <ul className="mt-2 max-h-64 space-y-2 overflow-auto">
        {summary.items.map((item, index) => <li key={`${item.kind}-${item.path ?? item.id ?? index}`}>
          <span className="block select-text break-all font-mono text-xs">{item.path ?? item.id ?? t("skips.unidentified")}</span>
          <span>{t(`skips.reasons.${item.reason}`)} · {t(`skips.stages.${item.stage}`)}</span>
        </li>)}
      </ul>
    </details>
    <p>{t("skips.shown", { shown: summary.items.length, omitted: summary.omitted, total: summary.total })}</p>
  </section>;
}
