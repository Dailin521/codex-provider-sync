import type { FileUpdateTiming } from "@codex-provider-sync/contracts";
import { useTranslation } from "react-i18next";

const MAIN_PHASES = ["copyTailMs", "flushMs", "replaceMs", "cleanupMs", "restoreMtimeMs"] as const;
const TECHNICAL_PHASES = ["workerStartupMs", "workerCloseMs", "requestRoundTripMs", "workerMs", "sourceOpenMs", "readHeaderMs", "tempCreateMs"] as const;

export function FileUpdateTimingDetails({ timing, pending }: { timing?: FileUpdateTiming; pending: boolean }) {
  const { t } = useTranslation();
  if (!timing) return <p className="text-sm text-[var(--muted)]">{t(pending ? "logs.fileTiming.pending" : "logs.fileTiming.unavailable")}</p>;
  const duration = (value: number) => t(value < 1000 ? "logs.fileTiming.milliseconds" : "logs.seconds", { value: (value < 1000 ? value : value / 1000).toFixed(value < 1000 ? 1 : 3) });
  const rows = (fields: readonly (typeof MAIN_PHASES[number] | typeof TECHNICAL_PHASES[number])[]) => (
    <dl className="mt-2 grid gap-2 text-sm">{fields.map((field) => <div className="flex items-baseline justify-between gap-3" key={field}><dt>{t(`logs.fileTiming.phases.${field}`)}</dt><dd className="shrink-0 tabular-nums">{duration(timing[field])}</dd></div>)}</dl>
  );
  return <section aria-label={t("logs.fileTiming.title")} className="rounded-lg border border-[var(--border)] p-3">
    <div className="flex flex-wrap items-baseline justify-between gap-2"><h3 className="font-semibold">{t("logs.fileTiming.title")}</h3><span className="text-sm tabular-nums">{duration(timing.totalMs)}</span></div>
    <p className="mt-2 text-sm text-[var(--muted)]">{t("logs.fileTiming.files", { measured: timing.measuredFiles, attempted: timing.attemptedFiles, inPlace: timing.inPlaceFiles, rewritten: timing.rewrittenFiles, skipped: timing.skippedFiles })}</p>
    {timing.measuredFiles < timing.attemptedFiles ? <p className="mt-2 text-sm text-[var(--warning)]">{t("logs.fileTiming.incomplete")}</p> : null}
    {rows(MAIN_PHASES)}
    <details className="mt-3"><summary className="cursor-pointer text-sm focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--focus)]">{t("logs.fileTiming.more")}</summary>{rows(TECHNICAL_PHASES)}<p className="mt-2 text-xs text-[var(--muted)]">{t("logs.fileTiming.nested")}</p></details>
  </section>;
}
