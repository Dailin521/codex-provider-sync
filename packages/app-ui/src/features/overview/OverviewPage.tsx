import type { StatusSnapshot } from "@codex-provider-sync/contracts";
import { AlertTriangle, CheckCircle2, RefreshCw } from "lucide-react";
import { Fragment } from "react";
import { useTranslation } from "react-i18next";

import { formatBytes, formatDate, KeyValue, PageHeading } from "../../shared/presentation.js";
import { Badge, Button, Card, cn } from "../../ui.js";

function Distribution({ title, counts, current }: { title: string; counts: unknown; current: string }) {
  const record = counts && typeof counts === "object" && !Array.isArray(counts)
    ? counts as Record<string, unknown>
    : {};
  const merged = new Map<string, number>();
  for (const scope of ["sessions", "archived_sessions"]) {
    const distribution = record[scope];
    if (!distribution || typeof distribution !== "object" || Array.isArray(distribution)) continue;
    for (const [provider, count] of Object.entries(distribution as Record<string, unknown>)) {
      if (typeof count === "number") merged.set(provider, (merged.get(provider) ?? 0) + count);
    }
  }
  const entries = [...merged.entries()].sort((left, right) => right[1] - left[1]);
  const total = entries.reduce((sum, [, count]) => sum + count, 0);
  return (
    <Card>
      <div className="mb-4 flex items-center justify-between">
        <h2 className="font-semibold">{title}</h2><Badge>{total}</Badge>
      </div>
      <div className="grid gap-3">
        {entries.length === 0 ? <span className="text-sm text-[var(--muted)]">—</span> : entries.map(([provider, count]) => (
          <div key={provider}>
            <div className="mb-1 flex justify-between text-sm"><span className="font-medium">{provider}</span><span>{count}</span></div>
            <progress
              aria-label={`${provider}: ${count}`}
              className={cn("h-2 w-full overflow-hidden rounded-full", provider === current ? "accent-[var(--accent)]" : "accent-[var(--muted)]")}
              max={Math.max(total, 1)}
              value={count}
            />
          </div>
        ))}
      </div>
    </Card>
  );
}

export function OverviewPage({ status, loading, refresh }: {
  status?: StatusSnapshot;
  loading: boolean;
  refresh(): void;
}) {
  const { t, i18n } = useTranslation();
  const alignment = status?.alignment && typeof status.alignment === "object"
    ? (status.alignment as Record<string, unknown>).aligned === true
    : false;
  return (
    <Fragment>
      <PageHeading
        title={t("overview.title")}
        subtitle={t("overview.subtitle")}
        action={<Button disabled={loading} onClick={refresh} type="button" variant="secondary"><RefreshCw className={cn(loading && "animate-spin")} size={16} />{t("common.refresh")}</Button>}
      />
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <Card><div className="text-sm text-[var(--muted)]">{t("common.provider")}</div><div className="mt-2 text-xl font-bold">{status?.currentProvider ?? "—"}</div></Card>
        <Card><div className="text-sm text-[var(--muted)]">{t("overview.alignment")}</div><div className="mt-2 flex items-center gap-2 text-lg font-bold">{alignment ? <CheckCircle2 className="text-[var(--success)]" size={20} /> : <AlertTriangle className="text-[var(--warning)]" size={20} />}{alignment ? t("overview.aligned") : t("overview.notAligned")}</div></Card>
        <Card><div className="text-sm text-[var(--muted)]">{t("overview.backupCount")}</div><div className="mt-2 text-xl font-bold">{status?.backupSummary.count ?? 0}</div><div className="text-xs text-[var(--muted)]">{formatBytes(status?.backupSummary.totalBytes ?? 0)}</div></Card>
        <Card><div className="text-sm text-[var(--muted)]">{t("overview.locked")}</div><div className="mt-2 text-xl font-bold">{status?.lockedRolloutFiles.length ?? 0}</div></Card>
      </div>
      <Card className="mt-4"><dl><KeyValue label={t("overview.codexHomeSource")} value={status?.codexHomeSource ?? "—"} /><KeyValue label={t("overview.sqliteHomeSource")} value={status?.sqliteHomeSource ?? "—"} /><KeyValue label={t("overview.snapshot")} value={formatDate(status?.snapshotAt, i18n.language)} /></dl></Card>
      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <Distribution counts={status?.rolloutCounts} current={status?.currentProvider ?? ""} title={t("overview.rollout")} />
        <Distribution counts={status?.sqliteCounts} current={status?.currentProvider ?? ""} title={t("overview.sqlite")} />
      </div>
    </Fragment>
  );
}
