import type { StatusSnapshot } from "@codex-provider-sync/contracts";
import { AlertTriangle, CheckCircle2, RefreshCw } from "lucide-react";
import { Fragment } from "react";
import { useTranslation } from "react-i18next";

import { formatBytes, formatDate, KeyValue, PageHeading } from "../../shared/presentation.js";
import { hasStatusSnapshot, statusAlignment } from "../../shared/status-feedback.js";
import type { SyncValues } from "../sync/SyncPage.js";
import { SyncPage } from "../sync/SyncPage.js";
import type { SwitchValues } from "../switch-provider/SwitchPage.js";
import { SwitchPage } from "../switch-provider/SwitchPage.js";
import { Badge, Button, Card, cn } from "../../ui.js";
import { DEFAULT_BACKUP_RETENTION_COUNT } from "../../schemas.js";

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
    <Card className="min-w-0 p-4">
      <div className="mb-3 flex items-center justify-between gap-2">
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

function sourceLabel(source: string | undefined, profileConfigured: boolean, t: (key: string, options?: Record<string, unknown>) => string): string {
  if (profileConfigured) return t("overview.sources.profile");
  if (source === "profile" || source === "cli") return t("overview.sources.explicit");
  if (source === "config" || source === "env" || source === "default") return t(`overview.sources.${source}`);
  return t("overview.sources.unknown");
}

export function OverviewPage({ status, loading, refresh, profileName, profileKey, providers, recentSuccessfulProviders, sqliteHomeConfigured, writeDisabled, prepareSync, directSync, prepareSwitch, manageStorage, retentionCount = DEFAULT_BACKUP_RETENTION_COUNT }: {
  retentionCount?: number;
  status?: StatusSnapshot;
  loading: boolean;
  refresh(): void;
  profileName: string;
  profileKey?: string;
  providers: string[];
  recentSuccessfulProviders?: string[];
  sqliteHomeConfigured: boolean;
  writeDisabled: boolean;
  prepareSync(values: SyncValues, trigger: HTMLButtonElement | null): Promise<void>;
  directSync?(values: SyncValues, trigger: HTMLButtonElement | null): Promise<void>;
  prepareSwitch(values: SwitchValues, trigger: HTMLButtonElement | null): Promise<void>;
  manageStorage(): void;
}) {
  const { t, i18n } = useTranslation();
  const hasSnapshot = hasStatusSnapshot(status) && !status.statusReadBlocked;
  const sessionUsageKnown = hasSnapshot && !status.statusReadBlocked && !status.operationInProgress
    && status.sessionActivity?.state === "checked";
  const alignment = statusAlignment(status);
  return (
    <Fragment>
      <PageHeading
        title={t("overview.title")}
        subtitle={t("overview.subtitle")}
        action={<Button disabled={loading} onClick={refresh} type="button" variant="secondary"><RefreshCw className={cn(loading && "animate-spin")} size={16} />{t("common.refresh")}</Button>}
      />
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Card className="min-w-0 p-4"><div className="text-sm text-[var(--muted)]">{t("common.provider")}</div><div className="mt-1 break-words text-xl font-bold">{hasSnapshot ? status.currentProvider : "—"}</div></Card>
        <Card className="min-w-0 p-4"><div className="text-sm text-[var(--muted)]">{t("overview.alignment")}</div><div className="mt-1 flex items-center gap-2 text-lg font-bold">{alignment === "aligned" ? <CheckCircle2 className="shrink-0 text-[var(--success)]" size={20} /> : alignment === "notAligned" ? <AlertTriangle className="shrink-0 text-[var(--warning)]" size={20} /> : null}{alignment === "unknown" ? t(loading && !hasSnapshot ? "ux.reading" : "ux.unknown") : t(`overview.${alignment}`)}</div></Card>
        <Card className="min-w-0 p-4"><div className="text-sm text-[var(--muted)]">{t("overview.backupCount")}</div><div className="mt-1 text-xl font-bold">{hasSnapshot ? status.backupSummary.count : "—"}</div>{hasSnapshot ? <div className="text-xs text-[var(--muted)]">{formatBytes(status.backupSummary.totalBytes)}</div> : null}</Card>
        <Card className="min-w-0 p-4">
          <div className="text-sm text-[var(--muted)]">{t("overview.locked")}</div>
          <div className="mt-1 text-xl font-bold">{sessionUsageKnown ? status.sessionActivity?.count : t("overview.usageUnknown")}</div>
        </Card>
      </div>
      {loading && hasSnapshot ? <p className="mt-2 text-xs text-[var(--muted)]" role="status">{t("ux.previousSnapshot")}</p> : null}
      {hasSnapshot ? <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <Distribution counts={status.rolloutCounts} current={status.currentProvider} title={t("overview.rollout")} />
        <Distribution counts={status.sqliteCounts} current={status.currentProvider} title={t("overview.sqlite")} />
      </div> : null}
      <section aria-labelledby="provider-operations" className="mt-4">
        <h2 className="sr-only" id="provider-operations">{t("overview.operations")}</h2>
        <div className="grid items-start gap-4 lg:grid-cols-2" data-testid="overview-storage-sync">
          <Card className="min-w-0 p-4">
            <dl className="[&>div]:py-2 sm:[&>div]:grid-cols-[140px_minmax(0,1fr)]">
              <KeyValue label={t("overview.profile")} value={profileName} />
              <KeyValue label={t("overview.codexHomeSource")} value={status?.displayPaths
                ? <span className="select-text break-all">{status.displayPaths.codexHome}</span>
                : sourceLabel(status?.codexHomeSource, true, t)} />
              <KeyValue label={t("overview.sqliteHomeSource")} value={status?.displayPaths
                ? <span className="select-text break-all">{status.displayPaths.sqliteHome}</span>
                : sourceLabel(status?.sqliteHomeSource, sqliteHomeConfigured, t)} />
              {status?.displayPaths ? <KeyValue label={t("overview.stateDbPath")} value={status.displayPaths.stateDbPath
                ? <span className="select-text break-all">{status.displayPaths.stateDbPath}</span>
                : t("overview.stateDbMissing")} /> : null}
              <KeyValue label={t("overview.snapshot")} value={formatDate(status?.snapshotAt, i18n.language)} />
            </dl>
            <Button className="mt-4" onClick={manageStorage} type="button" variant="secondary">{t("overview.manageStorage")}</Button>
          </Card>
          <div className="min-w-0">
            <SyncPage directSync={directSync} disabled={writeDisabled} embedded prepare={prepareSync} />
            <p className="mt-3 text-sm text-[var(--muted)]">{t("backupPolicy.operationHint", { count: retentionCount })}</p>
          </div>
        </div>
        <div className="mt-4"><SwitchPage profileKey={profileKey} currentProvider={hasSnapshot ? status.currentProvider : undefined} disabled={writeDisabled} embedded prepare={prepareSwitch} providers={providers} recentSuccessfulProviders={recentSuccessfulProviders} /></div>
      </section>
    </Fragment>
  );
}
