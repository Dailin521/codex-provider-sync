import { useQuery } from "@tanstack/react-query";
import { ArrowLeft, RefreshCw } from "lucide-react";
import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { displayProfileName, displayWarningText, formatDate, PageHeading, safeErrorText } from "../../shared/presentation.js";
import type { HostClient, OperationLogEntry, OperationLogStatus } from "../../types.js";
import { Badge, Button, Card, cn } from "../../ui.js";
import { useCopyText } from "../../shared/clipboard.js";
import { operationFailureLabel, operationStageLabel } from "../../shared/operation-feedback.js";
import { hasManyRewrittenSessions, SyncPerformanceTip } from "../sync/SyncPerformanceTip.js";
import { FileUpdateTimingDetails } from "./FileUpdateTimingDetails.js";

const PAGE_SIZE = 50;
const OPERATIONS = ["sync", "switch", "repair", "restore", "pruneBackups", "diagnostics", "watch", "update", "profile", "runtime"] as const;
const STATUSES: OperationLogStatus[] = ["running", "awaiting-confirmation", "completed", "partial", "failed", "cancelled", "dismissed", "interrupted"];
const KNOWN_COUNTS = new Set([
  "changedSessionFiles", "inPlaceSessionFiles", "rewrittenSessionFiles", "sqliteRowsUpdated",
  "sqliteProviderRowsUpdated", "sqliteModelRowsUpdated", "sqliteUserEventRowsUpdated",
  "sqliteCwdRowsUpdated", "skippedLockedRolloutFiles", "skippedChangedRolloutFiles",
  "updatedWorkspaceRoots", "savedWorkspaceRootCount", "resolvedOperationCount"
]);

type Translate = (key: string, options?: Record<string, unknown>) => string;

function duration(value: number | undefined, t: Translate): string {
  if (value === undefined) return "—";
  if (value < 1000) return t("logs.lessThanSecond");
  if (value < 60_000) return t("logs.seconds", { value: (value / 1000).toFixed(1) });
  return t("logs.minutesSeconds", {
    minutes: Math.floor(value / 60_000),
    seconds: String(Math.floor((value % 60_000) / 1000)).padStart(2, "0")
  });
}

function tone(status: OperationLogStatus): "success" | "warning" | "danger" | "neutral" {
  if (status === "completed") return "success";
  if (status === "partial" || status === "cancelled" || status === "dismissed") return "warning";
  if (status === "failed" || status === "interrupted") return "danger";
  return "neutral";
}

function operationLabel(operation: string, t: Translate): string {
  return OPERATIONS.includes(operation as typeof OPERATIONS[number])
    ? t(`logs.operations.${operation}`)
    : t("logs.otherOperation");
}

function CorrelationId({ label, value }: { label: string; value?: string }) {
  const { t } = useTranslation();
  const copyText = useCopyText();
  const [notice, setNotice] = useState("");
  if (!value) return null;
  const copy = async () => {
    try { await copyText(value); setNotice(t("common.copied")); }
    catch { setNotice(t("history.copyFailed")); }
  };
  return (
    <div>
      <dt className="text-[var(--muted)]">{label}</dt>
      <dd className="flex min-w-0 items-center gap-2">
        <span className="min-w-0 break-all font-mono text-xs">{value}</span>
        <Button aria-label={`${t("common.copy")} ${label}`} onClick={() => void copy()} type="button" variant="ghost">{t("common.copy")}</Button>
        <span aria-live="polite" className="text-xs">{notice}</span>
      </dd>
    </div>
  );
}

export function OperationLogsPage({ host, profileId, profileRevision, openBackupRestore, reviewOperation }: {
  host: HostClient;
  profileId: string;
  profileRevision?: string;
  openBackupRestore?(backupId: string): void;
  reviewOperation?(operation: string): void;
}) {
  const { t, i18n } = useTranslation();
  const [page, setPage] = useState(1);
  const [operation, setOperation] = useState("");
  const [status, setStatus] = useState<OperationLogStatus | "">("");
  const [profileFilter, setProfileFilter] = useState(profileId);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const listScroll = useRef<HTMLDivElement>(null);
  const detailScroll = useRef<HTMLDivElement>(null);
  const selectedButton = useRef<HTMLButtonElement | null>(null);
  const profiles = useQuery({ queryKey: ["profiles"], queryFn: ({ signal }) => host.listProfiles(signal), retry: false, staleTime: Infinity, refetchOnWindowFocus: false, refetchOnReconnect: false });
  const list = useQuery({
    queryKey: ["operation-logs", page, profileFilter, operation, status],
    queryFn: ({ signal }) => host.listOperationLogs!({ page, pageSize: PAGE_SIZE, ...(profileFilter ? { profileId: profileFilter } : {}), ...(operation ? { operation } : {}), ...(status ? { status } : {}) }, signal),
    enabled: Boolean(host.listOperationLogs), retry: false, staleTime: Infinity, refetchOnWindowFocus: false, refetchOnReconnect: false
  });
  const detail = useQuery({
    queryKey: ["operation-log", selectedId],
    queryFn: ({ signal }) => host.getOperationLog!(selectedId!, signal),
    enabled: Boolean(selectedId && host.getOperationLog), retry: false, staleTime: Infinity, refetchOnWindowFocus: false, refetchOnReconnect: false
  });
  useEffect(() => { setProfileFilter(profileId); setPage(1); }, [profileId]);
  useEffect(() => { setSelectedId(null); if (listScroll.current) listScroll.current.scrollTop = 0; }, [page, profileFilter, operation, status, profileId, profileRevision]);
  useEffect(() => {
    if (selectedId && !list.isFetching && list.isSuccess && !list.data.entries.some((entry) => entry.id === selectedId)) setSelectedId(null);
  }, [list.data, list.isFetching, list.isSuccess, selectedId]);
  useLayoutEffect(() => {
    if (!selectedId) {
      if (detailScroll.current?.parentElement?.contains(document.activeElement)) {
        globalThis.requestAnimationFrame(() => {
          const target = selectedButton.current?.isConnected ? selectedButton.current : listScroll.current;
          target?.focus({ preventScroll: true });
        });
      }
      return;
    }
    if (!detailScroll.current) return;
    detailScroll.current.scrollTop = 0;
    if (!globalThis.matchMedia?.("(min-width: 1024px)").matches) detailScroll.current.focus({ preventScroll: true });
  }, [selectedId]);
  const backToList = () => {
    setSelectedId(null);
    globalThis.requestAnimationFrame(() => (selectedButton.current?.isConnected ? selectedButton.current : listScroll.current)?.focus({ preventScroll: true }));
  };
  const selected: OperationLogEntry | null = detail.data === undefined
    ? list.data?.entries.find((entry) => entry.id === selectedId) ?? null
    : detail.data?.id === selectedId ? detail.data : null;
  const counts = selected ? Object.entries(selected.counts ?? {}).filter(([name]) => KNOWN_COUNTS.has(name)) : [];
  const previewCounts = selected?.previewCounts ? [
    ["rolloutFilesToChange", selected.previewCounts.rolloutFilesToChange],
    ["sqliteRowsToChange", selected.previewCounts.sqliteRowsToChange],
    ["lockedRolloutFiles", selected.previewCounts.lockedRolloutFiles]
  ] as const : [];

  return (
    <section className="flex min-h-0 flex-1 flex-col overflow-hidden" data-testid="operation-logs-workspace">
      <div className={cn("max-h-[45%] shrink-0 overflow-y-auto overscroll-contain", selectedId && "hidden lg:block")}>
      <PageHeading title={t("logs.title")} subtitle={t("logs.subtitle")} action={<Button disabled={list.isFetching || detail.isFetching} onClick={() => { void list.refetch(); if (selectedId) void detail.refetch(); }} type="button" variant="secondary"><RefreshCw className={cn((list.isFetching || detail.isFetching) && "animate-spin")} size={16} />{t("common.refresh")}</Button>} />
      <div className="mb-3 flex flex-wrap gap-2 text-sm [&>select]:max-w-full">
        <select aria-label={t("logs.profileFilter")} className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" onChange={(event) => { setProfileFilter(event.target.value); setPage(1); setSelectedId(null); }} value={profileFilter}><option value="">{t("logs.allProfiles")}</option>{profiles.data?.map((profile) => <option key={profile.id} value={profile.id}>{displayProfileName(profile, t)}</option>)}</select>
        <select aria-label={t("logs.operationFilter")} className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" onChange={(event) => { setOperation(event.target.value); setPage(1); setSelectedId(null); }} value={operation}><option value="">{t("logs.allOperations")}</option>{OPERATIONS.map((value) => <option key={value} value={value}>{operationLabel(value, t)}</option>)}</select>
        <select aria-label={t("logs.statusFilter")} className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" onChange={(event) => { setStatus(event.target.value as typeof status); setPage(1); setSelectedId(null); }} value={status}><option value="">{t("logs.allStatuses")}</option>{STATUSES.map((value) => <option key={value} value={value}>{t(`logs.statuses.${value}`)}</option>)}</select>
      </div>
      </div>
      <div className="grid min-h-0 flex-1 gap-4 overflow-hidden lg:grid-cols-[minmax(240px,0.85fr)_minmax(0,1.4fr)]">
        <Card className={cn("min-h-0 min-w-0 flex-col overflow-hidden p-0", selectedId ? "hidden lg:flex" : "flex")}>
          <div aria-label={t("logs.listRegion")} className="min-h-0 flex-1 overflow-y-auto overscroll-contain focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--focus)]" ref={listScroll} role="region" tabIndex={0}>
          {list.isPending ? <div className="p-5">{t("common.loading")}</div> : list.isError ? <div className="p-5 text-[var(--danger)]" role="alert">{safeErrorText(list.error, t)}</div> : list.data?.entries.length ? <div className="divide-y divide-[var(--border)]">{list.data.entries.map((entry) => <button aria-pressed={entry.id === selectedId} className={cn("block w-full px-4 py-3 text-left hover:bg-[var(--surface-hover)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--focus)]", entry.id === selectedId && "bg-[var(--accent-soft)]")} key={entry.id} onClick={(event) => { selectedButton.current = event.currentTarget; setSelectedId(entry.id); }} type="button"><div className="flex items-center justify-between gap-3"><span className="font-semibold">{operationLabel(entry.operation, t)}</span><Badge tone={tone(entry.status)}>{t(`logs.statuses.${entry.status}`)}</Badge></div><div className="mt-2 flex flex-wrap gap-3 text-xs text-[var(--muted)]"><span>{formatDate(entry.startedAt, i18n.language)}</span><span>{duration(entry.wallDurationMs ?? entry.activeDurationMs, t)}</span></div></button>)}</div> : <div className="p-5 text-[var(--muted)]">{t("logs.empty")}</div>}
          </div>
          {list.data ? <div className="flex shrink-0 flex-wrap items-center justify-between gap-2 border-t border-[var(--border)] p-3"><span className="text-xs text-[var(--muted)]">{t("logs.pageSummary", { page, total: list.data.total })}</span><div className="flex gap-2"><Button disabled={page <= 1 || list.isFetching} onClick={() => setPage((value) => Math.max(1, value - 1))} type="button" variant="secondary">{t("history.previous")}</Button><Button disabled={!list.data.hasNextPage || list.isFetching} onClick={() => setPage((value) => value + 1)} type="button" variant="secondary">{t("history.next")}</Button></div></div> : null}
        </Card>
        <Card className={cn("min-h-0 min-w-0 flex-col overflow-hidden p-0", selectedId ? "flex" : "hidden lg:flex")}>
          <div className="flex shrink-0 items-center justify-between gap-2 border-b border-[var(--border)] p-3"><Button className="lg:hidden" onClick={backToList} size="compact" type="button" variant="ghost"><ArrowLeft size={16} />{t("logs.backToList")}</Button><span className="hidden text-sm font-semibold lg:inline">{t("logs.detailRegion")}</span>{selectedId ? <Button aria-label={t("logs.refreshDetail")} className="lg:hidden" disabled={detail.isFetching} onClick={() => void detail.refetch()} size="icon" type="button" variant="ghost"><RefreshCw size={16} className={cn(detail.isFetching && "animate-spin")} /></Button> : null}</div>
          <div aria-label={t("logs.detailRegion")} className="min-h-0 flex-1 overflow-y-auto overscroll-contain p-4 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--focus)]" ref={detailScroll} role="region" tabIndex={0}>
          {!selectedId ? <div className="grid h-full place-items-center text-sm text-[var(--muted)]">{t("logs.select")}</div> : detail.isFetching && !selected ? <div>{t("common.loading")}</div> : detail.isError ? <div className="text-[var(--danger)]" role="alert">{safeErrorText(detail.error, t)}<Button className="mt-3" disabled={detail.isFetching} onClick={() => void detail.refetch()} type="button" variant="secondary">{t("common.retry")}</Button></div> : selected ? <div className="grid min-w-0 gap-5" key={selected.id}>
            <div><div className="flex flex-wrap items-center gap-3"><h2 className="text-lg font-semibold">{operationLabel(selected.operation, t)}</h2><Badge tone={tone(selected.status)}>{t(`logs.statuses.${selected.status}`)}</Badge></div><dl className="mt-4 grid gap-2 text-sm sm:grid-cols-2"><div><dt className="text-[var(--muted)]">{t("logs.startedAt")}</dt><dd>{formatDate(selected.startedAt, i18n.language)}</dd></div><div><dt className="text-[var(--muted)]">{t("logs.completedAt")}</dt><dd>{selected.completedAt ? formatDate(selected.completedAt, i18n.language) : "—"}</dd></div><div><dt className="text-[var(--muted)]">{t("logs.activeDuration")}</dt><dd>{duration(selected.activeDurationMs, t)}</dd></div><div><dt className="text-[var(--muted)]">{t("logs.wallDuration")}</dt><dd>{duration(selected.wallDurationMs, t)}</dd></div>{selected.targetProvider ? <div><dt className="text-[var(--muted)]">{t("logs.targetProvider")}</dt><dd className="break-all font-mono text-xs">{selected.targetProvider}</dd></div> : null}</dl></div>
            {hasManyRewrittenSessions(selected.operation, selected.counts) ? <SyncPerformanceTip afterOperation /> : null}
            {["sync", "switch", "watch"].includes(selected.operation) ? <FileUpdateTimingDetails timing={selected.fileUpdateTiming} pending={["running", "awaiting-confirmation"].includes(selected.status)} /> : null}
            {selected.failedStage || selected.failureCode || selected.partialReason ? <dl className="grid gap-3 rounded-lg border border-[var(--warning)] bg-[var(--warning-soft)] p-3 text-sm">{selected.failedStage ? <div><dt>{t("operationResult.fields.failedStage")}</dt><dd>{operationStageLabel(selected.failedStage, t)}</dd></div> : null}{selected.failureCode ? <div><dt>{t("operationResult.fields.failureCode")}</dt><dd>{operationFailureLabel(selected.failureCode, t)}</dd></div> : null}{selected.partialReason ? <div><dt>{t("operationResult.fields.partialReason")}</dt><dd>{t(`operationResult.partialReasons.${selected.partialReason}`, { defaultValue: t("global.partial") })}</dd></div> : null}</dl> : null}
            {selected.retryRecommended ? <p className="text-sm text-[var(--warning)]">{t(selected.partialReason === "locked-session" ? "operationResult.retryAfterSession" : "operationResult.retryFreshPlan")}</p> : null}
            {(selected.backupId || selected.retryRecommended) && (openBackupRestore || reviewOperation) ? selected.profileId === profileId && selected.profileRevision !== undefined && selected.profileRevision === profileRevision ? <div className="flex flex-wrap gap-2">{selected.retryRecommended && reviewOperation && ["sync", "switch", "repair", "watch"].includes(selected.operation) ? <Button onClick={() => reviewOperation(selected.operation)} type="button" variant="secondary">{t("operationResult.reviewOperation")}</Button> : null}{selected.backupId && openBackupRestore ? <Button onClick={() => openBackupRestore(selected.backupId!)} type="button" variant="secondary">{t("operationResult.openBackupRestore")}</Button> : null}</div> : <p className="text-sm text-[var(--muted)]">{t("logs.profileMismatch")}</p> : null}
            {previewCounts.length ? <div><h3 className="font-semibold">{t("logs.previewCounts")}</h3><dl className="mt-2 grid gap-2 text-sm sm:grid-cols-2">{previewCounts.map(([name, value]) => <div className="flex justify-between gap-3 rounded-lg border border-[var(--border)] px-3 py-2" key={name}><dt>{t(`logs.previewCountLabels.${name}`)}</dt><dd>{value}</dd></div>)}</dl></div> : null}
            {selected.switchPlan ? <div><h3 className="font-semibold">{t("logs.switchPlan")}</h3><dl className="mt-2 grid gap-2 text-sm sm:grid-cols-2"><div><dt className="text-[var(--muted)]">{t("logs.providerChange")}</dt><dd className="break-all font-mono text-xs">{selected.switchPlan.previousProvider} → {selected.switchPlan.targetProvider}</dd></div><div><dt className="text-[var(--muted)]">{t("logs.rootModelChange")}</dt><dd className="break-all font-mono text-xs">{selected.switchPlan.previousRootModel ?? t("logs.notSet")} → {selected.switchPlan.targetRootModel ?? t("logs.notSet")}</dd></div><div><dt className="text-[var(--muted)]">{t("logs.modelMode")}</dt><dd>{t(`plan.modelModes.${selected.switchPlan.modelMode}`)}</dd></div></dl>{selected.status === "partial" ? <p className="mt-2 text-sm text-[var(--warning)]">{t("logs.switchPlanPartial")}</p> : null}</div> : selected.operation === "switch" ? <p className="text-sm text-[var(--muted)]">{t("logs.switchPlanUnavailable")}</p> : null}
            {counts.length ? <div><h3 className="font-semibold">{t("logs.counts")}</h3><dl className="mt-2 grid gap-2 text-sm sm:grid-cols-2">{counts.map(([name, value]) => <div className="flex justify-between gap-3 rounded-lg border border-[var(--border)] px-3 py-2" key={name}><dt>{t(`logs.countLabels.${name}`)}</dt><dd>{value}</dd></div>)}</dl></div> : null}
            <div><h3 className="font-semibold">{t("logs.timeline")}</h3><ol className="mt-3 grid gap-3">{selected.stages.map((stage, index) => <li className="rounded-lg border border-[var(--border)] p-3" key={`${stage.stage}-${index}`}><div className="flex justify-between gap-3"><span className="font-medium">{operationStageLabel(stage.stage, t)}</span><span className="text-sm text-[var(--muted)]">{duration(stage.durationMs, t)}</span></div><div className="mt-1 text-xs text-[var(--muted)]">{t(`logs.stageStatuses.${stage.status}`, { defaultValue: t("logs.unknownStage") })}{stage.count === undefined ? "" : ` · ${stage.count}`} · {formatDate(stage.startedAt, i18n.language)}{stage.completedAt ? ` → ${formatDate(stage.completedAt, i18n.language)}` : ""}</div></li>)}</ol></div>
            {selected.errorCode ? <div className="rounded-lg border border-[var(--danger)] bg-[var(--danger-soft)] p-3 text-sm" role="alert"><p>{t(`errors.${selected.errorCode}`, { defaultValue: t("errors.fallback") })}</p>{selected.errorReason ? <p className="mt-1 text-[var(--muted)]">{t("logs.errorReason")}: {t(`logs.errorReasons.${selected.errorReason}`)}</p> : null}</div> : null}
            {selected.warnings.length ? <div><h3 className="font-semibold">{t("common.warnings")}</h3><ul className="mt-2 list-disc pl-5 text-sm">{selected.warnings.map((warning, index) => <li key={`${index}-${warning}`}>{displayWarningText(warning, t)}</li>)}</ul></div> : null}
            <div><h3 className="font-semibold">{t("logs.identifiers")}</h3><dl className="mt-3 grid gap-3 text-sm sm:grid-cols-2"><CorrelationId label={t("logs.logId")} value={selected.id} />{selected.requestIds.map((requestId, index) => <CorrelationId key={requestId} label={`${t("logs.requestId")} ${index + 1}`} value={requestId} />)}<CorrelationId label={t("logs.planId")} value={selected.planId} /><CorrelationId label={t("logs.operationId")} value={selected.operationId} /><CorrelationId label={t("logs.backupId")} value={selected.backupId} /></dl></div>
          </div> : <div className="text-sm text-[var(--muted)]" role="status">{t("logs.detailUnavailable")}</div>}
          </div>
        </Card>
      </div>
    </section>
  );
}
