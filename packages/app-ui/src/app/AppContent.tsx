import type { OperationResult, PlanSummary, ProgressEvent, SwitchModelMode } from "@codex-provider-sync/contracts";
import { CoreClientError } from "@codex-provider-sync/core-client";
import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Activity, ArchiveRestore, Database, FileClock, FolderCog, Gauge, History, RotateCcw, Settings, ShieldAlert, Workflow } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { BackupsRestorePage } from "../features/backups-restore/BackupsRestorePage.js";
import { DiagnosticsPage } from "../features/diagnostics/DiagnosticsPage.js";
import { HistoryPage } from "../features/history/HistoryPage.js";
import { OperationResultDialog, operationResultPresentation } from "../features/operations/OperationResultDialog.js";
import { PlanReview } from "../features/operations/PlanReview.js";
import { OverviewPage } from "../features/overview/OverviewPage.js";
import { ProfilesPage } from "../features/profiles/ProfilesPage.js";
import { SettingsPage } from "../features/settings/SettingsPage.js";
import { SwitchPage } from "../features/switch-provider/SwitchPage.js";
import { SyncPage } from "../features/sync/SyncPage.js";
import { type AppRoute } from "../routes.js";
import { profileSelector, safeErrorText } from "../shared/presentation.js";
import { FULL_APP_UI_CAPABILITIES, type AppUiCapabilities, type AppUiProps } from "../types.js";
import { Badge, Card, cn, useToast } from "../ui.js";

const navigation = [
  ["overview", "nav.overview", Gauge],
  ["sync", "nav.sync", Workflow],
  ["switch-provider", "nav.switchProvider", RotateCcw],
  ["backups-restore", "nav.backupsRestore", ArchiveRestore],
  ["history", "nav.history", History],
  ["profiles", "nav.profiles", FolderCog],
  ["diagnostics", "nav.diagnostics", Activity],
  ["settings", "nav.settings", Settings]
] as const;

function resolveCapabilities(value: AppUiProps["capabilities"]): AppUiCapabilities {
  return { ...FULL_APP_UI_CAPABILITIES, ...value };
}

function routeIsAvailable(route: AppRoute, capabilities: AppUiCapabilities): boolean {
  if (route === "sync") return capabilities.sync;
  if (route === "switch-provider") return capabilities.switchProvider;
  return true;
}

function isProfileStaleError(error: unknown): error is CoreClientError {
  return error instanceof CoreClientError
    && (error.code === "PROFILE_CHANGED"
      || (error.code === "STALE_STATE" && error.dto.details?.reason === "profile"));
}

export function AppContent({ props }: { props: AppUiProps }) {
  const { t, i18n } = useTranslation();
  const toast = useToast();
  const queryClient = useQueryClient();
  const capabilities = useMemo(() => resolveCapabilities(props.capabilities), [props.capabilities]);
  const visibleNavigation = useMemo(
    () => navigation.filter(([id]) => routeIsAvailable(id, capabilities)),
    [capabilities]
  );
  const [route, setRoute] = useState<AppRoute>("overview");
  const [selectedProfileId, setSelectedProfileId] = useState("default");
  const [plan, setPlan] = useState<PlanSummary | null>(null);
  const [operationResult, setOperationResult] = useState<OperationResult | null>(null);
  const [recoveryResultStatusChecked, setRecoveryResultStatusChecked] = useState(true);
  const [operationId, setOperationId] = useState<string | null>(null);
  const [operationProgress, setOperationProgress] = useState<ProgressEvent | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const applyController = useRef<AbortController | null>(null);
  const applySubmissionPending = useRef(false);
  const planReturnFocus = useRef<HTMLElement | null>(null);
  const resultOwnsReturnFocus = useRef(false);
  const profileStaleNoticeActive = useRef(false);
  const profileRefreshInFlight = useRef<Promise<void> | null>(null);
  const mutationCount = useIsMutating();
  const profilesQuery = useQuery({
    queryKey: ["profiles"],
    queryFn: ({ signal }) => props.host.listProfiles(signal)
  });
  const profiles = profilesQuery.data ?? [];
  const profile = profiles.find((entry) => entry.id === selectedProfileId) ?? profiles[0];
  const handleProfileStale = useCallback(async () => {
    if (!profileStaleNoticeActive.current) {
      profileStaleNoticeActive.current = true;
      toast.push({
        title: t("global.profileChanged"),
        description: t("global.profileChangedHint"),
        tone: "warning"
      });
    }
    if (!profileRefreshInFlight.current) {
      const refresh = profilesQuery.refetch()
        .then(() => undefined)
        .finally(() => {
          if (profileRefreshInFlight.current === refresh) profileRefreshInFlight.current = null;
        });
      profileRefreshInFlight.current = refresh;
    }
    await profileRefreshInFlight.current;
  }, [profilesQuery.refetch, t, toast]);

  useEffect(() => {
    document.documentElement.lang = i18n.resolvedLanguage?.toLowerCase().startsWith("zh") ? "zh-CN" : "en";
  }, [i18n.resolvedLanguage]);
  useEffect(() => {
    if (profiles.length && !profiles.some((entry) => entry.id === selectedProfileId)) {
      setSelectedProfileId(profiles[0].id);
    }
  }, [profiles, selectedProfileId]);
  useEffect(() => {
    if (!routeIsAvailable(route, capabilities)) setRoute("overview");
  }, [capabilities, route]);

  const statusQuery = useQuery({
    queryKey: ["status", profile?.id, profile?.revision],
    queryFn: ({ signal }) => props.core.getStatus({ profile: profileSelector(profile) }, { signal }),
    enabled: Boolean(profile)
  });
  const status = statusQuery.data;
  const statusReady = statusQuery.isSuccess && status !== undefined;
  useEffect(() => {
    if (statusReady && status.profile.revision === profile?.revision) {
      profileStaleNoticeActive.current = false;
      return;
    }
    if (isProfileStaleError(statusQuery.error) && !profileStaleNoticeActive.current) {
      void handleProfileStale();
    }
  }, [handleProfileStale, profile?.revision, status?.profile.revision, statusQuery.error, statusReady]);
  const externalWriteActive = status?.operationInProgress != null;
  const writeDisabled = !profile
    || !statusReady
    || status?.pendingRecovery === true
    || externalWriteActive
    || mutationCount > 0;
  const recoveryWriteDisabled = !profile || !statusReady || externalWriteActive || mutationCount > 0;
  const backupsQuery = useQuery({
    queryKey: ["backups", profile?.id, profile?.revision],
    queryFn: ({ signal }) => props.core.listBackups({ profile: profileSelector(profile) }, { signal }),
    enabled: Boolean(profile && route === "backups-restore")
  });
  const diagnosticsQuery = useQuery({
    queryKey: ["diagnostics", profile?.id, profile?.revision],
    queryFn: ({ signal }) => props.core.getDiagnostics({ profile: profileSelector(profile) }, { signal }),
    enabled: Boolean(profile && route === "diagnostics")
  });
  const refreshAfterWrite = useCallback(async ({ refreshStatus = true } = {}) => {
    const refreshes = [
      queryClient.invalidateQueries({ queryKey: ["backups"] }),
      queryClient.invalidateQueries({ queryKey: ["history"] }),
      queryClient.invalidateQueries({ queryKey: ["diagnostics"] })
    ];
    if (refreshStatus) refreshes.push(queryClient.invalidateQueries({ queryKey: ["status"] }));
    await Promise.all(refreshes);
  }, [queryClient]);
  const prepare = useCallback(async (action: () => Promise<PlanSummary>, trigger: HTMLElement | null) => {
    planReturnFocus.current = trigger;
    try {
      setPlan(await action());
    } catch (error) {
      planReturnFocus.current = null;
      if (isProfileStaleError(error)) {
        await handleProfileStale();
        return;
      }
      toast.push({
        title: t("global.failed"),
        description: safeErrorText(error, t("global.unexpected")),
        tone: "danger"
      });
    }
  }, [handleProfileStale, t, toast]);
  const closePlan = useCallback(() => {
    const target = planReturnFocus.current;
    resultOwnsReturnFocus.current = false;
    setPlan(null);
    setOperationId(null);
    setOperationProgress(null);
    setCancelling(false);
    globalThis.requestAnimationFrame(() => globalThis.requestAnimationFrame(() => {
      if (target?.isConnected) target.focus();
      if (planReturnFocus.current === target) planReturnFocus.current = null;
    }));
  }, []);
  const closePlanForResult = useCallback(() => {
    setPlan(null);
    setOperationId(null);
    setOperationProgress(null);
    setCancelling(false);
  }, []);
  const restorePlanFocus = useCallback(() => {
    if (resultOwnsReturnFocus.current) return;
    const target = planReturnFocus.current;
    planReturnFocus.current = null;
    target?.focus();
  }, []);
  const restoreOperationFocus = useCallback(() => {
    const target = planReturnFocus.current;
    planReturnFocus.current = null;
    resultOwnsReturnFocus.current = false;
    target?.focus();
  }, []);
  const applyMutation = useMutation({
    mutationFn: async (summary: PlanSummary): Promise<OperationResult> => {
      const input = { schemaVersion: 1 as const, planId: summary.planId };
      const controller = new AbortController();
      applyController.current = controller;
      setOperationId(null);
      setOperationProgress(null);
      setCancelling(false);
      const options = {
        signal: controller.signal,
        onOperationStarted: (event: { operationId: string }) => setOperationId(event.operationId),
        onProgress: (event: { progress: ProgressEvent }) => setOperationProgress(event.progress)
      };
      try {
        if (summary.operation === "sync") return await props.core.applySync(input, options);
        if (summary.operation === "switch") return await props.core.applySwitch(input, options);
        return await props.core.applyRestore(input, options);
      } finally {
        if (applyController.current === controller) applyController.current = null;
      }
    },
    onSuccess: async (result) => {
      const presentation = operationResultPresentation(result.outcome);
      const requiresRecovery = result.outcome === "recovery_required";
      resultOwnsReturnFocus.current = true;
      setRecoveryResultStatusChecked(!requiresRecovery);
      setOperationResult(result);
      closePlanForResult();
      try {
        if (requiresRecovery) {
          await refreshAfterWrite({ refreshStatus: false });
          const refreshedStatus = await statusQuery.refetch();
          setRecoveryResultStatusChecked(refreshedStatus.isSuccess);
        } else {
          await refreshAfterWrite();
        }
      } catch {
        // Keep recovery-required results non-dismissible until a later fresh
        // Status snapshot proves that pending recovery is clear.
      }
      toast.push({
        title: t(presentation.toastKey),
        description: result.backup?.backupId,
        tone: presentation.tone
      });
    },
    onError: async (error) => {
      await refreshAfterWrite();
      closePlan();
      if (error instanceof CoreClientError && error.code === "OPERATION_CANCELLED") {
        toast.push({ title: t("global.cancelled"), tone: "warning" });
        return;
      }
      if (isProfileStaleError(error)) {
        await handleProfileStale();
        return;
      }
      toast.push({
        title: t("global.failed"),
        description: safeErrorText(error, t("global.unexpected")),
        tone: "danger"
      });
    }
  });
  const pruneMutation = useMutation({
    mutationFn: async (keepCount: number) => {
      if (!profile) throw new Error("No profile is selected.");
      return props.core.pruneBackups({ profile: profileSelector(profile), keepCount });
    },
    onSuccess: async () => {
      await refreshAfterWrite();
      toast.push({ title: t("global.completed"), tone: "success" });
    },
    onError: (error) => {
      toast.push({
        title: t("global.failed"),
        description: safeErrorText(error, t("global.unexpected")),
        tone: "danger"
      });
    }
  });
  const exportDiagnostics = useMutation({
    mutationFn: async () => {
      if (!profile || !props.host.exportDiagnostics) throw new Error("Diagnostics export is unavailable.");
      return props.host.exportDiagnostics(profileSelector(profile));
    },
    onSuccess: (result) => {
      toast.push({
        title: result.status === "created"
          ? t("diagnostics.exportCreated")
          : result.status === "cancelled"
            ? t("diagnostics.exportCancelled")
            : t("diagnostics.exportFailed"),
        tone: result.status === "created" ? "success" : result.status === "cancelled" ? "warning" : "danger"
      });
    },
    onError: () => toast.push({ title: t("diagnostics.exportFailed"), tone: "danger" })
  });

  const configuredProviders = status?.configuredProviders && Array.isArray(status.configuredProviders)
    ? status.configuredProviders.filter((value): value is string => typeof value === "string")
    : [status?.currentProvider ?? "openai"];
  const page = !profile
    ? <Card>{profilesQuery.isPending ? t("common.loading") : safeErrorText(profilesQuery.error, t("global.failed"))}</Card>
    : route === "overview"
      ? <OverviewPage loading={statusQuery.isFetching} refresh={() => void statusQuery.refetch()} status={status} />
      : route === "sync" && capabilities.sync
        ? <SyncPage disabled={writeDisabled} prepare={(values, trigger) => prepare(() => props.core.prepareSync({ profile: profileSelector(profile), keepCount: values.keepCount }), trigger)} />
        : route === "switch-provider" && capabilities.switchProvider
          ? <SwitchPage disabled={writeDisabled} prepare={(values, trigger) => prepare(() => props.core.prepareSwitch({ profile: profileSelector(profile), provider: values.provider, modelMode: values.modelMode as SwitchModelMode, ...(values.modelMode === "explicit" ? { model: values.model } : {}), keepCount: values.keepCount }), trigger)} providers={configuredProviders} />
          : route === "backups-restore"
            ? <BackupsRestorePage backups={backupsQuery.data?.backups ?? []} canPrune={capabilities.pruneBackups} canRestore={capabilities.restore} disabled={recoveryWriteDisabled || pruneMutation.isPending} loading={backupsQuery.isPending} prepare={(values, trigger) => prepare(() => props.core.prepareRestore({ profile: profileSelector(profile), backupId: values.backupId, restoreConfig: values.restoreConfig, restoreDatabase: values.restoreDatabase, restoreSessions: values.restoreSessions, ...(values.allowSqliteHomeRelocation ? { allowSqliteHomeRelocation: true, relocationTargetProfileId: values.relocationTargetProfileId } : {}) }), trigger)} profile={profile} profiles={profiles} prune={(keepCount) => pruneMutation.mutate(keepCount)} />
            : route === "history"
              ? <HistoryPage core={props.core} key={`${profile.id}:${profile.revision}`} profile={profile} />
              : route === "profiles"
                ? <ProfilesPage canManage={capabilities.manageProfiles} host={props.host} profiles={profiles} refresh={() => profilesQuery.refetch()} revealPaths={capabilities.revealProfilePaths} surface={props.surface} />
                : route === "diagnostics"
                  ? <DiagnosticsPage canExport={capabilities.exportDiagnostics && Boolean(props.host.exportDiagnostics)} diagnostics={diagnosticsQuery.data} exportBundle={() => exportDiagnostics.mutate()} exporting={exportDiagnostics.isPending} loading={diagnosticsQuery.isFetching} refresh={() => void diagnosticsQuery.refetch()} />
                  : route === "settings"
                    ? <SettingsPage capabilities={capabilities} profile={profile} props={props} recoveryBlocked={status?.pendingRecovery === true} writeBlocked={!statusReady || externalWriteActive || mutationCount > 0} />
                    : <OverviewPage loading={statusQuery.isFetching} refresh={() => void statusQuery.refetch()} status={status} />;

  return (
    <div className="min-h-screen bg-[var(--surface)] text-[var(--text)]">
      <a className="sr-only focus:not-sr-only focus:fixed focus:left-4 focus:top-4 focus:z-[70] focus:rounded focus:bg-[var(--accent)] focus:px-4 focus:py-2 focus:text-white" href="#main-content" onClick={(event) => { event.preventDefault(); document.getElementById("main-content")?.focus(); }}>{t("a11y.skipToContent")}</a>
      <header className="sticky top-0 z-30 flex min-h-16 flex-wrap items-center justify-between gap-3 border-b border-[var(--border)] bg-[color:var(--surface-raised)/.96] px-4 py-3 backdrop-blur md:px-6">
        <div className="flex min-w-0 items-center gap-3"><div className="grid h-10 w-10 shrink-0 place-items-center rounded-xl bg-[var(--accent)] text-white"><Database size={20} /></div><div className="min-w-0"><div className="flex min-w-0 items-center gap-2"><div className="truncate font-bold">Codex Provider Sync</div><Badge>{t(`brand.${props.surface}.label`)}</Badge></div><div className="truncate text-xs text-[var(--muted)]">{t(`brand.${props.surface}.subtitle`)}</div></div></div>
        <div className="flex w-full min-w-0 items-center justify-between gap-3 sm:w-auto sm:justify-end"><select aria-label={t("a11y.profile")} className="min-w-0 max-w-[min(12rem,70vw)] rounded-[var(--radius-control)] border border-[var(--border)] bg-[var(--input)] px-3 py-2 text-sm" disabled={mutationCount > 0 || externalWriteActive} onChange={(event) => setSelectedProfileId(event.target.value)} value={profile?.id ?? ""}>{profiles.map((entry) => <option key={entry.id} value={entry.id}>{entry.name}</option>)}</select><Badge tone={mutationCount > 0 || externalWriteActive ? "warning" : "success"}>{mutationCount > 0 || externalWriteActive ? t("global.busy") : t("global.ready")}</Badge></div>
      </header>
      <div className="mx-auto grid w-full min-w-0 max-w-[1600px] md:grid-cols-[240px_minmax(0,1fr)]">
        <aside className="min-w-0 max-w-full overflow-hidden border-b border-[var(--border)] bg-[var(--surface-raised)] p-3 md:min-h-[calc(100vh-4rem)] md:border-b-0 md:border-r">
          <nav aria-label={t("a11y.primaryNavigation")} className="flex w-full min-w-0 max-w-full gap-1 overflow-x-auto pb-1 sm:grid sm:grid-cols-4 sm:overflow-visible sm:pb-0 md:grid-cols-1">{visibleNavigation.map(([id, label, Icon]) => <button aria-current={route === id ? "page" : undefined} className={cn("flex min-h-11 shrink-0 items-center gap-3 whitespace-nowrap rounded-[var(--radius-control)] px-3 text-left text-sm font-medium text-[var(--muted)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--focus)] sm:shrink", route === id ? "bg-[var(--accent-soft)] text-[var(--accent-strong)]" : "hover:bg-[var(--surface-hover)] hover:text-[var(--text)]")} key={id} onClick={() => setRoute(id)} type="button"><Icon size={17} /><span>{t(label)}</span></button>)}</nav>
        </aside>
        <main className="min-w-0 p-4 md:p-8" id="main-content" tabIndex={-1}>
          {status?.pendingRecovery ? <div className="mb-5 flex items-start gap-3 rounded-xl border border-[var(--danger)] bg-[var(--danger-soft)] p-4 text-sm" role="alert"><ShieldAlert className="mt-0.5 shrink-0 text-[var(--danger)]" size={20} /><div><div className="font-semibold">RECOVERY_REQUIRED</div><div className="mt-1">{t("global.recovery")}</div></div></div> : null}
          {status?.operationInProgress ? <div className="mb-5 flex items-start gap-3 rounded-xl border border-[var(--warning)] bg-[var(--warning-soft)] p-4 text-sm" role="status"><FileClock className="mt-0.5 shrink-0 text-[var(--warning)]" size={20} /><div><div className="font-semibold">{t("global.busy")}</div><div className="mt-1 text-[var(--muted)]">{t(`plan.operations.${String(status.operationInProgress.operation ?? "operation")}`, { defaultValue: t("plan.operations.operation") })} · {String(status.operationInProgress.busyScope ?? "")}</div></div></div> : null}
          {statusQuery.isError ? <div className="mb-5 rounded-xl border border-[var(--danger)] bg-[var(--danger-soft)] p-4 text-sm text-[var(--danger)]" role="alert">{safeErrorText(statusQuery.error, t("global.failed"))}</div> : null}
          {page}
        </main>
      </div>
      {capabilities.sync || capabilities.switchProvider || capabilities.restore ? <PlanReview apply={() => {
        if (!plan || applySubmissionPending.current || applyMutation.isPending) return;
        applySubmissionPending.current = true;
        applyMutation.mutate(plan, {
          onSettled: () => { applySubmissionPending.current = false; }
        });
      }} applying={applyMutation.isPending} cancel={() => { if (!applyMutation.isPending || cancelling) return; setCancelling(true); applyController.current?.abort(); }} cancelling={cancelling} close={closePlan} confirmDisabled={!statusReady || externalWriteActive || status?.pendingRecovery === true} operationId={operationId} plan={plan} progress={operationProgress} restoreFocus={restorePlanFocus} /> : null}
      <OperationResultDialog close={() => { setOperationResult(null); setRecoveryResultStatusChecked(true); }} closeDisabled={operationResult?.outcome === "recovery_required" && (!recoveryResultStatusChecked || status?.pendingRecovery !== false)} restoreFocus={restoreOperationFocus} result={operationResult} />
    </div>
  );
}
