import type { OperationResult, PlanSummary, ProgressEvent, RepairTarget, SwitchModelMode, WatchSnapshot, WatchStatusList } from "@codex-provider-sync/contracts";
import { CoreClientError } from "@codex-provider-sync/core-client";
import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Activity, ArchiveRestore, Database, FileClock, FolderCog, Gauge, History, ScrollText, Settings, ShieldAlert } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { BackupsRestorePage } from "../features/backups-restore/BackupsRestorePage.js";
import { DiagnosticsPage } from "../features/diagnostics/DiagnosticsPage.js";
import { HistoryPage } from "../features/history/HistoryPage.js";
import { OperationResultDialog, operationResultPresentation } from "../features/operations/OperationResultDialog.js";
import { OperationLogsPage } from "../features/operation-logs/OperationLogsPage.js";
import { PlanReview } from "../features/operations/PlanReview.js";
import { OverviewPage } from "../features/overview/OverviewPage.js";
import { ProfilesPage } from "../features/profiles/ProfilesPage.js";
import { SettingsPage } from "../features/settings/SettingsPage.js";
import { type AppRoute } from "../routes.js";
import type { SyncValues } from "../features/sync/SyncPage.js";
import { displayProfileName, profileSelector, safeErrorText } from "../shared/presentation.js";
import { hasStatusSnapshot, type PostWriteStatus } from "../shared/status-feedback.js";
import { readBackupRetention } from "../shared/backup-preferences.js";
import { keepCountSchema } from "../schemas.js";
import { useRequestProgress } from "../shared/request-progress.js";
import { FULL_APP_UI_CAPABILITIES, type AppUiCapabilities, type AppUiProps } from "../types.js";
import { Badge, Button, Card, cn, useToast } from "../ui.js";

const navigation = [
  ["overview", "nav.overview", Gauge],
  ["backups-restore", "nav.backupsRestore", ArchiveRestore],
  ["history", "nav.history", History],
  ["operation-logs", "nav.operationLogs", ScrollText],
  ["profiles", "nav.profiles", FolderCog],
  ["diagnostics", "nav.diagnostics", Activity],
  ["settings", "nav.settings", Settings]
] as const;

function resolveCapabilities(value: AppUiProps["capabilities"]): AppUiCapabilities {
  return { ...FULL_APP_UI_CAPABILITIES, ...value };
}

function routeIsAvailable(route: AppRoute, capabilities: AppUiCapabilities): boolean {
  if (route === "operation-logs") return capabilities.operationLogs;
  return true;
}

function isProfileStaleError(error: unknown): error is CoreClientError {
  return error instanceof CoreClientError
    && (error.code === "PROFILE_CHANGED"
      || (error.code === "STALE_STATE" && error.dto.details?.reason === "profile"));
}

function profileRevisionKey(profile: { id: string; revision: string } | undefined): string {
  return profile ? `${profile.id}:${profile.revision}` : "";
}

function watchStatusQueryKey(profileId: string, profileRevision: string) {
  return ["watch-status", profileId, profileRevision] as const;
}

function storeStoppedWatch(
  queryClient: ReturnType<typeof useQueryClient>,
  watch: WatchSnapshot
): void {
  queryClient.setQueriesData<WatchSnapshot | WatchStatusList>({ queryKey: ["watch-status"] }, (cached) => {
    if (!cached) return cached;
    if ("watches" in cached) {
      return { ...cached, watches: cached.watches.map((entry) => entry.watchId === watch.watchId ? watch : entry) };
    }
    return cached.watchId === watch.watchId ? watch : cached;
  });
}

export function AppContent({ props }: { props: AppUiProps }) {
  const { t, i18n } = useTranslation();
  const toast = useToast();
  const queryClient = useQueryClient();
  const capabilities = useMemo(() => resolveCapabilities(props.capabilities), [props.capabilities]);
  const terminalWatches = useRef(new Map<string, number>());
  useEffect(() => {
    if (!capabilities.viewUpdateStatus) return;
    return props.host.subscribeUpdateStatus?.((value) => queryClient.setQueryData(["desktop-update-status"], value));
  }, [capabilities.viewUpdateStatus, props.host, queryClient]);
  useEffect(() => {
    if (!capabilities.watch) return;
    return props.host.subscribeWatchStopped?.((event) => {
      const knownGeneration = terminalWatches.current.get(event.watch.watchId);
      if (knownGeneration !== undefined && knownGeneration >= event.generation) return;
      while (terminalWatches.current.size >= 256) {
        const oldest = terminalWatches.current.keys().next().value as string | undefined;
        if (!oldest) break;
        terminalWatches.current.delete(oldest);
      }
      terminalWatches.current.set(event.watch.watchId, event.generation);
      storeStoppedWatch(queryClient, event.watch);
      queryClient.setQueryData(watchStatusQueryKey(event.profileId, event.profileRevision), event.watch);
    });
  }, [capabilities.watch, props.host, queryClient]);
  const visibleNavigation = useMemo(
    () => navigation.filter(([id]) => routeIsAvailable(id, capabilities)),
    [capabilities]
  );
  const [route, setRoute] = useState<AppRoute>("overview");
  const [retentionCount, setRetentionCount] = useState(() => readBackupRetention(props.preferences));
  const [selectedProfileId, setSelectedProfileId] = useState("default");
  const [plan, setPlan] = useState<PlanSummary | null>(null);
  const [operationResult, setOperationResult] = useState<OperationResult | null>(null);
  const [postWriteStatus, setPostWriteStatus] = useState<PostWriteStatus | undefined>();
  const [recoveryResultStatusChecked, setRecoveryResultStatusChecked] = useState(true);
  const [operationProgress, setOperationProgress] = useState<ProgressEvent | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const [directSyncPhase, setDirectSyncPhase] = useState<"preparing" | "applying" | null>(null);
  const [repairSelectionPending, setRepairSelectionPending] = useState(false);
  const [repairSelectionFailed, setRepairSelectionFailed] = useState(false);
  const [repairDraftDirty, setRepairDraftDirty] = useState(false);
  const [repairRequest, setRepairRequest] = useState<{ targets: RepairTarget[]; keepCount: number } | null>(null);
  const [diagnosticsExpiryByProfile, setDiagnosticsExpiryByProfile] = useState<Record<string, number>>({});
  const [restoreBackupId, setRestoreBackupId] = useState<string | undefined>();
  const [operationResultProfile, setOperationResultProfile] = useState<{ id: string; revision: string } | null>(null);
  const applyController = useRef<AbortController | null>(null);
  const currentOperationId = useRef<string | null>(null);
  const applySubmissionPending = useRef(false);
  const repairSelectionPendingRef = useRef(false);
  const repairDraftDirtyRef = useRef(false);
  const repairPrepareController = useRef<AbortController | null>(null);
  const diagnosticsExpiryRef = useRef<Record<string, number>>({});
  const diagnosticsExpiryCounter = useRef(0);
  const currentProfileKeyRef = useRef("");
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
  const currentProfileKey = profileRevisionKey(profile);
  const { state: scanProgress, start: startScanProgress } = useRequestProgress(currentProfileKey);
  const { state: repairProgress, start: startRepairProgress } = useRequestProgress(currentProfileKey);
  currentProfileKeyRef.current = currentProfileKey;
  const markDiagnosticsExpired = useCallback((target: { id: string; revision: string }) => {
    const key = profileRevisionKey(target);
    const token = ++diagnosticsExpiryCounter.current;
    diagnosticsExpiryRef.current = { ...diagnosticsExpiryRef.current, [key]: token };
    setDiagnosticsExpiryByProfile(diagnosticsExpiryRef.current);
  }, []);
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
    setRestoreBackupId(undefined);
    repairPrepareController.current?.abort();
    repairPrepareController.current = null;
    repairSelectionPendingRef.current = false;
    repairDraftDirtyRef.current = false;
    setRepairSelectionPending(false);
    setRepairDraftDirty(false);
    setRepairSelectionFailed(false);
  }, [currentProfileKey]);
  useEffect(() => () => { repairPrepareController.current?.abort(); }, []);
  useEffect(() => {
    if (!routeIsAvailable(route, capabilities)) setRoute("overview");
  }, [capabilities, route]);

  const statusQuery = useQuery({
    queryKey: ["status", profile?.id, profile?.revision],
    queryFn: ({ signal }) => props.core.getStatus({ profile: profileSelector(profile) }, { signal }),
    enabled: Boolean(profile)
  });
  const status = statusQuery.isError ? undefined : statusQuery.data;
  const statusReadBlocked = Boolean(status?.statusReadBlocked);
  const readBlock = status?.statusReadBlocked;
  const statusChangedDuringRead = typeof readBlock === "object" && readBlock !== null && !Array.isArray(readBlock)
    && readBlock.reason === "state-changed-during-status";
  const statusReady = statusQuery.isSuccess && status !== undefined && !statusReadBlocked;
  const recentSwitchesQuery = useQuery({
    queryKey: ["recent-successful-switches", profile?.id, profile?.revision],
    queryFn: ({ signal }) => props.host.listOperationLogs!({ page: 1, pageSize: 100, profileId: profile!.id, profileRevision: profile!.revision, operation: "switch", status: "completed" }, signal),
    enabled: Boolean(profile && capabilities.operationLogs && props.host.listOperationLogs),
    retry: false,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false
  });
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
    || mutationCount > 0 || directSyncPhase !== null || repairProgress !== null;
  const recoveryWriteDisabled = !profile || !statusReady || externalWriteActive || mutationCount > 0 || directSyncPhase !== null;
  const backupsQuery = useQuery({
    queryKey: ["backups", profile?.id, profile?.revision],
    queryFn: ({ signal }) => props.core.listBackups({ profile: profileSelector(profile) }, { signal }),
    enabled: Boolean(profile && route === "backups-restore")
  });
  const diagnosticsQuery = useQuery({
    queryKey: ["diagnostics", profile?.id, profile?.revision],
    queryFn: async ({ signal }) => {
      const progress = startScanProgress();
      try {
        return await props.core.getDiagnostics({ profile: profileSelector(profile) }, { signal, onRequestProgress: progress.onRequestProgress });
      } finally { progress.finish(); }
    },
    enabled: false
  });
  const refreshDiagnostics = useCallback(async () => {
    if (!profile) return;
    const key = profileRevisionKey(profile);
    const expiryAtStart = diagnosticsExpiryRef.current[key] ?? 0;
    const response = await diagnosticsQuery.refetch();
    if (response.isSuccess && currentProfileKeyRef.current === key && (diagnosticsExpiryRef.current[key] ?? 0) === expiryAtStart) {
      const next = { ...diagnosticsExpiryRef.current };
      delete next[key];
      diagnosticsExpiryRef.current = next;
      setDiagnosticsExpiryByProfile(next);
    }
  }, [diagnosticsQuery, profile]);
  const refreshAfterWrite = useCallback(async ({ refreshStatus = true } = {}) => {
    const refreshes = [
      queryClient.invalidateQueries({ queryKey: ["backups"] }),
      queryClient.invalidateQueries({ queryKey: ["history"] }),
      queryClient.invalidateQueries({ queryKey: ["diagnostics"] }),
      queryClient.invalidateQueries({ queryKey: ["recent-successful-switches"] })
    ];
    // Mark every Profile snapshot stale, including relocation targets. Apply's
    // result verifier performs its one explicit read of the current Profile.
    refreshes.push(queryClient.invalidateQueries({ queryKey: ["status"], refetchType: refreshStatus ? "active" : "none" }));
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
        description: safeErrorText(error, t),
        tone: "danger"
      });
    }
  }, [handleProfileStale, t, toast]);
  const closePlan = useCallback(() => {
    const target = planReturnFocus.current;
    resultOwnsReturnFocus.current = false;
    setPlan(null);
    setRepairDraftDirty(false);
    repairDraftDirtyRef.current = false;
    setRepairSelectionFailed(false);
    currentOperationId.current = null;
    setOperationProgress(null);
    setCancelling(false);
    globalThis.requestAnimationFrame(() => globalThis.requestAnimationFrame(() => {
      if (target?.isConnected) target.focus();
      if (planReturnFocus.current === target) planReturnFocus.current = null;
    }));
  }, []);
  const dismissPlan = useCallback(() => {
    if (plan) void props.host.dismissOperationPlan?.(plan.planId);
    closePlan();
  }, [closePlan, plan, props.host]);
  const closePlanForResult = useCallback(() => {
    setPlan(null);
    setDirectSyncPhase(null);
    currentOperationId.current = null;
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
      currentOperationId.current = null;
      setOperationProgress(null);
      setCancelling(false);
      const options = {
        signal: controller.signal,
        onOperationStarted: (event: { operationId: string }) => { currentOperationId.current = event.operationId; },
        onProgress: (event: { progress: ProgressEvent }) => setOperationProgress(event.progress)
      };
      try {
        if (summary.operation === "sync") return await props.core.applySync(input, options);
        if (summary.operation === "switch") return await props.core.applySwitch(input, options);
        if (summary.operation === "repair") return await props.core.applyRepair(input, options);
        return await props.core.applyRestore(input, options);
      } finally {
        if (applyController.current === controller) applyController.current = null;
      }
    },
    onSuccess: async (result, summary) => {
      const presentation = operationResultPresentation(result.outcome);
      const requiresRecovery = result.outcome === "recovery_required";
      resultOwnsReturnFocus.current = true;
      setRecoveryResultStatusChecked(!requiresRecovery);
      setOperationResult(result);
      setPostWriteStatus({ operationId: result.operationId, state: "checking" });
      setOperationResultProfile(summary.profile);
      markDiagnosticsExpired(summary.profile);
      closePlanForResult();
      // Reuse the existing post-write Status read, but retain its success/failure
      // explicitly. Cached pre-operation data is never final verification.
      const [, checked] = await Promise.allSettled([
        refreshAfterWrite({ refreshStatus: false }), statusQuery.refetch()
      ]);
      const response = checked.status === "fulfilled" ? checked.value : undefined;
      const snapshot = response?.isSuccess && hasStatusSnapshot(response.data)
        && response.data.profile.id === summary.profile.id
        && response.data.profile.revision === summary.profile.revision
        && currentProfileKeyRef.current === profileRevisionKey(summary.profile)
        ? response.data : undefined;
      setPostWriteStatus({ operationId: result.operationId, state: snapshot ? "received" : "unverified", ...(snapshot ? { snapshot } : {}) });
      if (requiresRecovery) setRecoveryResultStatusChecked(Boolean(snapshot));
      toast.push({
        title: t(presentation.toastKey),
        description: result.backup ? t("operationResult.backupCreated") : undefined,
        tone: presentation.tone
      });
    },
    onError: async (error, summary) => {
      markDiagnosticsExpired(summary.profile);
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
        description: safeErrorText(error, t),
        tone: "danger"
      });
    }
  });
  const directSync = async (values: SyncValues, trigger: HTMLButtonElement | null) => {
    if (writeDisabled || !capabilities.sync || plan || operationResult || applySubmissionPending.current) return;
    applySubmissionPending.current = true;
    planReturnFocus.current = trigger;
    const controller = new AbortController();
    applyController.current = controller;
    setCancelling(false);
    setOperationProgress(null);
    setDirectSyncPhase("preparing");
    let applyStarted = false;
    try {
      const summary = await props.core.prepareSync({ profile: profileSelector(profile), keepCount: retentionCount }, { signal: controller.signal });
      if (controller.signal.aborted) {
        void props.host.dismissOperationPlan?.(summary.planId);
        toast.push({ title: t("global.cancelled"), tone: "warning" });
        return;
      }
      setDirectSyncPhase("applying");
      applyStarted = true;
      await applyMutation.mutateAsync(summary);
    } catch (error) {
      // Apply already owns result/error presentation and post-write refresh.
      if (!applyStarted) {
        if (controller.signal.aborted || (error instanceof CoreClientError && error.code === "OPERATION_CANCELLED")) {
          toast.push({ title: t("global.cancelled"), tone: "warning" });
        } else if (isProfileStaleError(error)) {
          await handleProfileStale();
        } else {
          toast.push({ title: t("global.failed"), description: safeErrorText(error, t), tone: "danger" });
        }
      }
    } finally {
      if (applyController.current === controller) applyController.current = null;
      applySubmissionPending.current = false;
      setDirectSyncPhase(null);
      setCancelling(false);
    }
  };
  const prepareRepair = useCallback(async (targets: RepairTarget[], keepCount: number, trigger: HTMLElement | null) => {
    if (!capabilities.repair || writeDisabled || !profile || targets.length === 0 || repairPrepareController.current) return;
    setRepairRequest({ targets, keepCount });
    setRepairSelectionFailed(false);
    setRepairDraftDirty(false);
    repairDraftDirtyRef.current = false;
    const controller = new AbortController();
    repairPrepareController.current = controller;
    const requestedProfileKey = currentProfileKey;
    const progress = startRepairProgress();
    planReturnFocus.current = trigger;
    try {
      const nextPlan = await props.core.prepareRepair({ profile: profileSelector(profile), targets, keepCount }, { signal: controller.signal, onRequestProgress: progress.onRequestProgress });
      if (controller.signal.aborted || requestedProfileKey !== currentProfileKeyRef.current) {
        void props.host.dismissOperationPlan?.(nextPlan.planId);
        return;
      }
      setPlan(nextPlan);
    } catch (error) {
      if (!controller.signal.aborted) {
        planReturnFocus.current = null;
        if (isProfileStaleError(error)) await handleProfileStale();
        else toast.push({ title: t("global.failed"), description: safeErrorText(error, t), tone: "danger" });
      }
    } finally {
      if (repairPrepareController.current === controller) repairPrepareController.current = null;
      progress.finish();
    }
  }, [capabilities.repair, currentProfileKey, handleProfileStale, writeDisabled, profile, props.core, props.host, startRepairProgress, t, toast]);
  const refineRepairSessions = useCallback(async (sessionIds: string[] | null) => {
    if (!plan || plan.operation !== "repair" || !repairRequest || repairSelectionPending) return;
    if (Array.isArray(sessionIds) && sessionIds.length === 0) return;
    const oldPlanId = plan.planId;
    const requestedProfileKey = currentProfileKey;
    const controller = new AbortController();
    repairPrepareController.current?.abort();
    repairPrepareController.current = controller;
    const progress = startRepairProgress();
    setRepairSelectionPending(true);
    repairSelectionPendingRef.current = true;
    setRepairSelectionFailed(false);
    setRepairDraftDirty(true);
    repairDraftDirtyRef.current = true;
    void props.host.dismissOperationPlan?.(oldPlanId);
    try {
      const input = {
        profile: profileSelector(profile),
        targets: repairRequest.targets,
        keepCount: repairRequest.keepCount,
        ...(sessionIds === null ? {} : { sessionIds })
      };
      const nextPlan = await props.core.prepareRepair(input, { signal: controller.signal, onRequestProgress: progress.onRequestProgress });
      if (controller.signal.aborted || requestedProfileKey !== currentProfileKeyRef.current) {
        void props.host.dismissOperationPlan?.(nextPlan.planId);
        return;
      }
      setPlan(nextPlan);
      setRepairDraftDirty(false);
      repairDraftDirtyRef.current = false;
    } catch (error) {
      if (!controller.signal.aborted) {
        setRepairSelectionFailed(true);
        if (isProfileStaleError(error)) await handleProfileStale();
        else toast.push({ title: t("global.failed"), description: safeErrorText(error, t), tone: "danger" });
      }
    } finally {
      progress.finish();
      if (repairPrepareController.current === controller) {
        repairPrepareController.current = null;
        setRepairSelectionPending(false);
        repairSelectionPendingRef.current = false;
      }
    }
  }, [currentProfileKey, handleProfileStale, plan, profile, props.core, props.host, repairRequest, repairSelectionPending, startRepairProgress, t, toast]);
  const retentionMutation = useMutation({
    mutationFn: async (count: number): Promise<"saved" | "watch-active"> => {
      keepCountSchema.parse(count);
      if (!props.preferences.setBackupRetention || plan || operationResult || externalWriteActive || !statusReady || directSyncPhase !== null) throw new Error("Backup preference unavailable.");
      // An explicit save checks every watcher in this Core process; no polling.
      const snapshot = await props.core.getWatchStatus({});
      const watches = "watches" in snapshot ? snapshot.watches : [snapshot];
      if (watches.some((watch) => watch.status !== "stopped")) return "watch-active";
      props.preferences.setBackupRetention(count);
      setRetentionCount(count);
      return "saved";
    }
  });
  const pruneMutation = useMutation({
    mutationFn: async (input: { keepCount: number; profile: { id: string; revision: string } }) => {
      return props.core.pruneBackups({ profile: { profileId: input.profile.id, profileRevision: input.profile.revision }, keepCount: input.keepCount });
    },
    onSuccess: async (_result, input) => {
      markDiagnosticsExpired(input.profile);
      await refreshAfterWrite();
      toast.push({ title: t("global.completed"), tone: "success" });
    },
    onError: (error, input) => {
      markDiagnosticsExpired(input.profile);
      toast.push({
        title: t("global.failed"),
        description: safeErrorText(error, t),
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
  const recentSuccessfulProviders = statusReady
    ? [...new Set((recentSwitchesQuery.data?.entries ?? [])
      .filter((entry) => entry.profileId === profile?.id && entry.profileRevision === profile?.revision
        && entry.status === "completed" && entry.outcome === "completed" && entry.switchPlan !== undefined)
      .map((entry) => entry.switchPlan!.targetProvider)
      .filter((provider) => configuredProviders.includes(provider)))].slice(0, 5)
    : [];
  const currentProfileName = profile ? displayProfileName(profile, t) : "";
  const statusIndicator = status?.pendingRecovery
    ? { label: t("global.recoveryTitle"), tone: "warning" as const }
    : statusQuery.isError
      ? { label: t("global.statusUnavailable"), tone: "danger" as const }
      : mutationCount > 0 || directSyncPhase !== null || externalWriteActive
        ? { label: t("global.busy"), tone: "warning" as const }
        : statusReadBlocked
          ? { label: t("global.statusNeedsRefresh"), tone: "neutral" as const }
        : !statusReady
          ? { label: t("global.readingStatus"), tone: "neutral" as const }
          : { label: t("global.ready"), tone: "success" as const };
  const page = !profile
    ? <Card>{profilesQuery.isPending ? t("common.loading") : safeErrorText(profilesQuery.error, t)}</Card>
    : route === "overview"
? <OverviewPage profileKey={currentProfileKey} retentionCount={retentionCount} directSync={capabilities.sync ? directSync : undefined} loading={statusQuery.isFetching} manageStorage={() => setRoute("profiles")} prepareSwitch={(values, trigger) => prepare(() => props.core.prepareSwitch({ profile: profileSelector(profile), provider: values.provider, modelMode: values.modelMode as SwitchModelMode, ...(values.modelMode === "explicit" ? { model: values.model } : {}), keepCount: retentionCount }), trigger)} prepareSync={(values, trigger) => prepare(() => props.core.prepareSync({ profile: profileSelector(profile), keepCount: retentionCount }), trigger)} profileName={currentProfileName} providers={configuredProviders} recentSuccessfulProviders={recentSuccessfulProviders} refresh={() => { void statusQuery.refetch(); void recentSwitchesQuery.refetch(); }} sqliteHomeConfigured={profile.sqliteHomeConfigured === true || Boolean(profile.sqliteHome)} status={status} writeDisabled={writeDisabled} />
      : route === "backups-restore"
            ? <BackupsRestorePage retentionCount={retentionCount} saveRetention={props.preferences.setBackupRetention ? (count) => retentionMutation.mutateAsync(count) : undefined} key={currentProfileKey} error={backupsQuery.error} refresh={() => void backupsQuery.refetch()} refreshing={backupsQuery.isFetching} backups={backupsQuery.data?.backups ?? []} canPrune={capabilities.pruneBackups} canRestore={capabilities.restore} disabled={recoveryWriteDisabled || pruneMutation.isPending} initialBackupId={restoreBackupId} loading={backupsQuery.isPending} prepare={(values, trigger) => prepare(() => props.core.prepareRestore({ profile: profileSelector(profile), backupId: values.backupId, restoreConfig: values.restoreConfig, restoreDatabase: values.restoreDatabase, restoreSessions: values.restoreSessions, ...(values.allowSqliteHomeRelocation ? { allowSqliteHomeRelocation: true, relocationTargetProfileId: values.relocationTargetProfileId } : {}) }), trigger)} profile={profile} profiles={profiles} prune={(keepCount) => pruneMutation.mutate({ keepCount, profile: { id: profile.id, revision: profile.revision } })} />
            : route === "history"
              ? <HistoryPage core={props.core} host={props.host} preferences={props.preferences} key={`${profile.id}:${profile.revision}`} profile={profile} />
              : route === "operation-logs" && capabilities.operationLogs && props.host.listOperationLogs && props.host.getOperationLog
                ? <OperationLogsPage key={currentProfileKey} host={props.host} profileId={profile.id} profileRevision={profile.revision} openBackupRestore={capabilities.restore ? (backupId) => { setRestoreBackupId(backupId); setRoute("backups-restore"); } : undefined} reviewOperation={(operation) => setRoute(operation === "repair" ? "diagnostics" : "overview")} />
              : route === "profiles"
                ? <ProfilesPage selectedProfileId={selectedProfileId} canManage={capabilities.manageProfiles} host={props.host} profiles={profiles} refresh={() => profilesQuery.refetch()} revealPaths={capabilities.revealProfilePaths} selectProfile={setSelectedProfileId} surface={props.surface} />
                : route === "diagnostics"
                  ? <DiagnosticsPage key={currentProfileKey} canExport={capabilities.exportDiagnostics && Boolean(props.host.exportDiagnostics)} canRepair={capabilities.repair} diagnostics={diagnosticsQuery.data} error={diagnosticsQuery.error} expired={Boolean(diagnosticsExpiryByProfile[currentProfileKey])} exportBundle={() => exportDiagnostics.mutate()} exporting={exportDiagnostics.isPending} loading={diagnosticsQuery.isFetching} prepareRepair={(values, trigger) => {
                      const targets = (["models", "cwd", "userEvent", "workspaceRoots"] as const)
                        .filter((target) => values[target]) as RepairTarget[];
                      return prepareRepair(targets, retentionCount, trigger);
                    }} refresh={() => { void refreshDiagnostics(); }} repairDisabled={writeDisabled || diagnosticsQuery.isFetching} scanProgress={scanProgress} repairProgress={repairProgress} />
                  : route === "settings"
                    ? <SettingsPage retentionCount={retentionCount} key={currentProfileKey} capabilities={capabilities} isWatchTerminal={(watchId) => terminalWatches.current.has(watchId)} profile={profile} props={props} recoveryBlocked={status?.pendingRecovery === true} writeBlocked={!statusReady || externalWriteActive || mutationCount > 0 || directSyncPhase !== null} />
                    : <OverviewPage profileKey={currentProfileKey} retentionCount={retentionCount} directSync={capabilities.sync ? directSync : undefined} loading={statusQuery.isFetching} manageStorage={() => setRoute("profiles")} prepareSwitch={(values, trigger) => prepare(() => props.core.prepareSwitch({ profile: profileSelector(profile), provider: values.provider, modelMode: values.modelMode as SwitchModelMode, ...(values.modelMode === "explicit" ? { model: values.model } : {}), keepCount: retentionCount }), trigger)} prepareSync={(values, trigger) => prepare(() => props.core.prepareSync({ profile: profileSelector(profile), keepCount: retentionCount }), trigger)} profileName={currentProfileName} providers={configuredProviders} recentSuccessfulProviders={recentSuccessfulProviders} refresh={() => { void statusQuery.refetch(); void recentSwitchesQuery.refetch(); }} sqliteHomeConfigured={profile.sqliteHomeConfigured === true || Boolean(profile.sqliteHome)} status={status} writeDisabled={writeDisabled} />;

  return (
    <div className={cn("bg-[var(--surface)] text-[var(--text)]", ["history", "operation-logs"].includes(route) ? "flex h-dvh min-h-0 flex-col overflow-hidden" : "min-h-screen")}>
      <a className="sr-only focus:not-sr-only focus:fixed focus:left-4 focus:top-4 focus:z-[70] focus:rounded focus:bg-[var(--accent)] focus:px-4 focus:py-2 focus:text-white" href="#main-content" onClick={(event) => { event.preventDefault(); document.getElementById("main-content")?.focus(); }}>{t("a11y.skipToContent")}</a>
      <header className={cn("sticky top-0 z-30 flex min-h-16 shrink-0 flex-wrap items-center justify-between gap-3 border-b border-[var(--border)] bg-[color:var(--surface-raised)/.96] px-4 py-3 backdrop-blur md:px-6", ["history", "operation-logs"].includes(route) && "[@media(max-height:500px)]:min-h-0 [@media(max-height:500px)]:py-1")}>
        <div className={cn("flex min-w-0 items-center gap-3", ["history", "operation-logs"].includes(route) && "[@media(max-height:500px)]:hidden")}><div className="grid h-10 w-10 shrink-0 place-items-center rounded-xl bg-[var(--accent)] text-white"><Database size={20} /></div><div className="min-w-0"><div className="flex min-w-0 items-center gap-2"><div className="truncate font-bold">Codex Provider Sync</div><Badge>{t(`brand.${props.surface}.label`)}</Badge></div><div className="truncate text-xs text-[var(--muted)]">{t(`brand.${props.surface}.subtitle`)}</div></div></div>
        <div className="flex w-full min-w-0 items-center justify-between gap-3 sm:w-auto sm:justify-end"><select aria-label={t("a11y.profile")} className="min-w-0 max-w-[min(12rem,70vw)] rounded-[var(--radius-control)] border border-[var(--border)] bg-[var(--input)] px-3 py-2 text-sm" disabled={mutationCount > 0 || directSyncPhase !== null || externalWriteActive || repairSelectionPending} onChange={(event) => setSelectedProfileId(event.target.value)} value={profile?.id ?? ""}>{profiles.map((entry) => <option key={entry.id} value={entry.id}>{displayProfileName(entry, t)}</option>)}</select><Badge tone={statusIndicator.tone}>{statusIndicator.label}</Badge></div>
      </header>
      <div className={cn("mx-auto grid w-full min-w-0 max-w-[1600px] md:grid-cols-[240px_minmax(0,1fr)]", ["history", "operation-logs"].includes(route) && "min-h-0 flex-1 grid-rows-[auto_minmax(0,1fr)] overflow-hidden md:grid-rows-1")}>
        <aside className={cn("min-w-0 max-w-full border-b border-[var(--border)] bg-[var(--surface-raised)] p-3 md:border-b-0 md:border-r", ["history", "operation-logs"].includes(route) ? "min-h-0 overflow-y-auto overscroll-contain [@media(max-height:500px)]:p-1" : "overflow-hidden md:min-h-[calc(100vh-4rem)]")}>
          <nav aria-label={t("a11y.primaryNavigation")} className="flex w-full min-w-0 max-w-full gap-1 overflow-x-auto pb-1 sm:grid sm:grid-cols-4 sm:overflow-visible sm:pb-0 md:grid-cols-1">{visibleNavigation.map(([id, label, Icon]) => <button aria-current={route === id ? "page" : undefined} className={cn("flex min-h-11 shrink-0 items-center gap-3 whitespace-nowrap rounded-[var(--radius-control)] px-3 text-left text-sm font-medium text-[var(--muted)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--focus)] sm:shrink", route === id ? "bg-[var(--accent-soft)] text-[var(--accent-strong)]" : "hover:bg-[var(--surface-hover)] hover:text-[var(--text)]")} key={id} onClick={() => setRoute(id)} type="button"><Icon size={17} /><span>{t(label)}</span></button>)}</nav>
        </aside>
        <main className={cn("min-w-0", ["history", "operation-logs"].includes(route) ? "flex min-h-0 flex-col overflow-hidden p-3 md:p-4" : "p-4 md:p-8")} id="main-content" tabIndex={-1}>
          <div className={["history", "operation-logs"].includes(route) ? "max-h-[30%] shrink-0 overflow-y-auto overscroll-contain" : undefined}>
          {status?.pendingRecovery ? <div className="mb-5 flex items-start gap-3 rounded-xl border border-[var(--danger)] bg-[var(--danger-soft)] p-4 text-sm" role="alert"><ShieldAlert className="mt-0.5 shrink-0 text-[var(--danger)]" size={20} /><div><div className="font-semibold">{t("global.recoveryTitle")}</div><div className="mt-1">{t("global.recovery")}</div></div></div> : null}
          {status?.operationInProgress ? <div className="mb-5 flex items-start gap-3 rounded-xl border border-[var(--warning)] bg-[var(--warning-soft)] p-4 text-sm" role="status"><FileClock className="mt-0.5 shrink-0 text-[var(--warning)]" size={20} /><div><div className="font-semibold">{t("global.busy")}</div><div className="mt-1 text-[var(--muted)]">{t("global.busyHint")}</div></div></div> : null}
          {statusReadBlocked && !externalWriteActive ? <div className="mb-5 flex flex-wrap items-center justify-between gap-3 rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-4 text-sm" role="status"><div><div className="font-semibold">{t("global.statusNeedsRefresh")}</div><p className="mt-1 text-[var(--muted)]">{t(statusChangedDuringRead ? "global.statusChangedHint" : "global.statusUnavailableHint")}</p></div><Button disabled={statusQuery.isFetching} onClick={() => void statusQuery.refetch()} type="button" variant="secondary">{t("global.retryStatus")}</Button></div> : null}
          {statusQuery.isError ? <div className="mb-5 rounded-xl border border-[var(--danger)] bg-[var(--danger-soft)] p-4 text-sm text-[var(--danger)]" role="alert">{safeErrorText(statusQuery.error, t)}</div> : null}
          </div>
          {page}
        </main>
      </div>
      {capabilities.sync || capabilities.switchProvider || capabilities.repair || capabilities.restore ? <PlanReview repairProgress={repairProgress} apply={() => {
        if (!plan || repairSelectionFailed || repairSelectionPendingRef.current || repairDraftDirtyRef.current || applySubmissionPending.current || applyMutation.isPending) return;
        applySubmissionPending.current = true;
        applyMutation.mutate(plan, {
          onSettled: () => { applySubmissionPending.current = false; }
        });
      }} directSyncPhase={directSyncPhase} applying={applyMutation.isPending || directSyncPhase !== null} cancel={() => { if ((!applyMutation.isPending && directSyncPhase === null) || cancelling) return; setCancelling(true); applyController.current?.abort(); }} cancelling={cancelling} close={dismissPlan} confirmDisabled={!statusReady || externalWriteActive || status?.pendingRecovery === true} currentModel={status?.currentModel} plan={plan} progress={operationProgress} repairDraftChanged={(dirty) => { repairDraftDirtyRef.current = dirty; setRepairDraftDirty(dirty); }} repairSelectionFailed={repairSelectionFailed} repairSelectionPending={repairSelectionPending} refineRepairSessions={refineRepairSessions} restoreFocus={restorePlanFocus} /> : null}
      <OperationResultDialog postWriteStatus={operationResultProfile?.id === profile?.id && operationResultProfile?.revision === profile?.revision ? postWriteStatus : operationResult ? { operationId: operationResult.operationId, state: "unverified" } : undefined} reviewOperation={operationResult?.outcome === "partial" && operationResultProfile?.id === profile?.id && operationResultProfile?.revision === profile?.revision ? () => { const operation = operationResult.operation; planReturnFocus.current = null; setOperationResult(null); setOperationResultProfile(null); setRoute(operation === "repair" ? "diagnostics" : "overview"); globalThis.requestAnimationFrame(() => document.getElementById("main-content")?.focus()); } : undefined} close={() => { setOperationResult(null); setOperationResultProfile(null); setRecoveryResultStatusChecked(true); }} closeDisabled={operationResult?.outcome === "recovery_required" && (!recoveryResultStatusChecked || status?.pendingRecovery !== false)} openBackupRestore={capabilities.restore && operationResultProfile?.id === profile?.id && operationResultProfile?.revision === profile?.revision ? (backupId) => { setRestoreBackupId(backupId); setOperationResult(null); setOperationResultProfile(null); setRecoveryResultStatusChecked(true); setRoute("backups-restore"); } : undefined} restoreFocus={restoreOperationFocus} result={operationResult} />
    </div>
  );
}
