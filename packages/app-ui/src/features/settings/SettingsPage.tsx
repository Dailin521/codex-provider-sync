import type { WatchSnapshot, WatchStatusList } from "@codex-provider-sync/contracts";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Globe2, Languages, Moon, Play, RefreshCw, Sun } from "lucide-react";
import { Fragment, useState } from "react";
import { useTranslation } from "react-i18next";

import { PageHeading, profileSelector, safeErrorText } from "../../shared/presentation.js";
import type { AppUiCapabilities, AppUiProps, HostProfile, HostUpdateStatus } from "../../types.js";
import { Badge, Button, Card, cn, Field } from "../../ui.js";
import { DEFAULT_BACKUP_RETENTION_COUNT } from "../../schemas.js";

function activeWatch(value: WatchSnapshot | WatchStatusList | undefined): WatchSnapshot | null {
  if (!value) return null;
  if ("watches" in value) return value.watches.find((watch) => watch.status !== "stopped") ?? value.watches[0] ?? null;
  return value;
}

function watchStatusQueryKey(profile: HostProfile) {
  return ["watch-status", profile.id, profile.revision] as const;
}

export function SettingsPage({ props, profile, capabilities, recoveryBlocked, writeBlocked, isWatchTerminal, retentionCount = DEFAULT_BACKUP_RETENTION_COUNT }: {
  retentionCount?: number;
  props: AppUiProps;
  profile: HostProfile;
  capabilities: AppUiCapabilities;
  recoveryBlocked: boolean;
  writeBlocked: boolean;
  isWatchTerminal?(watchId: string): boolean;
}) {
  const { t, i18n } = useTranslation();
  const queryClient = useQueryClient();
  const [theme, setTheme] = useState(props.preferences.getTheme() ?? props.initialTheme);
  const watchKey = watchStatusQueryKey(profile);
  const watch = useQuery({
    queryKey: watchKey,
    queryFn: ({ signal }) => props.core.getWatchStatus({ profile: profileSelector(profile) }, { signal }),
    enabled: capabilities.watch
  });
  const currentWatch = activeWatch(watch.data);
  const storeWatch = (value: WatchSnapshot, target: HostProfile) => {
    if (value.status !== "stopped" && isWatchTerminal?.(value.watchId)) return;
    // Aliases explicitly enabled for the same Home share one watchId.
    // Update only matching cached IDs, without polling other profiles.
    queryClient.setQueriesData<WatchSnapshot | WatchStatusList>({ queryKey: ["watch-status"] }, (cached) => {
      if (!cached) return cached;
      if ("watches" in cached) return { ...cached, watches: cached.watches.map((entry) => entry.watchId === value.watchId ? value : entry) };
      return cached.watchId === value.watchId ? value : cached;
    });
    queryClient.setQueryData(watchStatusQueryKey(target), value);
  };
  const start = useMutation({
    mutationFn: (target: HostProfile) => props.core.startWatch({ profile: profileSelector(target), includeStateDb: true, keepCount: retentionCount }),
    onSuccess: storeWatch
  });
  const stop = useMutation({
    mutationFn: ({ watchId }: { watchId: string; profile: HostProfile }) => props.core.stopWatch({ watchId }),
    onSuccess: (value, target) => storeWatch(value, target.profile)
  });
  const isCurrentProfile = (target?: HostProfile) => target?.id === profile.id && target.revision === profile.revision;
  const watchRequestError = watch.error ?? (isCurrentProfile(start.variables) ? start.error : null) ?? (isCurrentProfile(stop.variables?.profile) ? stop.error : null);
  const update = useQuery({
    queryKey: ["desktop-update-status"],
    queryFn: ({ signal }) => props.host.getUpdateStatus?.(signal),
    enabled: capabilities.viewUpdateStatus && Boolean(props.host.getUpdateStatus)
  });
  const canRefresh = capabilities.watch
    || (capabilities.viewUpdateStatus && Boolean(props.host.getUpdateStatus));
  const refreshing = watch.isFetching || update.isFetching;
  const refreshStatuses = async () => {
    await Promise.all([
      capabilities.watch ? watch.refetch() : Promise.resolve(),
      capabilities.viewUpdateStatus && props.host.getUpdateStatus
        ? update.refetch()
        : Promise.resolve()
    ]);
  };
  const storeUpdate = (value: HostUpdateStatus) => queryClient.setQueryData(["desktop-update-status"], value);
  const checkUpdate = useMutation({
    mutationFn: () => props.host.checkForUpdates?.() ?? Promise.reject(new Error("Update check unavailable.")),
    onSuccess: storeUpdate
  });
  const downloadUpdate = useMutation({
    mutationFn: () => props.host.downloadUpdate?.() ?? Promise.reject(new Error("Update download unavailable.")),
    onSuccess: storeUpdate
  });
  const installUpdate = useMutation({
    mutationFn: () => props.host.installUpdate?.() ?? Promise.reject(new Error("Update install unavailable.")),
    onSuccess: storeUpdate
  });
  const updateRequestFailed = update.isError || checkUpdate.isError || downloadUpdate.isError || installUpdate.isError;
  const updateBusy = checkUpdate.isPending || downloadUpdate.isPending || installUpdate.isPending;
  const setLocale = async (locale: "zh-CN" | "en") => {
    props.preferences.setLocale(locale);
    await i18n.changeLanguage(locale);
  };
  const applyTheme = (value: "system" | "light" | "dark") => {
    setTheme(value);
    props.preferences.setTheme(value);
    document.documentElement.dataset.theme = value;
  };
  return (
    <Fragment>
      <PageHeading
        action={canRefresh ? (
          <Button disabled={refreshing} onClick={() => void refreshStatuses()} type="button" variant="secondary">
            <RefreshCw className={cn(refreshing && "animate-spin")} size={16} />
            {t("common.refresh")}
          </Button>
        ) : undefined}
        title={t("settings.title")}
        subtitle={t(`settings.subtitle.${props.surface}`)}
      />
      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <Field label={t("settings.language")}>
            <select className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" onChange={(event) => void setLocale(event.target.value as "zh-CN" | "en")} value={i18n.language === "zh-CN" ? "zh-CN" : "en"}>
              <option value="zh-CN">简体中文</option><option value="en">English</option>
            </select>
          </Field>
          <div className="mt-3 flex items-center gap-2 text-xs text-[var(--muted)]"><Languages size={15} />{t("settings.languageHint")}</div>
        </Card>
        <Card>
          <fieldset>
            <legend className="mb-1.5 text-sm font-medium text-[var(--text)]">{t("settings.theme")}</legend>
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
              {(["system", "light", "dark"] as const).map((value) => <Button aria-pressed={theme === value} key={value} onClick={() => applyTheme(value)} type="button" variant={theme === value ? "primary" : "secondary"}>{value === "system" ? <Globe2 size={16} /> : value === "light" ? <Sun size={16} /> : <Moon size={16} />}{t(`settings.${value}`)}</Button>)}
            </div>
          </fieldset>
        </Card>
        {capabilities.watch ? (
          <Card>
            <h2 className="font-semibold">{t("settings.watch")}</h2>
            <p className="mt-1 text-sm text-[var(--muted)]">{t("settings.watchHint")}</p>
            <div className="mt-3 flex items-center justify-between gap-3">
              <Badge tone={currentWatch?.status === "running" ? "success" : "neutral"}>{watch.isPending ? t("common.loading") : watch.isError ? t("common.unknown") : currentWatch ? t(`settings.watchStatuses.${currentWatch.status}`, { defaultValue: t("common.unknown") }) : t("settings.watchStatuses.stopped")}</Badge>
              {currentWatch?.status === "running"
                ? <Button disabled={!watch.isSuccess || watch.isFetching || (stop.isPending && isCurrentProfile(stop.variables?.profile))} onClick={() => stop.mutate({ watchId: currentWatch.watchId, profile })} type="button" variant="secondary">{t("settings.watchStop")}</Button>
                : <Button disabled={(start.isPending && isCurrentProfile(start.variables)) || recoveryBlocked || writeBlocked || !watch.isSuccess || watch.isFetching || currentWatch?.status === "stopping"} onClick={() => start.mutate(profile)} type="button"><Play size={16} />{t("settings.watchStart")}</Button>}
            </div>
            {watchRequestError ? <p className="mt-3 text-xs text-[var(--danger)]" role="alert">{safeErrorText(watchRequestError, t)}</p> : null}
            {recoveryBlocked && currentWatch?.status !== "running" ? <p className="mt-3 text-xs text-[var(--danger)]">{t("settings.watchRecoveryBlocked")}</p> : null}
          </Card>
        ) : null}
        {capabilities.viewUpdateStatus && props.host.getUpdateStatus ? (
          <Card>
            <h2 className="font-semibold">{t("settings.update")}</h2>
            {update.data?.currentVersion ? <p className="mt-2 text-sm">{t("settings.updateCurrentVersion", { version: update.data.currentVersion })}</p> : null}
            <p className="mt-2 text-sm text-[var(--muted)]">{t(update.data?.mode === "manual" ? "settings.updateManualHint" : "settings.updateAutomaticHint")}</p>
            <div className="mt-3">
              <Badge tone={update.data?.state === "error" || Boolean(update.data?.installBlockedReason) ? "warning" : update.data?.state === "downloaded" ? "success" : "neutral"}>{update.isPending ? t("common.loading") : update.data ? t(`settings.updateStatus.${update.data.state}`) : t("common.unknown")}</Badge>
              {update.data?.version ? <p className="mt-3 text-sm">{t("settings.updateVersion", { version: update.data.version })}</p> : null}
              {update.data?.progressPercent !== undefined ? <p className="mt-2 text-sm text-[var(--muted)]">{t("settings.updateProgress", { percent: update.data.progressPercent })}</p> : null}
              {update.data?.reason ? <p className="mt-3 text-sm text-[var(--muted)]">{t(`settings.updateReason.${update.data.reason}`)}</p> : null}
              {update.data?.installBlockedReason ? <p className="mt-3 text-sm text-[var(--danger)]">{t(`settings.updateBlocked.${update.data.installBlockedReason}`)}</p> : null}
              <div className="mt-4 flex flex-wrap gap-2">
                {updateRequestFailed ? <p className="w-full text-sm text-[var(--danger)]" role="alert">{t("settings.updateRequestFailed")}</p> : null}
                {update.data && ["idle", "not-available", "error", "available"].includes(update.data.state) && props.host.checkForUpdates ? <Button disabled={updateBusy} onClick={() => checkUpdate.mutate()} type="button" variant="secondary">{checkUpdate.isPending ? t("settings.updateStatus.checking") : t("settings.updateCheck")}</Button> : null}
                {update.data?.state === "available" && props.host.downloadUpdate ? <Button disabled={updateBusy} onClick={() => downloadUpdate.mutate()} type="button">{t(update.data.mode === "manual" ? "settings.updateOpenDownload" : "settings.updateDownload")}</Button> : null}
                {update.data?.state === "downloaded" && props.host.installUpdate ? <Button disabled={!update.data.installAllowed || updateBusy || writeBlocked || recoveryBlocked || currentWatch?.status === "running"} onClick={() => installUpdate.mutate()} type="button">{t("settings.updateInstall")}</Button> : null}
              </div>
            </div>
          </Card>
        ) : null}
        {capabilities.forgetBrowser ? <Card><h2 className="font-semibold">{t("settings.forget")}</h2><p className="mt-2 text-sm text-[var(--muted)]">{t("settings.forgetHint")}</p><Button className="mt-4" onClick={() => void (props.onForgetBrowser?.() ?? props.host.forgetBrowser?.())} type="button" variant="danger">{t("settings.forget")}</Button></Card> : null}
      </div>
    </Fragment>
  );
}
