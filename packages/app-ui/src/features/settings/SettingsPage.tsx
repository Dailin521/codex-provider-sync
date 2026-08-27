import type { WatchSnapshot, WatchStatusList } from "@codex-provider-sync/contracts";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Globe2, Languages, Moon, Play, Sun } from "lucide-react";
import { Fragment, useState } from "react";
import { useTranslation } from "react-i18next";

import { PageHeading, profileSelector } from "../../shared/presentation.js";
import type { AppUiCapabilities, AppUiProps, HostProfile, HostUpdateStatus } from "../../types.js";
import { Badge, Button, Card, Field } from "../../ui.js";

function activeWatch(value: WatchSnapshot | WatchStatusList | undefined): WatchSnapshot | null {
  if (!value) return null;
  if ("watches" in value) return value.watches.find((watch) => watch.status !== "stopped") ?? value.watches[0] ?? null;
  return value;
}

export function SettingsPage({ props, profile, capabilities, recoveryBlocked, writeBlocked }: {
  props: AppUiProps;
  profile: HostProfile;
  capabilities: AppUiCapabilities;
  recoveryBlocked: boolean;
  writeBlocked: boolean;
}) {
  const { t, i18n } = useTranslation();
  const queryClient = useQueryClient();
  const [theme, setTheme] = useState(props.preferences.getTheme() ?? props.initialTheme);
  const watch = useQuery({
    queryKey: ["watch-status"],
    queryFn: () => props.core.getWatchStatus({}),
    enabled: capabilities.watch,
    refetchInterval: capabilities.watch ? 3000 : false
  });
  const currentWatch = activeWatch(watch.data);
  const start = useMutation({
    mutationFn: () => props.core.startWatch({ profile: profileSelector(profile), includeStateDb: true }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["watch-status"] })
  });
  const stop = useMutation({
    mutationFn: (watchId: string) => props.core.stopWatch({ watchId }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["watch-status"] })
  });
  const update = useQuery({
    queryKey: ["desktop-update-status"],
    queryFn: ({ signal }) => props.host.getUpdateStatus?.(signal),
    enabled: capabilities.viewUpdateStatus && Boolean(props.host.getUpdateStatus),
    refetchInterval: capabilities.viewUpdateStatus ? 3000 : false
  });
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
      <PageHeading title={t("settings.title")} subtitle={t("settings.subtitle")} />
      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <Field label={t("settings.language")}>
            <select className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" onChange={(event) => void setLocale(event.target.value as "zh-CN" | "en")} value={i18n.language === "zh-CN" ? "zh-CN" : "en"}>
              <option value="zh-CN">简体中文</option><option value="en">English</option>
            </select>
          </Field>
          <div className="mt-3 flex items-center gap-2 text-xs text-[var(--muted)]"><Languages size={15} />{t("settings.englishFallback")}</div>
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
            <div className="mt-3 flex items-center justify-between gap-3">
              <div><Badge tone={currentWatch?.status === "running" ? "success" : "neutral"}>{currentWatch?.status ?? t("common.none")}</Badge>{currentWatch ? <div className="mt-2 font-mono text-xs text-[var(--muted)]">{currentWatch.watchId}</div> : null}</div>
              {currentWatch?.status === "running"
                ? <Button disabled={stop.isPending} onClick={() => stop.mutate(currentWatch.watchId)} type="button" variant="secondary">{t("settings.watchStop")}</Button>
                : <Button disabled={start.isPending || recoveryBlocked || writeBlocked} onClick={() => start.mutate()} type="button"><Play size={16} />{t("settings.watchStart")}</Button>}
            </div>
            {recoveryBlocked && currentWatch?.status !== "running" ? <p className="mt-3 text-xs text-[var(--danger)]">{t("settings.watchRecoveryBlocked")}</p> : null}
          </Card>
        ) : null}
        {capabilities.viewUpdateStatus && props.host.getUpdateStatus ? (
          <Card>
            <h2 className="font-semibold">{t("settings.update")}</h2>
            <div className="mt-3">
              <Badge tone={update.data?.state === "error" || Boolean(update.data?.installBlockedReason) ? "warning" : update.data?.state === "downloaded" ? "success" : "neutral"}>{update.isPending ? t("common.loading") : update.data ? t(`settings.updateStatus.${update.data.state}`) : t("common.unknown")}</Badge>
              {update.data?.version ? <p className="mt-3 text-sm">{t("settings.updateVersion", { version: update.data.version })}</p> : null}
              {update.data?.progressPercent !== undefined ? <p className="mt-2 text-sm text-[var(--muted)]">{t("settings.updateProgress", { percent: update.data.progressPercent })}</p> : null}
              {update.data?.reason ? <p className="mt-3 text-sm text-[var(--muted)]">{t(`settings.updateReason.${update.data.reason}`)}</p> : null}
              {update.data?.installBlockedReason ? <p className="mt-3 text-sm text-[var(--danger)]">{t(`settings.updateBlocked.${update.data.installBlockedReason}`)}</p> : null}
              <div className="mt-4 flex flex-wrap gap-2">
                {update.data && ["idle", "not-available", "error"].includes(update.data.state) && props.host.checkForUpdates ? <Button disabled={checkUpdate.isPending} onClick={() => checkUpdate.mutate()} type="button" variant="secondary">{t("settings.updateCheck")}</Button> : null}
                {update.data?.state === "available" && props.host.downloadUpdate ? <Button disabled={downloadUpdate.isPending} onClick={() => downloadUpdate.mutate()} type="button">{t("settings.updateDownload")}</Button> : null}
                {update.data?.state === "downloaded" && props.host.installUpdate ? <Button disabled={!update.data.installAllowed || installUpdate.isPending} onClick={() => installUpdate.mutate()} type="button">{t("settings.updateInstall")}</Button> : null}
              </div>
            </div>
          </Card>
        ) : null}
        {capabilities.forgetBrowser ? <Card><h2 className="font-semibold">{t("settings.forget")}</h2><p className="mt-2 text-sm text-[var(--muted)]">{t("settings.forgetHint")}</p><Button className="mt-4" onClick={() => void (props.onForgetBrowser?.() ?? props.host.forgetBrowser?.())} type="button" variant="danger">{t("settings.forget")}</Button></Card> : null}
      </div>
    </Fragment>
  );
}
