import { useMutation } from "@tanstack/react-query";
import { Fragment, useEffect, useState } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";

import { displayProfileName, PageHeading, safeErrorText } from "../../shared/presentation.js";
import type { AppUiProps, AppUiSurface, HostDirectorySelection, HostProfile } from "../../types.js";
import { Badge, Button, Card, Field, Input, cn, useToast } from "../../ui.js";

interface ProfileValues {
  profileId: string;
  name: string;
  codexHome: string;
  sqliteHome: string;
}

interface SelectedDirectory {
  token: string;
  displayName: string;
}

export function ProfilesPage({ profiles, selectedProfileId, refresh, selectProfile, host, canManage, revealPaths, surface }: {
  profiles: HostProfile[];
  selectedProfileId: string;
  refresh(): Promise<unknown>;
  selectProfile(profileId: string): void;
  host: AppUiProps["host"];
  canManage: boolean;
  revealPaths: boolean;
  surface: AppUiSurface;
}) {
  const { t } = useTranslation();
  const toast = useToast();
  const desktop = surface === "desktop";
  const [editing, setEditing] = useState<HostProfile | null>(null);
  const [codexSelection, setCodexSelection] = useState<SelectedDirectory | null>(null);
  const [sqliteSelection, setSqliteSelection] = useState<SelectedDirectory | null>(null);
  const [sqliteMode, setSqliteMode] = useState<"preserve" | "inherit" | "selected">("inherit");
  const form = useForm<ProfileValues>({
    defaultValues: { profileId: "", name: "", codexHome: "", sqliteHome: "" }
  });
  useEffect(() => {
    form.reset(editing
      ? { profileId: editing.id, name: editing.name, codexHome: editing.codexHome ?? "", sqliteHome: editing.sqliteHome ?? "" }
      : { profileId: "", name: "", codexHome: "", sqliteHome: "" });
    setCodexSelection(null);
    setSqliteSelection(null);
    setSqliteMode(editing?.sqliteHomeConfigured ? "preserve" : "inherit");
  }, [editing, form]);

  const choose = async (kind: "codex-home" | "sqlite-home"): Promise<void> => {
    if (!host.selectProfileDirectory) return;
    const result: HostDirectorySelection = await host.selectProfileDirectory(kind);
    if (result.status !== "selected") return;
    const selected = { token: result.token, displayName: result.displayName };
    if (kind === "codex-home") setCodexSelection(selected);
    else {
      setSqliteSelection(selected);
      setSqliteMode("selected");
    }
  };

  const save = useMutation({
    mutationFn: async (values: ProfileValues) => {
      if (!canManage || !host.saveProfile) throw new Error(t("profiles.unavailable"));
      if (!values.name.trim()) throw new Error(t("validation.required"));
      if (desktop) {
        if (!editing && !codexSelection) throw new Error(t("profiles.selectCodexRequired"));
        return host.saveProfile({
          name: values.name,
          ...(editing ? { profileId: editing.id, profileRevision: editing.revision } : {}),
          ...(codexSelection ? { codexHomeSelectionToken: codexSelection.token } : {}),
          sqliteHomeMode: sqliteMode,
          ...(sqliteHomeModeSelection(sqliteMode, sqliteSelection))
        });
      }
      return host.saveProfile({ ...values, ...(editing ? { profileRevision: editing.revision } : {}) });
    },
    onSuccess: async (profile) => {
      await refresh();
      selectProfile(profile.id);
      setEditing(null);
      form.reset();
      toast.push({ title: t("profiles.saved"), tone: "success" });
    },
    onError: (error) => toast.push({ title: t("global.failed"), description: safeErrorText(error, t), tone: "danger" })
  });
  const remove = useMutation({
    mutationFn: (profile: HostProfile) => {
      if (!canManage || !host.deleteProfile) throw new Error(t("profiles.unavailable"));
      return host.deleteProfile(profile.id, profile.revision);
    },
    onSuccess: async (_result, deletedProfile) => {
      if (deletedProfile.id === selectedProfileId) selectProfile("default");
      await refresh();
      setEditing(null);
      toast.push({ title: t("profiles.deleted"), tone: "success" });
    },
    onError: (error) => toast.push({ title: t("global.failed"), description: safeErrorText(error, t), tone: "danger" })
  });
  return (
    <Fragment>
      <PageHeading title={t("profiles.title")} subtitle={t("profiles.subtitle")} />
      <div className={cn("grid min-w-0 gap-4", canManage && "xl:grid-cols-[minmax(0,1fr)_420px]")}>
        <Card className="min-w-0">
          <div className="grid gap-3">
            {profiles.map((profile) => {
              const content = (
                <Fragment>
                  <div className="flex min-w-0 flex-wrap justify-between gap-2"><span className="min-w-0 truncate font-semibold">{displayProfileName(profile, t)}</span><span className="flex flex-wrap gap-1">{profile.id === selectedProfileId ? <Badge tone="success">{t("ux.currentProfile")}</Badge> : null}{profile.id === "default" ? <Badge>{t("profiles.managed")}</Badge> : null}</span></div>
                  {!desktop ? <div className="mt-2 font-mono text-xs text-[var(--muted)]">{profile.id}</div> : null}
                  {revealPaths && profile.codexHome
                    ? <div className="mt-1 max-w-full truncate font-mono text-xs text-[var(--muted)]">{profile.codexHome}</div>
                    : <div className="mt-1 text-xs text-[var(--muted)]">{t(`profiles.pathManaged.${surface}`)}</div>}
                </Fragment>
              );
              if (!canManage || profile.id === "default") return <div className="min-w-0 max-w-full overflow-hidden rounded-lg border border-[var(--border)] p-4 text-left" key={profile.id}>{content}</div>;
              return <button className={cn("min-w-0 max-w-full overflow-hidden rounded-lg border p-4 text-left", editing?.id === profile.id ? "border-[var(--accent)] bg-[var(--accent-soft)]" : "border-[var(--border)] hover:bg-[var(--surface-hover)]")} key={profile.id} onClick={() => setEditing(profile)} type="button">{content}</button>;
            })}
          </div>
          {!canManage ? <p className="mt-4 text-xs text-[var(--muted)]">{t("profiles.readOnly")}</p> : null}
        </Card>
        {canManage ? (
          <Card className="min-w-0">
            <form className="grid min-w-0 gap-4" onSubmit={form.handleSubmit((values) => save.mutateAsync(values))}>
              {!desktop ? <Field label={t("profiles.id")}><Input disabled={Boolean(editing)} {...form.register("profileId", { required: true })} /></Field> : null}
              <Field label={t("profiles.name")}><Input {...form.register("name", { required: true, maxLength: 120 })} /></Field>
              {desktop ? (
                <Fragment>
                  <Field label={t("profiles.codexHome")}>
                    <div className="flex items-center gap-2"><Button onClick={() => void choose("codex-home")} type="button" variant="secondary">{t("profiles.chooseFolder")}</Button><span className="truncate text-sm text-[var(--muted)]">{codexSelection?.displayName ?? (editing ? t("profiles.keepCurrent") : t("profiles.notSelected"))}</span></div>
                  </Field>
                  <Field label={t("profiles.sqliteHome")}>
                    <div className="grid gap-2">
                      <select className="h-10 rounded-lg border border-[var(--border)] bg-[var(--surface)] px-3" onChange={(event) => setSqliteMode(event.target.value as typeof sqliteMode)} value={sqliteMode}>
                        {editing ? <option value="preserve">{t("profiles.keepCurrent")}</option> : null}
                        <option value="inherit">{t("profiles.inheritSqlite")}</option>
                        <option value="selected">{t("profiles.customSqlite")}</option>
                      </select>
                      {sqliteMode === "selected" ? <div className="flex items-center gap-2"><Button onClick={() => void choose("sqlite-home")} type="button" variant="secondary">{t("profiles.chooseFolder")}</Button><span className="truncate text-sm text-[var(--muted)]">{sqliteSelection?.displayName ?? t("profiles.notSelected")}</span></div> : null}
                    </div>
                  </Field>
                </Fragment>
              ) : (
                <Fragment>
                  <Field label={t("profiles.codexHome")}><Input {...form.register("codexHome", { required: true })} /></Field>
                  <Field label={t("profiles.sqliteHome")}><Input {...form.register("sqliteHome")} /></Field>
                </Fragment>
              )}
              <div className="flex flex-wrap gap-3"><Button disabled={save.isPending || (desktop && sqliteMode === "selected" && !sqliteSelection)} type="submit">{editing ? t("profiles.update") : t("profiles.create")}</Button>{editing ? <Button disabled={remove.isPending} onClick={() => remove.mutate(editing)} type="button" variant="danger">{t("common.delete")}</Button> : null}</div>
            </form>
            {desktop && editing && host.revealProfileDirectory ? <div className="mt-4 flex flex-wrap gap-2"><Button onClick={() => void host.revealProfileDirectory?.(editing.id, editing.revision, "codex-home")} type="button" variant="ghost">{t("profiles.revealCodex")}</Button>{editing.sqliteHomeConfigured ? <Button onClick={() => void host.revealProfileDirectory?.(editing.id, editing.revision, "sqlite-home")} type="button" variant="ghost">{t("profiles.revealSqlite")}</Button> : null}</div> : null}
            <p className="mt-4 text-xs text-[var(--muted)]">{t("profiles.defaultManaged")}</p>
          </Card>
        ) : null}
      </div>
    </Fragment>
  );
}

function sqliteHomeModeSelection(
  mode: "preserve" | "inherit" | "selected",
  selection: SelectedDirectory | null
): { sqliteHomeSelectionToken?: string } {
  return mode === "selected" && selection ? { sqliteHomeSelectionToken: selection.token } : {};
}
