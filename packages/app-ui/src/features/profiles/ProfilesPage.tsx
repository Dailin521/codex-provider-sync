import { zodResolver } from "@hookform/resolvers/zod";
import { useMutation } from "@tanstack/react-query";
import { Fragment, useEffect, useState } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { z } from "zod";

import { profileSchema } from "../../schemas.js";
import { PageHeading, safeErrorText } from "../../shared/presentation.js";
import type { AppUiProps, HostProfile } from "../../types.js";
import { Badge, Button, Card, Field, Input, cn, useToast } from "../../ui.js";

type ProfileValues = z.infer<typeof profileSchema>;

export function ProfilesPage({ profiles, refresh, host, canManage, revealPaths }: {
  profiles: HostProfile[];
  refresh(): Promise<unknown>;
  host: AppUiProps["host"];
  canManage: boolean;
  revealPaths: boolean;
}) {
  const { t } = useTranslation();
  const toast = useToast();
  const [editing, setEditing] = useState<HostProfile | null>(null);
  const form = useForm<ProfileValues>({
    resolver: zodResolver(profileSchema),
    defaultValues: { profileId: "", name: "", codexHome: "", sqliteHome: "" }
  });
  useEffect(() => {
    form.reset(editing
      ? { profileId: editing.id, name: editing.name, codexHome: editing.codexHome ?? "", sqliteHome: editing.sqliteHome ?? "" }
      : { profileId: "", name: "", codexHome: "", sqliteHome: "" });
  }, [editing, form]);
  const save = useMutation({
    mutationFn: async (values: ProfileValues) => {
      if (!canManage || !host.saveProfile) throw new Error("Profile management is unavailable.");
      return host.saveProfile({ ...values, ...(editing ? { profileRevision: editing.revision } : {}) });
    },
    onSuccess: async () => {
      await refresh();
      setEditing(null);
      form.reset();
      toast.push({ title: t("common.save"), tone: "success" });
    },
    onError: (error) => toast.push({
      title: t("global.failed"),
      description: safeErrorText(error, t("global.unexpected")),
      tone: "danger"
    })
  });
  const remove = useMutation({
    mutationFn: (profile: HostProfile) => {
      if (!canManage || !host.deleteProfile) throw new Error("Profile management is unavailable.");
      return host.deleteProfile(profile.id, profile.revision);
    },
    onSuccess: async () => {
      await refresh();
      setEditing(null);
      toast.push({ title: t("common.delete"), tone: "success" });
    },
    onError: (error) => toast.push({
      title: t("global.failed"),
      description: safeErrorText(error, t("global.unexpected")),
      tone: "danger"
    })
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
                  <div className="flex min-w-0 justify-between gap-2"><span className="min-w-0 truncate font-semibold">{profile.name}</span>{profile.id === "default" ? <Badge>{t("common.current")}</Badge> : null}</div>
                  <div className="mt-2 font-mono text-xs text-[var(--muted)]">{profile.id}</div>
                  {revealPaths && profile.codexHome
                    ? <div className="mt-1 max-w-full truncate font-mono text-xs text-[var(--muted)]">{profile.codexHome}</div>
                    : <div className="mt-1 text-xs text-[var(--muted)]">{t("profiles.pathManaged")}</div>}
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
              <Field error={form.formState.errors.profileId ? t("validation.profileId") : undefined} label={t("profiles.id")}><Input disabled={Boolean(editing)} {...form.register("profileId")} /></Field>
              <Field error={form.formState.errors.name ? t("validation.required") : undefined} label={t("profiles.name")}><Input {...form.register("name")} /></Field>
              <Field error={form.formState.errors.codexHome ? t("validation.path") : undefined} label={t("profiles.codexHome")}><Input {...form.register("codexHome")} /></Field>
              <Field error={form.formState.errors.sqliteHome ? t("validation.path") : undefined} label={t("profiles.sqliteHome")}><Input {...form.register("sqliteHome")} /></Field>
              <div className="flex flex-wrap gap-3"><Button disabled={save.isPending} type="submit">{editing ? t("profiles.update") : t("profiles.create")}</Button>{editing ? <Button disabled={remove.isPending} onClick={() => remove.mutate(editing)} type="button" variant="danger">{t("common.delete")}</Button> : null}</div>
            </form>
            <p className="mt-4 text-xs text-[var(--muted)]">{t("profiles.defaultManaged")}</p>
          </Card>
        ) : null}
      </div>
    </Fragment>
  );
}
