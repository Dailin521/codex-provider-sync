import type { ManagedBackup } from "@codex-provider-sync/contracts";
import { zodResolver } from "@hookform/resolvers/zod";
import { ArchiveRestore } from "lucide-react";
import { Fragment, useRef, useState } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { z } from "zod";

import { restoreSchema } from "../../schemas.js";
import { formatBytes, formatDate, PageHeading } from "../../shared/presentation.js";
import type { HostProfile } from "../../types.js";
import { Badge, Button, Card, Field, Input, cn } from "../../ui.js";

export type RestoreValues = z.infer<typeof restoreSchema>;

function BackupRow({ backup, selected, onSelect }: {
  backup: ManagedBackup;
  selected: boolean;
  onSelect?: () => void;
}) {
  const content = (
    <Fragment>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="font-mono text-xs font-semibold">{backup.backupId}</span>
        <Badge>{formatBytes(backup.sizeBytes)}</Badge>
      </div>
      {backup.createdAt ? <div className="mt-2 text-xs text-[var(--muted)]">{formatDate(backup.createdAt)}</div> : null}
    </Fragment>
  );
  const className = cn(
    "w-full rounded-lg border p-4 text-left",
    selected ? "border-[var(--accent)] bg-[var(--accent-soft)]" : "border-[var(--border)]"
  );
  if (!onSelect) return <div className={className}>{content}</div>;
  return (
    <button
      aria-pressed={selected}
      className={cn(className, "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--focus)] hover:bg-[var(--surface-hover)]")}
      onClick={onSelect}
      type="button"
    >
      {content}
    </button>
  );
}

export function BackupsRestorePage({
  profile,
  profiles,
  backups,
  loading,
  disabled,
  canRestore,
  canPrune,
  prepare,
  prune
}: {
  profile: HostProfile;
  profiles: HostProfile[];
  backups: ManagedBackup[];
  loading: boolean;
  disabled: boolean;
  canRestore: boolean;
  canPrune: boolean;
  prepare(values: RestoreValues, trigger: HTMLButtonElement | null): Promise<void>;
  prune(keepCount: number): void;
}) {
  const { t } = useTranslation();
  const form = useForm<RestoreValues>({
    resolver: zodResolver(restoreSchema),
    defaultValues: {
      backupId: "",
      restoreConfig: true,
      restoreDatabase: true,
      restoreSessions: true,
      allowSqliteHomeRelocation: false,
      relocationTargetProfileId: ""
    }
  });
  const relocation = form.watch("allowSqliteHomeRelocation");
  const [keepCount, setKeepCount] = useState(5);
  const prepareButton = useRef<HTMLButtonElement>(null);
  const selectedBackupId = form.watch("backupId");
  return (
    <Fragment>
      <PageHeading title={t("backups.title")} subtitle={t("backups.subtitle")} />
      <div className={cn("grid gap-4", (canRestore || canPrune) && "xl:grid-cols-[minmax(0,1fr)_minmax(320px,440px)]")}>
        <Card>
          <div className="grid gap-3">
            {loading
              ? <span className="text-sm text-[var(--muted)]">{t("common.loading")}</span>
              : backups.length === 0
                ? <span className="text-sm text-[var(--muted)]">{t("backups.empty")}</span>
                : backups.map((backup) => (
                    <BackupRow
                      backup={backup}
                      key={backup.backupId}
                      onSelect={canRestore ? () => form.setValue("backupId", backup.backupId, { shouldValidate: true }) : undefined}
                      selected={canRestore && selectedBackupId === backup.backupId}
                    />
                  ))}
          </div>
          {!canRestore && !canPrune ? <p className="mt-4 text-xs text-[var(--muted)]">{t("backups.readOnly")}</p> : null}
        </Card>
        {canRestore || canPrune ? (
          <div className="grid content-start gap-4">
            {canRestore ? (
              <Card>
                <form className="grid gap-4" onSubmit={form.handleSubmit((values) => prepare(values, prepareButton.current))}>
                  {(["restoreConfig", "restoreDatabase", "restoreSessions"] as const).map((name) => (
                    <label className="flex items-center gap-3 text-sm" key={name}>
                      <input className="h-4 w-4 accent-[var(--accent)]" type="checkbox" {...form.register(name)} />
                      {t(`backups.${name}`)}
                    </label>
                  ))}
                  <label className="flex items-center gap-3 text-sm"><input className="h-4 w-4 accent-[var(--accent)]" type="checkbox" {...form.register("allowSqliteHomeRelocation")} />{t("backups.relocation")}</label>
                  {relocation ? (
                    <Field error={form.formState.errors.relocationTargetProfileId ? t("validation.restore") : undefined} label={t("backups.targetProfile")}>
                      <select className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" {...form.register("relocationTargetProfileId")}>
                        <option value="">—</option>
                        {profiles.filter((entry) => (
                          entry.id !== profile.id
                            && (Boolean(entry.sqliteHome) || entry.sqliteHomeConfigured === true)
                        )).map((entry) => <option key={entry.id} value={entry.id}>{entry.name}</option>)}
                      </select>
                    </Field>
                  ) : null}
                  {form.formState.errors.restoreSessions ? <span className="text-xs text-[var(--danger)]" role="alert">{t("validation.restore")}</span> : null}
                  <Button disabled={disabled || !selectedBackupId} ref={prepareButton} type="submit"><ArchiveRestore size={17} />{t("backups.prepare")}</Button>
                </form>
              </Card>
            ) : null}
            {canPrune ? (
              <Card>
                <Field label={t("backups.pruneKeep")}><Input max={1000} min={0} onChange={(event) => setKeepCount(Number(event.target.value))} type="number" value={keepCount} /></Field>
                <Button className="mt-4 w-full" disabled={disabled || !Number.isInteger(keepCount) || keepCount < 0} onClick={() => prune(keepCount)} type="button" variant="secondary">{t("backups.prune")}</Button>
              </Card>
            ) : null}
          </div>
        ) : null}
      </div>
    </Fragment>
  );
}
