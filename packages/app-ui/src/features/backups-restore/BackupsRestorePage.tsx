import type { ManagedBackup } from "@codex-provider-sync/contracts";
import { zodResolver } from "@hookform/resolvers/zod";
import { ArchiveRestore, RefreshCw } from "lucide-react";
import { Fragment, useEffect, useRef, useState } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { z } from "zod";

import { DEFAULT_BACKUP_RETENTION_COUNT, keepCountSchema, restoreSchema } from "../../schemas.js";
import { displayProfileName, formatBytes, formatDate, PageHeading, safeErrorText } from "../../shared/presentation.js";
import type { HostProfile } from "../../types.js";
import { Badge, Button, Card, Dialog, Field, Input, cn } from "../../ui.js";

export type RestoreValues = z.infer<typeof restoreSchema>;

function availableTargets(backup: ManagedBackup | undefined) {
  const kinds = backup?.metadata.capturedTargetKinds;
  const legacy = Boolean(backup) && kinds === undefined;
  const captured = kinds && typeof kinds === "object" && !Array.isArray(kinds) ? kinds : {};
  return {
    restoreConfig: legacy || captured.config === true || captured.globalState === true,
    restoreDatabase: legacy || captured.sqlite === true,
    restoreSessions: legacy || captured.rollout === true
  };
}

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
  refreshing = false,
  error,
  refresh,
  disabled,
  canRestore,
  canPrune,
  initialBackupId,
  retentionCount = DEFAULT_BACKUP_RETENTION_COUNT,
  saveRetention,
  prepare,
  prune
}: {
  profile: HostProfile;
  profiles: HostProfile[];
  backups: ManagedBackup[];
  loading: boolean;
  refreshing?: boolean;
  error?: unknown;
  refresh?(): void;
  disabled: boolean;
  canRestore: boolean;
  canPrune: boolean;
  /** A completed write can direct the user to this backup's restore preview. */
  initialBackupId?: string;
  retentionCount?: number;
  saveRetention?(count: number): Promise<"saved" | "watch-active">;
  prepare(values: RestoreValues, trigger: HTMLButtonElement | null): Promise<void>;
  prune(keepCount: number): void;
}) {
  const { t } = useTranslation();
  const form = useForm<RestoreValues>({
    resolver: zodResolver(restoreSchema),
    defaultValues: {
      backupId: "",
      restoreConfig: false,
      restoreDatabase: false,
      restoreSessions: false,
      allowSqliteHomeRelocation: false,
      relocationTargetProfileId: ""
    }
  });
  const relocation = form.watch("allowSqliteHomeRelocation");
  const restoreDatabase = form.watch("restoreDatabase");
  const keepCount = retentionCount;
  const [retentionDraft, setRetentionDraft] = useState(String(retentionCount));
  const [savingRetention, setSavingRetention] = useState(false);
  const [retentionNotice, setRetentionNotice] = useState<"saved" | "watch-active" | "failed" | null>(null);
  const draftCount = /^\d+$/.test(retentionDraft) ? Number(retentionDraft) : NaN;
  const retentionValid = keepCountSchema.safeParse(draftCount).success;
  const retentionDirty = retentionDraft !== String(retentionCount);
  useEffect(() => { setRetentionDraft(String(retentionCount)); }, [retentionCount]);
  const [pruneConfirmation, setPruneConfirmation] = useState<{ keep: number; revision: string } | null>(null);
  const pruneButton = useRef<HTMLButtonElement>(null);
  const pruneTrigger = useRef<HTMLButtonElement | null>(null);
  const pruneRevision = JSON.stringify([profile.id, profile.revision, keepCount, backups.map(({ backupId, createdAt, sizeBytes }) => [backupId, createdAt, sizeBytes]).sort((a, b) => String(a[0]).localeCompare(String(b[0])))]);
  const keepValid = Number.isInteger(keepCount) && keepCount >= 0 && keepCount <= 1000;
  const estimate = (keep: number) => t("ux.pruneEstimate", { remove: Math.max(0, backups.length - keep), keep: Math.min(backups.length, keep) });
  const prepareButton = useRef<HTMLButtonElement>(null);
  const selectedBackupId = form.watch("backupId");
  const selectedBackup = backups.find((backup) => backup.backupId === selectedBackupId);
  const available = availableTargets(selectedBackup);
  const unavailable = disabled || loading || refreshing || Boolean(error);
  useEffect(() => {
    form.setValue("restoreConfig", available.restoreConfig && !form.getValues("allowSqliteHomeRelocation"));
    form.setValue("restoreDatabase", available.restoreDatabase);
    form.setValue("restoreSessions", available.restoreSessions);
    form.clearErrors();
  }, [form, selectedBackupId, available.restoreConfig, available.restoreDatabase, available.restoreSessions]);
  useEffect(() => {
    if (relocation) form.setValue("restoreConfig", false);
    form.clearErrors("relocationTargetProfileId");
  }, [form, relocation]);
  useEffect(() => {
    if (!restoreDatabase) form.setValue("allowSqliteHomeRelocation", false);
  }, [form, restoreDatabase]);
  useEffect(() => {
    if (initialBackupId) form.setValue("backupId", initialBackupId, { shouldValidate: true });
  }, [form, initialBackupId]);
  return (
    <Fragment>
      <PageHeading title={t("backups.title")} subtitle={t("backups.subtitle")} action={refresh ? <Button disabled={loading || refreshing} onClick={refresh} type="button" variant="secondary"><RefreshCw size={16} />{t("common.refresh")}</Button> : undefined} />
      {canPrune ? <Card className="mb-4">
        <h2 className="font-semibold">{t("backupPolicy.title")}</h2>
        <p className="mt-2 text-sm text-[var(--muted)]">{t("backupPolicy.scope")}</p>
        <p className="mt-2 text-sm">{t("backupPolicy.current", { count: keepCount })}</p>
        {saveRetention ? <form className="mt-3 flex flex-wrap items-end gap-3" onSubmit={async (event) => {
          event.preventDefault();
          if (!retentionValid || !retentionDirty || unavailable || savingRetention) return;
          setSavingRetention(true);
          setRetentionNotice(null);
          try { setRetentionNotice(await saveRetention(draftCount)); }
          catch { setRetentionNotice("failed"); }
          finally { setSavingRetention(false); }
        }}>
          <Field label={t("backupPolicy.count")} error={!retentionValid ? t("validation.keep") : undefined}><Input min={1} max={1000} type="number" value={retentionDraft} disabled={unavailable || savingRetention} onChange={(event) => { setRetentionDraft(event.target.value); setRetentionNotice(null); }} /></Field>
          <Button type="submit" disabled={!retentionDirty || !retentionValid || unavailable || savingRetention}>{t(savingRetention ? "common.loading" : "backupPolicy.save")}</Button>
        </form> : null}
        <p className="mt-3 text-xs text-[var(--muted)]">{t("backupPolicy.hint")}</p>
        {retentionNotice ? <p className="mt-2 text-sm" role={retentionNotice === "saved" ? "status" : "alert"}>{t(`backupPolicy.${retentionNotice}`)}</p> : null}
      </Card> : null}
      {initialBackupId && !loading && !error && !backups.some((backup) => backup.backupId === initialBackupId) ? <p className="mb-4 text-sm text-[var(--warning)]" role="alert">{t("backups.requestedMissing")}</p> : null}
      <div className={cn("grid gap-4", (canRestore || canPrune) && "xl:grid-cols-[minmax(0,1fr)_minmax(320px,440px)]")}>
        <Card>
          <div className="grid gap-3">
            {loading
              ? <span className="text-sm text-[var(--muted)]">{t("common.loading")}</span>
              : error
                ? <div className="grid justify-items-start gap-3"><p className="text-sm text-[var(--danger)]" role="alert">{t("backups.loadFailed")} {safeErrorText(error, t)}</p>{refresh ? <Button disabled={refreshing} onClick={refresh} type="button" variant="secondary">{t("common.retry")}</Button> : null}</div>
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
                <form onSubmit={form.handleSubmit((values) => prepare(values, prepareButton.current))}>
                  <fieldset className="grid gap-4" disabled={unavailable || form.formState.isSubmitting || !selectedBackup}>
                  <p className="text-xs text-[var(--muted)]">{t(selectedBackup ? "backups.capturedHint" : "backups.selectBackup")}</p>
                  {(["restoreConfig", "restoreDatabase", "restoreSessions"] as const).map((name) => (
                    <label className="flex items-center gap-3 text-sm" key={name}>
                      <input className="h-4 w-4 accent-[var(--accent)]" type="checkbox" disabled={!available[name] || (name === "restoreConfig" && relocation)} {...form.register(name)} />
                      {t(`backups.${name}`)}
                    </label>
                  ))}
                  <label className="flex items-center gap-3 text-sm"><input className="h-4 w-4 accent-[var(--accent)]" type="checkbox" disabled={!restoreDatabase} {...form.register("allowSqliteHomeRelocation")} />{t("backups.relocation")}</label>
                  {relocation ? (
                    <Field error={form.formState.errors.relocationTargetProfileId ? t("backups.relocationTargetRequired") : undefined} label={t("backups.targetProfile")}>
                      <select aria-label={t("backups.targetProfile")} aria-invalid={Boolean(form.formState.errors.relocationTargetProfileId)} className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" {...form.register("relocationTargetProfileId")}>
                        <option value="">—</option>
                        {profiles.filter((entry) => (
                          entry.id !== profile.id
                            && (Boolean(entry.sqliteHome) || entry.sqliteHomeConfigured === true)
                        )).map((entry) => <option key={entry.id} value={entry.id}>{displayProfileName(entry, t)}</option>)}
                      </select>
                    </Field>
                  ) : null}
                  {relocation ? <p className="text-xs text-[var(--muted)]">{t("backups.relocationHint")}</p> : null}
                  {form.formState.errors.restoreSessions ? <span className="text-xs text-[var(--danger)]" role="alert">{t("validation.restore")}</span> : null}
                  <Button ref={prepareButton} type="submit"><ArchiveRestore size={17} />{t("backups.prepare")}</Button>
                  </fieldset>
                </form>
              </Card>
            ) : null}
            {canPrune ? (
              <Card>
                <p className="text-sm font-medium">{t("backupPolicy.current", { count: keepCount })}</p>
                {!unavailable && keepValid ? <p className="mt-3 text-sm">{estimate(keepCount)}</p> : null}
                <p className="mt-2 text-xs text-[var(--muted)]">{t("ux.pruneCaution")}</p>
                <Button ref={pruneButton} className="mt-4 w-full" disabled={unavailable || !keepValid || retentionDirty || savingRetention} onClick={(event) => { pruneTrigger.current = event.currentTarget; setPruneConfirmation({ keep: keepCount, revision: pruneRevision }); }} type="button" variant="secondary">{t("backups.prune")}</Button>
                <details className="mt-3 text-sm"><summary className="cursor-pointer">{t("common.advanced")}</summary><Button className="mt-3" disabled={unavailable || retentionDirty || savingRetention} onClick={(event) => { pruneTrigger.current = event.currentTarget; setPruneConfirmation({ keep: 0, revision: pruneRevision }); }} type="button" variant="danger">{t("backupPolicy.clear")}</Button></details>
              </Card>
            ) : null}
          </div>
        ) : null}
      </div>
      <Dialog open={Boolean(pruneConfirmation)} onOpenChange={(open) => { if (!open) setPruneConfirmation(null); }} closeLabel={t("common.close")} title={t("ux.pruneTitle")} description={t("ux.pruneCaution")} restoreFocus={() => (pruneTrigger.current ?? pruneButton.current)?.focus()} footer={<Button type="button" variant="danger" disabled={!canPrune || unavailable || !pruneConfirmation || pruneConfirmation.revision !== pruneRevision} onClick={() => {
        if (!canPrune || !pruneConfirmation || unavailable || pruneConfirmation.revision !== pruneRevision) return;
        const keep = pruneConfirmation.keep;
        setPruneConfirmation(null);
        prune(keep);
      }}>{t("ux.pruneConfirm")}</Button>}>
        {pruneConfirmation ? <div className="mt-4 grid gap-3 text-sm"><p>{estimate(pruneConfirmation.keep)}</p>{pruneConfirmation.keep === 0 ? <p className="text-[var(--danger)]">{t("ux.pruneZero")}</p> : null}{pruneConfirmation.revision !== pruneRevision ? <p role="alert">{t("ux.pruneChanged")}</p> : null}</div> : null}
      </Dialog>
    </Fragment>
  );
}
