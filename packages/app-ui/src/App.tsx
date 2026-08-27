import { zodResolver } from "@hookform/resolvers/zod";
import {
  QueryClient,
  QueryClientProvider,
  useIsMutating,
  useMutation,
  useQuery,
  useQueryClient
} from "@tanstack/react-query";
import type {
  DiagnosticsSnapshot,
  HistorySessionDetail,
  ManagedBackup,
  OperationResult,
  PlanSummary,
  ProgressEvent,
  ProfileSelector,
  StatusSnapshot,
  SwitchModelMode,
  WatchSnapshot,
  WatchStatusList
} from "@codex-provider-sync/contracts";
import { CoreClientError } from "@codex-provider-sync/core-client";
import { I18nextProvider, useTranslation } from "react-i18next";
import {
  Activity,
  AlertTriangle,
  ArchiveRestore,
  CheckCircle2,
  Database,
  FileClock,
  FolderCog,
  Gauge,
  Globe2,
  History,
  Languages,
  Moon,
  Play,
  RefreshCw,
  RotateCcw,
  Settings,
  ShieldAlert,
  Sun,
  Workflow
} from "lucide-react";
import {
  Component,
  Fragment,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ErrorInfo,
  type ReactNode
} from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";

import { APP_ROUTES, type AppRoute } from "./routes.js";
import { createAppI18n } from "./i18n.js";
import { profileSchema, restoreSchema, switchSchema, syncSchema } from "./schemas.js";
import {
  FULL_APP_UI_CAPABILITIES,
  type AppUiCapabilities,
  type AppUiProps,
  type HostProfile,
  type HostUpdateStatus
} from "./types.js";
import { Badge, Button, Card, Dialog, Field, Input, ToastProvider, cn, useToast } from "./ui.js";

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

type SyncValues = z.infer<typeof syncSchema>;
type SwitchValues = z.infer<typeof switchSchema>;
type RestoreValues = z.infer<typeof restoreSchema>;
type ProfileValues = z.infer<typeof profileSchema>;

function profileSelector(profile: HostProfile): ProfileSelector {
  return { profileId: profile.id, profileRevision: profile.revision };
}

function formatBytes(bytes: number): string {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = Number.isFinite(bytes) ? bytes : 0;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return unit === 0 ? `${value} ${units[unit]}` : `${value.toFixed(value >= 10 ? 1 : 2)} ${units[unit]}`;
}

function formatDate(value?: string | null, locale = "en"): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) return "—";
  return new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeStyle: "medium" }).format(date);
}

function safeErrorText(error: unknown, fallback: string): string {
  if (error instanceof CoreClientError) return `${error.dto.message} (${error.code})`;
  return fallback;
}

function PageHeading({ title, subtitle, action }: { title: string; subtitle: string; action?: ReactNode }) {
  return (
    <div className="mb-6 flex flex-wrap items-start justify-between gap-4">
      <div>
        <h1 className="text-2xl font-bold tracking-tight text-[var(--text)]">{title}</h1>
        <p className="mt-1 max-w-3xl text-sm leading-6 text-[var(--muted)]">{subtitle}</p>
      </div>
      {action}
    </div>
  );
}

function KeyValue({ label, value, mono = false }: { label: string; value: ReactNode; mono?: boolean }) {
  return (
    <div className="grid gap-1 border-b border-[var(--border)] py-3 last:border-0 sm:grid-cols-[180px_1fr]">
      <dt className="text-sm text-[var(--muted)]">{label}</dt>
      <dd className={cn("min-w-0 break-words text-sm font-medium text-[var(--text)]", mono && "font-mono text-xs")}>{value}</dd>
    </div>
  );
}

function Distribution({ title, counts, current }: { title: string; counts: unknown; current: string }) {
  const record = counts && typeof counts === "object" && !Array.isArray(counts) ? counts as Record<string, unknown> : {};
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
    <Card>
      <div className="mb-4 flex items-center justify-between"><h2 className="font-semibold">{title}</h2><Badge>{total}</Badge></div>
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

function OverviewPage({ status, loading, refresh }: { status?: StatusSnapshot; loading: boolean; refresh(): void }) {
  const { t, i18n } = useTranslation();
  const alignment = status?.alignment && typeof status.alignment === "object"
    ? (status.alignment as Record<string, unknown>).aligned === true
    : false;
  return (
    <Fragment>
      <PageHeading
        title={t("overview.title")}
        subtitle={t("overview.subtitle")}
        action={<Button disabled={loading} onClick={refresh} type="button" variant="secondary"><RefreshCw className={cn(loading && "animate-spin")} size={16} />{t("common.refresh")}</Button>}
      />
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <Card><div className="text-sm text-[var(--muted)]">{t("common.provider")}</div><div className="mt-2 text-xl font-bold">{status?.currentProvider ?? "—"}</div></Card>
        <Card><div className="text-sm text-[var(--muted)]">{t("overview.alignment")}</div><div className="mt-2 flex items-center gap-2 text-lg font-bold">{alignment ? <CheckCircle2 className="text-[var(--success)]" size={20} /> : <AlertTriangle className="text-[var(--warning)]" size={20} />}{alignment ? t("overview.aligned") : t("overview.notAligned")}</div></Card>
        <Card><div className="text-sm text-[var(--muted)]">{t("overview.backupCount")}</div><div className="mt-2 text-xl font-bold">{status?.backupSummary.count ?? 0}</div><div className="text-xs text-[var(--muted)]">{formatBytes(status?.backupSummary.totalBytes ?? 0)}</div></Card>
        <Card><div className="text-sm text-[var(--muted)]">{t("overview.locked")}</div><div className="mt-2 text-xl font-bold">{status?.lockedRolloutFiles.length ?? 0}</div></Card>
      </div>
      <Card className="mt-4"><dl><KeyValue label={t("overview.codexHomeSource")} value={status?.codexHomeSource ?? "—"} /><KeyValue label={t("overview.sqliteHomeSource")} value={status?.sqliteHomeSource ?? "—"} /><KeyValue label={t("overview.snapshot")} value={formatDate(status?.snapshotAt, i18n.language)} /></dl></Card>
      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <Distribution counts={status?.rolloutCounts} current={status?.currentProvider ?? ""} title={t("overview.rollout")} />
        <Distribution counts={status?.sqliteCounts} current={status?.currentProvider ?? ""} title={t("overview.sqlite")} />
      </div>
    </Fragment>
  );
}

function SyncPage({ disabled, prepare }: { disabled: boolean; prepare(values: SyncValues, trigger: HTMLButtonElement | null): Promise<void> }) {
  const { t } = useTranslation();
  const form = useForm<SyncValues>({ resolver: zodResolver(syncSchema), defaultValues: { keepCount: 5 } });
  const prepareButton = useRef<HTMLButtonElement>(null);
  return (
    <Fragment>
      <PageHeading title={t("sync.title")} subtitle={t("sync.subtitle")} />
      <Card className="max-w-2xl">
        <form className="grid gap-5" onSubmit={form.handleSubmit((values) => prepare(values, prepareButton.current))}>
          <Field error={form.formState.errors.keepCount ? t("validation.keep") : undefined} label={t("sync.keep")}>
            <Input max={1000} min={0} type="number" {...form.register("keepCount", { valueAsNumber: true })} />
          </Field>
          <Button disabled={disabled || form.formState.isSubmitting} ref={prepareButton} type="submit"><Workflow size={17} />{t("sync.prepare")}</Button>
        </form>
      </Card>
    </Fragment>
  );
}

function SwitchPage({ disabled, providers, prepare }: { disabled: boolean; providers: string[]; prepare(values: SwitchValues, trigger: HTMLButtonElement | null): Promise<void> }) {
  const { t } = useTranslation();
  const prepareButton = useRef<HTMLButtonElement>(null);
  const form = useForm<SwitchValues>({
    resolver: zodResolver(switchSchema),
    defaultValues: { provider: providers[0] ?? "openai", modelMode: "provider-default", model: "", keepCount: 5 }
  });
  const modelMode = form.watch("modelMode");
  useEffect(() => { if (modelMode !== "explicit") form.setValue("model", ""); }, [form, modelMode]);
  return (
    <Fragment>
      <PageHeading title={t("switchPage.title")} subtitle={t("switchPage.subtitle")} />
      <Card className="max-w-2xl">
        <form className="grid gap-5" onSubmit={form.handleSubmit((values) => prepare(values, prepareButton.current))}>
          <Field error={form.formState.errors.provider ? t("validation.provider") : undefined} label={t("switchPage.provider")}>
            <Input list="configured-providers" {...form.register("provider")} />
          </Field>
          <datalist id="configured-providers">{providers.map((provider) => <option key={provider} value={provider} />)}</datalist>
          <Field label={t("switchPage.modelMode")}>
            <select className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" {...form.register("modelMode")}>
              <option value="provider-default">{t("switchPage.providerDefault")}</option>
              <option value="keep-root-model">{t("switchPage.keepModel")}</option>
              <option value="explicit">{t("switchPage.explicitModel")}</option>
            </select>
          </Field>
          {modelMode === "explicit" ? <Field error={form.formState.errors.model ? t("validation.model") : undefined} label={t("switchPage.model")}><Input {...form.register("model")} /></Field> : null}
          <Field error={form.formState.errors.keepCount ? t("validation.keep") : undefined} label={t("sync.keep")}><Input max={1000} min={0} type="number" {...form.register("keepCount", { valueAsNumber: true })} /></Field>
          <Button disabled={disabled || form.formState.isSubmitting} ref={prepareButton} type="submit"><RotateCcw size={17} />{t("switchPage.prepare")}</Button>
        </form>
      </Card>
    </Fragment>
  );
}

function BackupRow({ backup, selected, onSelect }: { backup: ManagedBackup; selected: boolean; onSelect?: () => void }) {
  const content = <Fragment><div className="flex flex-wrap items-center justify-between gap-2"><span className="font-mono text-xs font-semibold">{backup.backupId}</span><Badge>{formatBytes(backup.sizeBytes)}</Badge></div>{backup.createdAt ? <div className="mt-2 text-xs text-[var(--muted)]">{formatDate(backup.createdAt)}</div> : null}</Fragment>;
  const className = cn("w-full rounded-lg border p-4 text-left", selected ? "border-[var(--accent)] bg-[var(--accent-soft)]" : "border-[var(--border)]");
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

function BackupsPage({
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
  prune(keepCount: number): Promise<void>;
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
  return (
    <Fragment>
      <PageHeading title={t("backups.title")} subtitle={t("backups.subtitle")} />
      <div className={cn("grid gap-4", (canRestore || canPrune) && "xl:grid-cols-[minmax(0,1fr)_minmax(320px,440px)]")}>
        <Card>
          <div className="grid gap-3">
            {loading ? <span className="text-sm text-[var(--muted)]">{t("common.loading")}</span> : backups.length === 0 ? <span className="text-sm text-[var(--muted)]">{t("backups.empty")}</span> : backups.map((backup) => <BackupRow backup={backup} key={backup.backupId} onSelect={canRestore ? () => form.setValue("backupId", backup.backupId, { shouldValidate: true }) : undefined} selected={canRestore && form.watch("backupId") === backup.backupId} />)}
          </div>
          {!canRestore && !canPrune ? <p className="mt-4 text-xs text-[var(--muted)]">{t("backups.readOnly")}</p> : null}
        </Card>
        {canRestore || canPrune ? <div className="grid content-start gap-4">
          {canRestore ? <Card>
            <form className="grid gap-4" onSubmit={form.handleSubmit((values) => prepare(values, prepareButton.current))}>
              {["restoreConfig", "restoreDatabase", "restoreSessions"].map((name) => (
                <label className="flex items-center gap-3 text-sm" key={name}>
                  <input className="h-4 w-4 accent-[var(--accent)]" type="checkbox" {...form.register(name as "restoreConfig" | "restoreDatabase" | "restoreSessions")} />
                  {t(`backups.${name}`)}
                </label>
              ))}
              <label className="flex items-center gap-3 text-sm"><input className="h-4 w-4 accent-[var(--accent)]" type="checkbox" {...form.register("allowSqliteHomeRelocation")} />{t("backups.relocation")}</label>
              {relocation ? <Field error={form.formState.errors.relocationTargetProfileId ? t("validation.restore") : undefined} label={t("backups.targetProfile")}><select className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" {...form.register("relocationTargetProfileId")}><option value="">—</option>{profiles.filter((entry) => entry.id !== profile.id && entry.sqliteHome).map((entry) => <option key={entry.id} value={entry.id}>{entry.name}</option>)}</select></Field> : null}
              {form.formState.errors.restoreSessions ? <span className="text-xs text-[var(--danger)]" role="alert">{t("validation.restore")}</span> : null}
              <Button disabled={disabled || !form.watch("backupId")} ref={prepareButton} type="submit"><ArchiveRestore size={17} />{t("backups.prepare")}</Button>
            </form>
          </Card> : null}
          {canPrune ? <Card>
            <Field label={t("backups.pruneKeep")}><Input max={1000} min={0} onChange={(event) => setKeepCount(Number(event.target.value))} type="number" value={keepCount} /></Field>
            <Button className="mt-4 w-full" disabled={disabled || !Number.isInteger(keepCount) || keepCount < 0} onClick={() => void prune(keepCount)} type="button" variant="secondary">{t("backups.prune")}</Button>
          </Card> : null}
        </div> : null}
      </div>
    </Fragment>
  );
}

function HistoryPage({ core, profile }: { core: AppUiProps["core"]; profile: HostProfile }) {
  const { t, i18n } = useTranslation();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<HistorySessionDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const list = useQuery({
    queryKey: ["history", profile.id, profile.revision],
    queryFn: ({ signal }) => core.listHistory({ profile: profileSelector(profile), page: 1, pageSize: 100 }, { signal }),
    gcTime: 0,
    staleTime: 0
  });
  useEffect(() => {
    if (!selectedId) {
      setDetail(null);
      setDetailError(null);
      return;
    }
    const controller = new AbortController();
    setDetail(null);
    setDetailError(null);
    setDetailLoading(true);
    void core.getHistorySession({ profile: profileSelector(profile), sessionId: selectedId, messageLimit: 200 }, { signal: controller.signal })
      .then((value) => { if (!controller.signal.aborted) setDetail(value); })
      .catch((error: unknown) => { if (!controller.signal.aborted) setDetailError(safeErrorText(error, t("global.failed"))); })
      .finally(() => { if (!controller.signal.aborted) setDetailLoading(false); });
    return () => {
      controller.abort();
      setDetail(null);
    };
  }, [core, profile, selectedId, t]);
  if (selectedId) {
    return (
      <Fragment>
        <PageHeading title={detail ? (detail.session.title || t("history.untitled")) : t("history.title")} subtitle={t("history.subtitle")} action={<Button onClick={() => setSelectedId(null)} type="button" variant="secondary">{t("history.back")}</Button>} />
        <Card>
          {detailLoading ? <span>{t("common.loading")}</span> : detailError ? <span className="text-[var(--danger)]" role="alert">{detailError}</span> : detail ? <div className="grid gap-4">{detail.messages.map((message) => <article className="rounded-lg border border-[var(--border)] bg-[var(--surface)] p-4" key={`${message.sequence}-${message.role}`}><div className="mb-2 flex justify-between text-xs font-semibold uppercase text-[var(--muted)]"><span>{message.role}</span><span>{formatDate(message.timestamp, i18n.language)}</span></div><pre className="whitespace-pre-wrap break-words font-sans text-sm leading-6">{message.text}</pre></article>)}</div> : null}
        </Card>
      </Fragment>
    );
  }
  return (
    <Fragment>
      <PageHeading title={t("history.title")} subtitle={t("history.subtitle")} />
      <Card>
        {list.isPending ? <span>{t("common.loading")}</span> : list.isError ? <span className="text-[var(--danger)]" role="alert">{safeErrorText(list.error, t("global.failed"))}</span> : list.data.sessions.length === 0 ? <span className="text-[var(--muted)]">{t("history.empty")}</span> : <div className="divide-y divide-[var(--border)]">{list.data.sessions.map((session) => <div className="flex flex-wrap items-center justify-between gap-4 py-4" key={session.id}><div className="min-w-0"><div className="truncate font-semibold">{session.title || t("history.untitled")}</div><div className="mt-1 flex flex-wrap gap-2 text-xs text-[var(--muted)]"><span>{session.provider}</span><span>{session.messageCount} {t("history.messages")}</span><span>{formatDate(session.updatedAt, i18n.language)}</span>{session.archived ? <Badge>{t("history.archived")}</Badge> : null}</div></div><Button onClick={() => setSelectedId(session.id)} type="button" variant="secondary">{t("history.open")}</Button></div>)}</div>}
      </Card>
    </Fragment>
  );
}

function ProfilesPage({
  profiles,
  refresh,
  host,
  canManage,
  revealPaths
}: {
  profiles: HostProfile[];
  refresh(): Promise<unknown>;
  host: AppUiProps["host"];
  canManage: boolean;
  revealPaths: boolean;
}) {
  const { t } = useTranslation();
  const toast = useToast();
  const [editing, setEditing] = useState<HostProfile | null>(null);
  const form = useForm<ProfileValues>({ resolver: zodResolver(profileSchema), defaultValues: { profileId: "", name: "", codexHome: "", sqliteHome: "" } });
  useEffect(() => {
    form.reset(editing ? { profileId: editing.id, name: editing.name, codexHome: editing.codexHome ?? "", sqliteHome: editing.sqliteHome ?? "" } : { profileId: "", name: "", codexHome: "", sqliteHome: "" });
  }, [editing, form]);
  const save = useMutation({
    mutationFn: async (values: ProfileValues) => {
      if (!canManage || !host.saveProfile) throw new Error("Profile management is unavailable.");
      return host.saveProfile({ ...values, ...(editing ? { profileRevision: editing.revision } : {}) });
    },
    onSuccess: async () => { await refresh(); setEditing(null); form.reset(); toast.push({ title: t("common.save"), tone: "success" }); },
    onError: (error) => toast.push({ title: t("global.failed"), description: safeErrorText(error, t("global.unexpected")), tone: "danger" })
  });
  const remove = useMutation({
    mutationFn: (profile: HostProfile) => {
      if (!canManage || !host.deleteProfile) throw new Error("Profile management is unavailable.");
      return host.deleteProfile(profile.id, profile.revision);
    },
    onSuccess: async () => { await refresh(); setEditing(null); toast.push({ title: t("common.delete"), tone: "success" }); },
    onError: (error) => toast.push({ title: t("global.failed"), description: safeErrorText(error, t("global.unexpected")), tone: "danger" })
  });
  return (
    <Fragment>
      <PageHeading title={t("profiles.title")} subtitle={t("profiles.subtitle")} />
      <div className={cn("grid gap-4", canManage && "xl:grid-cols-[minmax(0,1fr)_420px]")}>
        <Card><div className="grid gap-3">{profiles.map((profile) => {
          const content = <Fragment><div className="flex justify-between"><span className="font-semibold">{profile.name}</span>{profile.id === "default" ? <Badge>{t("common.current")}</Badge> : null}</div><div className="mt-2 font-mono text-xs text-[var(--muted)]">{profile.id}</div>{revealPaths && profile.codexHome ? <div className="mt-1 truncate font-mono text-xs text-[var(--muted)]">{profile.codexHome}</div> : <div className="mt-1 text-xs text-[var(--muted)]">{t("profiles.pathManaged")}</div>}</Fragment>;
          if (!canManage || profile.id === "default") return <div className="rounded-lg border border-[var(--border)] p-4 text-left" key={profile.id}>{content}</div>;
          return <button className={cn("rounded-lg border p-4 text-left", editing?.id === profile.id ? "border-[var(--accent)] bg-[var(--accent-soft)]" : "border-[var(--border)] hover:bg-[var(--surface-hover)]")} key={profile.id} onClick={() => setEditing(profile)} type="button">{content}</button>;
        })}</div>{!canManage ? <p className="mt-4 text-xs text-[var(--muted)]">{t("profiles.readOnly")}</p> : null}</Card>
        {canManage ? <Card>
          <form className="grid gap-4" onSubmit={form.handleSubmit((values) => save.mutateAsync(values))}>
            <Field error={form.formState.errors.profileId ? t("validation.profileId") : undefined} label={t("profiles.id")}><Input disabled={Boolean(editing)} {...form.register("profileId")} /></Field>
            <Field error={form.formState.errors.name ? t("validation.required") : undefined} label={t("profiles.name")}><Input {...form.register("name")} /></Field>
            <Field error={form.formState.errors.codexHome ? t("validation.path") : undefined} label={t("profiles.codexHome")}><Input {...form.register("codexHome")} /></Field>
            <Field error={form.formState.errors.sqliteHome ? t("validation.path") : undefined} label={t("profiles.sqliteHome")}><Input {...form.register("sqliteHome")} /></Field>
            <div className="flex flex-wrap gap-3"><Button disabled={save.isPending} type="submit">{editing ? t("profiles.update") : t("profiles.create")}</Button>{editing ? <Button disabled={remove.isPending} onClick={() => remove.mutate(editing)} type="button" variant="danger">{t("common.delete")}</Button> : null}</div>
          </form>
          <p className="mt-4 text-xs text-[var(--muted)]">{t("profiles.defaultManaged")}</p>
        </Card> : null}
      </div>
    </Fragment>
  );
}

function DiagnosticsPage({
  diagnostics,
  loading,
  exporting,
  canExport,
  refresh,
  exportBundle
}: {
  diagnostics?: DiagnosticsSnapshot;
  loading: boolean;
  exporting: boolean;
  canExport: boolean;
  refresh(): void;
  exportBundle(): void;
}) {
  const { t } = useTranslation();
  const sections = diagnostics ? [["runtime", diagnostics.runtime], ["storage", diagnostics.storage], ["provider", diagnostics.provider], ["safety", diagnostics.safety]] as const : [];
  return (
    <Fragment>
      <PageHeading title={t("diagnostics.title")} subtitle={t("diagnostics.subtitle")} action={<div className="flex flex-wrap gap-2"><Button disabled={loading} onClick={refresh} type="button" variant="secondary"><RefreshCw size={16} />{t("common.refresh")}</Button>{canExport ? <Button disabled={exporting || !diagnostics} onClick={exportBundle} type="button"><ArchiveRestore size={16} />{exporting ? t("diagnostics.exporting") : t("diagnostics.export")}</Button> : null}</div>} />
      <div className="grid gap-4 lg:grid-cols-2">{sections.map(([key, value]) => <Card key={key}><h2 className="mb-3 font-semibold">{t(`diagnostics.${key}`)}</h2><pre className="overflow-auto whitespace-pre-wrap break-words rounded-lg bg-[var(--surface)] p-4 text-xs leading-5">{JSON.stringify(value, null, 2)}</pre></Card>)}</div>
    </Fragment>
  );
}

function activeWatch(value: WatchSnapshot | WatchStatusList | undefined): WatchSnapshot | null {
  if (!value) return null;
  if ("watches" in value) return value.watches.find((watch) => watch.status !== "stopped") ?? value.watches[0] ?? null;
  return value;
}

function SettingsPage({
  props,
  profile,
  capabilities,
  recoveryBlocked
}: {
  props: AppUiProps;
  profile: HostProfile;
  capabilities: AppUiCapabilities;
  recoveryBlocked: boolean;
}) {
  const { t, i18n } = useTranslation();
  const queryClient = useQueryClient();
  const [theme, setTheme] = useState(props.preferences.getTheme() ?? props.initialTheme);
  const watch = useQuery({ queryKey: ["watch-status"], queryFn: () => props.core.getWatchStatus({}), enabled: capabilities.watch, refetchInterval: capabilities.watch ? 3000 : false });
  const currentWatch = activeWatch(watch.data);
  const start = useMutation({ mutationFn: () => props.core.startWatch({ profile: profileSelector(profile), includeStateDb: true }), onSuccess: () => queryClient.invalidateQueries({ queryKey: ["watch-status"] }) });
  const stop = useMutation({ mutationFn: (watchId: string) => props.core.stopWatch({ watchId }), onSuccess: () => queryClient.invalidateQueries({ queryKey: ["watch-status"] }) });
  const update = useQuery({
    queryKey: ["desktop-update-status"],
    queryFn: ({ signal }) => props.host.getUpdateStatus?.(signal),
    enabled: capabilities.viewUpdateStatus && Boolean(props.host.getUpdateStatus),
    refetchInterval: capabilities.viewUpdateStatus ? 3000 : false
  });
  const storeUpdate = (value: HostUpdateStatus) => queryClient.setQueryData(
    ["desktop-update-status"],
    value
  );
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
  const setLocale = async (locale: "zh-CN" | "en") => { props.preferences.setLocale(locale); await i18n.changeLanguage(locale); };
  const applyTheme = (value: "system" | "light" | "dark") => {
    setTheme(value);
    props.preferences.setTheme(value);
    document.documentElement.dataset.theme = value;
  };
  return (
    <Fragment>
      <PageHeading title={t("settings.title")} subtitle={t("settings.subtitle")} />
      <div className="grid gap-4 lg:grid-cols-2">
        <Card><Field label={t("settings.language")}><select className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" onChange={(event) => void setLocale(event.target.value as "zh-CN" | "en")} value={i18n.language === "zh-CN" ? "zh-CN" : "en"}><option value="zh-CN">简体中文</option><option value="en">English</option></select></Field><div className="mt-3 flex items-center gap-2 text-xs text-[var(--muted)]"><Languages size={15} />{t("settings.englishFallback")}</div></Card>
        <Card><Field label={t("settings.theme")}><div className="grid grid-cols-3 gap-2">{(["system", "light", "dark"] as const).map((value) => <Button aria-pressed={theme === value} key={value} onClick={() => applyTheme(value)} type="button" variant={theme === value ? "primary" : "secondary"}>{value === "system" ? <Globe2 size={16} /> : value === "light" ? <Sun size={16} /> : <Moon size={16} />}{t(`settings.${value}`)}</Button>)}</div></Field></Card>
        {capabilities.watch ? <Card><h2 className="font-semibold">{t("settings.watch")}</h2><div className="mt-3 flex items-center justify-between gap-3"><div><Badge tone={currentWatch?.status === "running" ? "success" : "neutral"}>{currentWatch?.status ?? t("common.none")}</Badge>{currentWatch ? <div className="mt-2 font-mono text-xs text-[var(--muted)]">{currentWatch.watchId}</div> : null}</div>{currentWatch?.status === "running" ? <Button disabled={stop.isPending} onClick={() => stop.mutate(currentWatch.watchId)} type="button" variant="secondary">{t("settings.watchStop")}</Button> : <Button disabled={start.isPending || recoveryBlocked} onClick={() => start.mutate()} type="button"><Play size={16} />{t("settings.watchStart")}</Button>}</div>{recoveryBlocked && currentWatch?.status !== "running" ? <p className="mt-3 text-xs text-[var(--danger)]">{t("settings.watchRecoveryBlocked")}</p> : null}</Card> : null}
        {capabilities.viewUpdateStatus && props.host.getUpdateStatus ? <Card><h2 className="font-semibold">{t("settings.update")}</h2><div className="mt-3"><Badge tone={update.data?.state === "error" || Boolean(update.data?.installBlockedReason) ? "warning" : update.data?.state === "downloaded" ? "success" : "neutral"}>{update.isPending ? t("common.loading") : update.data ? t(`settings.updateStatus.${update.data.state}`) : t("common.unknown")}</Badge>{update.data?.version ? <p className="mt-3 text-sm">{t("settings.updateVersion", { version: update.data.version })}</p> : null}{update.data?.progressPercent !== undefined ? <p className="mt-2 text-sm text-[var(--muted)]">{t("settings.updateProgress", { percent: update.data.progressPercent })}</p> : null}{update.data?.reason ? <p className="mt-3 text-sm text-[var(--muted)]">{t(`settings.updateReason.${update.data.reason}`)}</p> : null}{update.data?.installBlockedReason ? <p className="mt-3 text-sm text-[var(--danger)]">{t(`settings.updateBlocked.${update.data.installBlockedReason}`)}</p> : null}<div className="mt-4 flex flex-wrap gap-2">{update.data && ["idle", "not-available", "error"].includes(update.data.state) && props.host.checkForUpdates ? <Button disabled={checkUpdate.isPending} onClick={() => checkUpdate.mutate()} type="button" variant="secondary">{t("settings.updateCheck")}</Button> : null}{update.data?.state === "available" && props.host.downloadUpdate ? <Button disabled={downloadUpdate.isPending} onClick={() => downloadUpdate.mutate()} type="button">{t("settings.updateDownload")}</Button> : null}{update.data?.state === "downloaded" && props.host.installUpdate ? <Button disabled={!update.data.installAllowed || installUpdate.isPending} onClick={() => installUpdate.mutate()} type="button">{t("settings.updateInstall")}</Button> : null}</div></div></Card> : null}
        {capabilities.forgetBrowser ? <Card><h2 className="font-semibold">{t("settings.forget")}</h2><p className="mt-2 text-sm text-[var(--muted)]">{t("settings.forgetHint")}</p><Button className="mt-4" onClick={() => void (props.onForgetBrowser?.() ?? props.host.forgetBrowser?.())} type="button" variant="danger">{t("settings.forget")}</Button></Card> : null}
      </div>
    </Fragment>
  );
}

function PlanReview({
  plan,
  applying,
  cancelling,
  operationId,
  progress,
  close,
  apply,
  cancel,
  restoreFocus
}: {
  plan: PlanSummary | null;
  applying: boolean;
  cancelling: boolean;
  operationId: string | null;
  progress: ProgressEvent | null;
  close(): void;
  apply(): void;
  cancel(): void;
  restoreFocus(): void;
}) {
  const { t, i18n } = useTranslation();
  return (
    <Dialog
      description={plan ? `${plan.operation} · ${formatDate(plan.expiresAt, i18n.language)}` : undefined}
      footer={<Fragment><Button disabled={applying} onClick={close} type="button" variant="secondary">{t("common.close")}</Button>{applying ? <Button disabled={cancelling} onClick={cancel} type="button" variant="danger">{cancelling ? t("plan.cancelling") : t("plan.cancelOperation")}</Button> : <Button onClick={apply} type="button">{t("common.confirm")}</Button>}</Fragment>}
      onOpenChange={(open) => { if (!open && !applying) close(); }}
      open={Boolean(plan)}
      restoreFocus={restoreFocus}
      title={t("plan.title")}
    >
      {plan ? <div className="grid gap-4"><Card><h3 className="mb-2 text-sm font-semibold">{t("plan.target")}</h3><pre className="whitespace-pre-wrap break-words text-xs">{JSON.stringify(plan.target, null, 2)}</pre></Card><Card><h3 className="mb-2 text-sm font-semibold">{t("plan.impact")}</h3><pre className="whitespace-pre-wrap break-words text-xs">{JSON.stringify(plan.impact, null, 2)}</pre></Card>{plan.warnings.length ? <div className="rounded-lg border border-[var(--warning)] bg-[var(--warning-soft)] p-4"><h3 className="font-semibold">{t("common.warnings")}</h3><ul className="mt-2 list-disc space-y-1 pl-5 text-sm">{plan.warnings.map((warning, index) => <li key={`${index}-${warning}`}>{warning}</li>)}</ul></div> : null}{applying ? <Card aria-live="polite" role="status"><h3 className="text-sm font-semibold">{t("plan.progress")}</h3><div className="mt-2 font-mono text-xs text-[var(--muted)]">{operationId ?? t("plan.starting")}</div>{progress ? <div className="mt-3 grid gap-2"><div className="text-sm">{progress.stage} · {progress.status}{progress.count === undefined ? "" : ` · ${progress.count}`}</div>{progress.progress === undefined ? null : <progress aria-label={t("plan.progress")} className="w-full" max={1} value={progress.progress} />}</div> : null}{cancelling ? <p className="mt-3 text-sm text-[var(--warning)]">{t("plan.cancelPending")}</p> : null}</Card> : null}<p className="text-sm text-[var(--muted)]">{t("plan.exactApply")}</p></div> : null}
    </Dialog>
  );
}

function AppContent({ props }: { props: AppUiProps }) {
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
  const [operationId, setOperationId] = useState<string | null>(null);
  const [operationProgress, setOperationProgress] = useState<ProgressEvent | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const applyController = useRef<AbortController | null>(null);
  const planReturnFocus = useRef<HTMLElement | null>(null);
  const mutationCount = useIsMutating();
  const profilesQuery = useQuery({ queryKey: ["profiles"], queryFn: ({ signal }) => props.host.listProfiles(signal), staleTime: 5000 });
  const profiles = profilesQuery.data ?? [];
  const profile = profiles.find((entry) => entry.id === selectedProfileId) ?? profiles[0];
  useEffect(() => {
    document.documentElement.lang = i18n.resolvedLanguage?.toLowerCase().startsWith("zh") ? "zh-CN" : "en";
  }, [i18n.resolvedLanguage]);
  useEffect(() => { if (profiles.length && !profiles.some((entry) => entry.id === selectedProfileId)) setSelectedProfileId(profiles[0].id); }, [profiles, selectedProfileId]);
  useEffect(() => {
    if (!routeIsAvailable(route, capabilities)) setRoute("overview");
  }, [capabilities, route]);
  const statusQuery = useQuery({
    queryKey: ["status", profile?.id, profile?.revision],
    queryFn: ({ signal }) => props.core.getStatus({ profile: profileSelector(profile) }, { signal }),
    enabled: Boolean(profile),
    refetchInterval: 5000
  });
  const status = statusQuery.data;
  const writeDisabled = !profile || status?.pendingRecovery === true || mutationCount > 0;
  const recoveryWriteDisabled = !profile || mutationCount > 0;
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
  const refreshAfterWrite = useCallback(async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["status"] }),
      queryClient.invalidateQueries({ queryKey: ["backups"] }),
      queryClient.invalidateQueries({ queryKey: ["history"] }),
      queryClient.invalidateQueries({ queryKey: ["diagnostics"] })
    ]);
  }, [queryClient]);
  const prepare = useCallback(async (action: () => Promise<PlanSummary>, trigger: HTMLElement | null) => {
    planReturnFocus.current = trigger;
    try { setPlan(await action()); }
    catch (error) {
      planReturnFocus.current = null;
      toast.push({ title: t("global.failed"), description: safeErrorText(error, t("global.unexpected")), tone: "danger" });
    }
  }, [t, toast]);
  const closePlan = useCallback(() => {
    const target = planReturnFocus.current;
    setPlan(null);
    setOperationId(null);
    setOperationProgress(null);
    setCancelling(false);
    globalThis.requestAnimationFrame(() => globalThis.requestAnimationFrame(() => {
      if (target?.isConnected) target.focus();
      if (planReturnFocus.current === target) planReturnFocus.current = null;
    }));
  }, []);
  const restorePlanFocus = useCallback(() => {
    const target = planReturnFocus.current;
    planReturnFocus.current = null;
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
      closePlan();
      await refreshAfterWrite();
      toast.push({ title: result.outcome === "partial" ? t("global.partial") : t("global.completed"), description: result.backup?.backupId, tone: result.outcome === "partial" ? "warning" : "success" });
    },
    onError: async (error) => {
      await refreshAfterWrite();
      closePlan();
      if (error instanceof CoreClientError && error.code === "OPERATION_CANCELLED") {
        toast.push({ title: t("global.cancelled"), tone: "warning" });
        return;
      }
      toast.push({ title: t("global.failed"), description: safeErrorText(error, t("global.unexpected")), tone: "danger" });
    }
  });
  const prune = useCallback(async (keepCount: number) => {
    if (!profile) return;
    try {
      await props.core.pruneBackups({ profile: profileSelector(profile), keepCount });
      await refreshAfterWrite();
      toast.push({ title: t("global.completed"), tone: "success" });
    } catch (error) {
      toast.push({ title: t("global.failed"), description: safeErrorText(error, t("global.unexpected")), tone: "danger" });
    }
  }, [profile, props.core, refreshAfterWrite, t, toast]);
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
  const page = !profile ? <Card>{profilesQuery.isPending ? t("common.loading") : safeErrorText(profilesQuery.error, t("global.failed"))}</Card>
    : route === "overview" ? <OverviewPage loading={statusQuery.isFetching} refresh={() => void statusQuery.refetch()} status={status} />
    : route === "sync" && capabilities.sync ? <SyncPage disabled={writeDisabled} prepare={(values, trigger) => prepare(() => props.core.prepareSync({ profile: profileSelector(profile), keepCount: values.keepCount }), trigger)} />
    : route === "switch-provider" && capabilities.switchProvider ? <SwitchPage disabled={writeDisabled} prepare={(values, trigger) => prepare(() => props.core.prepareSwitch({ profile: profileSelector(profile), provider: values.provider, modelMode: values.modelMode as SwitchModelMode, ...(values.modelMode === "explicit" ? { model: values.model } : {}), keepCount: values.keepCount }), trigger)} providers={configuredProviders} />
    : route === "backups-restore" ? <BackupsPage backups={backupsQuery.data?.backups ?? []} canPrune={capabilities.pruneBackups} canRestore={capabilities.restore} disabled={recoveryWriteDisabled} loading={backupsQuery.isPending} prepare={(values, trigger) => prepare(() => props.core.prepareRestore({ profile: profileSelector(profile), backupId: values.backupId, restoreConfig: values.restoreConfig, restoreDatabase: values.restoreDatabase, restoreSessions: values.restoreSessions, ...(values.allowSqliteHomeRelocation ? { allowSqliteHomeRelocation: true, relocationTargetProfileId: values.relocationTargetProfileId } : {}) }), trigger)} profile={profile} profiles={profiles} prune={prune} />
    : route === "history" ? <HistoryPage core={props.core} profile={profile} />
    : route === "profiles" ? <ProfilesPage canManage={capabilities.manageProfiles} host={props.host} profiles={profiles} refresh={() => profilesQuery.refetch()} revealPaths={capabilities.revealProfilePaths} />
    : route === "diagnostics" ? <DiagnosticsPage canExport={capabilities.exportDiagnostics && Boolean(props.host.exportDiagnostics)} diagnostics={diagnosticsQuery.data} exportBundle={() => exportDiagnostics.mutate()} exporting={exportDiagnostics.isPending} loading={diagnosticsQuery.isFetching} refresh={() => void diagnosticsQuery.refetch()} />
    : route === "settings" ? <SettingsPage capabilities={capabilities} profile={profile} props={props} recoveryBlocked={status?.pendingRecovery === true} />
    : <OverviewPage loading={statusQuery.isFetching} refresh={() => void statusQuery.refetch()} status={status} />;

  return (
    <div className="min-h-screen bg-[var(--surface)] text-[var(--text)]">
      <a className="sr-only focus:not-sr-only focus:fixed focus:left-4 focus:top-4 focus:z-[70] focus:rounded focus:bg-[var(--accent)] focus:px-4 focus:py-2 focus:text-white" href="#main-content" onClick={(event) => { event.preventDefault(); document.getElementById("main-content")?.focus(); }}>{t("a11y.skipToContent")}</a>
      <header className="sticky top-0 z-30 flex min-h-16 items-center justify-between gap-4 border-b border-[var(--border)] bg-[color:var(--surface-raised)/.96] px-4 backdrop-blur md:px-6">
        <div className="flex items-center gap-3"><div className="grid h-10 w-10 place-items-center rounded-xl bg-[var(--accent)] text-white"><Database size={20} /></div><div><div className="font-bold">Codex Provider Sync</div><div className="text-xs text-[var(--muted)]">{t("brandSubtitle")}</div></div></div>
        <div className="flex items-center gap-3"><select aria-label={t("a11y.profile")} className="max-w-48 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3 py-2 text-sm" disabled={mutationCount > 0} onChange={(event) => setSelectedProfileId(event.target.value)} value={profile?.id ?? ""}>{profiles.map((entry) => <option key={entry.id} value={entry.id}>{entry.name}</option>)}</select><Badge tone={mutationCount > 0 || status?.operationInProgress ? "warning" : "success"}>{mutationCount > 0 || status?.operationInProgress ? t("global.busy") : t("global.ready")}</Badge></div>
      </header>
      <div className="mx-auto grid max-w-[1600px] md:grid-cols-[240px_minmax(0,1fr)]">
        <aside className="border-b border-[var(--border)] bg-[var(--surface-raised)] p-3 md:min-h-[calc(100vh-4rem)] md:border-b-0 md:border-r">
          <nav aria-label={t("a11y.primaryNavigation")} className="grid grid-cols-2 gap-1 sm:grid-cols-4 md:grid-cols-1">{visibleNavigation.map(([id, label, Icon]) => <button aria-current={route === id ? "page" : undefined} className={cn("flex min-h-11 items-center gap-3 rounded-lg px-3 text-left text-sm font-medium text-[var(--muted)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--focus)]", route === id ? "bg-[var(--accent-soft)] text-[var(--accent-strong)]" : "hover:bg-[var(--surface-hover)] hover:text-[var(--text)]")} key={id} onClick={() => setRoute(id)} type="button"><Icon size={17} /><span>{t(label)}</span></button>)}</nav>
        </aside>
        <main className="min-w-0 p-4 md:p-8" id="main-content" tabIndex={-1}>
          {status?.pendingRecovery ? <div className="mb-5 flex items-start gap-3 rounded-xl border border-[var(--danger)] bg-[var(--danger-soft)] p-4 text-sm" role="alert"><ShieldAlert className="mt-0.5 shrink-0 text-[var(--danger)]" size={20} /><div><div className="font-semibold">RECOVERY_REQUIRED</div><div className="mt-1">{t("global.recovery")}</div></div></div> : null}
          {status?.operationInProgress ? <div className="mb-5 flex items-start gap-3 rounded-xl border border-[var(--warning)] bg-[var(--warning-soft)] p-4 text-sm" role="status"><FileClock className="mt-0.5 shrink-0 text-[var(--warning)]" size={20} /><div><div className="font-semibold">{t("global.busy")}</div><div className="mt-1 text-[var(--muted)]">{String(status.operationInProgress.operation ?? "operation")} · {String(status.operationInProgress.busyScope ?? "")}</div></div></div> : null}
          {statusQuery.isError ? <div className="mb-5 rounded-xl border border-[var(--danger)] bg-[var(--danger-soft)] p-4 text-sm text-[var(--danger)]" role="alert">{safeErrorText(statusQuery.error, t("global.failed"))}</div> : null}
          {page}
        </main>
      </div>
      {capabilities.sync || capabilities.switchProvider || capabilities.restore ? <PlanReview apply={() => { if (plan) applyMutation.mutate(plan); }} applying={applyMutation.isPending} cancel={() => { if (!applyMutation.isPending || cancelling) return; setCancelling(true); applyController.current?.abort(); }} cancelling={cancelling} close={closePlan} operationId={operationId} plan={plan} progress={operationProgress} restoreFocus={restorePlanFocus} /> : null}
    </div>
  );
}

class AppErrorBoundary extends Component<{ children: ReactNode; locale(): string }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError(): { failed: boolean } { return { failed: true }; }
  componentDidCatch(_error: Error, _info: ErrorInfo): void {}
  render(): ReactNode {
    if (!this.state.failed) return this.props.children;
    const chinese = this.props.locale().toLowerCase().startsWith("zh");
    return <div className="grid min-h-screen place-items-center bg-[var(--surface)] p-6 text-[var(--text)]"><Card className="max-w-lg text-center"><ShieldAlert className="mx-auto text-[var(--danger)]" size={40} /><h1 className="mt-4 text-xl font-bold">{chinese ? "应用错误" : "Application error"}</h1><p className="mt-2 text-sm text-[var(--muted)]">{chinese ? "页面遇到未预期错误；系统没有自动启动任何写操作。" : "The page encountered an unexpected error. No write was started automatically."}</p><Button className="mt-5" onClick={() => globalThis.location?.reload()} type="button">{chinese ? "重新加载" : "Reload"}</Button></Card></div>;
  }
}

export function AppUi(props: AppUiProps) {
  const requestedLocale = props.preferences.getLocale() ?? props.initialLocale;
  const [i18n, setI18n] = useState<Awaited<ReturnType<typeof createAppI18n>> | null>(null);
  const [queryClient] = useState(() => new QueryClient({ defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: false }, mutations: { retry: false } } }));
  useEffect(() => {
    let active = true;
    void createAppI18n(requestedLocale).then((instance) => { if (active) setI18n(instance); });
    document.documentElement.dataset.theme = props.preferences.getTheme() ?? props.initialTheme;
    return () => { active = false; queryClient.clear(); };
  }, [props.initialTheme, props.preferences, queryClient, requestedLocale]);
  if (!i18n) return <div className="grid min-h-screen place-items-center bg-[var(--surface)] text-[var(--text)]">{requestedLocale === "zh-CN" ? "正在加载…" : "Loading…"}</div>;
  return <I18nextProvider i18n={i18n}><AppErrorBoundary locale={() => i18n.language}><QueryClientProvider client={queryClient}><ToastProvider><AppContent props={props} /></ToastProvider></QueryClientProvider></AppErrorBoundary></I18nextProvider>;
}

export { APP_ROUTES };
