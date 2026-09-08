import type { HistoryPage, HistoryProjectGroup, HistorySessionSummary, ListHistoryInput } from "@codex-provider-sync/contracts";
import type { CoreClient } from "@codex-provider-sync/core-client";
import { useInfiniteQuery } from "@tanstack/react-query";
import { Archive, ChevronDown, ChevronRight, Folder, FolderOpen, GitBranch } from "lucide-react";
import { useRef, useState, type KeyboardEvent } from "react";
import { useTranslation } from "react-i18next";
import type { PreferenceStore } from "../../types.js";
import { formatDate, safeErrorText } from "../../shared/presentation.js";
import { cn } from "../../ui.js";
import { historyDisplayTitle } from "./history-title.js";
import { HistoryProjectAlias, type ProjectAliasTarget } from "./HistoryProjectAlias.js";

interface ListProps {
  core: CoreClient;
  input: ListHistoryInput;
  initialPage: HistoryPage;
  scope: string;
  preferenceScope: string;
  preferences?: PreferenceStore;
  selectedId: string | null;
  onSelect(session: HistorySessionSummary): void;
  onMenu(session: HistorySessionSummary, trigger: HTMLElement, point?: { x: number; y: number }): void;
  registerButton(id: string, button: HTMLButtonElement | null): void;
  registerGroupButton(ids: string[], button: HTMLButtonElement | null): void;
}

function menuKey(event: KeyboardEvent, open: () => void) {
  if (event.key === "ContextMenu" || (event.shiftKey && event.key === "F10")) { event.preventDefault(); open(); }
}

// Only explicitly expanded collections request more metadata. Never read messages here.
function useRows(props: ListProps, id: string, open: boolean, seed?: HistoryPage, parent = false) {
  return useInfiniteQuery({
    queryKey: ["history-rows", props.scope, parent ? "children" : "project", id],
    initialPageParam: 1,
    initialData: seed ? { pages: [seed], pageParams: [1] } : undefined,
    queryFn: ({ signal, pageParam }) => props.core.listHistory({ ...props.input, view: "projects", page: pageParam, ...(parent ? { parentId: id } : { projectId: id }) }, { signal }),
    getNextPageParam: (last) => last.hasNextPage ? last.page + 1 : undefined,
    enabled: open,
    staleTime: Infinity,
    gcTime: 0,
    retry: false,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false
  });
}

function uniqueRows(pages: HistoryPage[] | undefined) {
  return [...new Map((pages ?? []).flatMap((page) => page.sessions).map((row) => [row.id, row])).values()];
}

function MoreRows({ query }: { query: ReturnType<typeof useRows> }) {
  const { t } = useTranslation();
  return <>
    {query.isError ? <div className="px-3 py-2 text-xs text-[var(--danger)]" role="alert">{safeErrorText(query.error, t)} <button className="underline" type="button" onClick={() => void (query.data ? query.fetchNextPage() : query.refetch())}>{t("history.retryLoad")}</button></div> : null}
    {query.isFetching ? <p role="status" className="px-3 py-2 text-xs text-[var(--muted)]">{t("common.loading")}</p> : query.hasNextPage && !query.isError ? <button className="min-h-9 w-full rounded-md px-3 text-left text-xs text-[var(--muted)] hover:bg-[var(--surface-hover)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--focus)]" type="button" onClick={() => void query.fetchNextPage()}>{t("history.loadMore")}</button> : null}
  </>;
}

function SessionBranch({ session, props, fallback, depth = 0 }: { session: HistorySessionSummary; props: ListProps; fallback(): HTMLButtonElement | null; depth?: number }) {
  const { t, i18n } = useTranslation();
  const [open, setOpen] = useState(false);
  const expandButton = useRef<HTMLButtonElement>(null);
  const query = useRows(props, session.id, open, undefined, true);
  const children = uniqueRows(query.data?.pages);
  const title = historyDisplayTitle(session, t, i18n.language);
  const hasChildren = (session.childCount ?? 0) > 0;
  const hint = [title, session.provider, session.model, `${t("history.fileModifiedAt")}: ${formatDate(session.fileModifiedAt || session.updatedAt, i18n.language)}`, t("history.contextMenuHint")].filter(Boolean).join("\n");
  return <li className="min-w-0">
    <div className="flex min-w-0 items-center" style={{ paddingLeft: `${Math.min(depth, 6) * 12 + 16}px` }}>
      {hasChildren ? <button ref={expandButton} type="button" className="flex h-8 w-6 shrink-0 items-center justify-center rounded focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--focus)]" aria-label={t("history.toggleSubtasks", { title, count: session.childCount })} aria-expanded={open} onClick={() => setOpen(!open)}>{open ? <ChevronDown size={12} /> : <ChevronRight size={12} />}</button> : <span className="w-6 shrink-0" />}
      <button aria-current={props.selectedId === session.id ? "true" : undefined} aria-haspopup="menu" aria-label={`${t("history.open")}: ${title}`} className={cn("flex min-h-9 min-w-0 flex-1 items-center gap-2 rounded-lg px-2 py-1.5 text-left text-sm hover:bg-[var(--surface-hover)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--focus)]", props.selectedId === session.id && "bg-[var(--accent-soft)] font-medium")} data-history-session onClick={() => props.onSelect(session)} onContextMenu={(event) => { event.preventDefault(); props.onMenu(session, event.currentTarget, { x: event.clientX, y: event.clientY }); }} onKeyDown={(event) => menuKey(event, () => props.onMenu(session, event.currentTarget))} ref={(button) => { props.registerButton(session.id, button); if (button) props.registerGroupButton([session.id], fallback()); }} title={hint} type="button">
        <span className="min-w-0 flex-1 truncate">{title}</span>
        {session.sessionKind === "subagent" ? <GitBranch aria-label={t("history.subtasks")} className="shrink-0 text-[var(--muted)]" size={12} /> : null}
        {session.archived ? <Archive aria-label={t("history.archived")} className="shrink-0 text-[var(--muted)]" size={12} /> : null}
      </button>
    </div>
    {hasChildren && open ? <ul aria-label={t("history.childrenOf", { title })} className="min-w-0 space-y-0.5">
      {children.map((child) => <SessionBranch key={child.id} session={child} props={props} depth={depth + 1} fallback={() => expandButton.current || fallback()} />)}
      <li style={{ paddingLeft: `${Math.min(depth + 1, 6) * 12 + 40}px` }}><MoreRows query={query} /></li>
    </ul> : null}
  </li>;
}

function ProjectSection({ project, props, label, alias, onAlias }: { project: HistoryProjectGroup; props: ListProps; label: string; alias: string; onAlias(target: ProjectAliasTarget): void }) {
  const { t } = useTranslation();
  const seed = props.initialPage.projectId === project.id ? props.initialPage : undefined;
  const [open, setOpen] = useState(Boolean(seed) && project.kind !== "orphans");
  const button = useRef<HTMLButtonElement>(null);
  const query = useRows(props, project.id, open, seed);
  const rows = uniqueRows(query.data?.pages);
  const canAlias = Boolean(props.preferences?.setHistoryProjectAlias) && ["workspace", "directory"].includes(project.kind);
  const editAlias = (trigger: HTMLElement, point?: { x: number; y: number }) => {
    if (!canAlias) return;
    const bounds = trigger.getBoundingClientRect();
    onAlias({ id: project.id, name: project.name, alias, trigger, x: point?.x ?? bounds.left + 12, y: point?.y ?? bounds.bottom });
  };
  return <section aria-label={label}>
    <button ref={button} type="button" aria-expanded={open} aria-haspopup={canAlias ? "menu" : undefined} title={[label, t(`history.projectKinds.${project.kind}`), canAlias ? t("history.projectAliasContextHint") : ""].filter(Boolean).join("\n")} className="flex min-h-9 w-full items-center gap-2 rounded-lg px-2 py-1.5 text-left text-sm font-medium hover:bg-[var(--surface-hover)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--focus)]" onClick={() => setOpen(!open)} onContextMenu={(event) => { if (canAlias) { event.preventDefault(); editAlias(event.currentTarget, { x: event.clientX, y: event.clientY }); } }} onKeyDown={(event) => { if (canAlias) menuKey(event, () => editAlias(event.currentTarget)); }}>
      {open ? <ChevronDown className="shrink-0 text-[var(--muted)]" size={12} /> : <ChevronRight className="shrink-0 text-[var(--muted)]" size={12} />}
      {project.kind === "orphans" ? <GitBranch className="shrink-0" size={16} /> : open ? <FolderOpen className="shrink-0" size={16} /> : <Folder className="shrink-0" size={16} />}
      <span className="min-w-0 flex-1 truncate">{label}</span>
      {project.kind === "directory" && !alias ? <span className="shrink-0 text-[10px] font-normal text-[var(--muted)]">{t("history.directoryBadge")}</span> : null}
      <span className="text-xs font-normal text-[var(--muted)]" aria-label={t(project.kind === "orphans" ? "history.orphanCount" : "history.rootCount", { count: project.total })}>{project.total}</span>
    </button>
    {open ? <ul className="mt-0.5 min-w-0 space-y-0.5">
      {project.kind === "orphans" ? <li className="px-3 py-2 text-xs text-[var(--muted)]">{t("history.orphansHint")}</li> : null}
      {rows.map((session) => <SessionBranch key={session.id} session={session} props={props} fallback={() => button.current} />)}
      <li className="pl-8"><MoreRows query={query} /></li>
    </ul> : null}
  </section>;
}

export function HistoryProjectList(props: ListProps) {
  const { t } = useTranslation();
  const [aliasTarget, setAliasTarget] = useState<ProjectAliasTarget | null>(null);
  const [aliases, setAliases] = useState<Record<string, string>>({});
  const projects = props.initialPage.projects ?? [];
  const displayName = (project: HistoryProjectGroup) => project.kind === "orphans" ? t("history.orphans") : project.kind === "unassigned" ? t("history.noProject") : project.name;
  const getAlias = (project: HistoryProjectGroup) => {
    if (project.id in aliases) return aliases[project.id];
    try { return props.preferences?.getHistoryProjectAlias?.(props.preferenceScope, project.id) || ""; } catch { return ""; }
  };
  const labels = projects.map((project) => getAlias(project) || displayName(project));
  return <div className="space-y-2 p-2" data-history-projects>
    {projects.map((project, index) => {
      const label = labels[index];
      const ambiguous = labels.filter((entry) => entry === label).length > 1;
      return <ProjectSection key={project.id} project={project} props={props} label={ambiguous ? `${label} · ${project.id.slice(0, 6)}` : label} alias={getAlias(project)} onAlias={setAliasTarget} />;
    })}
    {aliasTarget ? <HistoryProjectAlias target={aliasTarget} close={() => setAliasTarget(null)} save={(id, value) => { props.preferences!.setHistoryProjectAlias!(props.preferenceScope, id, value); setAliases((current) => ({ ...current, [id]: value })); }} /> : null}
  </div>;
}
