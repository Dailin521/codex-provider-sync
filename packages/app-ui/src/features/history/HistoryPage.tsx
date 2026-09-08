import type { HistorySessionDetail, ListHistoryInput } from "@codex-provider-sync/contracts";
import { CoreClientError } from "@codex-provider-sync/core-client";
import { useQuery } from "@tanstack/react-query";
import { ArrowLeft, Check, Copy, RefreshCw, Search } from "lucide-react";
import { Fragment, isValidElement, type ReactNode, useCallback, useEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import { useTranslation } from "react-i18next";
import remarkGfm from "remark-gfm";

import { formatDate, PageHeading, profileSelector, safeErrorText } from "../../shared/presentation.js";
import type { AppUiProps, HostClient, HostProfile, PreferenceStore } from "../../types.js";
import { Button, Card, Input, cn } from "../../ui.js";
import { historyDisplayTitle } from "./history-title.js";
import { HistorySessionActions } from "./HistorySessionActions.js";
import { HistoryProjectList } from "./HistoryProjectList.js";
import { HistoryContextMenu, type HistoryMenuTarget } from "./HistoryContextMenu.js";
import { useCopyText } from "../../shared/clipboard.js";

export const HISTORY_PAGE_SIZE = 10;

function textFromNode(node: ReactNode): string {
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(textFromNode).join("");
  if (isValidElement<{ children?: ReactNode }>(node)) return textFromNode(node.props.children);
  return "";
}

function MarkdownPre({ children }: { children?: ReactNode }) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);
  const [copyFailed, setCopyFailed] = useState(false);
  const copyText = useCopyText();
  const copy = async () => {
    try {
      await copyText(textFromNode(children).replace(/\n$/, ""));
      setCopied(true);
      setCopyFailed(false);
      globalThis.setTimeout(() => setCopied(false), 1500);
    } catch {
      setCopied(false);
      setCopyFailed(true);
    }
  };
  return (
    <div className="group relative my-4 overflow-hidden rounded-xl border border-[var(--border)] bg-[var(--surface)]">
      <Button aria-label={t("common.copy")} className="absolute right-2 top-2 min-h-8 px-2" onClick={() => void copy()} type="button" variant="secondary">
        {copied ? <Check size={14} /> : <Copy size={14} />}<span className="sr-only">{copied ? t("common.copied") : t("common.copy")}</span>
      </Button>
      <pre className="overflow-x-auto p-4 pr-12 text-sm leading-6">{children}</pre>
      {copyFailed ? <p className="px-4 pb-3 text-sm text-[var(--danger)]" role="status">{t("history.copyFailed")}</p> : null}
    </div>
  );
}

function MarkdownBody({ text }: { text: string }) {
  return (
    <div className="min-w-0 break-words text-sm leading-7">
      <ReactMarkdown
        components={{
          a: ({ node: _node, ...props }) => <a {...props} className="text-[var(--accent-strong)] underline underline-offset-2" rel="noreferrer" target="_blank" />,
          blockquote: ({ node: _node, ...props }) => <blockquote {...props} className="my-3 border-l-4 border-[var(--border)] pl-4 text-[var(--muted)]" />,
          code: ({ node: _node, ...props }) => <code {...props} className={cn("rounded bg-[var(--surface)] px-1.5 py-0.5 font-mono text-[0.9em]", props.className)} />,
          h1: ({ node: _node, ...props }) => <h1 {...props} className="mb-3 mt-5 text-xl font-bold" />,
          h2: ({ node: _node, ...props }) => <h2 {...props} className="mb-3 mt-5 text-lg font-bold" />,
          h3: ({ node: _node, ...props }) => <h3 {...props} className="mb-2 mt-4 font-semibold" />,
          li: ({ node: _node, ...props }) => <li {...props} className="my-1" />,
          ol: ({ node: _node, ...props }) => <ol {...props} className="my-3 list-decimal pl-6" />,
          p: ({ node: _node, ...props }) => <p {...props} className="my-3 whitespace-pre-wrap first:mt-0 last:mb-0" />,
          pre: ({ node: _node, children }) => <MarkdownPre>{children}</MarkdownPre>,
          table: ({ node: _node, ...props }) => <div className="my-4 overflow-x-auto"><table {...props} className="w-full border-collapse text-sm" /></div>,
          td: ({ node: _node, ...props }) => <td {...props} className="border border-[var(--border)] px-3 py-2" />,
          th: ({ node: _node, ...props }) => <th {...props} className="border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-left" />,
          ul: ({ node: _node, ...props }) => <ul {...props} className="my-3 list-disc pl-6" />
        }}
        remarkPlugins={[remarkGfm]}
      >
        {text}
      </ReactMarkdown>
    </div>
  );
}

export function HistoryPage({ core, profile, host, preferences }: { core: AppUiProps["core"]; profile: HostProfile; host?: HostClient; preferences?: PreferenceStore }) {
  const { t, i18n } = useTranslation();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selectedPreview, setSelectedPreview] = useState<HistorySessionDetail["session"] | null>(null);
  const [metadataOnly, setMetadataOnly] = useState(false);
  const [loadedDetail, setDetail] = useState<HistorySessionDetail | null>(null);
  const detail = loadedDetail?.session.id === selectedId ? loadedDetail : null;
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [draftQuery, setDraftQuery] = useState("");
  const [query, setQuery] = useState("");
  const [draftScope, setDraftScope] = useState<"metadata" | "content">("metadata");
  const searchInput = useRef<HTMLInputElement>(null);
  const [searchScope, setSearchScope] = useState<"metadata" | "content">("metadata");
  const [sessionKind, setSessionKind] = useState<"all" | "main" | "subagent">("all");
  const [detailRefresh, setDetailRefresh] = useState(0);
  const [parentNavigation, setParentNavigation] = useState(false);
  const [draftProvider, setDraftProvider] = useState("");
  const [providerFilter, setProviderFilter] = useState("");
  const [archived, setArchived] = useState<"all" | "active" | "archived">("all");
  const [returnFocusId, setReturnFocusId] = useState<string | null>(null);
  const menuScope = JSON.stringify([profile.id, profile.revision, query, providerFilter, archived, searchScope, sessionKind]);
  const [storedMenuTarget, setMenuTarget] = useState<(HistoryMenuTarget & { scope: string }) | null>(null);
  const menuTarget = storedMenuTarget?.scope === menuScope ? storedMenuTarget : null;
  const [informationId, setInformationId] = useState<string | null>(null);
  const [actionNotice, setActionNotice] = useState("");
  const detailHeadingRef = useRef<HTMLHeadingElement | null>(null);
  const detailScrollRef = useRef<HTMLDivElement | null>(null);
  const listScrollRef = useRef<HTMLDivElement | null>(null);
  const sessionButtons = useRef(new Map<string, HTMLButtonElement>());
  const projectButtons = useRef(new Map<string, HTMLButtonElement>());
  const closeMenu = useCallback((restoreFocus = true) => {
    if (restoreFocus && menuTarget?.trigger.isConnected) menuTarget.trigger.focus({ preventScroll: true });
    setMenuTarget(null);
  }, [menuTarget]);
  const openMenu = (session: HistorySessionDetail["session"], trigger: HTMLElement, point?: { x: number; y: number }) => {
    const rect = trigger.getBoundingClientRect();
    setActionNotice("");
    setMenuTarget({ session, trigger, scope: menuScope, x: point?.x ?? rect.left + 24, y: point?.y ?? rect.bottom });
  };
  const historyInput: ListHistoryInput = { profile: profileSelector(profile), view: "projects", page: 1, pageSize: HISTORY_PAGE_SIZE, ...(query ? { query } : {}), ...(providerFilter ? { provider: providerFilter } : {}), archived, searchScope, sessionKind };
  const list = useQuery({
    queryKey: ["history", menuScope],
    queryFn: ({ signal }) => core.listHistory(historyInput, { signal }),
    gcTime: 0,
    retry: false,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false
  });

  useEffect(() => {
    setSelectedId(null); setSelectedPreview(null); setDetail(null); setDetailError(null);
    setDraftScope("metadata"); setSearchScope("metadata"); setSessionKind("all"); setParentNavigation(false);
    setDraftQuery(""); setQuery(""); setDraftProvider(""); setProviderFilter(""); setArchived("all");
  }, [profile.id, profile.revision]);
  useEffect(() => { setSelectedId(null); setSelectedPreview(null); setDetail(null); setDetailError(null); sessionButtons.current.clear(); projectButtons.current.clear(); }, [query, providerFilter, archived, searchScope, sessionKind]);
  useEffect(() => { setMenuTarget(null); setInformationId(null); setActionNotice(""); }, [profile.id, profile.revision, query, providerFilter, archived, searchScope, sessionKind, list.dataUpdatedAt]);

  useEffect(() => {
    if (!selectedId) { setDetail(null); setDetailError(null); setDetailLoading(false); return; }
    const controller = new AbortController();
    setDetail(null); setDetailError(null); setDetailLoading(true);
    void core.getHistorySession({ profile: profileSelector(profile), sessionId: selectedId, ...(metadataOnly ? { metadataOnly: true } : { messageLimit: 200 }) }, { signal: controller.signal })
      .then((value) => { if (!controller.signal.aborted) setDetail(value); })
      .catch((error: unknown) => { if (!controller.signal.aborted) setDetailError(parentNavigation && error instanceof CoreClientError && error.code === "INVALID_INPUT" ? t("history.parentUnavailable") : safeErrorText(error, t)); })
      .finally(() => { if (!controller.signal.aborted) setDetailLoading(false); });
    return () => { controller.abort(); setDetail(null); };
  }, [core, profile.id, profile.revision, selectedId, t, detailRefresh, parentNavigation, metadataOnly]);
  useEffect(() => { if (detail && !menuTarget) detailHeadingRef.current?.focus({ preventScroll: true }); }, [detail]);
  useEffect(() => { if (detailScrollRef.current) detailScrollRef.current.scrollTop = 0; }, [selectedId, profile.id, profile.revision]);
  useEffect(() => {
    if (selectedId || !returnFocusId) return;
    const button = [sessionButtons.current.get(returnFocusId), projectButtons.current.get(returnFocusId)].find((entry) => entry?.isConnected);
    (button || listScrollRef.current)?.focus({ preventScroll: true });
    setReturnFocusId(null);
  }, [list.data, returnFocusId, selectedId]);

  const sessions = list.data?.sessions ?? [];
  const selectedSummary = detail?.session ?? (selectedPreview?.id === selectedId ? selectedPreview : sessions.find((session) => session.id === selectedId));
  const selectedTitle = selectedSummary ? historyDisplayTitle(selectedSummary, t, i18n.language) : (detailError ? t("history.sessionInformation") : t("common.loading"));
  const closeDetail = () => { if (selectedId && !returnFocusId) setReturnFocusId(selectedId); setSelectedId(null); };
  const pendingFilters = draftQuery.trim() !== query || draftProvider.trim() !== providerFilter || draftScope !== searchScope;
  const hasFilters = pendingFilters || Boolean(query || providerFilter || draftQuery || draftProvider) || searchScope !== "metadata" || archived !== "all" || sessionKind !== "all";
  const clearFilters = () => {
    setDraftQuery(""); setQuery(""); setDraftProvider(""); setProviderFilter("");
    setDraftScope("metadata"); setSearchScope("metadata"); setArchived("all"); setSessionKind("all");
    searchInput.current?.focus();
  };
  return (
    <div className="flex min-h-0 flex-1 flex-col overflow-hidden" data-history-layout>
      <div className={cn("max-h-[45%] shrink-0 overflow-y-auto overscroll-contain", selectedId && "hidden lg:block")} data-history-controls>
      <PageHeading title={t("history.title")} subtitle={t("history.subtitle")} action={<Button disabled={list.isFetching} onClick={() => { void list.refetch(); setDetailRefresh((value) => value + 1); }} type="button" variant="secondary"><RefreshCw className={cn(list.isFetching && "animate-spin")} size={16} />{t("common.refresh")}</Button>} />
      <form className="mb-3 grid grid-cols-[minmax(0,1fr)_auto] gap-2" onSubmit={(event) => { event.preventDefault(); setQuery(draftQuery.trim()); setSearchScope(draftScope); setProviderFilter(draftProvider.trim()); }}>
        <Input ref={searchInput} aria-label={t("common.search")} onChange={(event) => setDraftQuery(event.target.value)} placeholder={t("history.searchPlaceholder")} value={draftQuery} />
        <Button type="submit"><Search size={16} />{t("common.search")}</Button>
        {hasFilters ? <div className="col-span-2 flex flex-wrap items-center gap-2"><Button type="button" size="compact" variant="secondary" onClick={clearFilters}>{t("ux.clearFilters")}</Button>{pendingFilters ? <p className="text-xs text-[var(--muted)]" role="status">{t("ux.pendingFilters")}</p> : null}</div> : null}
        <details className="col-span-2 rounded-lg border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2"><summary className="cursor-pointer text-xs text-[var(--muted)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--focus)]">{t("history.filters")}</summary><div className="mt-2 grid gap-2 md:grid-cols-2 xl:grid-cols-4">
        <Input aria-label={t("history.providerFilter")} onChange={(event) => setDraftProvider(event.target.value)} placeholder={t("history.providerFilter")} value={draftProvider} />
        <select aria-label={t("history.archivedFilter")} className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" onChange={(event) => setArchived(event.target.value as typeof archived)} value={archived}><option value="all">{t("history.all")}</option><option value="active">{t("history.active")}</option><option value="archived">{t("history.archived")}</option></select>
        <select aria-label={t("history.searchScope")} className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" value={draftScope} onChange={(event) => setDraftScope(event.target.value as typeof draftScope)}><option value="metadata">{t("history.metadataSearch")}</option><option value="content">{t("history.contentSearch")}</option></select>
        <select aria-label={t("history.sessionType")} className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" value={sessionKind} onChange={(event) => setSessionKind(event.target.value as typeof sessionKind)}><option value="all">{t("history.mainWithSubtasks")}</option><option value="main">{t("history.mainSessions")}</option><option value="subagent">{t("history.subtasks")}</option></select>
        <p className="text-xs text-[var(--muted)] md:col-span-2 xl:col-span-4">{t(draftScope === "content" ? "history.contentSearchHint" : "history.metadataSearchHint")}</p>
        </div></details>
      </form>
      </div>
      <div className="grid min-h-0 flex-1 grid-rows-1 gap-0 overflow-hidden rounded-xl border border-[var(--border)] lg:grid-cols-[minmax(240px,300px)_minmax(0,1fr)]">
        <Card aria-busy={list.isPending || list.isFetching} className={cn("min-h-0 min-w-0 flex-col overflow-hidden rounded-none border-0 bg-[var(--surface)] p-0 shadow-none lg:border-r", selectedId ? "hidden lg:flex" : "flex")}>
          <p className="shrink-0 px-3 py-2 text-xs text-[var(--muted)]">{t("history.projectTreeHint")}</p>
          <div aria-label={t("history.listRegion")} className="min-h-0 flex-1 overflow-y-auto overscroll-contain focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--focus)]" ref={listScrollRef} role="region" tabIndex={0} data-history-list>
          {list.isPending ? <div className="p-5" aria-live="polite" role="status">{t("common.loading")}</div> : list.isError ? <div className="p-5 text-[var(--danger)]" role="alert">{safeErrorText(list.error, t)}</div> : !list.data?.projects?.length ? <div className="p-5 text-[var(--muted)]">{t("history.empty")}</div> : <HistoryProjectList key={`${menuScope}:${list.dataUpdatedAt}`} scope={`${menuScope}:${list.dataUpdatedAt}`} preferenceScope={JSON.stringify([profile.id, profile.revision])} preferences={preferences} core={core} input={historyInput} initialPage={list.data} selectedId={selectedId} onSelect={(session) => { setReturnFocusId(null); setInformationId(null); setMetadataOnly(false); setParentNavigation(false); setSelectedPreview(session); setSelectedId(session.id); }} onMenu={openMenu} registerButton={(id, button) => { if (button) sessionButtons.current.set(id, button); else sessionButtons.current.delete(id); }} registerGroupButton={(ids, button) => { for (const id of ids) { if (button) projectButtons.current.set(id, button); else projectButtons.current.delete(id); } }} />}
          </div>
          {actionNotice ? <p className="shrink-0 px-3 py-2 text-xs" role="status">{actionNotice}</p> : null}
        </Card>
        <Card className={cn("min-h-0 min-w-0 flex-col overflow-hidden rounded-none border-0 p-0 shadow-none", !selectedId ? "hidden lg:flex" : "flex")}>
          {!selectedId ? <div className="grid min-h-0 flex-1 place-items-center p-4 text-sm text-[var(--muted)]">{t("history.select")}</div> : <Fragment>
            <div className="flex max-h-[40%] shrink-0 items-start justify-between gap-3 overflow-y-auto overscroll-contain border-b border-[var(--border)] p-3 md:p-4" data-history-detail-header><div className="min-w-0"><h2 className="truncate text-lg font-semibold" ref={detailHeadingRef} tabIndex={-1}>{selectedTitle}</h2>{detail ? <div className="mt-1 flex flex-wrap gap-2 text-xs text-[var(--muted)]"><span>{detail.session.provider}</span>{detail.session.model ? <span>{detail.session.model}</span> : null}<span>{formatDate(detail.session.updatedAt, i18n.language)}</span></div> : null}</div><div className="flex shrink-0 gap-1"><Button aria-label={t("history.refreshDetail")} disabled={detailLoading} onClick={() => setDetailRefresh((value) => value + 1)} type="button" variant="secondary"><RefreshCw size={16} /></Button><Button className="lg:hidden" onClick={closeDetail} type="button" variant="secondary"><ArrowLeft size={16} />{t("history.back")}</Button></div></div>
            <div aria-label={t("history.detailRegion")} className="min-h-0 flex-1 overflow-y-auto overscroll-contain p-3 focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--focus)] md:p-4" ref={detailScrollRef} role="region" tabIndex={0} data-history-detail-scroll>
            {selectedSummary && informationId === selectedId ? <div className="mb-4 rounded-xl border border-[var(--border)] p-3"><div className="mb-2 flex justify-end"><Button size="compact" onClick={() => setInformationId(null)} type="button" variant="ghost">{t("common.close")}</Button></div><HistorySessionActions key={`${profile.id}:${selectedSummary.id}`} openInformation session={selectedSummary} detail={detail} host={host} profile={profile} onOpenParent={(id) => { setInformationId(null); setMetadataOnly(false); setParentNavigation(true); if (!returnFocusId) setReturnFocusId(selectedId); setSelectedId(id); }} /></div> : null}
            {metadataOnly && !detailLoading ? <Button className="mb-4" onClick={() => { setMetadataOnly(false); setInformationId(null); }} type="button" variant="secondary">{t("history.open")}</Button> : null}
            {detailLoading ? <span aria-live="polite" role="status">{t("common.loading")}</span> : detailError ? <div className="grid justify-items-start gap-3"><p className="text-[var(--danger)]" role="alert">{detailError}</p><Button onClick={() => setDetailRefresh((value) => value + 1)} type="button" variant="secondary">{t("common.retry")}</Button></div> : detail && !metadataOnly ? <div>{detail.truncated ? <div className="mb-4 rounded-lg border border-[var(--warning)] bg-[var(--warning-soft)] p-3 text-sm">{t("history.truncated")}</div> : null}<div className="grid gap-6">{detail.messages.map((message) => message.role === "user" ? <article className="ml-auto max-w-[85%] rounded-2xl rounded-br-md bg-[var(--accent-soft)] px-4 py-3" key={`${message.sequence}-${message.role}`}><div className="mb-1 text-xs font-semibold text-[var(--muted)]">{t("history.roles.user")}{message.timestamp ? <span className="ml-2 font-normal">{formatDate(message.timestamp, i18n.language)}</span> : null}</div><div className="whitespace-pre-wrap break-words text-sm leading-7">{message.text}</div></article> : <article className="min-w-0" key={`${message.sequence}-${message.role}`}><div className="mb-2 text-xs font-semibold text-[var(--muted)]">{t("history.roles.assistant")}{message.timestamp ? <span className="ml-2 font-normal">{formatDate(message.timestamp, i18n.language)}</span> : null}</div><MarkdownBody text={message.text} /></article>)}</div></div> : null}
            </div>
          </Fragment>}
        </Card>
      </div>
      {menuTarget ? <HistoryContextMenu target={menuTarget} host={host} profile={profile} close={closeMenu} open={() => { setInformationId(null); setMetadataOnly(false); setReturnFocusId(null); setParentNavigation(false); setSelectedPreview(menuTarget.session); setSelectedId(menuTarget.session.id); }} information={() => { setInformationId(menuTarget.session.id); setMetadataOnly(true); setReturnFocusId(null); setParentNavigation(false); setSelectedPreview(menuTarget.session); setSelectedId(menuTarget.session.id); }} notice={setActionNotice} /> : null}
    </div>
  );
}
