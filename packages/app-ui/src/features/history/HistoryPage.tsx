import type { HistorySessionDetail } from "@codex-provider-sync/contracts";
import { useQuery } from "@tanstack/react-query";
import { RefreshCw } from "lucide-react";
import { Fragment, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { formatDate, PageHeading, profileSelector, safeErrorText } from "../../shared/presentation.js";
import type { AppUiProps, HostProfile } from "../../types.js";
import { Badge, Button, Card, cn } from "../../ui.js";

export const HISTORY_PAGE_SIZE = 50;

export function HistoryPage({ core, profile }: {
  core: AppUiProps["core"];
  profile: HostProfile;
}) {
  const { t, i18n } = useTranslation();
  const [page, setPage] = useState(1);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<HistorySessionDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [returnFocusId, setReturnFocusId] = useState<string | null>(null);
  const detailHeadingRef = useRef<HTMLHeadingElement | null>(null);
  const openButtons = useRef(new Map<string, HTMLButtonElement>());
  const list = useQuery({
    queryKey: ["history", profile.id, profile.revision, page, HISTORY_PAGE_SIZE],
    queryFn: ({ signal }) => core.listHistory({
      profile: profileSelector(profile),
      page,
      pageSize: HISTORY_PAGE_SIZE
    }, { signal }),
    gcTime: 0,
    retry: false,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false
  });

  useEffect(() => {
    setPage(1);
    setSelectedId(null);
    setDetail(null);
    setDetailError(null);
  }, [profile.id, profile.revision]);

  useEffect(() => {
    if (!selectedId) {
      setDetail(null);
      setDetailError(null);
      setDetailLoading(false);
      return;
    }
    const controller = new AbortController();
    setDetail(null);
    setDetailError(null);
    setDetailLoading(true);
    void core.getHistorySession({
      profile: profileSelector(profile),
      sessionId: selectedId,
      messageLimit: 200
    }, { signal: controller.signal })
      .then((value) => {
        if (!controller.signal.aborted) setDetail(value);
      })
      .catch((error: unknown) => {
        if (!controller.signal.aborted) setDetailError(safeErrorText(error, t("global.failed")));
      })
      .finally(() => {
        if (!controller.signal.aborted) setDetailLoading(false);
      });
    return () => {
      controller.abort();
      setDetail(null);
    };
  }, [core, profile.id, profile.revision, selectedId, t]);

  useEffect(() => {
    if (detail) detailHeadingRef.current?.focus();
  }, [detail]);

  useEffect(() => {
    if (selectedId || !returnFocusId) return;
    const button = openButtons.current.get(returnFocusId);
    if (!button) return;
    button.focus();
    setReturnFocusId(null);
  }, [list.data, returnFocusId, selectedId]);

  if (selectedId) {
    return (
      <Fragment>
        <PageHeading
          title={detail ? (detail.session.title || t("history.untitled")) : t("history.title")}
          subtitle={t("history.subtitle")}
          action={<Button onClick={() => { setReturnFocusId(selectedId); setSelectedId(null); }} type="button" variant="secondary">{t("history.back")}</Button>}
          headingRef={detailHeadingRef}
          headingTabIndex={-1}
        />
        <Card aria-busy={detailLoading}>
          {detailLoading
            ? <span aria-live="polite" role="status">{t("common.loading")}</span>
            : detailError
              ? <span className="text-[var(--danger)]" role="alert">{detailError}</span>
              : detail
                ? (
                    <div className="grid gap-4">
                      {detail.messages.map((message) => (
                        <article className="rounded-lg border border-[var(--border)] bg-[var(--surface)] p-4" key={`${message.sequence}-${message.role}`}>
                          <div className="mb-2 flex justify-between text-xs font-semibold text-[var(--muted)]"><span>{t(`history.roles.${message.role}`, { defaultValue: message.role })}</span><span>{formatDate(message.timestamp, i18n.language)}</span></div>
                          <pre className="whitespace-pre-wrap break-words font-sans text-sm leading-6">{message.text}</pre>
                        </article>
                      ))}
                    </div>
                  )
                : null}
        </Card>
      </Fragment>
    );
  }

  const sessions = list.data?.sessions ?? [];
  return (
    <Fragment>
      <PageHeading
        title={t("history.title")}
        subtitle={t("history.subtitle")}
        action={<Button disabled={list.isFetching} onClick={() => void list.refetch()} type="button" variant="secondary"><RefreshCw className={cn(list.isFetching && "animate-spin")} size={16} />{t("common.refresh")}</Button>}
      />
      <Card aria-busy={list.isPending || list.isFetching}>
        {list.isPending
          ? <span aria-live="polite" role="status">{t("common.loading")}</span>
          : list.isError
            ? <span className="text-[var(--danger)]" role="alert">{safeErrorText(list.error, t("global.failed"))}</span>
            : sessions.length === 0
              ? <span className="text-[var(--muted)]">{t("history.empty")}</span>
              : (
                  <div className="divide-y divide-[var(--border)]">
                    {sessions.map((session) => (
                      <div className="flex flex-wrap items-center justify-between gap-4 py-4" key={session.id}>
                        <div className="min-w-0">
                          <div className="truncate font-semibold">{session.title || t("history.untitled")}</div>
                          <div className="mt-1 flex flex-wrap gap-2 text-xs text-[var(--muted)]"><span>{session.provider}</span>{session.messageCountKnown !== false ? <span>{session.messageCount} {t("history.messages")}</span> : null}<span>{formatDate(session.updatedAt, i18n.language)}</span>{session.archived ? <Badge>{t("history.archived")}</Badge> : null}</div>
                        </div>
                        <Button onClick={() => { setReturnFocusId(null); setSelectedId(session.id); }} ref={(button) => { if (button) openButtons.current.set(session.id, button); else openButtons.current.delete(session.id); }} type="button" variant="secondary">{t("history.open")}</Button>
                      </div>
                    ))}
                  </div>
                )}
        {list.data ? (
          <nav aria-label={t("history.pagination")} className="mt-5 flex flex-wrap items-center justify-between gap-3 border-t border-[var(--border)] pt-4">
            <span className="text-xs text-[var(--muted)]">{t("history.pageSummary", { page: list.data.page, total: list.data.total })}</span>
            <div className="flex gap-2">
              <Button disabled={page <= 1 || list.isFetching} onClick={() => setPage((value) => Math.max(1, value - 1))} type="button" variant="secondary">{t("history.previous")}</Button>
              <Button disabled={!list.data.hasNextPage || list.isFetching} onClick={() => setPage((value) => value + 1)} type="button" variant="secondary">{t("history.next")}</Button>
            </div>
          </nav>
        ) : null}
      </Card>
    </Fragment>
  );
}
