import type { HistorySessionDetail, HistorySessionSummary } from "@codex-provider-sync/contracts";
import { Copy, FolderOpen } from "lucide-react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { HostClient, HostProfile } from "../../types.js";
import { formatDate, profileSelector } from "../../shared/presentation.js";
import { Button } from "../../ui.js";
import { useCopyText } from "../../shared/clipboard.js";

export function historyResumeCommand(session: HistorySessionSummary): string | null {
  const id = session.nativeSessionId;
  return typeof id === "string" && /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(id)
    ? `codex resume ${id}` : null;
}

export function HistorySessionActions({ session, detail, host, profile, onOpenParent, openInformation = false }: {
  session: HistorySessionSummary;
  detail: HistorySessionDetail | null;
  host?: HostClient;
  profile: HostProfile;
  onOpenParent(id: string): void;
  openInformation?: boolean;
}) {
  const { t, i18n } = useTranslation();
  const [notice, setNotice] = useState("");
  const copyText = useCopyText();
  const [revealing, setRevealing] = useState(false);
  const resumeCommand = historyResumeCommand(session);
  const storage = detail?.storage;
  const copy = async (value: string) => {
    try { await copyText(value); setNotice(t("common.copied")); }
    catch { setNotice(t("history.copyFailed")); }
  };
  const reveal = async () => {
    if (!host?.revealHistoryFile) return;
    setRevealing(true);
    try {
      const result = await host.revealHistoryFile(profileSelector(profile), session.id);
      setNotice(t(result.revealed ? "history.fileRevealed" : "history.revealFailed"));
    } catch { setNotice(t("history.revealFailed")); }
    finally { setRevealing(false); }
  };
  return <section aria-label={t("history.sessionActions")} className="mb-4 min-w-0 space-y-3">
    <div className="flex flex-wrap gap-2">
      <Button disabled={!session.nativeSessionId} onClick={() => void copy(session.nativeSessionId!)} type="button" variant="secondary"><Copy size={14} />{t("history.copyId")}</Button>
      <Button disabled={!resumeCommand} onClick={() => void copy(resumeCommand!)} type="button" variant="secondary"><Copy size={14} />{t("history.copyResume")}</Button>
    </div>
    {!session.nativeSessionId ? <p className="text-xs text-[var(--muted)]">{t("history.missingNativeId")}</p> : null}
    {resumeCommand ? <p className="text-xs text-[var(--muted)]">{t("history.resumeHint")}</p> : null}
    <details className="rounded-lg border border-[var(--border)] p-3" open={openInformation || undefined}>
      <summary className="cursor-pointer font-medium focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--accent)]">{t("history.sessionInformation")}</summary>
      <dl className="mt-3 grid min-w-0 gap-3 text-sm">
        <div><dt className="text-[var(--muted)]">{t("history.nativeId")}</dt><dd className="select-text break-all font-mono">{session.nativeSessionId || t("history.notRecorded")}</dd></div>
        <div><dt className="text-[var(--muted)]">{t("history.sessionType")}</dt><dd>{t(session.sessionKind === "subagent" ? "history.subtasks" : "history.mainSessions")}</dd></div>
        {session.parentSessionId ? <div><dt className="text-[var(--muted)]">{t("history.parentId")}</dt><dd className="flex flex-wrap items-center gap-2"><span className="select-text break-all font-mono">{session.parentSessionId}</span><Button onClick={() => onOpenParent(session.parentSessionId!)} type="button" variant="secondary">{t("history.openParent")}</Button></dd></div> : null}
        <div><dt className="text-[var(--muted)]">{t("history.recordedProvider")}</dt><dd>{session.provider}</dd></div>
        <div><dt className="text-[var(--muted)]">{t("history.recordedModel")}</dt><dd>{session.model || t("history.notRecorded")}</dd></div>
        <div><dt className="text-[var(--muted)]">{t("history.createdAt")}</dt><dd>{formatDate(session.createdAt, i18n.language)}</dd></div>
        <div><dt className="text-[var(--muted)]">{t("history.fileModifiedAt")}</dt><dd>{formatDate(session.fileModifiedAt, i18n.language)}</dd></div>
        <p className="text-xs text-[var(--muted)]">{t("history.fileTimeHint")}</p>
        {storage ? <>
          <div><dt className="text-[var(--muted)]">{t("history.projectDirectory")}</dt><dd className="select-text break-all">{storage.cwd || t("history.notRecorded")}</dd></div>
          <div><dt className="text-[var(--muted)]">{t("history.sessionFile")}</dt><dd className="select-text break-all">{storage.rolloutPath}</dd></div>
          <div className="flex flex-wrap gap-2"><Button onClick={() => void copy(storage.rolloutPath)} type="button" variant="secondary"><Copy size={14} />{t("history.copyPath")}</Button>{host?.revealHistoryFile ? <Button disabled={revealing} onClick={() => void reveal()} type="button" variant="secondary"><FolderOpen size={14} />{t("history.revealFile")}</Button> : null}</div>
        </> : <p className="text-xs text-[var(--muted)]">{t("history.localInfoHint")}</p>}
      </dl>
    </details>
    <p aria-live="polite" className="text-xs text-[var(--muted)]" role="status">{notice}</p>
  </section>;
}
