import type { HistorySessionSummary } from "@codex-provider-sync/contracts";
import { Copy, FolderOpen, Info, MessageSquare, Terminal } from "lucide-react";
import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useTranslation } from "react-i18next";
import { useCopyText } from "../../shared/clipboard.js";
import { profileSelector } from "../../shared/presentation.js";
import type { HostClient, HostProfile } from "../../types.js";
import { historyDisplayTitle } from "./history-title.js";
import { historyResumeCommand } from "./HistorySessionActions.js";

export interface HistoryMenuTarget {
  session: HistorySessionSummary;
  trigger: HTMLElement;
  x: number;
  y: number;
}

export function HistoryContextMenu({ target, host, profile, close, open, information, notice }: {
  target: HistoryMenuTarget;
  host?: HostClient;
  profile: HostProfile;
  close(restoreFocus?: boolean): void;
  open(): void;
  information(): void;
  notice(message: string): void;
}) {
  const { t, i18n } = useTranslation();
  const menu = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState({ x: target.x, y: target.y });
  const copyText = useCopyText();
  const session = target.session;
  const command = historyResumeCommand(session);
  useLayoutEffect(() => {
    const bounds = menu.current?.getBoundingClientRect();
    setPosition({ x: Math.max(8, Math.min(target.x, window.innerWidth - (bounds?.width || 240) - 8)), y: Math.max(8, Math.min(target.y, window.innerHeight - (bounds?.height || 240) - 8)) });
    menu.current?.querySelector<HTMLButtonElement>("button:not(:disabled)")?.focus({ preventScroll: true });
  }, [target]);
  useEffect(() => {
    const dismiss = () => close(false);
    const outside = (event: Event) => { if (!menu.current?.contains(event.target as Node)) close(false); };
    window.addEventListener("resize", dismiss);
    window.addEventListener("blur", dismiss);
    document.addEventListener("scroll", outside, true);
    document.addEventListener("pointerdown", outside, true);
    return () => {
      window.removeEventListener("resize", dismiss);
      window.removeEventListener("blur", dismiss);
      document.removeEventListener("scroll", outside, true);
      document.removeEventListener("pointerdown", outside, true);
    };
  }, [close]);
  const copy = async (value: string) => {
    close();
    try { await copyText(value); notice(t("common.copied")); }
    catch { notice(t("history.copyFailed")); }
  };
  const reveal = async () => {
    close();
    try {
      const result = await host!.revealHistoryFile!(profileSelector(profile), session.id);
      notice(t(result.revealed ? "history.fileRevealed" : "history.revealFailed"));
    } catch { notice(t("history.revealFailed")); }
  };
  const itemClass = "flex min-h-10 w-full items-center gap-3 rounded-md px-3 text-left text-sm hover:bg-[var(--surface-hover)] focus:bg-[var(--accent-soft)] focus:outline-none disabled:opacity-40";
  return createPortal(<div aria-label={`${t("history.sessionActions")}: ${historyDisplayTitle(session, t, i18n.language)}`} className="fixed z-[70] w-64 max-w-[calc(100vw-16px)] max-h-[calc(100dvh-16px)] overflow-y-auto overscroll-contain rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-1.5 text-[var(--text)] shadow-xl" ref={menu} role="menu" style={{ left: position.x, top: position.y }} onContextMenu={(event) => event.preventDefault()} onKeyDown={(event) => {
    if (event.key === "Escape" || event.key === "Tab") { event.preventDefault(); close(); return; }
    const buttons = Array.from(menu.current?.querySelectorAll<HTMLButtonElement>("button:not(:disabled)") || []);
    const current = buttons.indexOf(document.activeElement as HTMLButtonElement);
    const next = event.key === "Home" ? 0 : event.key === "End" ? buttons.length - 1 : event.key === "ArrowDown" ? (current + 1) % buttons.length : event.key === "ArrowUp" ? (current + buttons.length - 1) % buttons.length : -1;
    if (next >= 0) { event.preventDefault(); buttons[next]?.focus(); }
  }}>
    <button className={itemClass} role="menuitem" tabIndex={-1} type="button" onClick={() => { close(false); open(); }}><MessageSquare size={16} />{t("history.open")}</button>
    <button className={itemClass} disabled={!session.nativeSessionId} role="menuitem" tabIndex={-1} type="button" onClick={() => void copy(session.nativeSessionId!)}><Copy size={16} />{t("history.copyId")}</button>
    <button className={itemClass} disabled={!command} role="menuitem" tabIndex={-1} title={t("history.resumeHint")} type="button" onClick={() => void copy(command!)}><Terminal size={16} />{t("history.copyResume")}</button>
    <div className="my-1 border-t border-[var(--border)]" role="separator" />
    <button className={itemClass} role="menuitem" tabIndex={-1} type="button" onClick={() => { close(false); information(); }}><Info size={16} />{t("history.sessionInformation")}</button>
    {host?.revealHistoryFile ? <button className={itemClass} role="menuitem" tabIndex={-1} type="button" onClick={() => void reveal()}><FolderOpen size={16} />{t("history.revealFile")}</button> : null}
  </div>, document.body);
}
