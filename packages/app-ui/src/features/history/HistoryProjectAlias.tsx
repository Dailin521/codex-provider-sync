import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useTranslation } from "react-i18next";
import { Button, Dialog, Field, Input } from "../../ui.js";

export interface ProjectAliasTarget { id: string; name: string; alias: string; trigger: HTMLElement; x: number; y: number; }

export function HistoryProjectAlias({ target, close, save }: {
  target: ProjectAliasTarget;
  close(): void;
  save(id: string, alias: string): void;
}) {
  const { t } = useTranslation();
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(target.alias || target.name);
  const [error, setError] = useState(false);
  const menu = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState({ x: target.x, y: target.y });
  const restoreFocus = () => { if (target.trigger.isConnected) target.trigger.focus({ preventScroll: true }); };
  const dismiss = () => { restoreFocus(); close(); };
  useLayoutEffect(() => {
    const bounds = menu.current?.getBoundingClientRect();
    setPosition({ x: Math.max(8, Math.min(target.x, window.innerWidth - (bounds?.width || 256) - 8)), y: Math.max(8, Math.min(target.y, window.innerHeight - (bounds?.height || 100) - 8)) });
    menu.current?.querySelector<HTMLButtonElement>("button")?.focus({ preventScroll: true });
  }, [target]);
  useEffect(() => {
    if (editing) return;
    const outside = (event: Event) => { if (!menu.current?.contains(event.target as Node)) close(); };
    const blur = () => close();
    document.addEventListener("pointerdown", outside, true);
    document.addEventListener("scroll", outside, true);
    window.addEventListener("resize", blur);
    window.addEventListener("blur", blur);
    return () => {
      document.removeEventListener("pointerdown", outside, true);
      document.removeEventListener("scroll", outside, true);
      window.removeEventListener("resize", blur);
      window.removeEventListener("blur", blur);
    };
  }, [editing, close]);
  const submit = (value: string) => {
    try { save(target.id, value); dismiss(); }
    catch { setError(true); setEditing(true); }
  };
  if (editing) return <Dialog open onOpenChange={(open) => { if (!open) close(); }} restoreFocus={restoreFocus} title={t("history.projectAliasTitle")} description={t("history.projectAliasHint")} closeLabel={t("common.close")}>
    <form onSubmit={(event) => { event.preventDefault(); submit(draft.trim()); }}>
      <Field label={t("history.projectDisplayName")} error={error ? t("history.projectAliasFailed") : undefined}>
        <Input maxLength={160} value={draft} onChange={(event) => { setDraft(event.target.value); setError(false); }} />
      </Field>
      <div className="mt-5 flex justify-end gap-2"><Button type="button" variant="secondary" onClick={dismiss}>{t("common.cancel")}</Button><Button type="submit">{t("common.save")}</Button></div>
    </form>
  </Dialog>;
  const item = "min-h-10 w-full rounded-md px-3 text-left text-sm hover:bg-[var(--surface-hover)] focus:bg-[var(--accent-soft)] focus:outline-none disabled:opacity-40";
  return createPortal(<div ref={menu} role="menu" aria-label={t("history.projectActions")} style={{ left: position.x, top: position.y }} className="fixed z-[70] w-64 max-w-[calc(100vw-16px)] overflow-auto rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-1.5 text-[var(--text)] shadow-xl" onContextMenu={(event) => event.preventDefault()} onKeyDown={(event) => {
    if (event.key === "Escape" || event.key === "Tab") { event.preventDefault(); dismiss(); return; }
    const buttons = Array.from(menu.current?.querySelectorAll<HTMLButtonElement>("button:not(:disabled)") || []);
    const current = buttons.indexOf(document.activeElement as HTMLButtonElement);
    const index = event.key === "Home" ? 0 : event.key === "End" ? buttons.length - 1 : event.key === "ArrowDown" ? (current + 1) % buttons.length : event.key === "ArrowUp" ? (current + buttons.length - 1) % buttons.length : -1;
    if (index >= 0) { event.preventDefault(); buttons[index]?.focus(); }
  }}>
    <button className={item} role="menuitem" tabIndex={-1} type="button" onClick={() => setEditing(true)}>{t("history.projectAliasTitle")}</button>
    <button className={item} role="menuitem" tabIndex={-1} type="button" disabled={!target.alias} onClick={() => submit("")}>{t("history.projectAliasReset")}</button>
  </div>, document.body);
}
