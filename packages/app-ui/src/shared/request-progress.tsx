import type { CoreRequestProgressEnvelope, ProgressEvent } from "@codex-provider-sync/contracts";
import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

export type RequestProgressState = { startedAt: number; progress: ProgressEvent | null };

// Per-request, ephemeral UI state. No query invalidation, status polling or
// persistence; late frames from completed requests/other Profiles are ignored.
export function useRequestProgress(profileKey: string) {
  const [state, setState] = useState<(RequestProgressState & { profileKey: string }) | null>(null);
  const active = useRef<object | null>(null);
  const currentKey = useRef(profileKey);
  currentKey.current = profileKey;
  useEffect(() => { active.current = null; setState(null); return () => { active.current = null; }; }, [profileKey]);
  const start = useCallback(() => {
    const token = {};
    active.current = token;
    setState({ profileKey, startedAt: performance.now(), progress: null });
    return {
      onRequestProgress(event: CoreRequestProgressEnvelope) {
        if (active.current !== token || currentKey.current !== profileKey) return;
        setState((previous) => previous ? { ...previous, progress: event.progress } : previous);
      },
      finish() {
        if (active.current !== token) return;
        active.current = null;
        setState(null);
      }
    };
  }, [profileKey]);
  return { state: state?.profileKey === profileKey ? state : null, start };
}

export function RequestProgress({ state }: { state?: RequestProgressState | null }) {
  const { t } = useTranslation();
  const [now, setNow] = useState(() => performance.now());
  const startedAt = state?.startedAt;
  useEffect(() => {
    if (startedAt === undefined) return;
    setNow(performance.now());
    // A local elapsed-time clock, never a background data refresh.
    const timer = setInterval(() => setNow(performance.now()), 1000);
    return () => clearInterval(timer);
  }, [startedAt]);
  if (!state) return null;
  const event = state.progress;
  const percent = event?.progress === undefined ? undefined : Math.round(event.progress * 100);
  const seconds = Math.max(0, Math.floor((now - state.startedAt) / 1000));
  const elapsed = `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, "0")}`;
  const label = t(`requestProgress.stages.${event?.stage ?? "waiting"}`, { defaultValue: t("requestProgress.working") });
  return <div className="mt-3 space-y-2 rounded-lg border border-[var(--border)] bg-[var(--surface)] p-3 text-sm" data-testid="request-progress">
    <div className="flex flex-wrap items-center justify-between gap-2"><span role="status">{label}</span><span className="tabular-nums text-[var(--muted)]">{t("requestProgress.elapsed", { time: elapsed })}</span></div>
    <progress aria-label={label} className="block h-2 w-full accent-[var(--accent)]" max={100} value={percent} />
    <div className="flex flex-wrap justify-between gap-2 text-xs text-[var(--muted)]">{event?.count !== undefined ? <span>{t("requestProgress.files", { count: event.count })}</span> : <span>{t("requestProgress.working")}</span>}{percent !== undefined ? <span>{t("requestProgress.stagePercent", { percent })}</span> : null}</div>
  </div>;
}
