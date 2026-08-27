import type { ProfileSelector } from "@codex-provider-sync/contracts";
import { CoreClientError } from "@codex-provider-sync/core-client";
import type { ReactNode, Ref } from "react";

import type { HostProfile } from "../types.js";
import { cn } from "../ui.js";

export function profileSelector(profile: HostProfile): ProfileSelector {
  return { profileId: profile.id, profileRevision: profile.revision };
}

export function formatBytes(bytes: number): string {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = Number.isFinite(bytes) ? bytes : 0;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return unit === 0
    ? `${value} ${units[unit]}`
    : `${value.toFixed(value >= 10 ? 1 : 2)} ${units[unit]}`;
}

export function formatDate(value?: string | null, locale = "en"): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) return "—";
  return new Intl.DateTimeFormat(locale, {
    dateStyle: "medium",
    timeStyle: "medium"
  }).format(date);
}

export function safeErrorText(error: unknown, fallback: string): string {
  if (error instanceof CoreClientError) return `${error.dto.message} (${error.code})`;
  return fallback;
}

export function PageHeading({
  title,
  subtitle,
  action,
  headingRef,
  headingTabIndex
}: {
  title: string;
  subtitle: string;
  action?: ReactNode;
  headingRef?: Ref<HTMLHeadingElement>;
  headingTabIndex?: number;
}) {
  return (
    <div className="mb-[var(--space-6)] flex flex-wrap items-start justify-between gap-[var(--space-4)]">
      <div>
        <h1 className="[font-size:var(--text-2xl)] leading-[var(--leading-tight)] font-bold tracking-tight text-[var(--text)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--focus)]" ref={headingRef} tabIndex={headingTabIndex}>{title}</h1>
        <p className="mt-[var(--space-1)] max-w-3xl [font-size:var(--text-sm)] leading-[var(--leading-relaxed)] text-[var(--muted)]">{subtitle}</p>
      </div>
      {action}
    </div>
  );
}

export function KeyValue({
  label,
  value,
  mono = false
}: {
  label: string;
  value: ReactNode;
  mono?: boolean;
}) {
  return (
    <div className="grid gap-1 border-b border-[var(--border)] py-3 last:border-0 sm:grid-cols-[180px_1fr]">
      <dt className="text-sm text-[var(--muted)]">{label}</dt>
      <dd className={cn(
        "min-w-0 break-words text-sm font-medium text-[var(--text)]",
        mono && "font-mono text-xs"
      )}>
        {value}
      </dd>
    </div>
  );
}
