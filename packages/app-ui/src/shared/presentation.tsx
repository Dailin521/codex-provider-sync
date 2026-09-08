import type { ProfileSelector } from "@codex-provider-sync/contracts";
import { CoreClientError } from "@codex-provider-sync/core-client";
import type { ReactNode, Ref } from "react";

import type { HostProfile } from "../types.js";
import { cn } from "../ui.js";

type Translate = (key: string, options?: Record<string, unknown>) => string;

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

export function safeErrorText(error: unknown, t: Translate): string {
  const fallback = t("errors.fallback");
  if (error instanceof CoreClientError) {
    if (error.code === "INVALID_INPUT" && error.dto.details?.reason === "provider-not-configured") {
      return t("errors.providerNotConfigured");
    }
    return t(`errors.${error.code}`, { defaultValue: fallback });
  }
  return fallback;
}

export function displayProfileName(profile: Pick<HostProfile, "id" | "name">, t: Translate): string {
  return profile.id === "default" ? t("profiles.defaultName") : profile.name;
}

export function displayWarningText(warning: string, t: Translate): string {
  if (warning === "Backup inventory refresh failed.") return t("warnings.backupInventory");
  if (warning === "Automatic backup cleanup failed.") return t("warnings.backupCleanup");
  if (warning === "Some encrypted histories may require their original Provider or account for continuation.") return t("warnings.encryptedHistory");
  if (warning === "One or more rollout files are locked and may be skipped.") return t("warnings.lockedSessions");
  if (warning === "The selected Provider has no default model; the root model will remain unchanged.") return t("warnings.missingDefaultModel");
  if (warning === "Project visibility diagnostics are unavailable; backup-first protection remains enabled.") return t("warnings.projectVisibility");
  if (warning === "SQLite Home relocation is confirmed; config.toml will not be restored.") return t("warnings.relocationConfig");
  if (/^Restore skipped /.test(warning)) return t("warnings.restoreSkipped");
  if (warning === "The operation made only part of the requested change. Retry it to converge, or restore the managed backup.") return t("warnings.partial");
  return t("warnings.additional");
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
