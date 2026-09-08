import type { DiagnosticsSnapshot } from "@codex-provider-sync/contracts";
import { zodResolver } from "@hookform/resolvers/zod";
import { Wrench } from "lucide-react";
import { useId, useRef } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import type { z } from "zod";

import { repairSchema } from "../../schemas.js";
import { Button, Card } from "../../ui.js";
import { RequestProgress, type RequestProgressState } from "../../shared/request-progress.js";

export type RepairValues = z.infer<typeof repairSchema>;
type RepairTarget = keyof RepairValues;
const repairTargets = ["cwd", "userEvent", "workspaceRoots"] as const;
const emptyTargets: RepairValues = { models: false, cwd: false, userEvent: false, workspaceRoots: false };

function RepairForm({ targets, disabled, adjustment = false, prepare, progress }: {
  progress?: RequestProgressState | null;
  targets: readonly RepairTarget[];
  disabled: boolean;
  adjustment?: boolean;
  prepare(values: RepairValues, trigger: HTMLButtonElement | null): Promise<void>;
}) {
  const { t } = useTranslation();
  const id = useId();
  const button = useRef<HTMLButtonElement>(null);
  const form = useForm<RepairValues>({ resolver: zodResolver(repairSchema), defaultValues: { ...emptyTargets } });
  const blocked = disabled || form.formState.isSubmitting;
  return <form className="mt-4 grid gap-4" onSubmit={form.handleSubmit(async (values) => {
    if (disabled) return;
    // Each disclosure owns an independent selection. Hidden targets cannot leak into a preview.
    const selected = { ...emptyTargets };
    for (const target of targets) selected[target] = values[target];
    await prepare(selected, button.current);
  })}>
    <div className="grid gap-3">
      {targets.map((target) => <label className="grid gap-2 rounded-lg border border-[var(--border)] bg-[var(--surface)] p-3" key={target}>
        <span className="flex min-h-8 items-center gap-3"><input aria-label={t(`diagnostics.repairTargets.${target}`)} aria-describedby={`${id}-${target}`} data-repair-target={target} disabled={blocked} type="checkbox" {...form.register(target)} /><span className="font-medium">{t(`diagnostics.repairTargets.${target}`)}</span></span>
        <span className="pl-7 text-sm text-[var(--muted)]" id={`${id}-${target}`}>{t(`diagnostics.repairTargetHints.${target}`)}</span>
      </label>)}
    </div>
    {form.formState.errors.models ? <p className="text-sm text-[var(--danger)]" role="alert">{t("diagnostics.repairTargetRequired")}</p> : null}
    {!adjustment && form.watch("workspaceRoots") ? <p className="text-sm text-[var(--muted)]">{t("diagnostics.workspaceRootsIncludesCwd")}</p> : null}
    <Button disabled={blocked} ref={button} type="submit"><Wrench size={16} />{t(adjustment ? "diagnostics.previewAdjustment" : "diagnostics.prepareRepair")}</Button>
    {form.formState.isSubmitting ? <RequestProgress state={progress} /> : null}
  </form>;
}

export function RepairControls({ diagnostics, fresh, disabled, prepare, progress }: {
  progress?: RequestProgressState | null;
  diagnostics?: DiagnosticsSnapshot;
  fresh: boolean;
  disabled: boolean;
  prepare(values: RepairValues, trigger: HTMLButtonElement | null): Promise<void>;
}) {
  const { t } = useTranslation();
  const disclosure = useRef<HTMLDetailsElement>(null);
  const safety = diagnostics?.safety;
  // Missing/incomplete checks are not a clean bill of health and cannot produce recommendations.
  const usable = fresh && !disabled && safety?.rolloutScanComplete === true
    && safety.pendingRecovery === false && safety.operationInProgress === null && safety.lockedRolloutCount === 0
    && diagnostics?.storage.stateDbFound === true && diagnostics.storage.sqliteSupported === true
    && diagnostics.provider.sqliteCounts !== null && typeof diagnostics.provider.sqliteCounts === "object";
  const fields = { cwd: "cwdRowsNeedingRepair", userEvent: "userEventRowsNeedingRepair", workspaceRoots: "workspaceRootsNeedingRepair" } as const;
  const findings = usable ? repairTargets.flatMap((target) => {
    const count = diagnostics?.issues[fields[target]];
    return typeof count === "number" && Number.isSafeInteger(count) && count > 0 ? [{ target, count }] : [];
  }) : [];
  return <div className="mt-4 grid max-w-3xl gap-4">
    {findings.length ? <Card>
      <h2 className="font-semibold">{t("diagnostics.availableRepairs")}</h2>
      <ul className="mt-3 grid gap-3">{findings.map(({ target, count }) => <li className="flex flex-wrap items-center justify-between gap-2" key={target}>
        <span className="text-sm">{t(`diagnostics.findings.${target}`, { count })}</span>
        <Button aria-label={t("diagnostics.viewSpecificRepair", { target: t(`diagnostics.repairTargets.${target}`) })} onClick={() => {
          if (disabled || !disclosure.current) return;
          disclosure.current.open = true;
          disclosure.current.querySelector<HTMLInputElement>(`input[data-repair-target="${target}"]`)?.focus();
        }} type="button" variant="secondary">{t("diagnostics.viewRepair")}</Button>
      </li>)}</ul>
    </Card> : null}
    <Card><details ref={disclosure}>
      <summary className="cursor-pointer font-semibold focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--accent)]">{t("diagnostics.repairTitle")}</summary>
      <p className="mt-2 text-sm text-[var(--muted)]">{t("diagnostics.repairHint")}</p>
      <p className="mt-2 text-sm text-[var(--muted)]">{t("diagnostics.repairScope")}</p>
      <RepairForm progress={progress} disabled={disabled} targets={repairTargets} prepare={prepare} />
    </details></Card>
    <Card><details>
      <summary className="cursor-pointer font-semibold focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--accent)]">{t("diagnostics.adjustmentTitle")}</summary>
      <p className="mt-2 text-sm text-[var(--muted)]">{t("diagnostics.adjustmentHint")}</p>
      <RepairForm progress={progress} adjustment disabled={disabled} targets={["models"]} prepare={prepare} />
    </details></Card>
  </div>;
}
