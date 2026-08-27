import type { PlanSummary, ProgressEvent } from "@codex-provider-sync/contracts";
import { Fragment } from "react";
import { useTranslation } from "react-i18next";

import { formatDate, KeyValue } from "../../shared/presentation.js";
import { Button, Card, Dialog } from "../../ui.js";

function displayPlanValue(key: string, value: unknown, t: (key: string, options?: Record<string, unknown>) => string): string {
  if (value === null || value === undefined || value === "") return t("common.none");
  if (typeof value === "boolean") return value ? t("common.yes") : t("common.no");
  if (key === "modelMode" && typeof value === "string") {
    return t(`plan.modelModes.${value}`, { defaultValue: value });
  }
  if (Array.isArray(value)) return t("plan.items", { count: value.length });
  return String(value);
}

export function PlanReview({
  plan,
  applying,
  cancelling,
  confirmDisabled = false,
  operationId,
  progress,
  close,
  apply,
  cancel,
  restoreFocus
}: {
  plan: PlanSummary | null;
  applying: boolean;
  cancelling: boolean;
  confirmDisabled?: boolean;
  operationId: string | null;
  progress: ProgressEvent | null;
  close(): void;
  apply(): void;
  cancel(): void;
  restoreFocus(): void;
}) {
  const { t, i18n } = useTranslation();
  const operationLabel = plan
    ? t(`plan.operations.${plan.operation}`, { defaultValue: plan.operation })
    : "";
  const targetRows = plan ? [
    ["provider", t("common.provider")],
    ["model", t("common.model")],
    ["modelMode", t("plan.fields.modelMode")],
    ["backupId", t("operationResult.backupId")],
    ["restoreConfig", t("plan.fields.restoreConfig")],
    ["restoreDatabase", t("plan.fields.restoreDatabase")],
    ["restoreSessions", t("plan.fields.restoreSessions")],
    ["allowSqliteHomeRelocation", t("plan.fields.relocation")]
  ].filter(([key]) => key in plan.target) : [];
  const impactRows = plan ? [
    ["rolloutFilesToChange", t("plan.fields.rolloutFiles")],
    ["sqliteRowsToChange", t("plan.fields.sqliteRows")],
    ["workspaceRootsToChange", t("plan.fields.workspaceRoots")],
    ["stateDbFilesToChange", t("plan.fields.stateDbFiles")],
    ["configFilesToChange", t("plan.fields.configFiles")],
    ["lockedRolloutFiles", t("plan.fields.lockedRollouts")]
  ].filter(([key]) => key in plan.impact) : [];
  return (
    <Dialog
      closeDisabled={applying}
      closeLabel={t("common.close")}
      description={plan ? `${operationLabel} · ${t("plan.expires")} ${formatDate(plan.expiresAt, i18n.language)}` : undefined}
      footer={<Fragment><Button disabled={applying} onClick={close} type="button" variant="secondary">{t("common.close")}</Button>{applying ? <Button disabled={cancelling} onClick={cancel} type="button" variant="danger">{cancelling ? t("plan.cancelling") : t("plan.cancelOperation")}</Button> : <Button disabled={confirmDisabled} onClick={apply} type="button">{t("common.confirm")}</Button>}</Fragment>}
      onOpenChange={(open) => { if (!open && !applying) close(); }}
      open={Boolean(plan)}
      restoreFocus={restoreFocus}
      title={t("plan.title")}
    >
      {plan ? (
        <div className="grid gap-4">
          <Card>
            <h3 className="mb-2 text-sm font-semibold">{t("plan.target")}</h3>
            <dl>{targetRows.map(([key, label]) => <KeyValue key={key} label={label} value={displayPlanValue(key, plan.target[key], t)} />)}</dl>
          </Card>
          <Card>
            <h3 className="mb-2 text-sm font-semibold">{t("plan.impact")}</h3>
            <dl>{impactRows.map(([key, label]) => <KeyValue key={key} label={label} value={displayPlanValue(key, plan.impact[key], t)} />)}</dl>
          </Card>
          {plan.impact.backupExpected === true ? <div className="rounded-[var(--radius-control)] border border-[var(--success)] bg-[var(--success-soft)] p-4 text-sm font-medium text-[var(--success)]">{t("plan.backupExpected")}</div> : null}
          {plan.warnings.length ? <div className="rounded-lg border border-[var(--warning)] bg-[var(--warning-soft)] p-4"><h3 className="font-semibold">{t("common.warnings")}</h3><ul className="mt-2 list-disc space-y-1 pl-5 text-sm">{plan.warnings.map((warning, index) => <li key={`${index}-${warning}`}>{warning}</li>)}</ul></div> : null}
          {applying ? <Card aria-live="polite" role="status"><h3 className="text-sm font-semibold">{t("plan.progress")}</h3><div className="mt-2 font-mono text-xs text-[var(--muted)]">{operationId ?? t("plan.starting")}</div>{progress ? <div className="mt-3 grid gap-2"><div className="text-sm">{t(`plan.stages.${progress.stage}`, { defaultValue: progress.stage })} · {t(`plan.statuses.${progress.status}`, { defaultValue: progress.status })}{progress.count === undefined ? "" : ` · ${progress.count}`}</div>{progress.progress === undefined ? null : <progress aria-label={t("plan.progress")} className="w-full" max={1} value={progress.progress} />}</div> : null}{cancelling ? <p className="mt-3 text-sm text-[var(--warning)]">{t("plan.cancelPending")}</p> : null}</Card> : null}
          {confirmDisabled && !applying ? <p className="text-sm font-medium text-[var(--warning)]" role="status">{t("plan.writeBlocked")}</p> : null}
          <p className="text-sm text-[var(--muted)]">{t("plan.exactApply")}</p>
          <details className="rounded-[var(--radius-control)] border border-[var(--border)] bg-[var(--surface)] p-3 text-sm">
            <summary className="cursor-pointer font-medium">{t("plan.technicalDetails")}</summary>
            <pre className="mt-3 max-h-52 overflow-auto whitespace-pre-wrap break-words text-xs text-[var(--muted)]">{JSON.stringify({ target: plan.target, impact: plan.impact }, null, 2)}</pre>
          </details>
        </div>
      ) : null}
    </Dialog>
  );
}
