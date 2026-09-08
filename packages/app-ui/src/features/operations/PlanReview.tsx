import type { PlanSummary, ProgressEvent } from "@codex-provider-sync/contracts";
import { Fragment, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { displayWarningText, formatDate, KeyValue } from "../../shared/presentation.js";
import { Button, Card, Dialog } from "../../ui.js";
import { RequestProgress, type RequestProgressState } from "../../shared/request-progress.js";

function displayPlanValue(key: string, value: unknown, t: (key: string, options?: Record<string, unknown>) => string): string {
  if (value === null || value === undefined || value === "") return t("common.none");
  if (typeof value === "boolean") return value ? t("common.yes") : t("common.no");
  if (key === "modelMode" && typeof value === "string") {
    return t(`plan.modelModes.${value}`, { defaultValue: value });
  }
  if (key === "targets" && Array.isArray(value)) {
    return value.map((target) => t(`diagnostics.repairTargets.${String(target)}`, { defaultValue: String(target) })).join(", ");
  }
  if (Array.isArray(value)) return t("plan.items", { count: value.length });
  return String(value);
}

type RepairPreviewChange = { target: "models" | "cwd" | "userEvent"; before: string; after: string };
type RepairPreview = { sessionId: string; changes: RepairPreviewChange[] };

function repairPreview(plan: PlanSummary): RepairPreview[] {
  const candidate = plan.impact.repairPreview;
  if (!Array.isArray(candidate)) return [];
  return candidate.flatMap((entry) => {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) return [];
    const item = entry as Record<string, unknown>;
    if (typeof item.sessionId !== "string" || !Array.isArray(item.changes)) return [];
    const changes = item.changes.flatMap((change) => {
      if (!change || typeof change !== "object" || Array.isArray(change)) return [];
      const value = change as Record<string, unknown>;
      return (["models", "cwd", "userEvent"].includes(String(value.target)) && typeof value.before === "string" && typeof value.after === "string")
        ? [{ target: value.target as RepairPreviewChange["target"], before: value.before, after: value.after }]
        : [];
    });
    return changes.length ? [{ sessionId: item.sessionId, changes }] : [];
  });
}

function repairSelection(plan: PlanSummary, preview: RepairPreview[]): string[] | null {
  const scope = plan.target.scope;
  if (scope !== "selected") return null;
  const ids = plan.target.sessionIds;
  return Array.isArray(ids) && ids.every((id) => typeof id === "string") ? ids : preview.map((entry) => entry.sessionId);
}

function displayRepairMarker(target: RepairPreviewChange["target"], value: string, t: (key: string, options?: Record<string, unknown>) => string): string {
  if (target === "models") return value;
  const key = value === "different" || value === "rollout-cwd" || value === "false" || value === "true" ? value : null;
  return key ? t(`plan.repairPreview.markers.${key}`) : value;
}

export function PlanReview({
  plan,
  applying,
  cancelling,
  confirmDisabled = false,
  currentModel,
  directSyncPhase = null,
  progress,
  repairSelectionPending = false,
  repairProgress,
  repairSelectionFailed = false,
  repairDraftChanged,
  refineRepairSessions,
  close,
  apply,
  cancel,
  restoreFocus
}: {
  plan: PlanSummary | null;
  applying: boolean;
  cancelling: boolean;
  confirmDisabled?: boolean;
  currentModel?: string | null;
  directSyncPhase?: "preparing" | "applying" | null;
  progress: ProgressEvent | null;
  repairSelectionPending?: boolean;
  repairProgress?: RequestProgressState | null;
  repairSelectionFailed?: boolean;
  repairDraftChanged?(changed: boolean): void;
  /** Applies the locally selected native session IDs to a newly prepared plan. */
  refineRepairSessions?(sessionIds: string[] | null): void;
  close(): void;
  apply(): void;
  cancel(): void;
  restoreFocus(): void;
}) {
  const { t, i18n } = useTranslation();
  const operationLabel = plan
    ? t(`plan.operations.${plan.operation}`, { defaultValue: plan.operation })
    : "";
  const dialogTitle = plan
    ? t(`plan.titles.${plan.operation}`, { defaultValue: t("plan.title") })
    : t("plan.title");
  const confirmLabel = plan
    ? t(`plan.confirmActions.${plan.operation}`, { defaultValue: t("common.confirm") })
    : t("common.confirm");
  const targetRows = plan ? [
    ["provider", t("common.provider")],
    ...(plan.operation === "switch" ? [] : [["model", t("common.model")]]),
    ["modelMode", t("plan.fields.modelMode")],
    ["targets", t("plan.fields.repairTargets")],
    ["backupId", t("operationResult.backupId")],
    ["restoreConfig", t("plan.fields.restoreConfig")],
    ["restoreDatabase", t("plan.fields.restoreDatabase")],
    ["restoreSessions", t("plan.fields.restoreSessions")],
    ["allowSqliteHomeRelocation", t("plan.fields.relocation")]
  ].filter(([key]) => key in plan.target) : [];
  const impactRows = plan ? [
    ["rolloutFilesToChange", t("plan.fields.rolloutFiles")],
    ...(plan.operation === "repair" ? [
      ["repairPreviewTotal", t("plan.fields.affectedSessions")],
      ["sqliteRowsToChange", t("plan.fields.sqliteFields")],
      ["sqliteModelRowsToChange", t("plan.fields.sqliteModels")],
      ["sqliteCwdRowsToChange", t("plan.fields.sqliteCwd")],
      ["sqliteUserEventRowsToChange", t("plan.fields.sqliteUserEvent")]
    ] : [["sqliteRowsToChange", t("plan.fields.sqliteRows")]]),
    ["workspaceRootsToChange", t("plan.fields.workspaceSettings")],
    ["stateDbFilesToChange", t("plan.fields.stateDbFiles")],
    ["configFilesToChange", t("plan.fields.configFiles")],
    ["lockedRolloutFiles", t("plan.fields.lockedRollouts")]
  ].filter(([key]) => key in plan.impact) : [];
  const modelTransition = plan?.operation === "switch"
    ? `${displayPlanValue("model", currentModel, t)} → ${displayPlanValue("model", plan.target.model, t)}`
    : null;
  const activity = plan?.impact.sessionActivity;
  const activityCount = activity && typeof activity === "object" && !Array.isArray(activity)
    && activity.state === "checked" && typeof activity.count === "number" ? activity.count : t("overview.usageUnknown");
  const repairIsGlobal = plan?.operation === "repair" && Array.isArray(plan.target.targets) && plan.target.targets.includes("workspaceRoots");
  const workspaceChanges = ["savedRoots", "projectOrder", "activeRoots", "labels", "openTargets", "settingsBackup"].filter((kind) =>
    plan?.operation === "repair" && Array.isArray(plan.impact.workspaceSettingsChangeKinds) && plan.impact.workspaceSettingsChangeKinds.includes(kind));
  const preview = useMemo(() => plan?.operation === "repair" ? repairPreview(plan) : [], [plan]);
  const planSelection = useMemo(() => plan?.operation === "repair" ? repairSelection(plan, preview) : null, [plan, preview]);
  const [selectedRepairSessionIds, setSelectedRepairSessionIds] = useState<string[] | null>(planSelection);
  useEffect(() => { setSelectedRepairSessionIds(planSelection); }, [plan?.planId, planSelection]);
  const repairDraftIsDifferent = plan?.operation === "repair" && !repairIsGlobal
    ? (selectedRepairSessionIds === null) !== (planSelection === null)
      || JSON.stringify(selectedRepairSessionIds ?? []) !== JSON.stringify(planSelection ?? [])
    : false;
  useEffect(() => { repairDraftChanged?.(repairDraftIsDifferent); }, [repairDraftChanged, repairDraftIsDifferent]);
  const setRepairDraft = (next: string[] | null) => {
    if (!plan || repairIsGlobal || repairSelectionPending || applying) return;
    setSelectedRepairSessionIds(next);
  };
  return (
    <Dialog
      closeDisabled={applying || repairSelectionPending}
      closeLabel={t("common.close")}
      description={directSyncPhase ? t("sync.directHint") : plan ? `${operationLabel} · ${t("plan.expires")} ${formatDate(plan.expiresAt, i18n.language)}` : undefined}
      footer={<Fragment><Button disabled={applying || repairSelectionPending} onClick={close} type="button" variant="secondary">{t("common.close")}</Button>{applying ? <Button disabled={cancelling} onClick={cancel} type="button" variant="danger">{cancelling ? t("plan.cancelling") : t("plan.cancelOperation")}</Button> : <Button disabled={confirmDisabled || repairSelectionFailed || repairSelectionPending || repairDraftIsDifferent} onClick={apply} type="button">{confirmLabel}</Button>}</Fragment>}
      onOpenChange={(open) => { if (!open && !applying && !repairSelectionPending) close(); }}
      open={Boolean(plan || directSyncPhase)}
      restoreFocus={restoreFocus}
      title={directSyncPhase ? t(directSyncPhase === "preparing" ? "sync.preparingDirect" : "sync.runningDirect") : dialogTitle}
    >
      {directSyncPhase ? <Card aria-live="polite" role="status">
        <h3 className="text-sm font-semibold">{t("plan.progress")}</h3>
        <p className="mt-2 text-sm">{progress ? `${t(`plan.stages.${progress.stage}`, { defaultValue: t("common.processing") })} · ${t(`plan.statuses.${progress.status}`, { defaultValue: t("common.processing") })}${progress.count === undefined ? "" : ` · ${progress.count}`}` : t(directSyncPhase === "preparing" ? "sync.preparingDirect" : "plan.starting")}</p>
        {progress?.progress === undefined ? null : <progress aria-label={t("plan.progress")} className="mt-3 w-full" max={1} value={progress.progress} />}
        {cancelling ? <p className="mt-3 text-sm text-[var(--warning)]">{t("plan.cancelPending")}</p> : null}
      </Card> : plan ? (
        <div className="grid gap-4">
          <Card>
            <h3 className="mb-2 text-sm font-semibold">{t("plan.target")}</h3>
            <dl>{modelTransition ? <KeyValue label={t("plan.fields.rootModelChange")} value={modelTransition} /> : null}{targetRows.map(([key, label]) => <KeyValue key={key} label={label} value={displayPlanValue(key, plan.target[key], t)} />)}</dl>
          </Card>
          {plan.operation === "repair" ? <Card>
            <h3 className="mb-2 text-sm font-semibold">{t("plan.repairPreview.effectsTitle")}</h3>
            <ul className="mb-3 grid gap-2 text-sm text-[var(--muted)]">{(["models", "cwd", "userEvent", "workspaceRoots"] as const).filter((target) => Array.isArray(plan.target.targets) && plan.target.targets.includes(target)).map((target) => <li key={target}>{t(`diagnostics.repairTargetHints.${target}`)}</li>)}</ul>
            <p className="mb-4 text-sm text-[var(--muted)]">{t("plan.repairPreview.unchanged")}</p>
            <h3 className="mb-2 text-sm font-semibold">{t("plan.repairPreview.title")}</h3>
            <p className="text-sm text-[var(--muted)]">{t(repairIsGlobal ? "plan.repairPreview.workspaceGlobal" : "plan.repairPreview.hint")}</p>
              {!repairIsGlobal ? <div className="mt-3 flex flex-wrap gap-3 text-sm">
                <label className="flex items-center gap-2"><input checked={selectedRepairSessionIds === null} disabled={repairSelectionPending || applying} name="repair-scope" onChange={() => setRepairDraft(null)} type="radio" />{t("plan.repairPreview.all")}</label>
                <label className="flex items-center gap-2"><input checked={selectedRepairSessionIds !== null} disabled={repairSelectionPending || applying || preview.length === 0} name="repair-scope" onChange={() => setRepairDraft(selectedRepairSessionIds ?? [])} type="radio" />{t("plan.repairPreview.selected")}</label>
              </div> : null}
              {preview.length ? <div className="mt-3 grid max-h-80 gap-2 overflow-y-auto overscroll-contain" role="region" aria-label={t("plan.repairPreview.title")} tabIndex={0}>{preview.map((entry) => {
                const checked = selectedRepairSessionIds?.includes(entry.sessionId) ?? false;
                return <div className="rounded-[var(--radius-control)] border border-[var(--border)] bg-[var(--surface)] p-3 text-sm" key={entry.sessionId}><label className="flex items-start gap-2">{!repairIsGlobal ? <input aria-label={t("plan.repairPreview.selectSession", { sessionId: entry.sessionId })} checked={checked} disabled={repairSelectionPending || applying} onChange={(event) => {
                  const prior = selectedRepairSessionIds ?? [];
                  const next = event.target.checked ? [...new Set([...prior, entry.sessionId])] : prior.filter((id) => id !== entry.sessionId);
                  setRepairDraft(next);
                }} type="checkbox" /> : null}<span className="break-all font-mono text-xs">{entry.sessionId}</span></label><ul className="mt-2 grid gap-1 text-xs text-[var(--muted)]">{entry.changes.map((change, index) => <li key={`${index}-${change.target}`}>{t(`plan.repairPreview.changes.${change.target}`)}: {displayRepairMarker(change.target, change.before, t)} → {displayRepairMarker(change.target, change.after, t)}</li>)}</ul></div>;
              })}</div> : <p className="mt-3 text-sm text-[var(--muted)]">{t("plan.repairPreview.none")}</p>}
              {typeof plan.impact.repairPreviewTotal === "number" ? <p className="mt-3 text-xs text-[var(--muted)]">{t("plan.repairPreview.total", { count: plan.impact.repairPreviewTotal })}{plan.impact.repairPreviewTruncated === true ? ` · ${t("plan.repairPreview.truncated")}` : ""}</p> : null}
              {!repairIsGlobal && (repairDraftIsDifferent || repairSelectionFailed) ? <div className="mt-3 flex flex-wrap items-center gap-3"><p className="text-sm font-medium text-[var(--warning)]" role="status">{t("plan.repairPreview.selectionChanged")}</p><Button disabled={repairSelectionPending || applying || (Array.isArray(selectedRepairSessionIds) && selectedRepairSessionIds.length === 0)} onClick={() => refineRepairSessions?.(selectedRepairSessionIds)} type="button" variant="secondary">{t("plan.repairPreview.update")}</Button></div> : null}
              {repairSelectionPending ? <><p className="mt-3 text-sm font-medium text-[var(--warning)]" role="status">{t("plan.repairPreview.regenerating")}</p><RequestProgress state={repairProgress} /></> : null}
              {repairSelectionFailed ? <p className="mt-3 text-sm font-medium text-[var(--danger)]" role="alert">{t("plan.repairPreview.refineFailed")}</p> : null}
          </Card> : null}
          {plan.operation === "switch" ? <p className="rounded-[var(--radius-control)] border border-[var(--border)] bg-[var(--surface)] p-3 text-sm text-[var(--muted)]">{t("plan.historyModelsUnaffected")}</p> : null}
          <Card>
            <h3 className="mb-2 text-sm font-semibold">{t("plan.impact")}</h3>
            <dl>{plan.operation === "sync" || plan.operation === "switch" ? <KeyValue label={t("overview.locked")} value={activityCount} /> : null}{impactRows.map(([key, label]) => <KeyValue key={key} label={label} value={displayPlanValue(key, plan.impact[key], t)} />)}</dl>
            {workspaceChanges.length ? <ul className="mt-3 list-disc space-y-1 pl-5 text-sm text-[var(--muted)]" aria-label={t("plan.fields.workspaceSettings")}>{workspaceChanges.map((kind) => <li key={kind}>{t(`plan.workspaceChanges.${kind}`)}</li>)}</ul> : null}
          </Card>
          {plan.impact.backupExpected === true ? <div className="rounded-[var(--radius-control)] border border-[var(--success)] bg-[var(--success-soft)] p-4 text-sm font-medium text-[var(--success)]">{t("plan.backupExpected")}</div> : null}
          {plan.warnings.length ? <div className="rounded-lg border border-[var(--warning)] bg-[var(--warning-soft)] p-4"><h3 className="font-semibold">{t("common.warnings")}</h3><ul className="mt-2 list-disc space-y-1 pl-5 text-sm">{plan.warnings.map((warning, index) => <li key={`${index}-${warning}`}>{displayWarningText(warning, t)}</li>)}</ul></div> : null}
          {applying ? <Card aria-live="polite" role="status"><h3 className="text-sm font-semibold">{t("plan.progress")}</h3><div className="mt-2 text-xs text-[var(--muted)]">{t("plan.starting")}</div>{progress ? <div className="mt-3 grid gap-2"><div className="text-sm">{t(`plan.stages.${progress.stage}`, { defaultValue: t("common.processing") })} · {t(`plan.statuses.${progress.status}`, { defaultValue: t("common.processing") })}{progress.count === undefined ? "" : ` · ${progress.count}`}</div>{progress.progress === undefined ? null : <progress aria-label={t("plan.progress")} className="w-full" max={1} value={progress.progress} />}</div> : null}{cancelling ? <p className="mt-3 text-sm text-[var(--warning)]">{t("plan.cancelPending")}</p> : null}</Card> : null}
          {confirmDisabled && !applying ? <p className="text-sm font-medium text-[var(--warning)]" role="status">{t("plan.writeBlocked")}</p> : null}
          <p className="text-sm text-[var(--muted)]">{t("plan.exactApply")}</p>
        </div>
      ) : null}
    </Dialog>
  );
}
