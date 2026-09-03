import type { OperationOutcome, OperationResult } from "@codex-provider-sync/contracts";
import { Fragment } from "react";
import { useTranslation } from "react-i18next";

import { Button, Card, Dialog } from "../../ui.js";

export type OperationResultTone = "success" | "warning" | "danger";

const OUTCOME_PRESENTATION: Record<OperationOutcome, {
  tone: OperationResultTone;
  titleKey: string;
  descriptionKey: string;
  toastKey: string;
}> = {
  completed: { tone: "success", titleKey: "operationResult.completed.title", descriptionKey: "operationResult.completed.description", toastKey: "global.completed" },
  partial: { tone: "warning", titleKey: "operationResult.partial.title", descriptionKey: "operationResult.partial.description", toastKey: "global.partial" },
  failed_rolled_back: { tone: "warning", titleKey: "operationResult.failedRolledBack.title", descriptionKey: "operationResult.failedRolledBack.description", toastKey: "global.failed" },
  recovery_required: { tone: "danger", titleKey: "operationResult.recoveryRequired.title", descriptionKey: "operationResult.recoveryRequired.description", toastKey: "global.failed" },
  cancelled: { tone: "warning", titleKey: "operationResult.cancelled.title", descriptionKey: "operationResult.cancelled.description", toastKey: "global.cancelled" },
  stale: { tone: "warning", titleKey: "operationResult.stale.title", descriptionKey: "operationResult.stale.description", toastKey: "global.stale" }
};

export function operationResultPresentation(outcome: OperationOutcome) {
  return OUTCOME_PRESENTATION[outcome];
}

function publicResultEntries(value: OperationResult["result"]): Array<[string, string]> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return [];
  const result: Array<[string, string]> = [];
  const strings = new Set([
    "targetProvider",
    "targetModel",
    "modelSource",
    "restoreOperationId",
    "preRestoreSnapshotId",
    "restoreJournalState"
  ]);
  const numbers = new Set([
    "backupDurationMs",
    "changedSessionFiles",
    "inPlaceSessionFiles",
    "rewrittenSessionFiles",
    "sqliteRowsUpdated",
    "sqliteProviderRowsUpdated",
    "sqliteUserEventRowsUpdated",
    "sqliteCwdRowsUpdated",
    "updatedWorkspaceRoots",
    "savedWorkspaceRootCount",
    "restoreVersion",
    "resolvedOperationCount"
  ]);
  const booleans = new Set(["commitAcknowledgementRecovered"]);
  for (const [key, candidate] of Object.entries(value)) {
    if (!(strings.has(key) && typeof candidate === "string")
        && !(numbers.has(key) && typeof candidate === "number" && Number.isSafeInteger(candidate) && candidate >= 0)
        && !(booleans.has(key) && typeof candidate === "boolean")) continue;
    result.push([key, String(candidate)]);
  }
  return result;
}

function skippedRollouts(value: OperationResult["result"]): string[] {
  if (!value || typeof value !== "object" || Array.isArray(value)) return [];
  const candidate = value.skippedLockedRolloutFiles;
  return Array.isArray(candidate) ? candidate.filter((entry): entry is string => typeof entry === "string") : [];
}

export function OperationResultDialog({ result, close, closeDisabled = false, restoreFocus }: {
  result: OperationResult | null;
  close(): void;
  closeDisabled?: boolean;
  restoreFocus(): void;
}) {
  const { t } = useTranslation();
  const presentation = result ? operationResultPresentation(result.outcome) : null;
  const entries = result ? publicResultEntries(result.result) : [];
  const skipped = result ? skippedRollouts(result.result) : [];
  const providerSync = result?.providerSync;
  const alert = result?.outcome === "recovery_required";
  return (
    <Dialog
      closeDisabled={closeDisabled}
      closeLabel={t("common.close")}
      description={result ? (closeDisabled ? `${result.operationId} · ${t("operationResult.resolveBeforeClose")}` : result.operationId) : undefined}
      footer={<Button disabled={closeDisabled} onClick={close} type="button">{t("common.close")}</Button>}
      onOpenChange={(open) => { if (!open && !closeDisabled) close(); }}
      open={Boolean(result)}
      restoreFocus={restoreFocus}
      title={t("operationResult.title")}
    >
      {result && presentation ? (
        <div aria-live="polite" className="grid gap-4" role={alert ? "alert" : "status"}>
          <div className={presentation.tone === "danger" ? "rounded-lg border border-[var(--danger)] bg-[var(--danger-soft)] p-4" : presentation.tone === "warning" ? "rounded-lg border border-[var(--warning)] bg-[var(--warning-soft)] p-4" : "rounded-lg border border-[var(--success)] bg-[var(--success-soft)] p-4"}>
            <h3 className="font-semibold">{t(presentation.titleKey)}</h3>
            <p className="mt-1 text-sm">{t(presentation.descriptionKey)}</p>
          </div>
          <Card>
            <dl className="grid gap-3 text-sm">
              <div><dt className="text-[var(--muted)]">{t("operationResult.operationId")}</dt><dd className="mt-1 break-all font-mono text-xs">{result.operationId}</dd></div>
              {result.backup ? <div><dt className="text-[var(--muted)]">{t("operationResult.backupId")}</dt><dd className="mt-1 break-all font-mono text-xs">{result.backup.backupId}</dd></div> : null}
              {entries.map(([key, value]) => <div key={key}><dt className="text-[var(--muted)]">{t(`operationResult.fields.${key}`, { defaultValue: key })}</dt><dd className="mt-1 break-words">{value}</dd></div>)}
            </dl>
          </Card>
          {providerSync ? <Card><h3 className="mb-2 text-sm font-semibold">{t("plan.providerSync")}</h3><dl className="grid gap-3 text-sm"><div><dt className="text-[var(--muted)]">{t("plan.fields.syncMode")}</dt><dd className="mt-1">{t(`plan.syncModes.${providerSync.mode}`)}</dd></div><div><dt className="text-[var(--muted)]">{t("plan.fields.rolloutScanScope")}</dt><dd className="mt-1">{t(`plan.syncModes.${providerSync.rolloutScanScope}`)}</dd></div><div><dt className="text-[var(--muted)]">{t("plan.fields.unchecked")}</dt><dd className="mt-1">{providerSync.unchecked.length ? providerSync.unchecked.join(", ") : t("common.none")}</dd></div></dl></Card> : null}
          {result.warnings.length ? <div><h3 className="font-semibold">{t("common.warnings")}</h3><ul className="mt-2 list-disc space-y-1 pl-5 text-sm">{result.warnings.map((warning, index) => <li key={`${index}-${warning}`}>{warning}</li>)}</ul></div> : null}
          {skipped.length ? <div><h3 className="font-semibold">{t("operationResult.skippedRollouts")}</h3><ul className="mt-2 list-disc space-y-1 pl-5 font-mono text-xs">{skipped.map((file) => <li key={file}>{file}</li>)}</ul></div> : null}
          {closeDisabled ? <p className="text-sm text-[var(--danger)]">{t("operationResult.resolveBeforeClose")}</p> : null}
        </div>
      ) : null}
    </Dialog>
  );
}
