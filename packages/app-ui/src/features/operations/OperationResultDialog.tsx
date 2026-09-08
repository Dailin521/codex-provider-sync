import type { OperationOutcome, OperationResult } from "@codex-provider-sync/contracts";
import { Fragment } from "react";
import { useTranslation } from "react-i18next";

import { displayWarningText, formatDate } from "../../shared/presentation.js";
import { operationFailureLabel, operationStageLabel } from "../../shared/operation-feedback.js";
import { statusAlignment, type PostWriteStatus } from "../../shared/status-feedback.js";
import { Button, Card, Dialog } from "../../ui.js";
import { hasManyRewrittenSessions, SyncPerformanceTip } from "../sync/SyncPerformanceTip.js";

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
    "partialReason", "failedStage", "failureCode"
  ]);
  const numbers = new Set([
    "changedSessionFiles",
    "sqliteRowsUpdated",
    "sqliteProviderRowsUpdated",
    "sqliteModelRowsUpdated",
    "sqliteUserEventRowsUpdated",
    "sqliteCwdRowsUpdated",
    "updatedWorkspaceRoots",
    "savedWorkspaceRootCount",
    "resolvedOperationCount"
  ]);
  for (const [key, candidate] of Object.entries(value)) {
    if (key === "repairTargets" && Array.isArray(candidate)) {
      result.push([key, candidate.filter((entry) => typeof entry === "string").join(", ")]);
      continue;
    }
    if (!(strings.has(key) && typeof candidate === "string")
        && !(numbers.has(key) && typeof candidate === "number" && Number.isSafeInteger(candidate) && candidate >= 0)) continue;
    result.push([key, String(candidate)]);
  }
  return result;
}

function skippedRollouts(value: OperationResult["result"]): string[] {
  if (!value || typeof value !== "object" || Array.isArray(value)) return [];
  const candidate = value.skippedLockedRolloutFiles;
  return Array.isArray(candidate) ? candidate.filter((entry): entry is string => typeof entry === "string") : [];
}

function skippedChangedRollouts(value: OperationResult["result"]): string[] {
  if (!value || typeof value !== "object" || Array.isArray(value)) return [];
  const candidate = value.skippedChangedRolloutFiles;
  return Array.isArray(candidate) ? candidate.filter((entry): entry is string => typeof entry === "string") : [];
}

function displayResultValue(key: string, value: string, t: (key: string, options?: Record<string, unknown>) => string): string {
  if (key === "failedStage") return operationStageLabel(value, t);
  if (key === "failureCode") return operationFailureLabel(value, t);
  if (key === "partialReason") return t(`operationResult.partialReasons.${value}`, { defaultValue: t("global.partial") });
  if (key === "repairTargets") return value.split(", ").map((target) => t(`diagnostics.repairTargets.${target}`, { defaultValue: target })).join(", ");
  return value;
}

type Verification = {
  status: "verified" | "remaining" | "unavailable";
  remainingRolloutFiles: number;
  remainingSqliteRows: number;
  remainingWorkspaceRoots: number;
  skippedSessions: number;
};

function verification(value: OperationResult["result"]): Verification | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const candidate = value.verification;
  if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) return null;
  const item = candidate as Record<string, unknown>;
  if (!["verified", "remaining", "unavailable"].includes(String(item.status))) return null;
  const values = ["remainingRolloutFiles", "remainingSqliteRows", "remainingWorkspaceRoots", "skippedSessions"] as const;
  if (!values.every((key) => typeof item[key] === "number" && Number.isSafeInteger(item[key]) && (item[key] as number) >= 0)) return null;
  return {
    status: item.status as Verification["status"],
    remainingRolloutFiles: item.remainingRolloutFiles as number,
    remainingSqliteRows: item.remainingSqliteRows as number,
    remainingWorkspaceRoots: item.remainingWorkspaceRoots as number,
    skippedSessions: item.skippedSessions as number
  };
}

export function OperationResultDialog({ result, postWriteStatus, close, closeDisabled = false, openBackupRestore, reviewOperation, restoreFocus }: {
  result: OperationResult | null;
  postWriteStatus?: PostWriteStatus;
  close(): void;
  closeDisabled?: boolean;
  /** Opens the matching backup in Restore's preview form; it never starts a restore. */
  openBackupRestore?(backupId: string): void;
  reviewOperation?(): void;
  restoreFocus(): void;
}) {
  const { t, i18n } = useTranslation();
  const finalStatus = postWriteStatus?.operationId === result?.operationId ? postWriteStatus : undefined;
  const snapshot = finalStatus?.state === "received" ? finalStatus.snapshot : undefined;
  const finalAlignment = statusAlignment(snapshot);
  const presentation = result ? operationResultPresentation(result.outcome) : null;
  const entries = result ? publicResultEntries(result.result) : [];
  const skipped = result ? skippedRollouts(result.result) : [];
  const skippedChanged = result ? skippedChangedRollouts(result.result) : [];
  const skippedCount = skipped.length + skippedChanged.length;
  const partialReason = result?.result
    && typeof result.result === "object"
    && !Array.isArray(result.result)
    && typeof result.result.partialReason === "string"
    ? result.result.partialReason
    : null;
  const retryRecommended = result?.result
    && typeof result.result === "object"
    && !Array.isArray(result.result)
    && result.result.retryRecommended === true;
  const resultVerification = result ? verification(result.result) : null;
  const alert = result?.outcome === "recovery_required";
  return (
    <Dialog
      closeDisabled={closeDisabled}
      closeLabel={t("common.close")}
      description={closeDisabled ? t("operationResult.resolveBeforeClose") : undefined}
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
          {finalStatus ? <Card><h3 className="text-sm font-semibold">{t("ux.finalStatus")}</h3>{finalStatus.state === "checking" ? <p className="mt-2 text-sm">{t("ux.finalChecking")}</p> : <div className="mt-2 grid gap-2 text-sm">{snapshot ? <p>{t("common.provider")}: <span className="break-all font-semibold">{snapshot.currentProvider}</span></p> : null}<p>{finalAlignment === "unknown" ? t("ux.finalUnavailable") : t(`overview.${finalAlignment}`)}</p>{snapshot ? <p className="text-xs text-[var(--muted)]">{t("overview.snapshot")}: {formatDate(snapshot.snapshotAt, i18n.language)}</p> : null}</div>}</Card> : null}
          {result.backup ? <div className="rounded-lg border border-[var(--success)] bg-[var(--success-soft)] p-4 text-sm font-medium text-[var(--success)]"><p>{t("operationResult.backupCreated")}</p><p className="mt-2 break-all font-mono text-xs">{t("operationResult.backupId")}: {result.backup.backupId}</p>{openBackupRestore ? <Button className="mt-3" onClick={() => openBackupRestore(result.backup!.backupId)} type="button" variant="secondary">{t("operationResult.openBackupRestore")}</Button> : null}</div> : null}
          {entries.length ? <Card>
            <dl className="grid gap-3 text-sm">
              {entries.map(([key, value]) => <div key={key}><dt className="text-[var(--muted)]">{t(`operationResult.fields.${key}`, { defaultValue: key })}</dt><dd className="mt-1 break-words">{displayResultValue(key, value, t)}</dd></div>)}
            </dl>
          </Card> : null}
          {hasManyRewrittenSessions(result.operation, result.result) ? <SyncPerformanceTip key={result.operationId} afterOperation /> : null}
          {result.warnings.length ? <div><h3 className="font-semibold">{t("common.warnings")}</h3><ul className="mt-2 list-disc space-y-1 pl-5 text-sm">{result.warnings.map((warning, index) => <li key={`${index}-${warning}`}>{displayWarningText(warning, t)}</li>)}</ul></div> : null}
          {skippedCount ? <p className="rounded-lg border border-[var(--warning)] bg-[var(--warning-soft)] p-3 text-sm">{t("operationResult.skippedCount", { count: skippedCount })}</p> : null}
          {retryRecommended ? <p className="text-sm text-[var(--warning)]">{t(partialReason === "locked-session" ? "operationResult.retryAfterSession" : "operationResult.retryFreshPlan")}</p> : null}
          {retryRecommended && reviewOperation ? <Button onClick={reviewOperation} type="button" variant="secondary">{t("operationResult.reviewOperation")}</Button> : null}
          {resultVerification ? <Card><h3 className="text-sm font-semibold">{t("operationResult.verification.title")}</h3><p className="mt-1 text-sm text-[var(--muted)]">{t(`operationResult.verification.status.${resultVerification.status}`)}</p>{resultVerification.status === "remaining" ? <dl className="mt-3 grid gap-2 text-sm sm:grid-cols-2"><div><dt className="text-[var(--muted)]">{t("operationResult.verification.remainingRolloutFiles")}</dt><dd>{resultVerification.remainingRolloutFiles}</dd></div><div><dt className="text-[var(--muted)]">{t("operationResult.verification.remainingSqliteRows")}</dt><dd>{resultVerification.remainingSqliteRows}</dd></div><div><dt className="text-[var(--muted)]">{t("operationResult.verification.remainingWorkspaceRoots")}</dt><dd>{resultVerification.remainingWorkspaceRoots}</dd></div><div><dt className="text-[var(--muted)]">{t("operationResult.verification.skippedSessions")}</dt><dd>{resultVerification.skippedSessions}</dd></div></dl> : null}</Card> : null}
          {entries.length ? <p className="text-xs text-[var(--muted)]">{t("operationResult.changeCountersHint")}</p> : null}
          {closeDisabled ? <p className="text-sm text-[var(--danger)]">{t("operationResult.resolveBeforeClose")}</p> : null}
        </div>
      ) : null}
    </Dialog>
  );
}
