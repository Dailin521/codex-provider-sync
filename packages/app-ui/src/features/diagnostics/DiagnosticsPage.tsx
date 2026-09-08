import type { DiagnosticsSnapshot } from "@codex-provider-sync/contracts";
import { ArchiveRestore, RefreshCw } from "lucide-react";
import { Fragment } from "react";
import { useTranslation } from "react-i18next";
import { formatDate, KeyValue, PageHeading, safeErrorText } from "../../shared/presentation.js";
import { Badge, Button, Card } from "../../ui.js";
import { HistoryIntegrityPanel } from "./HistoryIntegrityPanel.js";
import { RepairControls, type RepairValues } from "./RepairControls.js";
import { RequestProgress, type RequestProgressState } from "../../shared/request-progress.js";

export type { RepairValues } from "./RepairControls.js";

export function DiagnosticsPage({ diagnostics, error, expired = false, loading, exporting, canExport, canRepair, repairDisabled, refresh, exportBundle, prepareRepair, scanProgress, repairProgress }: {
  scanProgress?: RequestProgressState | null;
  repairProgress?: RequestProgressState | null;
  diagnostics?: DiagnosticsSnapshot;
  error?: unknown;
  /** A successful write makes a cached, read-only scan historical until the user scans again. */
  expired?: boolean;
  loading: boolean;
  exporting: boolean;
  canExport: boolean;
  canRepair: boolean;
  repairDisabled: boolean;
  refresh(): void;
  exportBundle(): void;
  prepareRepair(values: RepairValues, trigger: HTMLButtonElement | null): Promise<void>;
}) {
  const { t, i18n } = useTranslation();
  const sections = diagnostics
    ? [["runtime", diagnostics.runtime], ["storage", diagnostics.storage], ["provider", diagnostics.provider], ["issues", diagnostics.issues], ["safety", diagnostics.safety]] as const
    : [];
  const summary = (value: unknown) => {
    if (value === null || value === undefined || value === "") return t("common.none");
    if (typeof value === "boolean") return <Badge tone={value ? "success" : "neutral"}>{value ? t("common.yes") : t("common.no")}</Badge>;
    if (typeof value === "string" || typeof value === "number") return String(value);
    if (Array.isArray(value)) return t("diagnostics.items", { count: value.length });
    if (typeof value === "object") return t("diagnostics.fieldsAvailable", { count: Object.keys(value).length });
    return t("common.unknown");
  };
  return (
    <Fragment>
      <PageHeading
        title={t("diagnostics.title")}
        subtitle={t("diagnostics.subtitle")}
      />
      <Card className="mb-4">
        <h2 className="font-semibold">{t("diagnostics.scanTitle")}</h2>
        <p className="mt-2 text-sm text-[var(--muted)]">{t("diagnostics.scanHint")}</p>
        <div className="mt-4 flex flex-wrap gap-2"><Button disabled={loading} onClick={refresh} type="button" variant="secondary"><RefreshCw size={16} />{loading ? t("common.loading") : error ? t("diagnostics.retryScan") : t("diagnostics.runScan")}</Button>{canExport ? <Button disabled={exporting || loading || Boolean(error) || !diagnostics} onClick={exportBundle} type="button" variant="secondary"><ArchiveRestore size={16} />{exporting ? t("diagnostics.exporting") : t("diagnostics.export")}</Button> : null}</div>
        {loading ? <p className="mt-3 text-sm text-[var(--muted)]" role="status">{t("diagnostics.scanning")}</p> : error ? <div className="mt-3 rounded-lg border border-[var(--danger)] p-3 text-sm" role="alert"><p className="font-semibold">{t("diagnostics.scanFailed")}</p><p>{safeErrorText(error, t)}</p><p>{t("diagnostics.scanFailedHint")}</p></div> : !diagnostics ? <p className="mt-3 text-sm text-[var(--muted)]">{t("diagnostics.notScanned")}</p> : null}
        {loading ? <RequestProgress state={scanProgress} /> : null}
        {diagnostics ? <p className="mt-3 text-sm text-[var(--muted)]">{t(error || loading ? "diagnostics.previousResult" : expired ? "diagnostics.expiredResult" : "diagnostics.scanCompleted", { time: formatDate(diagnostics.generatedAt, i18n.language) })}</p> : null}
        {diagnostics?.safety.rolloutScanComplete === false && !loading && !error ? <p className="mt-3 text-sm text-[var(--warning)]" role="status">{t("diagnostics.incompleteScan")}</p> : null}
      </Card>
      <div className="grid gap-4 lg:grid-cols-2">
        {sections.map(([key, value]) => (
          <Card key={key}>
            <h2 className="mb-2 font-semibold">{t(`diagnostics.${key}`)}</h2>
            {key === "issues" ? <div className="mb-3 space-y-2 text-sm text-[var(--muted)]">
              <p>{t("diagnostics.issuesHint")}</p>
              <p>{t("diagnostics.modelDifferenceHint")}</p>
              <p>{t("diagnostics.workspaceCountHint")}</p>
              <p>{t("diagnostics.encryptedHint")}</p>
            </div> : null}
            <dl>{Object.entries(value).map(([field, fieldValue]) => <KeyValue key={field} label={t(`diagnostics.fields.${field}`, { defaultValue: field })} value={summary(fieldValue)} />)}</dl>
            <details className="mt-3 rounded-[var(--radius-control)] border border-[var(--border)] bg-[var(--surface)] p-3 text-sm">
              <summary className="cursor-pointer font-medium">{t("diagnostics.technicalDetails")}</summary>
              <pre className="mt-3 max-h-72 overflow-auto whitespace-pre-wrap break-words text-xs leading-5 text-[var(--muted)]">{JSON.stringify(value, null, 2)}</pre>
            </details>
          </Card>
        ))}
      </div>
      {diagnostics ? <HistoryIntegrityPanel historyIntegrity={diagnostics.historyIntegrity} /> : null}
      {canRepair ? <RepairControls progress={repairProgress} diagnostics={diagnostics} fresh={Boolean(diagnostics) && !error && !expired && !loading} disabled={repairDisabled} prepare={prepareRepair} /> : null}
    </Fragment>
  );
}
