import type { DiagnosticsSnapshot } from "@codex-provider-sync/contracts";
import { ArchiveRestore, RefreshCw } from "lucide-react";
import { Fragment } from "react";
import { useTranslation } from "react-i18next";

import { KeyValue, PageHeading } from "../../shared/presentation.js";
import { Badge, Button, Card } from "../../ui.js";

export function DiagnosticsPage({ diagnostics, loading, exporting, canExport, refresh, exportBundle }: {
  diagnostics?: DiagnosticsSnapshot;
  loading: boolean;
  exporting: boolean;
  canExport: boolean;
  refresh(): void;
  exportBundle(): void;
}) {
  const { t } = useTranslation();
  const sections = diagnostics
    ? [["runtime", diagnostics.runtime], ["storage", diagnostics.storage], ["provider", diagnostics.provider], ["safety", diagnostics.safety]] as const
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
        action={<div className="flex flex-wrap gap-2"><Button disabled={loading} onClick={refresh} type="button" variant="secondary"><RefreshCw size={16} />{t("common.refresh")}</Button>{canExport ? <Button disabled={exporting || !diagnostics} onClick={exportBundle} type="button"><ArchiveRestore size={16} />{exporting ? t("diagnostics.exporting") : t("diagnostics.export")}</Button> : null}</div>}
      />
      <div className="grid gap-4 lg:grid-cols-2">
        {sections.map(([key, value]) => (
          <Card key={key}>
            <h2 className="mb-2 font-semibold">{t(`diagnostics.${key}`)}</h2>
            <dl>{Object.entries(value).map(([field, fieldValue]) => <KeyValue key={field} label={t(`diagnostics.fields.${field}`, { defaultValue: field })} value={summary(fieldValue)} />)}</dl>
            <details className="mt-3 rounded-[var(--radius-control)] border border-[var(--border)] bg-[var(--surface)] p-3 text-sm">
              <summary className="cursor-pointer font-medium">{t("diagnostics.technicalDetails")}</summary>
              <pre className="mt-3 max-h-72 overflow-auto whitespace-pre-wrap break-words text-xs leading-5 text-[var(--muted)]">{JSON.stringify(value, null, 2)}</pre>
            </details>
          </Card>
        ))}
      </div>
    </Fragment>
  );
}
