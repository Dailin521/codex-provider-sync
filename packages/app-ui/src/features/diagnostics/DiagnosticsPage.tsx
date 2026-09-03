import type { DiagnosticsSnapshot } from "@codex-provider-sync/contracts";
import { zodResolver } from "@hookform/resolvers/zod";
import { ArchiveRestore, RefreshCw, Wrench } from "lucide-react";
import { Fragment, useRef } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { z } from "zod";

import { repairSchema } from "../../schemas.js";
import { KeyValue, PageHeading } from "../../shared/presentation.js";
import { Badge, Button, Card, Field, Input } from "../../ui.js";

export type RepairValues = z.infer<typeof repairSchema>;

export function DiagnosticsPage({ diagnostics, loading, exporting, canExport, canRepair, repairDisabled, refresh, exportBundle, prepareRepair }: {
  diagnostics?: DiagnosticsSnapshot;
  loading: boolean;
  exporting: boolean;
  canExport: boolean;
  canRepair: boolean;
  repairDisabled: boolean;
  refresh(): void;
  exportBundle(): void;
  prepareRepair(values: RepairValues, trigger: HTMLButtonElement | null): Promise<void>;
}) {
  const { t } = useTranslation();
  const repairButton = useRef<HTMLButtonElement>(null);
  const repairForm = useForm<RepairValues>({
    resolver: zodResolver(repairSchema),
    defaultValues: {
      models: false,
      cwd: false,
      userEvent: false,
      workspaceRoots: false,
      keepCount: 5
    }
  });
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
        action={<div className="flex flex-wrap gap-2"><Button disabled={loading} onClick={refresh} type="button" variant="secondary"><RefreshCw size={16} />{loading ? t("common.loading") : t("diagnostics.runScan")}</Button>{canExport ? <Button disabled={exporting || !diagnostics} onClick={exportBundle} type="button"><ArchiveRestore size={16} />{exporting ? t("diagnostics.exporting") : t("diagnostics.export")}</Button> : null}</div>}
      />
      {!diagnostics && !loading ? <Card className="mb-4"><h2 className="font-semibold">{t("diagnostics.notScanned")}</h2><p className="mt-2 text-sm text-[var(--muted)]">{t("diagnostics.notScannedHint")}</p></Card> : null}
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
      {canRepair ? <Card className="mt-4 max-w-3xl">
        <h2 className="font-semibold">{t("diagnostics.repairTitle")}</h2>
        <p className="mt-1 text-sm text-[var(--muted)]">{t("diagnostics.repairHint")}</p>
        <form className="mt-4 grid gap-4" onSubmit={repairForm.handleSubmit((values) => prepareRepair(values, repairButton.current))}>
          <div className="grid gap-3 sm:grid-cols-2">
            {(["models", "cwd", "userEvent", "workspaceRoots"] as const).map((target) => <label className="flex min-h-11 items-center gap-3 rounded-lg border border-[var(--border)] bg-[var(--surface)] px-3" key={target}><input type="checkbox" {...repairForm.register(target)} /><span>{t(`diagnostics.repairTargets.${target}`)}</span></label>)}
          </div>
          {repairForm.formState.errors.models ? <p className="text-sm text-[var(--danger)]" role="alert">{t("diagnostics.repairTargetRequired")}</p> : null}
          <p className="text-sm text-[var(--muted)]">{t("diagnostics.workspaceRootsIncludesCwd")}</p>
          <Field error={repairForm.formState.errors.keepCount ? t("validation.keep") : undefined} label={t("sync.keep")}><Input max={1000} min={1} type="number" {...repairForm.register("keepCount", { valueAsNumber: true })} /></Field>
          <Button disabled={repairDisabled || repairForm.formState.isSubmitting} ref={repairButton} type="submit"><Wrench size={16} />{t("diagnostics.prepareRepair")}</Button>
        </form>
      </Card> : null}
    </Fragment>
  );
}
