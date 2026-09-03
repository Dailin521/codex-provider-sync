import { zodResolver } from "@hookform/resolvers/zod";
import { Workflow } from "lucide-react";
import { Fragment, useRef } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { z } from "zod";

import { syncSchema } from "../../schemas.js";
import { PageHeading } from "../../shared/presentation.js";
import { Button, Card, Field, Input } from "../../ui.js";

export type SyncValues = z.infer<typeof syncSchema>;

export function SyncPage({ disabled, prepare }: {
  disabled: boolean;
  prepare(values: SyncValues, trigger: HTMLButtonElement | null): Promise<void>;
}) {
  const { t } = useTranslation();
  const form = useForm<SyncValues>({
    resolver: zodResolver(syncSchema),
    defaultValues: { keepCount: 5, syncMode: "full" }
  });
  const prepareButton = useRef<HTMLButtonElement>(null);
  return (
    <Fragment>
      <PageHeading title={t("sync.title")} subtitle={t("sync.subtitle")} />
      <Card className="max-w-2xl">
        <form className="grid gap-5" onSubmit={form.handleSubmit((values) => prepare(values, prepareButton.current))}>
          <Field label={t("sync.mode")}>
            <select className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" {...form.register("syncMode")}>
              <option value="full">{t("sync.fullMode")}</option>
              <option value="fast">{t("sync.fastMode")}</option>
            </select>
          </Field>
          {form.watch("syncMode") === "fast" ? <p className="rounded-lg border border-[var(--accent)] bg-[var(--accent-soft)] p-3 text-sm text-[var(--muted)]">{t("sync.fastHint")}</p> : null}
          <Field error={form.formState.errors.keepCount ? t("validation.keep") : undefined} label={t("sync.keep")}>
            <Input max={1000} min={1} type="number" {...form.register("keepCount", { valueAsNumber: true })} />
          </Field>
          <Button disabled={disabled || form.formState.isSubmitting} ref={prepareButton} type="submit"><Workflow size={17} />{t("sync.prepare")}</Button>
        </form>
      </Card>
    </Fragment>
  );
}
