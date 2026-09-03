import { zodResolver } from "@hookform/resolvers/zod";
import { RotateCcw } from "lucide-react";
import { Fragment, useEffect, useRef } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { z } from "zod";

import { switchSchema } from "../../schemas.js";
import { PageHeading } from "../../shared/presentation.js";
import { Button, Card, Field, Input } from "../../ui.js";

export type SwitchValues = z.infer<typeof switchSchema>;

export function SwitchPage({ disabled, providers, prepare }: {
  disabled: boolean;
  providers: string[];
  prepare(values: SwitchValues, trigger: HTMLButtonElement | null): Promise<void>;
}) {
  const { t } = useTranslation();
  const prepareButton = useRef<HTMLButtonElement>(null);
  const form = useForm<SwitchValues>({
    resolver: zodResolver(switchSchema),
    defaultValues: {
      provider: providers[0] ?? "openai",
      modelMode: "provider-default",
      model: "",
      keepCount: 5,
      syncMode: "full"
    }
  });
  const modelMode = form.watch("modelMode");
  const syncMode = form.watch("syncMode");
  useEffect(() => {
    if (modelMode !== "explicit") form.setValue("model", "");
  }, [form, modelMode]);
  useEffect(() => {
    if (syncMode === "fast") {
      form.setValue("modelMode", "keep-root-model", { shouldValidate: true });
      form.setValue("model", "", { shouldValidate: true });
    }
  }, [form, syncMode]);
  return (
    <Fragment>
      <PageHeading title={t("switchPage.title")} subtitle={t("switchPage.subtitle")} />
      <Card className="max-w-2xl">
        <form className="grid gap-5" onSubmit={form.handleSubmit((values) => prepare(values, prepareButton.current))}>
          <Field error={form.formState.errors.provider ? t("validation.provider") : undefined} label={t("switchPage.provider")}>
            <Input list="configured-providers" {...form.register("provider")} />
          </Field>
          <datalist id="configured-providers">{providers.map((provider) => <option key={provider} value={provider} />)}</datalist>
          <Field label={t("sync.mode")}>
            <select className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" {...form.register("syncMode")}>
              <option value="full">{t("sync.fullMode")}</option>
              <option value="fast">{t("sync.fastMode")}</option>
            </select>
          </Field>
          {syncMode === "fast" ? <p className="rounded-lg border border-[var(--accent)] bg-[var(--accent-soft)] p-3 text-sm text-[var(--muted)]">{t("sync.fastHint")}</p> : null}
          <Field error={form.formState.errors.modelMode ? t("validation.model") : undefined} label={t("switchPage.modelMode")}>
            <select className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3 disabled:cursor-not-allowed disabled:opacity-60" disabled={syncMode === "fast"} {...form.register("modelMode")}>
              <option value="provider-default">{t("switchPage.providerDefault")}</option>
              <option value="keep-root-model">{t("switchPage.keepModel")}</option>
              <option value="explicit">{t("switchPage.explicitModel")}</option>
            </select>
          </Field>
          {modelMode === "explicit" ? <Field error={form.formState.errors.model ? t("validation.model") : undefined} label={t("switchPage.model")}><Input {...form.register("model")} /></Field> : null}
          <Field error={form.formState.errors.keepCount ? t("validation.keep") : undefined} label={t("sync.keep")}><Input max={1000} min={1} type="number" {...form.register("keepCount", { valueAsNumber: true })} /></Field>
          <Button disabled={disabled || form.formState.isSubmitting} ref={prepareButton} type="submit"><RotateCcw size={17} />{t("switchPage.prepare")}</Button>
        </form>
      </Card>
    </Fragment>
  );
}
