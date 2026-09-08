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

export function SwitchPage({ disabled, providers, currentProvider, recentSuccessfulProviders = [], profileKey, prepare, embedded = false }: {
  disabled: boolean;
  providers: string[];
  currentProvider?: string;
  recentSuccessfulProviders?: string[];
  profileKey?: string;
  prepare(values: SwitchValues, trigger: HTMLButtonElement | null): Promise<void>;
  embedded?: boolean;
}) {
  const { t } = useTranslation();
  const prepareButton = useRef<HTMLButtonElement>(null);
  const draftProfileKey = useRef(profileKey);
  const defaultProvider = currentProvider || providers[0] || "openai";
  const providerOptions = [...new Set(["openai", ...providers, ...(currentProvider ? [currentProvider] : [])])];
  const form = useForm<SwitchValues>({
    resolver: zodResolver(switchSchema),
    defaultValues: {
      provider: defaultProvider,
      modelMode: "provider-default",
      model: ""
    }
  });
  const provider = form.watch("provider");
  const modelMode = form.watch("modelMode");
  useEffect(() => {
    // Reset only the draft, not the DOM: plan dismissal must retain its focus target.
    if (draftProfileKey.current !== profileKey) {
      draftProfileKey.current = profileKey;
      form.reset({ provider: defaultProvider, modelMode: "provider-default", model: "" });
      return;
    }
    // Status arrives after the form mounts. Follow it until the user chooses a
    // target; when that target becomes current, adopt it as the new baseline.
    if (!currentProvider) return;
    if (!form.getFieldState("provider").isDirty || form.getValues("provider") === currentProvider) {
      form.resetField("provider", { defaultValue: currentProvider });
    }
  }, [currentProvider, defaultProvider, form, profileKey]);
  useEffect(() => {
    if (modelMode !== "explicit") form.setValue("model", "");
  }, [form, modelMode]);
  return (
    <Fragment>
      {!embedded ? <PageHeading title={t("switchPage.title")} subtitle={t("switchPage.subtitle")} /> : null}
      <Card className={embedded ? "min-w-0" : "max-w-2xl"}>
        {embedded ? <div className="mb-5"><h3 className="font-semibold">{t("switchPage.title")}</h3><p className="mt-1 text-sm text-[var(--muted)]">{t("switchPage.subtitle")}</p></div> : null}
          <form className="grid gap-5" onSubmit={form.handleSubmit((values) => prepare(values, prepareButton.current))}>
            {recentSuccessfulProviders.length ? <div className="grid gap-2"><span className="text-sm font-medium">{t("switchPage.recentSuccessful")}</span><div className="flex flex-wrap gap-2">{recentSuccessfulProviders.map((recentProvider) => <Button key={recentProvider} onClick={() => form.setValue("provider", recentProvider, { shouldDirty: true, shouldTouch: true })} size="compact" type="button" variant="secondary">{recentProvider}</Button>)}</div></div> : null}
            <Field error={form.formState.errors.provider ? t("validation.provider") : undefined} label={t("switchPage.provider")}>
            <Input list="configured-providers" {...form.register("provider")} />
          </Field>
          <datalist id="configured-providers">{providerOptions.map((provider) => <option key={provider} value={provider} />)}</datalist>
          <Field error={form.formState.errors.modelMode ? t("validation.model") : undefined} label={t("switchPage.modelMode")}>
            <select aria-describedby="switch-model-mode-description" className="min-h-10 rounded-lg border border-[var(--border)] bg-[var(--input)] px-3" {...form.register("modelMode")}>
              <option value="provider-default">{t("switchPage.providerDefault")}</option>
              <option value="keep-root-model">{t("switchPage.keepModel")}</option>
              <option value="explicit">{t("switchPage.explicitModel")}</option>
            </select>
          </Field>
          <div className="rounded-lg border border-[var(--border)] bg-[var(--surface)] p-3 text-sm text-[var(--muted)]" id="switch-model-mode-description">
            <p>{t(`switchPage.modelModeDescriptions.${modelMode}`, { provider: provider || t("common.provider") })}</p>
            <p className="mt-2">{t("switchPage.historyModelHint")}</p>
          </div>
          {modelMode === "explicit" ? <Field error={form.formState.errors.model ? t("validation.model") : undefined} label={t("switchPage.model")}><Input {...form.register("model")} /></Field> : null}
          <Button disabled={disabled || form.formState.isSubmitting} ref={prepareButton} type="submit"><RotateCcw size={17} />{t("switchPage.prepare")}</Button>
        </form>
      </Card>
    </Fragment>
  );
}
