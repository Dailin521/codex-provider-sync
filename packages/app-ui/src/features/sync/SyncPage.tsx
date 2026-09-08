import { zodResolver } from "@hookform/resolvers/zod";
import { Workflow } from "lucide-react";
import { Fragment, useRef } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { z } from "zod";

import { syncSchema } from "../../schemas.js";
import { PageHeading } from "../../shared/presentation.js";
import { Button, Card } from "../../ui.js";
import { SyncPerformanceTip } from "./SyncPerformanceTip.js";

export type SyncValues = z.infer<typeof syncSchema>;

export function SyncPage({ disabled, prepare, directSync, embedded = false }: {
  disabled: boolean;
  prepare(values: SyncValues, trigger: HTMLButtonElement | null): Promise<void>;
  directSync?(values: SyncValues, trigger: HTMLButtonElement | null): Promise<void>;
  embedded?: boolean;
}) {
  const { t } = useTranslation();
  const form = useForm<SyncValues>({
    resolver: zodResolver(syncSchema),
    defaultValues: {}
  });
  const prepareButton = useRef<HTMLButtonElement>(null);
  const directButton = useRef<HTMLButtonElement>(null);
  return (
    <Fragment>
      {!embedded ? <PageHeading title={t("sync.title")} subtitle={t("sync.subtitle")} /> : null}
      <Card className={embedded ? "min-w-0 p-4" : "max-w-2xl"}>
        {embedded ? <div className="mb-3"><h3 className="font-semibold">{t("sync.title")}</h3><p className="mt-1 text-sm text-[var(--muted)]">{t("sync.subtitle")}</p></div> : null}
        <form className="grid gap-5" onSubmit={form.handleSubmit((values) => prepare(values, prepareButton.current))}>
          <div className="flex flex-wrap gap-3">
            <Button disabled={disabled || form.formState.isSubmitting} ref={prepareButton} type="submit" variant="secondary"><Workflow size={17} />{t("sync.prepare")}</Button>
            {directSync ? <Button disabled={disabled || form.formState.isSubmitting} onClick={() => void form.handleSubmit((values) => directSync(values, directButton.current))()} ref={directButton} type="button"><Workflow size={17} />{t("sync.direct")}</Button> : null}
          </div>
          {directSync ? <p className="text-sm text-[var(--muted)]">{t("sync.directHint")}</p> : null}
        </form>
        <div className="mt-3"><SyncPerformanceTip /></div>
      </Card>
    </Fragment>
  );
}
