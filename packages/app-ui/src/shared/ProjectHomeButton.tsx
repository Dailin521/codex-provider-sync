import { useState } from "react";
import { useTranslation } from "react-i18next";

import { Button, useToast } from "../ui.js";

export function ProjectHomeButton({ openHome }: { openHome(): Promise<void> }) {
  const { t } = useTranslation();
  const toast = useToast();
  const [opening, setOpening] = useState(false);
  const open = async () => {
    setOpening(true);
    try {
      await openHome();
    } catch {
      toast.push({ title: t("global.projectHomeFailed"), tone: "danger" });
    } finally {
      setOpening(false);
    }
  };
  return (
    <Button aria-label={t("global.projectHome")} className="shrink-0" disabled={opening} onClick={() => void open()} size="icon" title={t("global.projectHome")} type="button" variant="ghost">
      <span aria-hidden="true" className="cps-github-mark" />
    </Button>
  );
}
