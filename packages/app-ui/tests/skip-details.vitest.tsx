import { render, screen, cleanup } from "@testing-library/react";
import { I18nextProvider } from "react-i18next";
import { afterEach, expect, it } from "vitest";
import { createAppI18n } from "../src/i18n.js";
import { SkipDetails } from "../src/features/operations/SkipDetails.js";
afterEach(cleanup);
it.each(["en", "zh-CN"] as const)("shows bounded actionable local skip details in %s", async locale => {
  const i18n = await createAppI18n(locale);
  const summary = { total: 201, rolloutFiles: 201, sqliteRows: 0, unconfirmed: 0, omitted: 1, retryRecommended: false,
    items: Array.from({ length: 200 }, (_, n) => ({ kind: "rollout", path: `D:/fixture/file-${n}.jsonl`, reason: n === 0 ? "metadata-invalid-utf8" : n === 1 ? "metadata-too-complex" : "metadata-invalid", stage: "scan", retryable: false })) };
  render(<I18nextProvider i18n={i18n}><SkipDetails value={summary} /></I18nextProvider>);
  expect(screen.getByText("D:/fixture/file-199.jsonl")).toBeInTheDocument();
  expect(screen.getByText(i18n.t("skips.reasons.metadata-invalid-utf8"), { exact: false })).toBeInTheDocument();
  expect(screen.getByText(i18n.t("skips.reasons.metadata-too-complex"), { exact: false })).toBeInTheDocument();
  expect(screen.getByText(i18n.t("skips.fix"))).toBeInTheDocument();
  expect(screen.queryByText(i18n.t("skips.retry"))).toBeNull();
  expect(screen.getByText(i18n.t("skips.shown", { shown: 200, total: 201, omitted: 1 }))).toBeInTheDocument();
});
