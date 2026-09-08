import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { expect, it, vi } from "vitest";
import { createAppI18n } from "../src/i18n.js";
import { SyncPage } from "../src/features/sync/SyncPage.js";
import { hasManyRewrittenSessions } from "../src/features/sync/SyncPerformanceTip.js";
import { OperationResultDialog } from "../src/features/operations/OperationResultDialog.js";

it.each(["en", "zh-CN"])("keeps speed advice collapsed and read-only in %s", async (locale) => {
  const i18n = await createAppI18n(locale);
  const prepare = vi.fn(async () => {});
  const directSync = vi.fn(async () => {});
  render(<I18nextProvider i18n={i18n}><SyncPage disabled={false} prepare={prepare} directSync={directSync} embedded /></I18nextProvider>);
  const summary = screen.getByText(i18n.t("sync.performance.title"));
  const details = summary.closest("details")!;
  expect(details).not.toHaveAttribute("open");
  await userEvent.click(summary);
  expect(details).toHaveAttribute("open");
  expect(screen.getByText(i18n.t("sync.performance.equalLength"))).toBeVisible();
  expect(details).toHaveTextContent("openai");
  expect(details).toHaveTextContent("prov_a");
  expect(screen.getByText(i18n.t("sync.performance.configuration"))).toBeVisible();
  expect(prepare).not.toHaveBeenCalled();
  expect(directSync).not.toHaveBeenCalled();
});

it("shows result advice only for many actual rewrites, not estimates or in-place writes", async () => {
  expect(hasManyRewrittenSessions("sync", { rewrittenSessionFiles: 100 })).toBe(true);
  expect(hasManyRewrittenSessions("switch", { rewrittenSessionFiles: 2347 })).toBe(true);
  for (const counts of [null, {}, [], { rewrittenSessionFiles: 99 }, { rewrittenSessionFiles: "100" }, { rewrittenSessionFiles: Infinity }, { rewrittenSessionFiles: 100.5 }, { rolloutFilesToChange: 2347 }, { inPlaceSessionFiles: 2347, rewrittenSessionFiles: 0 }]) {
    expect(hasManyRewrittenSessions("sync", counts)).toBe(false);
  }
  expect(hasManyRewrittenSessions("repair", { rewrittenSessionFiles: 2347 })).toBe(false);
  expect(hasManyRewrittenSessions("restore", { rewrittenSessionFiles: 2347 })).toBe(false);
  const i18n = await createAppI18n("en");
  render(<I18nextProvider i18n={i18n}><OperationResultDialog close={() => {}} result={{ schemaVersion: 1, operationId: "op-sync-performance", operation: "sync", outcome: "partial", warnings: [], result: { rewrittenSessionFiles: 100 } }} /></I18nextProvider>);
  const summary = await screen.findByText("View speed-up tips");
  expect(summary.closest("details")).not.toHaveAttribute("open");
  await userEvent.click(summary);
  expect(summary.closest("details")).toHaveAttribute("open");
});
