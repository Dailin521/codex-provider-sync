import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it, vi } from "vitest";
import { OperationLogsPage } from "../src/features/operation-logs/OperationLogsPage.js";
import { OperationResultDialog } from "../src/features/operations/OperationResultDialog.js";
import { createAppI18n } from "../src/i18n.js";
import type { OperationLogEntry } from "../src/types.js";

const entry: OperationLogEntry = {
  schemaVersion: 1, id: "log-1", operation: "sync", profileId: "fixture", profileRevision: "r1",
  startedAt: "2026-09-05T00:00:00Z", completedAt: "2026-09-05T00:00:01Z",
  activeDurationMs: 1000, wallDurationMs: 1000, status: "partial", outcome: "partial",
  requestIds: ["request-1"], planId: "plan-1", operationId: "op-1", backupId: "backup-1",
  counts: { changedSessionFiles: 2 }, warnings: [], stages: [],
  failedStage: "update_sqlite", failureCode: "SQLITE_BUSY", partialReason: "mutation-failed", retryRecommended: true
};

describe("Partial operation feedback", () => {
  it.each([
    { profileRevision: "r1", logRevision: "r1" },
    { profileRevision: "changed", logRevision: "r1" },
    { profileRevision: undefined, logRevision: "r1" },
    { profileRevision: "r1", logRevision: undefined }
  ])("shows failure details and offers actions only for the exact original profile revision (%j)", async ({ profileRevision, logRevision }) => {
    const i18n = await createAppI18n("en");
    const user = userEvent.setup();
    const openBackupRestore = vi.fn();
    const reviewOperation = vi.fn();
    const logEntry = { ...entry, profileRevision: logRevision };
    const host = {
      listProfiles: async () => [{ id: "fixture", name: "Fixture", revision: profileRevision ?? "r1" }],
      listOperationLogs: async () => ({ schemaVersion: 1 as const, page: 1, pageSize: 50, total: 1, hasNextPage: false, entries: [logEntry] }),
      getOperationLog: async () => logEntry
    };
    render(<I18nextProvider i18n={i18n}><QueryClientProvider client={new QueryClient()}><OperationLogsPage host={host} profileId="fixture" profileRevision={profileRevision} openBackupRestore={openBackupRestore} reviewOperation={reviewOperation} /></QueryClientProvider></I18nextProvider>);
    await user.click((await screen.findByText("1.0 seconds")).closest("button")!);
    expect(await screen.findByText(/\(SQLITE_BUSY\)/)).toBeVisible();
    expect(screen.getByText(i18n.t("logs.stages.update_sqlite"))).toBeVisible();
    expect(screen.getByText(i18n.t("operationResult.partialReasons.mutation-failed"))).toBeVisible();
    if (profileRevision === "r1" && logRevision === "r1") {
      await user.click(screen.getByRole("button", { name: "Open restore preview" }));
      expect(openBackupRestore).toHaveBeenCalledWith("backup-1");
      await user.click(screen.getByRole("button", { name: "Back to operation tools" }));
      expect(reviewOperation).toHaveBeenCalledWith("sync");
    } else {
      expect(screen.queryByRole("button", { name: "Open restore preview" })).not.toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Back to operation tools" })).not.toBeInTheDocument();
      expect(screen.getByText(i18n.t("logs.profileMismatch"))).toBeVisible();
      expect(openBackupRestore).not.toHaveBeenCalled();
    }
  });

  it("shows the failed stage and code in the immediate result, keeping retry explicit", async () => {
    const i18n = await createAppI18n("zh-CN");
    const user = userEvent.setup();
    const review = vi.fn();
    render(<I18nextProvider i18n={i18n}><OperationResultDialog close={() => {}} restoreFocus={() => {}} reviewOperation={review} result={{
      schemaVersion: 1, operationId: "operation-1", operation: "repair", outcome: "partial", backup: { backupId: "backup-1" }, warnings: [],
      result: { failedStage: "verify_repair", failureCode: "SQLITE_BUSY", partialReason: "mutation-failed", retryRecommended: true }
    }} /></I18nextProvider>);
    expect(screen.getByText(i18n.t("logs.stages.verify_repair"))).toBeVisible();
    expect(screen.getByText(/\(SQLITE_BUSY\)/)).toBeVisible();
    expect(review).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: i18n.t("operationResult.reviewOperation") }));
    expect(review).toHaveBeenCalledOnce();
  });
});
