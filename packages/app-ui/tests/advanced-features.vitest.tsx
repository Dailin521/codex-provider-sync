import { MockCoreClient } from "@codex-provider-sync/core-client";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it, vi } from "vitest";

import { AppUi } from "../src/App.js";
import { DiagnosticsPage } from "../src/features/diagnostics/DiagnosticsPage.js";
import { createAppI18n } from "../src/i18n.js";
import { statusFor } from "./helpers/app-fixtures.js";

describe("Everyday Provider sync and explicit advanced tools", () => {
  it("shows a pending scan, visible failure and retry, retaining the previous result without background scans", async () => {
    const user = userEvent.setup();
    let rejectScan!: (error: unknown) => void;
    const snapshot = {
      schemaVersion: 1 as const, generatedAt: "2026-09-04T00:00:00Z",
      runtime: { node: "v24", platform: "win32", arch: "x64" },
      storage: { sqliteHomeSource: "default", stateDbFound: true, sqliteSupported: true },
      provider: { current: "openai", implicit: false, configured: ["openai"], rolloutCounts: { sessions: { openai: 1 }, archived_sessions: {} }, sqliteCounts: null },
      safety: { storageRevision: "r1", pendingRecovery: false, pendingTransactions: [], operationInProgress: null, rolloutScanComplete: false, lockedRolloutCount: 0, projectThreadVisibilityAvailable: true },
      issues: { rootModelAvailable: true, rolloutModelFilesNeedingRepair: 0, sqliteModelRowsNeedingRepair: 0, cwdRowsNeedingRepair: 0, userEventRowsNeedingRepair: 0, workspaceRootsNeedingRepair: 0, encryptedContentFiles: 1849 }
    };
    const getDiagnostics = vi.fn()
      .mockImplementationOnce(() => new Promise((_, reject) => { rejectScan = reject; }))
      .mockResolvedValueOnce(snapshot)
      .mockRejectedValueOnce({ code: "INTERNAL_ERROR" });
    const core = new MockCoreClient({ getStatus: async () => statusFor(), getDiagnostics });
    render(<AppUi surface="desktop" core={core} host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }} initialLocale="en" initialTheme="system" preferences={{ getLocale: () => "en", setLocale: () => {}, getTheme: () => "system", setTheme: () => {} }} />);
    await screen.findByRole("button", { name: "Sync now" });
    await user.click(screen.getByRole("button", { name: "Advanced features", exact: true }));
    expect(getDiagnostics).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Start diagnostics" }));
    expect(await screen.findByText(/Scanning… Full diagnostics/)).toBeVisible();
    await act(async () => rejectScan({ code: "INTERNAL_ERROR" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Diagnostics could not finish");
    expect(screen.queryByText("Diagnostics have not been run")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Retry diagnostics" }));
    expect(await screen.findByText("1849", { exact: true })).toBeVisible();
    expect(screen.getByText(/Diagnostics completed ·/)).toBeVisible();
    expect(screen.getByText(/Some data changed or could not be read/)).toBeVisible();
    expect(screen.queryByRole("button", { name: "Review suggested repairs" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Start diagnostics" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Diagnostics could not finish");
    expect(screen.getByText(/Previous successful result ·/)).toBeVisible();
    expect(screen.getByText("1849", { exact: true })).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Overview", exact: true }));
    await user.click(screen.getByRole("button", { name: "Advanced features", exact: true }));
    expect(screen.getByText("1849", { exact: true })).toBeVisible();
    expect(getDiagnostics).toHaveBeenCalledTimes(3);
  });
  it.each(["en", "zh-CN"] as const)("explains differences and encrypted content without declaring damage in %s", async (locale) => {
    const i18n = await createAppI18n(locale);
    const refresh = vi.fn();
    const prepareRepair = vi.fn(async () => {});
    const diagnostics = { schemaVersion: 1 as const, generatedAt: "2026-09-04T00:00:00Z", runtime: {}, storage: {}, provider: {}, safety: {}, issues: {
      rootModelAvailable: true, rolloutModelFilesNeedingRepair: 9, sqliteModelRowsNeedingRepair: 9,
      cwdRowsNeedingRepair: 12, userEventRowsNeedingRepair: 10, workspaceRootsNeedingRepair: 2, encryptedContentFiles: 1849
    }, historyIntegrity: {
      version: 1, outcome: "findings-and-inconclusive", issuesTruncated: false,
      counts: { filesDiscovered: 2, filesScanned: 2, recordsRead: 4, sessionsWithId: 2, jsonCorruptRecords: 0, oversizedRecords: 0, duplicateOrdinals: 1, outOfOrderOrdinals: 0, changedFiles: 1, truncatedFiles: 0, unsupportedFiles: 0 },
      skipped: { symlinkOrReparse: 0, outOfRoot: 0, notRegular: 0, unreadable: 0, scanLimit: 0 },
      displayIndex: { status: "unsupported", reason: "no-known-display-index-schema" },
      issues: [{ code: "ordinal-duplicate-observed", sessionId: "session-1", scope: "sessions", line: 4 }],
      limits: { maxFiles: 100, maxRecordsPerFile: 1000, maxLineBytes: 1024, maxIssues: 100 }
    } } as import("@codex-provider-sync/contracts").DiagnosticsSnapshot;
    render(<I18nextProvider i18n={i18n}><DiagnosticsPage diagnostics={diagnostics} canExport={false} canRepair loading={false} exporting={false} repairDisabled={false} refresh={refresh} exportBundle={() => {}} prepareRepair={prepareRepair} /></I18nextProvider>);
    for (const key of ["issuesHint", "modelDifferenceHint", "workspaceCountHint", "encryptedHint"]) expect(screen.getByText(i18n.t(`diagnostics.${key}`))).toBeVisible();
    expect(screen.getByText("1849", { exact: true })).toBeVisible();
    expect(screen.getByText(i18n.t("diagnostics.fields.workspaceRootsNeedingRepair"))).toBeVisible();
    expect(screen.getByText(i18n.t("diagnostics.historyIntegrity.title"))).toBeVisible();
    expect(screen.getByText(i18n.t("diagnostics.historyIntegrity.displayIndexUnsupported"))).toBeVisible();
    expect(screen.getByText(i18n.t("diagnostics.historyIntegrity.manualReview"))).toBeVisible();
    expect(prepareRepair).not.toHaveBeenCalled();
    expect(refresh).not.toHaveBeenCalled();
  });

  it("never escalates a failed Sync or page navigation into diagnostics or repair", async () => {
    const user = userEvent.setup();
    const getDiagnostics = vi.fn(async () => { throw { code: "SQLITE_UNREADABLE" }; });
    const prepareRepair = vi.fn(async () => { throw { code: "INVALID_INPUT" }; });
    const applyRepair = vi.fn(async () => { throw { code: "INVALID_INPUT" }; });
    const prepareSync = vi.fn(async () => { throw { code: "INTERNAL_ERROR" }; });
    const core = new MockCoreClient({ getStatus: async () => statusFor(), getDiagnostics, prepareRepair, applyRepair, prepareSync });
    render(<AppUi surface="desktop" core={core} host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }} initialLocale="en" initialTheme="system" preferences={{ getLocale: () => "en", setLocale: () => {}, getTheme: () => "system", setTheme: () => {} }} />);
    const sync = await screen.findByRole("button", { name: "Preview sync" });
    await waitFor(() => expect(sync).toBeEnabled());
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
    await user.click(sync);
    await waitFor(() => expect(prepareSync).toHaveBeenCalledOnce());
    expect(getDiagnostics).not.toHaveBeenCalled();
    expect(prepareRepair).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "Advanced features", exact: true }));
    expect(await screen.findByRole("heading", { name: "Advanced features" })).toBeVisible();
    expect(getDiagnostics).not.toHaveBeenCalled();
    expect(screen.getByText("Targeted repair", { exact: true }).closest("details")).not.toHaveAttribute("open");
    await user.click(screen.getByText("Targeted repair", { exact: true }));
    for (const checkbox of screen.getAllByRole("checkbox")) expect(checkbox).not.toBeChecked();
    expect(prepareRepair).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Start diagnostics" }));
    await waitFor(() => expect(getDiagnostics).toHaveBeenCalledOnce());
    expect(prepareRepair).not.toHaveBeenCalled();
    expect(applyRepair).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Overview", exact: true }));
    await user.click(screen.getByRole("button", { name: "Advanced features", exact: true }));
    expect(getDiagnostics).toHaveBeenCalledOnce();
  });

  it.each(["en", "zh-CN"] as const)("keeps repairs collapsed, unselected and preview-only in %s", async (locale) => {
    const user = userEvent.setup();
    const i18n = await createAppI18n(locale);
    const refresh = vi.fn();
    const prepareRepair = vi.fn(async () => {});
    render(<I18nextProvider i18n={i18n}><DiagnosticsPage canExport={false} canRepair loading={false} exporting={false} repairDisabled={false} refresh={refresh} exportBundle={() => {}} prepareRepair={prepareRepair} /></I18nextProvider>);
    expect(screen.getByRole("heading", { name: i18n.t("diagnostics.title") })).toBeVisible();
    const disclosure = screen.getByText(i18n.t("diagnostics.repairTitle"), { exact: true });
    expect(disclosure.closest("details")).not.toHaveAttribute("open");
    await user.click(disclosure);
    expect(screen.getByText(i18n.t("diagnostics.repairScope"))).toBeVisible();
    await user.click(screen.getByRole("button", { name: i18n.t("diagnostics.prepareRepair") }));
    expect(await screen.findByRole("alert")).toHaveTextContent(i18n.t("diagnostics.repairTargetRequired"));
    expect(prepareRepair).not.toHaveBeenCalled();
    await user.click(screen.getByRole("checkbox", { name: i18n.t("diagnostics.repairTargets.cwd") }));
    await user.click(screen.getByRole("button", { name: i18n.t("diagnostics.prepareRepair") }));
    await waitFor(() => expect(prepareRepair).toHaveBeenCalledOnce());
    expect(prepareRepair.mock.calls[0]?.[0]).toEqual({ models: false, cwd: true, userEvent: false, workspaceRoots: false });
    expect(refresh).not.toHaveBeenCalled();
  });
});
