import type { ManagedBackup, OperationResult, StatusSnapshot } from "@codex-provider-sync/contracts";
import { MockCoreClient } from "@codex-provider-sync/core-client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it, vi } from "vitest";

import { AppUi } from "../src/App.js";
import { BackupsRestorePage } from "../src/features/backups-restore/BackupsRestorePage.js";
import { HistoryPage } from "../src/features/history/HistoryPage.js";
import { OperationResultDialog } from "../src/features/operations/OperationResultDialog.js";
import { OverviewPage } from "../src/features/overview/OverviewPage.js";
import { ProfilesPage } from "../src/features/profiles/ProfilesPage.js";
import { createAppI18n } from "../src/i18n.js";
import { statusAlignment } from "../src/shared/status-feedback.js";
import { ToastProvider } from "../src/ui.js";
import { statusFor, syncPlanFor } from "./helpers/app-fixtures.js";
import { historyProjectPage } from "./helpers/history-fixtures.js";

const profile = { id: "default", name: "Default", revision: "profile-r1" };
const otherProfile = { id: "other", name: "Other", revision: "profile-r2" };
const preferences = { getLocale: () => "en" as const, setLocale: () => {}, getTheme: () => "system" as const, setTheme: () => {} };
const backups: ManagedBackup[] = [
  { backupId: "backup-1", createdAt: "2026-09-01T00:00:00.000Z", sizeBytes: 100, metadata: {} },
  { backupId: "backup-2", createdAt: "2026-09-02T00:00:00.000Z", sizeBytes: 200, metadata: {} },
  { backupId: "backup-3", createdAt: "2026-09-03T00:00:00.000Z", sizeBytes: 300, metadata: {} },
  { backupId: "backup-4", createdAt: "2026-09-04T00:00:00.000Z", sizeBytes: 400, metadata: {} }
];

function alignedStatus(provider = "openai"): StatusSnapshot {
  return { ...statusFor(), currentProvider: provider, alignment: { sqliteReadable: true, aligned: true } };
}

function operationResult(operationId = "operation-1"): OperationResult {
  return { schemaVersion: 1, operationId, operation: "sync", outcome: "completed", backup: null, warnings: [], result: {} };
}

describe("UX polish regressions", () => {
  it.each([
    ["a valid positive Core verdict", alignedStatus(), "aligned"],
    ["a valid negative Core verdict", { ...alignedStatus(), alignment: { sqliteReadable: true, aligned: false } }, "notAligned"],
    ["a locked rollout file", { ...alignedStatus(), lockedRolloutFiles: ["fixture.jsonl"] }, "unknown"],
    ["an incomplete rollout scan", { ...alignedStatus(), rolloutScanComplete: false }, "unknown"],
    ["an unreadable SQLite verdict", { ...alignedStatus(), alignment: { sqliteReadable: false, aligned: false } }, "unknown"],
    ["an active operation", { ...alignedStatus(), operationInProgress: { operation: "sync" } }, "unknown"],
    ["a blocked status read", { ...alignedStatus(), statusReadBlocked: true }, "unknown"],
    ["an invalid snapshot", { ...alignedStatus(), snapshotAt: "not-a-date" }, "unknown"]
  ] as const)("returns %s as %s", (_case, snapshot, expected) => {
    expect(statusAlignment(snapshot)).toBe(expected);
  });

  it("shows absent status as unverified instead of not aligned or zero counts", async () => {
    const i18n = await createAppI18n("en");
    render(<I18nextProvider i18n={i18n}><OverviewPage status={undefined} loading={false} refresh={() => {}} profileName="Fixture" providers={[]} sqliteHomeConfigured={false} writeDisabled prepareSync={async () => {}} prepareSwitch={async () => {}} manageStorage={() => {}} /></I18nextProvider>);

    expect(screen.getByText("Not verified")).toBeVisible();
    expect(screen.queryByText("Not aligned")).not.toBeInTheDocument();
    expect(screen.queryByText("false")).not.toBeInTheDocument();
    expect(screen.getByText("Available backups").parentElement).toHaveTextContent("—");
    expect(screen.getByText("Sessions currently in use").parentElement).toHaveTextContent("Unknown");
  });

  it("labels a loading status read as reading without inventing an alignment", async () => {
    const i18n = await createAppI18n("en");
    render(<I18nextProvider i18n={i18n}><OverviewPage status={undefined} loading refresh={() => {}} profileName="Fixture" providers={[]} sqliteHomeConfigured={false} writeDisabled prepareSync={async () => {}} prepareSwitch={async () => {}} manageStorage={() => {}} /></I18nextProvider>);

    expect(screen.getByText("Reading status…")).toBeVisible();
    expect(screen.queryByText("Not aligned")).not.toBeInTheDocument();
    expect(screen.queryByText("false")).not.toBeInTheDocument();
    expect(screen.getByText("Available backups").parentElement).toHaveTextContent("—");
  });

  it("shows the cleanup upper-bound estimate and waits for confirmation", async () => {
    const i18n = await createAppI18n("en");
    const prune = vi.fn();
    const user = userEvent.setup();
    render(<I18nextProvider i18n={i18n}><BackupsRestorePage backups={backups} canPrune canRestore={false} disabled={false} loading={false} prepare={async () => {}} profile={profile} profiles={[profile]} prune={prune} /></I18nextProvider>);

    expect(screen.getByText("From the current list: up to 2 backups may be removed; at least 2 will remain.")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Delete older backups" }));
    expect(prune).not.toHaveBeenCalled();
    expect(screen.getByRole("dialog", { name: "Confirm backup cleanup" })).toHaveTextContent("up to 2 backups may be removed; at least 2 will remain");
    await user.click(screen.getByRole("button", { name: "Confirm cleanup" }));
    expect(prune).toHaveBeenCalledWith(2);
  });

  it("warns explicitly before confirming cleanup with keep zero", async () => {
    const i18n = await createAppI18n("en");
    const prune = vi.fn();
    const user = userEvent.setup();
    render(<I18nextProvider i18n={i18n}><BackupsRestorePage backups={backups} canPrune canRestore={false} disabled={false} loading={false} prepare={async () => {}} profile={profile} profiles={[profile]} prune={prune} /></I18nextProvider>);

    await user.click(screen.getByText("Advanced options", { exact: true }));
    await user.click(screen.getByRole("button", { name: "Delete all eligible backups" }));
    expect(screen.getByText(/This cleanup requests removal of every eligible managed backup/)).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Confirm cleanup" }));
    expect(prune).toHaveBeenCalledWith(0);
  });

  it("blocks cleanup confirmation when the backup revision changes", async () => {
    const i18n = await createAppI18n("en");
    const prune = vi.fn();
    const user = userEvent.setup();
    const view = (currentBackups: ManagedBackup[]) => <I18nextProvider i18n={i18n}><BackupsRestorePage backups={currentBackups} canPrune canRestore={false} disabled={false} loading={false} prepare={async () => {}} profile={profile} profiles={[profile]} prune={prune} /></I18nextProvider>;
    const { rerender } = render(view(backups));

    await user.click(screen.getByRole("button", { name: "Delete older backups" }));
    rerender(view([...backups, { backupId: "backup-5", createdAt: "2026-09-05T00:00:00.000Z", sizeBytes: 500, metadata: {} }]));
    expect(screen.getByRole("alert")).toHaveTextContent("backup list or storage profile changed");
    expect(screen.getByRole("button", { name: "Confirm cleanup" })).toBeDisabled();
    expect(prune).not.toHaveBeenCalled();
  });

  it("restores the cleanup trigger on cancel without pruning", async () => {
    const i18n = await createAppI18n("en");
    const prune = vi.fn();
    const user = userEvent.setup();
    render(<I18nextProvider i18n={i18n}><BackupsRestorePage backups={backups} canPrune canRestore={false} disabled={false} loading={false} prepare={async () => {}} profile={profile} profiles={[profile]} prune={prune} /></I18nextProvider>);

    const trigger = screen.getByRole("button", { name: "Delete older backups" });
    await user.click(trigger);
    await user.click(screen.getByRole("button", { name: "Close" }));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Confirm backup cleanup" })).not.toBeInTheDocument());
    await waitFor(() => expect(trigger).toHaveFocus());
    expect(prune).not.toHaveBeenCalled();
  });

  it("blocks cleanup confirmation when the profile revision changes", async () => {
    const i18n = await createAppI18n("en");
    const prune = vi.fn();
    const user = userEvent.setup();
    const view = (currentProfile: typeof profile) => <I18nextProvider i18n={i18n}><BackupsRestorePage backups={backups} canPrune canRestore={false} disabled={false} loading={false} prepare={async () => {}} profile={currentProfile} profiles={[currentProfile]} prune={prune} /></I18nextProvider>;
    const { rerender } = render(view(profile));

    await user.click(screen.getByRole("button", { name: "Delete older backups" }));
    rerender(view({ ...profile, revision: "profile-r2" }));
    expect(screen.getByRole("alert")).toHaveTextContent("backup list or storage profile changed");
    expect(screen.getByRole("button", { name: "Confirm cleanup" })).toBeDisabled();
    expect(prune).not.toHaveBeenCalled();
  });

  it("keeps the current-profile badge on the active profile while another profile is edited", async () => {
    const i18n = await createAppI18n("en");
    const user = userEvent.setup();
    render(<I18nextProvider i18n={i18n}><QueryClientProvider client={new QueryClient()}><ToastProvider><ProfilesPage canManage host={{ listProfiles: async () => [] }} profiles={[profile, otherProfile]} refresh={async () => {}} revealPaths={false} selectProfile={() => {}} selectedProfileId="default" surface="desktop" /></ToastProvider></QueryClientProvider></I18nextProvider>);

    await user.click(screen.getByRole("button", { name: /^Other/ }));
    expect(screen.getByText("In use").parentElement?.parentElement).toHaveTextContent("Default");
    expect(screen.getByRole("button", { name: /^Other/ })).toHaveClass("border-[var(--accent)]");
  });

  it("does not attach a final status from a different operation", async () => {
    const i18n = await createAppI18n("en");
    render(<I18nextProvider i18n={i18n}><OperationResultDialog close={() => {}} postWriteStatus={{ operationId: "different-operation", state: "received", snapshot: alignedStatus("relay") }} restoreFocus={() => {}} result={operationResult()} /></I18nextProvider>);

    expect(await screen.findByText("Completed")).toBeVisible();
    expect(screen.queryByText("Final Provider check")).not.toBeInTheDocument();
    expect(screen.queryByText("relay")).not.toBeInTheDocument();
  });

  it("keeps history drafts manual, then clears applied filters deliberately", async () => {
    const i18n = await createAppI18n("en");
    const user = userEvent.setup();
    const listHistory = vi.fn(async () => historyProjectPage([]));
    render(<I18nextProvider i18n={i18n}><QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><HistoryPage core={{ listHistory, getHistorySession: vi.fn() } as never} profile={profile} /></QueryClientProvider></I18nextProvider>);

    await waitFor(() => expect(listHistory).toHaveBeenCalledTimes(1));
    const search = screen.getByRole("textbox", { name: "Search" });
    await user.type(search, "alpha");
    expect(listHistory).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("status")).toHaveTextContent("Press Search to apply");
    await user.keyboard("{Enter}");
    await waitFor(() => expect(listHistory).toHaveBeenCalledTimes(2));
    expect(listHistory).toHaveBeenLastCalledWith(expect.objectContaining({ query: "alpha" }), expect.anything());
    await user.type(search, "-draft");
    expect(listHistory).toHaveBeenCalledTimes(2);
    await user.click(screen.getByRole("button", { name: "Clear filters" }));
    await waitFor(() => expect(listHistory).toHaveBeenCalledTimes(3));
    expect(search).toHaveValue("");
    expect(search).toHaveFocus();
    expect(listHistory).toHaveBeenLastCalledWith(expect.not.objectContaining({ query: expect.anything() }), expect.anything());
  });

  it("clears draft-only history filters without reading again and restores metadata defaults", async () => {
    const i18n = await createAppI18n("en");
    const user = userEvent.setup();
    const listHistory = vi.fn(async () => historyProjectPage([]));
    render(<I18nextProvider i18n={i18n}><QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><HistoryPage core={{ listHistory, getHistorySession: vi.fn() } as never} profile={profile} /></QueryClientProvider></I18nextProvider>);

    await waitFor(() => expect(listHistory).toHaveBeenCalledTimes(1));
    const search = screen.getByRole("textbox", { name: "Search" });
    await user.type(search, "draft-only");
    await user.click(screen.getByText("Search options and filters"));
    const provider = screen.getByRole("textbox", { name: "Filter by Provider" });
    const scope = screen.getByRole("combobox", { name: "Search scope" });
    await user.type(provider, "relay");
    await user.selectOptions(scope, "content");
    expect(listHistory).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("status")).toHaveTextContent("Press Search to apply");

    await user.click(screen.getByRole("button", { name: "Clear filters" }));
    expect(listHistory).toHaveBeenCalledTimes(1);
    expect(search).toHaveValue("");
    expect(provider).toHaveValue("");
    expect(scope).toHaveValue("metadata");
    expect(search).toHaveFocus();
  });

  it("uses one fresh post-write status read and displays that result", async () => {
    const user = userEvent.setup();
    const getStatus = vi.fn().mockResolvedValueOnce(alignedStatus("openai")).mockResolvedValueOnce(alignedStatus("relay"));
    render(<AppUi core={new MockCoreClient({ getStatus, prepareSync: async () => syncPlanFor(), applySync: async () => operationResult("fresh-operation") })} host={{ listProfiles: async () => [profile] }} preferences={preferences} initialLocale="en" initialTheme="system" surface="desktop" />);

    const preview = await screen.findByRole("button", { name: "Preview sync" });
    await waitFor(() => expect(preview).toBeEnabled());
    await user.click(preview);
    await user.click(await screen.findByRole("button", { name: "Confirm sync" }));
    expect(await screen.findByText("Final Provider check")).toBeVisible();
    const resultDialog = screen.getByRole("dialog", { name: "Operation result" });
    await waitFor(() => expect(within(resultDialog).getByText("relay")).toBeVisible());
    expect(within(resultDialog).getByText("In sync")).toBeVisible();
    expect(getStatus).toHaveBeenCalledTimes(2);
  });

  it("marks the final status unverified when its fresh read fails", async () => {
    const user = userEvent.setup();
    const getStatus = vi.fn().mockResolvedValueOnce(alignedStatus()).mockRejectedValueOnce(new Error("fixture status read failed"));
    render(<AppUi core={new MockCoreClient({ getStatus, prepareSync: async () => syncPlanFor(), applySync: async () => operationResult("failed-status-operation") })} host={{ listProfiles: async () => [profile] }} preferences={preferences} initialLocale="en" initialTheme="system" surface="desktop" />);

    const preview = await screen.findByRole("button", { name: "Preview sync" });
    await waitFor(() => expect(preview).toBeEnabled());
    await user.click(preview);
    await user.click(await screen.findByRole("button", { name: "Confirm sync" }));
    expect(await screen.findByText("The operation result is available, but the final Provider alignment has not been verified. Refresh Overview to check.")).toBeVisible();
    expect(getStatus).toHaveBeenCalledTimes(2);
  });
});
