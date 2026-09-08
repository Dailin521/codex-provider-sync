import type { ManagedBackup } from "@codex-provider-sync/contracts";
import { MockCoreClient } from "@codex-provider-sync/core-client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it, vi } from "vitest";

import { AppUi } from "../src/App.js";
import { BackupsRestorePage } from "../src/features/backups-restore/BackupsRestorePage.js";
import { HistoryPage } from "../src/features/history/HistoryPage.js";
import { ProfilesPage } from "../src/features/profiles/ProfilesPage.js";
import { createAppI18n } from "../src/i18n.js";
import { ToastProvider } from "../src/ui.js";
import { statusFor } from "./helpers/app-fixtures.js";
import { historyProjectPage } from "./helpers/history-fixtures.js";

const profile = { id: "default", name: "Default", revision: "r1" };
const destination = { id: "other", name: "Other", revision: "r2", sqliteHomeConfigured: true };
const backup: ManagedBackup = { backupId: "fixture-backup", sizeBytes: 200, metadata: {} };
const preferences = { getLocale: () => "en" as const, setLocale: () => {}, getTheme: () => "system" as const, setTheme: () => {} };

describe("Approved user-facing review fixes", () => {
  it("shows backup read failure and retries manually instead of claiming no backups exist", async () => {
    const listBackups = vi.fn().mockRejectedValueOnce({ code: "PERMISSION_DENIED" }).mockResolvedValueOnce({ backups: [backup] });
    const user = userEvent.setup();
    render(<AppUi core={new MockCoreClient({ getStatus: async () => statusFor(), listBackups })} host={{ listProfiles: async () => [{ ...profile, revision: "profile-r1" }] }} preferences={preferences} initialLocale="en" initialTheme="system" surface="desktop" />);
    await user.click(await screen.findByRole("button", { name: "Backups / Restore" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Could not read backups");
    expect(screen.queryByText(/No backups yet/)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Delete older backups" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Retry", exact: true }));
    expect(await screen.findByText(backup.backupId)).toBeVisible();
    expect(listBackups).toHaveBeenCalledTimes(2);
  });

  it("uses captured targets, supports legacy backups, and clears config on relocation", async () => {
    const i18n = await createAppI18n("en");
    const user = userEvent.setup();
    const prepare = vi.fn(async () => {});
    const reduced: ManagedBackup = { ...backup, metadata: { capturedTargetKinds: { config: false, globalState: false, sqlite: true, rollout: false } } };
    const legacy = { ...backup, backupId: "legacy-backup" };
    render(<I18nextProvider i18n={i18n}><BackupsRestorePage profile={profile} profiles={[profile, destination]} backups={[reduced, legacy]} loading={false} disabled={false} canRestore canPrune prepare={prepare} prune={() => {}} /></I18nextProvider>);
    expect(screen.getByRole("button", { name: "Preview restore" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: /fixture-backup/ }));
    const config = screen.getByRole("checkbox", { name: "Restore Codex configuration" });
    const sessions = screen.getByRole("checkbox", { name: "Restore session files" });
    expect(config).toBeDisabled(); expect(config).not.toBeChecked();
    expect(sessions).toBeDisabled(); expect(sessions).not.toBeChecked();
    expect(screen.getByRole("checkbox", { name: "Restore local chat index" })).toBeChecked();
    await user.click(screen.getByRole("button", { name: /legacy-backup/ }));
    expect(config).toBeEnabled(); expect(config).toBeChecked();
    expect(sessions).toBeEnabled(); expect(sessions).toBeChecked();
    await user.click(screen.getByRole("checkbox", { name: "Restore to another storage profile" }));
    expect(config).not.toBeChecked(); expect(config).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Preview restore" }));
    expect(await screen.findByText("Choose a destination profile with a custom SQLite location.")).toBeVisible();
    expect(prepare).not.toHaveBeenCalled();
    await user.selectOptions(screen.getByRole("combobox", { name: "Destination storage profile" }), "other");
    await user.click(screen.getByRole("button", { name: "Preview restore" }));
    await waitFor(() => expect(prepare).toHaveBeenCalledWith(expect.objectContaining({ backupId: "legacy-backup", restoreConfig: false, restoreDatabase: true, restoreSessions: true, allowSqliteHomeRelocation: true, relocationTargetProfileId: "other" }), expect.anything()));
  });

  it("treats global-state-only capture as configuration, but does not offer index relocation", async () => {
    const i18n = await createAppI18n("en");
    const user = userEvent.setup();
    render(<I18nextProvider i18n={i18n}><BackupsRestorePage profile={profile} profiles={[profile]} backups={[{ ...backup, metadata: { capturedTargetKinds: { config: false, globalState: true, sqlite: false, rollout: false } } }]} loading={false} disabled={false} canRestore canPrune={false} prepare={async () => {}} prune={() => {}} /></I18nextProvider>);
    await user.click(screen.getByRole("button", { name: /fixture-backup/ }));
    expect(screen.getByRole("checkbox", { name: "Restore Codex configuration" })).toBeChecked();
    expect(screen.getByRole("checkbox", { name: "Restore local chat index" })).toBeDisabled();
    expect(screen.getByRole("checkbox", { name: "Restore to another storage profile" })).toBeDisabled();
  });

  it.each(["default", "other"])("deleting a profile preserves a different selection (%s)", async (selectedProfileId) => {
    const i18n = await createAppI18n("en");
    const user = userEvent.setup();
    const refresh = vi.fn(async () => {});
    const selectProfile = vi.fn();
    const deleteProfile = vi.fn(async () => {});
    render(<I18nextProvider i18n={i18n}><QueryClientProvider client={new QueryClient()}><ToastProvider><ProfilesPage profiles={[profile, destination]} selectedProfileId={selectedProfileId} host={{ listProfiles: async () => [], deleteProfile }} canManage revealPaths={false} surface="desktop" refresh={refresh} selectProfile={selectProfile} /></ToastProvider></QueryClientProvider></I18nextProvider>);
    await user.click(screen.getByRole("button", { name: /Other/ }));
    await user.click(screen.getByRole("button", { name: "Delete", exact: true }));
    await waitFor(() => expect(refresh).toHaveBeenCalledOnce());
    expect(deleteProfile).toHaveBeenCalledWith("other", "r2");
    if (selectedProfileId === "other") expect(selectProfile).toHaveBeenCalledWith("default");
    else expect(selectProfile).not.toHaveBeenCalled();
  });

  it("retries a failed chat detail without losing loaded list pages or fetching lists again", async () => {
    const i18n = await createAppI18n("en");
    const user = userEvent.setup();
    const first = { id: "chat-1", title: "First", provider: "openai", archived: false, updatedAt: "2026-09-05T00:00:00Z", messageCount: 0, messageCountKnown: false };
    const second = { ...first, id: "chat-2", title: "Second" };
    const listHistory = vi.fn(async ({ page }) => historyProjectPage([page === 1 ? first : second], { page, total: 2, hasNextPage: page === 1 }));
    const getHistorySession = vi.fn().mockRejectedValueOnce({ code: "INTERNAL_ERROR" }).mockResolvedValueOnce({ session: second, messages: [{ role: "user", text: "Synthetic recovered detail", sequence: 1 }], truncated: false, returnedMessageCount: 1 });
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(<I18nextProvider i18n={i18n}><QueryClientProvider client={client}><HistoryPage profile={profile} core={new MockCoreClient({ listHistory, getHistorySession })} /></QueryClientProvider></I18nextProvider>);
    await user.click(await screen.findByRole("button", { name: "Load more" }));
    await user.click(await screen.findByRole("button", { name: "View chat: Second" }));
    await screen.findByRole("alert");
    expect(screen.getByRole("heading", { name: "Second" })).toBeVisible();
    const loadedPage = screen.getByRole("button", { name: "View chat: Second" });
    const callCount = listHistory.mock.calls.length;
    await user.click(screen.getByRole("button", { name: "Retry", exact: true }));
    expect(await screen.findByText("Synthetic recovered detail")).toBeVisible();
    expect(screen.getByRole("button", { name: "View chat: Second" })).toBe(loadedPage);
    expect(listHistory).toHaveBeenCalledTimes(callCount);
    expect(getHistorySession).toHaveBeenCalledTimes(2);
    expect(JSON.stringify(client.getQueryCache().getAll().map((query) => query.state.data))).not.toContain("Synthetic recovered detail");
  });
});
