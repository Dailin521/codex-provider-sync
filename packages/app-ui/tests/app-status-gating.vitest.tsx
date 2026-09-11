import type { StatusSnapshot } from "@codex-provider-sync/contracts";
import { MockCoreClient } from "@codex-provider-sync/core-client";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { AppUi } from "../src/App.js";

const status: StatusSnapshot = {
  schemaVersion: 1,
  snapshotAt: "2026-08-27T00:00:00.000Z",
  storageRevision: "storage-r1",
  profile: { id: "default", revision: "profile-r1" },
  currentProvider: "openai",
  rolloutCounts: { sessions: { openai: 1 }, archived_sessions: {} },
  sqliteCounts: { sessions: { openai: 1 }, archived_sessions: {} },
  codexHomeSource: "profile",
  sqliteHomeSource: "default",
  backupSummary: { count: 0, totalBytes: 0 },
  pendingRecovery: false,
  pendingTransactions: [],
  operationInProgress: null,
  rolloutScanComplete: true,
  lockedRolloutFiles: []
};

const preferences = {
  getLocale: () => "en" as const, setLocale: () => {},
  getTheme: () => "system" as const, setTheme: () => {}
};

describe("App write gating", () => {
  it("allows normal preview/sync for a verified stale lock without automatic cleanup", async () => {
    const readStatus = vi.fn().mockResolvedValue({ ...status, staleLockDetected: true });
    const prepareSync = vi.fn();
    const core = new MockCoreClient({ getStatus: readStatus, prepareSync });
    render(<AppUi surface="desktop" core={core} host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }} initialLocale="en" initialTheme="system" preferences={preferences} />);
    expect(await screen.findByText("A previous operation has ended")).toBeVisible();
    expect(screen.queryByText("Operation in progress")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Preview sync" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Sync now" })).toBeEnabled();
    expect(prepareSync).not.toHaveBeenCalled();
    expect(readStatus).toHaveBeenCalledTimes(1);
  });

  it("distinguishes an unverified lock from a running operation and permits only manual recheck", async () => {
    const readStatus = vi.fn().mockResolvedValueOnce({
      ...status, operationInProgress: { lockState: "unverifiable", errorCode: "LOCK_UNVERIFIABLE" },
      statusReadBlocked: { reason: "codex-home-lock" }
    }).mockResolvedValue(status);
    const core = new MockCoreClient({ getStatus: readStatus });
    const user = userEvent.setup();
    render(<AppUi surface="desktop" core={core} host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }} initialLocale="en" initialTheme="system" preferences={preferences} />);
    expect(await screen.findAllByText("Storage lock needs checking")).toHaveLength(2);
    expect(screen.queryByText("Operation in progress")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Sync now" })).toBeDisabled();
    expect(readStatus).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole("button", { name: "Check status again" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Sync now" })).toBeEnabled());
    expect(readStatus).toHaveBeenCalledTimes(2);
  });

  for (const reason of ["state-changed-during-status", "revision-unverifiable"]) {
    it(`shows refresh-needed rather than busy for ${reason}, with manual recovery`, async () => {
      const readStatus = vi.fn().mockResolvedValueOnce({
        ...status, statusReadBlocked: { reason }, rolloutScanComplete: false
      }).mockResolvedValue(status);
      const core = new MockCoreClient({ getStatus: readStatus, getWatchStatus: async () => ({ schemaVersion: 1, watches: [] }) });
      const user = userEvent.setup();
      render(<AppUi surface="desktop" core={core} host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }} initialLocale="en" initialTheme="system" preferences={preferences} />);
      expect(await screen.findByRole("button", { name: "Check status again" })).toBeEnabled();
      expect(screen.getAllByText("Refresh needed")).toHaveLength(2);
      expect(screen.queryByText("Operation in progress")).not.toBeInTheDocument();
      expect(screen.queryByText("Ready", { exact: true })).not.toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Preview sync" })).toBeDisabled();
      expect(screen.getByRole("button", { name: "Sync now" })).toBeDisabled();
      expect(screen.getByRole("button", { name: "Preview switch" })).toBeDisabled();
      expect(screen.queryByText("0 B")).not.toBeInTheDocument();
      await user.click(screen.getByRole("button", { name: "Settings", exact: true }));
      expect(await screen.findByRole("button", { name: "Enable automatic sync" })).toBeDisabled();
      expect(readStatus).toHaveBeenCalledTimes(1);
      await user.click(screen.getByRole("button", { name: "Check status again" }));
      await waitFor(() => expect(screen.getByRole("button", { name: "Enable automatic sync" })).toBeEnabled());
      expect(screen.queryByText("Refresh needed")).not.toBeInTheDocument();
      expect(readStatus).toHaveBeenCalledTimes(2);
      await user.click(screen.getByRole("button", { name: "Overview", exact: true }));
      expect(await screen.findByRole("button", { name: "Sync now" })).toBeEnabled();
      expect(readStatus).toHaveBeenCalledTimes(2);
    });
  }

  it("continues to block writes when a real external operation is present", async () => {
    const core = new MockCoreClient({ getStatus: async () => ({
      ...status, operationInProgress: { operationId: "held-operation", operation: "sync", actor: "external" },
      statusReadBlocked: { reason: "codex-home-lock" }
    }) });
    render(<AppUi surface="desktop" core={core} host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }} initialLocale="en" initialTheme="system" preferences={preferences} />);
    expect(await screen.findAllByText("Operation in progress")).toHaveLength(2);
    expect(screen.queryByRole("button", { name: "Check status again" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Preview sync" })).toBeDisabled();
  });

  it("keeps protected writes disabled until a successful Status snapshot arrives", async () => {
    let resolveStatus!: (value: StatusSnapshot) => void;
    const pendingStatus = new Promise<StatusSnapshot>((resolve) => { resolveStatus = resolve; });
    const core = new MockCoreClient({
      getStatus: () => pendingStatus,
      getWatchStatus: async () => ({ schemaVersion: 1, watches: [] })
    });
    const user = userEvent.setup();
    render(
      <AppUi
        surface="desktop"
        core={core}
        host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }}
        initialLocale="en"
        initialTheme="system"
        preferences={{
          getLocale: () => "en",
          setLocale: () => {},
          getTheme: () => "system",
          setTheme: () => {}
        }}
      />
    );

    expect(await screen.findByRole("button", { name: "Preview sync" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Settings", exact: true }));
    expect(await screen.findByRole("button", { name: "Enable automatic sync" })).toBeDisabled();

    resolveStatus(status);
    await waitFor(() => expect(screen.getByRole("button", { name: "Enable automatic sync" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "Overview", exact: true }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Preview sync" })).toBeEnabled());
  });
});
