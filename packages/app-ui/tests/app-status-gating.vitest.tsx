import type { StatusSnapshot } from "@codex-provider-sync/contracts";
import { MockCoreClient } from "@codex-provider-sync/core-client";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

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

describe("App write gating", () => {
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

    await user.click(await screen.findByRole("button", { name: "Sync", exact: true }));
    expect(await screen.findByRole("button", { name: "Prepare sync" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Settings", exact: true }));
    expect(await screen.findByRole("button", { name: "Start watch" })).toBeDisabled();

    resolveStatus(status);
    await waitFor(() => expect(screen.getByRole("button", { name: "Start watch" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "Sync", exact: true }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Prepare sync" })).toBeEnabled());
  });
});
