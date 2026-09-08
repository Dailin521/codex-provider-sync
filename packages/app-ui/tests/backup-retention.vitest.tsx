import { MockCoreClient } from "@codex-provider-sync/core-client";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { AppUi } from "../src/App.js";
import { createBackupPreferences, readBackupRetention } from "../src/shared/backup-preferences.js";
import type { PreferenceStore } from "../src/types.js";
import { statusFor } from "./helpers/app-fixtures.js";

function preferences(initial: string | null = null) {
  let stored = initial;
  const storage = { getItem: () => stored, setItem: vi.fn((_key: string, value: string) => { stored = value; }) };
  const store: PreferenceStore = { getLocale: () => "en", setLocale: () => {}, getTheme: () => "system", setTheme: () => {}, ...createBackupPreferences(storage, "fixture") };
  return { store, storage };
}
function mount(store: PreferenceStore, watchMode: "idle" | "active" | "failed" = "idle", watchVisible = true) {
  const failPrepare = () => vi.fn(async () => { throw { code: "INVALID_INPUT" }; });
  const prepareSync = failPrepare(), prepareSwitch = failPrepare(), prepareRepair = failPrepare(), startWatch = failPrepare();
  const pruneBackups = vi.fn(async () => ({ removed: [], kept: [] }));
  const getWatchStatus = vi.fn(async () => {
    if (watchMode === "failed") throw { code: "CORE_RUNTIME_CRASHED" };
    return { schemaVersion: 1 as const, watches: watchMode === "active" ? [{ schemaVersion: 1 as const, watchId: "watch-other", status: "running" as const, startedAt: "2026-09-07T00:00:00.000Z", stoppedAt: null, stopReason: null, includeStateDb: true, once: false }] : [] };
  });
  const core = new MockCoreClient({ getStatus: async () => statusFor(), listBackups: async () => ({ schemaVersion: 1, backups: [] }), getWatchStatus, prepareSync, prepareSwitch, prepareRepair, startWatch, pruneBackups });
  const view = render(<AppUi capabilities={{ watch: watchVisible }} surface="desktop" core={core} host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }, { id: "other", name: "Other", revision: "r2" }] }} preferences={store} initialLocale="en" initialTheme="system" />);
  return { ...view, prepareSync, prepareSwitch, prepareRepair, startWatch, pruneBackups, getWatchStatus };
}
async function editCount(user: ReturnType<typeof userEvent.setup>, value: string) {
  await user.click(await screen.findByRole("button", { name: "Backups / Restore" }));
  const input = await screen.findByRole("spinbutton", { name: "Backups to retain" });
  await waitFor(() => expect(input).toBeEnabled());
  await user.clear(input);
  if (value) await user.type(input, value);
}

describe("One backup management rule", () => {
  it("saves without pruning and supplies Sync, direct Sync, Switch, Repair and Watch", async () => {
    const user = userEvent.setup();
    const { store, storage } = preferences();
    const actions = mount(store);
    await editCount(user, "4");
    expect(screen.getAllByRole("spinbutton")).toHaveLength(1);
    expect(screen.getByRole("button", { name: "Delete older backups" })).toBeDisabled();
    expect(actions.pruneBackups).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Save backup settings" }));
    expect(await screen.findByText("Backup settings saved. No backups were deleted.")).toBeVisible();
    expect(actions.getWatchStatus).toHaveBeenCalledWith({}, expect.anything(), expect.anything());
    expect(storage.setItem).toHaveBeenCalledWith("fixture.backup.retention", "4");
    expect(actions.pruneBackups).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Delete older backups" }));
    expect(screen.getByRole("dialog", { name: "Confirm backup cleanup" })).toBeVisible();
    expect(actions.pruneBackups).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Confirm cleanup" }));
    await waitFor(() => expect(actions.pruneBackups.mock.calls[0]?.[0].keepCount).toBe(4));
    await user.click(screen.getByRole("button", { name: "Overview" }));
    expect(screen.queryByRole("spinbutton")).not.toBeInTheDocument();
    await user.click(await screen.findByRole("button", { name: "Preview sync" }));
    await waitFor(() => expect(actions.prepareSync.mock.calls[0]?.[0].keepCount).toBe(4));
    await user.click(screen.getByRole("button", { name: "Sync now" }));
    await waitFor(() => expect(actions.prepareSync.mock.calls[1]?.[0].keepCount).toBe(4));
    await user.click(screen.getByRole("button", { name: "Preview switch" }));
    await waitFor(() => expect(actions.prepareSwitch.mock.calls[0]?.[0].keepCount).toBe(4));
    await user.click(screen.getByRole("button", { name: "Advanced features" }));
    await user.click(screen.getByText("Advanced adjustments", { exact: true }));
    expect(screen.queryByRole("spinbutton")).not.toBeInTheDocument();
    await user.click(screen.getByLabelText("Unify historical model names"));
    await user.click(screen.getByRole("button", { name: "Preview adjustment" }));
    await waitFor(() => expect(actions.prepareRepair.mock.calls[0]?.[0].keepCount).toBe(4));
    await user.selectOptions(screen.getByLabelText("Profile"), "other");
    await user.click(screen.getByRole("button", { name: "Settings", exact: true }));
    const start = await screen.findByRole("button", { name: "Enable automatic sync" });
    await waitFor(() => expect(start).toBeEnabled());
    await user.click(start);
    await waitFor(() => expect(actions.startWatch.mock.calls[0]?.[0]).toMatchObject({ keepCount: 4, profile: { profileId: "other" } }));
    actions.unmount();
    const reopened = mount(store);
    const preview = await screen.findByRole("button", { name: "Preview sync" });
    await waitFor(() => expect(preview).toBeEnabled());
    await user.click(preview);
    await waitFor(() => expect(reopened.prepareSync.mock.calls[0]?.[0].keepCount).toBe(4));
  });

  it.each(["active", "failed"] as const)("does not save or prune when Watch is %s", async (mode) => {
    const user = userEvent.setup();
    const { store, storage } = preferences("5");
    const actions = mount(store, mode);
    await editCount(user, "3");
    await user.click(screen.getByRole("button", { name: "Save backup settings" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(mode === "active" ? "Automatic sync is active" : "Could not save backup settings");
    expect(storage.setItem).not.toHaveBeenCalled();
    expect(readBackupRetention(store)).toBe(5);
    expect(actions.pruneBackups).not.toHaveBeenCalled();
  });

  it("keeps the previous preference when storage fails", async () => {
    const user = userEvent.setup();
    const { store, storage } = preferences("5");
    storage.setItem.mockImplementation(() => { throw new Error("storage unavailable"); });
    const actions = mount(store);
    await editCount(user, "3");
    await user.click(screen.getByRole("button", { name: "Save backup settings" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Could not save backup settings");
    expect(readBackupRetention(store)).toBe(5);
    expect(actions.pruneBackups).not.toHaveBeenCalled();
  });

  it("still checks active Watch when the Watch UI is hidden", async () => {
    const user = userEvent.setup();
    const { store, storage } = preferences("5");
    const actions = mount(store, "active", false);
    await editCount(user, "3");
    await user.click(screen.getByRole("button", { name: "Save backup settings" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Automatic sync is active");
    expect(actions.getWatchStatus).toHaveBeenCalledOnce();
    expect(storage.setItem).not.toHaveBeenCalled();
  });

  it.each(["", "0", "1.5", "1001"])("rejects invalid saved count %s without cleanup", async (value) => {
    const user = userEvent.setup();
    const { store, storage } = preferences();
    const actions = mount(store);
    await editCount(user, value);
    expect(screen.getByRole("button", { name: "Save backup settings" })).toBeDisabled();
    expect(storage.setItem).not.toHaveBeenCalled();
    expect(actions.pruneBackups).not.toHaveBeenCalled();
  });

  it("defaults malformed/missing preferences to 2 and never persists invalid values", () => {
    for (const initial of [null, "", "0", "-1", "1.5", "5000", "nope"]) expect(readBackupRetention(preferences(initial).store)).toBe(2);
    const { store, storage } = preferences("7");
    expect(readBackupRetention(store)).toBe(7);
    expect(() => store.setBackupRetention?.(0)).toThrow();
    expect(storage.setItem).not.toHaveBeenCalled();
  });
});
