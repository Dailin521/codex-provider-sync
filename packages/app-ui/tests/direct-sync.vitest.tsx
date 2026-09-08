import type { OperationResult, PlanSummary } from "@codex-provider-sync/contracts";
import { MockCoreClient } from "@codex-provider-sync/core-client";
import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { AppUi } from "../src/App.js";
import { statusFor, syncPlanFor } from "./helpers/app-fixtures.js";

function mount(core: MockCoreClient, dismissOperationPlan = vi.fn(async () => {})) {
  render(<AppUi surface="desktop" core={core} host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }], dismissOperationPlan }} initialLocale="en" initialTheme="system" preferences={{ getLocale: () => "en", setLocale: () => {}, getTheme: () => "system", setTheme: () => {} }} />);
}

describe("Direct sync", () => {
  it.each(["completed", "partial"] as const)("prepares once and applies the same plan without confirmation, showing %s", async (outcome) => {
    const user = userEvent.setup();
    let finish!: (value: OperationResult) => void;
    const prepareSync = vi.fn(async () => syncPlanFor());
    const applySync = vi.fn(() => new Promise<OperationResult>(resolve => { finish = resolve; }));
    const diagnostics = vi.fn();
    const repair = vi.fn();
    mount(new MockCoreClient({ getStatus: async () => statusFor(), prepareSync, applySync, getDiagnostics: diagnostics, prepareRepair: repair }));
    const button = await screen.findByRole("button", { name: "Sync now" });
    await waitFor(() => expect(button).toBeEnabled());
    await user.dblClick(button);
    await waitFor(() => expect(applySync).toHaveBeenCalledOnce());
    expect(prepareSync).toHaveBeenCalledOnce();
    expect(prepareSync.mock.calls[0][0]).toEqual({ profile: { profileId: "default", profileRevision: "profile-r1" }, keepCount: 2 });
    expect(applySync.mock.calls[0][0]).toEqual({ schemaVersion: 1, planId: "plan-sync-1" });
    expect(screen.queryByRole("dialog", { name: "Confirm sync" })).not.toBeInTheDocument();
    expect(await screen.findByRole("dialog", { name: "Syncing" })).toBeVisible();
    expect(button).toBeDisabled();
    await act(async () => finish({ schemaVersion: 1, operationId: "direct-operation", operation: "sync", outcome, backup: { backupId: "undo-direct" }, result: {}, warnings: [] }));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Syncing" })).not.toBeInTheDocument());
    const result = await screen.findByRole("dialog");
    expect(within(result).getByText("A backup was created. You can find it under Backups and Restore.")).toBeVisible();
    expect(diagnostics).not.toHaveBeenCalled();
    expect(repair).not.toHaveBeenCalled();
    await user.click(within(result).getAllByRole("button", { name: "Close", exact: true }).at(-1)!);
    await waitFor(() => expect(button).toHaveFocus());
  });

  it("does not apply a plan returned after cancellation during prepare", async () => {
    const user = userEvent.setup();
    let finish!: (value: PlanSummary) => void;
    const prepareSync = vi.fn(() => new Promise<PlanSummary>(resolve => { finish = resolve; }));
    const applySync = vi.fn();
    const dismiss = vi.fn(async () => {});
    mount(new MockCoreClient({ getStatus: async () => statusFor(), prepareSync, applySync }), dismiss);
    const button = await screen.findByRole("button", { name: "Sync now" });
    await waitFor(() => expect(button).toBeEnabled());
    await user.click(button);
    const dialog = await screen.findByRole("dialog", { name: "Checking sync changes" });
    await user.click(within(dialog).getByRole("button", { name: /cancel operation/i }));
    await act(async () => finish(syncPlanFor()));
    await waitFor(() => expect(button).toBeEnabled());
    expect(applySync).not.toHaveBeenCalled();
    expect(dismiss).toHaveBeenCalledWith("plan-sync-1");
  });

  it("shows prepare failure without applying and keeps preview as a separate path", async () => {
    const user = userEvent.setup();
    const prepareSync = vi.fn().mockRejectedValueOnce({ code: "STALE_STATE" }).mockResolvedValue(syncPlanFor());
    const applySync = vi.fn();
    mount(new MockCoreClient({ getStatus: async () => statusFor(), prepareSync, applySync }));
    const button = await screen.findByRole("button", { name: "Sync now" });
    await waitFor(() => expect(button).toBeEnabled());
    await user.click(button);
    await waitFor(() => expect(prepareSync).toHaveBeenCalledOnce());
    await waitFor(() => expect(button).toBeEnabled());
    expect(applySync).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Preview sync" }));
    expect(await screen.findByRole("dialog", { name: "Confirm sync" })).toBeVisible();
    expect(applySync).not.toHaveBeenCalled();
  });
});
