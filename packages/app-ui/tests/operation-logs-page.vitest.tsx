import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it, vi } from "vitest";

import { OperationLogsPage } from "../src/features/operation-logs/OperationLogsPage.js";
import { createAppI18n } from "../src/i18n.js";
import { ClipboardContext } from "../src/shared/clipboard.js";

const entry = {
  schemaVersion: 1 as const,
  id: "log-1",
  operation: "sync",
  profileId: "fixture",
  startedAt: "2026-09-03T00:00:00.000Z",
  completedAt: "2026-09-03T00:00:01.000Z",
  activeDurationMs: 500,
  wallDurationMs: 1000,
  status: "completed" as const,
  outcome: "completed",
  targetProvider: "dal",
  previewCounts: { rolloutFilesToChange: 2, sqliteRowsToChange: 3, lockedRolloutFiles: 0 },
  requestIds: ["request-1"],
  planId: "plan-1",
  operationId: "operation-1",
  backupId: "backup-1",
  counts: { changedSessionFiles: 2, sqliteRowsUpdated: 3 },
  warnings: [],
  stages: [{ stage: "create_backup", status: "completed" as const, startedAt: "2026-09-03T00:00:00.000Z", completedAt: "2026-09-03T00:00:00.500Z", durationMs: 500, count: 1 }]
};

describe("OperationLogsPage", () => {
  it("uses independent panes, resets only detail scroll and restores the selected row on Back", async () => {
    const entries = [entry, { ...entry, id: "log-2", operation: "switch", counts: { rewrittenSessionFiles: 100 }, wallDurationMs: 21000 }];
    const getOperationLog = vi.fn(async (id: string) => entries.find((item) => item.id === id)!);
    const listOperationLogs = vi.fn(async () => ({ schemaVersion: 1 as const, page: 1, pageSize: 50, total: 2, hasNextPage: false, entries }));
    const i18n = await createAppI18n("en");
    render(<I18nextProvider i18n={i18n}><QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><OperationLogsPage host={{ listProfiles: async () => [], listOperationLogs, getOperationLog }} profileId="fixture" /></QueryClientProvider></I18nextProvider>);
    const user = userEvent.setup();
    const list = await screen.findByRole("region", { name: "Operation list" });
    const row = (await within(list).findByText("1.0 seconds")).closest("button")!;
    list.scrollTop = 800;
    await user.click(row);
    const detail = screen.getByRole("region", { name: "Operation details" });
    expect(row).toHaveAttribute("aria-pressed", "true");
    for (const region of [list, detail]) expect(region).toHaveClass("overflow-y-auto", "overscroll-contain");
    expect(detail).toHaveAttribute("tabindex", "0");
    expect(screen.getByTestId("operation-logs-workspace")).toHaveClass("min-h-0", "flex-1", "overflow-hidden");
    expect(list.parentElement).toHaveClass("hidden", "lg:flex");
    expect(detail.parentElement).toHaveClass("flex");
    await within(detail).findByText("Create backup");
    detail.scrollTop = 600;
    const secondRow = within(list).getByText("21.0 seconds").closest("button")!;
    await user.click(secondRow);
    await within(detail).findByText("View speed-up tips");
    expect(detail.scrollTop).toBe(0);
    expect(list.scrollTop).toBe(800);
    expect(row).toHaveAttribute("aria-pressed", "false");
    expect(secondRow).toHaveAttribute("aria-pressed", "true");
    await user.click(screen.getByRole("button", { name: "Refresh operation details" }));
    await waitFor(() => expect(getOperationLog).toHaveBeenCalledTimes(3));
    await user.click(screen.getByRole("button", { name: "Back to operations" }));
    await waitFor(() => expect(secondRow).toHaveFocus());
    expect(list.parentElement).not.toHaveClass("hidden");
    expect(detail.parentElement).toHaveClass("hidden", "lg:flex");
    expect(list.scrollTop).toBe(800);
    expect(listOperationLogs).toHaveBeenCalledOnce();
    expect(getOperationLog).toHaveBeenCalledTimes(3);
  });

  it("clears selection on pagination, filtering and removal during manual refresh", async () => {
    let entries = [entry];
    const listOperationLogs = vi.fn(async ({ page }: { page: number; operation?: string }) => ({ schemaVersion: 1 as const, page, pageSize: 50, total: 51, hasNextPage: page === 1, entries }));
    const getOperationLog = vi.fn(async () => entry);
    const i18n = await createAppI18n("en");
    render(<I18nextProvider i18n={i18n}><QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><OperationLogsPage host={{ listProfiles: async () => [], listOperationLogs, getOperationLog }} profileId="fixture" /></QueryClientProvider></I18nextProvider>);
    const user = userEvent.setup();
    const select = async () => { await user.click((await screen.findByText("1.0 seconds")).closest("button")!); await screen.findByText("request-1"); };
    await select();
    await user.click(screen.getByRole("button", { name: "Next" }));
    await waitFor(() => expect(screen.queryByText("request-1")).not.toBeInTheDocument());
    await screen.findByText("Page 2 · 51 operations");
    await select();
    await user.selectOptions(screen.getByRole("combobox", { name: "Operation filter" }), "sync");
    await waitFor(() => expect(screen.queryByText("request-1")).not.toBeInTheDocument());
    expect(listOperationLogs.mock.calls.some(([input]) => input.operation === "sync" && input.page === 2)).toBe(false);
    await select();
    entries = [];
    screen.getByRole("region", { name: "Operation details" }).focus();
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => expect(screen.queryByText("request-1")).not.toBeInTheDocument());
    await waitFor(() => expect(screen.getByRole("region", { name: "Operation list" })).toHaveFocus());
    expect(screen.getByText("Select an operation to view its result and timing.")).toBeInTheDocument();
  });

  it("does not show cached details from a late earlier selection and explains rotated logs", async () => {
    let resolveFirst!: (value: typeof entry) => void;
    const first = new Promise<typeof entry>((resolve) => { resolveFirst = resolve; });
    const getOperationLog = vi.fn((id: string) => id === "log-1" ? first : Promise.resolve(null));
    const i18n = await createAppI18n("en");
    render(<I18nextProvider i18n={i18n}><QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><OperationLogsPage host={{ listProfiles: async () => [], getOperationLog, listOperationLogs: async () => ({ schemaVersion: 1, page: 1, pageSize: 50, total: 2, hasNextPage: false, entries: [entry, { ...entry, id: "log-2", wallDurationMs: 21000 }] }) }} profileId="fixture" /></QueryClientProvider></I18nextProvider>);
    await userEvent.click((await screen.findByText("1.0 seconds")).closest("button")!);
    await userEvent.click(screen.getByText("21.0 seconds").closest("button")!);
    await screen.findByText("This log is no longer available. It may have been removed by log rotation.");
    resolveFirst(entry);
    await waitFor(() => expect(screen.queryByText("request-1")).not.toBeInTheDocument());
  });

  it("shows the Provider, planned counts and specific pre-write failure without inventing writes", async () => {
    const failed = { ...entry, status: "failed" as const, outcome: "failed", errorCode: "STALE_STATE", errorReason: "state-db" as const,
      backupId: undefined, counts: {}, stages: [{ stage: "validate_plan", status: "failed" as const, startedAt: entry.startedAt, durationMs: 500 }] };
    const i18n = await createAppI18n("en");
    render(<I18nextProvider i18n={i18n}><QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><OperationLogsPage host={{
      listProfiles: async () => [{ id: "fixture", name: "Fixture", revision: "r1" }],
      listOperationLogs: async () => ({ schemaVersion: 1, page: 1, pageSize: 50, total: 1, hasNextPage: false, entries: [failed] }),
      getOperationLog: async () => failed
    }} profileId="fixture" /></QueryClientProvider></I18nextProvider>);
    await userEvent.click((await screen.findByText("1.0 seconds")).closest("button")!);
    expect(await screen.findByText("Recheck before writing")).toBeVisible();
    expect(screen.getByText("dal")).toBeVisible();
    expect(screen.getByRole("alert")).toHaveTextContent("Local chat index changed");
    expect(screen.queryByText("backup-1")).not.toBeInTheDocument();
  });

  it("shows Switch plans, makes partial results explicit, and does not infer details for old records", async () => {
    const planned = { ...entry, operation: "switch", status: "partial" as const, outcome: "partial", switchPlan: { previousProvider: "openai", targetProvider: "relay", previousRootModel: null, targetRootModel: "relay/model", modelMode: "provider-default" } };
    const old = { ...entry, id: "old-switch", operation: "switch", targetProvider: undefined };
    let selected = planned;
    const i18n = await createAppI18n("en");
    render(<I18nextProvider i18n={i18n}><QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><OperationLogsPage host={{ listProfiles: async () => [], listOperationLogs: async () => ({ schemaVersion: 1, page: 1, pageSize: 50, total: 2, hasNextPage: false, entries: [planned, old] }), getOperationLog: async (id) => id === "old-switch" ? old : selected }} profileId="fixture" profileRevision="r2" /></QueryClientProvider></I18nextProvider>);
    const user = userEvent.setup();
    await user.click((await screen.findAllByText("1.0 seconds"))[0].closest("button")!);
    expect(await screen.findByText("Planned Provider switch")).toBeVisible();
    expect(screen.getByText("Not set → relay/model")).toBeVisible();
    expect(screen.getByText(/planned target.*finished partially/i)).toBeVisible();
    await user.click(screen.getAllByText("1.0 seconds")[1].closest("button")!);
    expect(await screen.findByText("This older record did not save switch details.")).toBeVisible();
  });

  it("loads once, refreshes manually and delays detail until selection", async () => {
    const listOperationLogs = vi.fn(async () => ({ schemaVersion: 1 as const, page: 1, pageSize: 50, total: 1, hasNextPage: false, entries: [entry] }));
    const getOperationLog = vi.fn(async () => entry);
    const i18n = await createAppI18n("en");
    const user = userEvent.setup();
    const copyText = vi.fn(async (_text: string) => {});
    render(<I18nextProvider i18n={i18n}><ClipboardContext.Provider value={copyText}><QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><OperationLogsPage host={{ listProfiles: async () => [{ id: "fixture", name: "Fixture", revision: "r1" }], listOperationLogs, getOperationLog }} profileId="fixture" /></QueryClientProvider></ClipboardContext.Provider></I18nextProvider>);
    const duration = await screen.findByText("1.0 seconds");
    const logRow = duration.closest("button");
    expect(logRow).not.toBeNull();
    expect(logRow).toBeVisible();
    expect(listOperationLogs).toHaveBeenCalledWith(expect.objectContaining({ profileId: "fixture" }), expect.anything());
    expect(getOperationLog).not.toHaveBeenCalled();
    window.dispatchEvent(new Event("focus"));
    expect(listOperationLogs).toHaveBeenCalledTimes(1);
    await user.click(logRow!);
    await waitFor(() => expect(getOperationLog).toHaveBeenCalledWith("log-1", expect.anything()));
    expect(await screen.findByText("Create backup")).toBeVisible();
    expect(screen.getByText("dal")).toBeVisible();
    expect(screen.getByText("Target Provider")).toBeVisible();
    expect(screen.getByRole("button", { name: "Copy Log number" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Copy Request number 1" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Copy Confirmation number" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Copy Operation number" })).toBeVisible();
    expect(screen.getByText("request-1")).toBeVisible();
    expect(screen.getByText("plan-1")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Copy Log number" }));
    expect(copyText).toHaveBeenLastCalledWith("log-1");
    expect(screen.getByText("Copied", { exact: true })).toBeVisible();
    copyText.mockRejectedValue(new Error("native failure"));
    await user.click(screen.getByRole("button", { name: "Copy Log number" }));
    expect(await screen.findByText("Could not copy. Try again, or select the text and copy it manually.")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => expect(listOperationLogs).toHaveBeenCalledTimes(2));
  });
});
