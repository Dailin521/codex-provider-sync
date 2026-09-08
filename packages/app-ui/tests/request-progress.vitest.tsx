import type { CoreRequestProgressEnvelope, PlanSummary } from "@codex-provider-sync/contracts";
import { MockCoreClient } from "@codex-provider-sync/core-client";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { afterEach, expect, it, vi } from "vitest";
import { AppUi } from "../src/App.js";
import { createAppI18n } from "../src/i18n.js";
import { RequestProgress, useRequestProgress } from "../src/shared/request-progress.js";
import { statusFor, syncPlanFor } from "./helpers/app-fixtures.js";

afterEach(() => vi.useRealTimers());
const frame = (requestId = "request-1"): CoreRequestProgressEnvelope => ({ protocolVersion: 1, requestId, event: "request-progress", progress: { stage: "scan_sessions", status: "running", progress: 0.25, count: 12 } });
const preferences = { getLocale: () => "en" as const, setLocale: () => {}, getTheme: () => "system" as const, setTheme: () => {} };

it("shows genuine per-stage percent, processed count and a local elapsed clock only while mounted", async () => {
  const i18n = await createAppI18n("en");
  vi.useFakeTimers();
  const startedAt = performance.now();
  const { rerender, unmount } = render(<I18nextProvider i18n={i18n}><RequestProgress state={{ startedAt, progress: null }} /></I18nextProvider>);
  expect(screen.getByRole("progressbar")).not.toHaveAttribute("value");
  expect(screen.getByText("Elapsed 0:00")).toBeVisible();
  act(() => { vi.advanceTimersByTime(65000); });
  expect(screen.getByText("Elapsed 1:05")).toBeVisible();
  rerender(<I18nextProvider i18n={i18n}><RequestProgress state={{ startedAt, progress: frame().progress }} /></I18nextProvider>);
  expect(screen.getByRole("progressbar", { name: "Checking session files…" })).toHaveAttribute("value", "25");
  expect(screen.getByText("12 files checked")).toBeVisible();
  expect(screen.getByText("This stage: 25%")).toBeVisible();
  unmount();
  expect(vi.getTimerCount()).toBe(0);
});

it("ignores stale frames and completion from a previous request or Profile", async () => {
  const i18n = await createAppI18n("en");
  let start!: ReturnType<typeof useRequestProgress>["start"];
  function Harness({ profileKey }: { profileKey: string }) {
    const progress = useRequestProgress(profileKey);
    start = progress.start;
    return <RequestProgress state={progress.state} />;
  }
  const { rerender } = render(<I18nextProvider i18n={i18n}><Harness profileKey="a:1" /></I18nextProvider>);
  let old!: ReturnType<typeof start>;
  act(() => { old = start(); });
  rerender(<I18nextProvider i18n={i18n}><Harness profileKey="b:1" /></I18nextProvider>);
  act(() => { old.onRequestProgress(frame()); });
  expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
  let current!: ReturnType<typeof start>;
  act(() => { current = start(); old.finish(); old.onRequestProgress(frame()); });
  expect(screen.getByRole("progressbar")).not.toHaveAttribute("value");
  act(() => { current.onRequestProgress(frame()); });
  expect(screen.getByRole("progressbar")).toHaveAttribute("value", "25");
  act(() => { current.finish(); current.onRequestProgress(frame()); });
  expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
});

it("subscribes only when diagnostics is explicitly requested and clears progress on failure without polling", async () => {
  const user = userEvent.setup();
  let notify!: (event: CoreRequestProgressEnvelope) => void;
  let reject!: (error: unknown) => void;
  const getDiagnostics = vi.fn((_input, _request, control) => {
    notify = control.onRequestProgress;
    return new Promise<never>((_resolve, fail) => { reject = fail; });
  });
  render(<AppUi surface="desktop" initialLocale="en" preferences={preferences} core={new MockCoreClient({ getStatus: async () => statusFor(), getDiagnostics })} host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }} />);
  await screen.findByRole("button", { name: "Sync now" });
  await user.click(screen.getByRole("button", { name: "Advanced features", exact: true }));
  expect(getDiagnostics).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "Start diagnostics" }));
  expect(await screen.findByRole("progressbar")).not.toHaveAttribute("value");
  act(() => { notify(frame()); });
  expect(screen.getByRole("progressbar")).toHaveAttribute("value", "25");
  await act(async () => reject({ code: "INTERNAL_ERROR" }));
  expect(await screen.findByRole("alert")).toHaveTextContent("Diagnostics could not finish");
  act(() => notify(frame()));
  expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Overview", exact: true }));
  await user.click(screen.getByRole("button", { name: "Advanced features", exact: true }));
  expect(getDiagnostics).toHaveBeenCalledOnce();
});

it("keeps Repair preview progress at the clicked form, blocks duplicates and stops when the plan opens", async () => {
  const user = userEvent.setup();
  let notify!: (event: CoreRequestProgressEnvelope) => void;
  let resolve!: (value: PlanSummary) => void;
  const prepareRepair = vi.fn((_input, _request, control) => {
    notify = control.onRequestProgress;
    return new Promise<PlanSummary>((done) => { resolve = done; });
  });
  render(<AppUi surface="desktop" initialLocale="en" preferences={preferences} core={new MockCoreClient({ getStatus: async () => statusFor(), prepareRepair })} host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }} />);
  await screen.findByRole("button", { name: "Sync now" });
  await user.click(screen.getByRole("button", { name: "Advanced features", exact: true }));
  await user.click(screen.getByText("Targeted repair", { exact: true }));
  await user.click(screen.getAllByRole("checkbox")[0]);
  const submit = screen.getByRole("button", { name: "Preview repair" });
  await user.click(submit);
  await waitFor(() => expect(prepareRepair).toHaveBeenCalledOnce());
  expect(submit).toBeDisabled();
  act(() => notify(frame()));
  expect(screen.getByRole("progressbar")).toBeVisible();
  expect(submit.closest("form")).toContainElement(screen.getByRole("progressbar"));
  const plan = { ...syncPlanFor(), operation: "repair" as const, target: { targets: ["cwd"], scope: "all" }, expiresAt: new Date(Date.now() + 600000).toISOString() };
  await act(async () => resolve(plan));
  expect(await screen.findByRole("dialog")).toBeVisible();
  expect(screen.queryByTestId("request-progress")).not.toBeInTheDocument();
});
