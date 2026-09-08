import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import { createAppI18n } from "../src/i18n.js";
import { ToastProvider, useToast } from "../src/ui.js";

// jsdom does not implement pointer capture; real Chromium covers the swipe path.
const pointerCaptureDescriptor = Object.getOwnPropertyDescriptor(Element.prototype, "hasPointerCapture");
beforeAll(() => {
  if (!pointerCaptureDescriptor) Object.defineProperty(Element.prototype, "hasPointerCapture", { configurable: true, value: () => false });
});
afterAll(() => {
  if (!pointerCaptureDescriptor) Reflect.deleteProperty(Element.prototype, "hasPointerCapture");
});

function Notifications() {
  const { push } = useToast();
  return <>
    <button onClick={() => push({ title: "Operation completed.", description: "Backup saved.", tone: "success" })}>Complete</button>
    <button onClick={() => push({ title: "Some sessions were skipped.", tone: "warning" })}>Partial</button>
    <button onClick={() => push({ title: "Operation failed.", tone: "danger" })}>Fail</button>
  </>;
}

async function setup(locale: "en" | "zh-CN" = "en") {
  const i18n = await createAppI18n(locale);
  render(<I18nextProvider i18n={i18n}><ToastProvider><Notifications /></ToastProvider></I18nextProvider>);
}

afterEach(() => vi.useRealTimers());

describe("dismissible notifications", () => {
  it.each(["title", "description", "close icon"])("dismisses a completed notification by clicking its %s", async (target) => {
    await setup();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Complete", exact: true }));
    const close = screen.getByRole("button", { name: "Dismiss notification: Operation completed." });
    const element = target === "title" ? within(close).getByText("Operation completed.")
      : target === "description" ? within(close).getByText("Backup saved.") : close.querySelector("svg")!;
    await user.click(element);
    expect(screen.queryByRole("button", { name: "Dismiss notification: Operation completed." })).not.toBeInTheDocument();
    // Dismissing feedback leaves the page and future operations available.
    await user.click(screen.getByRole("button", { name: "Complete", exact: true }));
    expect(screen.getByRole("button", { name: "Dismiss notification: Operation completed." })).toBeVisible();
  });

  it.each(["{Enter}", " ", "{Escape}"])("supports keyboard dismissal with %s", async (key) => {
    await setup();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Complete", exact: true }));
    const close = screen.getByRole("button", { name: "Dismiss notification: Operation completed." });
    const viewport = close.closest("ol");
    close.focus();
    expect(close).toHaveFocus();
    expect(close.className).toContain("focus-visible:ring-2");
    await user.keyboard(key);
    await waitFor(() => expect(screen.queryByRole("button", { name: "Dismiss notification: Operation completed." })).not.toBeInTheDocument());
    expect(viewport).toHaveFocus();
  });

  it("keeps the title and description in the screen-reader announcement", async () => {
    await setup();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Complete", exact: true }));
    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent("Operation completed.");
      expect(screen.getByRole("status")).toHaveTextContent("Backup saved.");
    });
  });

  it("localizes the close action and dismisses only the chosen notification", async () => {
    await setup("zh-CN");
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Complete", exact: true }));
    await user.click(screen.getByRole("button", { name: "Partial", exact: true }));
    await user.click(screen.getByRole("button", { name: "Fail", exact: true }));
    await user.click(screen.getByRole("button", { name: "关闭通知：Some sessions were skipped." }));
    expect(screen.queryByRole("button", { name: "关闭通知：Some sessions were skipped." })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "关闭通知：Operation completed." })).toBeVisible();
    expect(screen.getByRole("button", { name: "关闭通知：Operation failed." })).toBeVisible();
  });

  it("retains automatic dismissal when the notification is not interacted with", async () => {
    await setup();
    vi.useFakeTimers();
    act(() => screen.getByRole("button", { name: "Complete", exact: true }).click());
    expect(screen.getByRole("button", { name: "Dismiss notification: Operation completed." })).toBeInTheDocument();
    await act(async () => { await vi.advanceTimersByTimeAsync(5100); });
    expect(screen.queryByRole("button", { name: "Dismiss notification: Operation completed." })).not.toBeInTheDocument();
  });

  it("pauses automatic dismissal while focused and resumes after focus leaves", async () => {
    await setup();
    vi.useFakeTimers();
    act(() => screen.getByRole("button", { name: "Complete", exact: true }).click());
    act(() => screen.getByRole("button", { name: "Dismiss notification: Operation completed." }).focus());
    await act(async () => { await vi.advanceTimersByTimeAsync(6000); });
    expect(screen.getByRole("button", { name: "Dismiss notification: Operation completed." })).toHaveFocus();
    act(() => screen.getByRole("button", { name: "Complete", exact: true }).focus());
    await act(async () => { await vi.advanceTimersByTimeAsync(5100); });
    expect(screen.queryByRole("button", { name: "Dismiss notification: Operation completed." })).not.toBeInTheDocument();
  });
});
