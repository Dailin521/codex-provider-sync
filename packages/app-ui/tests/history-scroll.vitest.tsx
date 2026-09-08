import { MockCoreClient } from "@codex-provider-sync/core-client";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it, vi } from "vitest";
import { AppUi } from "../src/App.js";
import { statusFor } from "./helpers/app-fixtures.js";
import { historyProjectPage } from "./helpers/history-fixtures.js";

it("bounds History to the viewport, isolates scroll regions and resets only the selected detail", async () => {
  const user = userEvent.setup();
  const sessions = ["First", "Second"].map((title) => ({ id: title, title, provider: "openai", archived: false, updatedAt: "2026-09-04T00:00:00Z", messageCount: 1 }));
  const listHistory = vi.fn(async () => historyProjectPage(sessions));
  const getHistorySession = vi.fn(async ({ sessionId }: { sessionId: string }) => ({ session: sessions.find((entry) => entry.id === sessionId)!, messages: [{ role: "user" as const, text: "Synthetic message", sequence: 1 }], returnedMessageCount: 1, truncated: false }));
  const core = new MockCoreClient({ getStatus: async () => statusFor(), listHistory, getHistorySession });
  const focus = vi.spyOn(HTMLElement.prototype, "focus");
  try {
    render(<AppUi core={core} host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }} surface="desktop" initialLocale="en" initialTheme="system" preferences={{ getLocale: () => "en", setLocale: () => {}, getTheme: () => "system", setTheme: () => {} }} />);
    await user.click(await screen.findByRole("button", { name: "History", exact: true }));
    const main = screen.getByRole("main");
    expect(main).toHaveClass("min-h-0", "overflow-hidden");
    const list = await screen.findByRole("region", { name: "Chat list" });
    expect(list).toHaveClass("overflow-y-auto", "overscroll-contain");
    expect(list).toHaveAttribute("tabindex", "0");
    await user.click(await screen.findByRole("button", { name: "View chat: First" }));
    await screen.findByText("Synthetic message");
    const detail = screen.getByRole("region", { name: "Chat details and messages" });
    expect(detail).toHaveClass("overflow-y-auto", "overscroll-contain");
    expect(detail).toHaveAttribute("tabindex", "0");
    expect(focus).toHaveBeenCalledWith({ preventScroll: true });
    list.scrollTop = 1200;
    detail.scrollTop = 480;
    await user.click(screen.getByRole("button", { name: "View chat: Second" }));
    await waitFor(() => expect(getHistorySession).toHaveBeenCalledTimes(2));
    expect(detail.scrollTop).toBe(0);
    expect(list.scrollTop).toBe(1200);
    expect(listHistory).toHaveBeenCalledOnce();
    await user.click(screen.getByRole("button", { name: "Overview", exact: true }));
    expect(main).not.toHaveClass("overflow-hidden");
  } finally { focus.mockRestore(); }
});
