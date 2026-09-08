import type { HistorySessionSummary } from "@codex-provider-sync/contracts";
import type { CoreClient } from "@codex-provider-sync/core-client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it, vi } from "vitest";
import { HistoryPage } from "../src/features/history/HistoryPage.js";
import { historyDisplayTitle } from "../src/features/history/history-title.js";
import { createAppI18n } from "../src/i18n.js";
import { historyProjectPage } from "./helpers/history-fixtures.js";

const base: HistorySessionSummary = {
  id: "00000000-0000-0000-0000-1234abcdef12", title: "", provider: "openai",
  archived: false, createdAt: "2026-09-03T05:55:23Z", updatedAt: "2026-09-03T06:17:57Z",
  messageCount: 0, messageCountKnown: false
};

describe("History display identities", () => {
  it.each(["en", "zh-CN"])("keeps named chats and localizes stable fallback identities in %s", async (locale) => {
    const i18n = await createAppI18n(locale);
    const t = i18n.t.bind(i18n);
    const unnamed = historyDisplayTitle(base, t, locale);
    expect(unnamed).toContain("abcdef12");
    expect(unnamed).toContain("2026");
    expect(unnamed).toContain(locale === "en" ? "Untitled chat" : "无标题会话");
    expect(historyDisplayTitle({ ...base, updatedAt: "2027-01-01T00:00:00Z" }, t, locale)).toBe(unnamed);
    expect(historyDisplayTitle({ ...base, subagentName: "partial_reason_design" }, t, locale))
      .toBe(`${locale === "en" ? "Subtask" : "子任务"} · partial_reason_design`);
    expect(historyDisplayTitle({ ...base, title: "Saved name", subagentName: "worker" }, t, locale)).toBe("Saved name");
  });

  it("uses the same labels in rows, accessible names and detail without eagerly reading messages", async () => {
    const user = userEvent.setup();
    const i18n = await createAppI18n("zh-CN");
    const sessions = [{ ...base, id: "subtask-11112222", subagentName: "partial_reason_design" }, base];
    const listHistory = vi.fn(async () => historyProjectPage(sessions));
    const getHistorySession = vi.fn(async ({ sessionId }: { sessionId: string }) => ({
      session: { ...sessions.find((s) => s.id === sessionId)!, updatedAt: "2027-01-01T00:00:00Z" },
      messages: [], truncated: false, returnedMessageCount: 0
    }));
    render(<I18nextProvider i18n={i18n}><QueryClientProvider client={new QueryClient()}>
      <HistoryPage core={{ listHistory, getHistorySession } as unknown as CoreClient} profile={{ id: "fixture", name: "Fixture", revision: "1" }} />
    </QueryClientProvider></I18nextProvider>);
    const subtask = await screen.findByRole("button", { name: /子任务 · partial_reason_design/ });
    const unnamed = screen.getByRole("button", { name: /无标题会话.*abcdef12/ });
    expect(getHistorySession).not.toHaveBeenCalled();
    await user.click(subtask);
    expect(await screen.findByRole("heading", { name: "子任务 · partial_reason_design" })).toBeVisible();
    await user.click(unnamed);
    expect(await screen.findByRole("heading", { name: historyDisplayTitle(base, i18n.t.bind(i18n), "zh-CN") })).toBeVisible();
    expect(listHistory).toHaveBeenCalledTimes(1);
  });
});
