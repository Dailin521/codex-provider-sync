import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it, vi } from "vitest";
import type { CoreClient } from "@codex-provider-sync/core-client";
import type { HistorySessionSummary, ListHistoryInput } from "@codex-provider-sync/contracts";
import { HistoryPage } from "../src/features/history/HistoryPage.js";
import { createAppI18n } from "../src/i18n.js";

async function setup(locale: "en" | "zh-CN" = "en") {
  const sessions: HistorySessionSummary[] = Array.from({ length: 27 }, (_, index) => ({
    id: `chat-${index}`, nativeSessionId: `11111111-2222-4333-8444-55555555555${index}`,
    title: `Chat ${index}`, provider: "openai", archived: false, updatedAt: "2026-09-04T00:00:00Z", messageCount: 0, messageCountKnown: false,
    project: { id: (index < 12 || index >= 24 ? "a" : "b").repeat(64), name: "Same name" },
    sessionKind: index >= 24 ? "subagent" : "main",
    childCount: index === 0 || index === 24 ? 1 : 0,
    ...(index === 24 ? { parentSessionId: "chat-0" } : index === 26 ? { parentSessionId: "chat-24" } : {})
  }));
  const projects = [{ id: "a".repeat(64), name: "Same name", kind: "workspace" as const, total: 12 }, { id: "b".repeat(64), name: "Same name", kind: "directory" as const, total: 12 }, { id: "orphans", name: "Orphans", kind: "orphans" as const, total: 1 }];
  const listHistory = vi.fn(async (input: ListHistoryInput) => {
    const projectId = input.projectId || "a".repeat(64);
    const roots = input.parentId ? sessions.filter((session) => session.parentSessionId === input.parentId) : projectId === "orphans" ? [sessions[25]] : sessions.filter((session) => session.project?.id === projectId && session.sessionKind === "main");
    const page = input.page || 1;
    const start = (page - 1) * 10;
    return { view: "projects" as const, projects, projectId, page, pageSize: 10, total: roots.length, hasNextPage: start + 10 < roots.length, sessions: roots.slice(start, start + 10) };
  });
  const getHistorySession = vi.fn(async (input: { sessionId: string }) => ({ session: sessions.find((item) => item.id === input.sessionId)!, messages: [{ role: "user", text: "Explicit body", sequence: 1 }], truncated: false, returnedMessageCount: 1 }));
  const core = { listHistory, getHistorySession } as unknown as CoreClient;
  const i18n = await createAppI18n(locale);
  const aliases = new Map<string, string>();
  const setAlias = vi.fn((scope: string, id: string, value: string) => { aliases.set(`${scope}:${id}`, value); });
  const preferences = { getLocale: () => locale, setLocale: () => {}, getTheme: () => "system" as const, setTheme: () => {}, getHistoryProjectAlias: (scope: string, id: string) => aliases.get(`${scope}:${id}`) || null, setHistoryProjectAlias: setAlias };
  const rendered = render(<I18nextProvider i18n={i18n}><QueryClientProvider client={new QueryClient()}><HistoryPage core={core} profile={{ id: "default", revision: "r1", name: "Default" }} preferences={preferences} /></QueryClientProvider></I18nextProvider>);
  await screen.findByText("Chat 0");
  return { listHistory, getHistorySession, i18n, setAlias, ...rendered };
}

describe("History project list and context menus", () => {
  it.each(["en", "zh-CN"] as const)("pages within each project without mixing children or other projects in %s", async (locale) => {
    const user = userEvent.setup();
    const { listHistory, getHistorySession, i18n } = await setup(locale);
    const list = screen.getByRole("region", { name: i18n.t("history.listRegion") });
    expect(within(list).getAllByRole("region", { name: /Same name/ })).toHaveLength(2);
    const orphan = within(list).getByRole("region", { name: i18n.t("history.orphans") });
    expect(within(orphan).getByRole("button", { expanded: false })).toBeVisible();
    expect(screen.queryByText("Chat 24")).not.toBeInTheDocument();
    expect(screen.queryByText("Chat 10")).not.toBeInTheDocument();
    expect(screen.queryByText(/More actions|更多操作/)).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: i18n.t("history.loadMore") }));
    expect(await screen.findByText("Chat 10")).toBeVisible();
    expect(listHistory).toHaveBeenLastCalledWith(expect.objectContaining({ projectId: "a".repeat(64), page: 2 }), expect.anything());
    const firstGroup = within(list).getAllByRole("region", { name: /Same name/ })[0];
    const toggle = within(firstGroup).getByRole("button", { name: /Same name/ });
    await user.click(toggle);
    expect(screen.queryByText("Chat 0")).not.toBeInTheDocument();
    await user.click(toggle);
    expect(screen.getByText("Chat 0")).toBeVisible();
    expect(listHistory).toHaveBeenCalledTimes(2);
    const secondGroup = within(list).getAllByRole("region", { name: /Same name/ })[1];
    await user.click(within(secondGroup).getByRole("button", { expanded: false }));
    expect(await screen.findByText("Chat 12")).toBeVisible();
    expect(listHistory).toHaveBeenLastCalledWith(expect.objectContaining({ projectId: "b".repeat(64), page: 1 }), expect.anything());
    expect(screen.getByText("Chat 10")).toBeVisible();
    expect(getHistorySession).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: i18n.t("common.refresh"), exact: true }));
    await waitFor(() => expect(listHistory).toHaveBeenCalledTimes(4));
    await waitFor(() => expect(screen.queryByText("Chat 10")).not.toBeInTheDocument());
  });

  it("supports keyboard focus, Escape, outside dismissal and non-reading copy", async () => {
    const user = userEvent.setup();
    const clipboard = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();
    const { getHistorySession } = await setup();
    const row = screen.getByRole("button", { name: "View chat: Chat 0" });
    row.focus();
    await user.keyboard("{Shift>}{F10}{/Shift}");
    expect(screen.getByRole("menuitem", { name: "View chat", exact: true })).toHaveFocus();
    await user.keyboard("{ArrowDown}");
    expect(screen.getByRole("menuitem", { name: "Copy session ID" })).toHaveFocus();
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
    expect(row).toHaveFocus();
    fireEvent.keyDown(row, { key: "ContextMenu" });
    await user.click(screen.getByRole("menuitem", { name: "Copy session ID" }));
    expect(clipboard).toHaveBeenCalledWith("11111111-2222-4333-8444-555555555550");
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
    expect(getHistorySession).not.toHaveBeenCalled();
    await user.pointer({ target: row, keys: "[MouseRight]" });
    await user.click(screen.getByRole("textbox", { name: "Search" }));
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
  });

  it("uses the right-clicked identity without changing the open chat and reports copy failure", async () => {
    const user = userEvent.setup();
    vi.spyOn(navigator.clipboard, "writeText").mockRejectedValue(new Error("denied"));
    const { getHistorySession } = await setup();
    await user.click(screen.getByRole("button", { name: "View chat: Chat 0" }));
    await screen.findByText("Explicit body");
    await user.pointer({ target: screen.getByRole("button", { name: "View chat: Chat 1" }), keys: "[MouseRight]" });
    await user.click(screen.getByRole("menuitem", { name: "Copy session ID" }));
    expect(await screen.findByText(/Could not copy/)).toBeVisible();
    expect(screen.getByRole("heading", { name: "Chat 0" })).toBeVisible();
    expect(getHistorySession).toHaveBeenCalledOnce();
  });

  it("opens information without showing or requesting messages, then explicitly loads the chat", async () => {
    const user = userEvent.setup();
    const { getHistorySession } = await setup();
    await user.pointer({ target: screen.getByRole("button", { name: "View chat: Chat 0" }), keys: "[MouseRight]" });
    await user.click(screen.getByRole("menuitem", { name: "Session information" }));
    await screen.findByRole("heading", { name: "Chat 0" });
    expect(getHistorySession).toHaveBeenLastCalledWith(expect.objectContaining({ metadataOnly: true }), expect.anything());
    expect(screen.queryByText("Explicit body")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "View chat", exact: true }));
    await screen.findByText("Explicit body");
    expect(getHistorySession).toHaveBeenLastCalledWith(expect.objectContaining({ messageLimit: 200 }), expect.anything());
  });

  it("returns focus to the project when the selected chat's group was collapsed", async () => {
    const user = userEvent.setup();
    await setup();
    await user.click(screen.getByRole("button", { name: "View chat: Chat 0" }));
    await screen.findByText("Explicit body");
    const group = within(screen.getAllByRole("region", { name: /Same name/ })[0]).getByRole("button", { name: /Same name/ });
    await user.click(group);
    await user.click(screen.getByRole("button", { name: "Back to chats" }));
    await waitFor(() => expect(group).toHaveFocus());
  });

  it("loads nested children and unlinked subtasks only when expanded, never their bodies", async () => {
    const user = userEvent.setup();
    const { listHistory, getHistorySession } = await setup();
    expect(screen.queryByText("Chat 24")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Subtasks of Chat 0 (1)" }));
    expect(await screen.findByText("Chat 24")).toBeVisible();
    expect(listHistory).toHaveBeenLastCalledWith(expect.objectContaining({ parentId: "chat-0", view: "projects" }), expect.anything());
    expect(screen.queryByText("Chat 26")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Subtasks of Chat 24 (1)" }));
    expect(await screen.findByText("Chat 26")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Subtasks of Chat 24 (1)" }));
    await user.click(screen.getByRole("button", { name: "Subtasks of Chat 24 (1)" }));
    expect(listHistory).toHaveBeenCalledTimes(3);
    await user.click(within(screen.getByRole("region", { name: "Unlinked subtasks" })).getByRole("button", { expanded: false }));
    expect(await screen.findByText("Chat 25")).toBeVisible();
    expect(getHistorySession).not.toHaveBeenCalled();
  });

  it("saves and resets only the project display alias through profile-scoped preferences", async () => {
    const user = userEvent.setup();
    const { listHistory, getHistorySession, setAlias } = await setup();
    const header = within(screen.getAllByRole("region", { name: /Same name/ })[0]).getByRole("button", { name: /Same name/ });
    header.focus();
    await user.keyboard("{Shift>}{F10}{/Shift}");
    await user.click(screen.getByRole("menuitem", { name: "Set project display name" }));
    const input = screen.getByRole("textbox", { name: "Display name" });
    await user.clear(input);
    await user.type(input, "My project");
    await user.click(screen.getByRole("button", { name: "Save", exact: true }));
    expect(screen.getByRole("region", { name: "My project" })).toBeVisible();
    expect(setAlias).toHaveBeenCalledWith(JSON.stringify(["default", "r1"]), "a".repeat(64), "My project");
    await user.click(screen.getByRole("button", { name: "Refresh", exact: true }));
    await waitFor(() => expect(listHistory).toHaveBeenCalledTimes(2));
    expect(screen.getByRole("region", { name: "My project" })).toBeVisible();
    const renamed = within(screen.getByRole("region", { name: "My project" })).getByRole("button", { name: /My project/ });
    await user.pointer({ target: renamed, keys: "[MouseRight]" });
    await user.click(screen.getByRole("menuitem", { name: "Use original name" }));
    expect(screen.queryByRole("region", { name: "My project" })).not.toBeInTheDocument();
    expect(getHistorySession).not.toHaveBeenCalled();
    expect(JSON.stringify(listHistory.mock.calls)).not.toContain("My project");
  });
});
