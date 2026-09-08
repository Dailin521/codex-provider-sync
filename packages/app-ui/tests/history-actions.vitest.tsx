import type { HistorySessionSummary } from "@codex-provider-sync/contracts";
import { createPublicCoreErrorDto } from "@codex-provider-sync/contracts";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it, vi } from "vitest";
import { CoreClientError, type CoreClient } from "@codex-provider-sync/core-client";
import { createAppI18n } from "../src/i18n.js";
import { HistoryPage } from "../src/features/history/HistoryPage.js";
import { HistorySessionActions, historyResumeCommand } from "../src/features/history/HistorySessionActions.js";
import { historyProjectPage } from "./helpers/history-fixtures.js";

const id = "11111111-2222-4333-8444-555555555555";
const profile = { id: "fixture", name: "Fixture", revision: "rev-1" };
const session: HistorySessionSummary = { id, nativeSessionId: id, parentSessionId: "parent-fixture", sessionKind: "subagent", title: "Saved session", provider: "openai", archived: false, createdAt: "2026-09-04T00:00:00Z", updatedAt: "2026-09-04T01:00:00Z", fileModifiedAt: "2026-09-04T02:00:00Z", messageCount: 0 };

describe("History session actions", () => {
  it("explains a missing parent without leaving the child body under the parent heading", async () => {
    const user = userEvent.setup();
    const listHistory = vi.fn(async () => historyProjectPage([session]));
    const getHistorySession = vi.fn(async (input: { sessionId: string }) => {
      if (input.sessionId === "parent-fixture") throw new CoreClientError(createPublicCoreErrorDto("INVALID_INPUT"));
      return { session, messages: [{ role: "user", text: "Child-only message", sequence: 1 }], truncated: false, returnedMessageCount: 1 };
    });
    const i18n = await createAppI18n("en");
    render(<I18nextProvider i18n={i18n}><QueryClientProvider client={new QueryClient()}><HistoryPage core={{ listHistory, getHistorySession } as unknown as CoreClient} profile={profile} /></QueryClientProvider></I18nextProvider>);
    await user.click(await screen.findByRole("button", { name: "View chat: Saved session" }));
    await screen.findByText("Child-only message");
    await user.pointer({ target: screen.getByRole("button", { name: "View chat: Saved session" }), keys: "[MouseRight]" });
    await user.click(screen.getByRole("menuitem", { name: "Session information" }));
    const actions = screen.getByRole("region", { name: "Session actions" });
    await user.click(within(actions).getByRole("button", { name: "View parent session" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(i18n.t("history.parentUnavailable"));
    expect(screen.queryByText("Child-only message")).not.toBeInTheDocument();
    expect(getHistorySession).toHaveBeenLastCalledWith(expect.objectContaining({ sessionId: "parent-fixture" }), expect.anything());
  });

  it("copies actual identity and a non-executing command, reveals by ID and navigates the explicit parent", async () => {
    const user = userEvent.setup();
    const clipboard = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();
    const revealHistoryFile = vi.fn(async () => ({ revealed: true }));
    const onOpenParent = vi.fn();
    const i18n = await createAppI18n("en");
    render(<I18nextProvider i18n={i18n}><HistorySessionActions session={session} detail={{ session, messages: [], truncated: false, returnedMessageCount: 0, storage: { cwd: "C:\\project", rolloutPath: "C:\\fixture\\sessions\\rollout-a.jsonl" } }} host={{ listProfiles: async () => [], revealHistoryFile }} profile={profile} onOpenParent={onOpenParent} /></I18nextProvider>);
    await user.click(screen.getByRole("button", { name: "Copy session ID" }));
    expect(clipboard).toHaveBeenLastCalledWith(id);
    await user.click(screen.getByRole("button", { name: "Copy resume command" }));
    expect(clipboard).toHaveBeenLastCalledWith(`codex resume ${id}`);
    await user.click(screen.getByText("Session information"));
    await user.click(screen.getByRole("button", { name: "Copy file path" }));
    expect(clipboard).toHaveBeenLastCalledWith("C:\\fixture\\sessions\\rollout-a.jsonl");
    await user.click(screen.getByRole("button", { name: "Show in File Explorer" }));
    expect(revealHistoryFile).toHaveBeenCalledWith({ profileId: "fixture", profileRevision: "rev-1" }, id);
    await user.click(screen.getByRole("button", { name: "View parent session" }));
    expect(onOpenParent).toHaveBeenCalledWith("parent-fixture");
    clipboard.mockRejectedValue(new Error("denied"));
    await user.click(screen.getByRole("button", { name: "Copy session ID" }));
    expect(await screen.findByText("Could not copy. Try again, or select the text and copy it manually.")).toBeVisible();
  });

  it("never builds a command from a synthetic ID or command-shaped native ID", async () => {
    expect(historyResumeCommand({ ...session, nativeSessionId: null })).toBeNull();
    expect(historyResumeCommand({ ...session, nativeSessionId: "abc;calc" })).toBeNull();
    const i18n = await createAppI18n("zh-CN");
    render(<I18nextProvider i18n={i18n}><HistorySessionActions session={{ ...session, nativeSessionId: null, id: "rollout:internal-hash" }} detail={null} profile={profile} onOpenParent={() => {}} /></I18nextProvider>);
    expect(screen.getByRole("button", { name: "复制会话 ID" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "复制继续命令" })).toBeDisabled();
    expect(screen.getByText(/内部列表标识不能用于继续会话/)).toBeVisible();
  });

  it("defaults to metadata search and all sessions; scope requires submit and kind filters explicitly", async () => {
    const user = userEvent.setup();
    const listHistory = vi.fn(async () => historyProjectPage([session]));
    const getHistorySession = vi.fn(async () => ({ session, messages: [], truncated: false, returnedMessageCount: 0 }));
    const i18n = await createAppI18n("en");
    render(<I18nextProvider i18n={i18n}><QueryClientProvider client={new QueryClient()}><HistoryPage core={{ listHistory, getHistorySession } as unknown as CoreClient} profile={profile} /></QueryClientProvider></I18nextProvider>);
    await screen.findByText("Saved session");
    expect(listHistory).toHaveBeenLastCalledWith(expect.objectContaining({ searchScope: "metadata", sessionKind: "all" }), expect.anything());
    await user.type(screen.getByRole("textbox", { name: "Search" }), "marker");
    await user.click(screen.getByText("Search options and filters"));
    await user.selectOptions(screen.getByRole("combobox", { name: "Search scope" }), "content");
    expect(listHistory).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole("button", { name: "Search", exact: true }));
    await waitFor(() => expect(listHistory).toHaveBeenLastCalledWith(expect.objectContaining({ query: "marker", searchScope: "content" }), expect.anything()));
    await user.selectOptions(screen.getByRole("combobox", { name: "Session type" }), "subagent");
    await waitFor(() => expect(listHistory).toHaveBeenLastCalledWith(expect.objectContaining({ sessionKind: "subagent" }), expect.anything()));
    expect(getHistorySession).not.toHaveBeenCalled();
    // List actions do not load the body just to copy an identity.
    await user.pointer({ target: screen.getByRole("button", { name: "View chat: Saved session" }), keys: "[MouseRight]" });
    await user.click(screen.getByRole("menuitem", { name: "Copy session ID" }));
    expect(getHistorySession).not.toHaveBeenCalled();
  });
});
