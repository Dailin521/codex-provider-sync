import type { HistoryPage as HistoryPageDto, HistorySessionDetail } from "@codex-provider-sync/contracts";
import type { CoreClient } from "@codex-provider-sync/core-client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it, vi } from "vitest";

import { HistoryPage, HISTORY_PAGE_SIZE } from "../src/features/history/HistoryPage.js";
import { createAppI18n } from "../src/i18n.js";

const profile = { id: "fixture", name: "Fixture", revision: "rev-1" };

function page(pageNumber: number): HistoryPageDto {
  return {
    page: pageNumber,
    pageSize: HISTORY_PAGE_SIZE,
    total: 51,
    hasNextPage: pageNumber === 1,
    sessions: [{
      id: `session-${pageNumber}`,
      title: `Session ${pageNumber}`,
      provider: "fixture-provider",
      archived: false,
      updatedAt: "2026-08-27T00:00:00.000Z",
      messageCount: 1
    }]
  };
}

function detail(): HistorySessionDetail {
  return {
    session: page(2).sessions[0],
    messages: [{
      role: "user",
      text: "history-body-marker",
      timestamp: "2026-08-27T00:00:00.000Z",
      sequence: 1
    }],
    truncated: false,
    returnedMessageCount: 1
  };
}

async function renderHistory() {
  const listHistory = vi.fn(async (input: { page: number }) => page(input.page));
  const getHistorySession = vi.fn(async () => detail());
  const core = { listHistory, getHistorySession } as unknown as CoreClient;
  const i18n = await createAppI18n("en");
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } }
  });
  render(
    <I18nextProvider i18n={i18n}>
      <QueryClientProvider client={queryClient}>
        <HistoryPage core={core} profile={profile} />
      </QueryClientProvider>
    </I18nextProvider>
  );
  return { getHistorySession, listHistory, queryClient };
}

describe("HistoryPage privacy and pagination", () => {
  it("paginates summaries and only reads a body after explicit open", async () => {
    const user = userEvent.setup();
    const { getHistorySession, listHistory, queryClient } = await renderHistory();

    expect(await screen.findByText("Session 1")).toBeVisible();
    expect(getHistorySession).not.toHaveBeenCalled();
    expect(listHistory).toHaveBeenLastCalledWith(
      expect.objectContaining({ page: 1, pageSize: HISTORY_PAGE_SIZE }),
      expect.objectContaining({ signal: expect.any(AbortSignal) })
    );

    await user.click(screen.getByRole("button", { name: "Next" }));
    expect(await screen.findByText("Session 2")).toBeVisible();
    expect(listHistory).toHaveBeenLastCalledWith(
      expect.objectContaining({ page: 2, pageSize: HISTORY_PAGE_SIZE }),
      expect.objectContaining({ signal: expect.any(AbortSignal) })
    );
    expect(getHistorySession).not.toHaveBeenCalled();

    const open = screen.getByRole("button", { name: "Open session" });
    await user.click(open);
    expect(await screen.findByText("history-body-marker")).toBeVisible();
    expect(screen.getByRole("heading", { name: "Session 2" })).toHaveFocus();
    expect(getHistorySession).toHaveBeenCalledWith(
      expect.objectContaining({ sessionId: "session-2", messageLimit: 200 }),
      expect.objectContaining({ signal: expect.any(AbortSignal) })
    );
    await waitFor(() => {
      expect(JSON.stringify(queryClient.getQueryCache().getAll().map((query) => query.state.data)))
        .not.toContain("history-body-marker");
    });
    await user.click(screen.getByRole("button", { name: "Back to sessions" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Open session" })).toHaveFocus());
  });
});
