import { MockCoreClient } from "@codex-provider-sync/core-client";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { AppUi } from "../src/App.js";
import { statusFor } from "./helpers/app-fixtures.js";
import { historyProjectPage } from "./helpers/history-fixtures.js";

describe("Host clipboard integration", () => {
  it("routes row, detail, path and Markdown copy through Host without browser access or extra reads", async () => {
    const user = userEvent.setup();
    const browserWrite = vi.spyOn(navigator.clipboard, "writeText").mockRejectedValue(new Error("denied"));
    const copyText = vi.fn(async (_text: string) => {});
    const id = "11111111-2222-4333-8444-555555555555";
    const session = { id, nativeSessionId: id, title: "Clipboard fixture", provider: "openai", archived: false, updatedAt: "2026-09-04T00:00:00Z", messageCount: 1 };
    const getHistorySession = vi.fn(async () => ({ session, messages: [{ role: "assistant" as const, text: "```js\nconst fixture = 1;\n```", sequence: 1 }], truncated: false, returnedMessageCount: 1, storage: { cwd: "C:\\synthetic", rolloutPath: "C:\\synthetic\\rollout.jsonl" } }));
    const core = new MockCoreClient({ getStatus: async () => statusFor(), listHistory: async () => historyProjectPage([session]), getHistorySession });
    try {
      render(<AppUi surface="desktop" core={core} host={{ copyText, listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }} initialLocale="en" initialTheme="system" preferences={{ getLocale: () => "en", setLocale: () => {}, getTheme: () => "system", setTheme: () => {} }} />);
      await user.click(await screen.findByRole("button", { name: "History", exact: true }));
      await screen.findByText("Clipboard fixture");
      const row = screen.getByRole("button", { name: "View chat: Clipboard fixture" });
      await user.pointer({ target: row, keys: "[MouseRight]" });
      await user.click(screen.getByRole("menuitem", { name: "Copy session ID", exact: true }));
      expect(copyText).toHaveBeenLastCalledWith(id);
      await user.pointer({ target: row, keys: "[MouseRight]" });
      await user.click(screen.getByRole("menuitem", { name: "Copy resume command" }));
      expect(copyText).toHaveBeenLastCalledWith(`codex resume ${id}`);
      expect(getHistorySession).not.toHaveBeenCalled();
      await user.pointer({ target: row, keys: "[MouseRight]" });
      await user.click(screen.getByRole("menuitem", { name: "Session information" }));
      const actions = within(await screen.findByRole("region", { name: "Session actions" }));
      await user.click(actions.getByRole("button", { name: "Copy session ID", exact: true }));
      expect(copyText).toHaveBeenLastCalledWith(id);
      await user.click(await actions.findByRole("button", { name: "Copy file path" }));
      expect(copyText).toHaveBeenLastCalledWith("C:\\synthetic\\rollout.jsonl");
      copyText.mockRejectedValue(new Error("native failure"));
      await user.click(actions.getByRole("button", { name: "Copy session ID", exact: true }));
      await waitFor(() => expect(actions.getByRole("status")).toHaveTextContent("Could not copy."));
      expect(browserWrite).not.toHaveBeenCalled();
      expect(getHistorySession).toHaveBeenCalledOnce();
      expect(getHistorySession).toHaveBeenCalledWith(expect.objectContaining({ metadataOnly: true }), expect.anything(), expect.anything());
      copyText.mockResolvedValue();
      await user.click(screen.getByRole("button", { name: "View chat", exact: true }));
      await user.click(await screen.findByRole("button", { name: "Copy", exact: true }));
      expect(copyText).toHaveBeenLastCalledWith("const fixture = 1;");
      expect(getHistorySession).toHaveBeenCalledTimes(2);
    } finally { browserWrite.mockRestore(); }
  });
});
