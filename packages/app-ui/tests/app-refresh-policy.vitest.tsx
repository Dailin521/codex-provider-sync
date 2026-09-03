import { MockCoreClient } from "@codex-provider-sync/core-client";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { AppUi } from "../src/App.js";
import { statusFor } from "./helpers/app-fixtures.js";

const preferences = {
  getLocale: () => "en" as const,
  setLocale: vi.fn(),
  getTheme: () => "system" as const,
  setTheme: vi.fn()
};

describe("App refresh policy", () => {
  it("loads Status once and refreshes it only from the explicit action", async () => {
    const getStatus = vi.fn(async () => statusFor());
    const core = new MockCoreClient({ getStatus });
    const user = userEvent.setup();

    render(
      <AppUi
        core={core}
        host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }}
        initialLocale="en"
        initialTheme="system"
        preferences={preferences}
        surface="desktop"
      />
    );

    const refresh = await screen.findByRole("button", { name: "Refresh" });
    await waitFor(() => expect(getStatus).toHaveBeenCalledTimes(1));
    await user.click(refresh);
    await waitFor(() => expect(getStatus).toHaveBeenCalledTimes(2));
  });

  it("loads Watch and update state once and refreshes both from Settings", async () => {
    const getWatchStatus = vi.fn(async () => ({ schemaVersion: 1 as const, watches: [] }));
    const getUpdateStatus = vi.fn(async () => ({
      state: "disabled" as const,
      reason: "not-authorized" as const,
      installAllowed: false
    }));
    const core = new MockCoreClient({
      getStatus: async () => statusFor(),
      getWatchStatus
    });
    const user = userEvent.setup();

    render(
      <AppUi
        core={core}
        host={{
          listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }],
          getUpdateStatus
        }}
        initialLocale="en"
        initialTheme="system"
        preferences={preferences}
        surface="desktop"
      />
    );

    await user.click(await screen.findByRole("button", { name: "Settings", exact: true }));
    await waitFor(() => expect(getWatchStatus).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(getUpdateStatus).toHaveBeenCalledTimes(1));

    await user.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => expect(getWatchStatus).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(getUpdateStatus).toHaveBeenCalledTimes(2));
  });
});
