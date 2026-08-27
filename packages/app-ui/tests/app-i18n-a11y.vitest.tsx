import { MockCoreClient } from "@codex-provider-sync/core-client";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { AppUi } from "../src/App.js";
import { resourcesHaveMatchingKeys } from "../src/i18n.js";
import { statusFor } from "./helpers/app-fixtures.js";

describe("App localization and keyboard navigation", () => {
  it("supports skip navigation, keyboard routes, and an in-place locale change", async () => {
    const setLocale = vi.fn();
    const core = new MockCoreClient({ getStatus: async () => statusFor() });
    const user = userEvent.setup();

    render(
      <AppUi
        core={core}
        host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }}
        initialLocale="en"
        initialTheme="system"
        preferences={{
          getLocale: () => "en",
          setLocale,
          getTheme: () => "system",
          setTheme: vi.fn()
        }}
      />
    );

    await screen.findByRole("button", { name: "Overview", exact: true });
    await user.tab();
    const skipLink = screen.getByRole("link", { name: "Skip to content" });
    expect(skipLink).toHaveFocus();
    await user.keyboard("{Enter}");
    expect(document.getElementById("main-content")).toHaveFocus();

    const syncRoute = screen.getByRole("button", { name: "Sync", exact: true });
    syncRoute.focus();
    await user.keyboard("{Enter}");
    expect(await screen.findByRole("heading", { name: "Sync current Provider" })).toBeVisible();
    expect(syncRoute).toHaveAttribute("aria-current", "page");

    const settingsRoute = screen.getByRole("button", { name: "Settings", exact: true });
    settingsRoute.focus();
    await user.keyboard("{Enter}");
    const language = await screen.findByRole("combobox", { name: "Language" });
    await user.selectOptions(language, "zh-CN");

    expect(setLocale).toHaveBeenCalledWith("zh-CN");
    await waitFor(() => expect(document.documentElement.lang).toBe("zh-CN"));
    expect(await screen.findByRole("button", { name: "设置", exact: true })).toHaveAttribute("aria-current", "page");
    expect(resourcesHaveMatchingKeys()).toBe(true);
  });
});
