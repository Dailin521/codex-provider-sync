import { MockCoreClient } from "@codex-provider-sync/core-client";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { AppUi } from "../src/App.js";
import type { HostClient } from "../src/types.js";
import { statusFor } from "./helpers/app-fixtures.js";

function renderApp(locale: "zh-CN" | "en", openProjectHome?: HostClient["openProjectHome"], surface: "desktop" | "web" = "desktop") {
  const getStatus = vi.fn(async () => statusFor());
  const core = new MockCoreClient({ getStatus });
  render(<AppUi core={core} host={{ openProjectHome, listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }} initialLocale={locale} initialTheme="system" preferences={{ getLocale: () => locale, setLocale: () => {}, getTheme: () => "system", setTheme: () => {} }} surface={surface} />);
  return { core, getStatus };
}

describe("Desktop project home entry", () => {
  it.each([
    ["en", "Open GitHub project home", "Could not open the browser. Please try again."],
    ["zh-CN", "打开 GitHub 项目主页", "无法打开浏览器，请重试。"]
  ] as const)("opens only on an explicit keyboard action and reports native failure in %s", async (locale, label, failure) => {
    const openProjectHome = vi.fn(async () => {});
    const user = userEvent.setup();
    const { core } = renderApp(locale, openProjectHome);
    const button = await screen.findByRole("button", { name: label });
    expect(button).toHaveAttribute("title", label);
    expect(openProjectHome).not.toHaveBeenCalled();
    await waitFor(() => expect(core.requests.some((request) => request.method === "getStatus")).toBe(true));
    const before = core.requests.length;
    button.focus();
    await user.keyboard("{Enter}");
    expect(openProjectHome).toHaveBeenCalledExactlyOnceWith();
    expect(core.requests).toHaveLength(before);
    openProjectHome.mockRejectedValueOnce(new Error("native failure"));
    await user.click(button);
    await screen.findByText(failure);
    expect(button).toBeEnabled();
    expect(core.requests).toHaveLength(before);
  });

  it.each(["desktop", "web"] as const)("does not expose an unsupported action on %s", async (surface) => {
    const openProjectHome = surface === "web" ? vi.fn(async () => {}) : undefined;
    renderApp("en", openProjectHome, surface);
    await screen.findByText("Codex Provider Sync");
    expect(screen.queryByRole("button", { name: "Open GitHub project home" })).not.toBeInTheDocument();
    if (openProjectHome) expect(openProjectHome).not.toHaveBeenCalled();
  });
});
