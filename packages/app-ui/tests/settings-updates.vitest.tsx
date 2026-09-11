import { MockCoreClient } from "@codex-provider-sync/core-client";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { AppUi } from "../src/App.js";
import type { HostClient, HostUpdateStatus } from "../src/types.js";
import { statusFor } from "./helpers/app-fixtures.js";

function mount(host: Partial<HostClient>) {
  render(<AppUi surface="desktop" core={new MockCoreClient({ getStatus: async () => statusFor() })} capabilities={{ watch: false }} host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }], ...host }} initialLocale="en" initialTheme="system" preferences={{ getLocale: () => "en", setLocale: () => {}, getTheme: () => "system", setTheme: () => {} }} />);
}

describe("Desktop updates in Settings", () => {
  it("ignores/restores only the displayed version and keeps explicit installer download/install usable", async () => {
    const user = userEvent.setup();
    let status: HostUpdateStatus = { currentVersion: "1.0.1", state: "available", version: "1.0.2", installAllowed: false };
    const reminder = vi.fn(async (version: string, ignored: boolean) => {
      expect(version).toBe("1.0.2");
      return status = { ...status, reminderIgnored: ignored };
    });
    const download = vi.fn(async () => status = { ...status, state: "downloaded", progressPercent: 100, installAllowed: true });
    const install = vi.fn(async () => ({ ...status, state: "installing" as const, progressPercent: undefined, installAllowed: false }));
    mount({ getUpdateStatus: async () => status, setUpdateReminder: reminder, downloadUpdate: download, installUpdate: install });
    await user.click(await screen.findByRole("button", { name: "Settings", exact: true }));
    await user.click(await screen.findByRole("button", { name: "Don't remind me about this version" }));
    expect(await screen.findByText("Reminders for this version are off. You can still update now.")).toBeVisible();
    expect(download).not.toHaveBeenCalled();
    expect(install).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Remind me about this version" }));
    await waitFor(() => expect(reminder).toHaveBeenLastCalledWith("1.0.2", false));
    await user.click(screen.getByRole("button", { name: "Download update", exact: true }));
    const installButton = await screen.findByRole("button", { name: "Restart and install" });
    expect(install).not.toHaveBeenCalled();
    await user.click(installButton);
    await waitFor(() => expect(install).toHaveBeenCalledOnce());
  });

  it("shows reminder save failure and leaves the reminder enabled", async () => {
    const user = userEvent.setup();
    mount({ getUpdateStatus: async () => ({ state: "available", version: "1.0.2", installAllowed: false }),
      setUpdateReminder: async () => { throw new Error("private userData path"); } });
    await user.click(await screen.findByRole("button", { name: "Settings", exact: true }));
    await user.click(await screen.findByRole("button", { name: "Don't remind me about this version" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Could not save your preference");
    expect(screen.queryByText(/private userData/)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Don't remind me about this version" })).toBeEnabled();
  });

  it("offers manual check and official downloads for portable builds without installing", async () => {
    const user = userEvent.setup();
    const base: HostUpdateStatus = { currentVersion: "1.0.0", mode: "manual", state: "idle", installAllowed: false };
    const check = vi.fn(async () => ({ ...base, state: "available" as const, version: "1.1.0" }));
    const download = vi.fn(async () => ({ ...base, state: "available" as const, version: "1.1.0" }));
    const install = vi.fn();
    mount({ getUpdateStatus: async () => base, checkForUpdates: check, downloadUpdate: download, installUpdate: install });
    await user.click(await screen.findByRole("button", { name: "Settings", exact: true }));
    expect(await screen.findByText("Current version: 1.0.0")).toBeVisible();
    expect(check).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Check for updates" }));
    expect(await screen.findByText("Version 1.1.0")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Open official download page" }));
    await waitFor(() => expect(download).toHaveBeenCalledOnce());
    expect(screen.queryByRole("button", { name: "Restart and install" })).not.toBeInTheDocument();
    expect(install).not.toHaveBeenCalled();
  });

  it("shows pushed download progress and request failures without polling", async () => {
    const user = userEvent.setup();
    let receive!: (value: HostUpdateStatus) => void;
    const unsubscribe = vi.fn();
    const getStatus = vi.fn(async (): Promise<HostUpdateStatus> => ({ currentVersion: "1.0.0", state: "idle", installAllowed: false }));
    const check = vi.fn(async () => { throw new Error("IPC failed"); });
    mount({ getUpdateStatus: getStatus, checkForUpdates: check, subscribeUpdateStatus: listener => { receive = listener; return unsubscribe; } });
    await user.click(await screen.findByRole("button", { name: "Settings", exact: true }));
    await user.click(await screen.findByRole("button", { name: "Check for updates" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("The update request failed");
    await act(async () => receive({ state: "downloading", version: "1.1.0", progressPercent: 57, installAllowed: false }));
    expect(await screen.findByText("57% downloaded")).toBeVisible();
    expect(getStatus).toHaveBeenCalledOnce();
    await user.click(screen.getByRole("button", { name: "Overview", exact: true }));
    expect(unsubscribe).not.toHaveBeenCalled();
    await act(async () => receive({ state: "available", version: "1.2.0", installAllowed: false }));
    await user.click(screen.getByRole("button", { name: "Settings", exact: true }));
    expect(await screen.findByText("Version 1.2.0")).toBeVisible();
  });
});
