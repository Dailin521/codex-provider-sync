import { MockCoreClient } from "@codex-provider-sync/core-client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { I18nextProvider } from "react-i18next";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { AppUi } from "../src/App.js";
import { SettingsPage } from "../src/features/settings/SettingsPage.js";
import { createAppI18n } from "../src/i18n.js";
import { READ_ONLY_APP_UI_CAPABILITIES, type AppUiProps } from "../src/types.js";
import { statusFor } from "./helpers/app-fixtures.js";

const alpha = { id: "alpha", name: "Alpha", revision: "alpha-r1" };
const beta = { id: "beta", name: "Beta", revision: "beta-r1" };

describe("Settings Watch profile scope", () => {
  it("does not show another profile's failed action after changing profile", async () => {
    const i18n = await createAppI18n("en");
    const user = userEvent.setup();
    const core = new MockCoreClient({ getWatchStatus: async () => ({ schemaVersion: 1, watches: [] }), startWatch: async () => { throw { code: "SQLITE_BUSY" }; } });
    const props: AppUiProps = { surface: "desktop", core, host: { listProfiles: async () => [alpha, beta] }, initialLocale: "en", initialTheme: "system", preferences: { getLocale: () => "en", setLocale() {}, getTheme: () => "system", setTheme() {} } };
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const view = (profile: typeof alpha) => <I18nextProvider i18n={i18n}><QueryClientProvider client={client}><SettingsPage profile={profile} props={props} capabilities={{ ...READ_ONLY_APP_UI_CAPABILITIES, watch: true }} recoveryBlocked={false} writeBlocked={false} /></QueryClientProvider></I18nextProvider>;
    const { rerender } = render(view(alpha));
    const enable = await screen.findByRole("button", { name: "Enable automatic sync" });
    await waitFor(() => expect(enable).toBeEnabled());
    await user.click(enable);
    expect(await screen.findByRole("alert")).toBeVisible();
    rerender(view(beta));
    await waitFor(() => expect(screen.getByRole("button", { name: "Enable automatic sync" })).toBeEnabled());
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("aborts the previous profile's pending status read when selecting another", async () => {
    const i18n = await createAppI18n("en");
    let alphaSignal: AbortSignal | undefined;
    const core = new MockCoreClient({});
    vi.spyOn(core, "getWatchStatus").mockImplementation(async ({ profile }, options) => {
      if (profile?.profileId === "alpha") { alphaSignal = options?.signal; return new Promise(() => {}); }
      return { schemaVersion: 1, watches: [] };
    });
    const props: AppUiProps = { surface: "desktop", core, host: { listProfiles: async () => [alpha, beta] }, initialLocale: "en", initialTheme: "system", preferences: { getLocale: () => "en", setLocale() {}, getTheme: () => "system", setTheme() {} } };
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const view = (profile: typeof alpha) => <I18nextProvider i18n={i18n}><QueryClientProvider client={client}><SettingsPage profile={profile} props={props} capabilities={{ ...READ_ONLY_APP_UI_CAPABILITIES, watch: true }} recoveryBlocked={false} writeBlocked={false} /></QueryClientProvider></I18nextProvider>;
    const { rerender } = render(view(alpha));
    await waitFor(() => expect(alphaSignal).toBeDefined());
    rerender(view(beta));
    await waitFor(() => expect(alphaSignal?.aborted).toBe(true));
    await waitFor(() => expect(screen.getByRole("button", { name: "Enable automatic sync" })).toBeEnabled());
  });
  it("shows only the selected profile Watch and stops that Watch ID", async () => {
    const getWatchStatus = vi.fn(async (input) => input.profile.profileId === "alpha"
      ? {
          schemaVersion: 1 as const,
          watchId: "watch-alpha",
          status: "running" as const,
          startedAt: "2026-09-05T00:00:00.000Z",
          stoppedAt: null,
          stopReason: null,
          includeStateDb: true,
          once: false
        }
      : { schemaVersion: 1 as const, watches: [] });
    const stopWatch = vi.fn(async ({ watchId }) => ({
      schemaVersion: 1 as const,
      watchId,
      status: "stopped" as const,
      startedAt: "2026-09-05T00:00:00.000Z",
      stoppedAt: "2026-09-05T00:01:00.000Z",
      stopReason: "manual",
      includeStateDb: true,
      once: false
    }));
    const user = userEvent.setup();
    render(
      <AppUi
        core={new MockCoreClient({ getStatus: async () => statusFor(), getWatchStatus, stopWatch })}
        host={{ listProfiles: async () => [alpha, beta] }}
        initialLocale="en"
        initialTheme="system"
        preferences={{ getLocale: () => "en", setLocale: () => {}, getTheme: () => "system", setTheme: () => {} }}
        surface="desktop"
      />
    );

    await user.click(await screen.findByRole("button", { name: "Settings", exact: true }));
    await waitFor(() => expect(getWatchStatus.mock.calls.at(-1)?.[0]).toEqual({
      profile: { profileId: "alpha", profileRevision: "alpha-r1" }
    }));
    expect(await screen.findByRole("button", { name: "Disable automatic sync" })).toBeVisible();

    await user.selectOptions(screen.getByLabelText("Profile"), "beta");
    await waitFor(() => expect(getWatchStatus.mock.calls.at(-1)?.[0]).toEqual({
      profile: { profileId: "beta", profileRevision: "beta-r1" }
    }));
    expect(screen.getByRole("button", { name: "Enable automatic sync" })).toBeVisible();
    expect(screen.queryByRole("button", { name: "Disable automatic sync" })).not.toBeInTheDocument();

    await user.selectOptions(screen.getByLabelText("Profile"), "alpha");
    await user.click(await screen.findByRole("button", { name: "Disable automatic sync" }));
    await waitFor(() => expect(stopWatch.mock.calls.at(-1)?.[0]).toEqual({ watchId: "watch-alpha" }));
  });

  it("settles a running Watch from the terminal host event without another status read", async () => {
    const i18n = await createAppI18n("en");
    const user = userEvent.setup();
    let stoppedListener: ((event: { generation: number; profileId: string; profileRevision: string; watch: {
      schemaVersion: 1; watchId: string; status: "stopped"; startedAt: string; stoppedAt: string; stopReason: string; includeStateDb: boolean; once: boolean;
    } }) => void) | undefined;
    const getWatchStatus = vi.fn(async () => ({
      schemaVersion: 1 as const,
      watchId: "watch-alpha",
      status: "running" as const,
      startedAt: "2026-09-05T00:00:00.000Z",
      stoppedAt: null,
      stopReason: null,
      includeStateDb: true,
      once: false
    }));
    render(
      <AppUi
        core={new MockCoreClient({ getStatus: async () => statusFor(), getWatchStatus })}
        host={{
          listProfiles: async () => [alpha],
          subscribeWatchStopped(listener) { stoppedListener = listener; return () => { stoppedListener = undefined; }; }
        }}
        initialLocale="en"
        initialTheme="system"
        preferences={{ getLocale: () => "en", setLocale: () => {}, getTheme: () => "system", setTheme: () => {} }}
        surface="desktop"
      />
    );
    await user.click(await screen.findByRole("button", { name: "Settings", exact: true }));
    expect(await screen.findByRole("button", { name: "Disable automatic sync" })).toBeVisible();
    const readsBefore = getWatchStatus.mock.calls.length;
    stoppedListener?.({
      generation: 1,
      profileId: alpha.id,
      profileRevision: alpha.revision,
      watch: {
        schemaVersion: 1,
        watchId: "watch-alpha",
        status: "stopped",
        startedAt: "2026-09-05T00:00:00.000Z",
        stoppedAt: "2026-09-05T00:01:00.000Z",
        stopReason: "recovery-required",
        includeStateDb: true,
        once: false
      }
    });
    expect(await screen.findByRole("button", { name: "Enable automatic sync" })).toBeVisible();
    expect(getWatchStatus.mock.calls.length).toBe(readsBefore);
  });
});
