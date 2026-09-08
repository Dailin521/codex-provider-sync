import type { StatusSnapshot } from "@codex-provider-sync/contracts";
import { MockCoreClient } from "@codex-provider-sync/core-client";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it, vi } from "vitest";

import { AppUi } from "../src/App.js";
import { SwitchPage } from "../src/features/switch-provider/SwitchPage.js";
import { createAppI18n } from "../src/i18n.js";
import { FULL_APP_UI_CAPABILITIES } from "../src/types.js";
import { statusFor } from "./helpers/app-fixtures.js";

const preferences = { getLocale: () => "en" as const, setLocale() {}, getTheme: () => "system" as const, setTheme() {} };
const defaultProfile = { id: "default", name: "Default", revision: "profile-r1" };

describe("Switch Provider defaults follow the current profile snapshot", () => {
  it("updates the mounted Overview after initial Status and manual refresh without extra reads", async () => {
    let resolveStatus!: (value: StatusSnapshot) => void;
    const pendingStatus = new Promise<StatusSnapshot>((resolve) => { resolveStatus = resolve; });
    let snapshot = { ...statusFor(), currentProvider: "dal", configuredProviders: ["openai", "dal", "relay"] };
    const getStatus = vi.fn().mockReturnValueOnce(pendingStatus).mockImplementation(async () => snapshot);
    const prepareSwitch = vi.fn();
    const user = userEvent.setup();
    render(<AppUi surface="desktop" core={new MockCoreClient({ getStatus, prepareSwitch })} host={{ listProfiles: async () => [defaultProfile] }} preferences={preferences} initialLocale="en" initialTheme="system" />);

    expect(await screen.findByLabelText("Provider ID")).toHaveValue("openai");
    expect(screen.getByRole("button", { name: "Preview switch" })).toBeDisabled();
    await act(async () => resolveStatus(snapshot));
    await waitFor(() => expect(screen.getByLabelText("Provider ID")).toHaveValue("dal"));
    expect(getStatus).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "Preview switch" })).toBeEnabled();

    snapshot = { ...snapshot, currentProvider: "relay", storageRevision: "storage-r2" };
    await user.click(screen.getByRole("button", { name: "Refresh", exact: true }));
    await waitFor(() => expect(screen.getByLabelText("Provider ID")).toHaveValue("relay"));
    expect(getStatus).toHaveBeenCalledTimes(2);
    expect(prepareSwitch).not.toHaveBeenCalled();
  });

  it("uses the current Provider, includes it among suggestions, and renames only the card", async () => {
    const i18n = await createAppI18n("zh-CN");
    const prepare = vi.fn(async () => {});
    const user = userEvent.setup();
    const { container } = render(<I18nextProvider i18n={i18n}><SwitchPage currentProvider="dal" providers={["openai", "relay", "openai"]} disabled={false} prepare={prepare} embedded /></I18nextProvider>);
    expect(screen.getByRole("heading", { name: "单独切换 Provider" })).toBeVisible();
    expect(screen.getByText(/再执行同样的 Provider 同步/)).toBeVisible();
    expect(screen.getByLabelText("Provider ID")).toHaveValue("dal");
    expect(Array.from(container.querySelectorAll("datalist option"), (option) => option.getAttribute("value"))).toEqual(["openai", "relay", "dal"]);
    await user.click(screen.getByRole("button", { name: "预览切换" }));
    await waitFor(() => expect(prepare).toHaveBeenCalledWith({ provider: "dal", modelMode: "provider-default", model: "" }, expect.anything()));
  });

  it("fills a recent successful Provider only after a click, without preparing or changing the model mode", async () => {
    const i18n = await createAppI18n("en");
    const prepare = vi.fn(async () => {});
    const user = userEvent.setup();
    render(<I18nextProvider i18n={i18n}><SwitchPage currentProvider="dal" providers={["openai", "dal", "relay"]} recentSuccessfulProviders={["relay", "openai"]} disabled={false} prepare={prepare} /></I18nextProvider>);
    expect(screen.getByLabelText("Provider ID")).toHaveValue("dal");
    await user.click(screen.getByRole("button", { name: "relay" }));
    expect(screen.getByLabelText("Provider ID")).toHaveValue("relay");
    expect(screen.getByLabelText("Model handling")).toHaveValue("provider-default");
    expect(prepare).not.toHaveBeenCalled();
  });

  it("uses only the current profile and revision from the latest 100 completed Switch logs, then refreshes them manually", async () => {
    const recentLogs = vi.fn(async () => ({ schemaVersion: 1 as const, page: 1, pageSize: 100, total: 4, hasNextPage: false, entries: [
      { schemaVersion: 1 as const, id: "ok", operation: "switch", profileId: "default", profileRevision: "profile-r1", startedAt: "2026-09-01T00:00:00.000Z", activeDurationMs: 0, status: "completed" as const, outcome: "completed", requestIds: [], switchPlan: { previousProvider: "openai", targetProvider: "relay", previousRootModel: null, targetRootModel: null, modelMode: "keep-root-model" as const }, counts: {}, warnings: [], stages: [] },
      { schemaVersion: 1 as const, id: "partial", operation: "switch", profileId: "default", profileRevision: "profile-r1", startedAt: "2026-09-01T00:00:00.000Z", activeDurationMs: 0, status: "partial" as const, outcome: "partial", requestIds: [], switchPlan: { previousProvider: "openai", targetProvider: "openai", previousRootModel: null, targetRootModel: null, modelMode: "keep-root-model" as const }, counts: {}, warnings: [], stages: [] },
      { schemaVersion: 1 as const, id: "old-revision", operation: "switch", profileId: "default", profileRevision: "old", startedAt: "2026-09-01T00:00:00.000Z", activeDurationMs: 0, status: "completed" as const, outcome: "completed", requestIds: [], switchPlan: { previousProvider: "openai", targetProvider: "openai", previousRootModel: null, targetRootModel: null, modelMode: "keep-root-model" as const }, counts: {}, warnings: [], stages: [] },
      { schemaVersion: 1 as const, id: "undefined", operation: "switch", profileId: "default", profileRevision: "profile-r1", startedAt: "2026-09-01T00:00:00.000Z", activeDurationMs: 0, status: "completed" as const, outcome: "completed", requestIds: [], switchPlan: { previousProvider: "openai", targetProvider: "removed", previousRootModel: null, targetRootModel: null, modelMode: "keep-root-model" as const }, counts: {}, warnings: [], stages: [] }
    ] }));
    const user = userEvent.setup();
    render(<AppUi surface="desktop" capabilities={{ ...FULL_APP_UI_CAPABILITIES, operationLogs: true }} core={new MockCoreClient({ getStatus: async () => statusFor() })} host={{ listProfiles: async () => [defaultProfile], listOperationLogs: recentLogs }} preferences={preferences} initialLocale="en" initialTheme="system" />);
    expect(await screen.findByRole("button", { name: "relay" })).toBeVisible();
    expect(screen.queryByRole("button", { name: "removed" })).not.toBeInTheDocument();
    expect(recentLogs).toHaveBeenCalledWith(expect.objectContaining({ pageSize: 100, profileId: "default", profileRevision: "profile-r1", operation: "switch", status: "completed" }), expect.anything());
    await user.click(screen.getByRole("button", { name: "Refresh", exact: true }));
    await waitFor(() => expect(recentLogs).toHaveBeenCalledTimes(2));
  });

  it("preserves an edited target, then follows Status again after that target becomes current", async () => {
    const i18n = await createAppI18n("en");
    const prepare = vi.fn(async () => {});
    const user = userEvent.setup();
    const view = (currentProvider: string) => <I18nextProvider i18n={i18n}><SwitchPage currentProvider={currentProvider} providers={["openai", "dal", "relay"]} disabled={false} prepare={prepare} /></I18nextProvider>;
    const { rerender } = render(view("dal"));
    await user.clear(screen.getByLabelText("Provider ID"));
    await user.type(screen.getByLabelText("Provider ID"), "relay");
    rerender(view("openai"));
    expect(screen.getByLabelText("Provider ID")).toHaveValue("relay");
    await user.click(screen.getByRole("button", { name: "Preview switch" }));
    await waitFor(() => expect(prepare).toHaveBeenCalledWith(expect.objectContaining({ provider: "relay" }), expect.anything()));
    rerender(view("relay"));
    rerender(view("dal"));
    expect(screen.getByLabelText("Provider ID")).toHaveValue("dal");
  });

  it("does not replace the last target on failed Status, and model edits do not freeze Provider defaults", async () => {
    const i18n = await createAppI18n("en");
    const user = userEvent.setup();
    const view = (currentProvider?: string) => <I18nextProvider i18n={i18n}><SwitchPage currentProvider={currentProvider} providers={["openai", "dal", "relay"]} disabled={!currentProvider} prepare={async () => {}} /></I18nextProvider>;
    const { rerender } = render(view("dal"));
    await user.selectOptions(screen.getByLabelText("Model handling"), "explicit");
    await user.type(screen.getByLabelText("Model name"), "fixture-model");
    rerender(view());
    expect(screen.getByLabelText("Provider ID")).toHaveValue("dal");
    expect(screen.getByRole("button", { name: "Preview switch" })).toBeDisabled();
    rerender(view("relay"));
    expect(screen.getByLabelText("Provider ID")).toHaveValue("relay");
    expect(screen.getByLabelText("Model name")).toHaveValue("fixture-model");
  });

  it("resets Provider and model drafts when switching storage profiles", async () => {
    const otherProfile = { id: "other", name: "Other", revision: "other-r1" };
    const getStatus = vi.fn(async (input) => {
      const profile = input.profile.profileId === "other" ? otherProfile : defaultProfile;
      return { ...statusFor(profile.revision), profile: { id: profile.id, revision: profile.revision }, currentProvider: profile.id === "other" ? "relay" : "dal", configuredProviders: ["openai", "dal", "relay"] };
    });
    const user = userEvent.setup();
    render(<AppUi surface="desktop" core={new MockCoreClient({ getStatus })} host={{ listProfiles: async () => [defaultProfile, otherProfile] }} preferences={preferences} initialLocale="en" initialTheme="system" />);
    await waitFor(() => expect(screen.getByLabelText("Provider ID")).toHaveValue("dal"));
    await user.clear(screen.getByLabelText("Provider ID"));
    await user.type(screen.getByLabelText("Provider ID"), "openai");
    await user.selectOptions(screen.getByLabelText("Model handling"), "explicit");
    await user.type(screen.getByLabelText("Model name"), "profile-a-model");
    await user.selectOptions(screen.getByLabelText("Profile"), "other");
    await waitFor(() => expect(screen.getByLabelText("Provider ID")).toHaveValue("relay"));
    expect(screen.getByLabelText("Model handling")).toHaveValue("provider-default");
    expect(screen.queryByLabelText("Model name")).not.toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Model handling"), "explicit");
    expect(screen.getByLabelText("Model name")).toHaveValue("");
    await user.selectOptions(screen.getByLabelText("Profile"), "default");
    await waitFor(() => expect(screen.getByLabelText("Provider ID")).toHaveValue("dal"));
    expect(screen.getByLabelText("Model handling")).toHaveValue("provider-default");
  });

  it("resets drafts on a revision change without replacing the confirmation focus target", async () => {
    const i18n = await createAppI18n("en");
    const user = userEvent.setup();
    const view = (profileKey: string) => <I18nextProvider i18n={i18n}><SwitchPage profileKey={profileKey} currentProvider="dal" providers={["openai", "dal"]} disabled={false} prepare={async () => {}} /></I18nextProvider>;
    const { rerender } = render(view("default:r1"));
    await user.clear(screen.getByLabelText("Provider ID"));
    await user.type(screen.getByLabelText("Provider ID"), "openai");
    await user.selectOptions(screen.getByLabelText("Model handling"), "keep-root-model");
    const button = screen.getByRole("button", { name: "Preview switch" });
    button.focus();
    rerender(view("default:r2"));
    expect(screen.getByLabelText("Provider ID")).toHaveValue("dal");
    expect(screen.getByLabelText("Model handling")).toHaveValue("provider-default");
    expect(screen.getByRole("button", { name: "Preview switch" })).toBe(button);
    expect(button).toHaveFocus();
  });
});
