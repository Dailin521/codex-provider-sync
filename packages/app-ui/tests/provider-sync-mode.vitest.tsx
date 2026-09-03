import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it, vi } from "vitest";

import { SwitchPage } from "../src/features/switch-provider/SwitchPage.js";
import { SyncPage } from "../src/features/sync/SyncPage.js";
import { createAppI18n } from "../src/i18n.js";

describe("Provider sync mode forms", () => {
  it("submits the selected fast mode from Sync", async () => {
    const i18n = await createAppI18n("en");
    const prepare = vi.fn(async () => {});
    const user = userEvent.setup();
    render(<I18nextProvider i18n={i18n}><SyncPage disabled={false} prepare={prepare} /></I18nextProvider>);

    await user.selectOptions(screen.getByLabelText("Sync mode"), "fast");
    expect(screen.getByText(/Reads rollout metadata only/)).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Prepare sync" }));

    await waitFor(() => expect(prepare).toHaveBeenCalledTimes(1));
    expect(prepare.mock.calls[0]?.[0]).toEqual({ keepCount: 5, syncMode: "fast" });
  });

  it("forces and submits keep-root-model when Switch selects fast mode", async () => {
    const i18n = await createAppI18n("en");
    const prepare = vi.fn(async () => {});
    const user = userEvent.setup();
    render(<I18nextProvider i18n={i18n}><SwitchPage disabled={false} prepare={prepare} providers={["relay"]} /></I18nextProvider>);

    const modelMode = screen.getByLabelText("Model handling");
    await user.selectOptions(screen.getByLabelText("Sync mode"), "fast");
    await waitFor(() => expect(modelMode).toHaveValue("keep-root-model"));
    expect(modelMode).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Prepare switch" }));

    await waitFor(() => expect(prepare).toHaveBeenCalledTimes(1));
    expect(prepare.mock.calls[0]?.[0]).toEqual({
      provider: "relay",
      modelMode: "keep-root-model",
      model: "",
      keepCount: 5,
      syncMode: "fast"
    });
  });
});
