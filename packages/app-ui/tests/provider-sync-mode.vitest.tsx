import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it, vi } from "vitest";

import { BackupsRestorePage } from "../src/features/backups-restore/BackupsRestorePage.js";
import { DiagnosticsPage } from "../src/features/diagnostics/DiagnosticsPage.js";
import { SwitchPage } from "../src/features/switch-provider/SwitchPage.js";
import { SyncPage } from "../src/features/sync/SyncPage.js";
import { createAppI18n } from "../src/i18n.js";

describe("Provider-only Sync and Switch forms", () => {
  it("Sync has no per-operation retention input", async () => {
    const i18n = await createAppI18n("en");
    const prepare = vi.fn(async () => {});
    const user = userEvent.setup();
    render(<I18nextProvider i18n={i18n}><SyncPage disabled={false} prepare={prepare} /></I18nextProvider>);

    expect(screen.queryByLabelText("Sync mode")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Preview sync" }));

    await waitFor(() => expect(prepare).toHaveBeenCalledTimes(1));
    expect(prepare.mock.calls[0]?.[0]).toEqual({});
    expect(screen.queryByRole("spinbutton")).not.toBeInTheDocument();
  });

  it("submits the selected Switch model strategy without a sync mode", async () => {
    const i18n = await createAppI18n("en");
    const prepare = vi.fn(async () => {});
    const user = userEvent.setup();
    render(<I18nextProvider i18n={i18n}><SwitchPage disabled={false} prepare={prepare} providers={["relay"]} /></I18nextProvider>);

    const modelMode = screen.getByLabelText("Model handling");
    expect(screen.queryByLabelText("Sync mode")).not.toBeInTheDocument();
    expect(screen.getByText(/Read \[model_providers\.relay\]\.model from config\.toml/)).toBeVisible();
    expect(screen.getByText(/does not query the Provider online/)).toBeVisible();
    await user.selectOptions(modelMode, "keep-root-model");
    expect(screen.getByText("Only switch model_provider. Do not change the root-level model in config.toml.")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Preview switch" }));

    await waitFor(() => expect(prepare).toHaveBeenCalledTimes(1));
    expect(prepare.mock.calls[0]?.[0]).toEqual({
      provider: "relay",
      modelMode: "keep-root-model",
      model: ""
    });
  });

  it("Repair submits targets without a separate retention value", async () => {
    const i18n = await createAppI18n("en");
    const prepareRepair = vi.fn(async () => {});
    const user = userEvent.setup();
    render(
      <I18nextProvider i18n={i18n}>
        <DiagnosticsPage
          canExport={false}
          canRepair
          exportBundle={() => {}}
          exporting={false}
          loading={false}
          prepareRepair={prepareRepair}
          refresh={() => {}}
          repairDisabled={false}
        />
      </I18nextProvider>
    );

    await user.click(screen.getByText("Advanced adjustments", { exact: true }));
    await user.click(screen.getByRole("checkbox", { name: "Unify historical model names" }));
    await user.click(screen.getByRole("button", { name: "Preview adjustment" }));

    await waitFor(() => expect(prepareRepair).toHaveBeenCalledTimes(1));
    expect(prepareRepair.mock.calls[0]?.[0]).toEqual({
      models: true,
      cwd: false,
      userEvent: false,
      workspaceRoots: false
    });
  });

  it("opens cleanup confirmation before pruning the default retention", async () => {
    const i18n = await createAppI18n("en");
    const prune = vi.fn();
    const user = userEvent.setup();
    render(
      <I18nextProvider i18n={i18n}>
        <BackupsRestorePage
          backups={[]}
          canPrune
          canRestore={false}
          disabled={false}
          loading={false}
          prepare={async () => {}}
          profile={{ id: "default", name: "Default", revision: "r1" }}
          profiles={[]}
          prune={prune}
        />
      </I18nextProvider>
    );

    expect(screen.getAllByText("Saved rule: keep the newest 2 backups per Codex Home.").length).toBeGreaterThan(0);
    await user.click(screen.getByRole("button", { name: "Delete older backups" }));
    expect(prune).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Confirm cleanup" }));
    expect(prune).toHaveBeenCalledWith(2);
  });
});
