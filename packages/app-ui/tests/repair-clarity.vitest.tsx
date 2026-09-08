import type { DiagnosticsSnapshot } from "@codex-provider-sync/contracts";
import { MockCoreClient } from "@codex-provider-sync/core-client";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it, vi } from "vitest";

import { AppUi } from "../src/App.js";
import { DiagnosticsPage } from "../src/features/diagnostics/DiagnosticsPage.js";
import { createAppI18n } from "../src/i18n.js";
import { statusFor } from "./helpers/app-fixtures.js";

function snapshot(): DiagnosticsSnapshot {
  return {
    schemaVersion: 1, generatedAt: "2026-09-07T00:00:00Z", runtime: {},
    storage: { sqliteHomeSource: "default", stateDbFound: true, sqliteSupported: true },
    provider: { sqliteCounts: { sessions: { openai: 12 }, archived_sessions: {} } },
    safety: { pendingRecovery: false, operationInProgress: null, rolloutScanComplete: true, lockedRolloutCount: 0 },
    issues: { rootModelAvailable: true, rolloutModelFilesNeedingRepair: 9, sqliteModelRowsNeedingRepair: 9,
      cwdRowsNeedingRepair: 12, userEventRowsNeedingRepair: 10, workspaceRootsNeedingRepair: 2, encryptedContentFiles: 1849 }
  };
}

describe("User-facing repair choices", () => {
  it.each(["en", "zh-CN"] as const)("separates optional model adjustment and keeps preview selections independent in %s", async (locale) => {
    const i18n = await createAppI18n(locale);
    const user = userEvent.setup();
    const prepare = vi.fn(async () => {});
    render(<I18nextProvider i18n={i18n}><DiagnosticsPage canExport={false} canRepair loading={false} exporting={false} repairDisabled={false} refresh={vi.fn()} exportBundle={vi.fn()} prepareRepair={prepare} /></I18nextProvider>);
    const adjustment = screen.getByText(i18n.t("diagnostics.adjustmentTitle"), { exact: true });
    const repair = screen.getByText(i18n.t("diagnostics.repairTitle"), { exact: true });
    expect(adjustment.closest("details")).not.toHaveAttribute("open");
    expect(repair.closest("details")).not.toHaveAttribute("open");
    await user.click(repair);
    expect(within(repair.closest("details")!).getAllByRole("checkbox")).toHaveLength(3);
    expect(screen.getByRole("checkbox", { name: i18n.t("diagnostics.repairTargets.models") })).not.toBeVisible();
    await user.click(screen.getByRole("checkbox", { name: i18n.t("diagnostics.repairTargets.cwd") }));
    await user.click(adjustment);
    const model = screen.getByRole("checkbox", { name: i18n.t("diagnostics.repairTargets.models") });
    expect(model).not.toBeChecked();
    expect(model).toHaveAccessibleDescription(i18n.t("diagnostics.repairTargetHints.models"));
    await user.click(model);
    await user.click(screen.getByRole("button", { name: i18n.t("diagnostics.previewAdjustment") }));
    await waitFor(() => expect(prepare).toHaveBeenCalledOnce());
    expect(prepare.mock.calls[0]?.[0]).toEqual({ models: true, cwd: false, userEvent: false, workspaceRoots: false });
    await user.click(screen.getByRole("checkbox", { name: i18n.t("diagnostics.repairTargets.workspaceRoots") }));
    expect(screen.getByText(i18n.t("diagnostics.workspaceRootsIncludesCwd"))).toBeVisible();
    await user.click(screen.getByRole("button", { name: i18n.t("diagnostics.prepareRepair") }));
    await waitFor(() => expect(prepare).toHaveBeenCalledTimes(2));
    expect(prepare.mock.calls[1]?.[0]).toEqual({ models: false, cwd: true, userEvent: false, workspaceRoots: true });
  });

  it.each(["en", "zh-CN"] as const)("links verified findings to the right unselected control without scanning or preparing in %s", async (locale) => {
    const i18n = await createAppI18n(locale);
    const user = userEvent.setup();
    const prepare = vi.fn(async () => {});
    const refresh = vi.fn();
    render(<I18nextProvider i18n={i18n}><DiagnosticsPage diagnostics={snapshot()} canExport={false} canRepair loading={false} exporting={false} repairDisabled={false} refresh={refresh} exportBundle={vi.fn()} prepareRepair={prepare} /></I18nextProvider>);
    for (const target of ["cwd", "userEvent", "workspaceRoots"] as const) {
      await user.click(screen.getByRole("button", { name: i18n.t("diagnostics.viewSpecificRepair", { target: i18n.t(`diagnostics.repairTargets.${target}`) }) }));
      expect(screen.getByRole("checkbox", { name: i18n.t(`diagnostics.repairTargets.${target}`) })).toHaveFocus();
      for (const checkbox of screen.getAllByRole("checkbox")) expect(checkbox).not.toBeChecked();
    }
    expect(screen.getByText(i18n.t("diagnostics.adjustmentTitle"), { exact: true }).closest("details")).not.toHaveAttribute("open");
    expect(screen.getAllByRole("button", { name: new RegExp(i18n.t("diagnostics.viewRepair")) })).toHaveLength(3);
    expect(prepare).not.toHaveBeenCalled();
    expect(refresh).not.toHaveBeenCalled();
  });

  it("does not offer recommendations for stale, failed, incomplete, blocked or unknown checks", async () => {
    const i18n = await createAppI18n("en");
    const defaults = { diagnostics: snapshot(), canExport: false, canRepair: true, loading: false, exporting: false, repairDisabled: false, refresh: vi.fn(), exportBundle: vi.fn(), prepareRepair: vi.fn(async () => {}) };
    const view = render(<I18nextProvider i18n={i18n}><DiagnosticsPage {...defaults} /></I18nextProvider>);
    expect(screen.getAllByRole("button", { name: /^View repair:/ })).toHaveLength(3);
    const variants = [
      { expired: true }, { error: { code: "INTERNAL_ERROR" } }, { loading: true }, { repairDisabled: true }, { canRepair: false },
      { diagnostics: { ...snapshot(), safety: {} } },
      { diagnostics: { ...snapshot(), safety: { ...snapshot().safety, rolloutScanComplete: false } } },
      { diagnostics: { ...snapshot(), safety: { ...snapshot().safety, lockedRolloutCount: 1 } } },
      { diagnostics: { ...snapshot(), safety: { ...snapshot().safety, pendingRecovery: true } } },
      { diagnostics: { ...snapshot(), storage: { stateDbFound: false, sqliteSupported: true } } },
      { diagnostics: { ...snapshot(), provider: { sqliteCounts: null } } },
      { diagnostics: { ...snapshot(), issues: { cwdRowsNeedingRepair: -1, userEventRowsNeedingRepair: "10", workspaceRootsNeedingRepair: 0.5 } } },
      { diagnostics: { ...snapshot(), issues: { rootModelAvailable: true, rolloutModelFilesNeedingRepair: 9, sqliteModelRowsNeedingRepair: 9, encryptedContentFiles: 1849 } } }
    ];
    for (const variant of variants) {
      view.rerender(<I18nextProvider i18n={i18n}><DiagnosticsPage {...defaults} {...variant} /></I18nextProvider>);
      expect(screen.queryByRole("button", { name: /^View repair:/ })).not.toBeInTheDocument();
    }
    expect(defaults.prepareRepair).not.toHaveBeenCalled();
    expect(defaults.refresh).not.toHaveBeenCalled();
  });

  it("blocks disabled controls and programmatic form submission with an existing selection", async () => {
    const i18n = await createAppI18n("en");
    const user = userEvent.setup();
    const prepare = vi.fn(async () => {});
    const props = { canExport: false, canRepair: true, loading: false, exporting: false, repairDisabled: false, refresh: vi.fn(), exportBundle: vi.fn(), prepareRepair: prepare };
    const view = render(<I18nextProvider i18n={i18n}><DiagnosticsPage {...props} /></I18nextProvider>);
    await user.click(screen.getByText("Targeted repair", { exact: true }));
    await user.click(screen.getByRole("checkbox", { name: "Correct chat project folders" }));
    view.rerender(<I18nextProvider i18n={i18n}><DiagnosticsPage {...props} repairDisabled /></I18nextProvider>);
    for (const checkbox of screen.getAllByRole("checkbox")) expect(checkbox).toBeDisabled();
    const button = screen.getByRole("button", { name: "Preview repair" });
    expect(button).toBeDisabled();
    fireEvent.submit(button.closest("form")!);
    await waitFor(() => expect(button).toBeDisabled());
    expect(prepare).not.toHaveBeenCalled();
  });

  it("clears both selections and disclosures on profile switch without triggering diagnostics", async () => {
    const user = userEvent.setup();
    const getDiagnostics = vi.fn();
    render(<AppUi surface="desktop" core={new MockCoreClient({ getStatus: async () => statusFor(), getDiagnostics })} host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }, { id: "other", name: "Other", revision: "profile-r1" }] }} initialLocale="en" initialTheme="system" preferences={{ getLocale: () => "en", setLocale: () => {}, getTheme: () => "system", setTheme: () => {} }} />);
    await user.click(await screen.findByRole("button", { name: "Advanced features", exact: true }));
    await user.click(screen.getByText("Targeted repair", { exact: true }));
    await user.click(screen.getByRole("checkbox", { name: "Correct chat project folders" }));
    await user.click(screen.getByText("Advanced adjustments", { exact: true }));
    await user.click(screen.getByRole("checkbox", { name: "Unify historical model names" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "Profile" }), "other");
    await waitFor(() => expect(screen.getByText("Targeted repair", { exact: true }).closest("details")).not.toHaveAttribute("open"));
    await user.click(screen.getByText("Targeted repair", { exact: true }));
    await user.click(screen.getByText("Advanced adjustments", { exact: true }));
    for (const checkbox of screen.getAllByRole("checkbox")) expect(checkbox).not.toBeChecked();
    expect(getDiagnostics).not.toHaveBeenCalled();
  });
});
