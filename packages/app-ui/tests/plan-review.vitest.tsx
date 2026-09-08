import type { PlanSummary } from "@codex-provider-sync/contracts";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it, vi } from "vitest";

import { PlanReview } from "../src/features/operations/PlanReview.js";
import { createAppI18n } from "../src/i18n.js";

const plan: PlanSummary = {
  schemaVersion: 1,
  planId: "opaque-plan-id",
  operation: "sync",
  createdAt: "2026-08-27T00:00:00.000Z",
  expiresAt: "2026-08-27T00:10:00.000Z",
  profile: { id: "default", revision: "r1" },
  storageRevision: "storage-r1",
  configRevision: "config-r1",
  rolloutRevision: "rollout-r1",
  stateDbRevision: "state-r1",
  target: { provider: "openai", model: null },
  impact: {
    rolloutFilesToChange: 2,
    sqliteRowsToChange: 1,
    lockedRolloutFiles: 2,
    sessionActivity: { state: "checked", count: 3 },
    backupExpected: true
  },
  warnings: [],
  requiresConfirmation: true
};

describe("PlanReview", () => {
  it.each(["en", "zh-CN"] as const)("regenerates a repair plan when a selected session changes in %s", async (locale) => {
    const user = userEvent.setup();
    const i18n = await createAppI18n(locale);
    const refineRepairSessions = vi.fn();
    const repairPlan = {
      ...plan,
      operation: "repair" as const,
      target: { provider: "openai", targets: ["models"], scope: "all" },
      impact: {
        ...plan.impact,
        repairPreview: [{ sessionId: "session-1", changes: [{ target: "models", before: "old-model", after: "new-model" }] }],
        repairPreviewTotal: 1,
        repairPreviewTruncated: false
      }
    } as PlanSummary;
    const rendered = render(<I18nextProvider i18n={i18n}><PlanReview apply={vi.fn()} applying={false} cancel={vi.fn()} cancelling={false} close={vi.fn()} plan={repairPlan} progress={null} refineRepairSessions={refineRepairSessions} restoreFocus={vi.fn()} /></I18nextProvider>);
    expect(screen.getByText(i18n.t("plan.repairPreview.all"))).toBeVisible();
    expect(screen.getByRole("heading", { name: i18n.t("plan.repairPreview.effectsTitle") })).toBeVisible();
    expect(screen.getByText(i18n.t("diagnostics.repairTargetHints.models"))).toBeVisible();
    expect(screen.getByText(i18n.t("plan.repairPreview.unchanged"))).toBeVisible();
    expect(screen.getByText((_, element) => element?.tagName === "LI" && element.textContent?.includes("old-model → new-model") === true)).toBeVisible();
    await user.click(screen.getByRole("checkbox", { name: i18n.t("plan.repairPreview.selectSession", { sessionId: "session-1" }) }));
    expect(screen.getByRole("button", { name: i18n.t("plan.confirmActions.repair") })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: i18n.t("plan.repairPreview.update") }));
    expect(refineRepairSessions).toHaveBeenCalledWith(["session-1"]);
    rendered.rerender(<I18nextProvider i18n={i18n}><PlanReview apply={vi.fn()} applying={false} cancel={vi.fn()} cancelling={false} close={vi.fn()} plan={repairPlan} progress={null} repairSelectionPending refineRepairSessions={refineRepairSessions} restoreFocus={vi.fn()} /></I18nextProvider>);
    expect(screen.getByRole("button", { name: i18n.t("plan.confirmActions.repair") })).toBeDisabled();
    expect(screen.getByText(i18n.t("plan.repairPreview.regenerating"))).toBeVisible();
  });

  it.each(["en", "zh-CN"] as const)("keeps workspace repair global with readonly details and correctly labelled counts in %s", async (locale) => {
    const i18n = await createAppI18n(locale);
    render(<I18nextProvider i18n={i18n}><PlanReview apply={vi.fn()} applying={false} cancel={vi.fn()} cancelling={false} close={vi.fn()} plan={{ ...plan, operation: "repair", target: { provider: "openai", targets: ["workspaceRoots", "cwd", "userEvent"], scope: "all" }, impact: {
      rolloutFilesToChange: 0, sqliteRowsToChange: 739,
      sqliteCwdRowsToChange: 700, sqliteUserEventRowsToChange: 39,
      workspaceRootsToChange: 2, workspaceSettingsChangeKinds: ["projectOrder", "settingsBackup", "untrusted-key"],
      repairPreviewTotal: 710, repairPreviewTruncated: true,
      repairPreview: [{ sessionId: "session-1", changes: [{ target: "cwd", before: "different", after: "rollout-cwd" }] }]
    } } as PlanSummary} progress={null} restoreFocus={vi.fn()} /></I18nextProvider>);
    expect(screen.getByText(i18n.t("plan.repairPreview.workspaceGlobal"))).toBeVisible();
    expect(screen.getByText(i18n.t("diagnostics.repairTargetHints.workspaceRoots"))).toBeVisible();
    expect(screen.queryByRole("radio")).not.toBeInTheDocument();
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: i18n.t("plan.repairPreview.update") })).not.toBeInTheDocument();
    expect(screen.getByText("session-1")).toBeVisible();
    expect(screen.getByRole("region", { name: i18n.t("plan.repairPreview.title") })).toHaveClass("max-h-80", "overflow-y-auto");
    for (const [key, count] of [["sqliteFields", 739], ["sqliteCwd", 700], ["sqliteUserEvent", 39], ["affectedSessions", 710], ["workspaceSettings", 2]] as const) {
      expect(screen.getByText(i18n.t(`plan.fields.${key}`)).parentElement).toHaveTextContent(String(count));
    }
    expect(screen.queryByText(i18n.t("plan.fields.sqliteRows"))).not.toBeInTheDocument();
    expect(screen.queryByText(i18n.t("plan.fields.workspaceRoots"))).not.toBeInTheDocument();
    expect(screen.getByText(i18n.t("plan.workspaceChanges.settingsBackup"))).toBeVisible();
    expect(screen.getByText(i18n.t("plan.workspaceChanges.projectOrder"))).toBeVisible();
    expect(screen.queryByText("untrusted-key")).not.toBeInTheDocument();
    expect(screen.getByText(new RegExp(i18n.t("plan.repairPreview.truncated").replace(/\./g, "\\.")))).toBeVisible();
  });

  it("presents a user-facing operation summary without raw plan data", async () => {
    const i18n = await createAppI18n("en");
    render(
      <I18nextProvider i18n={i18n}>
        <PlanReview
          apply={vi.fn()}
          applying={false}
          cancel={vi.fn()}
          cancelling={false}
          close={vi.fn()}
          plan={plan}
          progress={null}
          restoreFocus={vi.fn()}
        />
      </I18nextProvider>
    );

    expect(await screen.findByRole("dialog", { name: "Confirm sync" })).toBeVisible();
    expect(screen.getByText(/Sync Provider information/)).toBeVisible();
    expect(screen.getByText("Provider")).toBeVisible();
    expect(screen.getByText("openai")).toBeVisible();
    expect(screen.getByText("Session files to update")).toBeVisible();
    expect(screen.getByText("Sessions currently in use")).toBeVisible();
    expect(screen.getByText("3", { exact: true })).toBeVisible();
    expect(screen.getByText("Sessions to skip this time")).toBeVisible();
    expect(screen.getAllByText("2")).toHaveLength(2);
    expect(screen.getByText("A backup will be created before writes.")).toBeVisible();
    expect(screen.queryByText("Operation details")).not.toBeInTheDocument();
    expect(screen.queryByText(/"provider": "openai"/)).not.toBeInTheDocument();
  });

  it("shows the root model transition and the historical-model boundary for Switch", async () => {
    const i18n = await createAppI18n("en");
    render(
      <I18nextProvider i18n={i18n}>
        <PlanReview
          apply={vi.fn()}
          applying={false}
          cancel={vi.fn()}
          cancelling={false}
          close={vi.fn()}
          currentModel="gpt-5"
          plan={{
            ...plan,
            operation: "switch",
            target: { provider: "relay", model: "relay-model", modelMode: "provider-default" }
          }}
          progress={null}
          restoreFocus={vi.fn()}
        />
      </I18nextProvider>
    );

    expect(await screen.findByRole("dialog", { name: "Confirm Provider switch" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Confirm switch" })).toBeVisible();
    expect(screen.getByText("Root model change")).toBeVisible();
    expect(screen.getByText("gpt-5 → relay-model")).toBeVisible();
    expect(screen.getByText("Use model configured for this Provider")).toBeVisible();
    expect(screen.getByText(/models recorded by historical sessions will not be changed/)).toBeVisible();
  });
});
