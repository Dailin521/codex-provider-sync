import type { PlanSummary } from "@codex-provider-sync/contracts";
import { render, screen } from "@testing-library/react";
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
    lockedRolloutFiles: [],
    backupExpected: true
  },
  warnings: [],
  requiresConfirmation: true
};

describe("PlanReview", () => {
  it("presents an auditable product summary and keeps raw JSON collapsed", async () => {
    const i18n = await createAppI18n("en");
    render(
      <I18nextProvider i18n={i18n}>
        <PlanReview
          apply={vi.fn()}
          applying={false}
          cancel={vi.fn()}
          cancelling={false}
          close={vi.fn()}
          operationId={null}
          plan={plan}
          progress={null}
          restoreFocus={vi.fn()}
        />
      </I18nextProvider>
    );

    expect(await screen.findByRole("dialog", { name: "Review plan" })).toBeVisible();
    expect(screen.getByText(/Sync Provider metadata/)).toBeVisible();
    expect(screen.getByText("Provider")).toBeVisible();
    expect(screen.getByText("openai")).toBeVisible();
    expect(screen.getByText("Rollout files affected")).toBeVisible();
    expect(screen.getByText("A backup will be created before writes.")).toBeVisible();
    expect(screen.getByText("Technical details")).toBeVisible();
    expect(screen.getByText(/"provider": "openai"/)).not.toBeVisible();
  });
});
