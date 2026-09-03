import type { OperationOutcome, OperationResult } from "@codex-provider-sync/contracts";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createElement, Fragment, useState } from "react";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it } from "vitest";

import { OperationResultDialog, operationResultPresentation } from "../src/features/operations/OperationResultDialog.js";
import { createAppI18n } from "../src/i18n.js";

describe("operation result presentation", () => {
  it("maps every public outcome without a fallthrough", () => {
    const outcomes: OperationOutcome[] = [
      "completed",
      "partial",
      "failed_rolled_back",
      "recovery_required",
      "cancelled",
      "stale"
    ];

    expect(outcomes.map((outcome) => [outcome, operationResultPresentation(outcome)])).toEqual([
      ["completed", expect.objectContaining({ tone: "success", toastKey: "global.completed" })],
      ["partial", expect.objectContaining({ tone: "warning", toastKey: "global.partial" })],
      ["failed_rolled_back", expect.objectContaining({ tone: "warning", toastKey: "global.failed" })],
      ["recovery_required", expect.objectContaining({ tone: "danger", toastKey: "global.failed" })],
      ["cancelled", expect.objectContaining({ tone: "warning", toastKey: "global.cancelled" })],
      ["stale", expect.objectContaining({ tone: "warning", toastKey: "global.stale" })]
    ]);
    expect(new Set(outcomes.map((outcome) => operationResultPresentation(outcome).titleKey)).size).toBe(outcomes.length);
  });

  it("keeps recovery-required details open and only renders whitelisted result fields", async () => {
    const user = userEvent.setup();
    const i18n = await createAppI18n("en");
    let closeCalls = 0;
    const result: OperationResult = {
      schemaVersion: 1,
      operationId: "11111111-1111-4111-8111-111111111118",
      operation: "restore",
      outcome: "recovery_required",
      backup: { backupId: "managed-backup" },
      warnings: ["Recovery evidence is pending."],
      result: {
        restoreJournalState: "recovery-required",
        skippedLockedRolloutFiles: ["rollout-safe-name.jsonl"],
        token: "must-not-render",
        messageBody: "must-not-render",
        messageBodyChanged: "suffix-string-must-not-render",
        secretCount: 42
      }
    };
    render(createElement(
      I18nextProvider,
      { i18n },
      createElement(OperationResultDialog, {
        close: () => { closeCalls += 1; },
        closeDisabled: true,
        restoreFocus: () => {},
        result
      })
    ));

    expect(await screen.findByRole("alert")).toBeVisible();
    expect(screen.getByText("managed-backup")).toBeVisible();
    expect(screen.getByText("rollout-safe-name.jsonl")).toBeVisible();
    expect(screen.queryByText("must-not-render")).not.toBeInTheDocument();
    expect(screen.queryByText("suffix-string-must-not-render")).not.toBeInTheDocument();
    expect(screen.queryByText("42")).not.toBeInTheDocument();
    const closeButtons = screen.getAllByRole("button", { name: "Close" });
    expect(closeButtons).toHaveLength(2);
    for (const close of closeButtons) expect(close).toBeDisabled();
    await user.click(closeButtons.at(-1)!);
    expect(closeCalls).toBe(0);
  });

  it("restores focus after a completed result closes", async () => {
    const user = userEvent.setup();
    const i18n = await createAppI18n("en");
    const result: OperationResult = {
      schemaVersion: 1,
      operationId: "11111111-1111-4111-8111-111111111119",
      operation: "sync",
      outcome: "completed",
      backup: null,
      warnings: [],
      result: { inPlaceSessionFiles: 1, rewrittenSessionFiles: 0 },
      providerSync: {
        mode: "fast",
        rolloutScanScope: "metadata",
        inPlaceSessionFiles: 1,
        rewrittenSessionFiles: 0,
        unchecked: ["historyModels", "userEventFlags", "encryptedContent"]
      }
    };
    function Harness() {
      const [current, setCurrent] = useState<OperationResult | null>(result);
      return createElement(
        Fragment,
        null,
        createElement("button", { id: "prepare-trigger", type: "button" }, "Prepare sync"),
        createElement(OperationResultDialog, {
          close: () => setCurrent(null),
          restoreFocus: () => document.getElementById("prepare-trigger")?.focus(),
          result: current
        })
      );
    }
    render(createElement(I18nextProvider, { i18n }, createElement(Harness)));

    expect(await screen.findByText("Fast Provider-only sync")).toBeVisible();
    expect(screen.getByText("Metadata only")).toBeVisible();
    expect(screen.getByText("historyModels, userEventFlags, encryptedContent")).toBeVisible();
    await user.click((await screen.findAllByRole("button", { name: "Close" })).at(-1)!);
    await waitFor(() => expect(screen.getByRole("button", { name: "Prepare sync" })).toHaveFocus());
  });
});
