import type { FileUpdateTiming } from "@codex-provider-sync/contracts";
import { fireEvent, render, screen } from "@testing-library/react";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it } from "vitest";
import { FileUpdateTimingDetails } from "../src/features/operation-logs/FileUpdateTimingDetails.js";
import { createAppI18n } from "../src/i18n.js";

const timing: FileUpdateTiming = {
  schemaVersion: 1, scope: "windows-first-line", attemptedFiles: 3, measuredFiles: 2,
  inPlaceFiles: 1, rewrittenFiles: 1, skippedFiles: 0, totalMs: 21465,
  workerStartupMs: 250, workerCloseMs: 15, requestRoundTripMs: 20300, workerMs: 19000,
  sourceOpenMs: 160, readHeaderMs: 300, tempCreateMs: 200,
  copyTailMs: 3100, flushMs: 6200, replaceMs: 1200, cleanupMs: 775, restoreMtimeMs: 427
};

describe("File update timing", () => {
  it.each(["en", "zh-CN"] as const)("shows actual sub-second timings and incomplete coverage in %s", async (locale) => {
    const i18n = await createAppI18n(locale);
    render(<I18nextProvider i18n={i18n}><FileUpdateTimingDetails timing={timing} pending={false} /></I18nextProvider>);
    expect(screen.getByRole("heading", { name: i18n.t("logs.fileTiming.title") })).toBeVisible();
    expect(screen.getByText(i18n.t("logs.fileTiming.phases.restoreMtimeMs"))).toBeVisible();
    expect(screen.getByText(i18n.t("logs.fileTiming.milliseconds", { value: "427.0" }))).toBeVisible();
    expect(screen.getByText(i18n.t("logs.fileTiming.incomplete"))).toBeVisible();
    const summary = screen.getByText(i18n.t("logs.fileTiming.more"));
    expect(summary.closest("details")).not.toHaveAttribute("open");
    fireEvent.click(summary);
    expect(summary.closest("details")).toHaveAttribute("open");
    expect(screen.getByText(i18n.t("logs.fileTiming.nested"))).toBeVisible();
  });

  it("does not invent zero timing for old/noop/interrupted records", async () => {
    const i18n = await createAppI18n("en");
    const { rerender } = render(<I18nextProvider i18n={i18n}><FileUpdateTimingDetails pending={false} /></I18nextProvider>);
    expect(screen.getByText("File timing was not recorded for this operation.")).toBeVisible();
    expect(screen.queryByRole("heading", { name: "File update timing" })).not.toBeInTheDocument();
    rerender(<I18nextProvider i18n={i18n}><FileUpdateTimingDetails pending /></I18nextProvider>);
    expect(screen.getByText("File timing will be available after execution.")).toBeVisible();
  });
});
