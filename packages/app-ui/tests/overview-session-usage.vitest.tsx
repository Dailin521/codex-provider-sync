import type { StatusSnapshot } from "@codex-provider-sync/contracts";
import { render, screen, within } from "@testing-library/react";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it } from "vitest";

import { OverviewPage } from "../src/features/overview/OverviewPage.js";
import { createAppI18n } from "../src/i18n.js";
import { statusFor } from "./helpers/app-fixtures.js";

async function renderOverview(status?: StatusSnapshot, locale: "en" | "zh-CN" = "en") {
  const i18n = await createAppI18n(locale);
  return render(<I18nextProvider i18n={i18n}><OverviewPage status={status} loading={false} refresh={() => {}} profileName="Fixture" providers={[]} sqliteHomeConfigured={false} writeDisabled prepareSync={async () => {}} prepareSwitch={async () => {}} manageStorage={() => {}} /></I18nextProvider>);
}

describe("Overview shows Codex writer-owned sessions independently of Sync targets", () => {
  it.each([
    ["en", "Sessions currently in use", "Same check as Preview Sync, within this sync's scope. Already-aligned sessions are not counted; checked again before writing."],
    ["zh-CN", "正在使用的会话", "与预览同步一致，仅统计本次同步范围；已同步的会话不计入。写入前会再次检查。"]
  ] as const)("shows only the label and checked count without implementation notes in %s", async (locale, title, hint) => {
    await renderOverview({ ...statusFor(), sessionActivity: { state: "checked", count: 0 } }, locale);
    const card = screen.getByText(title).parentElement!;
    expect(within(card).getByText("0", { exact: true })).toBeVisible();
    expect(within(card).queryByText(hint)).not.toBeInTheDocument();
    expect(card).toHaveTextContent(`${title}0`);
    expect(card.querySelectorAll("p")).toHaveLength(0);
  });

  it("counts already-aligned sessions and ignores legacy read/write blocker counts", async () => {
    const status: StatusSnapshot = { ...statusFor(), sessionActivity: { state: "checked", count: 3 }, syncSessionUsage: { state: "checked", count: 0 }, lockedRolloutFiles: ["fixture-a.jsonl", "fixture-b.jsonl"], rolloutScanComplete: false };
    status.rolloutCounts = { sessions: { openai: 25 }, archived_sessions: { relay: 10 } };
    await renderOverview(status);
    const card = screen.getByText("Sessions currently in use").parentElement!;
    expect(within(card).getByText("3", { exact: true })).toBeVisible();
    expect(screen.getByText("Not verified")).toBeVisible();
  });

  it.each([
    ["missing snapshot", undefined],
    ["older host without ownership", statusFor()],
    ["older target-only counter", { ...statusFor(), syncSessionUsage: { state: "checked", count: 0 } }],
    ["failed probe", { ...statusFor(), sessionActivity: { state: "unavailable", count: null } }],
    ["unsupported probe", { ...statusFor(), sessionActivity: { state: "unsupported", count: null } }],
    ["invalid snapshot", { ...statusFor(), sessionActivity: { state: "checked", count: 0 }, snapshotAt: "invalid" }],
    ["blocked status read", { ...statusFor(), sessionActivity: { state: "checked", count: 0 }, statusReadBlocked: { reason: "codex-home-lock" } }],
    ["cached snapshot during a write", { ...statusFor(), sessionActivity: { state: "checked", count: 0 }, operationInProgress: { operation: "sync" } }]
  ] as const)("does not invent a current zero for %s", async (_label, status) => {
    await renderOverview(status);
    const card = screen.getByText("Sessions currently in use").parentElement!;
    expect(within(card).getByText("Unknown", { exact: true })).toBeVisible();
    expect(card.querySelectorAll("p")).toHaveLength(0);
    expect(within(card).queryByText("0", { exact: true })).not.toBeInTheDocument();
  });
});
