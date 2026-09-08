import { render, screen } from "@testing-library/react";
import { I18nextProvider } from "react-i18next";
import { describe, expect, it } from "vitest";
import { OverviewPage } from "../src/features/overview/OverviewPage.js";
import { createAppI18n } from "../src/i18n.js";
import { statusFor } from "./helpers/app-fixtures.js";

describe("Overview storage paths", () => {
  it("pairs storage and Sync after distributions, preserving reading order and keeping Switch last", async () => {
    const i18n = await createAppI18n("en");
    const status = { ...statusFor(), displayPaths: { codexHome: "C:\\Synthetic\\.codex", sqliteHome: "C:\\Synthetic\\.codex\\sqlite", stateDbPath: null } };
    render(<I18nextProvider i18n={i18n}><OverviewPage status={status} loading={false} refresh={() => {}} profileName="Fixture" providers={["openai"]} sqliteHomeConfigured={false} writeDisabled={false} prepareSync={async () => {}} directSync={async () => {}} prepareSwitch={async () => {}} manageStorage={() => {}} /></I18nextProvider>);
    const button = screen.getByRole("button", { name: "Sync now" });
    const fullPath = screen.getByText(status.displayPaths.codexHome);
    const fileDistribution = screen.getByRole("heading", { name: "Session files", exact: true });
    const indexDistribution = screen.getByRole("heading", { name: "Local chat index", exact: true });
    const storageProfile = screen.getByText("Storage profile", { exact: true });
    const switchHeading = screen.getByRole("heading", { name: "Switch Provider separately" });
    for (const distribution of [fileDistribution, indexDistribution]) {
      expect(distribution.compareDocumentPosition(storageProfile) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    }
    expect(fullPath.compareDocumentPosition(button) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(button.compareDocumentPosition(switchHeading) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    const pairedRow = screen.getByTestId("overview-storage-sync");
    expect(pairedRow).toHaveClass("lg:grid-cols-2");
    expect(pairedRow.children[0]).toContainElement(fullPath);
    expect(pairedRow.children[1]).toContainElement(button);
    expect(pairedRow).not.toContainElement(switchHeading);
    const forms = screen.getByRole("region", { name: "Sync and switch" }).querySelectorAll("form");
    expect(forms).toHaveLength(2);
    expect(forms[1]).toContainElement(screen.getByLabelText("Provider ID"));
    expect(forms[1].parentElement!.parentElement).not.toHaveClass("lg:grid-cols-2");
    expect(fullPath).toBeVisible();
  });
  it("shows complete trusted paths and updates them with the next snapshot", async () => {
    const i18n = await createAppI18n("zh-CN");
    const status = { ...statusFor(), displayPaths: { codexHome: "C:\\Users\\Fixture\\.codex", sqliteHome: "D:\\Chat Index", stateDbPath: "D:\\Chat Index\\state_5.sqlite" as string | null } };
    const view = (snapshot: typeof status) => <I18nextProvider i18n={i18n}><OverviewPage status={snapshot} loading={false} refresh={() => {}} profileName="测试位置" providers={["openai"]} sqliteHomeConfigured={true} writeDisabled={false} prepareSync={async () => {}} prepareSwitch={async () => {}} manageStorage={() => {}} /></I18nextProvider>;
    const { rerender } = render(view(status));
    expect(screen.getByText(status.displayPaths.codexHome)).toBeVisible();
    expect(screen.getByText(status.displayPaths.sqliteHome)).toBeVisible();
    expect(screen.getByText(status.displayPaths.stateDbPath!)).toBeVisible();
    rerender(view({ ...status, displayPaths: { codexHome: "E:\\Other Codex", sqliteHome: "E:\\Other Index", stateDbPath: null } }));
    expect(screen.queryByText(status.displayPaths.codexHome)).not.toBeInTheDocument();
    expect(screen.getByText("E:\\Other Codex")).toBeVisible();
    expect(screen.getByText("尚未找到数据库")).toBeVisible();
  });
});
