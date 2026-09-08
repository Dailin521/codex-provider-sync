import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { AppErrorBoundary } from "../src/app/AppErrorBoundary.js";

function Thrower(): never {
  throw new Error("expected render failure");
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("AppErrorBoundary", () => {
  it.each([
    ["en", "This page is temporarily unavailable", "Your data was not changed. Reopen the app; if the problem continues, check Operation logs or export diagnostics.", "Reopen"],
    ["zh-CN", "页面暂时无法显示", "你的数据没有被更改。请重新打开应用；如果问题持续，请查看操作日志或导出诊断信息。", "重新打开"]
  ])("renders the %s fail-closed recovery surface", (locale, heading, message, reload) => {
    vi.spyOn(console, "error").mockImplementation(() => {});

    render(
      <AppErrorBoundary locale={() => locale}>
        <Thrower />
      </AppErrorBoundary>
    );

    expect(screen.getByRole("heading", { name: heading })).toBeVisible();
    expect(screen.getByText(message)).toBeVisible();
    expect(screen.getByRole("button", { name: reload })).toBeEnabled();
  });
});
