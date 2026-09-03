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
    ["en", "Application error", "The page encountered an unexpected error. No write was started automatically.", "Reload"],
    ["zh-CN", "应用错误", "页面遇到未预期错误；系统没有自动启动任何写操作。", "重新加载"]
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
