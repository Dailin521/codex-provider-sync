import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";

if (!globalThis.requestAnimationFrame) {
  globalThis.requestAnimationFrame = (callback: FrameRequestCallback): number => globalThis.setTimeout(
    () => callback(globalThis.performance.now()),
    0
  );
}

if (!globalThis.cancelAnimationFrame) {
  globalThis.cancelAnimationFrame = (handle: number): void => globalThis.clearTimeout(handle);
}

afterEach(() => {
  cleanup();
  document.documentElement.lang = "";
  delete document.documentElement.dataset.theme;
});
