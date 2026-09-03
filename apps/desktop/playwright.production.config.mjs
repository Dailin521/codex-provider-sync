import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  testMatch: "desktop-production-boundary.spec.mjs",
  timeout: 45_000,
  expect: { timeout: 10_000 },
  forbidOnly: true,
  retries: 0,
  workers: 1,
  reporter: "line",
  use: { trace: "retain-on-failure" }
});
