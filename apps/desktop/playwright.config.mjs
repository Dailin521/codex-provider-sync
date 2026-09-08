import { defineConfig } from "@playwright/test";

export default defineConfig({
  forbidOnly: true,
  testDir: "./e2e",
  testMatch: [
    "desktop-readonly.spec.mjs",
    "desktop-sync-switch.spec.mjs",
    "desktop-restore-relocation.spec.mjs"
  ],
  timeout: 45_000,
  expect: { timeout: 10_000 },
  workers: 1,
  fullyParallel: false,
  reporter: "line"
});
