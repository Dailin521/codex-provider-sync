import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "jsdom",
    globals: true,
    include: ["tests/**/*.vitest.ts", "tests/**/*.vitest.tsx"],
    setupFiles: ["./tests/setup.ts"]
  }
});
