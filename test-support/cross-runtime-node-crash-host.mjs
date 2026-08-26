import { runSync } from "../src/public-api.js";

if (process.argv.length !== 3) process.exit(64);

await runSync({
  codexHome: process.argv[2],
  provider: "openai",
  faultInjector: async ({ point }) => {
    if (point === "after_rollout_mutation_before_applied") process.exit(86);
  }
});

process.exit(65);
