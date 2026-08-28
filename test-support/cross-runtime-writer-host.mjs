import fs from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";

import { runSync } from "../src/public-api.js";

if (process.argv.length !== 7) process.exit(64);

const [, , codexHome, provider, sqliteHome, readyPath, releasePath] = process.argv;

const result = await runSync({
  codexHome,
  provider,
  sqliteHome,
  faultInjector: async ({ point }) => {
    if (point !== "before_backup") return;
    await fs.writeFile(readyPath, "ready\n", { flag: "wx" });
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      try {
        await fs.access(releasePath);
        return;
      } catch (error) {
        if (error?.code !== "ENOENT") throw error;
      }
      await delay(25);
    }
    throw new Error("Timed out waiting for the cross-runtime writer release marker.");
  }
});

console.log(JSON.stringify({
  schemaVersion: 1,
  ok: true,
  operation: "sync",
  backupDir: result.backupDir,
  targetProvider: result.targetProvider
}));
