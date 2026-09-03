import path from "node:path";

import { createBackup } from "../src/backup.js";
import { applySessionChanges, collectProviderChanges } from "../src/session-files.js";
import { TransactionJournal } from "../src/transaction-journal.js";

if (process.argv.length !== 3) process.exit(64);

// This host deliberately creates historical v0.5 Node recovery evidence. New
// ordinary writes do not create transaction journals; Restore still needs a
// real legacy journal/backup pair for cross-runtime compatibility coverage.
const codexHome = path.resolve(process.argv[2]);
const targetProvider = "openai";
const { changes } = await collectProviderChanges(codexHome, targetProvider);
if (changes.length === 0) process.exit(66);
const backupDir = await createBackup({
  codexHome,
  targetProvider,
  sessionChanges: changes,
  configPath: path.join(codexHome, "config.toml")
});
const journal = await TransactionJournal.create(backupDir, {
  codexHome,
  targetProvider,
  potentialTargets: changes.map((change) => change.path)
});
await journal.applying("rollout", changes[0].path);
await applySessionChanges([changes[0]], {
  onMutation: () => process.exit(86)
});

process.exit(65);
