import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

// Transitional orchestration only. Removing an exemption is welcome; adding one
// requires an explicit architecture decision, never a blanket compiler change.
const legacyExemptions = new Set([
  "application/backups.js",
  "application/operation-result.js",
  "application/ordinary-write-runtime.js",
  "application/plan-context.js",
  "application/provider-switch.js",
  "application/provider-sync.js",
  "application/repair-targets.js",
  "application/repair.js",
  "application/restore.js",
  "application/runtime-context.js",
  "application/runtime-support.js",
  "application/status.js",
  "application/watch-runtime.js"
]);

async function sourceFiles(directory, prefix = "") {
  const files = [];
  for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
    const relative = `${prefix}${entry.name}`;
    if (entry.isDirectory()) files.push(...await sourceFiles(path.join(directory, entry.name), `${relative}/`));
    else if (entry.name.endsWith(".js")) files.push(relative);
  }
  return files;
}

test("every Core JS file explicitly opts into checking or a frozen migration exemption", async () => {
  const root = fileURLToPath(new URL("../src/", import.meta.url));
  for (const file of await sourceFiles(root)) {
    const source = await fs.readFile(path.join(root, file), "utf8");
    const directive = source.split(/\r?\n/, 1)[0];
    assert.ok(directive === "// @ts-check" || (directive === "// @ts-nocheck" && legacyExemptions.has(file)), `${file} must be checked; no new exemptions`);
    if (directive === "// @ts-check") assert.doesNotMatch(source, /@ts-nocheck/, `${file} must not disable its check later`);
  }
});
