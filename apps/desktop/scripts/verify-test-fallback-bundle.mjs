import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const desktopRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const mainRoot = path.join(desktopRoot, "out", "main");

async function filesUnder(root) {
  const result = [];
  async function visit(directory) {
    for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
      const absolute = path.join(directory, entry.name);
      if (entry.isDirectory()) await visit(absolute);
      else if (entry.isFile() && entry.name.endsWith(".js")) result.push(absolute);
    }
  }
  await visit(root);
  return result;
}

const text = (await Promise.all((await filesUnder(mainRoot)).map((file) => fs.readFile(file, "utf8")))).join("\n");
assert.match(text, /import\(["']better-sqlite3["']\)/, "Desktop test output is missing the native SQLite fallback.");
assert.doesNotMatch(text, /import\(["']node:sqlite["']\)/, "Desktop test output did not compile out node:sqlite.");
assert.doesNotMatch(
  text,
  /__CPS_DESKTOP_FORCE_BETTER_SQLITE3__/,
  "Desktop test output retained the compile-time SQLite driver selector."
);

process.stdout.write("Desktop test bundle is pinned to the native SQLite fallback.\n");
