import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const testRoot = path.join(repositoryRoot, "test");
const testFiles = fs.readdirSync(testRoot, { withFileTypes: true })
  .filter((entry) => entry.isFile() && entry.name.endsWith(".test.js"))
  .map((entry) => path.join("test", entry.name))
  .sort((left, right) => left.localeCompare(right));

if (testFiles.length === 0) {
  throw new Error("No root test files were found.");
}

if (process.platform === "win32" && Number(process.versions.node.split(".")[0]) === 16) {
  // Node 16 has no --test-concurrency flag. Its public runner supports the
  // same option: bound simultaneous cold PowerShell/native fixture workers
  // without extending the production lock probe's fail-closed deadline.
  const { run } = await import("node:test");
  const stream = run({ files: testFiles.map((file) => path.join(repositoryRoot, file)), concurrency: 2 });
  stream.once("test:fail", () => { process.exitCode = 1; });
  stream.pipe(process.stdout);
} else {
  const result = spawnSync(process.execPath, ["--test", ...testFiles], {
    cwd: repositoryRoot,
    env: process.env,
    stdio: "inherit"
  });
  if (result.error) throw result.error;
  if (result.signal) {
    throw new Error(`Root tests were terminated by ${result.signal}.`);
  }
  process.exitCode = result.status ?? 1;
}
