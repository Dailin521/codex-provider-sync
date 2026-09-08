import fs from "node:fs/promises";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const desktopRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const repositoryRoot = path.resolve(desktopRoot, "../..");
const outputRoot = path.join(repositoryRoot, "dist-desktop");

async function existing(candidates) {
  for (const candidate of candidates) {
    try {
      const stat = await fs.stat(candidate);
      if (stat.isFile()) return candidate;
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
  }
  return null;
}

const directories = (await fs.readdir(outputRoot, { withFileTypes: true }))
  .filter((entry) => entry.isDirectory())
  .map((entry) => entry.name)
  .sort();
let candidates;
if (process.platform === "win32") {
  candidates = [
    path.join(outputRoot, "win-unpacked", "Codex Provider Sync.exe"),
    path.join(outputRoot, "win-unpacked", "codex-provider-sync.exe")
  ];
} else if (process.platform === "darwin") {
  candidates = directories
    .filter((name) => name.startsWith("mac"))
    .flatMap((name) => [
      path.join(outputRoot, name, "Codex Provider Sync.app", "Contents", "MacOS", "Codex Provider Sync"),
      path.join(outputRoot, name, "Codex Provider Sync.app", "Contents", "MacOS", "codex-provider-sync")
    ]);
} else if (process.platform === "linux") {
  candidates = directories
    .filter((name) => name.startsWith("linux") && name.endsWith("unpacked"))
    .flatMap((name) => [
      path.join(outputRoot, name, "codex-provider-sync"),
      path.join(outputRoot, name, "Codex Provider Sync")
    ]);
} else {
  throw new Error(`Unsupported packaged Electron smoke platform: ${process.platform}`);
}

const executable = await existing(candidates);
if (!executable) {
  throw new Error(`No unpacked Electron executable found under ${outputRoot}.`);
}
const npmCli = process.env.npm_execpath;
if (!npmCli) throw new Error("npm_execpath is required to run the packaged Electron smoke.");
const result = spawnSync(process.execPath, [
  npmCli,
  "run",
  "test:e2e:production",
  "--workspace",
  "@codex-provider-sync/desktop"
], {
  cwd: repositoryRoot,
  env: { ...process.env, CPS_DESKTOP_EXECUTABLE: executable },
  encoding: "utf8",
  stdio: "inherit"
});
if (result.error) throw result.error;
if (result.status !== 0) process.exit(result.status ?? 1);
process.stdout.write(`Unpacked Electron smoke passed: ${executable}\n`);
