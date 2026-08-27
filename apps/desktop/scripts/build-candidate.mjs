import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { DESKTOP_CANDIDATE_TARGETS } from "./resolve-candidate-build.mjs";

const desktopRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const repositoryRoot = path.resolve(desktopRoot, "../..");
const VERSION_PATTERN = /^1\.0\.0-(?:alpha|beta|rc)\.\d+$/;
const BUILD_ID_PATTERN = /^[A-Za-z0-9._-]{1,128}$/;

const TARGET_CONFIG = Object.freeze({
  "windows-x64": { platform: "win32", arch: "x64", args: ["--win", "nsis", "zip", "--x64"] },
  "macos-x64": { platform: "darwin", arch: "x64", args: ["--mac", "dmg", "zip", "--x64"] },
  "macos-arm64": { platform: "darwin", arch: "arm64", args: ["--mac", "dmg", "zip", "--arm64"] },
  "linux-x64": { platform: "linux", arch: "x64", args: ["--linux", "AppImage", "deb", "--x64"] }
});

function runNpm(args, { cwd = repositoryRoot, env = process.env } = {}) {
  const npmCli = process.env.npm_execpath;
  if (!npmCli) throw new Error("npm_execpath is required for a candidate build.");
  const result = spawnSync(process.execPath, [npmCli, ...args], {
    cwd,
    env,
    encoding: "utf8",
    stdio: "inherit"
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`npm ${args.join(" ")} failed with exit code ${result.status}.`);
}

const target = process.env.CPS_CANDIDATE_TARGET;
const version = process.env.CPS_DESKTOP_VERSION;
const buildId = process.env.CPS_DESKTOP_BUILD_ID;
if (!DESKTOP_CANDIDATE_TARGETS.includes(target)) throw new Error("CPS_CANDIDATE_TARGET is invalid.");
if (!VERSION_PATTERN.test(version || "")) throw new Error("CPS_DESKTOP_VERSION is not a supported v1 candidate version.");
if (!BUILD_ID_PATTERN.test(buildId || "")) throw new Error("CPS_DESKTOP_BUILD_ID is invalid.");

const config = TARGET_CONFIG[target];
if (process.platform !== config.platform || process.arch !== config.arch) {
  throw new Error(`Candidate ${target} must be built on native ${config.platform}/${config.arch}, got ${process.platform}/${process.arch}.`);
}

const buildEnvironment = { ...process.env, CPS_DESKTOP_BUILD_ID: buildId };
runNpm(["run", "workspaces:build"], { env: buildEnvironment });
runNpm(["run", "build:electron"], { cwd: desktopRoot, env: buildEnvironment });
runNpm(["run", "verify:production-bundle"], { cwd: desktopRoot, env: buildEnvironment });
runNpm([
  "exec",
  "--",
  "electron-builder",
  ...config.args,
  "--publish",
  "never",
  "--config",
  "electron-builder.yml",
  `--config.extraMetadata.version=${version}`
], { cwd: desktopRoot, env: buildEnvironment });

process.stdout.write(`Desktop candidate built: ${target} ${version} ${buildId}\n`);
