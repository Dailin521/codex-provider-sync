#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function usage() {
  console.log(`Usage: npm run publish:npm -- [options]

Options:
  --dry-run       Build, test, and preview the package without publishing.
  --skip-tests    Skip npm test (the Web UI build still runs).
  --otp CODE      Pass a one-time npm 2FA code without storing it.
  --tag TAG       Publish with an npm dist-tag (default: latest).
  --registry URL  npm registry (default: https://registry.npmjs.org/).
`);
}

function parseArgs(argv) {
  const options = { dryRun: false, skipTests: false, otp: process.env.NPM_OTP ?? "", tag: "latest", registry: "https://registry.npmjs.org/" };
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--help" || argument === "-h") { usage(); process.exit(0); }
    if (argument === "--dry-run") { options.dryRun = true; continue; }
    if (argument === "--skip-tests") { options.skipTests = true; continue; }
    if (argument === "--otp" || argument === "--tag" || argument === "--registry") {
      const value = argv[++index];
      if (!value) throw new Error(`${argument} requires a value.`);
      if (argument === "--otp") options.otp = value;
      if (argument === "--tag") options.tag = value;
      if (argument === "--registry") options.registry = value;
      continue;
    }
    throw new Error(`Unknown option: ${argument}`);
  }
  return options;
}

function npmInvocation(args) {
  const candidates = [
    process.env.npm_execpath?.trim(),
    path.resolve(path.dirname(process.execPath), "node_modules", "npm", "bin", "npm-cli.js"),
    path.resolve(path.dirname(process.execPath), "..", "lib", "node_modules", "npm", "bin", "npm-cli.js"),
    ...(process.env.PATH ?? "")
      .split(path.delimiter)
      .filter(Boolean)
      .map((directory) => path.resolve(directory.replace(/^"|"$/g, ""), "node_modules", "npm", "bin", "npm-cli.js")),
  ];
  const npmCli = candidates.find((candidate) => candidate && fs.existsSync(candidate));
  if (npmCli) {
    return { command: process.execPath, args: [npmCli, ...args] };
  }

  if (process.platform === "win32") {
    throw new Error("Could not locate npm-cli.js. Run this script through `npm run publish:npm` or repair the Node.js/npm installation.");
  }
  return { command: "npm", args };
}

function runNpm(args, { env = process.env } = {}) {
  console.log(`\n$ npm ${args.map((value) => value === env.NPM_OTP ? "--otp ******" : value).join(" ")}`);
  const invocation = npmInvocation(args);
  const result = spawnSync(invocation.command, invocation.args, { cwd: rootDir, env, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`npm ${args[0]} failed with exit code ${result.status}.`);
}

function packageInfo() {
  const packagePath = path.join(rootDir, "package.json");
  const manifest = JSON.parse(fs.readFileSync(packagePath, "utf8"));
  if (!manifest.name || !manifest.version) throw new Error("package.json must define name and version.");
  if (!manifest.bin?.["codex-provider"]) throw new Error("package.json must expose the codex-provider bin entry.");
  const binPath = path.resolve(rootDir, manifest.bin["codex-provider"]);
  if (!fs.existsSync(binPath)) throw new Error(`npm bin entry does not exist: ${binPath}`);
  return manifest;
}

function main() {
  const options = parseArgs(process.argv.slice(2));
  const manifest = packageInfo();
  console.log(`Preparing ${manifest.name}@${manifest.version} for npm.`);

  const registryArgs = ["--registry", options.registry];
  if (!options.dryRun) runNpm(["whoami", ...registryArgs]);
  runNpm(["run", "web:build"]);
  if (!options.skipTests) runNpm(["test"]);
  runNpm(["pack", "--dry-run", "--json", ...registryArgs]);

  const publishArgs = ["publish", "--access", "public", "--tag", options.tag, ...registryArgs];
  if (options.otp) publishArgs.push("--otp", options.otp);
  if (options.dryRun) {
    console.log("npm dry-run completed; nothing was published.");
    return;
  }
  runNpm(publishArgs, { env: { ...process.env, ...(options.otp ? { NPM_OTP: options.otp } : {}) } });
  console.log(`${manifest.name}@${manifest.version} published successfully.`);
}

try {
  main();
} catch (error) {
  console.error(`\nPublish failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
}
