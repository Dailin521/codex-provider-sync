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
  --node16 PATH   Exact Node 16.20.2 executable (or CPS_NODE16_EXECUTABLE).
  --node16-npm PATH  npm 8 npm-cli.js (or CPS_NODE16_NPM_CLI; defaults beside Node 16).
  --otp CODE      Pass a one-time npm 2FA code without storing it.
  --tag TAG       Publish with an npm dist-tag (default: latest).
  --registry URL  npm registry (default: https://registry.npmjs.org/).
`);
}

export function parseArgs(argv, env = process.env) {
  const options = { dryRun: false, help: false, node16: env.CPS_NODE16_EXECUTABLE ?? "", node16Npm: env.CPS_NODE16_NPM_CLI ?? "", otp: env.NPM_OTP ?? "", tag: "latest", registry: "https://registry.npmjs.org/" };
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--help" || argument === "-h") { options.help = true; continue; }
    if (argument === "--dry-run") { options.dryRun = true; continue; }
    if (argument === "--skip-tests") throw new Error("--skip-tests is no longer supported. Publishing gates cannot be bypassed.");
    if (["--otp", "--tag", "--registry", "--node16", "--node16-npm"].includes(argument)) {
      const value = argv[++index];
      if (!value) throw new Error(`${argument} requires a value.`);
      if (argument === "--otp") options.otp = value;
      if (argument === "--tag") options.tag = value;
      if (argument === "--registry") options.registry = value;
      if (argument === "--node16") options.node16 = value;
      if (argument === "--node16-npm") options.node16Npm = value;
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

function runNpm(args, { env = process.env, node16 = null } = {}) {
  console.log(`\n$ ${node16 ? "Node 16 / " : ""}npm ${args.map((value, index) => args[index - 1] === "--otp" ? "******" : value).join(" ")}`);
  const invocation = node16 ? { command: node16.command, args: [node16.npmCli, ...args] } : npmInvocation(args);
  const runtimeEnv = node16 ? {
    ...env,
    PATH: `${path.dirname(node16.command)}${path.delimiter}${env.PATH ?? ""}`,
    npm_execpath: node16.npmCli,
    npm_node_execpath: node16.command
  } : env;
  const result = spawnSync(invocation.command, invocation.args, { cwd: rootDir, env: runtimeEnv, stdio: "inherit", windowsHide: true });
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

export function resolveNode16(options) {
  if (!options.node16 || !path.isAbsolute(options.node16) || !fs.statSync(options.node16, { throwIfNoEntry: false })?.isFile()) {
    throw new Error("Provide an absolute Node 16.20.2 executable via --node16 or CPS_NODE16_EXECUTABLE before running publishing gates.");
  }
  const npmCli = options.node16Npm || [
    path.join(path.dirname(options.node16), "node_modules", "npm", "bin", "npm-cli.js"),
    path.resolve(path.dirname(options.node16), "../lib/node_modules/npm/bin/npm-cli.js")
  ].find((candidate) => fs.existsSync(candidate));
  if (!npmCli || !path.isAbsolute(npmCli) || !fs.statSync(npmCli, { throwIfNoEntry: false })?.isFile()) {
    throw new Error("Provide the matching npm 8 npm-cli.js via --node16-npm or CPS_NODE16_NPM_CLI.");
  }
  return { command: options.node16, npmCli };
}

// Injected runner keeps unit tests entirely offline: they never authenticate or publish.
export function runPublishingGates(options, { run = runNpm, nodeVersion = process.version, resolveRuntime = resolveNode16, log = console.log } = {}) {
  if (!/^v24\./.test(nodeVersion)) throw new Error("Run the publishing workflow on Node 24; Node 16 is a separate compatibility gate.");
  const node16 = resolveRuntime(options);
  const registryArgs = ["--registry", options.registry];
  // Fail on a wrong compatibility toolchain before doing expensive builds.
  run(["run", "runtime:verify-node16"], { node16 });
  run(["run", "web:build"]);
  run(["run", "architecture:check"]);
  run(["test"]);
  run(["run", "web:test:e2e"]);
  run(["run", "package:smoke:lifecycle"]);
  // The installed tarball test uses its own temporary directory; it does not
  // replace the modern workspace's node_modules with Node 16 dependencies.
  run(["run", "package:smoke:lifecycle"], { node16 });
  run(["audit", "--omit=dev", "--audit-level=moderate", ...registryArgs]);
  run(["audit", "--audit-level=high", ...registryArgs]);
  run(["pack", "--dry-run", "--json", ...registryArgs]);

  const publishArgs = ["publish", "--access", "public", "--tag", options.tag, ...registryArgs];
  if (options.otp) publishArgs.push("--otp", options.otp);
  if (options.dryRun) {
    log("npm dry-run completed; nothing was published.");
    return;
  }
  run(["whoami", ...registryArgs]);
  run(publishArgs, { env: { ...process.env, ...(options.otp ? { NPM_OTP: options.otp } : {}) } });
}

function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) { usage(); return; }
  const manifest = packageInfo();
  console.log(`Preparing ${manifest.name}@${manifest.version} for npm.`);
  runPublishingGates(options);
  if (options.dryRun) return;
  console.log(`${manifest.name}@${manifest.version} published successfully.`);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    main();
  } catch (error) {
    console.error(`\nPublish failed: ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  }
}
