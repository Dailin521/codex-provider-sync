import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const installLifecycle = process.argv.slice(2).includes("--install-lifecycle");
const npmCliPath = process.env.npm_execpath;
if (!npmCliPath) {
  throw new Error("Run this smoke through npm so the exact npm CLI path is available.");
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd ?? repositoryRoot,
    encoding: "utf8",
    windowsHide: true,
    env: { ...process.env, NO_COLOR: "1", ...(options.env ?? {}) }
  });
  if (result.status !== 0) {
    throw new Error([
      `${command} ${args.join(" ")} failed with exit code ${String(result.status)}.`,
      result.stdout,
      result.stderr
    ].filter(Boolean).join("\n"));
  }
  return result;
}

function dependencyNames(tree, result = new Set()) {
  for (const [name, metadata] of Object.entries(tree?.dependencies ?? {})) {
    result.add(name);
    dependencyNames(metadata, result);
  }
  return result;
}

const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-pack-smoke-"));
let tarballPath = null;
try {
  const packed = run(process.execPath, [npmCliPath, "pack", "--json", "--ignore-scripts"]);
  const packResult = JSON.parse(packed.stdout);
  if (!Array.isArray(packResult) || packResult.length !== 1) {
    throw new Error("npm pack did not return exactly one root package.");
  }
  const tarballCandidates = [
    path.resolve(repositoryRoot, packResult[0].filename),
    path.join(
      repositoryRoot,
      `${packResult[0].name.replace(/^@/, "").replaceAll("/", "-")}-${packResult[0].version}.tgz`
    )
  ];
  for (const candidate of tarballCandidates) {
    try {
      await fs.access(candidate);
      tarballPath = candidate;
      break;
    } catch {
      // npm 8 reports a scoped filename while writing a sanitized basename.
    }
  }
  if (!tarballPath) throw new Error("Could not locate the npm pack tarball.");
  const packedPaths = new Set(packResult[0].files.map((entry) => entry.path.replaceAll("\\", "/")));
  for (const required of ["src/cli.js", "src/public-api.js", "web/dist/index.html"] ) {
    if (!packedPaths.has(required)) throw new Error(`Root tarball is missing ${required}.`);
  }
  for (const packedPath of packedPaths) {
    if (packedPath.startsWith("apps/")
        || packedPath.startsWith("packages/")
        || packedPath.startsWith("node_modules/")) {
      throw new Error(`Root tarball contains forbidden workspace/runtime content: ${packedPath}`);
    }
  }

  await fs.writeFile(path.join(tempRoot, "package.json"), JSON.stringify({
    name: "codex-provider-sync-pack-smoke",
    version: "0.0.0",
    private: true,
    scripts: { provider: "codex-provider" }
  }));
  const installArgs = [npmCliPath, "install", "--omit=dev"];
  if (installLifecycle) installArgs.push("--ignore-scripts=false");
  else installArgs.push("--ignore-scripts");
  installArgs.push(tarballPath);
  run(process.execPath, installArgs, { cwd: tempRoot });

  const help = run(process.execPath, [
    npmCliPath,
    "run",
    "--silent",
    "provider",
    "--",
    "help"
  ], { cwd: tempRoot });
  if (!`${help.stdout}\n${help.stderr}`.includes("codex-provider status")) {
    throw new Error(`Installed CLI help smoke failed. stdout=${JSON.stringify(help.stdout)} stderr=${JSON.stringify(help.stderr)}`);
  }

  const codexHome = path.join(tempRoot, "synthetic-codex-home");
  await fs.mkdir(path.join(codexHome, "sessions"), { recursive: true });
  await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
  await fs.mkdir(path.join(codexHome, "sqlite"), { recursive: true });
  await fs.writeFile(path.join(codexHome, "config.toml"), 'model_provider = "openai"\n');
  if (installLifecycle) {
    const stateDbPath = path.join(codexHome, "sqlite", "state_5.sqlite");
    const createDatabase = [
      'const Database = require("better-sqlite3");',
      'const database = new Database(process.env.PROVIDER_SYNC_SMOKE_DB);',
      'database.exec(`CREATE TABLE threads (',
      '  id TEXT PRIMARY KEY,',
      '  model_provider TEXT,',
      "  cwd TEXT NOT NULL DEFAULT '',",
      '  archived INTEGER NOT NULL DEFAULT 0,',
      "  first_user_message TEXT NOT NULL DEFAULT '',",
      '  model TEXT',
      ');`);',
      'database.prepare("INSERT INTO threads (id, model_provider, cwd, archived, first_user_message) VALUES (?, ?, ?, ?, ?)")',
      '  .run("synthetic-thread", "openai", "synthetic", 0, "synthetic");',
      'database.close();'
    ].join("\n");
    run(process.execPath, ["-e", createDatabase], {
      cwd: tempRoot,
      env: { PROVIDER_SYNC_SMOKE_DB: stateDbPath }
    });
  }
  const status = run(process.execPath, [
    npmCliPath,
    "run",
    "--silent",
    "provider",
    "--",
    "status",
    "--json",
    "--codex-home",
    codexHome
  ], { cwd: tempRoot });
  const statusEnvelope = JSON.parse(status.stdout);
  if (statusEnvelope.schemaVersion !== 1
      || statusEnvelope.command !== "status"
      || statusEnvelope.ok !== true) {
    throw new Error("Installed CLI JSON status smoke failed.");
  }
  if (installLifecycle
      && (statusEnvelope.result?.sqliteCounts === null
        || statusEnvelope.result?.sqliteCounts?.unreadable === true)) {
    throw new Error("Installed CLI did not open the synthetic SQLite database.");
  }

  const productionTree = JSON.parse(run(process.execPath, [npmCliPath, "ls", "--omit=dev", "--json"], {
    cwd: tempRoot
  }).stdout);
  const installedNames = dependencyNames(productionTree);
  for (const forbidden of ["react", "react-dom", "vite", "typescript", "electron"]) {
    if (installedNames.has(forbidden)) throw new Error(`Root production tree contains ${forbidden}.`);
  }

  process.stdout.write(
    `Root tarball ${installLifecycle ? "lifecycle + SQLite" : "content"} smoke passed on Node ${process.version}.\n`
  );
} finally {
  await fs.rm(tempRoot, { recursive: true, force: true });
  if (tarballPath) await fs.rm(tarballPath, { force: true });
}
