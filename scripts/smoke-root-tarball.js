import fs from "node:fs/promises";
import crypto from "node:crypto";
import http from "node:http";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
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

function containsPrivateStorageField(value) {
  if (Array.isArray(value)) {
    return value.some(containsPrivateStorageField);
  }
  if (!value || typeof value !== "object") {
    return false;
  }
  for (const [key, child] of Object.entries(value)) {
    if (["codexHome", "sqliteHome", "cwd"].includes(key)) {
      return true;
    }
    if (containsPrivateStorageField(child)) {
      return true;
    }
  }
  return false;
}

function reserveLoopbackPort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close();
        reject(new Error("Could not reserve a loopback port."));
        return;
      }
      server.close((error) => error ? reject(error) : resolve(address.port));
    });
  });
}

function requestPage(url, options = {}) {
  return new Promise((resolve, reject) => {
    const request = http.request(url, {
      method: options.method ?? "GET",
      headers: options.headers ?? {}
    }, (response) => {
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("end", () => resolve({
        status: response.statusCode,
        headers: response.headers,
        body: Buffer.concat(chunks).toString("utf8")
      }));
    });
    request.once("error", reject);
    request.end(options.body ?? null);
  });
}

async function smokeInstalledWeb(tempRoot, codexHome) {
  const port = await reserveLoopbackPort();
  const installedCliPath = path.join(
    tempRoot,
    "node_modules",
    "@dailin521",
    "codex-provider-sync",
    "src",
    "cli.js"
  );
  const child = spawn(process.execPath, [
    installedCliPath,
    "web",
    "--no-open",
    "--port",
    String(port),
    "--codex-home",
    codexHome
  ], {
    cwd: tempRoot,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, NO_COLOR: "1" }
  });
  let output = "";
  child.stdout.on("data", (chunk) => { output += chunk.toString("utf8"); });
  child.stderr.on("data", (chunk) => { output += chunk.toString("utf8"); });
  try {
    const deadline = Date.now() + 10_000;
    let health = null;
    while (Date.now() < deadline) {
      if (child.exitCode !== null) throw new Error(`Installed Web UI exited early.\n${output}`);
      try {
        health = await requestPage(`http://127.0.0.1:${port}/api/health`);
        if (health.status === 200) break;
      } catch {}
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    if (!health || health.status !== 200) throw new Error(`Installed Web UI did not become healthy.\n${output}`);
    const page = await requestPage(`http://127.0.0.1:${port}/`);
    if (page.status !== 200 || !page.body.includes("Codex Provider Sync")) {
      throw new Error("Installed Web UI did not serve the production application shell.");
    }
    if (!String(page.headers["content-security-policy"] ?? "").includes("style-src 'self'")) {
      throw new Error("Installed Web UI did not serve the strict production CSP.");
    }
    const pairingUrl = output.match(/One-time pairing link:\s+(\S+)/)?.[1];
    if (!pairingUrl) throw new Error("Installed Web UI did not emit a one-time pairing link.");
    const pairingToken = decodeURIComponent(new URL(pairingUrl).hash.replace(/^#pair=/, ""));
    const origin = `http://127.0.0.1:${port}`;
    const paired = await requestPage(`${origin}/api/pair`, {
      method: "POST",
      headers: { Origin: origin, "X-Codex-Provider-Pairing": pairingToken }
    });
    const pairedPayload = JSON.parse(paired.body);
    if (paired.status !== 200 || typeof pairedPayload.deviceCredential !== "string") {
      throw new Error("Installed Web UI pairing smoke failed.");
    }
    const authenticatedHeaders = {
      Origin: origin,
      "X-Codex-Provider-Device": pairedPayload.deviceCredential
    };
    const profiles = await requestPage(`${origin}/api/profiles`, { headers: authenticatedHeaders });
    const profilesPayload = JSON.parse(profiles.body);
    const profile = profilesPayload.profiles?.find((entry) => entry.id === "default");
    if (profiles.status !== 200 || !profile?.revision) {
      throw new Error("Installed Web UI profile smoke failed.");
    }
    const coreHeaders = { ...authenticatedHeaders, "Content-Type": "application/json" };
    const statusRequest = {
      protocolVersion: 1,
      requestId: crypto.randomUUID(),
      method: "getStatus",
      payload: { profile: { profileId: profile.id, profileRevision: profile.revision } }
    };
    const coreStatus = await requestPage(`${origin}/api/core`, {
      method: "POST",
      headers: coreHeaders,
      body: JSON.stringify(statusRequest)
    });
    const coreStatusPayload = JSON.parse(coreStatus.body);
    const serializedStatus = JSON.stringify(coreStatusPayload);
    if (coreStatus.status !== 200 || coreStatusPayload.ok !== true) {
      throw new Error("Installed Web UI Core facade smoke failed.");
    }
    if (containsPrivateStorageField(coreStatusPayload) || serializedStatus.includes(codexHome)) {
      throw new Error("Installed Web UI Core facade exposed a private storage path.");
    }
    const staleStatus = await requestPage(`${origin}/api/core`, {
      method: "POST",
      headers: coreHeaders,
      body: JSON.stringify({
        ...statusRequest,
        requestId: crypto.randomUUID(),
        payload: {
          profile: {
            profileId: profile.id,
            profileRevision: `${profile.revision}-stale`
          }
        }
      })
    });
    const stalePayload = JSON.parse(staleStatus.body);
    const serializedError = JSON.stringify(stalePayload);
    if (staleStatus.status !== 409
        || stalePayload.ok !== false
        || stalePayload.error?.code !== "PROFILE_CHANGED"
        || serializedError.includes(codexHome)) {
      throw new Error("Installed Web UI Core error redaction smoke failed.");
    }
  } finally {
    child.kill();
    await Promise.race([
      new Promise((resolve) => child.once("exit", resolve)),
      new Promise((resolve) => setTimeout(resolve, 3000))
    ]);
  }
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
  for (const required of [
    "src/cli.js",
    "src/public-api.js",
    "src/web-core-adapter.js",
    "packages/contracts/dist/index.js",
    "packages/core/src/index.js",
    "web/dist/index.html"
  ] ) {
    if (!packedPaths.has(required)) throw new Error(`Root tarball is missing ${required}.`);
  }
  for (const packedPath of packedPaths) {
    if (packedPath.endsWith(".map")) {
      throw new Error(`Root tarball contains a production source map: ${packedPath}`);
    }
    const allowedExact = new Set([
      "AGENTS.md",
      "CHANGELOG.md",
      "CONTRIBUTING.md",
      "CONTRIBUTORS.md",
      "LICENSE",
      "README.md",
      "package.json"
    ]);
    const allowedPrefix = [
      "docs/",
      "images/README/",
      "packages/contracts/dist/",
      "packages/core/src/",
      "src/",
      "web/dist/"
    ];
    if (!allowedExact.has(packedPath)
        && !allowedPrefix.some((prefix) => packedPath.startsWith(prefix))) {
      throw new Error(`Root tarball contains an unapproved path: ${packedPath}`);
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
      'let Database;',
      'try {',
      '  Database = require("node:sqlite").DatabaseSync;',
      '} catch (error) {',
      '  if (!["ERR_UNKNOWN_BUILTIN_MODULE", "MODULE_NOT_FOUND"].includes(error?.code)) throw error;',
      '  Database = require("better-sqlite3");',
      '}',
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
  if (installLifecycle) await smokeInstalledWeb(tempRoot, codexHome);

  const productionTree = JSON.parse(run(process.execPath, [npmCliPath, "ls", "--omit=dev", "--json"], {
    cwd: tempRoot
  }).stdout);
  const installedNames = dependencyNames(productionTree);
  for (const forbidden of ["react", "react-dom", "vite", "typescript", "electron"]) {
    if (installedNames.has(forbidden)) throw new Error(`Root production tree contains ${forbidden}.`);
  }
  for (const name of installedNames) {
    if (name.startsWith("electron-")) throw new Error(`Root production tree contains ${name}.`);
  }

  process.stdout.write(
    `Root tarball ${installLifecycle ? "lifecycle + SQLite" : "content"} smoke passed on Node ${process.version}.\n`
  );
} finally {
  await fs.rm(tempRoot, { recursive: true, force: true });
  if (tarballPath) await fs.rm(tarballPath, { force: true });
}
