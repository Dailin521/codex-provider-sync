import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { runRestore } from "../src/public-api.js";
import { defaultBackupRoot } from "../src/constants.js";

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const producerProject = path.join(
  repositoryRoot,
  "test-support",
  "HistoricalBackupProducer",
  "HistoricalBackupProducer.csproj"
);
const historicalTags = Object.freeze([
  {
    tag: "v0.2.9",
    expectedCommit: "1a2b290791a35d9cd29dba7c2fbacd324f1b9c72",
    metadataVersion: 1
  },
  {
    tag: "v0.4.1",
    expectedCommit: "75f45756cf732333e7c52f45c8cd1b183291a029",
    metadataVersion: 2
  }
]);

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd ?? repositoryRoot,
    env: { ...process.env, ...(options.env ?? {}) },
    encoding: "utf8",
    windowsHide: true,
    maxBuffer: 16 * 1024 * 1024,
    timeout: options.timeoutMs ?? 10 * 60_000
  });
  assert.equal(result.error, undefined, result.error?.message);
  assert.equal(result.status, 0, [result.stdout, result.stderr].filter(Boolean).join("\n"));
  return result.stdout.trim();
}

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

async function treeSha256(root, forbiddenMarker) {
  const records = [];
  async function visit(directory) {
    const entries = await fs.readdir(directory, { withFileTypes: true });
    entries.sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      const absolute = path.join(directory, entry.name);
      const relative = path.relative(root, absolute).replaceAll("\\", "/");
      assert.equal(entry.isSymbolicLink(), false, `historical backup contains a link: ${relative}`);
      if (entry.isDirectory()) await visit(absolute);
      else if (entry.isFile()) {
        const bytes = await fs.readFile(absolute);
        if (forbiddenMarker) {
          assert.equal(bytes.includes(Buffer.from(forbiddenMarker, "utf8")), false,
            `historical backup copied a forbidden synthetic credential marker: ${relative}`);
        }
        records.push([relative, sha256(bytes)]);
      }
      else assert.fail(`historical backup contains an unsupported entry: ${relative}`);
    }
  }
  await visit(root);
  return sha256(JSON.stringify(records));
}

function sqliteProvider(databasePath, nextProvider) {
  const database = new DatabaseSync(databasePath);
  try {
    if (nextProvider) {
      database.prepare("UPDATE threads SET model_provider = ? WHERE id = ?")
        .run(nextProvider, "historical-synthetic-thread");
    }
    return database.prepare("SELECT model_provider AS provider FROM threads WHERE id = ?")
      .get("historical-synthetic-thread").provider;
  } finally {
    database.close();
  }
}

async function createSyntheticHome(root) {
  const codexHome = path.join(root, "codex-home");
  const rolloutPath = path.join(
    codexHome,
    "sessions",
    "2026",
    "08",
    "28",
    "rollout-historical-synthetic.jsonl"
  );
  const databasePath = path.join(codexHome, "sqlite", "state_5.sqlite");
  const configPath = path.join(codexHome, "config.toml");
  const config = 'model_provider = "openai"\n';
  const authCanary = `HISTORICAL_AUTH_CANARY_${crypto.randomBytes(16).toString("hex")}`;
  const rollout = `${JSON.stringify({
    type: "session_meta",
    timestamp: "2026-08-28T00:00:00.000Z",
    payload: {
      id: "historical-synthetic-thread",
      cwd: "C:\\synthetic\\historical-tag",
      model_provider: "apigather"
    }
  })}\n`;
  await fs.mkdir(path.dirname(rolloutPath), { recursive: true });
  await fs.mkdir(path.dirname(databasePath), { recursive: true });
  await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
  await fs.writeFile(configPath, config, "utf8");
  await fs.writeFile(rolloutPath, rollout, "utf8");
  await fs.writeFile(path.join(codexHome, "auth.json"), `${JSON.stringify({ token: authCanary })}\n`, "utf8");
  const database = new DatabaseSync(databasePath);
  try {
    database.exec(`
      CREATE TABLE threads (
        id TEXT PRIMARY KEY,
        model_provider TEXT,
        cwd TEXT NOT NULL DEFAULT '',
        archived INTEGER NOT NULL DEFAULT 0,
        first_user_message TEXT NOT NULL DEFAULT '',
        model TEXT,
        has_user_event INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL DEFAULT 0
      );
    `);
    database.prepare(`
      INSERT INTO threads (
        id, model_provider, cwd, archived, first_user_message, model,
        has_user_event, updated_at, updated_at_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      "historical-synthetic-thread",
      "apigather",
      "C:\\synthetic\\historical-tag",
      0,
      "",
      null,
      0,
      1787875200,
      1787875200000
    );
  } finally {
    database.close();
  }
  return { codexHome, rolloutPath, databasePath, configPath, config, rollout, authCanary };
}

async function buildHistoricalCore(commit, tag, root) {
  const archive = path.join(root, `${tag}.tar`);
  const source = path.join(root, "source");
  const output = path.join(root, "core-output");
  await fs.mkdir(source, { recursive: true });
  run("git", ["archive", "--format=tar", `--output=${archive}`, commit]);
  run("tar", ["-xf", archive, "-C", source]);
  const project = path.join(source, "desktop", "CodexProviderSync.Core", "CodexProviderSync.Core.csproj");
  run("dotnet", [
    "publish",
    project,
    "--configuration",
    "Release",
    "--runtime",
    "win-x64",
    "--self-contained",
    "false",
    "--output",
    output,
    "--nologo",
    "-p:CopyLocalLockFileAssemblies=true"
  ]);
  return path.join(output, "CodexProviderSync.Core.dll");
}

test("current Node restores backups produced by historical repository tags", async (t) => {
  assert.equal(process.platform, "win32", "Historical tag producer evidence is a Windows CI fixture.");
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-historical-tag-backups-"));
  t.after(() => fs.rm(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 100 }));
  const producerOutput = path.join(root, "producer-output");
  run("dotnet", [
    "build",
    producerProject,
    "--configuration",
    "Release",
    "--output",
    producerOutput,
    "--nologo"
  ]);
  const producerDll = path.join(producerOutput, "HistoricalBackupProducer.dll");
  const evidence = [];

  for (const scenario of historicalTags) {
    const tagCommit = run("git", ["rev-parse", "--verify", `${scenario.tag}^{commit}`]);
    assert.equal(tagCommit, scenario.expectedCommit, `${scenario.tag} no longer resolves to its frozen commit`);
    const scenarioRoot = path.join(root, scenario.tag);
    const coreAssembly = await buildHistoricalCore(
      scenario.expectedCommit,
      scenario.tag,
      path.join(scenarioRoot, "tag")
    );
    const fixture = await createSyntheticHome(path.join(scenarioRoot, "fixture"));
    const produced = JSON.parse(run("dotnet", [producerDll, coreAssembly, fixture.codexHome]));
    assert.equal(produced.schemaVersion, 1);
    assert.equal(path.isAbsolute(produced.backupDir), true);
    const managedRoot = await fs.realpath(defaultBackupRoot(fixture.codexHome));
    const backupDir = await fs.realpath(produced.backupDir);
    assert.equal(
      process.platform === "win32" ? path.dirname(backupDir).toLowerCase() : path.dirname(backupDir),
      process.platform === "win32" ? managedRoot.toLowerCase() : managedRoot,
      `${scenario.tag} returned a backup outside the synthetic managed root`
    );
    const backupInfo = await fs.lstat(produced.backupDir);
    assert.equal(backupInfo.isDirectory(), true);
    assert.equal(backupInfo.isSymbolicLink(), false);
    const metadataBytes = await fs.readFile(path.join(produced.backupDir, "metadata.json"));
    const metadata = JSON.parse(metadataBytes);
    assert.equal(metadata.version, scenario.metadataVersion, scenario.tag);
    assert.equal(sqliteProvider(fixture.databasePath), "openai", scenario.tag);

    await fs.writeFile(fixture.configPath, 'model_provider = "relay"\n', "utf8");
    await fs.writeFile(fixture.rolloutPath, fixture.rollout.replace("apigather", "relay"), "utf8");
    assert.equal(sqliteProvider(fixture.databasePath, "relay"), "relay", scenario.tag);
    await runRestore({
      codexHome: fixture.codexHome,
      backupDir: produced.backupDir,
      restoreConfig: true,
      restoreDatabase: true,
      restoreSessions: true
    });
    assert.equal(await fs.readFile(fixture.configPath, "utf8"), fixture.config, scenario.tag);
    assert.equal(await fs.readFile(fixture.rolloutPath, "utf8"), fixture.rollout, scenario.tag);
    assert.equal(sqliteProvider(fixture.databasePath), "apigather", scenario.tag);

    evidence.push({
      tag: scenario.tag,
      tagCommit,
      metadataVersion: scenario.metadataVersion,
      coreAssemblyVersion: produced.coreAssemblyVersion,
      metadataSha256: sha256(metadataBytes),
      backupTreeSha256: await treeSha256(produced.backupDir, fixture.authCanary),
      syntheticOnly: true,
      currentNodeRestoreVerified: true,
      provenance: "repository-tag-source",
      limitation: "This is tag-produced evidence, not execution of a hosted formal Release binary."
    });
  }

  const reportPath = path.join(
    repositoryRoot,
    "artifacts",
    "test-fixtures",
    "historical-tag-backup-evidence.json"
  );
  await fs.mkdir(path.dirname(reportPath), { recursive: true });
  const report = `${JSON.stringify({
    schemaVersion: 1,
    containsRealUserData: false,
    generatedAt: new Date().toISOString(),
    runtime: {
      node: process.version,
      platform: process.platform,
      arch: process.arch,
      dotnet: run("dotnet", ["--version"])
    },
    evidence
  }, null, 2)}\n`;
  for (const scenario of historicalTags) {
    const canaryPath = path.join(root, scenario.tag, "fixture", "codex-home", "auth.json");
    const canary = JSON.parse(await fs.readFile(canaryPath, "utf8")).token;
    assert.equal(report.includes(canary), false);
  }
  await fs.writeFile(reportPath, report, "utf8");
});
