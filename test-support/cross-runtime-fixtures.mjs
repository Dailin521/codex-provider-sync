import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createRuntimeDifference, runFixtureInTemp } from "../packages/test-fixtures/src/index.js";
import { runRestore, runSync } from "../src/public-api.js";
import { openDatabase } from "../src/sqlite.js";
import {
  findPendingTransactions,
  readTransactionJournal,
  TRANSACTION_JOURNAL_BASENAME
} from "../src/transaction-journal.js";

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const staticRoot = path.join(repositoryRoot, "packages", "test-fixtures", "static");
const fixtureHostDll = process.env.CPS_DOTNET_FIXTURE_HOST
  ?? path.join(repositoryRoot, "desktop", "CodexProviderSync.Core.Tests", "FixtureHost", "bin", "Release", "net10.0", "CodexProviderSync.FixtureHost.dll");
const crashHostDll = process.env.CPS_DOTNET_CRASH_HOST
  ?? path.join(repositoryRoot, "desktop", "CodexProviderSync.Core.Tests", "CrashHost", "bin", "Release", "net10.0", "CodexProviderSync.CrashHost.dll");
const nodeCrashHost = path.join(repositoryRoot, "test-support", "cross-runtime-node-crash-host.mjs");

function digest(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function canonicalSqliteValue(value) {
  if (Buffer.isBuffer(value)) return ["blob", value.toString("base64")];
  if (typeof value === "bigint") return ["integer", value.toString()];
  return value;
}

async function rolloutFiles(root) {
  const result = [];
  async function visit(current) {
    let entries = [];
    try {
      entries = await fs.readdir(current, { withFileTypes: true });
    } catch (error) {
      if (error?.code === "ENOENT") return;
      throw error;
    }
    for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name))) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) await visit(fullPath);
      else if (entry.isFile() && entry.name.endsWith(".jsonl")) result.push(fullPath);
    }
  }
  await visit(root);
  return result;
}

async function canonicalState(codexHome) {
  const files = [];
  const configPath = path.join(codexHome, "config.toml");
  files.push(["config.toml", digest(await fs.readFile(configPath))]);
  for (const fileName of [".codex-global-state.json", ".codex-global-state.json.bak"]) {
    const filePath = path.join(codexHome, fileName);
    try {
      files.push([fileName, digest(await fs.readFile(filePath))]);
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
      files.push([fileName, null]);
    }
  }
  for (const scope of ["sessions", "archived_sessions"]) {
    for (const filePath of await rolloutFiles(path.join(codexHome, scope))) {
      files.push([
        path.relative(codexHome, filePath).replaceAll("\\", "/"),
        digest(await fs.readFile(filePath))
      ]);
    }
  }
  const database = await openDatabase(path.join(codexHome, "sqlite", "state_5.sqlite"), { readOnly: true });
  try {
    const integrity = database.prepare("PRAGMA integrity_check").get();
    assert.equal(integrity.integrity_check, "ok");
    const columns = database.prepare("PRAGMA table_info(threads)").all()
      .sort((left, right) => Number(left.cid) - Number(right.cid))
      .map((column) => String(column.name));
    const quotedColumns = columns
      .map((column) => `"${column.replaceAll('"', '""')}"`)
      .join(", ");
    const rows = database.prepare(`SELECT ${quotedColumns} FROM threads ORDER BY id`).all()
      .map((row) => columns.map((column) => canonicalSqliteValue(row[column])));
    const schema = database.prepare(`
      SELECT type, name, tbl_name, sql
      FROM sqlite_schema
      WHERE sql IS NOT NULL
      ORDER BY type, name
    `).all().map((row) => [row.type, row.name, row.tbl_name, row.sql]);
    const canonical = {
      files,
      sqlite: {
        schema,
        columns,
        rows,
        userVersion: Number(database.prepare("PRAGMA user_version").get().user_version)
      }
    };
    const providerIndex = columns.indexOf("model_provider");
    return {
      hash: digest(JSON.stringify(canonical)),
      provider: providerIndex >= 0 ? rows[0]?.[providerIndex] ?? null : null
    };
  } finally {
    database.close();
  }
}

async function createCase(fixture, name) {
  const caseRoot = path.join(fixture.root, "work", name);
  const codexHome = path.join(caseRoot, ".codex");
  await fs.mkdir(caseRoot, { recursive: true });
  await fs.cp(fixture.codexHome, codexHome, { recursive: true, force: false, errorOnExist: true });
  await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
  const seed = await fs.readFile(path.join(fixture.root, "sqlite-seed.sql"), "utf8");
  const database = await openDatabase(path.join(codexHome, "sqlite", "state_5.sqlite"));
  try {
    database.exec(seed);
  } finally {
    database.close();
  }
  return { codexHome, initial: await canonicalState(codexHome) };
}

function runProcess(command, args, { expectCrash = false } = {}) {
  const result = spawnSync(command, args, {
    cwd: repositoryRoot,
    encoding: "utf8",
    windowsHide: true,
    maxBuffer: 1024 * 1024,
    env: { ...process.env, NO_COLOR: "1" }
  });
  if (expectCrash) {
    assert.notEqual(result.status, 0, "The crash host unexpectedly completed successfully.");
    return null;
  }
  assert.equal(result.status, 0, [result.stdout, result.stderr].filter(Boolean).join("\n"));
  const line = result.stdout.trim().split(/\r?\n/).filter(Boolean).at(-1);
  const output = JSON.parse(line);
  assert.equal(output.schemaVersion, 1);
  assert.equal(output.ok, true);
  return output;
}

function runDotnet(operation, codexHome, argument) {
  return runProcess("dotnet", [fixtureHostDll, operation, codexHome, argument]);
}

async function managedBackupDirectories(codexHome) {
  const root = path.join(codexHome, "backups_state", "provider-sync");
  const entries = await fs.readdir(root, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isDirectory())
    .map((entry) => path.join(root, entry.name))
    .sort();
}

async function assertJournal(backupDir, expectedState) {
  const journal = await readTransactionJournal(path.join(backupDir, TRANSACTION_JOURNAL_BASENAME));
  assert.equal(journal.invalidTail, false);
  assert.equal(journal.terminal, true);
  assert.equal(journal.state, expectedState);
}

async function assertRestored(caseState, backupDir, expectedJournal) {
  const restored = await canonicalState(caseState.codexHome);
  assert.equal(restored.hash, caseState.initial.hash);
  assert.equal(restored.provider, "relay");
  assert.deepEqual(await findPendingTransactions(caseState.codexHome), []);
  await assertJournal(backupDir, expectedJournal);
}

test("bidirectional-backup-roundtrip uses one synthetic corpus across Node and .NET", async () => {
  await Promise.all([fs.access(fixtureHostDll), fs.access(crashHostDll)]);
  const fixtureRoot = path.join(staticRoot, "bidirectional-backup-roundtrip");
  const evidence = await runFixtureInTemp(fixtureRoot, async (fixture) => {
    const nodeToDotnet = await createCase(fixture, "node-to-dotnet");
    const nodeResult = await runSync({ codexHome: nodeToDotnet.codexHome, provider: "openai" });
    assert.notEqual((await canonicalState(nodeToDotnet.codexHome)).hash, nodeToDotnet.initial.hash);
    runDotnet("restore", nodeToDotnet.codexHome, nodeResult.backupDir);
    await assertRestored(nodeToDotnet, nodeResult.backupDir, fixture.manifest.expected.journalTerminal);

    const dotnetToNode = await createCase(fixture, "dotnet-to-node");
    const dotnetResult = runDotnet("sync", dotnetToNode.codexHome, "openai");
    assert.notEqual((await canonicalState(dotnetToNode.codexHome)).hash, dotnetToNode.initial.hash);
    await runRestore({ codexHome: dotnetToNode.codexHome, backupDir: dotnetResult.BackupDir });
    await assertRestored(dotnetToNode, dotnetResult.BackupDir, fixture.manifest.expected.journalTerminal);

    return createRuntimeDifference({
      fixtureId: fixture.manifest.id,
      status: "matched",
      node: { restored: true },
      dotnet: { restored: true },
      decision: "Both directed backup/restore paths match the synthetic canonical state."
    });
  });
  assert.equal(evidence.status, "matched");
});

test("foreign-pending-restore converges both crash directions to rolledBack", async () => {
  await Promise.all([fs.access(fixtureHostDll), fs.access(crashHostDll)]);
  const fixtureRoot = path.join(staticRoot, "foreign-pending-restore");
  const evidence = await runFixtureInTemp(fixtureRoot, async (fixture) => {
    const nodeToDotnet = await createCase(fixture, "node-pending-to-dotnet");
    runProcess(process.execPath, [nodeCrashHost, nodeToDotnet.codexHome], { expectCrash: true });
    const nodePending = await findPendingTransactions(nodeToDotnet.codexHome);
    assert.equal(nodePending.length, 1);
    assert.notEqual((await canonicalState(nodeToDotnet.codexHome)).hash, nodeToDotnet.initial.hash);
    runDotnet("restore", nodeToDotnet.codexHome, nodePending[0].backupDir);
    await assertRestored(nodeToDotnet, nodePending[0].backupDir, fixture.manifest.expected.journalTerminal);

    const dotnetToNode = await createCase(fixture, "dotnet-pending-to-node");
    runProcess("dotnet", [crashHostDll, dotnetToNode.codexHome], { expectCrash: true });
    const dotnetPending = await findPendingTransactions(dotnetToNode.codexHome);
    assert.equal(dotnetPending.length, 1);
    assert.notEqual((await canonicalState(dotnetToNode.codexHome)).hash, dotnetToNode.initial.hash);
    await runRestore({ codexHome: dotnetToNode.codexHome, backupDir: dotnetPending[0].backupDir });
    await assertRestored(dotnetToNode, dotnetPending[0].backupDir, fixture.manifest.expected.journalTerminal);

    assert.equal((await managedBackupDirectories(nodeToDotnet.codexHome)).length, 1);
    assert.equal((await managedBackupDirectories(dotnetToNode.codexHome)).length, 1);
    return createRuntimeDifference({
      fixtureId: fixture.manifest.id,
      status: "matched",
      node: { pendingRecovered: true },
      dotnet: { pendingRecovered: true },
      decision: "Both foreign pending journals reached a valid rolledBack terminal."
    });
  });
  assert.equal(evidence.status, "matched");
});
