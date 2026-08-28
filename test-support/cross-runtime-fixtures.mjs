import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createRuntimeDifference, runFixtureInTemp } from "../packages/test-fixtures/src/index.js";
import { runRestore, runSync } from "../src/public-api.js";
import {
  captureRestoreSourceIdentity,
  RESTORE_SNAPSHOT_MANIFEST_BASENAME
} from "../src/restore-v2.js";
import {
  readRestoreJournal,
  RESTORE_JOURNAL_BASENAME
} from "../src/restore-journal.js";
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
const nodeWriterHost = path.join(repositoryRoot, "test-support", "cross-runtime-writer-host.mjs");
const nodeRestoreCrashHost = path.join(repositoryRoot, "test-support", "restore-v2-crash-host.mjs");
const ordinalCompare = (left, right) => left < right ? -1 : left > right ? 1 : 0;
const physicalPathKey = (value) => process.platform === "win32"
  ? path.resolve(value).toLowerCase()
  : path.resolve(value);

function windowsShortDirectoryPath(directory) {
  assert.equal(process.platform, "win32");
  const executable = path.join(
    process.env.SystemRoot ?? "C:\\Windows",
    "System32",
    "WindowsPowerShell",
    "v1.0",
    "powershell.exe"
  );
  const command = [
    "$fso = New-Object -ComObject Scripting.FileSystemObject",
    "$folder = $fso.GetFolder($env:CPS_SHORT_PATH_TARGET)",
    "$folder.ShortPath"
  ].join("; ");
  const result = spawnSync(executable, [
    "-NoLogo",
    "-NoProfile",
    "-NonInteractive",
    "-Command",
    command
  ], {
    cwd: repositoryRoot,
    encoding: "utf8",
    windowsHide: true,
    env: { ...process.env, CPS_SHORT_PATH_TARGET: directory }
  });
  assert.equal(result.error, undefined, result.error?.message);
  assert.equal(result.status, 0, [result.stdout, result.stderr].filter(Boolean).join("\n"));
  const shortPath = result.stdout.trim();
  assert.equal(path.isAbsolute(shortPath), true, "PowerShell must return an absolute 8.3 alias");
  assert.notEqual(
    physicalPathKey(shortPath),
    physicalPathKey(directory),
    "The Windows volume must expose an actual short-path alias for this fixture"
  );
  return shortPath;
}

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

async function canonicalState(
  codexHome,
  databasePath = path.join(codexHome, "sqlite", "state_5.sqlite")
) {
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
  const sidecars = [];
  for (const suffix of ["-wal", "-shm"]) {
    try {
      sidecars.push([suffix, digest(await fs.readFile(`${databasePath}${suffix}`))]);
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
      sidecars.push([suffix, null]);
    }
  }
  const database = await openDatabase(databasePath, { readOnly: true });
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
        userVersion: Number(database.prepare("PRAGMA user_version").get().user_version),
        sidecars
      }
    };
    const providerIndex = columns.indexOf("model_provider");
    return {
      hash: digest(JSON.stringify(canonical)),
      provider: providerIndex >= 0 ? rows[0]?.[providerIndex] ?? null : null,
      canonical
    };
  } finally {
    database.close();
  }
}

async function managedBackupTree(codexHome) {
  const root = path.join(codexHome, "backups_state", "provider-sync");
  const entries = [];
  async function visit(current) {
    let children = [];
    try {
      children = await fs.readdir(current, { withFileTypes: true });
    } catch (error) {
      if (error?.code === "ENOENT") return;
      throw error;
    }
    for (const child of children.sort((left, right) => ordinalCompare(left.name, right.name))) {
      const fullPath = path.join(current, child.name);
      const relativePath = path.relative(root, fullPath).replaceAll("\\", "/");
      if (child.isDirectory()) {
        entries.push([relativePath, "directory"]);
        await visit(fullPath);
      } else if (child.isFile()) {
        entries.push([relativePath, "file", digest(await fs.readFile(fullPath))]);
      } else {
        entries.push([relativePath, "unsupported"]);
      }
    }
  }
  await visit(root);
  return entries;
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
    timeout: 60_000,
    maxBuffer: 1024 * 1024,
    env: { ...process.env, NO_COLOR: "1" }
  });
  assert.equal(
    result.error,
    undefined,
    [result.error?.message, result.stdout, result.stderr].filter(Boolean).join("\n")
  );
  if (expectCrash) {
    if (command === process.execPath) {
      assert.equal(
        result.status,
        86,
        ["The Node crash host did not reach its exact fault point.", result.stdout, result.stderr]
          .filter(Boolean)
          .join("\n")
      );
    } else {
      assert.notEqual(result.status, 0, "The crash host unexpectedly completed successfully.");
    }
    return result;
  }
  assert.equal(result.status, 0, [result.stdout, result.stderr].filter(Boolean).join("\n"));
  const line = result.stdout.trim().split(/\r?\n/).filter(Boolean).at(-1);
  const output = JSON.parse(line);
  assert.equal(output.schemaVersion, 1);
  assert.equal(output.ok, true);
  return output;
}

function runProcessFailure(command, args, expectedCode = "RECOVERY_REQUIRED") {
  const result = spawnSync(command, args, {
    cwd: repositoryRoot,
    encoding: "utf8",
    windowsHide: true,
    timeout: 60_000,
    maxBuffer: 1024 * 1024,
    env: { ...process.env, NO_COLOR: "1" }
  });
  assert.equal(
    result.error,
    undefined,
    [result.error?.message, result.stdout, result.stderr].filter(Boolean).join("\n")
  );
  assert.notEqual(result.status, 0, "The fixture operation unexpectedly completed successfully.");
  const line = result.stderr.trim().split(/\r?\n/).filter(Boolean).at(-1);
  assert.ok(line, result.stdout);
  const output = JSON.parse(line);
  assert.equal(output.schemaVersion, 1);
  assert.equal(output.ok, false);
  assert.equal(output.errorCode, expectedCode);
  return output;
}

function startProcess(command, args) {
  const child = spawn(command, args, {
    cwd: repositoryRoot,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, NO_COLOR: "1" }
  });
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => { stdout += chunk; });
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  const completed = new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (status, signal) => resolve({ status, signal, stdout, stderr }));
  });
  return { child, completed };
}

async function finishProcess(runner) {
  const result = await runner.completed;
  assert.equal(
    result.status,
    0,
    [
      `The gated writer exited with status ${result.status ?? "null"} and signal ${result.signal ?? "none"}.`,
      result.stdout,
      result.stderr
    ].filter(Boolean).join("\n")
  );
  const line = result.stdout.trim().split(/\r?\n/).filter(Boolean).at(-1);
  const output = JSON.parse(line);
  assert.equal(output.schemaVersion, 1);
  assert.equal(output.ok, true);
  return output;
}

async function waitForPath(filePath, timeoutMs = 20_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      await fs.access(filePath);
      return;
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
    await delay(25);
  }
  assert.fail(`Timed out waiting for gated writer marker: ${filePath}`);
}

async function releaseAndFinish(runner, releasePath) {
  await fs.writeFile(releasePath, "release\n", { flag: "wx" }).catch((error) => {
    if (error?.code !== "EEXIST") throw error;
  });
  return finishProcess(runner);
}

function runDotnet(operation, codexHome, argument) {
  return runProcess("dotnet", [fixtureHostDll, operation, codexHome, argument]);
}

function runDotnetFailure(operation, codexHome, argument, expectedCode = "RECOVERY_REQUIRED") {
  return runProcessFailure(
    "dotnet",
    [fixtureHostDll, operation, codexHome, argument],
    expectedCode
  );
}

async function createSharedStateDbCase(fixture, name) {
  const nodeHome = await createCase(fixture, `${name}-node-home`);
  const dotnetHome = await createCase(fixture, `${name}-dotnet-home`);
  const sharedSqliteHome = path.join(fixture.root, "work", name, "shared-sqlite");
  const sharedStateDb = path.join(sharedSqliteHome, "state_5.sqlite");
  await fs.mkdir(sharedSqliteHome, { recursive: true });
  await fs.copyFile(
    path.join(nodeHome.codexHome, "sqlite", "state_5.sqlite"),
    sharedStateDb
  );
  return { nodeHome, dotnetHome, sharedSqliteHome, sharedStateDb };
}

async function writeUnknownRestoreJournal(codexHome, sourceBackupDir, suffix) {
  const snapshotDir = path.join(
    codexHome,
    "backups_state",
    "provider-sync",
    `restore-v2-unknown-${suffix}`
  );
  await fs.mkdir(snapshotDir, { recursive: true });
  const journalPath = path.join(snapshotDir, RESTORE_JOURNAL_BASENAME);
  const event = {
    schemaVersion: 99,
    protocolVersion: 99,
    operationKind: "restore",
    operationId: `unknown-${suffix}`,
    sequence: 1,
    state: "prepared",
    recordedAt: "2026-08-27T00:00:00.000Z",
    sourceBackup: {
      backupId: path.basename(sourceBackupDir),
      backupDir: path.resolve(sourceBackupDir),
      revision: "unknown-schema-source"
    },
    preRestoreSnapshot: {
      backupId: path.basename(snapshotDir),
      backupDir: path.resolve(snapshotDir),
      revision: "unknown-schema-snapshot",
      manifestSha256: "unknown-schema-manifest"
    }
  };
  const raw = `${JSON.stringify(event)}\n`;
  await fs.writeFile(journalPath, raw, "utf8");
  return { journalPath, raw };
}

async function mismatchManifestPreparedBinding(journal) {
  const manifestPath = path.join(journal.snapshotDir, RESTORE_SNAPSHOT_MANIFEST_BASENAME);
  const manifest = JSON.parse(await fs.readFile(manifestPath, "utf8"));
  manifest.sourceBackup.revision = `${manifest.sourceBackup.revision}-mismatched`;
  const manifestText = `${JSON.stringify(manifest, null, 2)}\n`;
  const manifestSha256 = crypto.createHash("sha256")
    .update(manifestText, "utf8")
    .digest("base64url");
  await fs.writeFile(manifestPath, manifestText, "utf8");
  const lines = (await fs.readFile(journal.filePath, "utf8")).trimEnd().split(/\r?\n/);
  const prepared = JSON.parse(lines[0]);
  prepared.preRestoreSnapshot.manifestSha256 = manifestSha256;
  lines[0] = JSON.stringify(prepared);
  await fs.writeFile(journal.filePath, `${lines.join("\n")}\n`, "utf8");
}

async function mismatchPreparedPhysicalHome(journal, physicalHome) {
  const lines = (await fs.readFile(journal.filePath, "utf8")).trimEnd().split(/\r?\n/);
  const prepared = JSON.parse(lines[0]);
  prepared.storage.codexHomePhysical = path.resolve(await fs.realpath(physicalHome));
  lines[0] = JSON.stringify(prepared);
  await fs.writeFile(journal.filePath, `${lines.join("\n")}\n`, "utf8");
}

async function managedBackupDirectories(codexHome) {
  const root = path.join(codexHome, "backups_state", "provider-sync");
  const entries = await fs.readdir(root, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isDirectory())
    .map((entry) => path.join(root, entry.name))
    .sort(ordinalCompare);
}

async function restoreJournals(codexHome) {
  const directories = await managedBackupDirectories(codexHome);
  const journals = [];
  for (const directory of directories) {
    try {
      journals.push(await readRestoreJournal(path.join(directory, RESTORE_JOURNAL_BASENAME)));
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
  }
  return journals.sort((left, right) => ordinalCompare(left.filePath, right.filePath));
}

async function assertJournal(backupDir, expectedState) {
  const journal = await readTransactionJournal(path.join(backupDir, TRANSACTION_JOURNAL_BASENAME));
  assert.equal(journal.invalidTail, false);
  assert.equal(journal.terminal, true);
  assert.equal(journal.state, expectedState);
}

async function assertRestored(caseState, backupDir, expectedJournal) {
  const restored = await canonicalState(caseState.codexHome);
  assert.deepEqual(restored.canonical, caseState.initial.canonical);
  assert.equal(restored.provider, "relay");
  const pending = await findPendingTransactions(caseState.codexHome);
  const restoreEvidence = pending.length > 0
    ? (await restoreJournals(caseState.codexHome)).map((journal) => ({
        operationId: journal.operationId,
        state: journal.state,
        invalidTail: journal.invalidTail,
        validationError: journal.validationError,
        resolvesOperationIds: journal.prepared?.resolvesOperationIds ?? [],
        sourceBackup: journal.prepared?.sourceBackup ?? null,
        codexHome: journal.prepared?.storage?.codexHome ?? null,
        requiredTargetKinds: journal.prepared?.requiredTargetKinds ?? []
      }))
    : [];
  assert.equal(
    pending.length,
    0,
    JSON.stringify({
      pending: pending.map((journal) => ({
        operationId: journal.operationId ?? null,
        operationKind: journal.operationKind,
        state: journal.state,
        invalidTail: journal.invalidTail ?? false,
        validationError: journal.validationError ?? null
      })),
      restoreEvidence
    })
  );
  await assertJournal(backupDir, expectedJournal);
}

async function assertCompletedRestoreEvidence({
  result,
  codexHome,
  sourceBackupDir,
  expectedSourceRevision,
  journals,
  expectedResolvedOperationIds = []
}) {
  const operationId = result.restoreOperationId ?? result.RestoreOperationId;
  const snapshotId = result.preRestoreSnapshotId ?? result.PreRestoreSnapshotId;
  const journalState = result.restoreJournalState ?? result.RestoreJournalState;
  assert.equal(typeof operationId, "string");
  assert.equal(typeof snapshotId, "string");
  assert.equal(journalState, "completed");
  const matching = journals.filter((journal) => journal.operationId === operationId);
  assert.equal(matching.length, 1, "Restore result must bind to exactly one completed journal.");
  const [journal] = matching;
  assert.equal(journal.invalidTail, false);
  assert.equal(journal.state, "completed");
  assert.equal(journal.prepared.preRestoreSnapshot.backupId, snapshotId);
  assert.equal(
    physicalPathKey(await fs.realpath(journal.prepared.sourceBackup.backupDir)),
    physicalPathKey(await fs.realpath(sourceBackupDir))
  );
  assert.equal(
    journal.prepared.sourceBackup.revision,
    expectedSourceRevision
  );
  assert.equal(
    physicalPathKey(await fs.realpath(journal.prepared.storage.codexHome)),
    physicalPathKey(await fs.realpath(codexHome))
  );
  assert.equal(
    physicalPathKey(journal.prepared.storage.codexHomePhysical),
    physicalPathKey(await fs.realpath(codexHome))
  );
  assert.equal(
    physicalPathKey(await fs.realpath(journal.prepared.preRestoreSnapshot.backupDir)),
    physicalPathKey(await fs.realpath(journal.snapshotDir))
  );
  await Promise.all([
    fs.access(journal.snapshotDir),
    fs.access(path.join(journal.snapshotDir, RESTORE_SNAPSHOT_MANIFEST_BASENAME)),
    fs.access(path.join(journal.snapshotDir, RESTORE_JOURNAL_BASENAME))
  ]);
  assert.deepEqual(
    journal.prepared.resolvesOperationIds,
    expectedResolvedOperationIds
  );
}

test("bidirectional-backup-roundtrip uses one synthetic corpus across Node and .NET", async () => {
  await Promise.all([fs.access(fixtureHostDll), fs.access(crashHostDll)]);
  const fixtureRoot = path.join(staticRoot, "bidirectional-backup-roundtrip");
  const evidence = await runFixtureInTemp(fixtureRoot, async (fixture) => {
    const nodeToDotnet = await createCase(fixture, "node-to-dotnet");
    const nodeResult = await runSync({ codexHome: nodeToDotnet.codexHome, provider: "openai" });
    const nodeIdentity = await captureRestoreSourceIdentity(nodeResult.backupDir);
    const dotnetIdentity = runDotnet("source-identity", nodeToDotnet.codexHome, nodeResult.backupDir);
    assert.equal(dotnetIdentity.Revision, nodeIdentity);
    assert.notEqual((await canonicalState(nodeToDotnet.codexHome)).hash, nodeToDotnet.initial.hash);
    const dotnetRestore = runDotnet("restore-v2", nodeToDotnet.codexHome, nodeResult.backupDir);
    assert.equal(dotnetRestore.RestoreVersion, 2);
    assert.equal(dotnetRestore.RestoreJournalState, "completed");
    await assertRestored(nodeToDotnet, nodeResult.backupDir, fixture.manifest.expected.journalTerminal);
    const nodeToDotnetJournals = await restoreJournals(nodeToDotnet.codexHome);
    await assertCompletedRestoreEvidence({
      result: dotnetRestore,
      codexHome: nodeToDotnet.codexHome,
      sourceBackupDir: nodeResult.backupDir,
      expectedSourceRevision: nodeIdentity,
      journals: nodeToDotnetJournals
    });

    const dotnetToNode = await createCase(fixture, "dotnet-to-node");
    const dotnetResult = runDotnet("sync", dotnetToNode.codexHome, "openai");
    const dotnetSourceIdentity = runDotnet(
      "source-identity",
      dotnetToNode.codexHome,
      dotnetResult.BackupDir
    );
    assert.equal(
      dotnetSourceIdentity.Revision,
      await captureRestoreSourceIdentity(dotnetResult.BackupDir)
    );
    assert.notEqual((await canonicalState(dotnetToNode.codexHome)).hash, dotnetToNode.initial.hash);
    const nodeRestore = await runRestore({
      codexHome: dotnetToNode.codexHome,
      backupDir: dotnetResult.BackupDir
    });
    assert.equal(nodeRestore.restoreVersion, 2);
    assert.equal(nodeRestore.restoreJournalState, "completed");
    await assertRestored(dotnetToNode, dotnetResult.BackupDir, fixture.manifest.expected.journalTerminal);
    const dotnetToNodeJournals = await restoreJournals(dotnetToNode.codexHome);
    await assertCompletedRestoreEvidence({
      result: nodeRestore,
      codexHome: dotnetToNode.codexHome,
      sourceBackupDir: dotnetResult.BackupDir,
      expectedSourceRevision: dotnetSourceIdentity.Revision,
      journals: dotnetToNodeJournals
    });

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

test("actual Node and .NET writers contend across different Homes sharing one State DB", async () => {
  await Promise.all([fs.access(fixtureHostDll), fs.access(nodeWriterHost)]);
  const fixtureRoot = path.join(staticRoot, "bidirectional-backup-roundtrip");
  const evidence = await runFixtureInTemp(fixtureRoot, async (fixture) => {
    const nodeWinnerCase = await createSharedStateDbCase(fixture, "node-winner");
    const nodeReadyPath = path.join(fixture.root, "work", "node-winner.ready");
    const nodeReleasePath = path.join(fixture.root, "work", "node-winner.release");
    const nodeWinner = startProcess(process.execPath, [
      nodeWriterHost,
      nodeWinnerCase.nodeHome.codexHome,
      "openai",
      nodeWinnerCase.sharedSqliteHome,
      nodeReadyPath,
      nodeReleasePath
    ]);
    try {
      await waitForPath(nodeReadyPath);
      const loserBefore = await canonicalState(
        nodeWinnerCase.dotnetHome.codexHome,
        nodeWinnerCase.sharedStateDb
      );
      const loserBackupsBefore = await managedBackupTree(nodeWinnerCase.dotnetHome.codexHome);
      const failure = runProcessFailure(
        "dotnet",
        [
          fixtureHostDll,
          "sync-explicit",
          nodeWinnerCase.dotnetHome.codexHome,
          "openai",
          nodeWinnerCase.sharedSqliteHome
        ],
        "OPERATION_BUSY"
      );
      assert.equal(failure.busyScope, "state-db");
      assert.deepEqual(
        (await canonicalState(
          nodeWinnerCase.dotnetHome.codexHome,
          nodeWinnerCase.sharedStateDb
        )).canonical,
        loserBefore.canonical
      );
      assert.deepEqual(
        await managedBackupTree(nodeWinnerCase.dotnetHome.codexHome),
        loserBackupsBefore
      );
      assert.equal((await findPendingTransactions(nodeWinnerCase.dotnetHome.codexHome)).length, 0);
    } finally {
      await releaseAndFinish(nodeWinner, nodeReleasePath);
    }
    assert.equal(
      (await canonicalState(nodeWinnerCase.nodeHome.codexHome, nodeWinnerCase.sharedStateDb)).provider,
      "openai"
    );

    const dotnetWinnerCase = await createSharedStateDbCase(fixture, "dotnet-winner");
    const dotnetReadyPath = path.join(fixture.root, "work", "dotnet-winner.ready");
    const dotnetReleasePath = path.join(fixture.root, "work", "dotnet-winner.release");
    const dotnetWinner = startProcess("dotnet", [
      fixtureHostDll,
      "sync-gated",
      dotnetWinnerCase.dotnetHome.codexHome,
      "openai",
      dotnetWinnerCase.sharedSqliteHome,
      dotnetReadyPath,
      dotnetReleasePath
    ]);
    try {
      await waitForPath(dotnetReadyPath);
      const loserBefore = await canonicalState(
        dotnetWinnerCase.nodeHome.codexHome,
        dotnetWinnerCase.sharedStateDb
      );
      const loserBackupsBefore = await managedBackupTree(dotnetWinnerCase.nodeHome.codexHome);
      await assert.rejects(
        () => runSync({
          codexHome: dotnetWinnerCase.nodeHome.codexHome,
          provider: "openai",
          sqliteHome: dotnetWinnerCase.sharedSqliteHome
        }),
        (error) => error?.code === "OPERATION_BUSY"
          && error?.details?.busyScope === "state-db"
      );
      assert.deepEqual(
        (await canonicalState(
          dotnetWinnerCase.nodeHome.codexHome,
          dotnetWinnerCase.sharedStateDb
        )).canonical,
        loserBefore.canonical
      );
      assert.deepEqual(
        await managedBackupTree(dotnetWinnerCase.nodeHome.codexHome),
        loserBackupsBefore
      );
      assert.equal((await findPendingTransactions(dotnetWinnerCase.nodeHome.codexHome)).length, 0);
    } finally {
      await releaseAndFinish(dotnetWinner, dotnetReleasePath);
    }
    assert.equal(
      (await canonicalState(dotnetWinnerCase.dotnetHome.codexHome, dotnetWinnerCase.sharedStateDb)).provider,
      "openai"
    );

    return createRuntimeDifference({
      fixtureId: "shared-sqlite-home-actual-writer-contention",
      status: "matched",
      node: {
        heldStateDbLockAgainstDotnetWriter: true,
        rejectedByDotnetWriter: true
      },
      dotnet: {
        heldStateDbLockAgainstNodeWriter: true,
        rejectedByNodeWriter: true
      },
      decision: "Actual Node and .NET Sync writers both fail the losing Home before Backup, Journal, or business mutation."
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

    assert.equal((await managedBackupDirectories(nodeToDotnet.codexHome)).length, 2);
    assert.equal((await managedBackupDirectories(dotnetToNode.codexHome)).length, 2);
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

test("Restore v2 crash recovery is bidirectional across Node and .NET", async () => {
  await Promise.all([fs.access(fixtureHostDll), fs.access(crashHostDll)]);
  const fixtureRoot = path.join(staticRoot, "bidirectional-backup-roundtrip");
  const evidence = await runFixtureInTemp(fixtureRoot, async (fixture) => {
    const nodeToDotnet = await createCase(fixture, "node-restore-crash-to-dotnet");
    const nodeSource = await runSync({ codexHome: nodeToDotnet.codexHome, provider: "openai" });
    const nodeSourceRevision = await captureRestoreSourceIdentity(nodeSource.backupDir);
    runProcess(
      process.execPath,
      [
        nodeRestoreCrashHost,
        nodeToDotnet.codexHome,
        nodeSource.backupDir,
        "after_restore_target_write_before_complete",
        "--with-database"
      ],
      { expectCrash: true }
    );
    assert.equal((await findPendingTransactions(nodeToDotnet.codexHome)).length, 1);
    const nodeCrashJournals = await restoreJournals(nodeToDotnet.codexHome);
    assert.equal(nodeCrashJournals.length, 1);
    assert.equal(nodeCrashJournals[0].state, "applying");
    assert.equal([...nodeCrashJournals[0].targetPhases.values()].includes("intent"), true);
    const recoveredByDotnet = runDotnet(
      "restore-v2",
      nodeToDotnet.codexHome,
      nodeSource.backupDir
    );
    assert.equal(recoveredByDotnet.RestoreJournalState, "completed");
    assert.equal(recoveredByDotnet.ResolvedOperationIds.length, 1);
    await assertRestored(nodeToDotnet, nodeSource.backupDir, fixture.manifest.expected.journalTerminal);
    const nodeOriginJournals = await restoreJournals(nodeToDotnet.codexHome);
    assert.equal(nodeOriginJournals.length, 2);
    assert.equal(nodeOriginJournals.filter((journal) => journal.state === "applying").length, 1);
    assert.equal(nodeOriginJournals.filter((journal) => journal.state === "completed").length, 1);
    await assertCompletedRestoreEvidence({
      result: recoveredByDotnet,
      codexHome: nodeToDotnet.codexHome,
      sourceBackupDir: nodeSource.backupDir,
      expectedSourceRevision: nodeSourceRevision,
      journals: nodeOriginJournals,
      expectedResolvedOperationIds: [nodeCrashJournals[0].operationId]
    });

    const dotnetToNode = await createCase(fixture, "dotnet-restore-crash-to-node");
    const dotnetSource = runDotnet("sync", dotnetToNode.codexHome, "openai");
    const dotnetSourceRevision = await captureRestoreSourceIdentity(dotnetSource.BackupDir);
    runProcess(
      "dotnet",
      [
        crashHostDll,
        "restore-v2",
        dotnetToNode.codexHome,
        dotnetSource.BackupDir,
        "after_restore_target_write_before_complete"
      ],
      { expectCrash: true }
    );
    assert.equal((await findPendingTransactions(dotnetToNode.codexHome)).length, 1);
    const dotnetCrashJournals = await restoreJournals(dotnetToNode.codexHome);
    assert.equal(dotnetCrashJournals.length, 1);
    assert.equal(dotnetCrashJournals[0].state, "applying");
    assert.equal([...dotnetCrashJournals[0].targetPhases.values()].includes("intent"), true);
    const recoveredByNode = await runRestore({
      codexHome: dotnetToNode.codexHome,
      backupDir: dotnetSource.BackupDir
    });
    assert.equal(recoveredByNode.restoreJournalState, "completed");
    assert.equal(recoveredByNode.resolvedOperationIds.length, 1);
    await assertRestored(dotnetToNode, dotnetSource.BackupDir, fixture.manifest.expected.journalTerminal);
    const dotnetOriginJournals = await restoreJournals(dotnetToNode.codexHome);
    assert.equal(dotnetOriginJournals.length, 2);
    assert.equal(dotnetOriginJournals.filter((journal) => journal.state === "applying").length, 1);
    assert.equal(dotnetOriginJournals.filter((journal) => journal.state === "completed").length, 1);
    await assertCompletedRestoreEvidence({
      result: recoveredByNode,
      codexHome: dotnetToNode.codexHome,
      sourceBackupDir: dotnetSource.BackupDir,
      expectedSourceRevision: dotnetSourceRevision,
      journals: dotnetOriginJournals,
      expectedResolvedOperationIds: [dotnetCrashJournals[0].operationId]
    });

    return createRuntimeDifference({
      fixtureId: "restore-v2-bidirectional-crash-recovery",
      status: "matched",
      node: { recoveredDotnetCrash: true },
      dotnet: { recoveredNodeCrash: true },
      decision: "Each runtime safely resolved the other runtime's applying Restore journal."
    });
  });
  assert.equal(evidence.status, "matched");
});

test("Restore v2 recovery accepts real Windows 8.3 aliases across Node and .NET", {
  skip: process.platform !== "win32"
}, async () => {
  await Promise.all([fs.access(fixtureHostDll), fs.access(crashHostDll)]);
  const fixtureRoot = path.join(staticRoot, "bidirectional-backup-roundtrip");
  const evidence = await runFixtureInTemp(fixtureRoot, async (fixture) => {
    const nodeToDotnet = await createCase(fixture, "node-short-alias-to-dotnet");
    const nodeSource = await runSync({ codexHome: nodeToDotnet.codexHome, provider: "openai" });
    runProcess(process.execPath, [
      nodeRestoreCrashHost,
      nodeToDotnet.codexHome,
      nodeSource.backupDir,
      "after_restore_prepared_before_applying",
      "--with-database"
    ], { expectCrash: true });
    const [nodePending] = await restoreJournals(nodeToDotnet.codexHome);
    const nodePendingBytes = await fs.readFile(nodePending.filePath);
    const recoveredByDotnet = runDotnet(
      "restore-v2",
      windowsShortDirectoryPath(nodeToDotnet.codexHome),
      windowsShortDirectoryPath(nodeSource.backupDir)
    );
    assert.equal(recoveredByDotnet.RestoreJournalState, "completed");
    assert.deepEqual(recoveredByDotnet.ResolvedOperationIds, [nodePending.operationId]);
    assert.deepEqual(await fs.readFile(nodePending.filePath), nodePendingBytes);
    await assertRestored(nodeToDotnet, nodeSource.backupDir, fixture.manifest.expected.journalTerminal);

    const dotnetToNode = await createCase(fixture, "dotnet-short-alias-to-node");
    const dotnetSource = runDotnet("sync", dotnetToNode.codexHome, "openai");
    runProcess("dotnet", [
      crashHostDll,
      "restore-v2",
      dotnetToNode.codexHome,
      dotnetSource.BackupDir,
      "after_restore_prepared_before_applying"
    ], { expectCrash: true });
    const [dotnetPending] = await restoreJournals(dotnetToNode.codexHome);
    const dotnetPendingBytes = await fs.readFile(dotnetPending.filePath);
    const recoveredByNode = await runRestore({
      codexHome: windowsShortDirectoryPath(dotnetToNode.codexHome),
      backupDir: windowsShortDirectoryPath(dotnetSource.BackupDir)
    });
    assert.equal(recoveredByNode.restoreJournalState, "completed");
    assert.deepEqual(recoveredByNode.resolvedOperationIds, [dotnetPending.operationId]);
    assert.deepEqual(await fs.readFile(dotnetPending.filePath), dotnetPendingBytes);
    await assertRestored(dotnetToNode, dotnetSource.BackupDir, fixture.manifest.expected.journalTerminal);

    const nodeAliasCreated = await createCase(fixture, "node-created-via-short-alias");
    const nodeAliasSource = await runSync({ codexHome: nodeAliasCreated.codexHome, provider: "openai" });
    runProcess(process.execPath, [
      nodeRestoreCrashHost,
      windowsShortDirectoryPath(nodeAliasCreated.codexHome),
      windowsShortDirectoryPath(nodeAliasSource.backupDir),
      "after_restore_prepared_before_applying",
      "--with-database"
    ], { expectCrash: true });
    const [nodeAliasPending] = await restoreJournals(nodeAliasCreated.codexHome);
    const nodeAliasPhysical = await fs.realpath(nodeAliasSource.backupDir);
    assert.equal(
      physicalPathKey(await fs.realpath(nodeAliasPending.prepared.sourceBackup.backupDir)),
      physicalPathKey(nodeAliasPhysical)
    );
    assert.equal(nodeAliasPending.prepared.sourceBackup.backupId, path.basename(nodeAliasPhysical));
    const nodeAliasRecoveredByDotnet = runDotnet(
      "restore-v2",
      nodeAliasCreated.codexHome,
      nodeAliasSource.backupDir
    );
    assert.equal(nodeAliasRecoveredByDotnet.RestoreJournalState, "completed");
    assert.deepEqual(nodeAliasRecoveredByDotnet.ResolvedOperationIds, [nodeAliasPending.operationId]);

    const dotnetAliasCreated = await createCase(fixture, "dotnet-created-via-short-alias");
    const dotnetAliasSource = runDotnet("sync", dotnetAliasCreated.codexHome, "openai");
    runProcess("dotnet", [
      crashHostDll,
      "restore-v2",
      windowsShortDirectoryPath(dotnetAliasCreated.codexHome),
      windowsShortDirectoryPath(dotnetAliasSource.BackupDir),
      "after_restore_prepared_before_applying"
    ], { expectCrash: true });
    const [dotnetAliasPending] = await restoreJournals(dotnetAliasCreated.codexHome);
    const dotnetAliasPhysical = await fs.realpath(dotnetAliasSource.BackupDir);
    assert.equal(
      physicalPathKey(await fs.realpath(dotnetAliasPending.prepared.sourceBackup.backupDir)),
      physicalPathKey(dotnetAliasPhysical)
    );
    assert.equal(dotnetAliasPending.prepared.sourceBackup.backupId, path.basename(dotnetAliasPhysical));
    const dotnetAliasRecoveredByNode = await runRestore({
      codexHome: dotnetAliasCreated.codexHome,
      backupDir: dotnetAliasSource.BackupDir
    });
    assert.equal(dotnetAliasRecoveredByNode.restoreJournalState, "completed");
    assert.deepEqual(dotnetAliasRecoveredByNode.resolvedOperationIds, [dotnetAliasPending.operationId]);

    return createRuntimeDifference({
      fixtureId: "restore-v2-windows-path-alias",
      status: "matched",
      node: {
        recoveredDotnetPendingViaShortAlias: true,
        createdPendingViaShortAliasRecoveredByDotnet: true
      },
      dotnet: {
        recoveredNodePendingViaShortAlias: true,
        createdPendingViaShortAliasRecoveredByNode: true
      },
      decision: "Both runtimes created and recovered the other runtime's pending Restore through actual Windows 8.3 aliases."
    });
  });
  assert.equal(evidence.status, "matched");
});

test("Restore v2 recovery binds Windows junction aliases at creation across Node and .NET", {
  skip: process.platform !== "win32"
}, async () => {
  await Promise.all([fs.access(fixtureHostDll), fs.access(crashHostDll)]);
  const fixtureRoot = path.join(staticRoot, "bidirectional-backup-roundtrip");
  const evidence = await runFixtureInTemp(fixtureRoot, async (fixture) => {
    const nodeToDotnet = await createCase(fixture, "node-junction-create-to-dotnet");
    const nodeSource = await runSync({ codexHome: nodeToDotnet.codexHome, provider: "openai" });
    const nodeSourceAlias = path.join(path.dirname(nodeToDotnet.codexHome), "source-backup-junction");
    await fs.symlink(nodeSource.backupDir, nodeSourceAlias, "junction");
    runProcess(process.execPath, [
      nodeRestoreCrashHost,
      nodeToDotnet.codexHome,
      nodeSourceAlias,
      "after_restore_prepared_before_applying",
      "--with-database"
    ], { expectCrash: true });
    const [nodePending] = await restoreJournals(nodeToDotnet.codexHome);
    assert.equal(
      physicalPathKey(await fs.realpath(nodePending.prepared.sourceBackup.backupDir)),
      physicalPathKey(await fs.realpath(nodeSource.backupDir))
    );
    const nodeRecoveredByDotnet = runDotnet(
      "restore-v2",
      nodeToDotnet.codexHome,
      nodeSource.backupDir
    );
    assert.equal(nodeRecoveredByDotnet.RestoreJournalState, "completed");
    assert.deepEqual(nodeRecoveredByDotnet.ResolvedOperationIds, [nodePending.operationId]);
    await assertRestored(nodeToDotnet, nodeSource.backupDir, fixture.manifest.expected.journalTerminal);

    const dotnetToNode = await createCase(fixture, "dotnet-junction-create-to-node");
    const dotnetSource = runDotnet("sync", dotnetToNode.codexHome, "openai");
    const dotnetSourceAlias = path.join(path.dirname(dotnetToNode.codexHome), "source-backup-junction");
    await fs.symlink(dotnetSource.BackupDir, dotnetSourceAlias, "junction");
    runProcess("dotnet", [
      crashHostDll,
      "restore-v2",
      dotnetToNode.codexHome,
      dotnetSourceAlias,
      "after_restore_prepared_before_applying"
    ], { expectCrash: true });
    const [dotnetPending] = await restoreJournals(dotnetToNode.codexHome);
    assert.equal(
      physicalPathKey(await fs.realpath(dotnetPending.prepared.sourceBackup.backupDir)),
      physicalPathKey(await fs.realpath(dotnetSource.BackupDir))
    );
    const dotnetRecoveredByNode = await runRestore({
      codexHome: dotnetToNode.codexHome,
      backupDir: dotnetSource.BackupDir
    });
    assert.equal(dotnetRecoveredByNode.restoreJournalState, "completed");
    assert.deepEqual(dotnetRecoveredByNode.resolvedOperationIds, [dotnetPending.operationId]);
    await assertRestored(dotnetToNode, dotnetSource.BackupDir, fixture.manifest.expected.journalTerminal);

    return createRuntimeDifference({
      fixtureId: "restore-v2-windows-junction-alias",
      status: "matched",
      node: { createdThroughJunctionRecoveredByDotnet: true },
      dotnet: { createdThroughJunctionRecoveredByNode: true },
      decision: "Both runtimes persisted physical Restore source identity when the pending journal was created through a Windows junction."
    });
  });
  assert.equal(evidence.status, "matched");
});

test("Restore v2 prepared, committing and rollback-pending crashes recover across runtimes", async () => {
  await Promise.all([fs.access(fixtureHostDll), fs.access(crashHostDll)]);
  const fixtureRoot = path.join(staticRoot, "bidirectional-backup-roundtrip");
  const scenarios = [
    {
      name: "prepared",
      point: "after_restore_prepared_before_applying",
      state: "prepared"
    },
    {
      name: "committing",
      point: "after_restore_committing_before_committed_pending_ack",
      state: "committing"
    },
    {
      name: "rollback-pending",
      point: "after_restore_rollback_pending_before_target",
      failurePoint: "after_restore_target_write_before_complete",
      state: "rollback-pending"
    }
  ];
  const evidence = await runFixtureInTemp(fixtureRoot, async (fixture) => {
    for (const scenario of scenarios) {
      const nodeToDotnet = await createCase(fixture, `node-${scenario.name}-to-dotnet`);
      const nodeSource = await runSync({ codexHome: nodeToDotnet.codexHome, provider: "openai" });
      const nodeSourceRevision = await captureRestoreSourceIdentity(nodeSource.backupDir);
      const nodeCrashArgs = [
        nodeRestoreCrashHost,
        nodeToDotnet.codexHome,
        nodeSource.backupDir,
        scenario.point,
        "--with-database"
      ];
      if (scenario.failurePoint) nodeCrashArgs.push("--fail-at", scenario.failurePoint);
      runProcess(process.execPath, nodeCrashArgs, { expectCrash: true });
      const nodePendingJournals = await restoreJournals(nodeToDotnet.codexHome);
      assert.equal(nodePendingJournals.length, 1);
      const [nodePending] = nodePendingJournals;
      assert.equal(nodePending.state, scenario.state);
      const recoveredByDotnet = runDotnet(
        "restore-v2",
        nodeToDotnet.codexHome,
        nodeSource.backupDir
      );
      assert.equal(recoveredByDotnet.RestoreJournalState, "completed");
      assert.deepEqual(recoveredByDotnet.ResolvedOperationIds, [nodePending.operationId]);
      await assertRestored(nodeToDotnet, nodeSource.backupDir, fixture.manifest.expected.journalTerminal);
      const nodeJournals = await restoreJournals(nodeToDotnet.codexHome);
      assert.equal(nodeJournals.length, 2);
      assert.equal(nodeJournals.filter((journal) => journal.state === scenario.state).length, 1);
      assert.equal(nodeJournals.filter((journal) => journal.state === "completed").length, 1);
      await assertCompletedRestoreEvidence({
        result: recoveredByDotnet,
        codexHome: nodeToDotnet.codexHome,
        sourceBackupDir: nodeSource.backupDir,
        expectedSourceRevision: nodeSourceRevision,
        journals: nodeJournals,
        expectedResolvedOperationIds: [nodePending.operationId]
      });

      const dotnetToNode = await createCase(fixture, `dotnet-${scenario.name}-to-node`);
      const dotnetSource = runDotnet("sync", dotnetToNode.codexHome, "openai");
      const dotnetSourceRevision = await captureRestoreSourceIdentity(dotnetSource.BackupDir);
      const dotnetCrashArgs = [
        crashHostDll,
        "restore-v2",
        dotnetToNode.codexHome,
        dotnetSource.BackupDir,
        scenario.point
      ];
      if (scenario.failurePoint) dotnetCrashArgs.push("--fail-at", scenario.failurePoint);
      runProcess("dotnet", dotnetCrashArgs, { expectCrash: true });
      const dotnetPendingJournals = await restoreJournals(dotnetToNode.codexHome);
      assert.equal(dotnetPendingJournals.length, 1);
      const [dotnetPending] = dotnetPendingJournals;
      assert.equal(dotnetPending.state, scenario.state);
      const recoveredByNode = await runRestore({
        codexHome: dotnetToNode.codexHome,
        backupDir: dotnetSource.BackupDir
      });
      assert.equal(recoveredByNode.restoreJournalState, "completed");
      assert.deepEqual(recoveredByNode.resolvedOperationIds, [dotnetPending.operationId]);
      await assertRestored(dotnetToNode, dotnetSource.BackupDir, fixture.manifest.expected.journalTerminal);
      const dotnetJournals = await restoreJournals(dotnetToNode.codexHome);
      assert.equal(dotnetJournals.length, 2);
      assert.equal(dotnetJournals.filter((journal) => journal.state === scenario.state).length, 1);
      assert.equal(dotnetJournals.filter((journal) => journal.state === "completed").length, 1);
      await assertCompletedRestoreEvidence({
        result: recoveredByNode,
        codexHome: dotnetToNode.codexHome,
        sourceBackupDir: dotnetSource.BackupDir,
        expectedSourceRevision: dotnetSourceRevision,
        journals: dotnetJournals,
        expectedResolvedOperationIds: [dotnetPending.operationId]
      });
    }
    return createRuntimeDifference({
      fixtureId: "restore-v2-cross-runtime-crash-matrix",
      status: "matched",
      node: { recoveredStates: scenarios.map((scenario) => scenario.state) },
      dotnet: { recoveredStates: scenarios.map((scenario) => scenario.state) },
      decision: "Both runtimes resolved the other runtime's durable pre-commit Restore states."
    });
  });
  assert.equal(evidence.status, "matched");
});

test("foreign Restore v2 pending is rejected without mutation in both runtime directions", async () => {
  await Promise.all([fs.access(fixtureHostDll), fs.access(crashHostDll)]);
  const fixtureRoot = path.join(staticRoot, "bidirectional-backup-roundtrip");
  const evidence = await runFixtureInTemp(fixtureRoot, async (fixture) => {
    const nodeToDotnet = await createCase(fixture, "node-foreign-v2-to-dotnet");
    const nodeSourceA = await runSync({ codexHome: nodeToDotnet.codexHome, provider: "openai" });
    const nodeSourceB = await runSync({ codexHome: nodeToDotnet.codexHome, provider: "relay" });
    assert.notEqual(path.resolve(nodeSourceA.backupDir), path.resolve(nodeSourceB.backupDir));
    runProcess(process.execPath, [
      nodeRestoreCrashHost,
      nodeToDotnet.codexHome,
      nodeSourceA.backupDir,
      "after_restore_target_write_before_complete",
      "--with-database"
    ], { expectCrash: true });
    const nodePendingJournals = await restoreJournals(nodeToDotnet.codexHome);
    assert.equal(nodePendingJournals.length, 1);
    const [nodePending] = nodePendingJournals;
    assert.equal(nodePending.state, "applying");
    assert.equal([...nodePending.targetPhases.values()].includes("intent"), true);
    const nodeCanonicalBefore = await canonicalState(nodeToDotnet.codexHome);
    const nodeTreeBefore = await managedBackupTree(nodeToDotnet.codexHome);
    const nodeJournalBefore = await fs.readFile(nodePending.filePath);
    runDotnetFailure("restore-v2", nodeToDotnet.codexHome, nodeSourceB.backupDir);
    assert.deepEqual((await canonicalState(nodeToDotnet.codexHome)).canonical, nodeCanonicalBefore.canonical);
    assert.deepEqual(await managedBackupTree(nodeToDotnet.codexHome), nodeTreeBefore);
    assert.deepEqual(await fs.readFile(nodePending.filePath), nodeJournalBefore);
    assert.equal((await restoreJournals(nodeToDotnet.codexHome)).length, 1);

    const dotnetToNode = await createCase(fixture, "dotnet-foreign-v2-to-node");
    const dotnetSourceA = runDotnet("sync", dotnetToNode.codexHome, "openai");
    const dotnetSourceB = runDotnet("sync", dotnetToNode.codexHome, "relay");
    assert.notEqual(path.resolve(dotnetSourceA.BackupDir), path.resolve(dotnetSourceB.BackupDir));
    runProcess("dotnet", [
      crashHostDll,
      "restore-v2",
      dotnetToNode.codexHome,
      dotnetSourceA.BackupDir,
      "after_restore_prepared_before_applying"
    ], { expectCrash: true });
    const dotnetPendingJournals = await restoreJournals(dotnetToNode.codexHome);
    assert.equal(dotnetPendingJournals.length, 1);
    const [dotnetPending] = dotnetPendingJournals;
    assert.equal(dotnetPending.state, "prepared");
    const dotnetCanonicalBefore = await canonicalState(dotnetToNode.codexHome);
    const dotnetTreeBefore = await managedBackupTree(dotnetToNode.codexHome);
    const dotnetJournalBefore = await fs.readFile(dotnetPending.filePath);
    await assert.rejects(
      () => runRestore({ codexHome: dotnetToNode.codexHome, backupDir: dotnetSourceB.BackupDir }),
      (error) => error?.code === "RECOVERY_REQUIRED"
    );
    assert.deepEqual((await canonicalState(dotnetToNode.codexHome)).canonical, dotnetCanonicalBefore.canonical);
    assert.deepEqual(await managedBackupTree(dotnetToNode.codexHome), dotnetTreeBefore);
    assert.deepEqual(await fs.readFile(dotnetPending.filePath), dotnetJournalBefore);
    assert.equal((await restoreJournals(dotnetToNode.codexHome)).length, 1);

    return createRuntimeDifference({
      fixtureId: "restore-v2-cross-runtime-foreign-pending",
      status: "matched",
      node: { rejectedDotnetForeignPending: true },
      dotnet: { rejectedNodeForeignPending: true },
      decision: "Foreign Restore v2 journals failed closed without mutating data or evidence."
    });
  });
  assert.equal(evidence.status, "matched");
});

test("unknown Restore journal schema blocks Node and .NET writes without rewriting evidence", async () => {
  await Promise.all([fs.access(fixtureHostDll), fs.access(crashHostDll)]);
  const fixtureRoot = path.join(staticRoot, "bidirectional-backup-roundtrip");
  const evidence = await runFixtureInTemp(fixtureRoot, async (fixture) => {
    const nodeToDotnet = await createCase(fixture, "unknown-schema-to-dotnet");
    const nodeSource = await runSync({ codexHome: nodeToDotnet.codexHome, provider: "openai" });
    const nodeUnknown = await writeUnknownRestoreJournal(
      nodeToDotnet.codexHome,
      nodeSource.backupDir,
      "node-to-dotnet"
    );
    const nodeCanonicalBefore = await canonicalState(nodeToDotnet.codexHome);
    const nodeTreeBefore = await managedBackupTree(nodeToDotnet.codexHome);
    runDotnetFailure("sync", nodeToDotnet.codexHome, "relay");
    runDotnetFailure("restore-v2", nodeToDotnet.codexHome, nodeSource.backupDir);
    assert.deepEqual((await canonicalState(nodeToDotnet.codexHome)).canonical, nodeCanonicalBefore.canonical);
    assert.deepEqual(await managedBackupTree(nodeToDotnet.codexHome), nodeTreeBefore);
    assert.equal(await fs.readFile(nodeUnknown.journalPath, "utf8"), nodeUnknown.raw);

    const dotnetToNode = await createCase(fixture, "unknown-schema-to-node");
    const dotnetSource = runDotnet("sync", dotnetToNode.codexHome, "openai");
    const dotnetUnknown = await writeUnknownRestoreJournal(
      dotnetToNode.codexHome,
      dotnetSource.BackupDir,
      "dotnet-to-node"
    );
    const dotnetCanonicalBefore = await canonicalState(dotnetToNode.codexHome);
    const dotnetTreeBefore = await managedBackupTree(dotnetToNode.codexHome);
    await assert.rejects(
      () => runSync({ codexHome: dotnetToNode.codexHome, provider: "relay" }),
      (error) => error?.code === "RECOVERY_REQUIRED"
    );
    await assert.rejects(
      () => runRestore({ codexHome: dotnetToNode.codexHome, backupDir: dotnetSource.BackupDir }),
      (error) => error?.code === "RECOVERY_REQUIRED"
    );
    assert.deepEqual((await canonicalState(dotnetToNode.codexHome)).canonical, dotnetCanonicalBefore.canonical);
    assert.deepEqual(await managedBackupTree(dotnetToNode.codexHome), dotnetTreeBefore);
    assert.equal(await fs.readFile(dotnetUnknown.journalPath, "utf8"), dotnetUnknown.raw);

    return createRuntimeDifference({
      fixtureId: "restore-v2-cross-runtime-unknown-schema",
      status: "matched",
      node: { failedClosed: true },
      dotnet: { failedClosed: true },
      decision: "Both readers preserve unknown Restore journal bytes and block Sync and Restore writes."
    });
  });
  assert.equal(evidence.status, "matched");
});

test("Restore v2 commit acknowledgement is forward-only across runtimes", async () => {
  await Promise.all([fs.access(fixtureHostDll), fs.access(crashHostDll)]);
  const fixtureRoot = path.join(staticRoot, "bidirectional-backup-roundtrip");
  const evidence = await runFixtureInTemp(fixtureRoot, async (fixture) => {
    const nodeToDotnet = await createCase(fixture, "node-ack-to-dotnet");
    const nodeSource = await runSync({ codexHome: nodeToDotnet.codexHome, provider: "openai" });
    const nodeSourceRevision = await captureRestoreSourceIdentity(nodeSource.backupDir);
    runProcess(
      process.execPath,
      [
        nodeRestoreCrashHost,
        nodeToDotnet.codexHome,
        nodeSource.backupDir,
        "after_restore_committed_pending_ack_before_completed",
        "--with-database"
      ],
      { expectCrash: true }
    );
    const nodePending = await restoreJournals(nodeToDotnet.codexHome);
    assert.equal(nodePending.length, 1);
    assert.equal(nodePending[0].state, "committed-pending-ack");
    const dotnetAck = runDotnet("restore-v2", nodeToDotnet.codexHome, nodeSource.backupDir);
    assert.equal(dotnetAck.CommitAcknowledgementRecovered, true);
    const nodeAcknowledged = await restoreJournals(nodeToDotnet.codexHome);
    assert.equal(nodeAcknowledged.length, 1);
    assert.equal(nodeAcknowledged[0].state, "completed");
    assert.equal(nodeAcknowledged[0].events.some((event) => event.state === "rollback-pending"), false);
    await assertCompletedRestoreEvidence({
      result: dotnetAck,
      codexHome: nodeToDotnet.codexHome,
      sourceBackupDir: nodeSource.backupDir,
      expectedSourceRevision: nodeSourceRevision,
      journals: nodeAcknowledged
    });
    await assertRestored(nodeToDotnet, nodeSource.backupDir, fixture.manifest.expected.journalTerminal);

    const dotnetToNode = await createCase(fixture, "dotnet-ack-to-node");
    const dotnetSource = runDotnet("sync", dotnetToNode.codexHome, "openai");
    const dotnetSourceRevision = await captureRestoreSourceIdentity(dotnetSource.BackupDir);
    runProcess(
      "dotnet",
      [
        crashHostDll,
        "restore-v2",
        dotnetToNode.codexHome,
        dotnetSource.BackupDir,
        "after_restore_committed_pending_ack_before_completed"
      ],
      { expectCrash: true }
    );
    const dotnetPending = await restoreJournals(dotnetToNode.codexHome);
    assert.equal(dotnetPending.length, 1);
    assert.equal(dotnetPending[0].state, "committed-pending-ack");
    const nodeAck = await runRestore({
      codexHome: dotnetToNode.codexHome,
      backupDir: dotnetSource.BackupDir
    });
    assert.equal(nodeAck.commitAcknowledgementRecovered, true);
    const dotnetAcknowledged = await restoreJournals(dotnetToNode.codexHome);
    assert.equal(dotnetAcknowledged.length, 1);
    assert.equal(dotnetAcknowledged[0].state, "completed");
    assert.equal(dotnetAcknowledged[0].events.some((event) => event.state === "rollback-pending"), false);
    await assertCompletedRestoreEvidence({
      result: nodeAck,
      codexHome: dotnetToNode.codexHome,
      sourceBackupDir: dotnetSource.BackupDir,
      expectedSourceRevision: dotnetSourceRevision,
      journals: dotnetAcknowledged
    });
    await assertRestored(dotnetToNode, dotnetSource.BackupDir, fixture.manifest.expected.journalTerminal);

    return createRuntimeDifference({
      fixtureId: "restore-v2-cross-runtime-commit-ack",
      status: "matched",
      node: { acknowledgedDotnetCommit: true },
      dotnet: { acknowledgedNodeCommit: true },
      decision: "Both runtimes completed the other's committed Restore without compensation."
    });
  });
  assert.equal(evidence.status, "matched");
});

test("Restore v2 manifest and prepared journal binding fails closed across runtimes", async () => {
  await Promise.all([fs.access(fixtureHostDll), fs.access(crashHostDll)]);
  const fixtureRoot = path.join(staticRoot, "bidirectional-backup-roundtrip");
  const evidence = await runFixtureInTemp(fixtureRoot, async (fixture) => {
    const nodeToDotnet = await createCase(fixture, "node-binding-to-dotnet");
    const nodeSource = await runSync({ codexHome: nodeToDotnet.codexHome, provider: "openai" });
    runProcess(
      process.execPath,
      [
        nodeRestoreCrashHost,
        nodeToDotnet.codexHome,
        nodeSource.backupDir,
        "after_restore_committed_pending_ack_before_completed",
        "--with-database"
      ],
      { expectCrash: true }
    );
    const [nodePending] = await restoreJournals(nodeToDotnet.codexHome);
    await mismatchManifestPreparedBinding(nodePending);
    const nodeBusinessBefore = await canonicalState(nodeToDotnet.codexHome);
    runDotnetFailure("restore-v2", nodeToDotnet.codexHome, nodeSource.backupDir);
    assert.deepEqual((await canonicalState(nodeToDotnet.codexHome)).canonical, nodeBusinessBefore.canonical);
    const [nodeFailed] = await restoreJournals(nodeToDotnet.codexHome);
    assert.equal(nodeFailed.state, "recovery-required");
    assert.equal(nodeFailed.events.some((event) => event.state === "rollback-pending"), false);

    const dotnetToNode = await createCase(fixture, "dotnet-binding-to-node");
    const dotnetSource = runDotnet("sync", dotnetToNode.codexHome, "openai");
    runProcess(
      "dotnet",
      [
        crashHostDll,
        "restore-v2",
        dotnetToNode.codexHome,
        dotnetSource.BackupDir,
        "after_restore_committed_pending_ack_before_completed"
      ],
      { expectCrash: true }
    );
    const [dotnetPending] = await restoreJournals(dotnetToNode.codexHome);
    await mismatchManifestPreparedBinding(dotnetPending);
    const dotnetBusinessBefore = await canonicalState(dotnetToNode.codexHome);
    await assert.rejects(
      () => runRestore({
        codexHome: dotnetToNode.codexHome,
        backupDir: dotnetSource.BackupDir
      }),
      (error) => error?.code === "RECOVERY_REQUIRED"
    );
    assert.deepEqual((await canonicalState(dotnetToNode.codexHome)).canonical, dotnetBusinessBefore.canonical);
    const [dotnetFailed] = await restoreJournals(dotnetToNode.codexHome);
    assert.equal(dotnetFailed.state, "recovery-required");
    assert.equal(dotnetFailed.events.some((event) => event.state === "rollback-pending"), false);

    return createRuntimeDifference({
      fixtureId: "restore-v2-manifest-prepared-binding",
      status: "matched",
      node: { rejectedDotnetMismatch: true },
      dotnet: { rejectedNodeMismatch: true },
      decision: "Both runtimes reject a rehashed manifest that disagrees with durable prepared evidence."
    });
  });
  assert.equal(evidence.status, "matched");
});

test("Restore v2 persisted physical Home binding fails closed across runtimes", async () => {
  await Promise.all([fs.access(fixtureHostDll), fs.access(crashHostDll)]);
  const fixtureRoot = path.join(staticRoot, "bidirectional-backup-roundtrip");
  const evidence = await runFixtureInTemp(fixtureRoot, async (fixture) => {
    const foreignPhysicalHome = path.join(fixture.root, "work", "foreign-physical-home");
    await fs.mkdir(foreignPhysicalHome, { recursive: true });

    const nodeToDotnet = await createCase(fixture, "node-home-binding-to-dotnet");
    const nodeSource = await runSync({ codexHome: nodeToDotnet.codexHome, provider: "openai" });
    runProcess(
      process.execPath,
      [
        nodeRestoreCrashHost,
        nodeToDotnet.codexHome,
        nodeSource.backupDir,
        "after_restore_prepared_before_applying",
        "--with-database"
      ],
      { expectCrash: true }
    );
    const [nodePending] = await restoreJournals(nodeToDotnet.codexHome);
    await mismatchPreparedPhysicalHome(nodePending, foreignPhysicalHome);
    const nodeJournalBefore = await fs.readFile(nodePending.filePath);
    const nodeBusinessBefore = await canonicalState(nodeToDotnet.codexHome);
    const nodeDirectoriesBefore = await managedBackupDirectories(nodeToDotnet.codexHome);
    runDotnetFailure("restore-v2", nodeToDotnet.codexHome, nodeSource.backupDir);
    assert.deepEqual((await canonicalState(nodeToDotnet.codexHome)).canonical, nodeBusinessBefore.canonical);
    assert.deepEqual(await managedBackupDirectories(nodeToDotnet.codexHome), nodeDirectoriesBefore);
    assert.deepEqual(await fs.readFile(nodePending.filePath), nodeJournalBefore);

    const dotnetToNode = await createCase(fixture, "dotnet-home-binding-to-node");
    const dotnetSource = runDotnet("sync", dotnetToNode.codexHome, "openai");
    runProcess(
      "dotnet",
      [
        crashHostDll,
        "restore-v2",
        dotnetToNode.codexHome,
        dotnetSource.BackupDir,
        "after_restore_prepared_before_applying"
      ],
      { expectCrash: true }
    );
    const [dotnetPending] = await restoreJournals(dotnetToNode.codexHome);
    await mismatchPreparedPhysicalHome(dotnetPending, foreignPhysicalHome);
    const dotnetJournalBefore = await fs.readFile(dotnetPending.filePath);
    const dotnetBusinessBefore = await canonicalState(dotnetToNode.codexHome);
    const dotnetDirectoriesBefore = await managedBackupDirectories(dotnetToNode.codexHome);
    await assert.rejects(
      () => runRestore({
        codexHome: dotnetToNode.codexHome,
        backupDir: dotnetSource.BackupDir
      }),
      (error) => error?.code === "RECOVERY_REQUIRED"
    );
    assert.deepEqual((await canonicalState(dotnetToNode.codexHome)).canonical, dotnetBusinessBefore.canonical);
    assert.deepEqual(await managedBackupDirectories(dotnetToNode.codexHome), dotnetDirectoriesBefore);
    assert.deepEqual(await fs.readFile(dotnetPending.filePath), dotnetJournalBefore);

    return createRuntimeDifference({
      fixtureId: "restore-v2-persisted-physical-home-binding",
      status: "matched",
      node: { rejectedDotnetPhysicalMismatch: true },
      dotnet: { rejectedNodePhysicalMismatch: true },
      decision: "Both runtimes reject a pending Restore whose persisted physical Home differs from the current locked Home."
    });
  });
  assert.equal(evidence.status, "matched");
});
