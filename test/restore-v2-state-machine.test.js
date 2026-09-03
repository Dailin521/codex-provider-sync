import { execFile, spawn } from "node:child_process";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { defaultBackupRoot } from "../src/constants.js";
import { pruneBackups } from "../src/backup.js";
import {
  readRestoreJournal,
  RESTORE_JOURNAL_BASENAME,
  RestoreJournal
} from "../src/restore-journal.js";
import {
  acknowledgePendingRestore,
  RESTORE_SNAPSHOT_MANIFEST_BASENAME
} from "../src/restore-v2.js";
import { getStatus, runRestore, runSwitch } from "../src/service.js";
import { openDatabase } from "../src/sqlite.js";
import { resolveStateDbLockResource } from "../src/state-db-lock.js";
import { findPendingTransactions } from "../src/transaction-journal.js";

const execFileAsync = promisify(execFile);

async function windowsShortDirectoryPath(directory) {
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
  const { stdout } = await execFileAsync(executable, [
    "-NoLogo",
    "-NoProfile",
    "-NonInteractive",
    "-Command",
    command
  ], {
    env: { ...process.env, CPS_SHORT_PATH_TARGET: directory },
    windowsHide: true
  });
  const shortPath = stdout.trim();
  assert.equal(path.isAbsolute(shortPath), true, "PowerShell must return an absolute 8.3 alias");
  assert.notEqual(
    path.resolve(shortPath).toLowerCase(),
    path.resolve(directory).toLowerCase(),
    "The Windows volume must expose an actual short-path alias for this fixture"
  );
  return shortPath;
}

async function makeFixture({ withDatabase = false } = {}) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-restore-v2-"));
  const codexHome = path.join(root, "home");
  await fs.mkdir(codexHome, { recursive: true });
  await fs.writeFile(
    path.join(codexHome, "config.toml"),
    'model_provider = "apigather"\n\n[model_providers.apigather]\nname = "API Gather"\n',
    "utf8"
  );
  if (withDatabase) {
    const dbPath = path.join(codexHome, "sqlite", "state_5.sqlite");
    await fs.mkdir(path.dirname(dbPath), { recursive: true });
    const db = await openDatabase(dbPath);
    try {
      db.exec(`
        CREATE TABLE threads (
          id TEXT PRIMARY KEY,
          model_provider TEXT,
          archived INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO threads (id, model_provider, archived)
        VALUES ('restore-v2', 'apigather', 0);
      `);
    } finally {
      db.close();
    }
  }
  const rolloutPath = path.join(
    codexHome,
    "sessions",
    "2026",
    "08",
    "26",
    "rollout-restore-v2.jsonl"
  );
  await fs.mkdir(path.dirname(rolloutPath), { recursive: true });
  await fs.writeFile(
    rolloutPath,
    [
      JSON.stringify({
        type: "session_meta",
        payload: { id: "restore-v2", model_provider: "apigather", cwd: root }
      }),
      JSON.stringify({ type: "turn_context", payload: { model: "source-model" } }),
      ""
    ].join("\n"),
    "utf8"
  );
  const switched = await runSwitch({
    codexHome,
    provider: "openai",
    model: "target-model"
  });
  return {
    root,
    codexHome,
    rolloutPath,
    sourceBackup: switched.backupDir
  };
}

async function readDbProvider(codexHome) {
  const db = await openDatabase(path.join(codexHome, "sqlite", "state_5.sqlite"), { readOnly: true });
  try {
    return db.prepare("SELECT model_provider FROM threads WHERE id = 'restore-v2'").get().model_provider;
  } finally {
    db.close();
  }
}

async function listRestoreJournals(codexHome) {
  const root = defaultBackupRoot(codexHome);
  const entries = await fs.readdir(root, { withFileTypes: true });
  const journals = [];
  for (const entry of entries) {
    if (!entry.isDirectory() || !entry.name.startsWith("restore-v2-")) continue;
    journals.push(await readRestoreJournal(path.join(root, entry.name, RESTORE_JOURNAL_BASENAME)));
  }
  return journals.sort((left, right) => left.filePath.localeCompare(right.filePath));
}

function spawnCrash(
  codexHome,
  backupDir,
  point,
  { withDatabase = false, failurePoint = null } = {}
) {
  const host = fileURLToPath(new URL("../test-support/restore-v2-crash-host.mjs", import.meta.url));
  return new Promise((resolve, reject) => {
    const args = [host, codexHome, backupDir, point];
    if (withDatabase) args.push("--with-database");
    if (failurePoint) args.push("--fail-at", failurePoint);
    const child = spawn(process.execPath, args, {
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true
    });
    let stderr = "";
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.on("error", reject);
    child.on("exit", (code, signal) => resolve({ code, signal, stderr }));
  });
}

async function completeResolverJournal(journal, prepared, manifestSha256) {
  await journal.applying();
  for (const target of prepared.targets) {
    await journal.targetIntent(target.id);
    await journal.targetCompleted(target.id, target.expectedPost.digest);
  }
  await journal.committing(manifestSha256);
  await journal.committedPendingAck(manifestSha256);
  await journal.completed();
}

for (const scenario of [
  {
    name: "prepared",
    crashPoint: "after_restore_prepared_before_applying",
    expectedState: "prepared",
    withDatabase: false
  },
  {
    name: "committing with SQLite",
    crashPoint: "after_restore_committing_before_committed_pending_ack",
    expectedState: "committing",
    withDatabase: true
  },
  {
    name: "rollback-pending with SQLite",
    crashPoint: "after_restore_rollback_pending_before_target",
    failurePoint: "after_restore_target_write_before_complete",
    expectedState: "rollback-pending",
    withDatabase: true
  }
]) {
  test(`an explicit same-source Restore resolves a real ${scenario.name} process crash`, async () => {
    const fixture = await makeFixture({ withDatabase: scenario.withDatabase });
    const crashed = await spawnCrash(
      fixture.codexHome,
      fixture.sourceBackup,
      scenario.crashPoint,
      {
        withDatabase: scenario.withDatabase,
        failurePoint: scenario.failurePoint ?? null
      }
    );
    assert.equal(crashed.signal, null, crashed.stderr);
    assert.equal(crashed.code, 86, crashed.stderr);
    const [pending] = await listRestoreJournals(fixture.codexHome);
    assert.equal(pending.state, scenario.expectedState);
    assert.equal((await getStatus({ codexHome: fixture.codexHome })).pendingRecovery, true);

    const recovered = await runRestore({
      codexHome: fixture.codexHome,
      backupDir: fixture.sourceBackup,
      restoreDatabase: scenario.withDatabase
    });
    assert.equal(recovered.restoreJournalState, "completed");
    assert.deepEqual(recovered.resolvedOperationIds, [pending.operationId]);
    assert.equal((await getStatus({ codexHome: fixture.codexHome })).pendingRecovery, false);
    assert.match(await fs.readFile(path.join(fixture.codexHome, "config.toml"), "utf8"), /apigather/);
    if (scenario.withDatabase) {
      assert.equal(await readDbProvider(fixture.codexHome), "apigather");
    }
  });
}

test("Restore v2 snapshot failure is pre-mutation and leaves no journal", async () => {
  const fixture = await makeFixture();
  const configBefore = await fs.readFile(path.join(fixture.codexHome, "config.toml"), "utf8");
  const rolloutBefore = await fs.readFile(fixture.rolloutPath, "utf8");

  await assert.rejects(
    () => runRestore({
      codexHome: fixture.codexHome,
      backupDir: fixture.sourceBackup,
      restoreDatabase: false,
      faultInjector: ({ point }) => {
        if (point === "after_restore_pre_snapshot_target_before_hash") {
          throw new Error("snapshot fault");
        }
      }
    }),
    (error) => error?.code === "BACKUP_FAILED"
  );

  assert.equal(await fs.readFile(path.join(fixture.codexHome, "config.toml"), "utf8"), configBefore);
  assert.equal(await fs.readFile(fixture.rolloutPath, "utf8"), rolloutBefore);
  assert.deepEqual(await listRestoreJournals(fixture.codexHome), []);
});

test("legacy runRestore preserves cancellation and does not mutate after a prepared-only abort", async () => {
  const fixture = await makeFixture();
  const controller = new AbortController();
  const configPath = path.join(fixture.codexHome, "config.toml");
  const configBefore = await fs.readFile(configPath);
  const rolloutBefore = await fs.readFile(fixture.rolloutPath);
  const configStatBefore = await fs.stat(configPath);
  const rolloutStatBefore = await fs.stat(fixture.rolloutPath);

  await assert.rejects(
    () => runRestore({
      codexHome: fixture.codexHome,
      backupDir: fixture.sourceBackup,
      restoreDatabase: false,
      signal: controller.signal,
      faultInjector: ({ point }) => {
        if (point === "after_restore_prepared_before_applying") controller.abort();
      }
    }),
    (error) => error?.code === "OPERATION_CANCELLED"
  );

  assert.deepEqual(await fs.readFile(configPath), configBefore);
  assert.deepEqual(await fs.readFile(fixture.rolloutPath), rolloutBefore);
  assert.equal((await fs.stat(configPath)).mtimeMs, configStatBefore.mtimeMs);
  assert.equal((await fs.stat(fixture.rolloutPath)).mtimeMs, rolloutStatBefore.mtimeMs);
  const [journal] = await listRestoreJournals(fixture.codexHome);
  assert.equal(journal.state, "rolled-back");
  assert.equal([...journal.targetPhases.values()].every((phase) => phase === "compensated"), true);
});

test("Restore progress observer failures are non-authoritative", async () => {
  const fixture = await makeFixture();
  let calls = 0;
  const result = await runRestore({
    codexHome: fixture.codexHome,
    backupDir: fixture.sourceBackup,
    restoreDatabase: false,
    onProgress: () => {
      calls += 1;
      if (calls % 2 === 0) return Promise.reject(new Error("async observer failure"));
      throw new Error("sync observer failure");
    }
  });

  assert.ok(calls > 0);
  assert.equal(result.restoreJournalState, "completed");
  assert.equal((await listRestoreJournals(fixture.codexHome))[0].state, "completed");
});

test("Restore compensation rejects a swapped rollout junction before touching the external target", async () => {
  const fixture = await makeFixture();
  const sessionsPath = path.join(fixture.codexHome, "sessions");
  const preservedSessionsPath = path.join(fixture.codexHome, "sessions-preserved");
  const externalSessionsPath = path.join(fixture.root, "external-sessions");
  const externalRolloutPath = path.join(
    externalSessionsPath,
    "2026",
    "08",
    "26",
    path.basename(fixture.rolloutPath)
  );
  await fs.mkdir(path.dirname(externalRolloutPath), { recursive: true });
  await fs.writeFile(externalRolloutPath, "external-sentinel\n", "utf8");
  const externalBefore = await fs.readFile(externalRolloutPath);
  let swapped = false;

  await assert.rejects(
    () => runRestore({
      codexHome: fixture.codexHome,
      backupDir: fixture.sourceBackup,
      restoreConfig: false,
      restoreDatabase: false,
      faultInjector: async ({ point, targetKind }) => {
        if (!swapped
            && point === "after_restore_rollback_pending_before_target"
            && targetKind === "rollout") {
          swapped = true;
          await fs.rename(sessionsPath, preservedSessionsPath);
          await fs.symlink(
            externalSessionsPath,
            sessionsPath,
            process.platform === "win32" ? "junction" : "dir"
          );
        }
        if (point === "after_restore_target_write_before_complete" && targetKind === "rollout") {
          throw new Error("force compensation");
        }
      }
    }),
    (error) => error?.code === "RECOVERY_REQUIRED"
  );

  assert.equal(swapped, true);
  assert.deepEqual(await fs.readFile(externalRolloutPath), externalBefore);
  const [journal] = await listRestoreJournals(fixture.codexHome);
  assert.equal(journal.state, "recovery-required");
});

test("Restore v2 compensates a mid-target failure to rolled-back", async () => {
  const fixture = await makeFixture();
  const configBefore = await fs.readFile(path.join(fixture.codexHome, "config.toml"), "utf8");
  const rolloutBefore = await fs.readFile(fixture.rolloutPath, "utf8");

  await assert.rejects(
    () => runRestore({
      codexHome: fixture.codexHome,
      backupDir: fixture.sourceBackup,
      restoreDatabase: false,
      faultInjector: ({ point, targetKind }) => {
        if (point === "after_restore_target_write_before_complete" && targetKind === "config") {
          throw new Error("mid-restore fault");
        }
      }
    }),
    /mid-restore fault/
  );

  assert.equal(await fs.readFile(path.join(fixture.codexHome, "config.toml"), "utf8"), configBefore);
  assert.equal(await fs.readFile(fixture.rolloutPath, "utf8"), rolloutBefore);
  const journals = await listRestoreJournals(fixture.codexHome);
  assert.equal(journals.length, 1);
  assert.equal(journals[0].state, "rolled-back");
  assert.equal((await getStatus({ codexHome: fixture.codexHome })).pendingRecovery, false);
});

test("Restore v2 reconciles committed-pending-ack without rollback", async () => {
  const fixture = await makeFixture();
  const result = await runRestore({
    codexHome: fixture.codexHome,
    backupDir: fixture.sourceBackup,
    restoreDatabase: false,
    faultInjector: ({ point }) => {
      if (point === "after_restore_committed_pending_ack_before_completed") {
        throw new Error("lost final acknowledgement");
      }
    }
  });

  assert.equal(result.restoreJournalState, "completed");
  assert.equal(result.commitAcknowledgementRecovered, true);
  assert.match(await fs.readFile(path.join(fixture.codexHome, "config.toml"), "utf8"), /apigather/);
  assert.match(await fs.readFile(fixture.rolloutPath, "utf8"), /"model_provider":"apigather"/);
  const journals = await listRestoreJournals(fixture.codexHome);
  assert.equal(journals.length, 1);
  assert.equal(journals[0].state, "completed");
});

test("an explicit same-source Restore resolves an applying crash journal", async () => {
  const fixture = await makeFixture();
  const crashed = await spawnCrash(
    fixture.codexHome,
    fixture.sourceBackup,
    "after_restore_target_write_before_complete"
  );
  assert.equal(crashed.signal, null, crashed.stderr);
  assert.equal(crashed.code, 86, crashed.stderr);

  const afterCrash = await getStatus({ codexHome: fixture.codexHome });
  assert.equal(afterCrash.pendingRecovery, true);
  assert.equal(afterCrash.pendingTransactions.some((item) => item.operationKind === "restore"), true);

  const recovered = await runRestore({
    codexHome: fixture.codexHome,
    backupDir: fixture.sourceBackup,
    restoreDatabase: false
  });
  assert.equal(recovered.restoreJournalState, "completed");
  assert.equal(recovered.resolvedOperationIds.length, 1);
  assert.match(await fs.readFile(path.join(fixture.codexHome, "config.toml"), "utf8"), /apigather/);
  assert.match(await fs.readFile(fixture.rolloutPath, "utf8"), /"model_provider":"apigather"/);
  assert.equal((await getStatus({ codexHome: fixture.codexHome })).pendingRecovery, false);

  const journals = await listRestoreJournals(fixture.codexHome);
  const [oldJournal] = journals.filter((journal) => journal.state === "applying");
  assert.ok(oldJournal);
  assert.equal(journals.filter((journal) => journal.state === "completed").length, 1);

  // Resolution admits the new explicit Restore, but does not authorize Prune
  // to delete evidence referenced by the still-nonterminal older journal.
  await pruneBackups(fixture.codexHome, 0);
  await fs.access(fixture.sourceBackup);
  await fs.access(oldJournal.snapshotDir);
});

test("foreign or incomplete Restore leaves a crash journal and its backups protected", async () => {
  const fixture = await makeFixture();
  const crashed = await spawnCrash(
    fixture.codexHome,
    fixture.sourceBackup,
    "after_restore_target_write_before_complete"
  );
  assert.equal(crashed.code, 86, crashed.stderr);
  const [pending] = await listRestoreJournals(fixture.codexHome);
  assert.equal(pending.state, "applying");

  await assert.rejects(
    () => runRestore({
      codexHome: fixture.codexHome,
      backupDir: fixture.sourceBackup,
      restoreDatabase: false,
      restoreSessions: false
    }),
    (error) => error?.code === "RECOVERY_REQUIRED"
  );
  assert.equal((await listRestoreJournals(fixture.codexHome)).length, 1);

  const pruned = await pruneBackups(fixture.codexHome, 0);
  assert.equal(pruned.deletedCount, 0);
  await fs.access(fixture.sourceBackup);
  await fs.access(pending.snapshotDir);
});

test("a completed resolver with a different source cannot hide a pending Restore", async () => {
  const fixture = await makeFixture();
  const crashed = await spawnCrash(
    fixture.codexHome,
    fixture.sourceBackup,
    "after_restore_target_write_before_complete"
  );
  assert.equal(crashed.code, 86, crashed.stderr);
  const [pending] = await listRestoreJournals(fixture.codexHome);
  const pendingBefore = await fs.readFile(pending.filePath);

  const resolverDir = path.join(defaultBackupRoot(fixture.codexHome), "restore-v2-mismatched-resolver");
  await fs.mkdir(resolverDir, { recursive: true });
  const resolver = await RestoreJournal.create(resolverDir, {
    ...pending.prepared,
    operationId: "mismatched-resolver",
    sourceBackup: {
      ...pending.prepared.sourceBackup,
      revision: `${pending.prepared.sourceBackup.revision}-different`
    },
    preRestoreSnapshot: {
      ...pending.prepared.preRestoreSnapshot,
      backupId: "mismatched-resolver-snapshot",
      backupDir: resolverDir
    },
    resolvesOperationIds: [pending.operationId]
  });
  await completeResolverJournal(resolver, pending.prepared, "resolver-post-manifest");

  assert.equal((await getStatus({ codexHome: fixture.codexHome })).pendingRecovery, true);
  await assert.rejects(
    () => runSwitch({ codexHome: fixture.codexHome, provider: "openai" }),
    (error) => error?.code === "RECOVERY_REQUIRED"
      || error?.code === "PENDING_TRANSACTION"
  );
  assert.deepEqual(await fs.readFile(pending.filePath), pendingBefore);
});

test("a completed resolver binds physical source and revision instead of display backupId", async () => {
  const fixture = await makeFixture();
  const crashed = await spawnCrash(
    fixture.codexHome,
    fixture.sourceBackup,
    "after_restore_prepared_before_applying"
  );
  assert.equal(crashed.code, 86, crashed.stderr);
  const [pending] = await listRestoreJournals(fixture.codexHome);
  const pendingBefore = await fs.readFile(pending.filePath);

  const resolverDir = path.join(defaultBackupRoot(fixture.codexHome), "restore-v2-display-id-resolver");
  await fs.mkdir(resolverDir, { recursive: true });
  const resolver = await RestoreJournal.create(resolverDir, {
    ...pending.prepared,
    operationId: "display-id-resolver",
    sourceBackup: {
      ...pending.prepared.sourceBackup,
      backupId: "different-lexical-alias-name"
    },
    preRestoreSnapshot: {
      ...pending.prepared.preRestoreSnapshot,
      backupId: "display-id-resolver-snapshot",
      backupDir: resolverDir
    },
    resolvesOperationIds: [pending.operationId]
  });
  await completeResolverJournal(resolver, pending.prepared, "display-id-resolver-post-manifest");

  assert.equal((await getStatus({ codexHome: fixture.codexHome })).pendingRecovery, false);
  assert.equal(
    (await findPendingTransactions(fixture.codexHome))
      .some((transaction) => transaction.operationId === pending.operationId),
    false
  );
  assert.deepEqual(await fs.readFile(pending.filePath), pendingBefore);
  await pruneBackups(fixture.codexHome, 0);
  await fs.access(fixture.sourceBackup);
  await fs.access(pending.snapshotDir);
});

test("a completed resolver matches real Windows 8.3 and long physical path aliases", {
  skip: process.platform !== "win32"
}, async () => {
  const fixture = await makeFixture();
  const crashed = await spawnCrash(
    fixture.codexHome,
    fixture.sourceBackup,
    "after_restore_prepared_before_applying"
  );
  assert.equal(crashed.code, 86, crashed.stderr);
  const [pending] = await listRestoreJournals(fixture.codexHome);
  assert.equal(pending.state, "prepared");
  const pendingBefore = await fs.readFile(pending.filePath);
  const shortCodexHome = await windowsShortDirectoryPath(pending.prepared.storage.codexHome);
  const shortSourceBackup = await windowsShortDirectoryPath(
    pending.prepared.sourceBackup.backupDir
  );

  const resolverDir = path.join(defaultBackupRoot(fixture.codexHome), "restore-v2-physical-alias-resolver");
  await fs.mkdir(resolverDir, { recursive: true });
  const resolver = await RestoreJournal.create(resolverDir, {
    ...pending.prepared,
    operationId: "physical-alias-resolver",
    sourceBackup: {
      ...pending.prepared.sourceBackup,
      backupDir: shortSourceBackup
    },
    storage: {
      ...pending.prepared.storage,
      codexHome: shortCodexHome
    },
    preRestoreSnapshot: {
      ...pending.prepared.preRestoreSnapshot,
      backupId: "physical-alias-resolver-snapshot",
      backupDir: await fs.realpath(resolverDir),
      revision: "physical-alias-resolver-revision",
      manifestSha256: "physical-alias-resolver-manifest"
    },
    resolvesOperationIds: [pending.operationId]
  });
  await completeResolverJournal(resolver, pending.prepared, "physical-alias-post-manifest");

  assert.equal((await getStatus({ codexHome: fixture.codexHome })).pendingRecovery, false);
  assert.deepEqual(await fs.readFile(pending.filePath), pendingBefore);
});

test("a completed resolver with a different persisted physical Home cannot hide pending Restore", async () => {
  const fixture = await makeFixture();
  const crashed = await spawnCrash(
    fixture.codexHome,
    fixture.sourceBackup,
    "after_restore_prepared_before_applying"
  );
  assert.equal(crashed.code, 86, crashed.stderr);
  const [pending] = await listRestoreJournals(fixture.codexHome);
  const otherPhysicalHome = path.join(fixture.root, "other-physical-home");
  await fs.mkdir(otherPhysicalHome, { recursive: true });
  const resolverDir = path.join(defaultBackupRoot(fixture.codexHome), "restore-v2-other-home-resolver");
  await fs.mkdir(resolverDir, { recursive: true });
  const resolverPrepared = {
    ...pending.prepared,
    operationId: "other-home-resolver",
    storage: {
      ...pending.prepared.storage,
      codexHomePhysical: await fs.realpath(otherPhysicalHome)
    },
    preRestoreSnapshot: {
      ...pending.prepared.preRestoreSnapshot,
      backupId: "other-home-resolver-snapshot",
      backupDir: resolverDir,
      revision: "other-home-resolver-revision",
      manifestSha256: "other-home-resolver-manifest"
    },
    resolvesOperationIds: [pending.operationId]
  };
  const resolver = await RestoreJournal.create(
    resolverDir,
    resolverPrepared
  );
  await completeResolverJournal(resolver, resolverPrepared, "other-home-post-manifest");

  assert.equal((await getStatus({ codexHome: fixture.codexHome })).pendingRecovery, true);
  await assert.rejects(
    () => runSwitch({ codexHome: fixture.codexHome, provider: "openai" }),
    (error) => error?.code === "RECOVERY_REQUIRED"
      || error?.code === "PENDING_TRANSACTION"
  );
});

test("Restore preflight rejects a same-source pending journal bound to another physical Home", async () => {
  const fixture = await makeFixture();
  const crashed = await spawnCrash(
    fixture.codexHome,
    fixture.sourceBackup,
    "after_restore_prepared_before_applying"
  );
  assert.equal(crashed.code, 86, crashed.stderr);
  const [pending] = await listRestoreJournals(fixture.codexHome);
  const otherPhysicalHome = path.join(fixture.root, "foreign-physical-home");
  await fs.mkdir(otherPhysicalHome, { recursive: true });
  const lines = (await fs.readFile(pending.filePath, "utf8")).trimEnd().split(/\r?\n/);
  const prepared = JSON.parse(lines[0]);
  prepared.storage.codexHomePhysical = await fs.realpath(otherPhysicalHome);
  lines[0] = JSON.stringify(prepared);
  await fs.writeFile(pending.filePath, `${lines.join("\n")}\n`, "utf8");
  const journalBefore = await fs.readFile(pending.filePath);
  const backupDirectoriesBefore = (await fs.readdir(defaultBackupRoot(fixture.codexHome))).sort();
  const configBefore = await fs.readFile(path.join(fixture.codexHome, "config.toml"));
  const rolloutBefore = await fs.readFile(fixture.rolloutPath);

  await assert.rejects(
    () => runRestore({
      codexHome: fixture.codexHome,
      backupDir: fixture.sourceBackup,
      restoreDatabase: false
    }),
    (error) => error?.code === "RECOVERY_REQUIRED"
  );

  assert.deepEqual(await fs.readFile(pending.filePath), journalBefore);
  assert.deepEqual((await fs.readdir(defaultBackupRoot(fixture.codexHome))).sort(), backupDirectoriesBefore);
  assert.deepEqual(await fs.readFile(path.join(fixture.codexHome, "config.toml")), configBefore);
  assert.deepEqual(await fs.readFile(fixture.rolloutPath), rolloutBefore);
});

test("a completed resolver without per-target evidence cannot hide a pending Restore", async () => {
  const fixture = await makeFixture();
  const crashed = await spawnCrash(
    fixture.codexHome,
    fixture.sourceBackup,
    "after_restore_prepared_before_applying"
  );
  assert.equal(crashed.code, 86, crashed.stderr);
  const [pending] = await listRestoreJournals(fixture.codexHome);
  const resolverDir = path.join(defaultBackupRoot(fixture.codexHome), "restore-v2-incomplete-resolver");
  await fs.mkdir(resolverDir, { recursive: true });
  const resolver = await RestoreJournal.create(resolverDir, {
    ...pending.prepared,
    operationId: "incomplete-resolver",
    preRestoreSnapshot: {
      ...pending.prepared.preRestoreSnapshot,
      backupId: "incomplete-resolver-snapshot",
      backupDir: resolverDir,
      revision: "incomplete-resolver-revision",
      manifestSha256: "incomplete-resolver-manifest"
    },
    resolvesOperationIds: [pending.operationId]
  });
  await resolver.applying();
  await resolver.committing("incomplete-resolver-post-manifest");
  await resolver.committedPendingAck("incomplete-resolver-post-manifest");
  await resolver.completed();

  const journals = await listRestoreJournals(fixture.codexHome);
  const incomplete = journals.find((journal) => journal.snapshotDir === resolverDir);
  assert.equal(incomplete.invalidTail, true);
  assert.equal((await getStatus({ codexHome: fixture.codexHome })).pendingRecovery, true);
  await assert.rejects(
    () => runSwitch({ codexHome: fixture.codexHome, provider: "openai" }),
    (error) => error?.code === "PENDING_TRANSACTION"
  );
});

test("committed-pending-ack hash drift becomes recovery-required without compensation", async () => {
  const fixture = await makeFixture();
  const crashed = await spawnCrash(
    fixture.codexHome,
    fixture.sourceBackup,
    "after_restore_committed_pending_ack_before_completed"
  );
  assert.equal(crashed.code, 86, crashed.stderr);
  const configPath = path.join(fixture.codexHome, "config.toml");
  await fs.writeFile(configPath, 'model_provider = "external-drift"\n', "utf8");

  await assert.rejects(
    () => runRestore({
      codexHome: fixture.codexHome,
      backupDir: fixture.sourceBackup,
      restoreDatabase: false
    }),
    (error) => error?.code === "RECOVERY_REQUIRED"
  );

  assert.match(await fs.readFile(configPath, "utf8"), /external-drift/);
  const [journal] = await listRestoreJournals(fixture.codexHome);
  assert.equal(journal.state, "recovery-required");
  assert.equal((await getStatus({ codexHome: fixture.codexHome })).pendingRecovery, true);
});

test("committed-pending-ack rejects a rehashed manifest that disagrees with prepared evidence", async () => {
  const fixture = await makeFixture();
  const crashed = await spawnCrash(
    fixture.codexHome,
    fixture.sourceBackup,
    "after_restore_committed_pending_ack_before_completed"
  );
  assert.equal(crashed.code, 86, crashed.stderr);
  const [pending] = await listRestoreJournals(fixture.codexHome);
  const manifestPath = path.join(pending.snapshotDir, RESTORE_SNAPSHOT_MANIFEST_BASENAME);
  const manifest = JSON.parse(await fs.readFile(manifestPath, "utf8"));
  manifest.sourceBackup.revision = `${manifest.sourceBackup.revision}-mismatched`;
  const manifestText = `${JSON.stringify(manifest, null, 2)}\n`;
  const manifestSha256 = createHash("sha256")
    .update(manifestText, "utf8")
    .digest("base64url");
  await fs.writeFile(manifestPath, manifestText, "utf8");
  const lines = (await fs.readFile(pending.filePath, "utf8")).trimEnd().split(/\r?\n/);
  const prepared = JSON.parse(lines[0]);
  prepared.preRestoreSnapshot.manifestSha256 = manifestSha256;
  lines[0] = JSON.stringify(prepared);
  await fs.writeFile(pending.filePath, `${lines.join("\n")}\n`, "utf8");
  const configBefore = await fs.readFile(path.join(fixture.codexHome, "config.toml"));
  const rolloutBefore = await fs.readFile(fixture.rolloutPath);

  await assert.rejects(
    () => runRestore({
      codexHome: fixture.codexHome,
      backupDir: fixture.sourceBackup,
      restoreDatabase: false
    }),
    (error) => error?.code === "RECOVERY_REQUIRED"
  );

  assert.deepEqual(await fs.readFile(path.join(fixture.codexHome, "config.toml")), configBefore);
  assert.deepEqual(await fs.readFile(fixture.rolloutPath), rolloutBefore);
  const [failed] = await listRestoreJournals(fixture.codexHome);
  assert.equal(failed.state, "recovery-required");
  assert.equal(failed.events.some((event) => event.state === "rollback-pending"), false);
});

test("commit acknowledgement revalidates the current physical State DB identity", async () => {
  const fixture = await makeFixture({ withDatabase: true });
  const crashed = await spawnCrash(
    fixture.codexHome,
    fixture.sourceBackup,
    "after_restore_committed_pending_ack_before_completed",
    { withDatabase: true }
  );
  assert.equal(crashed.code, 86, crashed.stderr);
  const [pending] = await listRestoreJournals(fixture.codexHome);
  assert.equal(pending.state, "committed-pending-ack");
  const dbPath = path.join(fixture.codexHome, "sqlite", "state_5.sqlite");
  const stateDbResource = await resolveStateDbLockResource(dbPath);

  await assert.rejects(
    () => acknowledgePendingRestore(pending, {
      stateDbResource,
      resolveStateDbResource: async () => ({
        ...stateDbResource,
        resourceKey: `${stateDbResource.resourceKey}-changed`
      })
    }),
    (error) => error?.code === "RECOVERY_REQUIRED"
  );

  assert.equal(await readDbProvider(fixture.codexHome), "apigather");
  assert.equal((await listRestoreJournals(fixture.codexHome))[0].state, "recovery-required");
});

test("Restore v2 uses online SQLite snapshot for compensation and commit", async () => {
  const fixture = await makeFixture({ withDatabase: true });
  assert.equal(await readDbProvider(fixture.codexHome), "openai");

  await assert.rejects(
    () => runRestore({
      codexHome: fixture.codexHome,
      backupDir: fixture.sourceBackup,
      faultInjector: ({ point, targetKind }) => {
        if (point === "after_restore_target_write_before_complete" && targetKind === "sqlite") {
          throw new Error("sqlite restore fault");
        }
      }
    }),
    /sqlite restore fault/
  );
  assert.equal(await readDbProvider(fixture.codexHome), "openai");
  assert.equal((await listRestoreJournals(fixture.codexHome))[0].state, "rolled-back");

  const restored = await runRestore({
    codexHome: fixture.codexHome,
    backupDir: fixture.sourceBackup
  });
  assert.equal(restored.restoreJournalState, "completed");
  assert.equal(await readDbProvider(fixture.codexHome), "apigather");
});

test("unknown Restore journal schema fails closed and protects referenced evidence", async () => {
  const fixture = await makeFixture();
  const crashed = await spawnCrash(
    fixture.codexHome,
    fixture.sourceBackup,
    "after_restore_target_write_before_complete"
  );
  assert.equal(crashed.code, 86, crashed.stderr);
  const [journal] = await listRestoreJournals(fixture.codexHome);
  const originalText = await fs.readFile(journal.filePath, "utf8");
  const lines = originalText.trimEnd().split("\n");
  const first = JSON.parse(lines[0]);
  first.schemaVersion = 99;
  lines[0] = JSON.stringify(first);
  const unknownText = `${lines.join("\n")}\n`;
  await fs.writeFile(journal.filePath, unknownText, "utf8");

  const status = await getStatus({ codexHome: fixture.codexHome });
  assert.equal(status.pendingRecovery, true);
  await assert.rejects(
    () => runRestore({
      codexHome: fixture.codexHome,
      backupDir: fixture.sourceBackup,
      restoreDatabase: false
    }),
    (error) => error?.code === "RECOVERY_REQUIRED"
  );
  assert.equal(await fs.readFile(journal.filePath, "utf8"), unknownText);

  const pruned = await pruneBackups(fixture.codexHome, 0);
  assert.equal(pruned.deletedCount, 0);
  await fs.access(fixture.sourceBackup);
  await fs.access(journal.snapshotDir);
});

test("truncated Restore prepared record makes Prune a global no-op", async () => {
  const fixture = await makeFixture();
  const crashed = await spawnCrash(
    fixture.codexHome,
    fixture.sourceBackup,
    "after_restore_target_write_before_complete"
  );
  assert.equal(crashed.code, 86, crashed.stderr);
  const [journal] = await listRestoreJournals(fixture.codexHome);
  const truncated = '{"schemaVersion":2,"sourceBackup":';
  await fs.writeFile(journal.filePath, truncated, "utf8");

  const parsed = await readRestoreJournal(journal.filePath);
  assert.equal(parsed.invalidTail, true);
  assert.equal(parsed.protectionReferencesUnverifiable, true);

  const before = await fs.readdir(defaultBackupRoot(fixture.codexHome));
  const pruned = await pruneBackups(fixture.codexHome, 0);
  const after = await fs.readdir(defaultBackupRoot(fixture.codexHome));
  assert.equal(pruned.deletedCount, 0);
  assert.deepEqual(after.sort(), before.sort());
  await fs.access(fixture.sourceBackup);
  await fs.access(journal.snapshotDir);
});
