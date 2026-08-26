import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  applyRestore,
  applySwitch,
  applySync,
  getStatus,
  prepareRestore,
  prepareSwitch,
  prepareSync
} from "../src/service.js";
import { listBackups } from "../src/backup.js";
import { openDatabase } from "../src/sqlite.js";

async function makeFixture() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-plan-apply-"));
  const codexHome = path.join(root, ".codex");
  const rolloutPath = path.join(codexHome, "sessions", "2026", "08", "25", "rollout-a.jsonl");
  const stateDbPath = path.join(codexHome, "sqlite", "state_5.sqlite");
  await fs.mkdir(path.dirname(rolloutPath), { recursive: true });
  await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
  await fs.mkdir(path.dirname(stateDbPath), { recursive: true });
  await fs.writeFile(
    path.join(codexHome, "config.toml"),
    'model_provider = "openai"\nmodel = "gpt-5"\n',
    "utf8"
  );
  const meta = {
    id: "thread-a",
    timestamp: "2026-08-25T00:00:00.000Z",
    cwd: "C:\\AITemp",
    source: "cli",
    cli_version: "0.115.0",
    model_provider: "custom"
  };
  await fs.writeFile(
    rolloutPath,
    `${JSON.stringify({ timestamp: meta.timestamp, type: "session_meta", payload: meta })}\n`,
    "utf8"
  );
  const db = await openDatabase(stateDbPath);
  try {
    db.exec(`
      CREATE TABLE threads (
        id TEXT PRIMARY KEY,
        model_provider TEXT,
        cwd TEXT NOT NULL DEFAULT '',
        archived INTEGER NOT NULL DEFAULT 0,
        first_user_message TEXT NOT NULL DEFAULT '',
        model TEXT
      )
    `);
    db.prepare("INSERT INTO threads (id, model_provider, cwd, archived, first_user_message, model) VALUES (?, ?, ?, ?, ?, ?)")
      .run("thread-a", "custom", "C:\\AITemp", 0, "hello", "old-model");
  } finally {
    db.close();
  }
  return { root, codexHome, rolloutPath, stateDbPath };
}

function backupRoot(codexHome) {
  return path.join(codexHome, "backups_state", "provider-sync");
}

async function backupCount(codexHome) {
  try {
    return (await fs.readdir(backupRoot(codexHome))).length;
  } catch (error) {
    if (error?.code === "ENOENT") return 0;
    throw error;
  }
}

test("prepareSync returns schema v1 summary and applySync consumes it exactly once", async () => {
  const value = await makeFixture();
  try {
    const plan = await prepareSync({ codexHome: value.codexHome, provider: "openai", model: "gpt-5" });
    assert.equal(plan.schemaVersion, 1);
    assert.equal(plan.operation, "sync");
    assert.equal(plan.requiresConfirmation, true);
    assert.equal(plan.target.provider, "openai");
    assert.equal(plan.impact.rolloutFilesToChange, 1);
    assert.match(plan.planId, /^[A-Za-z0-9_-]{32,128}$/);

    const applied = await applySync({ schemaVersion: 1, planId: plan.planId });
    assert.equal(applied.schemaVersion, 1);
    assert.equal(applied.operation, "sync");
    assert.equal(applied.outcome, "completed");
    assert.match(applied.operationId, /^[0-9a-f-]{36}$/);
    assert.equal(applied.result.targetProvider, "openai");
    assert.equal(await backupCount(value.codexHome), 1);

    await assert.rejects(
      applySync({ schemaVersion: 1, planId: plan.planId }),
      (error) => error?.code === "PLAN_EXPIRED"
    );
  } finally {
    await fs.rm(value.root, { recursive: true, force: true });
  }
});

test("Apply publishes one operation id, projects progress, and cancels before backup", async () => {
  const value = await makeFixture();
  const controller = new AbortController();
  const lifecycle = [];
  try {
    const plan = await prepareSync({ codexHome: value.codexHome });
    await assert.rejects(
      applySync(
        { schemaVersion: 1, planId: plan.planId },
        {
          onOperationStarted(started) {
            lifecycle.push({ kind: "started", ...started });
          },
          onProgress(progress) {
            lifecycle.push({ kind: "progress", ...progress });
            if (progress.stage === "create_backup" && progress.status === "start") {
              controller.abort();
            }
          },
          signal: controller.signal
        }
      ),
      (error) => error?.code === "OPERATION_CANCELLED"
        && error?.operationId === lifecycle[0]?.operationId
    );
    assert.equal(lifecycle[0]?.kind, "started");
    assert.equal(lifecycle[0]?.operation, "sync");
    assert.ok(lifecycle.some((event) => event.kind === "progress"));
    assert.equal(await backupCount(value.codexHome), 0);
    assert.equal((await getStatus({ codexHome: value.codexHome })).operationInProgress, null);
  } finally {
    await fs.rm(value.root, { recursive: true, force: true });
  }
});

test("Apply cancellation after rollout mutation preserves rolled-back failure semantics", async () => {
  const value = await makeFixture();
  const controller = new AbortController();
  const rolloutBefore = await fs.readFile(value.rolloutPath);
  let startedOperationId;
  try {
    const plan = await prepareSync({
      codexHome: value.codexHome,
      faultInjector({ point }) {
        if (point === "after_rollout_mutation_before_applied") controller.abort();
      }
    });
    await assert.rejects(
      applySync(
        { schemaVersion: 1, planId: plan.planId },
        {
          signal: controller.signal,
          onOperationStarted(value) { startedOperationId = value.operationId; }
        }
      ),
      (error) => error?.code === "SYNC_FAILED_ROLLED_BACK"
        && error?.operationId === startedOperationId
    );
    assert.deepEqual(await fs.readFile(value.rolloutPath), rolloutBefore);
    const database = await openDatabase(value.stateDbPath, { readOnly: true });
    try {
      assert.equal(
        database.prepare("SELECT model_provider FROM threads WHERE id = ?").get("thread-a").model_provider,
        "custom"
      );
    } finally {
      database.close();
    }
    const status = await getStatus({ codexHome: value.codexHome });
    assert.equal(status.pendingRecovery, false);
    assert.deepEqual(status.pendingTransactions, []);
  } finally {
    await fs.rm(value.root, { recursive: true, force: true });
  }
});

test("applySync rejects config drift under the write locks before backup", async () => {
  const value = await makeFixture();
  try {
    const plan = await prepareSync({ codexHome: value.codexHome });
    await fs.appendFile(path.join(value.codexHome, "config.toml"), "# changed\n", "utf8");
    await assert.rejects(
      applySync({ schemaVersion: 1, planId: plan.planId }),
      (error) => error?.code === "STALE_STATE" && error?.details?.reason === "config"
    );
    assert.equal(await backupCount(value.codexHome), 0);
  } finally {
    await fs.rm(value.root, { recursive: true, force: true });
  }
});

test("applySync rejects rollout and State DB drift before backup", async () => {
  for (const drift of ["rollout", "state-db"]) {
    const value = await makeFixture();
    try {
      const plan = await prepareSync({ codexHome: value.codexHome });
      if (drift === "rollout") {
        await fs.appendFile(value.rolloutPath, '{"type":"event_msg"}\n', "utf8");
      } else {
        const db = await openDatabase(value.stateDbPath);
        try {
          db.prepare("UPDATE threads SET first_user_message = ? WHERE id = ?").run("changed", "thread-a");
        } finally {
          db.close();
        }
      }
      await assert.rejects(
        applySync({ schemaVersion: 1, planId: plan.planId }),
        (error) => error?.code === "STALE_STATE" && error?.details?.reason === drift,
        drift
      );
      assert.equal(await backupCount(value.codexHome), 0, drift);
    } finally {
      await fs.rm(value.root, { recursive: true, force: true });
    }
  }
});

test("profile revision drift is checked after preparation and consumes the plan", async () => {
  const value = await makeFixture();
  try {
    let revision = "profile-r1";
    const plan = await prepareSync({
      codexHome: value.codexHome,
      profileId: "work",
      profileRevision: revision,
      profileResolver: async () => ({
        id: "work",
        revision,
        codexHome: value.codexHome
      })
    });
    revision = "profile-r2";
    await assert.rejects(
      applySync({ schemaVersion: 1, planId: plan.planId }),
      (error) => error?.code === "STALE_STATE" && error?.details?.reason === "profile"
    );
    await assert.rejects(
      applySync({ schemaVersion: 1, planId: plan.planId }),
      (error) => error?.code === "PLAN_EXPIRED"
    );
    assert.equal(await backupCount(value.codexHome), 0);
  } finally {
    await fs.rm(value.root, { recursive: true, force: true });
  }
});

test("status returns the last complete snapshot with operation metadata while Apply is paused", async () => {
  const value = await makeFixture();
  let entered;
  let release;
  const enteredPromise = new Promise((resolve) => { entered = resolve; });
  const releasePromise = new Promise((resolve) => { release = resolve; });
  try {
    const plan = await prepareSync({
      codexHome: value.codexHome,
      faultInjector: async ({ point }) => {
        if (point === "before_backup") {
          entered();
          await releasePromise;
        }
      }
    });
    const applyPromise = applySync({ schemaVersion: 1, planId: plan.planId });
    await enteredPromise;
    const status = await getStatus({ codexHome: value.codexHome });
    assert.equal(status.rolloutScanComplete, true);
    assert.equal(status.operationInProgress.operation, "sync");
    assert.equal(status.operationInProgress.actor, "manual");
    assert.match(status.operationInProgress.operationId, /^[0-9a-f-]{36}$/);
    release();
    await applyPromise;
    assert.equal((await getStatus({ codexHome: value.codexHome })).operationInProgress, null);
  } finally {
    release?.();
    await fs.rm(value.root, { recursive: true, force: true });
  }
});

test("two concurrent Apply calls for one plan start exactly one operation", async () => {
  const value = await makeFixture();
  try {
    const plan = await prepareSync({ codexHome: value.codexHome });
    const settled = await Promise.allSettled([
      applySync({ schemaVersion: 1, planId: plan.planId }),
      applySync({ schemaVersion: 1, planId: plan.planId })
    ]);
    assert.equal(settled.filter((entry) => entry.status === "fulfilled").length, 1);
    const rejected = settled.find((entry) => entry.status === "rejected");
    assert.equal(rejected.reason.code, "PLAN_EXPIRED");
    assert.equal(await backupCount(value.codexHome), 1);
  } finally {
    await fs.rm(value.root, { recursive: true, force: true });
  }
});

test("different Codex Homes sharing one State DB contend before the losing backup", async () => {
  const value = await makeFixture();
  const secondHome = path.join(value.root, "second-codex-home");
  const secondRollout = path.join(secondHome, "sessions", "2026", "08", "25", "rollout-b.jsonl");
  let entered;
  let release;
  const enteredPromise = new Promise((resolve) => { entered = resolve; });
  const releasePromise = new Promise((resolve) => { release = resolve; });
  try {
    await fs.mkdir(path.dirname(secondRollout), { recursive: true });
    await fs.mkdir(path.join(secondHome, "archived_sessions"), { recursive: true });
    await fs.writeFile(
      path.join(secondHome, "config.toml"),
      `model_provider = "openai"\nsqlite_home = ${JSON.stringify(path.dirname(value.stateDbPath))}\n`,
      "utf8"
    );
    await fs.writeFile(secondRollout, `${JSON.stringify({
      timestamp: "2026-08-25T00:00:00.000Z",
      type: "session_meta",
      payload: {
        id: "thread-b",
        timestamp: "2026-08-25T00:00:00.000Z",
        cwd: "C:\\AITemp",
        source: "cli",
        cli_version: "0.115.0",
        model_provider: "custom"
      }
    })}\n`, "utf8");
    const secondConfigBefore = await fs.readFile(path.join(secondHome, "config.toml"));
    const secondRolloutBefore = await fs.readFile(secondRollout);

    const firstPlan = await prepareSync({
      codexHome: value.codexHome,
      faultInjector: async ({ point }) => {
        if (point === "before_backup") {
          entered();
          await releasePromise;
        }
      }
    });
    const firstApply = applySync({ schemaVersion: 1, planId: firstPlan.planId });
    await enteredPromise;

    const secondPlan = await prepareSync({ codexHome: secondHome });
    await assert.rejects(
      applySync({ schemaVersion: 1, planId: secondPlan.planId }),
      (error) => error?.code === "OPERATION_BUSY" && error?.details?.busyScope === "state-db"
    );
    assert.equal(await backupCount(secondHome), 0);
    assert.deepEqual(await fs.readFile(path.join(secondHome, "config.toml")), secondConfigBefore);
    assert.deepEqual(await fs.readFile(secondRollout), secondRolloutBefore);

    release();
    await firstApply;
  } finally {
    release?.();
    await fs.rm(value.root, { recursive: true, force: true });
  }
});

test("prepareSwitch/applySwitch preserves all three model-mode intents in consumed plans", async () => {
  for (const fixture of [
    { expectedMode: "provider-default", options: {}, expectedModel: "relay-model" },
    { expectedMode: "keep-root-model", options: { keepRootModel: true }, expectedModel: "gpt-5" },
    { expectedMode: "explicit", options: { model: "explicit-model" }, expectedModel: "explicit-model" }
  ]) {
    const value = await makeFixture();
    try {
      await fs.appendFile(
        path.join(value.codexHome, "config.toml"),
        '\n[model_providers.relay]\nmodel = "relay-model"\nbase_url = "https://example.invalid"\n',
        "utf8"
      );
      const plan = await prepareSwitch({
        codexHome: value.codexHome,
        provider: "relay",
        ...fixture.options
      });
      assert.equal(plan.operation, "switch");
      assert.equal(plan.target.modelMode, fixture.expectedMode);
      const applied = await applySwitch({ schemaVersion: 1, planId: plan.planId });
      assert.equal(applied.outcome, "completed");
      const configText = await fs.readFile(path.join(value.codexHome, "config.toml"), "utf8");
      assert.match(configText, /^model_provider = "relay"/m);
      assert.match(configText, new RegExp(`^model = "${fixture.expectedModel}"`, "m"));
    } finally {
      await fs.rm(value.root, { recursive: true, force: true });
    }
  }
});

test("Switch re-resolves the authoritative State DB under the Home lock", async () => {
  const value = await makeFixture();
  const legacyStateDbPath = path.join(value.codexHome, "state_5.sqlite");
  let newDefaultBytes = null;
  try {
    await fs.appendFile(
      path.join(value.codexHome, "config.toml"),
      '\n[model_providers.relay]\nmodel = "relay-model"\nbase_url = "https://example.invalid"\n',
      "utf8"
    );
    await fs.rename(value.stateDbPath, legacyStateDbPath);
    await fs.rm(path.dirname(value.stateDbPath), { recursive: true, force: true });
    assert.equal(
      (await getStatus({ codexHome: value.codexHome })).stateDbLocation.path,
      legacyStateDbPath
    );
    const configBefore = await fs.readFile(path.join(value.codexHome, "config.toml"));
    const rolloutBefore = await fs.readFile(value.rolloutPath);
    const legacyBefore = await fs.readFile(legacyStateDbPath);

    const plan = await prepareSwitch({
      codexHome: value.codexHome,
      provider: "relay",
      faultInjector: async ({ point }) => {
        if (point !== "after_switch_storage_preflight") return;
        await fs.mkdir(path.dirname(value.stateDbPath), { recursive: true });
        await fs.copyFile(legacyStateDbPath, value.stateDbPath);
        newDefaultBytes = await fs.readFile(value.stateDbPath);
      }
    });

    await assert.rejects(
      applySwitch({ schemaVersion: 1, planId: plan.planId }),
      (error) => error?.code === "STALE_STATE" && error?.details?.reason === "storage"
    );
    assert.equal(await backupCount(value.codexHome), 0);
    assert.deepEqual(await fs.readFile(path.join(value.codexHome, "config.toml")), configBefore);
    assert.deepEqual(await fs.readFile(value.rolloutPath), rolloutBefore);
    assert.deepEqual(await fs.readFile(legacyStateDbPath), legacyBefore);
    assert.deepEqual(await fs.readFile(value.stateDbPath), newDefaultBytes);
  } finally {
    await fs.rm(value.root, { recursive: true, force: true });
  }
});

test("prepareRestore/applyRestore binds a managed backup and rejects backup drift before mutation", async () => {
  const value = await makeFixture();
  try {
    const syncPlan = await prepareSync({ codexHome: value.codexHome, provider: "openai" });
    await applySync({ schemaVersion: 1, planId: syncPlan.planId });
    const inventory = await listBackups(value.codexHome);
    const backup = inventory.backups[0];
    assert.ok(backup?.id);

    const stalePlan = await prepareRestore({
      codexHome: value.codexHome,
      backupId: backup.id,
      restoreConfig: false,
      restoreDatabase: true,
      restoreSessions: true
    });
    await fs.appendFile(path.join(backup.path, "metadata.json"), "\n", "utf8");
    await assert.rejects(
      applyRestore({ schemaVersion: 1, planId: stalePlan.planId }),
      (error) => error?.code === "STALE_STATE" && error?.details?.reason === "backup"
    );
    const dbBefore = await fs.readFile(value.stateDbPath);

    // Restore the exact metadata bytes from the immutable source plan fixture,
    // then prepare a fresh plan and apply it.
    const metadataText = await fs.readFile(path.join(backup.path, "metadata.json"), "utf8");
    await fs.writeFile(path.join(backup.path, "metadata.json"), metadataText.trimEnd(), "utf8");
    const freshPlan = await prepareRestore({
      codexHome: value.codexHome,
      backupId: backup.id,
      restoreConfig: false,
      restoreDatabase: true,
      restoreSessions: true
    });
    const restored = await applyRestore({ schemaVersion: 1, planId: freshPlan.planId });
    assert.equal(restored.operation, "restore");
    assert.equal(restored.outcome, "completed");
    assert.notDeepEqual(await fs.readFile(value.stateDbPath), dbBefore);
    const db = await openDatabase(value.stateDbPath);
    try {
      assert.equal(
        db.prepare("SELECT model_provider FROM threads WHERE id = ?").get("thread-a").model_provider,
        "custom"
      );
    } finally {
      db.close();
    }
  } finally {
    await fs.rm(value.root, { recursive: true, force: true });
  }
});
