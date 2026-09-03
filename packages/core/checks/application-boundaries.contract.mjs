import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { createConcurrencyGuard } from "../src/application/concurrency-guard.js";
import { createOperationRuntime } from "../src/application/operation-runtime.js";
import { createPlanApplyGuard } from "../src/application/plan-apply-guard.js";
import { createCodexStorage } from "../src/infrastructure/codex-storage.js";
import { createRestoreRecovery } from "../src/infrastructure/restore-recovery.js";
import { createSqliteTransaction } from "../src/infrastructure/sqlite-transaction.js";
import { createUndoBackup } from "../src/infrastructure/undo-backup.js";

const coreRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

test("application control modules never import storage or the legacy root", async () => {
  const applicationRoot = path.join(coreRoot, "src", "application");
  const relativePaths = (await fs.readdir(applicationRoot))
    .filter((name) => name.endsWith(".js"))
    .map((name) => path.join("src", "application", name));
  for (const relativePath of relativePaths) {
    const source = await fs.readFile(path.join(coreRoot, relativePath), "utf8");
    assert.doesNotMatch(source, /(?:\.\.\/)+src\//);
    assert.doesNotMatch(source, /node:(?:fs|sqlite)/);
    assert.doesNotMatch(source, /src\/(?:backup|transaction-journal|session-files|state-db-lock)\.js/);
  }
});

test("Core application and infrastructure never route through the legacy public API", async () => {
  const sourceFiles = [];
  for (const folder of ["application", "infrastructure"]) {
    for (const name of await fs.readdir(path.join(coreRoot, "src", folder))) {
      if (name.endsWith(".js")) sourceFiles.push(path.join("src", folder, name));
    }
  }
  sourceFiles.push(path.join("src", "index.js"));
  for (const relativePath of sourceFiles) {
    const source = await fs.readFile(path.join(coreRoot, relativePath), "utf8");
    assert.equal(source.includes("src/public-api.js"), false, relativePath);
  }
});

test("compatibility runtime is a thin adapter and concrete use cases own their ports", async () => {
  const compat = await fs.readFile(path.join(coreRoot, "src", "application", "service-runtime.js"), "utf8");
  assert.doesNotMatch(compat, /codexStorage\.|acquireLock\(|executeRestoreV2\(|collectProviderChanges\(/);
  for (const [relativePath, ports] of [
    ["src/application/status.js", ["config", "sessions", "stateDb", "globalState"]],
    ["src/application/provider-sync.js", ["config", "sessions", "stateDb"]],
    ["src/application/repair.js", ["config", "sessions", "stateDb", "globalState"]],
    ["src/application/ordinary-write-runtime.js", ["config", "stateDb"]]
  ]) {
    const source = await fs.readFile(path.join(coreRoot, relativePath), "utf8");
    for (const port of ports) assert.match(source, new RegExp(`codexStorage\\.${port}`), relativePath);
  }
  const ordinary = await fs.readFile(path.join(coreRoot, "src", "application", "ordinary-write-runtime.js"), "utf8");
  assert.doesNotMatch(ordinary, /collectProviderChanges|collectRepairChanges|applySessionChanges|syncWorkspaceRoots/);
  assert.doesNotMatch(ordinary, /sqliteTransaction|sqliteProviderRowsToChange|repairSqliteRowsToChange/);
  assert.doesNotMatch(ordinary, /readCurrentProviderFromConfigText|readRootModelFromConfigText|splitLockedSessionChanges/);
  assert.doesNotMatch(ordinary, /readSqliteProviderCounts|readSqliteRepairStats|readWorkspaceRootRepairStats/);
  assert.doesNotMatch(ordinary, /DEFAULT_PROVIDER|targetModel|workspaceRoots|configMutationExpected|afterBackup/);
  assert.doesNotMatch(ordinary, /codexStorage\.(?:sessions|globalState)/);
});

test("CoreFacade delegates into concrete use cases without a handler round-trip", async () => {
  const facade = await fs.readFile(path.join(coreRoot, "src", "index.js"), "utf8");
  const application = await fs.readFile(
    path.join(coreRoot, "src", "application", "core-application.js"),
    "utf8"
  );
  assert.match(facade, /const application = createCoreApplication\(\)/);
  assert.match(facade, /application\.prepareSync/);
  assert.doesNotMatch(facade, /application\/service-runtime|application\/watch-runtime/);
  assert.match(application, /createProviderSyncUseCase\(\)/);
  assert.doesNotMatch(application, /createCoreApplication\(handlers\)|UseCase\(handlers\)/);
  for (const relativePath of [
    "src/application/provider-sync.js",
    "src/application/provider-switch.js",
    "src/application/repair.js",
    "src/application/restore.js",
    "src/application/status.js",
    "src/application/watch.js"
  ]) {
    const source = await fs.readFile(path.join(coreRoot, relativePath), "utf8");
    assert.doesNotMatch(source, /handlers\./, relativePath);
  }
});

test("Switch and Watch call the internal ProviderSync runtime without reversing into CoreFacade", async () => {
  for (const relativePath of ["src/application/provider-switch.js", "src/application/watch.js"]) {
    const source = await fs.readFile(path.join(coreRoot, relativePath), "utf8");
    assert.doesNotMatch(source, /CoreFacade|createCoreFacade|\.\/index/);
  }
  const switchUseCase = await fs.readFile(path.join(coreRoot, "src", "application", "provider-switch.js"), "utf8");
  const watch = await fs.readFile(path.join(coreRoot, "src", "application", "watch-runtime.js"), "utf8");
  assert.match(switchUseCase, /executeProviderSyncMutation/);
  assert.match(watch, /prepareWatchProviderSync[\s\S]*?applyWatchProviderSync/);
});

test("CodexStorage is an immutable composition of four independent ports", () => {
  const storage = createCodexStorage({
    config: { read() {} },
    sessions: { scan() {} },
    stateDb: { query() {} },
    globalState: { update() {} }
  });
  assert.equal(Object.isFrozen(storage), true);
  assert.equal(Object.isFrozen(storage.config), true);
  assert.deepEqual(Object.keys(storage).sort(), ["config", "globalState", "sessions", "stateDb"]);
  assert.throws(() => createCodexStorage({}), /config port is missing/);
});

test("ordinary SQLite, UndoBackup, and RestoreRecovery infrastructure stay independently injectable", async () => {
  const calls = [];
  const sqlite = createSqliteTransaction({
    updateProvider(...args) { calls.push(["provider", ...args]); },
    repair(...args) { calls.push(["repair", ...args]); }
  });
  const backup = createUndoBackup({
    capture(...args) { calls.push(["backup", ...args]); },
    refreshInventory(...args) { calls.push(["refresh", ...args]); }
  });
  const restore = createRestoreRecovery({
    resolveResource(...args) { calls.push(["resource", ...args]); },
    acknowledge(...args) { calls.push(["ack", ...args]); },
    execute(...args) { calls.push(["restore", ...args]); }
  });
  sqlite.updateProvider("db", "provider");
  sqlite.repair("db", "targets");
  backup.capture("snapshot");
  backup.refreshInventory("snapshot");
  restore.resolveResource("db");
  restore.acknowledge("journal");
  restore.execute("restore");
  assert.deepEqual(calls.map(([name]) => name), [
    "provider", "repair", "backup", "refresh", "resource", "ack", "restore"
  ]);
  assert.equal(Object.isFrozen(sqlite), true);
  assert.equal(Object.isFrozen(backup), true);
  assert.equal(Object.isFrozen(restore), true);
});

test("OperationRuntime owns Plan consumption and observer-safe operation lifecycle", async () => {
  const entries = new Map();
  const coordinatorEvents = [];
  const coordinator = {
    registerManualIntent(codexHome, planId) {
      coordinatorEvents.push(["intent", codexHome, planId]);
    },
    begin(codexHome, operation) {
      coordinatorEvents.push(["begin", codexHome, operation]);
      return { operationId: "operation-1" };
    },
    end(codexHome, operationId) {
      coordinatorEvents.push(["end", codexHome, operationId]);
    },
    waitForManualOperation(codexHome) {
      return { codexHome };
    }
  };
  const concurrencyGuard = createConcurrencyGuard({ coordinator });
  const planApplyGuard = createPlanApplyGuard({
    concurrencyGuard,
    planLedger: {
      issue(operation, summary, internal) {
        const plan = { ...summary, planId: "plan-1", expiresAt: "2099-01-01T00:00:00.000Z" };
        entries.set(plan.planId, { operation, internal });
        return plan;
      },
      consume(input, operation) {
        const entry = entries.get(input.planId);
        entries.delete(input.planId);
        assert.equal(entry.operation, operation);
        return entry;
      }
    }
  });
  class TestCoreError extends Error {
    constructor(code, message, options = {}) {
      super(message);
      this.code = code;
      Object.assign(this, options);
    }
  }
  let statusRefreshes = 0;
  const runtime = createOperationRuntime({
    planApplyGuard,
    concurrencyGuard,
    CoreError: TestCoreError,
    async getStatus() {
      statusRefreshes += 1;
      throw new Error("observational refresh failure");
    },
    normalizeCodexHome: (value) => path.resolve(value),
    toOperationResult: (operation, operationId, result) => ({ operation, operationId, result }),
    emitProgress(observer, event) {
      try { observer?.(event); } catch {}
    }
  });

  const plan = runtime.issuePreparedPlan("sync", { target: {} }, {
    actor: "manual",
    codexHome: "C:\\codex",
    platform: "win32",
    executionOptions: {
      onProgress() { throw new Error("observer"); }
    },
    expectedPlanState: {},
    statusOptions: {}
  });
  let started = 0;
  const result = await runtime.applyPrepared(
    { schemaVersion: 1, planId: plan.planId },
    "sync",
    async ({ onProgress }) => {
      onProgress({ stage: "write", status: "start" });
      return { changed: 1 };
    },
    {
      onOperationStarted() {
        started += 1;
        throw new Error("observer");
      },
      onProgress() { throw new Error("observer"); }
    }
  );

  assert.equal(result.operationId, "operation-1");
  assert.deepEqual(result.result, { changed: 1 });
  assert.equal(started, 1);
  assert.equal(statusRefreshes, 1);
  assert.deepEqual(coordinatorEvents.map(([event]) => event), ["intent", "begin", "end"]);
  assert.deepEqual(runtime.waitForManualOperationEnd({ codexHome: "C:\\codex", platform: "win32" }), {
    codexHome: path.resolve("C:\\codex")
  });
});

test("OperationRuntime maps a pre-commit AbortError without losing correlation", async () => {
  const concurrencyGuard = createConcurrencyGuard({
    coordinator: {
      registerManualIntent() {},
      begin() { return { operationId: "operation-cancelled" }; },
      end() {},
      waitForManualOperation() { return null; }
    }
  });
  const planApplyGuard = createPlanApplyGuard({
    concurrencyGuard,
    planLedger: {
      issue() { return {}; },
      consume() {
        return {
          internal: {
            actor: "manual",
            codexHome: "C:\\codex",
            platform: "win32",
            executionOptions: {},
            expectedPlanState: {},
            statusOptions: {}
          }
        };
      }
    }
  });
  class TestCoreError extends Error {
    constructor(code, message, options = {}) {
      super(message);
      this.code = code;
      Object.assign(this, options);
    }
  }
  const runtime = createOperationRuntime({
    planApplyGuard,
    concurrencyGuard,
    CoreError: TestCoreError,
    async getStatus() {},
    normalizeCodexHome: (value) => String(value),
    toOperationResult: () => ({}),
    emitProgress() {}
  });
  await assert.rejects(
    runtime.applyPrepared(
      { schemaVersion: 1, planId: "plan" },
      "sync",
      async () => {
        const error = new Error("cancelled");
        error.name = "AbortError";
        error.code = "ABORT_ERR";
        throw error;
      }
    ),
    (error) => error.code === "OPERATION_CANCELLED"
      && error.operationId === "operation-cancelled"
  );
});
