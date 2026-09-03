import assert from "node:assert/strict";
import nodeFs from "node:fs";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import * as core from "../src/index.js";
import { toPublicProgress } from "../src/progress.js";

const EXPECTED_METHODS = [
  "applyRepair",
  "applyRestore",
  "applySwitch",
  "applySync",
  "getDiagnostics",
  "getHistorySession",
  "getStatus",
  "getWatchStatus",
  "listBackups",
  "listHistory",
  "prepareRepair",
  "prepareRestore",
  "prepareSwitch",
  "prepareSync",
  "pruneBackups",
  "startWatch",
  "stopWatch"
];

test("Core workspace exposes only the stable vNext method surface", () => {
  assert.deepEqual(Object.keys(core), ["createCoreFacade"]);
  const facade = core.createCoreFacade({
    resolveProfile: async ({ profileId }) => ({
      id: profileId,
      revision: "r1",
      codexHome: process.cwd()
    })
  });
  assert.deepEqual(Object.keys(facade).sort(), EXPECTED_METHODS);
  assert.equal("runSync" in core, false);
  assert.equal("resolveStorageLayout" in core, false);
});

test("Core progress projection enforces the shared DTO numeric ranges", () => {
  assert.deepEqual(
    toPublicProgress({ stage: "scan", status: "running", progress: 0.5, count: 2 }),
    { stage: "scan", status: "running", progress: 0.5, count: 2 }
  );
  assert.deepEqual(
    toPublicProgress({ stage: "scan", status: "running", progress: 1.1, count: -1 }),
    { stage: "scan", status: "running" }
  );
  assert.deepEqual(
    toPublicProgress({ stage: "scan", status: "running", progress: -0.1, count: 1.5 }),
    { stage: "scan", status: "running" }
  );
  assert.equal(toPublicProgress({ stage: "", status: "running" }), null);
});

test("Core workspace owns orchestration and root service remains compatibility-only", async () => {
  const source = await fs.readFile(new URL("../src/index.js", import.meta.url), "utf8");
  const application = await fs.readFile(new URL("../src/application/core-application.js", import.meta.url), "utf8");
  const serviceRuntime = await fs.readFile(new URL("../src/application/service-runtime.js", import.meta.url), "utf8");
  const storagePorts = await fs.readFile(new URL("../src/infrastructure/node-core-ports.js", import.meta.url), "utf8");
  const rootService = await fs.readFile(new URL("../../../src/service.js", import.meta.url), "utf8");
  const declarations = await fs.readFile(new URL("../src/index.d.ts", import.meta.url), "utf8");
  const rootDeclarations = await fs.readFile(
    new URL("../../../src/public-api.d.ts", import.meta.url),
    "utf8"
  );
  assert.doesNotMatch(source, /src\/public-api\.js/);
  assert.doesNotMatch(source, /src\/(service|locking|backup|history|watch)\.js/);
  assert.doesNotMatch(source, /application\/(service|watch)-runtime\.js/);
  assert.match(source, /application\.prepareSync/);
  assert.match(application, /createProviderSyncUseCase\(\)/);
  assert.doesNotMatch(application, /UseCase\(handlers\)/);
  assert.match(serviceRuntime, /codexStorage\.config/);
  assert.match(serviceRuntime, /codexStorage\.sessions/);
  assert.match(serviceRuntime, /codexStorage\.stateDb/);
  assert.match(serviceRuntime, /codexStorage\.globalState/);
  assert.match(storagePorts, /src\/config-file\.js/);
  assert.match(rootService, /packages\/core\/src\/application\/service-runtime\.js/);
  assert.doesNotMatch(rootService, /function\s+runSyncCore/);
  assert.doesNotMatch(source, /faultInjector/);
  assert.doesNotMatch(declarations, /faultInjector/);
  assert.doesNotMatch(rootDeclarations, /faultInjector/);
});

test("profile resolution fails closed before a Core path can be selected", async () => {
  let calls = 0;
  const facade = core.createCoreFacade({
    resolveProfile: async ({ profileId }) => {
      calls += 1;
      return {
        id: `${profileId}-wrong`,
        revision: "r1",
        codexHome: process.cwd()
      };
    }
  });
  await assert.rejects(
    facade.getStatus({ profile: { profileId: "selected" } }),
    (error) => error?.code === "INVALID_INPUT"
  );
  assert.equal(calls, 1);
});

test("trusted profile selection never falls back to the process default Codex Home", async () => {
  const testRoot = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-core-facade-"));
  const defaultHome = path.join(testRoot, "default-home");
  const selectedHome = path.join(testRoot, "selected-home");
  const originalCodexHome = process.env.CODEX_HOME;
  try {
    for (const home of [defaultHome, selectedHome]) {
      await fs.mkdir(path.join(home, "sessions"), { recursive: true });
      await fs.mkdir(path.join(home, "archived_sessions"), { recursive: true });
      await fs.mkdir(path.join(home, "sqlite"), { recursive: true });
    }
    await fs.writeFile(path.join(defaultHome, "config.toml"), 'model_provider = "wrong-default"\n');
    await fs.writeFile(path.join(selectedHome, "config.toml"), 'model_provider = "openai"\n');
    await fs.writeFile(path.join(selectedHome, "sessions", "rollout-synthetic.jsonl"), `${JSON.stringify({
      type: "session_meta",
      timestamp: "2026-08-25T00:00:00.000Z",
      payload: {
        id: "synthetic-session",
        title: "Synthetic session",
        cwd: path.join(selectedHome, "private-project"),
        model_provider: "relay",
        encrypted_content: "synthetic-ciphertext"
      }
    })}\n`);
    process.env.CODEX_HOME = defaultHome;
    const selectors = [];
    const facade = core.createCoreFacade({
      resolveProfile: async (selector) => {
        selectors.push(selector);
        return {
          id: "selected",
          revision: "selected-r1",
          codexHome: selectedHome
        };
      }
    });
    const input = { profile: { profileId: "selected", profileRevision: "selected-r1" } };
    const status = await facade.getStatus(input);
    const backups = await facade.listBackups(input);
    const history = await facade.listHistory(input);
    const diagnostics = await facade.getDiagnostics(input);
    const pruned = await facade.pruneBackups({ ...input, keepCount: 1 });
    const plan = await facade.prepareSync({ ...input, keepCount: 1 });
    assert.equal(status.currentProvider, "openai");
    assert.deepEqual(status.profile, { id: "selected", revision: "selected-r1" });
    assert.equal(status.codexHomeSource, "profile");
    assert.equal("codexHome" in status, false);
    assert.equal("sqliteHome" in status, false);
    assert.deepEqual(backups, { backups: [] });
    assert.equal(history.sessions.length, 1);
    assert.equal(history.sessions[0].id, "synthetic-session");
    assert.equal(history.sessions[0].messageCount, 0);
    assert.equal(history.sessions[0].messageCountKnown, false);
    assert.equal("cwd" in history.sessions[0], false);
    assert.deepEqual(plan.warnings, []);
    assert.doesNotMatch(JSON.stringify(plan), /private-project|synthetic-ciphertext/);
    assert.equal(diagnostics.storage.sqliteHomeSource, "default");
    assert.equal("codexHome" in diagnostics.storage, false);
    assert.deepEqual(Object.keys(diagnostics.runtime).sort(), ["arch", "node", "platform"]);
    assert.deepEqual(
      Object.keys(diagnostics.storage).sort(),
      ["sqliteHomeSource", "sqliteSupported", "stateDbFound"]
    );
    assert.deepEqual(
      Object.keys(diagnostics.provider).sort(),
      ["configured", "current", "implicit", "rolloutCounts", "sqliteCounts"]
    );
    assert.deepEqual(
      Object.keys(diagnostics.issues).sort(),
      [
        "cwdRowsNeedingRepair",
        "encryptedContentFiles",
        "rolloutModelFilesNeedingRepair",
        "rootModelAvailable",
        "sqliteModelRowsNeedingRepair",
        "userEventRowsNeedingRepair",
        "workspaceRootsNeedingRepair"
      ]
    );
    assert.deepEqual(
      Object.keys(diagnostics.safety).sort(),
      [
        "lockedRolloutCount",
        "operationInProgress",
        "pendingRecovery",
        "pendingTransactions",
        "projectThreadVisibilityAvailable",
        "rolloutScanComplete",
        "storageRevision"
      ]
    );
    assert.doesNotMatch(
      JSON.stringify(diagnostics),
      new RegExp(selectedHome.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i")
    );
    assert.doesNotMatch(JSON.stringify(diagnostics), /private-project|synthetic-ciphertext/i);
    assert.equal(pruned.deletedCount, 0);
    assert.equal(selectors.length, 6);
    assert.equal(selectors.every((selector) => selector.profileId === "selected"), true);
  } finally {
    if (originalCodexHome === undefined) delete process.env.CODEX_HOME;
    else process.env.CODEX_HOME = originalCodexHome;
    await fs.rm(testRoot, { recursive: true, force: true });
  }
});

test("public Status and Provider Sync preparation read rollout metadata only", async () => {
  const testRoot = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-core-status-metadata-"));
  const codexHome = path.join(testRoot, "codex-home");
  const rollout = path.join(codexHome, "sessions", "rollout-large.jsonl");
  const originalCreateReadStream = nodeFs.createReadStream;
  const originalReadFile = nodeFs.promises.readFile;
  let streamBodyReadAttempts = 0;
  let revisionBodyReadAttempts = 0;
  try {
    await fs.mkdir(path.dirname(rollout), { recursive: true });
    await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
    await fs.writeFile(path.join(codexHome, "config.toml"), 'model_provider = "openai"\n');
    await fs.writeFile(rollout, [
      JSON.stringify({
        type: "session_meta",
        timestamp: "2026-08-28T00:00:00.000Z",
        payload: { id: "large", cwd: "C:\\private", model_provider: "relay" }
      }),
      JSON.stringify({ type: "event_msg", payload: { type: "user_message", message: "private body" } }),
      JSON.stringify({ type: "turn_context", payload: { model: "private-model" } })
    ].join("\n") + "\n");

    nodeFs.createReadStream = ((filePath, ...args) => {
      if (path.resolve(String(filePath)) === path.resolve(rollout)) {
        streamBodyReadAttempts += 1;
        throw new Error("rollout body scan sentinel");
      }
      return originalCreateReadStream.call(nodeFs, filePath, ...args);
    });
    nodeFs.promises.readFile = (async (filePath, ...args) => {
      if (path.resolve(String(filePath)) === path.resolve(rollout)) {
        revisionBodyReadAttempts += 1;
        throw new Error("rollout body revision sentinel");
      }
      return originalReadFile.call(nodeFs.promises, filePath, ...args);
    });

    const facade = core.createCoreFacade({
      resolveProfile: async () => ({
        id: "default",
        revision: "r1",
        codexHome
      })
    });
    const input = { profile: { profileId: "default", profileRevision: "r1" } };
    const status = await facade.getStatus(input);

    assert.equal(status.rolloutCounts.sessions.relay, 1);
    assert.equal(status.rolloutScanComplete, true);
    assert.equal(streamBodyReadAttempts, 0);
    assert.equal(revisionBodyReadAttempts, 0);
    assert.doesNotMatch(JSON.stringify(status), /private body|private-model|C:\\private/);

    nodeFs.promises.readFile = originalReadFile;
    const plan = await facade.prepareSync({ ...input, keepCount: 1 });
    assert.equal(plan.operation, "sync");
    assert.equal("providerSync" in plan, false);
    assert.equal(streamBodyReadAttempts, 0);
  } finally {
    nodeFs.createReadStream = originalCreateReadStream;
    nodeFs.promises.readFile = originalReadFile;
    await fs.rm(testRoot, { recursive: true, force: true });
  }
});

test("public Status bounds malformed oversized session metadata and fails alignment closed", async () => {
  const testRoot = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-core-status-limit-"));
  const codexHome = path.join(testRoot, "codex-home");
  try {
    await fs.mkdir(path.join(codexHome, "sessions"), { recursive: true });
    await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
    await fs.writeFile(path.join(codexHome, "config.toml"), 'model_provider = "openai"\n');
    await fs.writeFile(
      path.join(codexHome, "sessions", "rollout-oversized.jsonl"),
      Buffer.alloc((1024 * 1024) + 1, 0x78)
    );
    const facade = core.createCoreFacade({
      resolveProfile: async () => ({ id: "default", revision: "r1", codexHome })
    });

    const status = await facade.getStatus({
      profile: { profileId: "default", profileRevision: "r1" }
    });

    assert.equal(status.rolloutScanComplete, false);
    assert.equal(status.alignment.aligned, false);
    assert.deepEqual(status.rolloutCounts.sessions, {});
    assert.deepEqual(status.lockedRolloutFiles, []);
  } finally {
    await fs.rm(testRoot, { recursive: true, force: true });
  }
});

test("an unverifiable Home lock remains a valid fail-closed public StatusSnapshot", async () => {
  const testRoot = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-core-blocked-status-"));
  const codexHome = path.join(testRoot, "codex-home");
  const lockDir = path.join(codexHome, "tmp", "provider-sync.lock");
  try {
    await fs.mkdir(path.join(codexHome, "sessions"), { recursive: true });
    await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
    await fs.mkdir(path.join(codexHome, "sqlite"), { recursive: true });
    await fs.mkdir(lockDir, { recursive: true });
    await fs.writeFile(path.join(codexHome, "config.toml"), 'model_provider = "openai"\n');
    await fs.writeFile(path.join(lockDir, "owner.json"), "{malformed", "utf8");
    const facade = core.createCoreFacade({
      resolveProfile: async () => ({
        id: "default",
        revision: "r1",
        codexHome
      })
    });

    const status = await facade.getStatus({
      profile: { profileId: "default", profileRevision: "r1" }
    });

    assert.equal(status.sqliteHomeSource, "unknown");
    assert.equal(typeof status.storageRevision, "string");
    assert.ok(status.storageRevision.length > 0);
    assert.equal(status.operationInProgress.lockState, "unverifiable");
    assert.equal(status.operationInProgress.errorCode, "LOCK_UNVERIFIABLE");
    assert.equal(status.rolloutScanComplete, false);
    assert.equal(status.alignment.aligned, false);
  } finally {
    await fs.rm(testRoot, { recursive: true, force: true });
  }
});

test("host operation controls stay off the method surface and project pathless progress", async () => {
  const testRoot = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-core-control-"));
  const codexHome = path.join(testRoot, "codex-home");
  const rollout = path.join(codexHome, "sessions", "rollout-control.jsonl");
  try {
    await fs.mkdir(path.dirname(rollout), { recursive: true });
    await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
    await fs.writeFile(path.join(codexHome, "config.toml"), 'model_provider = "openai"\n');
    await fs.writeFile(rollout, `${JSON.stringify({
      type: "session_meta",
      timestamp: "2026-08-26T00:00:00.000Z",
      payload: { id: "control", cwd: "C:\\private", model_provider: "legacy" }
    })}\n`);
    const facade = core.createCoreFacade({
      resolveProfile: async () => ({
        id: "default",
        revision: "r1",
        codexHome
      })
    });
    const plan = await facade.prepareSync({
      profile: { profileId: "default", profileRevision: "r1" },
      keepCount: 1
    });
    assert.equal(plan.operation, "sync");
    assert.equal("providerSync" in plan, false);
    const started = [];
    const progress = [];
    const result = await facade.applySync(
      { schemaVersion: 1, planId: plan.planId },
      {
        onOperationStarted(event) { started.push(event); },
        onProgress(event) {
          progress.push(event);
          throw new Error("observer failure must not change the transaction");
        }
      }
    );
    assert.equal(result.outcome, "completed");
    assert.equal("providerSync" in result, false);
    assert.equal(started.length, 1);
    assert.equal(started[0].operationId, result.operationId);
    assert.ok(progress.length > 0);
    assert.equal(progress.every((event) => (
      Object.keys(event).every((key) => ["stage", "status", "progress", "count"].includes(key))
    )), true);
    assert.doesNotMatch(JSON.stringify(progress), /private|backupDir|state_5\.sqlite/i);
    assert.deepEqual(Object.keys(facade).sort(), EXPECTED_METHODS);
  } finally {
    await fs.rm(testRoot, { recursive: true, force: true });
  }
});
