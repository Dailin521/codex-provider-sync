import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { getWatchStatus, runWatch, startWatch, stopWatch } from "../src/watch.js";
import { OperationCoordinator } from "../src/operation-coordinator.js";

delete process.env.CODEX_SQLITE_HOME;

const testTempDir = process.platform === "win32" ? os.tmpdir() : "/tmp";

async function makeTempCodexHome() {
  const root = await fs.mkdtemp(path.join(testTempDir, "codex-provider-sync-watch-"));
  const codexHome = path.join(root, ".codex");
  await fs.mkdir(path.join(codexHome, "sqlite"), { recursive: true });
  await fs.writeFile(
    path.join(codexHome, "config.toml"),
    `model_provider = "openai"\n\n[model_providers.apigather]\nbase_url = "https://example.com"\n`,
    "utf8"
  );
  // minimal empty sqlite db so detectStateDb finds it
  await fs.writeFile(path.join(codexHome, "sqlite", "state_5.sqlite"), "", "utf8");
  return { root, codexHome };
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

async function waitUntil(predicate, message, timeoutMs = 3000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(message);
}

test("runWatch rejects invalid debounce-ms values", async () => {
  const { codexHome } = await makeTempCodexHome();
  await assert.rejects(
    () => runWatch({ codexHome, debounceMs: -1 }),
    (error) => error?.code === "INVALID_INPUT"
      && /Invalid --debounce-ms value/.test(error.message)
  );
  await fs.rm(codexHome, { recursive: true, force: true });
});

test("runWatch rejects when codex home or config.toml is missing", async () => {
  const root = await fs.mkdtemp(path.join(testTempDir, "codex-provider-sync-watch-"));
  await assert.rejects(
    () => runWatch({ codexHome: path.join(root, "does-not-exist") }),
    (error) => error?.code === "CODEX_HOME_NOT_FOUND"
      && /Codex home not found/.test(error.message)
  );
  const codexHome = path.join(root, ".codex");
  await fs.mkdir(codexHome, { recursive: true });
  await assert.rejects(
    () => runWatch({ codexHome }),
    (error) => error?.code === "CODEX_HOME_NOT_FOUND"
      && /config\.toml not found/.test(error.message)
  );
  await fs.rm(root, { recursive: true, force: true });
});

test("runWatch distinguishes access denial from a missing Codex home", async () => {
  const denied = new Error("permission denied fixture");
  denied.code = "EACCES";
  await assert.rejects(
    () => runWatch({
      codexHome: path.join(testTempDir, "permission-denied-codex-home"),
      accessImpl: async () => { throw denied; }
    }),
    (error) => error?.code === "PERMISSION_DENIED"
      && error.details?.causeCode === "EACCES"
  );
});

test("runWatch blocks Windows WSL UNC SQLite homes before creating watchers", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  let handle;
  try {
    await assert.rejects(
      async () => {
        handle = await runWatch({
          codexHome,
          sqliteHome: "\\\\wsl.localhost\\Ubuntu\\home\\user\\.codex\\sqlite",
          platform: "win32"
        });
      },
      /Cannot watch.*Run codex-provider inside WSL/
    );
  } finally {
    await handle?.stop();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("runWatch invokes the injected sync handler when config.toml changes and stops on --once", async () => {
  const { codexHome } = await makeTempCodexHome();
  const configPath = path.join(codexHome, "config.toml");

  let syncCalls = 0;
  const gate = deferred();
  const handle = await runWatch({
    codexHome,
    debounceMs: 30,
    includeStateDb: false,
    once: true,
    onSync: async () => {
      syncCalls += 1;
      gate.resolve();
      return { targetProvider: "openai", changedSessionFiles: 0, sqliteRowsUpdated: 0 };
    }
  });

  // Trigger a change
  await fs.writeFile(configPath, `model_provider = "apigather"\n`, "utf8");

  await Promise.race([
    gate.promise,
    new Promise((_resolve, reject) => setTimeout(() => reject(new Error("sync not invoked")), 5000))
  ]);

  // Once mode should stop the watcher automatically and resolve the
  // `done` promise so the CLI can exit cleanly.
  const reason = await Promise.race([
    handle.done,
    new Promise((_, reject) => setTimeout(() => reject(new Error("watcher did not auto-stop within 1 second")), 1000))
  ]);
  assert.equal(reason, "once-mode-complete", "watcher must auto-stop with reason 'once-mode-complete'");
  assert.equal(syncCalls, 1, "sync handler should have been invoked exactly once");

  // Subsequent change should be ignored (watcher already stopped)
  await fs.writeFile(configPath, `model_provider = "openai"\n`, "utf8");
  await new Promise((resolve) => setTimeout(resolve, 200));
  assert.equal(syncCalls, 1, "sync handler should not be invoked again after once-mode exit");

  await handle.stop();
  await fs.rm(codexHome, { recursive: true, force: true });
});

test("runWatch uses the typed SQLITE_BUSY code and keeps watching", async () => {
  const { codexHome } = await makeTempCodexHome();
  const configPath = path.join(codexHome, "config.toml");

  const logs = [];
  let firstSyncCalls = 0;
  const handle = await runWatch({
    codexHome,
    debounceMs: 30,
    includeStateDb: false,
    onSync: async () => {
      firstSyncCalls += 1;
      const error = new Error("localized busy diagnostic");
      error.code = "SQLITE_BUSY";
      throw error;
    },
    onLog: (line) => logs.push(line)
  });

  await fs.writeFile(configPath, `model_provider = "apigather"\n`, "utf8");
  await new Promise((resolve) => setTimeout(resolve, 200));

  assert.ok(firstSyncCalls >= 1, "first sync should have been attempted");
  assert.ok(
    logs.some((line) => /Sync skipped: localized busy diagnostic/.test(line)),
    "the locked-sqlite error should have been logged as a soft skip"
  );

  // Watcher should still be alive — emit another change and verify it triggers
  await fs.writeFile(configPath, `model_provider = "longcat"\n`, "utf8");
  await new Promise((resolve) => setTimeout(resolve, 300));
  assert.ok(
    firstSyncCalls >= 2,
    "the watcher should still trigger after the first sync errored; got " + firstSyncCalls + " calls"
  );
  await handle.stop();
  await fs.rm(codexHome, { recursive: true, force: true });
});

test("runWatch coalesces in-flight events and never overlaps applies", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  const configPath = path.join(codexHome, "config.toml");
  const firstStarted = deferred();
  const releaseFirst = deferred();
  let calls = 0;
  let concurrent = 0;
  let maxConcurrent = 0;
  const observedReasons = [];
  const handle = await runWatch({
    codexHome,
    debounceMs: 20,
    includeStateDb: false,
    onLog: () => {},
    onSync: async ({ reasons }) => {
      calls += 1;
      concurrent += 1;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      observedReasons.push(reasons);
      if (calls === 1) {
        firstStarted.resolve();
        await releaseFirst.promise;
      }
      concurrent -= 1;
      return { targetProvider: "openai", changedSessionFiles: 0, sqliteRowsUpdated: 0 };
    }
  });

  try {
    await fs.writeFile(configPath, 'model_provider = "first"\n', "utf8");
    await firstStarted.promise;
    await fs.writeFile(configPath, 'model_provider = "second"\n', "utf8");
    await fs.writeFile(configPath, 'model_provider = "third"\n', "utf8");
    await new Promise((resolve) => setTimeout(resolve, 80));
    assert.equal(calls, 1, "an in-flight Apply must not be overlapped");
    releaseFirst.resolve();
    await waitUntil(() => calls === 2, "coalesced follow-up Apply did not run");
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(calls, 2, "all in-flight events must merge into exactly one follow-up Apply");
    assert.equal(maxConcurrent, 1);
    assert.ok(observedReasons.every((reasons) => Array.isArray(reasons) && reasons.includes("config.toml")));
  } finally {
    releaseFirst.resolve();
    await handle.stop();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("runWatch yields on OPERATION_BUSY without counting consecutive failures", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  const configPath = path.join(codexHome, "config.toml");
  const logs = [];
  let attempts = 0;
  let shutdownReason = null;
  const handle = await runWatch({
    codexHome,
    debounceMs: 10,
    includeStateDb: false,
    onLog: (line) => logs.push(line),
    onShutdown: (reason) => { shutdownReason = reason; },
    onSync: async () => {
      attempts += 1;
      const error = new Error("manual operation owns the lock");
      error.code = "OPERATION_BUSY";
      error.details = { busyScope: "codex-home" };
      throw error;
    }
  });

  try {
    for (let index = 0; index < 6; index += 1) {
      await fs.writeFile(configPath, `model_provider = "busy-${index}"\n`, "utf8");
      await new Promise((resolve) => setTimeout(resolve, 60));
    }
    assert.ok(attempts >= 5, `expected repeated event-driven attempts, got ${attempts}`);
    assert.equal(shutdownReason, null);
    assert.ok(logs.some((line) => /yielded to an active manual operation/.test(line)));
    assert.ok(!logs.some((line) => /giving up after/.test(line)));
  } finally {
    await handle.stop();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("runWatch retains a busy batch and applies it exactly once after a local manual operation ends", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  const configPath = path.join(codexHome, "config.toml");
  const manualCompleted = deferred();
  let attempts = 0;
  let cancelled = 0;
  const handle = await runWatch({
    codexHome,
    debounceMs: 15,
    includeStateDb: false,
    onLog: () => {},
    manualOperationWaiter: () => ({
      promise: manualCompleted.promise,
      cancel: () => { cancelled += 1; }
    }),
    onSync: async () => {
      attempts += 1;
      if (attempts === 1) {
        const error = new Error("manual operation owns the lock");
        error.code = "OPERATION_BUSY";
        error.details = { busyScope: "codex-home" };
        throw error;
      }
      return { targetProvider: "openai", changedSessionFiles: 0, sqliteRowsUpdated: 0 };
    }
  });

  try {
    await fs.writeFile(configPath, 'model_provider = "manual-busy"\n', "utf8");
    await waitUntil(() => attempts === 1, "Watch did not yield to the manual operation");
    await fs.writeFile(configPath, 'model_provider = "coalesced-a"\n', "utf8");
    await fs.writeFile(configPath, 'model_provider = "coalesced-b"\n', "utf8");
    await new Promise((resolve) => setTimeout(resolve, 80));
    assert.equal(attempts, 1, "events must remain queued while the manual operation is active");

    manualCompleted.resolve();
    await waitUntil(() => attempts === 2, "Watch did not retry after manual completion");
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(attempts, 2, "the retained busy batch must merge into one follow-up Apply");
    assert.equal(cancelled, 0);
  } finally {
    manualCompleted.resolve();
    await handle.stop();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("runWatch cancels its manual-completion subscription when stopped", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  const configPath = path.join(codexHome, "config.toml");
  const manualCompleted = deferred();
  let attempts = 0;
  let cancelled = 0;
  const handle = await runWatch({
    codexHome,
    debounceMs: 10,
    includeStateDb: false,
    onLog: () => {},
    manualOperationWaiter: () => ({
      promise: manualCompleted.promise,
      cancel: () => { cancelled += 1; }
    }),
    onSync: async () => {
      attempts += 1;
      const error = new Error("manual operation owns the lock");
      error.code = "OPERATION_BUSY";
      error.details = { busyScope: "codex-home" };
      throw error;
    }
  });

  await fs.writeFile(configPath, 'model_provider = "manual-stop"\n', "utf8");
  await waitUntil(() => attempts === 1, "Watch did not enter the busy wait");
  await handle.stop();
  assert.equal(cancelled, 1);
  manualCompleted.resolve();
  await new Promise((resolve) => setTimeout(resolve, 80));
  assert.equal(attempts, 1, "a stopped watcher must not apply after completion notification");
  await fs.rm(root, { recursive: true, force: true });
});

test("runWatch retries one retained batch when a prepared manual intent expires", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  const configPath = path.join(codexHome, "config.toml");
  let operationSequence = 0;
  const coordinator = new OperationCoordinator({
    randomOperationId: () => `watch-${++operationSequence}`
  });
  coordinator.registerManualIntent(codexHome, "abandoned-plan", Date.now() + 80);
  let attempts = 0;
  const handle = await runWatch({
    codexHome,
    debounceMs: 10,
    includeStateDb: false,
    onLog: () => {},
    manualOperationWaiter: () => coordinator.waitForManualOperation(codexHome),
    onSync: async () => {
      attempts += 1;
      const active = coordinator.begin(codexHome, "sync", { actor: "watch" });
      coordinator.end(codexHome, active.operationId);
      return { targetProvider: "openai", changedSessionFiles: 0, sqliteRowsUpdated: 0 };
    }
  });

  try {
    await fs.writeFile(configPath, 'model_provider = "manual-intent"\n', "utf8");
    await waitUntil(() => attempts === 1, "Watch did not observe the prepared manual intent");
    await waitUntil(() => attempts === 2, "Watch did not resume after manual intent expiry");
    await new Promise((resolve) => setTimeout(resolve, 80));
    assert.equal(attempts, 2, "the retained batch must be applied exactly once after expiry");
  } finally {
    await handle.stop();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("runWatch stops immediately when explicit recovery is required", async () => {
  for (const code of ["RECOVERY_REQUIRED", "PENDING_TRANSACTION"]) {
    const { root, codexHome } = await makeTempCodexHome();
    const configPath = path.join(codexHome, "config.toml");
    let attempts = 0;
    const handle = await runWatch({
      codexHome,
      debounceMs: 10,
      includeStateDb: false,
      onLog: () => {},
      onSync: async () => {
        attempts += 1;
        const error = new Error("fixture recovery blocker");
        error.code = code;
        throw error;
      }
    });
    try {
      await fs.writeFile(configPath, `model_provider = "${code}"\n`, "utf8");
      assert.equal(await handle.done, "recovery-required", code);
      assert.equal(attempts, 1, code);
      await fs.writeFile(configPath, 'model_provider = "after-stop"\n', "utf8");
      await new Promise((resolve) => setTimeout(resolve, 80));
      assert.equal(attempts, 1, `${code} must stop all later Watch applies`);
    } finally {
      await handle.stop();
      await fs.rm(root, { recursive: true, force: true });
    }
  }
});

test("startWatch exposes registry status and stopWatch is idempotent", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  try {
    const started = await startWatch({
      codexHome,
      includeStateDb: false,
      onLog: () => {},
      onSync: async () => ({ targetProvider: "openai", changedSessionFiles: 0, sqliteRowsUpdated: 0 })
    });
    assert.equal(started.schemaVersion, 1);
    assert.equal(started.status, "running");
    assert.equal(getWatchStatus({ watchId: started.watchId }).status, "running");
    const stopped = await stopWatch({ watchId: started.watchId });
    assert.equal(stopped.status, "stopped");
    assert.equal((await stopWatch({ watchId: started.watchId })).status, "stopped");
    assert.ok(getWatchStatus().watches.some((watch) => watch.watchId === started.watchId));
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("runWatch stops itself after consecutive non-busy sync failures and resolves done", async () => {
  // Regression guard for B11: when the sync handler keeps
  // throwing something other than `state_5.sqlite is currently
  // in use` (e.g. config corruption, codex home moved,
  // permission denied, ...), the watcher must not loop forever
  // spamming logs. It should give up after a small threshold of
  // consecutive failures so the user notices via the
  // `codex-provider watch` exit instead.
  const { codexHome } = await makeTempCodexHome();
  const configPath = path.join(codexHome, "config.toml");
  const shutdownReason = deferred();

  const handle = await runWatch({
    codexHome,
    debounceMs: 30,
    includeStateDb: false,
    onSync: async () => {
      // Throw something that is NOT a "busy" error so the watcher
      // counts it toward the failure threshold.
      throw new Error("config.toml is malformed (test fixture)");
    },
    onShutdown: async (reason) => {
      shutdownReason.resolve(reason);
    }
  });

  // Drive enough change events to exceed the threshold. The
  // watcher should auto-shutdown after 5 consecutive failures.
  for (let i = 0; i < 6; i += 1) {
    await fs.writeFile(configPath, `model_provider = "apigather-${i}"\n`, "utf8");
    await new Promise((resolve) => setTimeout(resolve, 120));
  }

  const reason = await Promise.race([
    shutdownReason.promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error("watcher did not auto-shutdown within 3 seconds")), 3000))
  ]);
  assert.equal(reason, "consecutive-failures", "watcher must auto-shutdown with reason 'consecutive-failures'");

  // The `done` promise must also resolve so the CLI can exit
  // cleanly without waiting for a SIGINT/SIGTERM that will never
  // arrive.
  const doneReason = await Promise.race([
    handle.done,
    new Promise((_, reject) => setTimeout(() => reject(new Error("handle.done did not resolve within 1 second")), 1000))
  ]);
  assert.equal(doneReason, "consecutive-failures", "handle.done must resolve with the same reason as the shutdown");

  await handle.stop();
  await fs.rm(codexHome, { recursive: true, force: true });
});

test("runWatch observes the active state database chosen by detectStateDb", async () => {
  // Regression guard: the watcher must register exactly one
  // filesystem watcher for the SQLite state database — the one
  // `detectStateDb` identified as the live Codex layout. Walking
  // every candidate and emitting a sync for any of them is the
  // responsibility of the sync path (and is a single-active-DB
  // semantic there now), not the watcher.
  const { codexHome } = await makeTempCodexHome();
  // Both layouts are present here so we exercise the
  // "live in sqlite-dir" choice that detectStateDb makes.
  const legacyDbPath = path.join(codexHome, "state_5.sqlite");
  await fs.writeFile(legacyDbPath, "", "utf8");

  const syncCalls = [];
  const handle = await runWatch({
    codexHome,
    debounceMs: 30,
    includeStateDb: true,
    onSync: async () => {
      syncCalls.push(Date.now());
      return { targetProvider: "openai", changedSessionFiles: 0, sqliteRowsUpdated: 0 };
    }
  });

  // Verify the watcher advertised only the new sqlite/state_5.sqlite
  // path. The legacy root DB exists on disk but must not be
  // reported as the watched file.
  const newDbPath = path.join(codexHome, "sqlite", "state_5.sqlite");
  assert.equal(
    handle.watchedStateDbPath,
    newDbPath,
    "watcher must report the active DB path, not the legacy fallback"
  );

  // Touching the active DB triggers a sync; touching the legacy
  // root DB does not (the watcher is not observing it).
  const activeBefore = syncCalls.length;
  await fs.writeFile(newDbPath, "x", "utf8");
  await new Promise((resolve) => setTimeout(resolve, 300));
  assert.ok(
    syncCalls.length > activeBefore,
    "watcher must react to writes in the active sqlite-dir state DB"
  );

  const legacyBefore = syncCalls.length;
  await fs.writeFile(legacyDbPath, "y", "utf8");
  await new Promise((resolve) => setTimeout(resolve, 300));
  assert.equal(
    syncCalls.length,
    legacyBefore,
    "watcher must NOT react to writes in the legacy root DB anymore"
  );

  await handle.stop();
  await fs.rm(codexHome, { recursive: true, force: true });
});

test("runWatch rebinds SQLite watchers after config changes and drops invalid homes", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  const configPath = path.join(codexHome, "config.toml");
  const originalDbPath = path.join(codexHome, "sqlite", "state_5.sqlite");
  const sqliteHomeA = path.join(root, "sqlite-a");
  const sqliteHomeB = path.join(root, "sqlite-b");
  const dbPathA = path.join(sqliteHomeA, "state_5.sqlite");
  const dbPathB = path.join(sqliteHomeB, "state_5.sqlite");
  await fs.mkdir(sqliteHomeA, { recursive: true });
  await fs.mkdir(sqliteHomeB, { recursive: true });
  await fs.writeFile(dbPathA, "", "utf8");
  await fs.writeFile(dbPathB, "", "utf8");

  const syncCalls = [];
  const handle = await runWatch({
    codexHome,
    debounceMs: 30,
    includeStateDb: true,
    onSync: async ({ reason, sqliteHome }) => {
      syncCalls.push({ reason, sqliteHome });
      return { targetProvider: "openai", changedSessionFiles: 0, sqliteRowsUpdated: 0 };
    }
  });

  await fs.writeFile(
    configPath,
    `model_provider = "openai"\nsqlite_home = '${sqliteHomeA}'\n`,
    "utf8"
  );
  await waitUntil(() => handle.watchedStateDbPath === dbPathA, "watcher did not bind SQLite home A");
  const beforeA = syncCalls.length;
  await fs.appendFile(dbPathA, "a", "utf8");
  await waitUntil(() => syncCalls.length > beforeA, "watcher did not observe SQLite home A");

  const missingSqliteHome = path.join(root, "missing-sqlite");
  await fs.writeFile(
    configPath,
    `model_provider = "openai"\nsqlite_home = '${missingSqliteHome}'\n`,
    "utf8"
  );
  await waitUntil(() => handle.watchedStateDbPath === null, "watcher did not drop the invalid SQLite home");
  await new Promise((resolve) => setTimeout(resolve, 100));
  const beforeOldDbWrite = syncCalls.length;
  await fs.appendFile(dbPathA, "stale", "utf8");
  await fs.appendFile(originalDbPath, "stale", "utf8");
  await new Promise((resolve) => setTimeout(resolve, 250));
  assert.equal(syncCalls.length, beforeOldDbWrite, "invalid layout must leave only the config watcher active");

  await fs.writeFile(
    configPath,
    `model_provider = "openai"\nsqlite_home = '${sqliteHomeB}'\n`,
    "utf8"
  );
  await waitUntil(() => handle.watchedStateDbPath === dbPathB, "watcher did not bind SQLite home B");
  const beforeB = syncCalls.length;
  await fs.appendFile(dbPathB, "b", "utf8");
  await waitUntil(() => syncCalls.length > beforeB, "watcher did not observe SQLite home B");

  await handle.stop();
  await fs.rm(root, { recursive: true, force: true });
});

test("runWatch reacts to writes in the SQLite WAL sidecar", async () => {
  // Regression guard for owner review: when Codex runs against a
  // SQLite database in WAL journal mode (the default), new
  // transactions are appended to `state_5.sqlite-wal` and only
  // checkpointed back to the main file on a checkpoint. A
  // watcher that only listens on the main DB file would miss
  // every write Codex makes between checkpoints. We have to
  // attach a watcher to the WAL sidecar too.
  const { codexHome } = await makeTempCodexHome();
  const walPath = path.join(codexHome, "sqlite", "state_5.sqlite-wal");
  await fs.writeFile(walPath, "", "utf8");

  const syncCalls = [];
  const handle = await runWatch({
    codexHome,
    debounceMs: 30,
    includeStateDb: true,
    onSync: async () => {
      syncCalls.push(Date.now());
      return { targetProvider: "openai", changedSessionFiles: 0, sqliteRowsUpdated: 0 };
    }
  });

  // Writing only to the WAL file (not the main DB) must still
  // trigger a sync.
  const before = syncCalls.length;
  await fs.writeFile(walPath, "wal-tx", "utf8");
  await new Promise((resolve) => setTimeout(resolve, 400));
  assert.ok(
    syncCalls.length > before,
    "watcher must react to writes in the SQLite WAL sidecar; got " + (syncCalls.length - before) + " new calls"
  );

  await handle.stop();
  await fs.rm(codexHome, { recursive: true, force: true });
});

test("runWatch attaches when the SQLite WAL sidecar is created later", async () => {
  const { codexHome } = await makeTempCodexHome();
  const walPath = path.join(codexHome, "sqlite", "state_5.sqlite-wal");
  const syncCalls = [];
  const handle = await runWatch({
    codexHome,
    debounceMs: 30,
    includeStateDb: true,
    onSync: async () => {
      syncCalls.push(Date.now());
      return { targetProvider: "openai", changedSessionFiles: 0, sqliteRowsUpdated: 0 };
    }
  });

  await fs.writeFile(walPath, "", "utf8");
  await new Promise((resolve) => setTimeout(resolve, 350));
  const before = syncCalls.length;
  await fs.appendFile(walPath, "wal-created-after-start", "utf8");
  await new Promise((resolve) => setTimeout(resolve, 400));
  assert.ok(
    syncCalls.length > before,
    "watcher must attach to a WAL sidecar that did not exist at startup"
  );

  await handle.stop();
  await fs.rm(codexHome, { recursive: true, force: true });
});

test("runWatch uses the top-level model field, ignoring provider sections", async () => {
  // Regression guard for owner review: the watcher must read the
  // root-level `model = "..."` line for the per-turn model sync
  // and must not pick up a `model = "..."` value that lives
  // inside a `[model_providers.*]` section — otherwise the
  // provider-section model would be propagated to every
  // rollout's turn_context.model.
  const root = await fs.mkdtemp(path.join(testTempDir, "codex-provider-sync-watch-"));
  const codexHome = path.join(root, ".codex");
  await fs.mkdir(path.join(codexHome, "sqlite"), { recursive: true });
  await fs.writeFile(
    path.join(codexHome, "config.toml"),
    [
      'model_provider = "foo"',
      'model = "gpt-5"',
      '',
      '[model_providers.foo]',
      'base_url = "https://example.com"',
      'model = "gpt-4o-mini"',  // this one must be ignored
      ''
    ].join("\n"),
    "utf8"
  );
  await fs.writeFile(path.join(codexHome, "sqlite", "state_5.sqlite"), "", "utf8");

  let observedModel = null;
  const handle = await runWatch({
    codexHome,
    debounceMs: 30,
    includeStateDb: false,
    onSync: async ({ model }) => {
      observedModel = model;
      return { targetProvider: "foo", changedSessionFiles: 0, sqliteRowsUpdated: 0 };
    }
  });

  await fs.writeFile(
    path.join(codexHome, "config.toml"),
    [
      'model_provider = "foo"',
      'model = "gpt-5.1"',  // change at root level
      '',
      '[model_providers.foo]',
      'base_url = "https://example.com"',
      'model = "gpt-4o-mini"',  // unchanged at provider section
      ''
    ].join("\n"),
    "utf8"
  );

  // Wait for the debounce + sync.
  for (let i = 0; i < 50 && observedModel === null; i += 1) {
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  assert.equal(
    observedModel,
    "gpt-5.1",
    "watcher must read the root-level model; provider-section model must be ignored"
  );

  await handle.stop();
  await fs.rm(codexHome, { recursive: true, force: true });
});
