import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { runWatch } from "../src/watch.js";

async function makeTempCodexHome() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-watch-"));
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

test("runWatch rejects invalid debounce-ms values", async () => {
  const { codexHome } = await makeTempCodexHome();
  await assert.rejects(
    () => runWatch({ codexHome, debounceMs: -1 }),
    /Invalid --debounce-ms value/
  );
  await fs.rm(codexHome, { recursive: true, force: true });
});

test("runWatch rejects when codex home or config.toml is missing", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-watch-"));
  await assert.rejects(
    () => runWatch({ codexHome: path.join(root, "does-not-exist") }),
    /Codex home not found/
  );
  const codexHome = path.join(root, ".codex");
  await fs.mkdir(codexHome, { recursive: true });
  await assert.rejects(
    () => runWatch({ codexHome }),
    /config\.toml not found/
  );
  await fs.rm(root, { recursive: true, force: true });
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

  // Once mode should stop the watcher automatically
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.equal(syncCalls, 1, "sync handler should have been invoked exactly once");

  // Subsequent change should be ignored
  await fs.writeFile(configPath, `model_provider = "openai"\n`, "utf8");
  await new Promise((resolve) => setTimeout(resolve, 200));
  assert.equal(syncCalls, 1, "sync handler should not be invoked again after once-mode exit");

  await handle.stop();
  await fs.rm(codexHome, { recursive: true, force: true });
});

test("runWatch swallows 'sqlite in use' errors and keeps watching", async () => {
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
      throw new Error("state_5.sqlite is currently in use by another process");
    },
    onLog: (line) => logs.push(line)
  });

  await fs.writeFile(configPath, `model_provider = "apigather"\n`, "utf8");
  await new Promise((resolve) => setTimeout(resolve, 200));

  assert.ok(firstSyncCalls >= 1, "first sync should have been attempted");
  assert.ok(
    logs.some((line) => /state_5\.sqlite is currently in use/.test(line)),
    "the locked-sqlite error should have been logged as a soft skip"
  );

  // Watcher should still be alive — emit another change and verify it triggers
  const gate = deferred();
  let secondSyncCalls = 0;
  const secondOnSync = async () => {
    secondSyncCalls += 1;
    gate.resolve();
    return { targetProvider: "openai", changedSessionFiles: 0, sqliteRowsUpdated: 0 };
  };
  // Manually re-route onSync without restarting the watcher; we can do this by
  // waiting for the next debounce tick after flipping the onSync closure via
  // the handle. Since runWatch doesn't expose onSync swap, we simply verify
  // that the debounced path was re-invoked (calls increment) after a second
  // change:
  await fs.writeFile(configPath, `model_provider = "longcat"\n`, "utf8");
  await new Promise((resolve) => setTimeout(resolve, 300));
  assert.ok(
    firstSyncCalls >= 2,
    "the watcher should still trigger after the first sync errored; got " + firstSyncCalls + " calls"
  );
  void gate; void secondOnSync; // silence unused
  await handle.stop();
  await fs.rm(codexHome, { recursive: true, force: true });
});

test("runWatch stops itself after consecutive non-busy sync failures", async () => {
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

test("runWatch stops itself after consecutive non-busy sync failures", async () => {
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

  await handle.stop();
  await fs.rm(codexHome, { recursive: true, force: true });
});
