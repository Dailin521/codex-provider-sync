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
