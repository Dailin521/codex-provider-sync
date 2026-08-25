import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import * as core from "../src/index.js";

const EXPECTED_METHODS = [
  "applyRestore",
  "applySwitch",
  "applySync",
  "getDiagnostics",
  "getHistorySession",
  "getStatus",
  "getWatchStatus",
  "listBackups",
  "listHistory",
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

test("Core workspace bridge imports only the root public API", async () => {
  const source = await fs.readFile(new URL("../src/index.js", import.meta.url), "utf8");
  const rootImports = [...source.matchAll(/from\s+["'](\.\.\/\.\.\/\.\.\/src\/[^"']+)["']/g)]
    .map((match) => match[1]);
  assert.deepEqual(rootImports, ["../../../src/public-api.js"]);
  assert.doesNotMatch(source, /src\/(service|locking|backup|history|watch)\.js/);
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
    assert.equal(status.currentProvider, "openai");
    assert.deepEqual(status.profile, { id: "selected", revision: "selected-r1" });
    assert.deepEqual(backups, { backups: [] });
    assert.deepEqual(history.sessions, []);
    assert.equal(diagnostics.storage.codexHome, selectedHome);
    assert.equal(pruned.deletedCount, 0);
    assert.equal(selectors.length, 5);
    assert.equal(selectors.every((selector) => selector.profileId === "selected"), true);
  } finally {
    if (originalCodexHome === undefined) delete process.env.CODEX_HOME;
    else process.env.CODEX_HOME = originalCodexHome;
    await fs.rm(testRoot, { recursive: true, force: true });
  }
});
