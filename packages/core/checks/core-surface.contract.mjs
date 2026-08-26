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
    assert.equal("cwd" in history.sessions[0], false);
    assert.ok(plan.warnings.includes(
      "Some encrypted histories may require their original Provider or account for continuation."
    ));
    assert.equal(plan.warnings.every((warning) => [
      "Some encrypted histories may require their original Provider or account for continuation.",
      "Project visibility diagnostics are unavailable; backup-first protection remains enabled."
    ].includes(warning)), true);
    assert.doesNotMatch(JSON.stringify(plan), /private-project|synthetic-ciphertext/);
    assert.equal(diagnostics.storage.sqliteHomeSource, "default");
    assert.equal("codexHome" in diagnostics.storage, false);
    assert.equal(pruned.deletedCount, 0);
    assert.equal(selectors.length, 6);
    assert.equal(selectors.every((selector) => selector.profileId === "selected"), true);
  } finally {
    if (originalCodexHome === undefined) delete process.env.CODEX_HOME;
    else process.env.CODEX_HOME = originalCodexHome;
    await fs.rm(testRoot, { recursive: true, force: true });
  }
});
