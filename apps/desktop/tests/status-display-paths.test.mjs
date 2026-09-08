import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { createCoreFacade } from "@codex-provider-sync/core";
import { assertCoreMethodInput, assertCoreMethodOutput } from "@codex-provider-sync/contracts";

test("Desktop display paths follow resolved storage, including config, override and legacy DB", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-display-paths-"));
  const previousSqliteHome = process.env.CODEX_SQLITE_HOME;
  try {
    delete process.env.CODEX_SQLITE_HOME;
    const codexHome = path.join(root, "home");
    const configHome = path.join(root, "configured-index");
    const overrideHome = path.join(root, "profile-index");
    for (const directory of [codexHome, configHome, overrideHome]) await fs.mkdir(directory, { recursive: true });
    const configFile = path.join(codexHome, "config.toml");
    await fs.writeFile(configFile, `model_provider = "openai"\nsqlite_home = ${JSON.stringify(configHome.replaceAll("\\", "/"))}\n`);
    const selector = { profile: { profileId: "fixture", profileRevision: "r1" } };
    const makeCore = (sqliteHome) => createCoreFacade({
      includeLocalDisplayPaths: true,
      resolveProfile: () => ({ id: "fixture", revision: "r1", codexHome, ...(sqliteHome ? { sqliteHome } : {}) })
    });
    const configured = await makeCore().getStatus(selector);
    assert.equal(configured.displayPaths.sqliteHome, configHome);
    assert.equal(configured.displayPaths.stateDbPath, null);
    assert.equal(configured.sqliteHomeSource, "config");
    assertCoreMethodOutput("getStatus", configured);
    const overridden = await makeCore(overrideHome).getStatus(selector);
    assert.equal(overridden.displayPaths.sqliteHome, overrideHome);
    assert.equal(overridden.sqliteHomeSource, "cli");

    await fs.writeFile(configFile, 'model_provider = "openai"\n');
    process.env.CODEX_SQLITE_HOME = configHome;
    assert.equal((await makeCore().getStatus(selector)).displayPaths.sqliteHome, configHome);
    delete process.env.CODEX_SQLITE_HOME;
    const legacyDb = path.join(codexHome, "state_5.sqlite");
    const db = new DatabaseSync(legacyDb);
    db.exec("CREATE TABLE threads (id TEXT, model_provider TEXT, archived INTEGER)");
    db.close();
    const legacy = await makeCore().getStatus(selector);
    assert.deepEqual(legacy.displayPaths, { codexHome, sqliteHome: path.join(codexHome, "sqlite"), stateDbPath: legacyDb });
    assertCoreMethodOutput("getStatus", legacy);
    for (const displayPaths of [{ ...legacy.displayPaths, stateDbPath: "relative.sqlite" }, { ...legacy.displayPaths, extra: "forbidden" }]) {
      assert.throws(() => assertCoreMethodOutput("getStatus", { ...legacy, displayPaths }));
    }
    assert.throws(() => assertCoreMethodInput("getStatus", { ...selector, includeLocalDisplayPaths: true }));
  } finally {
    if (previousSqliteHome === undefined) delete process.env.CODEX_SQLITE_HOME;
    else process.env.CODEX_SQLITE_HOME = previousSqliteHome;
    await fs.rm(root, { recursive: true, force: true });
  }
});
