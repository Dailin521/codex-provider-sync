import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";

import { resolveStorageLayout } from "../src/storage-layout.js";

const cwd = path.resolve("/work");
const codexHome = path.resolve("/codex-home");

test("resolveStorageLayout applies override, config, environment, and default precedence", () => {
  const explicit = resolveStorageLayout({
    codexHome,
    sqliteHome: "explicit-db",
    configText: 'sqlite_home = "config-db"',
    env: { CODEX_SQLITE_HOME: "env-db" },
    cwd
  });
  assert.equal(explicit.sqliteHome, path.resolve(cwd, "explicit-db"));
  assert.equal(explicit.sqliteHomeSource, "cli");

  const configured = resolveStorageLayout({
    codexHome,
    configText: "sqlite_home = 'config-db'",
    env: { CODEX_SQLITE_HOME: "env-db" },
    cwd
  });
  assert.equal(configured.sqliteHome, path.resolve(cwd, "config-db"));
  assert.equal(configured.sqliteHomeSource, "config");

  const environment = resolveStorageLayout({
    codexHome,
    env: { CODEX_SQLITE_HOME: "env-db" },
    cwd
  });
  assert.equal(environment.sqliteHome, path.resolve(cwd, "env-db"));
  assert.equal(environment.sqliteHomeSource, "env");

  const fallback = resolveStorageLayout({ codexHome, env: {}, cwd });
  assert.equal(fallback.sqliteHome, path.join(codexHome, "sqlite"));
  assert.equal(fallback.sqliteHomeSource, "default");
});

test("resolveStorageLayout only enables legacy root fallback for the default layout", () => {
  const fallback = resolveStorageLayout({ codexHome, env: {}, cwd });
  assert.deepEqual(
    fallback.stateDbCandidates.map((candidate) => candidate.path),
    [path.join(codexHome, "sqlite", "state_5.sqlite"), path.join(codexHome, "state_5.sqlite")]
  );

  const explicit = resolveStorageLayout({ codexHome, sqliteHome: "/external", env: {}, cwd });
  assert.deepEqual(
    explicit.stateDbCandidates.map((candidate) => candidate.path),
    [path.resolve("/external", "state_5.sqlite")]
  );
  assert.equal(explicit.allowLegacyRootFallback, false);
});
