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

test("resolveStorageLayout blocks Windows processes from accessing WSL UNC SQLite homes", () => {
  for (const sqliteHome of [
    "\\\\wsl.localhost\\Ubuntu\\home\\user\\.codex\\sqlite",
    "\\\\WSL$\\Ubuntu\\home\\user\\.codex\\sqlite",
    "\\\\?\\UNC\\wsl.localhost\\Ubuntu\\home\\user\\.codex\\sqlite"
  ]) {
    const layout = resolveStorageLayout({
      codexHome,
      sqliteHome,
      env: {},
      cwd,
      platform: "win32"
    });

    assert.equal(layout.sqliteAccess.supported, false);
    assert.equal(layout.sqliteAccess.reason, "windows-wsl-unc");
    assert.match(layout.sqliteAccess.message, /Run codex-provider inside WSL/);
  }
});

test("resolveStorageLayout allows WSL paths outside a Windows process", () => {
  const wslUncOnLinux = resolveStorageLayout({
    codexHome,
    sqliteHome: "\\\\wsl.localhost\\Ubuntu\\home\\user\\.codex\\sqlite",
    env: {},
    cwd,
    platform: "linux"
  });
  const linuxPath = resolveStorageLayout({
    codexHome,
    sqliteHome: "/home/user/.codex/sqlite",
    env: {},
    cwd,
    platform: "linux"
  });

  assert.equal(wslUncOnLinux.sqliteAccess.supported, true);
  assert.equal(linuxPath.sqliteAccess.supported, true);
});
