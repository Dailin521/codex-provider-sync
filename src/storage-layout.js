import fs from "node:fs/promises";
import path from "node:path";

import { DB_FILE_BASENAME, defaultCodexHome } from "./constants.js";
import { readSqliteHomeFromConfigText } from "./config-file.js";

function resolvePath(value, cwd) {
  return path.resolve(cwd, value);
}

export function normalizeCodexHome(explicitCodexHome, { env = process.env, cwd = process.cwd() } = {}) {
  return resolvePath(explicitCodexHome ?? env.CODEX_HOME ?? defaultCodexHome(), cwd);
}

export function resolveStorageLayout({
  codexHome: explicitCodexHome,
  sqliteHome: explicitSqliteHome,
  configText = "",
  env = process.env,
  cwd = process.cwd()
} = {}) {
  const codexHome = normalizeCodexHome(explicitCodexHome, { env, cwd });
  const configuredSqliteHome = readSqliteHomeFromConfigText(configText);
  const selected = [
    [explicitSqliteHome, "cli"],
    [configuredSqliteHome, "config"],
    [env.CODEX_SQLITE_HOME, "env"]
  ].find(([value]) => typeof value === "string" && value.trim());

  const sqliteHomeSource = selected?.[1] ?? "default";
  const sqliteHome = selected
    ? resolvePath(selected[0].trim(), cwd)
    : path.join(codexHome, "sqlite");
  const allowLegacyRootFallback = sqliteHomeSource === "default";
  const stateDbCandidates = [
    {
      path: path.join(sqliteHome, DB_FILE_BASENAME),
      relativePath: allowLegacyRootFallback
        ? path.join("sqlite", DB_FILE_BASENAME)
        : DB_FILE_BASENAME,
      source: allowLegacyRootFallback ? "sqlite-dir" : "sqlite-home"
    },
    ...(allowLegacyRootFallback
      ? [{
          path: path.join(codexHome, DB_FILE_BASENAME),
          relativePath: DB_FILE_BASENAME,
          source: "legacy-root"
        }]
      : [])
  ];

  return {
    codexHome,
    sqliteHome,
    sqliteHomeSource,
    allowLegacyRootFallback,
    stateDbCandidates
  };
}

export async function ensureCodexHome(storage) {
  await fs.access(storage.codexHome).catch(() => {
    throw new Error(`Codex home not found at ${storage.codexHome}`);
  });
}

export function withStateDbLocation(storage, stateDbLocation) {
  return { ...storage, stateDbLocation };
}

export function isConfiguredSqliteHome(storage) {
  return storage.sqliteHomeSource !== "default";
}

export function missingConfiguredStateDbError(storage) {
  return new Error(
    `state_5.sqlite not found in configured SQLite home ${storage.sqliteHome} (source: ${storage.sqliteHomeSource}).`
  );
}
