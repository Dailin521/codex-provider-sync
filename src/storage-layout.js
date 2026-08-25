import fs from "node:fs/promises";
import path from "node:path";

import { DB_FILE_BASENAME, defaultCodexHome } from "./constants.js";
import { CoreError } from "./core-error.js";
import { readSqliteHomeFromConfigText } from "./config-file.js";

function resolvePath(value, cwd) {
  return path.resolve(cwd, value);
}

function isWslUncPath(value) {
  if (typeof value !== "string") {
    return false;
  }
  const normalized = value.replaceAll("/", "\\");
  return /^\\\\(?:wsl\.localhost|wsl\$)\\/i.test(normalized)
    || /^\\\\\?\\UNC\\(?:wsl\.localhost|wsl\$)\\/i.test(normalized);
}

function resolveSqliteAccess(sqliteHome, rawSqliteHome, platform) {
  if (platform === "win32" && (isWslUncPath(rawSqliteHome) || isWslUncPath(sqliteHome))) {
    return {
      supported: false,
      reason: "windows-wsl-unc",
      message: `Windows cannot safely access SQLite through the WSL UNC path ${rawSqliteHome ?? sqliteHome}. Run codex-provider inside WSL with a Linux SQLite Home path instead.`
    };
  }
  return { supported: true, reason: null, message: null };
}

export function normalizeCodexHome(explicitCodexHome, { env = process.env, cwd = process.cwd() } = {}) {
  return resolvePath(explicitCodexHome ?? env.CODEX_HOME ?? defaultCodexHome(), cwd);
}

export function resolveStorageLayout({
  codexHome: explicitCodexHome,
  sqliteHome: explicitSqliteHome,
  configText = "",
  env = process.env,
  cwd = process.cwd(),
  platform = process.platform
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
  const sqliteAccess = resolveSqliteAccess(sqliteHome, selected?.[0]?.trim(), platform);
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
    sqliteAccess,
    allowLegacyRootFallback,
    stateDbCandidates
  };
}

export async function ensureCodexHome(storage) {
  await fs.access(storage.codexHome).catch((error) => {
    const permissionDenied = error?.code === "EACCES" || error?.code === "EPERM";
    throw new CoreError(
      permissionDenied ? "PERMISSION_DENIED" : "CODEX_HOME_NOT_FOUND",
      permissionDenied
        ? `Permission denied while accessing Codex home at ${storage.codexHome}`
        : `Codex home not found at ${storage.codexHome}`,
      {
        cause: error,
        details: typeof error?.code === "string" ? { causeCode: error.code } : undefined
      }
    );
  });
}

export function withStateDbLocation(storage, stateDbLocation) {
  return { ...storage, stateDbLocation };
}

export function isConfiguredSqliteHome(storage) {
  return storage.sqliteHomeSource !== "default";
}

export function assertSqliteAccessSupported(storage, operation) {
  if (storage.sqliteAccess?.supported === false) {
    throw new CoreError("SQLITE_UNSUPPORTED_PATH", `Cannot ${operation}: ${storage.sqliteAccess.message}`, {
      details: storage.sqliteAccess.reason ? { reason: storage.sqliteAccess.reason } : undefined
    });
  }
}

export function missingConfiguredStateDbError(storage) {
  return new CoreError(
    "STATE_DB_NOT_FOUND",
    `state_5.sqlite not found in configured SQLite home ${storage.sqliteHome} (source: ${storage.sqliteHomeSource}).`,
    { details: { sqliteHomeSource: storage.sqliteHomeSource } }
  );
}
