import { spawn } from "node:child_process";
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import {
  createBackup,
  getBackupSummary,
  pruneBackups,
  restoreBackup,
  updateSessionBackupManifest
} from "../src/backup.js";
import { getStatus, renderStatus, runRestore, runSwitch, runSync } from "../src/service.js";
import { DB_FILE_BASENAME, DEFAULT_BACKUP_RETENTION_COUNT, SQLITE_DIR_BASENAME } from "../src/constants.js";
import { getUnsupportedNodeVersionMessage } from "../src/node-version.js";
import { applySessionChanges, collectSessionChanges } from "../src/session-files.js";
import { openDatabase } from "../src/sqlite.js";

delete process.env.CODEX_SQLITE_HOME;

async function makeTempCodexHome() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-"));
  const codexHome = path.join(root, ".codex");
  await fs.mkdir(path.join(codexHome, "sessions", "2026", "03", "19"), { recursive: true });
  await fs.mkdir(path.join(codexHome, "archived_sessions", "2026", "03", "18"), { recursive: true });
  return { root, codexHome };
}

async function writeRollout(filePath, id, provider) {
  const payload = {
    id,
    timestamp: "2026-03-19T00:00:00.000Z",
    cwd: "C:\\AITemp",
    source: "cli",
    cli_version: "0.115.0",
    model_provider: provider
  };
  const lines = [
    JSON.stringify({ timestamp: payload.timestamp, type: "session_meta", payload }),
    JSON.stringify({ timestamp: payload.timestamp, type: "event_msg", payload: { type: "user_message", message: "hi" } })
  ];
  await fs.writeFile(filePath, `${lines.join("\n")}\n`, "utf8");
}

async function writeCustomRollout(filePath, payload, message = "hi") {
  const lines = [
    JSON.stringify({ timestamp: payload.timestamp, type: "session_meta", payload }),
    JSON.stringify({ timestamp: payload.timestamp, type: "event_msg", payload: { type: "user_message", message } })
  ];
  await fs.writeFile(filePath, `${lines.join("\n")}\n`, "utf8");
}

async function writeRolloutWithTurnContext(filePath, { id, provider, model }) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  const metaPayload = {
    id,
    timestamp: "2026-06-09T09:16:03.878Z",
    cwd: "C:\\AITemp",
    source: "cli",
    cli_version: "0.115.0",
    model_provider: provider
  };
  const turnContext = {
    timestamp: "2026-06-09T09:16:03.880Z",
    type: "turn_context",
    payload: {
      turn_id: "019eabaa-e391-7e21-89cd-e761b5dee114",
      cwd: "C:\\AITemp",
      current_date: "2026-06-09",
      model,
      collaboration_mode: { mode: "default", settings: { model, reasoning_effort: "xhigh" } }
    }
  };
  const heartBeat = {
    timestamp: "2026-06-09T10:16:03.880Z",
    type: "turn_context",
    payload: {
      turn_id: "019eabaa-e391-7e21-89cd-e761b5dee115",
      cwd: "C:\\AITemp",
      current_date: "2026-06-09",
      model,
      collaboration_mode: { mode: "default", settings: { model, reasoning_effort: "xhigh" } }
    }
  };
  const lines = [
    JSON.stringify({ timestamp: metaPayload.timestamp, type: "session_meta", payload: metaPayload }),
    JSON.stringify(turnContext),
    JSON.stringify(heartBeat)
  ];
  await fs.writeFile(filePath, `${lines.join("\n")}\n`, "utf8");
}

function backupRoot(codexHome) {
  return path.join(codexHome, "backups_state", "provider-sync");
}

async function writeBackup(codexHome, directoryName, files) {
  const backupDir = path.join(backupRoot(codexHome), directoryName);
  await fs.mkdir(backupDir, { recursive: true });
  let totalBytes = 0;
  if (!files.some(([relativePath]) => relativePath === "metadata.json")) {
    const metadataPath = path.join(backupDir, "metadata.json");
    const metadataContent = JSON.stringify({
      version: 1,
      namespace: "provider-sync",
      codexHome,
      targetProvider: "openai",
      createdAt: "2026-03-24T00:00:00.000Z",
      dbFiles: [],
      changedSessionFiles: 0
    }, null, 2);
    await fs.writeFile(metadataPath, metadataContent, "utf8");
    const metadataStat = await fs.stat(metadataPath);
    totalBytes += metadataStat.size;
  }
  for (const [relativePath, content] of files) {
    const fullPath = path.join(backupDir, relativePath);
    await fs.mkdir(path.dirname(fullPath), { recursive: true });
    await fs.writeFile(fullPath, content, "utf8");
    const stat = await fs.stat(fullPath);
    totalBytes += stat.size;
  }
  return totalBytes;
}

async function writeConfig(codexHome, modelProviderLine = "") {
  const config = `${modelProviderLine}${modelProviderLine ? "\n" : ""}sandbox_mode = "danger-full-access"\n\n[model_providers.apigather]\nbase_url = "https://example.com"\n`;
  await fs.writeFile(path.join(codexHome, "config.toml"), config, "utf8");
}

async function writeGlobalState(codexHome, value) {
  const text = `${JSON.stringify(value, null, 2)}\n`;
  await fs.writeFile(path.join(codexHome, ".codex-global-state.json"), text, "utf8");
  await fs.writeFile(path.join(codexHome, ".codex-global-state.json.bak"), text, "utf8");
}

function stateDbPath(codexHome) {
  return path.join(codexHome, SQLITE_DIR_BASENAME, DB_FILE_BASENAME);
}

function legacyStateDbPath(codexHome) {
  return path.join(codexHome, DB_FILE_BASENAME);
}

async function writeStateDbAt(dbPath, rows) {
  await fs.mkdir(path.dirname(dbPath), { recursive: true });
  const db = await openDatabase(dbPath);
  try {
    db.exec(`
      CREATE TABLE threads (
        id TEXT PRIMARY KEY,
        model_provider TEXT,
        cwd TEXT NOT NULL DEFAULT '',
        archived INTEGER NOT NULL DEFAULT 0,
        first_user_message TEXT NOT NULL DEFAULT '',
        model TEXT
      )
    `);
    const stmt = db.prepare("INSERT INTO threads (id, model_provider, cwd, archived, first_user_message, model) VALUES (?, ?, ?, ?, ?, ?)");
    for (const row of rows) {
      stmt.run(row.id, row.model_provider, row.cwd ?? "C:\\AITemp", row.archived ? 1 : 0, row.first_user_message ?? "hello", row.model ?? null);
    }
  } finally {
    db.close();
  }
}

async function writeStateDb(codexHome, rows) {
  await writeStateDbAt(stateDbPath(codexHome), rows);
}

async function writeLegacyStateDb(codexHome, rows) {
  await writeStateDbAt(legacyStateDbPath(codexHome), rows);
}

async function writeStateDbWithUserEventColumn(codexHome, rows) {
  const dbPath = stateDbPath(codexHome);
  await fs.mkdir(path.dirname(dbPath), { recursive: true });
  const db = await openDatabase(dbPath);
  try {
    db.exec(`
      CREATE TABLE threads (
        id TEXT PRIMARY KEY,
        model_provider TEXT,
        cwd TEXT NOT NULL DEFAULT '',
        archived INTEGER NOT NULL DEFAULT 0,
        has_user_event INTEGER NOT NULL DEFAULT 0,
        first_user_message TEXT NOT NULL DEFAULT ''
      )
    `);
    const stmt = db.prepare("INSERT INTO threads (id, model_provider, cwd, archived, has_user_event, first_user_message) VALUES (?, ?, ?, ?, ?, ?)");
    for (const row of rows) {
      stmt.run(row.id, row.model_provider, row.cwd ?? "C:\\AITemp", row.archived ? 1 : 0, row.has_user_event ? 1 : 0, row.first_user_message ?? "hello");
    }
  } finally {
    db.close();
  }
}

async function writeStateDbForProjectVisibility(codexHome, rows) {
  const dbPath = stateDbPath(codexHome);
  await fs.mkdir(path.dirname(dbPath), { recursive: true });
  const db = await openDatabase(dbPath);
  try {
    db.exec(`
      CREATE TABLE threads (
        id TEXT PRIMARY KEY,
        model_provider TEXT,
        cwd TEXT NOT NULL DEFAULT '',
        source TEXT NOT NULL DEFAULT 'cli',
        archived INTEGER NOT NULL DEFAULT 0,
        first_user_message TEXT NOT NULL DEFAULT '',
        updated_at_ms INTEGER NOT NULL DEFAULT 0
      )
    `);
    const stmt = db.prepare("INSERT INTO threads (id, model_provider, cwd, source, archived, first_user_message, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?)");
    for (const row of rows) {
      stmt.run(
        row.id,
        row.model_provider ?? "dal",
        row.cwd,
        row.source ?? "cli",
        row.archived ? 1 : 0,
        row.first_user_message ?? "hello",
        row.updated_at_ms ?? 0
      );
    }
  } finally {
    db.close();
  }
}

async function lockRolloutFile(filePath, shareMode = "None") {
  const script = `
& {
  param([string]$path, [string]$shareMode)
  $share = [System.Enum]::Parse([System.IO.FileShare], $shareMode)
  $stream = [System.IO.File]::Open($path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::ReadWrite, $share)
  try {
    Write-Output 'locked'
    [Console]::Out.Flush()
    Start-Sleep -Seconds 30
  } finally {
    $stream.Close()
  }
}
`.trim();

  const child = spawn("powershell.exe", [
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-Command",
    script,
    filePath,
    shareMode
  ], {
    stdio: ["ignore", "pipe", "pipe"]
  });

  await new Promise((resolve, reject) => {
    let settled = false;
    let stdout = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString("utf8");
      if (!settled && stdout.includes("locked")) {
        settled = true;
        resolve();
      }
    });

    child.once("error", (error) => {
      if (!settled) {
        settled = true;
        reject(error);
      }
    });

    child.once("exit", (code, signal) => {
      if (!settled) {
        settled = true;
        reject(new Error(`Failed to acquire rollout file lock. Exit code: ${code ?? "null"}, signal: ${signal ?? "null"}`));
      }
    });
  });

  return child;
}

async function runCli(args) {
  const cliPath = path.resolve("src", "cli.js");
  return await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [cliPath, ...args], {
      cwd: path.resolve("."),
      stdio: ["ignore", "pipe", "pipe"]
    });

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString("utf8");
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString("utf8");
    });
    child.once("error", reject);
    child.once("exit", (code) => {
      resolve({ code, stdout, stderr });
    });
  });
}

test("runSync rewrites rollout files and sqlite, then restore reverts both", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  const archivedPath = path.join(codexHome, "archived_sessions", "2026", "03", "18", "rollout-b.jsonl");
  await writeRollout(sessionPath, "thread-a", "apigather");
  await writeRollout(archivedPath, "thread-b", "newapi");
  await writeStateDb(codexHome, [
    { id: "thread-a", model_provider: "apigather", archived: false },
    { id: "thread-b", model_provider: "newapi", archived: true }
  ]);

  const syncResult = await runSync({ codexHome });
  assert.equal(syncResult.targetProvider, "openai");
  assert.equal(typeof syncResult.backupDurationMs, "number");
  assert.ok(syncResult.backupDurationMs >= 0);
  assert.equal(syncResult.changedSessionFiles, 2);
  assert.deepEqual(syncResult.skippedLockedRolloutFiles, []);
  assert.equal(syncResult.sqliteRowsUpdated, 2);
  const backupMetadata = JSON.parse(await fs.readFile(path.join(syncResult.backupDir, "metadata.json"), "utf8"));
  assert.equal(backupMetadata.version, 2);
  assert.equal(backupMetadata.sqliteHome, path.join(codexHome, SQLITE_DIR_BASENAME));
  assert.deepEqual(backupMetadata.sqliteDbFiles, [DB_FILE_BASENAME]);
  assert.deepEqual(
    backupMetadata.dbFiles.map((fileName) => fileName.replaceAll("\\", "/")),
    ["sqlite/state_5.sqlite"]
  );

  const syncedSession = await fs.readFile(sessionPath, "utf8");
  const syncedArchived = await fs.readFile(archivedPath, "utf8");
  assert.match(syncedSession, /"model_provider":"openai"/);
  assert.match(syncedArchived, /"model_provider":"openai"/);

  const db = await openDatabase(stateDbPath(codexHome));
  try {
    const providers = db
      .prepare("SELECT id, model_provider FROM threads ORDER BY id")
      .all()
      .map((row) => ({ ...row }));
    assert.deepEqual(providers, [
      { id: "thread-a", model_provider: "openai" },
      { id: "thread-b", model_provider: "openai" }
    ]);
  } finally {
    db.close();
  }

  await runRestore({ codexHome, backupDir: syncResult.backupDir });

  const restoredSession = await fs.readFile(sessionPath, "utf8");
  const restoredArchived = await fs.readFile(archivedPath, "utf8");
  assert.match(restoredSession, /"model_provider":"apigather"/);
  assert.match(restoredArchived, /"model_provider":"newapi"/);
});

test("runSync updates legacy root sqlite database when sqlite-dir state is stale", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-active-a.jsonl");
  const archivedPath = path.join(codexHome, "archived_sessions", "2026", "03", "18", "rollout-active-b.jsonl");
  await writeRollout(sessionPath, "thread-active-a", "dal");
  await writeRollout(archivedPath, "thread-active-b", "dal");
  await writeStateDb(codexHome, [
    { id: "thread-active-a", model_provider: "dal", archived: false }
  ]);
  await writeLegacyStateDb(codexHome, [
    { id: "thread-active-a", model_provider: "dal", archived: false },
    { id: "thread-active-b", model_provider: "dal", archived: true }
  ]);

  const syncResult = await runSync({ codexHome });

  assert.equal(syncResult.sqliteRowsUpdated, 2);
  const backupMetadata = JSON.parse(await fs.readFile(path.join(syncResult.backupDir, "metadata.json"), "utf8"));
  assert.equal(backupMetadata.version, 2);
  assert.equal(backupMetadata.sqliteHome, codexHome);
  assert.deepEqual(backupMetadata.sqliteDbFiles, [DB_FILE_BASENAME]);
  assert.deepEqual(backupMetadata.dbFiles, [DB_FILE_BASENAME]);

  const legacyDb = await openDatabase(legacyStateDbPath(codexHome));
  try {
    assert.deepEqual(
      legacyDb.prepare("SELECT id, model_provider FROM threads ORDER BY id").all().map((row) => ({ ...row })),
      [
        { id: "thread-active-a", model_provider: "openai" },
        { id: "thread-active-b", model_provider: "openai" }
      ]
    );
  } finally {
    legacyDb.close();
  }

  const staleDb = await openDatabase(stateDbPath(codexHome));
  try {
    assert.deepEqual(
      staleDb.prepare("SELECT id, model_provider FROM threads ORDER BY id").all().map((row) => ({ ...row })),
      [
        { id: "thread-active-a", model_provider: "dal" }
      ]
    );
  } finally {
    staleDb.close();
  }
});

test("runSync uses an explicit SQLite home and never touches a stale Codex Home database", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-external.jsonl");
  await writeRollout(sessionPath, "thread-external", "custom");

  const sqliteHome = path.join(root, "external-sqlite");
  const externalDbPath = path.join(sqliteHome, DB_FILE_BASENAME);
  await writeStateDbAt(externalDbPath, [
    { id: "thread-external", model_provider: "custom", archived: false }
  ]);
  await writeStateDb(codexHome, [
    { id: "thread-stale", model_provider: "stale", archived: false }
  ]);

  const result = await runSync({ codexHome, sqliteHome });
  assert.equal(result.sqliteHome, sqliteHome);
  assert.equal(result.sqliteHomeSource, "cli");

  const externalDb = await openDatabase(externalDbPath);
  try {
    assert.equal(
      externalDb.prepare("SELECT model_provider FROM threads WHERE id = ?").get("thread-external").model_provider,
      "openai"
    );
  } finally {
    externalDb.close();
  }

  const staleDb = await openDatabase(stateDbPath(codexHome));
  try {
    assert.equal(
      staleDb.prepare("SELECT model_provider FROM threads WHERE id = ?").get("thread-stale").model_provider,
      "stale"
    );
  } finally {
    staleDb.close();
  }

  const metadata = JSON.parse(await fs.readFile(path.join(result.backupDir, "metadata.json"), "utf8"));
  assert.equal(metadata.version, 2);
  assert.equal(metadata.sqliteHome, sqliteHome);
  assert.deepEqual(metadata.dbFiles, []);
  assert.deepEqual(metadata.sqliteDbFiles, [DB_FILE_BASENAME]);
  await fs.access(path.join(result.backupDir, "db", "sqlite-home", DB_FILE_BASENAME));

  await runRestore({ codexHome, sqliteHome, backupDir: result.backupDir });
  const restoredDb = await openDatabase(externalDbPath);
  try {
    assert.equal(
      restoredDb.prepare("SELECT model_provider FROM threads WHERE id = ?").get("thread-external").model_provider,
      "custom"
    );
  } finally {
    restoredDb.close();
  }
});

test("runSync blocks Windows WSL UNC SQLite homes before creating a backup", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const configPath = path.join(codexHome, "config.toml");
  const originalConfig = await fs.readFile(configPath, "utf8");

  try {
    await assert.rejects(
      () => runSync({
        codexHome,
        sqliteHome: "\\\\wsl.localhost\\Ubuntu\\home\\user\\.codex\\sqlite",
        platform: "win32"
      }),
      /Cannot sync.*Run codex-provider inside WSL/
    );
    assert.equal(await fs.readFile(configPath, "utf8"), originalConfig);
    await assert.rejects(() => fs.access(backupRoot(codexHome)));
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("configured SQLite home reports a missing database and blocks writes without fallback", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  const sqliteHome = path.join(root, "missing-sqlite");
  await fs.writeFile(
    path.join(codexHome, "config.toml"),
    `model_provider = "openai"\nsqlite_home = '${sqliteHome}'\n`,
    "utf8"
  );
  await writeStateDb(codexHome, [
    { id: "thread-stale", model_provider: "custom", archived: false }
  ]);

  const status = await getStatus({ codexHome });
  assert.equal(status.sqliteHome, sqliteHome);
  assert.equal(status.sqliteHomeSource, "config");
  assert.equal(status.stateDbLocation, null);
  assert.match(renderStatus(status), /database: not found/);
  assert.match(renderStatus(status), new RegExp(sqliteHome.replace(/[\\^$.*+?()[\]{}|]/g, "\\$&")));

  await assert.rejects(
    () => runSync({ codexHome }),
    /not found in configured SQLite home/
  );
});

test("v2 restore rejects SQLite home relocation unless explicitly allowed", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-relocation.jsonl");
  await writeRollout(sessionPath, "thread-relocation", "custom");

  const sourceSqliteHome = path.join(root, "source-sqlite");
  const targetSqliteHome = path.join(root, "target-sqlite");
  await writeStateDbAt(path.join(sourceSqliteHome, DB_FILE_BASENAME), [
    { id: "thread-relocation", model_provider: "custom", archived: false }
  ]);
  await writeStateDbAt(path.join(targetSqliteHome, DB_FILE_BASENAME), [
    { id: "thread-relocation", model_provider: "openai", archived: false }
  ]);

  const syncResult = await runSync({ codexHome, sqliteHome: sourceSqliteHome });
  await assert.rejects(
    () => runRestore({ codexHome, sqliteHome: targetSqliteHome, backupDir: syncResult.backupDir }),
    /Use --allow-sqlite-home-relocation/
  );
  await assert.rejects(
    () => runRestore({ codexHome, backupDir: syncResult.backupDir, allowSqliteHomeRelocation: true }),
    /requires an explicit --sqlite-home/
  );
  await assert.rejects(
    () => runRestore({
      codexHome,
      sqliteHome: targetSqliteHome,
      backupDir: syncResult.backupDir,
      allowSqliteHomeRelocation: true
    }),
    /Cannot restore config\.toml while relocating SQLite home/
  );

  await runRestore({
    codexHome,
    sqliteHome: targetSqliteHome,
    backupDir: syncResult.backupDir,
    restoreConfig: false,
    allowSqliteHomeRelocation: true
  });
  const targetDb = await openDatabase(path.join(targetSqliteHome, DB_FILE_BASENAME));
  try {
    assert.equal(
      targetDb.prepare("SELECT model_provider FROM threads WHERE id = ?").get("thread-relocation").model_provider,
      "custom"
    );
  } finally {
    targetDb.close();
  }
});

test("v2 restore rebuilds a missing default SQLite database", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  await writeStateDb(codexHome, [
    { id: "thread-missing-default", model_provider: "custom", archived: false }
  ]);

  const syncResult = await runSync({ codexHome });
  await fs.rm(stateDbPath(codexHome));

  await runRestore({
    codexHome,
    backupDir: syncResult.backupDir,
    restoreConfig: false,
    restoreSessions: false
  });

  const restoredDb = await openDatabase(stateDbPath(codexHome));
  try {
    assert.equal(
      restoredDb.prepare("SELECT model_provider FROM threads WHERE id = ?").get("thread-missing-default").model_provider,
      "custom"
    );
  } finally {
    restoredDb.close();
  }
});

test("v2 restore rebuilds a missing legacy root SQLite database in place", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  await writeLegacyStateDb(codexHome, [
    { id: "thread-missing-legacy", model_provider: "custom", archived: false }
  ]);

  const syncResult = await runSync({ codexHome });
  await fs.rm(legacyStateDbPath(codexHome));

  await runRestore({
    codexHome,
    backupDir: syncResult.backupDir,
    restoreConfig: false,
    restoreSessions: false
  });

  await assert.rejects(() => fs.access(stateDbPath(codexHome)));
  const restoredDb = await openDatabase(legacyStateDbPath(codexHome));
  try {
    assert.equal(
      restoredDb.prepare("SELECT model_provider FROM threads WHERE id = ?").get("thread-missing-legacy").model_provider,
      "custom"
    );
  } finally {
    restoredDb.close();
  }
});

test("restoreBackup keeps metadata v1 database paths compatible", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  await writeStateDb(codexHome, [
    { id: "thread-v1", model_provider: "openai", archived: false }
  ]);

  const backupDir = path.join(backupRoot(codexHome), "v1-restore");
  const backupDbPath = path.join(backupDir, "db", SQLITE_DIR_BASENAME, DB_FILE_BASENAME);
  await writeStateDbAt(backupDbPath, [
    { id: "thread-v1", model_provider: "custom", archived: false }
  ]);
  await fs.writeFile(
    path.join(backupDir, "metadata.json"),
    JSON.stringify({
      version: 1,
      namespace: "provider-sync",
      codexHome,
      targetProvider: "custom",
      createdAt: "2026-03-24T00:00:00.000Z",
      dbFiles: [path.join(SQLITE_DIR_BASENAME, DB_FILE_BASENAME)],
      changedSessionFiles: 0
    }),
    "utf8"
  );
  await fs.rm(stateDbPath(codexHome));

  await restoreBackup(backupDir, codexHome, {
    restoreConfig: false,
    restoreSessions: false
  });

  const restoredDb = await openDatabase(stateDbPath(codexHome));
  try {
    assert.equal(
      restoredDb.prepare("SELECT model_provider FROM threads WHERE id = ?").get("thread-v1").model_provider,
      "custom"
    );
  } finally {
    restoredDb.close();
  }
});

test("restore validates v2 SQLite files before restoring config", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  await writeStateDb(codexHome, [
    { id: "thread-restore-validation", model_provider: "custom", archived: false }
  ]);
  const syncResult = await runSync({ codexHome });
  await fs.rm(path.join(syncResult.backupDir, "db", "sqlite-home", DB_FILE_BASENAME));

  const currentConfig = 'model_provider = "sentinel"\n';
  await fs.writeFile(path.join(codexHome, "config.toml"), currentConfig, "utf8");
  await assert.rejects(
    () => runRestore({ codexHome, backupDir: syncResult.backupDir }),
    /declares a missing SQLite file/
  );
  assert.equal(await fs.readFile(path.join(codexHome, "config.toml"), "utf8"), currentConfig);
});

test("runSync reports stage progress and backup duration", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  await writeRollout(sessionPath, "thread-a", "apigather");
  await writeStateDb(codexHome, [
    { id: "thread-a", model_provider: "apigather", archived: false }
  ]);

  const progressEvents = [];
  const result = await runSync({
    codexHome,
    onProgress(event) {
      progressEvents.push(event);
    }
  });

  assert.ok(result.backupDurationMs >= 0);
  assert.deepEqual(
    progressEvents
      .filter((event) => event.status === "start")
      .map((event) => event.stage),
    [
      "scan_rollout_files",
      "check_locked_rollout_files",
      "create_backup",
      "update_sqlite",
      "rewrite_rollout_files",
      "clean_backups"
    ]
  );

  const backupCompleteEvent = progressEvents.find((event) => event.stage === "create_backup" && event.status === "complete");
  assert.ok(backupCompleteEvent);
  assert.equal(backupCompleteEvent.backupDir, result.backupDir);
  assert.ok(backupCompleteEvent.durationMs >= 0);
});

test("runSync repairs SQLite has_user_event from rollout user messages", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  await writeRollout(sessionPath, "thread-a", "openai");
  await writeStateDbWithUserEventColumn(codexHome, [
    { id: "thread-a", model_provider: "openai", archived: false, has_user_event: false }
  ]);

  const syncResult = await runSync({ codexHome });

  assert.equal(syncResult.changedSessionFiles, 0);
  assert.equal(syncResult.sqliteRowsUpdated, 1);
  assert.equal(syncResult.sqliteUserEventRowsUpdated, 1);

  const db = await openDatabase(stateDbPath(codexHome));
  try {
    const row = db
      .prepare("SELECT has_user_event FROM threads WHERE id = ?")
      .get("thread-a");
    assert.equal(row.has_user_event, 1);
  } finally {
    db.close();
  }
});

test("runSync repairs SQLite cwd from rollout session metadata", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-cwd.jsonl");
  await writeCustomRollout(sessionPath, {
    id: "thread-cwd",
    timestamp: "2026-03-19T00:00:00.000Z",
    cwd: "D:\\GitHubProject\\oss-maintainer-hub",
    source: "vscode",
    cli_version: "0.115.0",
    model_provider: "openai"
  });
  await writeStateDb(codexHome, [
    {
      id: "thread-cwd",
      model_provider: "openai",
      archived: false,
      cwd: "\\\\?\\D:\\GitHubProject\\oss-maintainer-hub"
    }
  ]);

  const syncResult = await runSync({ codexHome });

  assert.equal(syncResult.changedSessionFiles, 0);
  assert.equal(syncResult.sqliteRowsUpdated, 1);
  assert.equal(syncResult.sqliteCwdRowsUpdated, 1);

  const db = await openDatabase(stateDbPath(codexHome));
  try {
    const row = db
      .prepare("SELECT cwd FROM threads WHERE id = ?")
      .get("thread-cwd");
    assert.equal(row.cwd, "D:\\GitHubProject\\oss-maintainer-hub");
  } finally {
    db.close();
  }
});

test("runSync normalizes extended rollout cwd before repairing SQLite", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-cwd-extended.jsonl");
  await writeCustomRollout(sessionPath, {
    id: "thread-cwd-extended",
    timestamp: "2026-03-19T00:00:00.000Z",
    cwd: "\\\\?\\E:\\GitHubProject\\lin-framework",
    source: "vscode",
    cli_version: "0.115.0",
    model_provider: "openai"
  });
  await writeStateDb(codexHome, [
    {
      id: "thread-cwd-extended",
      model_provider: "openai",
      archived: false,
      cwd: "\\\\?\\E:\\GitHubProject\\lin-framework"
    }
  ]);

  const syncResult = await runSync({ codexHome });

  assert.equal(syncResult.sqliteRowsUpdated, 1);
  assert.equal(syncResult.sqliteCwdRowsUpdated, 1);

  const db = await openDatabase(stateDbPath(codexHome));
  try {
    const row = db
      .prepare("SELECT cwd FROM threads WHERE id = ?")
      .get("thread-cwd-extended");
    assert.equal(row.cwd, "E:\\GitHubProject\\lin-framework");
  } finally {
    db.close();
  }
});

test("runSync restores workspace roots from project order and normalizes them to Desktop path variants", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const originalState = {
    "electron-saved-workspace-roots": [
      "\\\\?\\D:\\GitHubProject\\codex-provider-sync"
    ],
    "project-order": [
      "\\\\?\\D:\\GitHubProject\\codex-provider-sync",
      "\\\\?\\E:\\NewRich\\BrainLife\\Code\\BrainLife\\Assets"
    ],
    "active-workspace-roots": [
      "\\\\?\\D:\\GitHubProject\\codex-provider-sync"
    ],
    "electron-workspace-root-labels": {
      "\\\\?\\E:\\NewRich\\BrainLife\\Code\\BrainLife\\Assets": "BrainLifeAssets"
    }
  };
  await writeGlobalState(codexHome, originalState);
  await writeStateDb(codexHome, [
    {
      id: "thread-a",
      model_provider: "openai",
      archived: false,
      cwd: "\\\\?\\D:\\GitHubProject\\codex-provider-sync"
    },
    {
      id: "thread-b",
      model_provider: "openai",
      archived: false,
      cwd: "\\\\?\\E:\\NewRich\\BrainLife\\Code\\BrainLife\\Assets"
    }
  ]);

  const syncResult = await runSync({ codexHome });
  assert.equal(syncResult.updatedWorkspaceRoots, 2);

  const syncedState = JSON.parse(await fs.readFile(path.join(codexHome, ".codex-global-state.json"), "utf8"));
  assert.deepEqual(syncedState["electron-saved-workspace-roots"], [
    "D:\\GitHubProject\\codex-provider-sync",
    "E:\\NewRich\\BrainLife\\Code\\BrainLife\\Assets"
  ]);
  assert.deepEqual(syncedState["project-order"], [
    "D:\\GitHubProject\\codex-provider-sync",
    "E:\\NewRich\\BrainLife\\Code\\BrainLife\\Assets"
  ]);
  assert.deepEqual(syncedState["active-workspace-roots"], [
    "D:\\GitHubProject\\codex-provider-sync"
  ]);
  assert.equal(
    syncedState["electron-workspace-root-labels"]["E:\\NewRich\\BrainLife\\Code\\BrainLife\\Assets"],
    "BrainLifeAssets"
  );

  await runRestore({ codexHome, backupDir: syncResult.backupDir });

  const restoredState = JSON.parse(await fs.readFile(path.join(codexHome, ".codex-global-state.json"), "utf8"));
  assert.deepEqual(restoredState["electron-saved-workspace-roots"], originalState["electron-saved-workspace-roots"]);
  assert.deepEqual(restoredState["project-order"], originalState["project-order"]);
});

test("runSwitch updates config and syncs provider metadata", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome);
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  await writeRollout(sessionPath, "thread-a", "openai");
  await writeStateDb(codexHome, [
    { id: "thread-a", model_provider: "openai", archived: false }
  ]);

  const result = await runSwitch({ codexHome, provider: "apigather" });
  assert.equal(result.targetProvider, "apigather");

  const config = await fs.readFile(path.join(codexHome, "config.toml"), "utf8");
  assert.match(config, /^model_provider = "apigather"/m);
  const rollout = await fs.readFile(sessionPath, "utf8");
  assert.match(rollout, /"model_provider":"apigather"/);
});

test("runSwitch completes a pre-switch backup before mutating config", async () => {
  const { codexHome } = await makeTempCodexHome();
  const originalConfig = `model_provider = "openai"\nmodel = "gpt-5.4-mini"\n\n[model_providers.apigather]\nmodel = "apigather-prod"\nbase_url = "https://example.com"\n`;
  const configPath = path.join(codexHome, "config.toml");
  await fs.writeFile(configPath, originalConfig, "utf8");

  const progressEvents = [];
  const result = await runSwitch({
    codexHome,
    provider: "apigather",
    onProgress(event) {
      progressEvents.push(event);
    }
  });

  const backupCompleteIndex = progressEvents.findIndex(
    (event) => event.stage === "create_backup" && event.status === "complete"
  );
  const configUpdateIndex = progressEvents.findIndex(
    (event) => event.stage === "update_config" && event.status === "start"
  );
  assert.ok(backupCompleteIndex >= 0);
  assert.ok(configUpdateIndex > backupCompleteIndex);
  assert.equal(
    await fs.readFile(path.join(result.backupDir, "config.toml"), "utf8"),
    originalConfig
  );

  const switchedConfig = await fs.readFile(configPath, "utf8");
  assert.match(switchedConfig, /^model_provider = "apigather"/m);
  assert.match(switchedConfig, /^model = "apigather-prod"/m);
});

test("runSwitch does not touch config when pre-switch backup creation fails", async () => {
  const { codexHome } = await makeTempCodexHome();
  const originalConfig = `model_provider = "openai"\nmodel = "gpt-5.4-mini"\n\n[model_providers.apigather]\nbase_url = "https://example.com"\n`;
  const configPath = path.join(codexHome, "config.toml");
  await fs.writeFile(configPath, originalConfig, "utf8");
  const pinnedMtime = new Date("2001-02-03T04:05:06.000Z");
  await fs.utimes(configPath, pinnedMtime, pinnedMtime);

  await fs.mkdir(path.dirname(backupRoot(codexHome)), { recursive: true });
  await fs.writeFile(backupRoot(codexHome), "blocks backup directory creation", "utf8");

  await assert.rejects(() => runSwitch({ codexHome, provider: "apigather" }));
  assert.equal(await fs.readFile(configPath, "utf8"), originalConfig);
  assert.equal((await fs.stat(configPath)).mtimeMs, pinnedMtime.getTime());
});

test("runSwitch restores config after a post-backup sync failure", async () => {
  const { codexHome } = await makeTempCodexHome();
  const originalConfig = `model_provider = "openai"\nmodel = "gpt-5.4-mini"\n\n[model_providers.apigather]\nbase_url = "https://example.com"\n`;
  const configPath = path.join(codexHome, "config.toml");
  await fs.writeFile(configPath, originalConfig, "utf8");
  await fs.writeFile(path.join(codexHome, ".codex-global-state.json"), "{not-json", "utf8");

  await assert.rejects(
    () => runSwitch({ codexHome, provider: "apigather" }),
    /JSON|position|property name/i
  );
  assert.equal(await fs.readFile(configPath, "utf8"), originalConfig);

  const backupDirectories = await fs.readdir(backupRoot(codexHome));
  assert.equal(backupDirectories.length, 1);
  assert.equal(
    await fs.readFile(path.join(backupRoot(codexHome), backupDirectories[0], "config.toml"), "utf8"),
    originalConfig
  );
});

test("runSwitch copies root-level model from the new provider section", async () => {
  const { codexHome } = await makeTempCodexHome();
  const config = `model_provider = "openai"\nmodel = "gpt-5.4-mini"\n\n[model_providers.apigather]\nmodel = "apigather-prod"\nbase_url = "https://example.com"\n`;
  await fs.writeFile(path.join(codexHome, "config.toml"), config, "utf8");

  const result = await runSwitch({ codexHome, provider: "apigather" });
  assert.equal(result.modelSync.applied, true);
  assert.equal(result.modelSync.source, "provider-section");
  assert.equal(result.modelSync.model, "apigather-prod");

  const next = await fs.readFile(path.join(codexHome, "config.toml"), "utf8");
  assert.match(next, /^model_provider = "apigather"/m);
  assert.match(next, /^model = "apigather-prod"/m);
});

test("runSwitch keeps existing model when --keep-root-model is set", async () => {
  const { codexHome } = await makeTempCodexHome();
  const config = `model_provider = "openai"\nmodel = "gpt-5.4-mini"\n\n[model_providers.apigather]\nmodel = "apigather-prod"\nbase_url = "https://example.com"\n`;
  await fs.writeFile(path.join(codexHome, "config.toml"), config, "utf8");

  const result = await runSwitch({ codexHome, provider: "apigather", keepRootModel: true });
  assert.equal(result.modelSync.applied, false);
  assert.equal(result.modelSync.source, "none");

  const next = await fs.readFile(path.join(codexHome, "config.toml"), "utf8");
  assert.match(next, /^model_provider = "apigather"/m);
  assert.match(next, /^model = "gpt-5.4-mini"/m);
});

test("runSwitch applies --model override and writes it to config.toml", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model = "gpt-5.4-mini"');

  const result = await runSwitch({ codexHome, provider: "apigather", model: "Custom-Large" });
  assert.equal(result.modelSync.applied, true);
  assert.equal(result.modelSync.source, "explicit");
  assert.equal(result.modelSync.model, "Custom-Large");

  const next = await fs.readFile(path.join(codexHome, "config.toml"), "utf8");
  assert.match(next, /^model = "Custom-Large"/m);
});

test("runSwitch emits a warning when the new provider has no model field", async () => {
  const { codexHome } = await makeTempCodexHome();
  const config = `model_provider = "openai"\nmodel = "gpt-5.4-mini"\n\n[model_providers.apigather]\nbase_url = "https://example.com"\n`;
  await fs.writeFile(path.join(codexHome, "config.toml"), config, "utf8");

  const result = await runSwitch({ codexHome, provider: "apigather" });
  assert.equal(result.modelSync.applied, false);
  assert.match(result.modelSync.warning ?? "", /no model field/);

  const next = await fs.readFile(path.join(codexHome, "config.toml"), "utf8");
  assert.match(next, /^model = "gpt-5.4-mini"/m);
});

test("runSwitch does not treat a provider-section model as the root model", async () => {
  const { codexHome } = await makeTempCodexHome();
  const config = `model_provider = "apigather"\n\n[model_providers.apigather]\nmodel = "provider-section-only"\nbase_url = "https://example.com"\n`;
  await fs.writeFile(path.join(codexHome, "config.toml"), config, "utf8");
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-root-model.jsonl");
  await writeRolloutWithTurnContext(sessionPath, {
    id: "thread-root-model",
    provider: "apigather",
    model: "original-rollout-model"
  });

  const result = await runSwitch({ codexHome, provider: "openai" });

  assert.equal(result.modelSync.applied, false);
  const nextConfig = await fs.readFile(path.join(codexHome, "config.toml"), "utf8");
  assert.doesNotMatch(nextConfig.split("[model_providers.", 1)[0], /^model\s*=/m);
  const turnContexts = (await fs.readFile(sessionPath, "utf8"))
    .trim()
    .split(/\r?\n/)
    .map((line) => JSON.parse(line))
    .filter((entry) => entry.type === "turn_context");
  assert.ok(turnContexts.length > 0);
  for (const entry of turnContexts) {
    assert.equal(entry.payload.model, "original-rollout-model");
    assert.equal(entry.payload.collaboration_mode.settings.model, "original-rollout-model");
  }
});

test("runSwitch rejects --model and --keep-root-model together", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome);
  const before = await fs.readFile(path.join(codexHome, "config.toml"), "utf8");

  await assert.rejects(
    () => runSwitch({ codexHome, provider: "apigather", model: "X", keepRootModel: true }),
    /--model and --keep-root-model are mutually exclusive/
  );

  // Confirm the file on disk was not mutated by the failed call.
  const after = await fs.readFile(path.join(codexHome, "config.toml"), "utf8");
  assert.equal(after, before);
});

test("runSync rewrites the per-thread model column when a model is provided", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"\nmodel = "gpt-5.4"\n');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  await writeRollout(sessionPath, "thread-a", "openai");
  await writeStateDb(codexHome, [
    { id: "thread-a", model_provider: "openai", model: "gpt-5.4-mini", archived: false }
  ]);

  const result = await runSync({ codexHome, model: "MiniMax-M3" });
  assert.ok(result.sqliteRowsUpdated >= 1, "model column should be updated");

  const db = await openDatabase(path.join(codexHome, "sqlite", "state_5.sqlite"));
  try {
    const row = db.prepare("SELECT model, model_provider FROM threads WHERE id = ?").get("thread-a");
    assert.equal(row.model, "MiniMax-M3");
    assert.equal(row.model_provider, "openai");
  } finally {
    db.close();
  }
});

test("runSync leaves the per-thread model column untouched when no model is provided", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"\nmodel = "gpt-5.4"\n');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  await writeRollout(sessionPath, "thread-a", "openai");
  await writeStateDb(codexHome, [
    { id: "thread-a", model_provider: "openai", model: "gpt-5.4-mini", archived: false }
  ]);

  await runSync({ codexHome });

  const db = await openDatabase(path.join(codexHome, "sqlite", "state_5.sqlite"));
  try {
    const row = db.prepare("SELECT model, model_provider FROM threads WHERE id = ?").get("thread-a");
    assert.equal(row.model, "gpt-5.4-mini", "model must remain unchanged when caller does not pass one");
    assert.equal(row.model_provider, "openai");
  } finally {
    db.close();
  }
});

test("runSync rewrites the per-turn turn_context model field in rollout files", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"\nmodel = "MiniMax-M3"\n');
  const sessionPath = path.join(codexHome, "sessions", "2026", "06", "09", "rollout-a.jsonl");
  await writeRolloutWithTurnContext(sessionPath, {
    id: "thread-a",
    provider: "apigather",
    model: "gpt-5.4"
  });

  const result = await runSync({ codexHome, model: "MiniMax-M3" });
  assert.equal(result.changedSessionFiles, 1);

  const lines = (await fs.readFile(sessionPath, "utf8")).split("\n").filter(Boolean);
  const turnContextLines = lines
    .map((line) => JSON.parse(line))
    .filter((entry) => entry.type === "turn_context");
  assert.equal(turnContextLines.length, 2);
  for (const entry of turnContextLines) {
    assert.equal(entry.payload.model, "MiniMax-M3");
    assert.equal(entry.payload.collaboration_mode.settings.model, "MiniMax-M3");
  }
});

test("runSync rewrites turn_context model even when the line is larger than 64 KB", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"\nmodel = "MiniMax-M3"\n');
  const sessionPath = path.join(codexHome, "sessions", "2026", "06", "09", "rollout-huge.jsonl");
  await fs.mkdir(path.dirname(sessionPath), { recursive: true });

  // Build a turn_context line whose `developer_instructions` blob
  // pushes the encoded JSON well past the previous 64 KB scanner cap.
  const devInstructions = "x".repeat(150 * 1024);
  const turnContext = {
    timestamp: "2026-06-09T09:16:03.880Z",
    type: "turn_context",
    payload: {
      turn_id: "019eabaa-very-large",
      cwd: "C:\\AITemp",
      model: "gpt-5.4",
      developer_instructions: devInstructions,
      collaboration_mode: { mode: "default", settings: { model: "gpt-5.4", reasoning_effort: "xhigh" } }
    }
  };
  const meta = JSON.stringify({
    timestamp: "2026-06-09T09:16:03.878Z",
    type: "session_meta",
    payload: { id: "thread-huge", timestamp: "2026-06-09T09:16:03.878Z", cwd: "C:\\AITemp", source: "cli", cli_version: "0.115.0", model_provider: "apigather" }
  });
  await fs.writeFile(sessionPath, `${meta}\n${JSON.stringify(turnContext)}\n`, "utf8");

  // Sanity: the encoded turn_context line should exceed 64 KB.
  const onDisk = await fs.readFile(sessionPath, "utf8");
  const lineLengths = onDisk.split("\n").filter(Boolean).map((line) => line.length);
  assert.ok(Math.max(...lineLengths) > 64 * 1024, "test setup: line should exceed 64 KB");

  const result = await runSync({ codexHome, model: "MiniMax-M3" });
  assert.equal(result.changedSessionFiles, 1);

  const lines = (await fs.readFile(sessionPath, "utf8")).split("\n").filter(Boolean);
  for (const line of lines) {
    if (!line.includes('"turn_context"')) continue;
    const parsed = JSON.parse(line);
    assert.equal(parsed.payload.model, "MiniMax-M3");
    assert.equal(parsed.payload.collaboration_mode.settings.model, "MiniMax-M3");
  }
});

test("runSync rewrites turn_context whose model name contains regex metacharacters", async () => {
  // Names like `gpt-5.4-mini` and `apigather.fixed+name` should
  // be matched literally — `.` is a regex any-char, `+` is a
  // quantifier, and an unbalanced `{` would refuse to compile. We
  // rewrite every turn_context.model field to the new target so
  // the per-turn model is normalised regardless of what was
  // there before. The decoy line at the bottom (a non-turn_context
  // event with the literal text in a field) confirms the rewrite
  // does not over-match into unrelated events.
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"\nmodel = "weird(target)+v2"\n');
  const sessionPath = path.join(codexHome, "sessions", "2026", "06", "09", "rollout-a.jsonl");
  await writeRolloutWithTurnContext(sessionPath, {
    id: "thread-a",
    provider: "apigather",
    model: "weird(target)+v2"
  });
  // A second turn_context with a different model that has the same
  // metacharacter pattern; both must be normalised to the target.
  await fs.appendFile(sessionPath, JSON.stringify({
    timestamp: "2026-06-09T09:16:03.881Z", type: "turn_context",
    payload: { turn_id: "decoy", model: "weirdAtargetAv2" }
  }) + "\n", "utf8");
  // A non-turn_context event whose text happens to include the
  // literal model string. The rewrite must NOT touch this line.
  await fs.appendFile(sessionPath, JSON.stringify({
    timestamp: "2026-06-09T09:16:03.882Z", type: "user_message",
    payload: { message: "echo weird(target)+v2 please" }
  }) + "\n", "utf8");

  const result = await runSync({ codexHome, model: "weird(target)+v2" });
  assert.equal(result.changedSessionFiles, 1);

  const lines = (await fs.readFile(sessionPath, "utf8")).split("\n").filter(Boolean);
  const parsed = lines.map((line) => JSON.parse(line));
  // Both turn_context lines should now have the target model.
  for (const entry of parsed) {
    if (entry.type === "turn_context") {
      assert.equal(entry.payload.model, "weird(target)+v2");
    }
  }
  // The non-turn_context line must be left alone.
  const userMessage = parsed.find((entry) => entry.type === "user_message");
  assert.ok(userMessage, "user_message line should still be present");
  assert.equal(userMessage.payload.message, "echo weird(target)+v2 please");
});

test("runSync rewrites turn_context model when the provider is already correct (model-only change)", async () => {
  // Owner review regression: when the root-level `model = "..."`
  // in config.toml changes but `model_provider` is the same as
  // what every rollout already has, the rollout's
  // turn_context.model must still be updated. Otherwise the
  // Codex GUI bottom-right of an old conversation would show the
  // old model even though the user just switched the active
  // model.
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"\nmodel = "gpt-5.1"\n');
  const sessionPath = path.join(codexHome, "sessions", "2026", "06", "09", "rollout-a.jsonl");
  await writeRolloutWithTurnContext(sessionPath, {
    id: "thread-a",
    provider: "openai",
    model: "gpt-5"
  });

  // Run sync with no provider change (provider = openai is already
  // the target), only the model changes. The rollout's
  // turn_context.model must move from "gpt-5" to "gpt-5.1" and
  // the first line (session_meta) must NOT be touched.
  const originalFirstLine = (await fs.readFile(sessionPath, "utf8")).split("\n")[0];
  const result = await runSync({ codexHome, provider: "openai", model: "gpt-5.1" });
  assert.ok(
    result.changedSessionFiles >= 1,
    "rollout with a stale turn_context.model must be picked up as a model-only change"
  );

  const lines = (await fs.readFile(sessionPath, "utf8")).split("\n").filter(Boolean);
  // First line (session_meta) must be untouched — the provider was
  // already correct.
  assert.equal(lines[0], originalFirstLine, "session_meta first line must not be touched on a model-only change");
  for (const line of lines) {
    if (!line.includes('"turn_context"')) continue;
    const parsed = JSON.parse(line);
    assert.equal(parsed.payload.model, "gpt-5.1");
  }
});

test("runSync normalises multiple distinct models in the same session to the target model", async () => {
  // Owner review regression: a single Codex session can use
  // different models in different turn_context lines (the user
  // switched models mid-conversation). The per-turn rewrite
  // pass must normalise ALL of them to the new target, not just
  // the ones whose value happens to match the first turn_context
  // model.
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"\nmodel = "gpt-5.1"\n');
  const sessionPath = path.join(codexHome, "sessions", "2026", "06", "09", "rollout-a.jsonl");
  await writeRolloutWithTurnContext(sessionPath, {
    id: "thread-a",
    provider: "apigather",
    model: "gpt-5"
  });
  // Three more turn_context lines with three different models.
  for (const [turnId, model] of [
    ["t2", "gpt-4o-mini"],
    ["t3", "gpt-5"],
    ["t4", "claude-3.5-sonnet"]
  ]) {
    await fs.appendFile(
      sessionPath,
      JSON.stringify({
        timestamp: "2026-06-09T09:16:03.880Z",
        type: "turn_context",
        payload: { turn_id: turnId, model, collaboration_mode: { mode: "default", settings: { model } } }
      }) + "\n",
      "utf8"
    );
  }

  const result = await runSync({ codexHome, model: "gpt-5.1" });
  assert.equal(result.changedSessionFiles, 1);

  const lines = (await fs.readFile(sessionPath, "utf8")).split("\n").filter(Boolean);
  for (const line of lines) {
    if (!line.includes('"turn_context"')) continue;
    const parsed = JSON.parse(line);
    assert.equal(parsed.payload.model, "gpt-5.1", `turn ${parsed.payload.turn_id} should be normalised`);
    assert.equal(parsed.payload.collaboration_mode.settings.model, "gpt-5.1");
  }
});

test("runSync preserves CRLF line separators and the original mtime when rewriting turn_context model", async () => {
  // Owner review regression: rewriting the per-turn model field
  // must preserve the original newline format (CRLF on Windows)
  // and the original mtime. The previous code joined lines with
  // "\n" unconditionally, which silently lost the 0x0d byte on
  // CRLF rollouts.
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"\nmodel = "gpt-5.1"\n');
  const sessionPath = path.join(codexHome, "sessions", "2026", "06", "09", "rollout-crlf.jsonl");
  await fs.mkdir(path.dirname(sessionPath), { recursive: true });

  // Build a CRLF-terminated rollout with a turn_context line.
  const meta = JSON.stringify({
    timestamp: "2026-06-09T09:16:03.878Z",
    type: "session_meta",
    payload: { id: "thread-crlf", timestamp: "2026-06-09T09:16:03.878Z", cwd: "C:\\AITemp", source: "cli", cli_version: "0.115.0", model_provider: "apigather" }
  });
  const turn = JSON.stringify({
    timestamp: "2026-06-09T09:16:03.880Z",
    type: "turn_context",
    payload: { turn_id: "t1", cwd: "C:\\AITemp", model: "gpt-5", collaboration_mode: { mode: "default", settings: { model: "gpt-5" } } }
  });
  const originalBytes = Buffer.from(`${meta}\r\n${turn}\r\n`, "utf8");
  await fs.writeFile(sessionPath, originalBytes);
  // Pin the mtime to a recognisable value so we can confirm it
  // is restored after the rewrite.
  const pinnedMtime = new Date("2026-06-01T12:00:00.000Z");
  await fs.utimes(sessionPath, pinnedMtime, pinnedMtime);

  const result = await runSync({ codexHome, model: "gpt-5.1" });
  assert.equal(result.changedSessionFiles, 1);

  const afterBytes = await fs.readFile(sessionPath);
  // The rewritten file must still contain 0x0d bytes (CRLF) —
  // the rewrite pass would have lost them if it joined with
  // plain "\n".
  assert.ok(
    afterBytes.includes(0x0d),
    "rewritten rollout must preserve the original CRLF line separators"
  );
  // The mtime must match what we pinned above.
  const afterStat = await fs.stat(sessionPath);
  assert.equal(
    afterStat.mtimeMs,
    pinnedMtime.getTime(),
    "rewritten rollout must preserve the original mtime"
  );
  // The new file should still end with a CRLF terminator (the
  // source did, and the rewrite pass re-adds it).
  assert.equal(afterBytes[afterBytes.length - 1], 0x0a);
  assert.equal(afterBytes[afterBytes.length - 2], 0x0d);
  // And the per-turn model must have moved.
  const lines = afterBytes.toString("utf8").split("\r\n").filter(Boolean);
  const turnContextLines = lines
    .map((line) => JSON.parse(line))
    .filter((entry) => entry.type === "turn_context");
  for (const entry of turnContextLines) {
    assert.equal(entry.payload.model, "gpt-5.1");
  }
});

test("runSync restores turn_context model on failure rollback (no half-completed state)", async () => {
  // Owner review regression: when the SQLite step fails after the
  // rollout rewrite has already been applied, the rollback path
  // must put the per-turn `model` field back to its original
  // value, not just the first-line session_meta. Without the
  // per-line backup in the manifest, the restore would leave
  // the rollout in a half-completed state: session_meta pointing
  // at the original provider, but per-turn model pointing at the
  // new one.
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"\nmodel = "gpt-5.1"\n');
  const sessionPath = path.join(codexHome, "sessions", "2026", "06", "09", "rollout-restore.jsonl");
  await fs.mkdir(path.dirname(sessionPath), { recursive: true });
  await writeRolloutWithTurnContext(sessionPath, {
    id: "thread-restore",
    provider: "apigather",
    model: "gpt-5"
  });

  // Pre-flight: capture the original line + the original per-turn
  // model.
  const originalContent = await fs.readFile(sessionPath, "utf8");
  const originalFirstLine = originalContent.split("\n")[0];
  const originalTurnLine = originalContent
    .split("\n")
    .filter(Boolean)
    .find((line) => line.includes('"turn_context"'));
  assert.ok(originalTurnLine, "test setup: rollout should have a turn_context line");
  const originalTurn = JSON.parse(originalTurnLine);
  assert.equal(originalTurn.payload.model, "gpt-5");

  // Stub a failing SQLite update so the runSync's try/catch
  // triggers the restore path. We do this by passing a
  // `sqliteBusyTimeoutMs` so small that the busy timeout fires
  // and the underlying sqlite update throws.
  // The simpler route: call collectSessionChanges + applySessionChanges
  // by hand, then trigger restoreBackup explicitly. We want to
  // exercise the restore path under realistic conditions.
  const configPath = path.join(codexHome, "config.toml");
  const { changes } = await collectSessionChanges(codexHome, "openai", { targetModel: "gpt-5.1" });
  const backupDir = await createBackup({
    codexHome,
    targetProvider: "openai",
    sessionChanges: changes,
    configPath
  });

  // Apply the rewrite so the rollout's per-turn model moves to
  // "gpt-5.1" and the first line gets the new provider.
  await applySessionChanges(changes, { targetModel: "gpt-5.1" });
  await updateSessionBackupManifest(backupDir, changes);

  // Debug: dump the manifest so we can see what updateSessionBackupManifest wrote.
  const sessionManifest = JSON.parse(await fs.readFile(path.join(backupDir, "session-meta-backup.json"), "utf8"));
  void sessionManifest; // suppress unused warning; the asserts below are the real check
  // Simulate Codex appending a new event to the rollout between
  // the rewrite and the rollback.
  await fs.appendFile(
    sessionPath,
    JSON.stringify({
      timestamp: "2026-06-09T09:16:03.881Z",
      type: "user_message",
      payload: { message: "after-rewrite" }
    }) + "\n",
    "utf8"
  );

  // Now roll back. The first line must return to its original
  // session_meta, the per-turn model must return to "gpt-5",
  // and the appended user_message must be left alone (the
  // restore pass is append-tolerant).
  await restoreBackup(backupDir, codexHome, { restoreDatabase: false, restoreSessions: true });

  const restored = await fs.readFile(sessionPath, "utf8");
  const restoredLines = restored.split("\n").filter(Boolean);
  assert.equal(restoredLines[0], originalFirstLine, "first line must be restored to the original session_meta");
  const restoredTurn = restoredLines
    .map((line) => JSON.parse(line))
    .find((entry) => entry.type === "turn_context");
  assert.equal(restoredTurn.payload.model, "gpt-5", "per-turn model must be restored to its original value");
  // Codex's append must be preserved.
  const appended = restoredLines
    .map((line) => JSON.parse(line))
    .find((entry) => entry.type === "user_message");
  assert.ok(appended, "appended user_message line must be preserved on rollback");
  assert.equal(appended.payload.message, "after-rewrite");
});

test("runSync leaves rollout files alone when original turn_context model already equals target", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"\nmodel = "MiniMax-M3"\n');
  const sessionPath = path.join(codexHome, "sessions", "2026", "06", "09", "rollout-a.jsonl");
  await writeRolloutWithTurnContext(sessionPath, {
    id: "thread-a",
    provider: "apigather",
    model: "MiniMax-M3"
  });

  // First sync moves provider apigather → openai and rewrites both
  // SQLite and the rollout turn_context. Second sync with no
  // arguments should be a no-op (original model already equals
  // the new target), so session files stay untouched.
  await runSync({ codexHome, model: "MiniMax-M3" });
  const firstMtime = (await fs.stat(sessionPath)).mtimeMs;
  // Touch the file system time back a bit to detect a no-op
  // (real rewrites preserve mtime, but if we touch it forward we
  // can still see whether anything wrote to the file).
  const mtimeBefore = firstMtime - 1000;
  await fs.utimes(sessionPath, new Date(mtimeBefore), new Date(mtimeBefore));

  await runSync({ codexHome });
  const finalText = await fs.readFile(sessionPath, "utf8");
  // The turn_context lines must still report MiniMax-M3 — i.e.
  // the rewrite did not corrupt them with stale gpt-5.4.
  for (const line of finalText.split("\n").filter(Boolean)) {
    if (!line.includes('"turn_context"')) continue;
    const parsed = JSON.parse(line);
    assert.equal(parsed.payload.model, "MiniMax-M3");
  }
});

test("runSync leaves turn_context model field alone when no model is provided", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"\n');
  const sessionPath = path.join(codexHome, "sessions", "2026", "06", "09", "rollout-a.jsonl");
  await writeRolloutWithTurnContext(sessionPath, {
    id: "thread-a",
    provider: "apigather",
    model: "gpt-5.4"
  });

  // No `model` arg → caller doesn't want to align the per-turn
  // model field, so even though we are rewriting `model_provider`
  // from apigather → openai, the turn_context model is left alone.
  await runSync({ codexHome });

  const lines = (await fs.readFile(sessionPath, "utf8")).split("\n").filter(Boolean);
  const turnContextLines = lines
    .map((line) => JSON.parse(line))
    .filter((entry) => entry.type === "turn_context");
  for (const entry of turnContextLines) {
    assert.equal(entry.payload.model, "gpt-5.4", "turn_context model must stay put when caller does not pass a target");
  }
});

test("status reports implicit default provider and rollout/sqlite counts", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome);
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  const archivedPath = path.join(codexHome, "archived_sessions", "2026", "03", "18", "rollout-b.jsonl");
  await writeRollout(sessionPath, "thread-a", "apigather");
  await writeRollout(archivedPath, "thread-b", "openai");
  const backupOneBytes = await writeBackup(codexHome, "20260319T000000000Z", [["note.txt", "backup-one"]]);
  const backupTwoBytes = await writeBackup(codexHome, "20260320T000000000Z", [["note.txt", "backup-two"]]);
  await writeStateDb(codexHome, [
    { id: "thread-a", model_provider: "apigather", archived: false },
    { id: "thread-b", model_provider: "openai", archived: true }
  ]);

  const status = await getStatus({ codexHome });
  assert.equal(status.currentProvider, "openai");
  assert.equal(status.currentProviderImplicit, true);
  assert.deepEqual(status.rolloutCounts.sessions, { apigather: 1 });
  assert.deepEqual(status.sqliteCounts.archived_sessions, { openai: 1 });
  assert.equal(status.stateDbLocation.source, "sqlite-dir");
  assert.equal(status.stateDbLocation.path, stateDbPath(codexHome));
  assert.equal(status.backupSummary.count, 2);
  assert.equal(status.backupSummary.totalBytes, backupOneBytes + backupTwoBytes);
  assert.match(renderStatus(status), new RegExp(`database: ${stateDbPath(codexHome).replace(/[\\^$.*+?()[\]{}|]/g, "\\$&")}`));
});

test("status reports Windows WSL UNC SQLite homes without opening the database", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');

  try {
    const status = await getStatus({
      codexHome,
      sqliteHome: "\\\\wsl.localhost\\Ubuntu\\home\\user\\.codex\\sqlite",
      platform: "win32"
    });
    const rendered = renderStatus(status);

    assert.equal(status.sqliteAccess.supported, false);
    assert.equal(status.stateDbLocation, null);
    assert.equal(status.sqliteCounts, null);
    assert.match(rendered, /Windows cannot safely access SQLite through the WSL UNC path/);
    assert.doesNotMatch(rendered, /currently in use/i);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("status falls back to legacy root sqlite database", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome);
  const sqliteDir = path.dirname(stateDbPath(codexHome));
  await fs.mkdir(sqliteDir, { recursive: true });
  const db = await openDatabase(legacyStateDbPath(codexHome));
  try {
    db.exec(`
      CREATE TABLE threads (
        id TEXT PRIMARY KEY,
        model_provider TEXT,
        archived INTEGER NOT NULL DEFAULT 0
      )
    `);
    db.prepare("INSERT INTO threads (id, model_provider, archived) VALUES (?, ?, ?)").run("legacy-thread", "openai", 0);
  } finally {
    db.close();
  }

  const status = await getStatus({ codexHome });

  assert.equal(status.stateDbLocation.source, "legacy-root");
  assert.equal(status.stateDbLocation.path, legacyStateDbPath(codexHome));
  assert.deepEqual(status.sqliteCounts.sessions, { openai: 1 });
  assert.match(renderStatus(status), /legacy root/);
});

test("status chooses legacy root sqlite database when sqlite-dir state is stale", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome);
  await writeRollout(
    path.join(codexHome, "sessions", "2026", "03", "19", "rollout-active-a.jsonl"),
    "thread-active-a",
    "openai"
  );
  await writeRollout(
    path.join(codexHome, "sessions", "2026", "03", "19", "rollout-active-b.jsonl"),
    "thread-active-b",
    "openai"
  );
  await writeRollout(
    path.join(codexHome, "archived_sessions", "2026", "03", "18", "rollout-active-c.jsonl"),
    "thread-active-c",
    "openai"
  );
  await writeStateDb(codexHome, [
    { id: "thread-active-a", model_provider: "custom", archived: false }
  ]);
  await writeLegacyStateDb(codexHome, [
    { id: "thread-active-a", model_provider: "openai", archived: false },
    { id: "thread-active-b", model_provider: "openai", archived: false },
    { id: "thread-active-c", model_provider: "openai", archived: true }
  ]);

  const status = await getStatus({ codexHome });

  assert.equal(status.stateDbLocation.source, "legacy-root");
  assert.equal(status.stateDbLocation.path, legacyStateDbPath(codexHome));
  assert.deepEqual(status.sqliteCounts.sessions, { openai: 2 });
  assert.deepEqual(status.sqliteCounts.archived_sessions, { openai: 1 });
  assert.match(renderStatus(status), /legacy root/);
});

test("status reports pending SQLite user-event and cwd repairs", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-repair-status.jsonl");
  await writeCustomRollout(sessionPath, {
    id: "thread-repair-status",
    timestamp: "2026-03-19T00:00:00.000Z",
    cwd: "E:\\GitHubProject\\lin-framework",
    source: "vscode",
    cli_version: "0.115.0",
    model_provider: "openai"
  });
  await writeStateDbWithUserEventColumn(codexHome, [
    {
      id: "thread-repair-status",
      model_provider: "openai",
      archived: false,
      has_user_event: false,
      cwd: "\\\\?\\E:\\GitHubProject\\lin-framework"
    }
  ]);

  const status = await getStatus({ codexHome });

  assert.equal(status.sqliteRepairStats.userEventRowsNeedingRepair, 1);
  assert.equal(status.sqliteRepairStats.cwdRowsNeedingRepair, 1);
  assert.match(renderStatus(status), /user-event flags needing repair: 1/);
  assert.match(renderStatus(status), /cwd paths needing repair: 1/);
});

test("status reports project visibility ranks and cwd exact-match diagnostics", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "dal"');
  await writeGlobalState(codexHome, {
    "electron-saved-workspace-roots": [
      "E:\\GitHubProject\\lin-framework"
    ]
  });

  const unrelatedRows = Array.from({ length: 51 }, (_, index) => ({
    id: `thread-other-${String(index).padStart(2, "0")}`,
    cwd: "D:\\OtherProject",
    updated_at_ms: 1000 - index
  }));
  await writeStateDbForProjectVisibility(codexHome, [
    ...unrelatedRows,
    {
      id: "thread-lin",
      cwd: "\\\\?\\E:\\GitHubProject\\lin-framework",
      updated_at_ms: 1
    }
  ]);

  const status = await getStatus({ codexHome });
  const [project] = status.projectThreadVisibility;

  assert.equal(project.root, "E:\\GitHubProject\\lin-framework");
  assert.equal(project.interactiveThreads, 1);
  assert.equal(project.firstPageThreads, 0);
  assert.deepEqual(project.ranks, [52]);
  assert.equal(project.exactCwdMatches, 0);
  assert.equal(project.verbatimCwdRows, 1);
  assert.match(renderStatus(status), /Project visibility:/);
  assert.match(renderStatus(status), /first page 0\/50, ranks 52, exact cwd 0\/1, verbatim cwd 1/);
});

test("runSwitch rejects unknown custom providers", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome);
  await assert.rejects(
    () => runSwitch({ codexHome, provider: "missing" }),
    /Provider "missing" is not available/
  );
});

test("runSwitch blocks Windows WSL UNC SQLite homes before updating config", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const configPath = path.join(codexHome, "config.toml");
  const originalConfig = await fs.readFile(configPath, "utf8");

  try {
    await assert.rejects(
      () => runSwitch({
        codexHome,
        sqliteHome: "\\\\wsl$\\Ubuntu\\home\\user\\.codex\\sqlite",
        provider: "apigather",
        platform: "win32"
      }),
      /Cannot switch.*Run codex-provider inside WSL/
    );
    assert.equal(await fs.readFile(configPath, "utf8"), originalConfig);
    await assert.rejects(() => fs.access(backupRoot(codexHome)));
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("runRestore blocks Windows WSL UNC SQLite homes before reading the backup", async () => {
  const { root, codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');

  try {
    await assert.rejects(
      () => runRestore({
        codexHome,
        sqliteHome: "\\\\wsl.localhost\\Ubuntu\\home\\user\\.codex\\sqlite",
        backupDir: path.join(root, "missing-backup"),
        platform: "win32"
      }),
      /Cannot restore.*Run codex-provider inside WSL/
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("runSync leaves rollout files and sqlite untouched when sqlite is locked", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  await writeRollout(sessionPath, "thread-a", "apigather");
  await writeStateDb(codexHome, [
    { id: "thread-a", model_provider: "apigather", archived: false }
  ]);

  const lockDb = await openDatabase(stateDbPath(codexHome));
  try {
    lockDb.exec("BEGIN IMMEDIATE");
    await assert.rejects(
      () => runSync({ codexHome, sqliteBusyTimeoutMs: 0 }),
      /state_5\.sqlite is currently in use/
    );
  } finally {
    try {
      lockDb.exec("ROLLBACK");
    } catch {
      // Ignore cleanup failures in tests.
    }
    lockDb.close();
  }

  const rollout = await fs.readFile(sessionPath, "utf8");
  assert.match(rollout, /"model_provider":"apigather"/);

  const db = await openDatabase(stateDbPath(codexHome));
  try {
    const row = db
      .prepare("SELECT model_provider FROM threads WHERE id = ?")
      .get("thread-a");
    assert.equal(row.model_provider, "apigather");
  } finally {
    db.close();
  }
});

test("runSync skips locked rollout files and still updates sqlite", async () => {
  if (process.platform !== "win32") {
    return;
  }

  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  await writeRollout(sessionPath, "thread-a", "apigather");
  await writeStateDb(codexHome, [
    { id: "thread-a", model_provider: "apigather", archived: false }
  ]);

  const lockProcess = await lockRolloutFile(sessionPath);
  let result;
  try {
    result = await runSync({ codexHome, sqliteBusyTimeoutMs: 0 });
  } finally {
    lockProcess.kill();
    await new Promise((resolve) => lockProcess.once("exit", resolve));
  }

  assert.equal(result.changedSessionFiles, 0);
  assert.equal(result.sqliteRowsUpdated, 1);
  assert.deepEqual(result.skippedLockedRolloutFiles, [sessionPath]);

  const rollout = await fs.readFile(sessionPath, "utf8");
  assert.match(rollout, /"model_provider":"apigather"/);

  const db = await openDatabase(stateDbPath(codexHome));
  try {
    const row = db
      .prepare("SELECT model_provider FROM threads WHERE id = ?")
      .get("thread-a");
    assert.equal(row.model_provider, "openai");
  } finally {
    db.close();
  }
});

test("applySessionChanges skips rollout files that changed after collection", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  await writeRollout(sessionPath, "thread-a", "openai");

  const { changes } = await collectSessionChanges(codexHome, "prov_a");
  await fs.appendFile(
    sessionPath,
    '{"timestamp":"2026-03-19T00:00:01.000Z","type":"event_msg","payload":{"type":"assistant_message","message":"later"}}\n',
    "utf8"
  );

  const result = await applySessionChanges(changes);
  assert.equal(result.appliedChanges, 0);
  assert.deepEqual(result.skippedPaths, [sessionPath]);

  const rollout = await fs.readFile(sessionPath, "utf8");
  assert.match(rollout, /"model_provider":"openai"/);
  assert.match(rollout, /"message":"later"/);
});

test("applySessionChanges preserves large UTF-8 session metadata", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-large.jsonl");
  const payload = {
    id: "thread-large",
    timestamp: "2026-03-19T00:00:00.000Z",
    cwd: "C:\\AITemp\\中文",
    source: "cli",
    cli_version: "0.115.0",
    model_provider: "apigather",
    title: "中文会话",
    note: "保留 UTF-8 内容",
    large_blob: "数据块".repeat(40000)
  };
  await writeCustomRollout(sessionPath, payload, "你好");

  const { changes } = await collectSessionChanges(codexHome, "openai");
  const result = await applySessionChanges(changes);

  assert.equal(result.appliedChanges, 1);
  assert.deepEqual(result.skippedPaths, []);

  const rollout = await fs.readFile(sessionPath, "utf8");
  assert.match(rollout, /"model_provider":"openai"/);
  assert.match(rollout, /"title":"中文会话"/);
  assert.match(rollout, /"note":"保留 UTF-8 内容"/);
  assert.match(rollout, /"message":"你好"/);
  assert.match(rollout, /"large_blob":"数据块数据块/);
});

test("applySessionChanges replaces equal-length provider IDs in place", async () => {
  const { codexHome } = await makeTempCodexHome();
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-in-place.jsonl");
  await writeRollout(sessionPath, "thread-in-place", "openai");
  await fs.appendFile(
    sessionPath,
    `${JSON.stringify({ type: "event_msg", payload: { message: "x".repeat(4 * 1024 * 1024) } })}\n`,
    "utf8"
  );
  const original = (await fs.readFile(sessionPath, "utf8"))
    .replace('"cwd":"C:\\\\AITemp"', '"cwd":"中文路径"')
    .replace('"model_provider":"openai"', '"model_provider" : "openai"');
  await fs.writeFile(sessionPath, original, "utf8");
  const originalTime = new Date("2026-01-02T03:04:05.000Z");
  await fs.utimes(sessionPath, originalTime, originalTime);

  const before = await fs.stat(sessionPath);
  const { changes } = await collectSessionChanges(codexHome, "prov_a");
  const result = await applySessionChanges(changes);
  const after = await fs.stat(sessionPath);
  const rollout = await fs.readFile(sessionPath, "utf8");

  assert.equal(result.appliedChanges, 1);
  assert.equal(result.inPlaceChanges, 1);
  if (process.platform !== "win32") {
    assert.equal(after.ino, before.ino);
  }
  assert.equal(Math.round(after.mtimeMs), originalTime.getTime());
  assert.equal(
    rollout,
    original.replace('"model_provider" : "openai"', '"model_provider" : "prov_a"')
  );
});

test("applySessionChanges falls back when equal-length provider IDs have different JSON byte lengths", async () => {
  const { codexHome } = await makeTempCodexHome();
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-escaped-provider.jsonl");
  await writeRollout(sessionPath, "thread-escaped-provider", "openai");

  const { changes } = await collectSessionChanges(codexHome, 'bad"id');
  const result = await applySessionChanges(changes);
  const [firstLine] = (await fs.readFile(sessionPath, "utf8")).split(/\r?\n/);

  assert.equal(result.appliedChanges, 1);
  assert.equal(result.inPlaceChanges, 0);
  assert.equal(JSON.parse(firstLine).payload.model_provider, 'bad"id');
});

test("applySessionChanges falls back when rollout model fields also need rewriting", async () => {
  const { codexHome } = await makeTempCodexHome();
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-provider-and-model.jsonl");
  await writeRolloutWithTurnContext(sessionPath, {
    id: "thread-provider-and-model",
    provider: "openai",
    model: "old-model"
  });

  const { changes } = await collectSessionChanges(
    codexHome,
    "prov_a",
    { targetModel: "target-model" }
  );
  const result = await applySessionChanges(changes, { targetModel: "target-model" });
  const entries = (await fs.readFile(sessionPath, "utf8"))
    .trim()
    .split(/\r?\n/)
    .map((line) => JSON.parse(line));

  assert.equal(result.appliedChanges, 1);
  assert.equal(result.inPlaceChanges, 0);
  assert.equal(entries[0].payload.model_provider, "prov_a");
  for (const entry of entries.filter((item) => item.type === "turn_context")) {
    assert.equal(entry.payload.model, "target-model");
    assert.equal(entry.payload.collaboration_mode.settings.model, "target-model");
  }
});

test("applySessionChanges restores original rollout mtime", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-mtime.jsonl");
  await writeRollout(sessionPath, "thread-mtime", "apigather");
  const originalTime = new Date("2026-01-02T03:04:05.000Z");
  await fs.utimes(sessionPath, originalTime, originalTime);

  const { changes } = await collectSessionChanges(codexHome, "openai");
  const result = await applySessionChanges(changes);

  assert.equal(result.appliedChanges, 1);
  const stat = await fs.stat(sessionPath);
  assert.equal(Math.round(stat.mtimeMs), originalTime.getTime());
});

test("collectSessionChanges reports encrypted_content counts by provider and scope", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-enc.jsonl");
  const archivedPath = path.join(codexHome, "archived_sessions", "2026", "03", "18", "rollout-enc-archived.jsonl");
  await writeRollout(sessionPath, "thread-enc", "apigather");
  await fs.appendFile(sessionPath, '{"type":"event_msg","payload":{"encrypted_content":"gAAA"}}\n', "utf8");
  await writeRollout(archivedPath, "thread-enc-archived", "openai");
  await fs.appendFile(archivedPath, '{"type":"event_msg","payload":{"encrypted_content":"gBBB"}}\n', "utf8");

  const { encryptedContentCounts } = await collectSessionChanges(codexHome, "openai");

  assert.deepEqual(encryptedContentCounts, {
    sessions: { apigather: 1 },
    archived_sessions: { openai: 1 }
  });
});

test("collectSessionChanges scans large rollout content without full-file reads", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-streamed.jsonl");
  const payload = {
    id: "thread-streamed",
    timestamp: "2026-03-19T00:00:00.000Z",
    cwd: "C:\\AITemp",
    source: "cli",
    cli_version: "0.115.0",
    model_provider: "apigather"
  };
  await fs.writeFile(
    sessionPath,
    `${JSON.stringify({ timestamp: payload.timestamp, type: "session_meta", payload })}\n`,
    "utf8"
  );

  const chunkBytes = 1024 * 1024;
  const tokenPrefix = "encrypted_";
  await fs.appendFile(
    sessionPath,
    `${"x".repeat(chunkBytes - tokenPrefix.length)}${tokenPrefix}content\n${JSON.stringify({
      type: "event_msg",
      payload: { type: "user_message", message: "after large content" }
    })}\n`,
    "utf8"
  );

  const originalReadFile = fs.readFile;
  fs.readFile = async (filePath, ...args) => {
    if (path.resolve(String(filePath)) === path.resolve(sessionPath)) {
      throw new Error("rollout scan should not read the full file");
    }
    return originalReadFile.call(fs, filePath, ...args);
  };

  try {
    const { encryptedContentCounts, userEventThreadIds } = await collectSessionChanges(codexHome, "openai");

    assert.deepEqual(encryptedContentCounts, {
      sessions: { apigather: 1 },
      archived_sessions: {}
    });
    assert.equal(userEventThreadIds.has("thread-streamed"), true);
  } finally {
    fs.readFile = originalReadFile;
  }
});

test("applySessionChanges skips only the rollout file that becomes locked on Windows", async () => {
  if (process.platform !== "win32") {
    return;
  }

  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const lockedPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-locked.jsonl");
  const writablePath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-writable.jsonl");
  await writeRollout(lockedPath, "thread-locked", "apigather");
  await writeRollout(writablePath, "thread-writable", "apigather");

  const { changes } = await collectSessionChanges(codexHome, "openai");
  const lockProcess = await lockRolloutFile(lockedPath);
  let result;
  try {
    result = await applySessionChanges(changes);
  } finally {
    lockProcess.kill();
    await new Promise((resolve) => lockProcess.once("exit", resolve));
  }

  assert.equal(result.appliedChanges, 1);
  assert.deepEqual(result.appliedPaths, [writablePath]);
  assert.deepEqual(result.skippedPaths, [lockedPath]);

  const lockedRollout = await fs.readFile(lockedPath, "utf8");
  const writableRollout = await fs.readFile(writablePath, "utf8");
  assert.match(lockedRollout, /"model_provider":"apigather"/);
  assert.match(writableRollout, /"model_provider":"openai"/);
});

test("restoreBackup only restores rollout files that were actually applied", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const configPath = path.join(codexHome, "config.toml");
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  await writeRollout(sessionPath, "thread-a", "apigather");

  const { changes } = await collectSessionChanges(codexHome, "openai");
  const backupDir = await createBackup({
    codexHome,
    targetProvider: "openai",
    sessionChanges: changes,
    configPath
  });

  await updateSessionBackupManifest(backupDir, []);
  await writeRollout(sessionPath, "thread-a", "manual");

  await restoreBackup(backupDir, codexHome, {
    restoreConfig: false,
    restoreDatabase: false,
    restoreSessions: true
  });

  const rollout = await fs.readFile(sessionPath, "utf8");
  assert.match(rollout, /"model_provider":"manual"/);
});

test("restoreBackup can skip config, database, and sessions", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const configPath = path.join(codexHome, "config.toml");
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-skip.jsonl");
  await writeRollout(sessionPath, "thread-skip", "apigather");
  await writeStateDb(codexHome, [
    { id: "thread-skip", model_provider: "apigather", archived: false }
  ]);
  const { changes } = await collectSessionChanges(codexHome, "openai");
  const backupDir = await createBackup({ codexHome, targetProvider: "openai", sessionChanges: changes, configPath });

  await writeConfig(codexHome, 'model_provider = "manual"');
  await writeRollout(sessionPath, "thread-skip", "manual");
  await restoreBackup(backupDir, codexHome, {
    restoreConfig: false,
    restoreDatabase: false,
    restoreSessions: false
  });

  assert.match(await fs.readFile(configPath, "utf8"), /^model_provider = "manual"/m);
  assert.match(await fs.readFile(sessionPath, "utf8"), /"model_provider":"manual"/);
});

test("runSync fails before rollout rewrite when SQLite is malformed", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-malformed-db.jsonl");
  await writeRollout(sessionPath, "thread-malformed", "apigather");
  await fs.mkdir(path.dirname(stateDbPath(codexHome)), { recursive: true });
  await fs.writeFile(stateDbPath(codexHome), "not sqlite", "utf8");

  await assert.rejects(
    () => runSync({ codexHome }),
    /state_5\.sqlite is malformed or unreadable/
  );
  assert.match(await fs.readFile(sessionPath, "utf8"), /"model_provider":"apigather"/);
});

test("status reports malformed SQLite without failing", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  await writeRollout(path.join(codexHome, "sessions", "2026", "03", "19", "rollout-status-db.jsonl"), "thread-status", "openai");
  await fs.mkdir(path.dirname(stateDbPath(codexHome)), { recursive: true });
  await fs.writeFile(stateDbPath(codexHome), "not sqlite", "utf8");

  const status = await getStatus({ codexHome });
  assert.equal(status.sqliteCounts.unreadable, true);
  assert.match(renderStatus(status), /state_5\.sqlite is malformed or unreadable/);
});

test("status skips locked rollout files without failing", async () => {
  if (process.platform !== "win32") {
    return;
  }

  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-status-locked.jsonl");
  await writeRollout(sessionPath, "thread-status-locked", "openai");

  const lockProcess = await lockRolloutFile(sessionPath);
  try {
    const status = await getStatus({ codexHome });
    assert.deepEqual(status.lockedRolloutFiles, [sessionPath]);
    assert.match(renderStatus(status), /Locked rollout files skipped during status scan: 1/);
  } finally {
    lockProcess.kill();
    await new Promise((resolve) => lockProcess.once("exit", resolve));
  }
});

test("pruneBackups removes the oldest backup directories", async () => {
  const { codexHome } = await makeTempCodexHome();
  const oldestBytes = await writeBackup(codexHome, "20260319T000000000Z", [
    ["note.txt", "oldest"],
    ["db/state_5.sqlite", "sqlite"]
  ]);
  await writeBackup(codexHome, "20260320T000000000Z", [["note.txt", "middle"]]);
  await writeBackup(codexHome, "20260321T000000000Z", [["note.txt", "newest"]]);

  const result = await pruneBackups(codexHome, 2);

  assert.equal(result.backupRoot, backupRoot(codexHome));
  assert.equal(result.deletedCount, 1);
  assert.equal(result.remainingCount, 2);
  assert.equal(result.freedBytes, oldestBytes);
  await assert.rejects(fs.access(path.join(backupRoot(codexHome), "20260319T000000000Z")));
  await fs.access(path.join(backupRoot(codexHome), "20260320T000000000Z"));
  await fs.access(path.join(backupRoot(codexHome), "20260321T000000000Z"));
});

test("pruneBackups ignores directories without managed backup metadata", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeBackup(codexHome, "20260320T000000000Z", [
    ["metadata.json", JSON.stringify({ namespace: "provider-sync" })]
  ]);
  const junkDirectory = path.join(backupRoot(codexHome), "manual-notes");
  await fs.mkdir(junkDirectory, { recursive: true });
  await fs.writeFile(path.join(junkDirectory, "readme.txt"), "keep me", "utf8");

  const result = await pruneBackups(codexHome, 0);

  assert.equal(result.deletedCount, 1);
  assert.equal(result.remainingCount, 0);
  await fs.access(junkDirectory);
});

test("runSync auto-prunes backups to the default retention count", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  await writeRollout(sessionPath, "thread-a", "apigather");
  await writeStateDb(codexHome, [
    { id: "thread-a", model_provider: "apigather", archived: false }
  ]);

  for (let index = 0; index < DEFAULT_BACKUP_RETENTION_COUNT; index += 1) {
    await writeBackup(codexHome, `20240101T0000${String(index).padStart(2, "0")}000Z`, [
      ["note.txt", `backup-${index}`]
    ]);
  }

  const result = await runSync({ codexHome });
  const summary = await getBackupSummary(codexHome);

  assert.equal(summary.count, DEFAULT_BACKUP_RETENTION_COUNT);
  await fs.access(result.backupDir);
  assert.equal(result.autoPruneResult.deletedCount, 1);
  assert.equal(result.autoPruneResult.remainingCount, DEFAULT_BACKUP_RETENTION_COUNT);
  assert.equal(result.autoPruneWarning, null);
});

test("runSync uses a custom automatic backup retention count", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  await writeRollout(sessionPath, "thread-a", "apigather");
  await writeStateDb(codexHome, [
    { id: "thread-a", model_provider: "apigather", archived: false }
  ]);

  for (let index = 0; index < 4; index += 1) {
    await writeBackup(codexHome, `20240101T0000${String(index).padStart(2, "0")}000Z`, [
      ["note.txt", `backup-${index}`]
    ]);
  }

  const result = await runSync({ codexHome, keepCount: 2 });
  const summary = await getBackupSummary(codexHome);

  assert.equal(summary.count, 2);
  await fs.access(result.backupDir);
  assert.equal(result.autoPruneResult.deletedCount, 3);
  assert.equal(result.autoPruneResult.remainingCount, 2);
  assert.equal(result.autoPruneWarning, null);
});

test("cli rejects non-integer keep values", async () => {
  const result = await runCli(["prune-backups", "--keep", "1.5"]);
  assert.equal(result.code, 1);
  assert.match(result.stderr, /Invalid --keep value: 1\.5/);
});

test("node version guard allows Node 16 and rejects older releases", () => {
  assert.equal(getUnsupportedNodeVersionMessage("16.0.0"), null);
  assert.match(
    getUnsupportedNodeVersionMessage("14.21.3"),
    /requires Node\.js 16\+/
  );
});

test("cli sync prints stage progress and backup timing", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  await writeRollout(sessionPath, "thread-a", "apigather");
  await writeStateDb(codexHome, [
    { id: "thread-a", model_provider: "apigather", archived: false }
  ]);

  const result = await runCli(["sync", "--codex-home", codexHome]);
  assert.equal(result.code, 0);
  assert.match(result.stdout, /\[1\/6\] Scanning rollout files\.\.\./);
  assert.match(result.stdout, /\[2\/6\] Checking locked rollout files\.\.\./);
  assert.match(result.stdout, /\[3\/6\] Creating backup\.\.\./);
  assert.match(result.stdout, /\[4\/6\] Updating SQLite\.\.\./);
  assert.match(result.stdout, /\[5\/6\] Rewriting rollout files\.\.\./);
  assert.match(result.stdout, /\[6\/6\] Cleaning backups\.\.\./);
  assert.match(result.stdout, /Backup created in .*: .+/);
  assert.match(result.stdout, /Backup creation time: /);
});
