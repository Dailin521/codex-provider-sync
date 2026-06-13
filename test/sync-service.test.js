import { spawn } from "node:child_process";
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

import {
  createBackup,
  getBackupSummary,
  pruneBackups,
  restoreBackup,
  updateSessionBackupManifest
} from "../src/backup.js";
import { getStatus, renderStatus, runRestore, runSwitch, runSync } from "../src/service.js";
import { DEFAULT_BACKUP_RETENTION_COUNT } from "../src/constants.js";
import { getUnsupportedNodeVersionMessage } from "../src/node-version.js";
import { applySessionChanges, collectSessionChanges } from "../src/session-files.js";

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

async function writeStateDb(codexHome, rows) {
  await writeStateDbAtPath(path.join(codexHome, "state_5.sqlite"), rows);
}

async function writeStateDbAtPath(dbPath, rows) {
  await fs.mkdir(path.dirname(dbPath), { recursive: true });
  const db = new DatabaseSync(dbPath);
  try {
    db.exec(`
      CREATE TABLE threads (
        id TEXT PRIMARY KEY,
        model_provider TEXT,
        cwd TEXT NOT NULL DEFAULT '',
        archived INTEGER NOT NULL DEFAULT 0,
        first_user_message TEXT NOT NULL DEFAULT ''
      )
    `);
    const stmt = db.prepare("INSERT INTO threads (id, model_provider, cwd, archived, first_user_message) VALUES (?, ?, ?, ?, ?)");
    for (const row of rows) {
      stmt.run(row.id, row.model_provider, row.cwd ?? "C:\\AITemp", row.archived ? 1 : 0, row.first_user_message ?? "hello");
    }
  } finally {
    db.close();
  }
}

async function writeStateDbWithUserEventColumn(codexHome, rows) {
  const dbPath = path.join(codexHome, "state_5.sqlite");
  const db = new DatabaseSync(dbPath);
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
  const dbPath = path.join(codexHome, "state_5.sqlite");
  const db = new DatabaseSync(dbPath);
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

  const syncedSession = await fs.readFile(sessionPath, "utf8");
  const syncedArchived = await fs.readFile(archivedPath, "utf8");
  assert.match(syncedSession, /"model_provider":"openai"/);
  assert.match(syncedArchived, /"model_provider":"openai"/);

  const db = new DatabaseSync(path.join(codexHome, "state_5.sqlite"));
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

  const db = new DatabaseSync(path.join(codexHome, "state_5.sqlite"));
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

  const db = new DatabaseSync(path.join(codexHome, "state_5.sqlite"));
  try {
    const row = db
      .prepare("SELECT cwd FROM threads WHERE id = ?")
      .get("thread-cwd");
    assert.equal(row.cwd, "D:\\GitHubProject\\oss-maintainer-hub");
  } finally {
    db.close();
  }
});

test("runSync updates nested sqlite state used by newer Codex Desktop", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "custom"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-nested-db.jsonl");
  await writeCustomRollout(sessionPath, {
    id: "thread-nested-db",
    timestamp: "2026-03-19T00:00:00.000Z",
    cwd: "C:\\Users\\me\\Desktop\\project-mentor",
    source: "vscode",
    cli_version: "0.115.0",
    model_provider: "custom"
  });
  const nestedDbPath = path.join(codexHome, "sqlite", "state_5.sqlite");
  await writeStateDbAtPath(nestedDbPath, [
    {
      id: "thread-nested-db",
      model_provider: "openai",
      archived: false,
      cwd: "\\\\?\\C:\\Users\\me\\Desktop\\project-mentor"
    }
  ]);

  const syncResult = await runSync({ codexHome });

  assert.equal(syncResult.changedSessionFiles, 0);
  assert.equal(syncResult.sqliteProviderRowsUpdated, 1);
  assert.equal(syncResult.sqliteCwdRowsUpdated, 1);
  assert.equal(syncResult.sqliteRowsUpdated, 2);

  const db = new DatabaseSync(nestedDbPath);
  try {
    const row = db
      .prepare("SELECT model_provider, cwd FROM threads WHERE id = ?")
      .get("thread-nested-db");
    assert.deepEqual({ ...row }, {
      model_provider: "custom",
      cwd: "C:\\Users\\me\\Desktop\\project-mentor"
    });
  } finally {
    db.close();
  }

  await fs.access(path.join(syncResult.backupDir, "db", "sqlite", "state_5.sqlite"));
  const metadataPath = path.join(syncResult.backupDir, "metadata.json");
  const metadata = JSON.parse(await fs.readFile(metadataPath, "utf8"));
  assert.ok(metadata.dbFiles.includes("sqlite/state_5.sqlite"));
  assert.equal(metadata.dbFiles.some((fileName) => fileName.includes("\\")), false);
  metadata.dbFiles = metadata.dbFiles.map((fileName) =>
    fileName.replace("sqlite/state_5.sqlite", "sqlite\\state_5.sqlite")
  );
  await fs.writeFile(metadataPath, JSON.stringify(metadata, null, 2), "utf8");

  await runRestore({ codexHome, backupDir: syncResult.backupDir });

  const restoredDb = new DatabaseSync(nestedDbPath);
  try {
    const row = restoredDb
      .prepare("SELECT model_provider, cwd FROM threads WHERE id = ?")
      .get("thread-nested-db");
    assert.deepEqual({ ...row }, {
      model_provider: "openai",
      cwd: "\\\\?\\C:\\Users\\me\\Desktop\\project-mentor"
    });
  } finally {
    restoredDb.close();
  }
});

test("runSync updates both root and nested sqlite state files when both exist", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "custom"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-dual-db.jsonl");
  await writeRollout(sessionPath, "thread-dual-db", "custom");
  await writeStateDb(codexHome, [
    { id: "thread-root", model_provider: "openai", archived: false }
  ]);
  const nestedDbPath = path.join(codexHome, "sqlite", "state_5.sqlite");
  await writeStateDbAtPath(nestedDbPath, [
    { id: "thread-nested", model_provider: "openai", archived: false }
  ]);

  const syncResult = await runSync({ codexHome });

  assert.equal(syncResult.sqliteProviderRowsUpdated, 2);
  assert.equal(syncResult.sqlitePresent, true);

  for (const dbPath of [path.join(codexHome, "state_5.sqlite"), nestedDbPath]) {
    const db = new DatabaseSync(dbPath);
    try {
      const providers = db
        .prepare("SELECT DISTINCT model_provider FROM threads")
        .all()
        .map((row) => row.model_provider);
      assert.deepEqual(providers, ["custom"]);
    } finally {
      db.close();
    }
  }

  await fs.access(path.join(syncResult.backupDir, "db", "state_5.sqlite"));
  await fs.access(path.join(syncResult.backupDir, "db", "sqlite", "state_5.sqlite"));
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

  const db = new DatabaseSync(path.join(codexHome, "state_5.sqlite"));
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
  assert.equal(status.backupSummary.count, 2);
  assert.equal(status.backupSummary.totalBytes, backupOneBytes + backupTwoBytes);
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

test("runSync leaves rollout files and sqlite untouched when sqlite is locked", async () => {
  const { codexHome } = await makeTempCodexHome();
  await writeConfig(codexHome, 'model_provider = "openai"');
  const sessionPath = path.join(codexHome, "sessions", "2026", "03", "19", "rollout-a.jsonl");
  await writeRollout(sessionPath, "thread-a", "apigather");
  await writeStateDb(codexHome, [
    { id: "thread-a", model_provider: "apigather", archived: false }
  ]);

  const lockDb = new DatabaseSync(path.join(codexHome, "state_5.sqlite"));
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

  const db = new DatabaseSync(path.join(codexHome, "state_5.sqlite"));
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

  const db = new DatabaseSync(path.join(codexHome, "state_5.sqlite"));
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
  await writeRollout(sessionPath, "thread-a", "apigather");

  const { changes } = await collectSessionChanges(codexHome, "openai");
  await fs.appendFile(
    sessionPath,
    '{"timestamp":"2026-03-19T00:00:01.000Z","type":"event_msg","payload":{"type":"assistant_message","message":"later"}}\n',
    "utf8"
  );

  const result = await applySessionChanges(changes);
  assert.equal(result.appliedChanges, 0);
  assert.deepEqual(result.skippedPaths, [sessionPath]);

  const rollout = await fs.readFile(sessionPath, "utf8");
  assert.match(rollout, /"model_provider":"apigather"/);
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

test("collectSessionChanges scans large rollout content without full-file reads", async (t) => {
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
  t.mock.method(fs, "readFile", async (filePath, ...args) => {
    if (path.resolve(String(filePath)) === path.resolve(sessionPath)) {
      throw new Error("rollout scan should not read the full file");
    }
    return originalReadFile.call(fs, filePath, ...args);
  });

  const { encryptedContentCounts, userEventThreadIds } = await collectSessionChanges(codexHome, "openai");

  assert.deepEqual(encryptedContentCounts, {
    sessions: { apigather: 1 },
    archived_sessions: {}
  });
  assert.equal(userEventThreadIds.has("thread-streamed"), true);
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
  await fs.writeFile(path.join(codexHome, "state_5.sqlite"), "not sqlite", "utf8");

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
  await fs.writeFile(path.join(codexHome, "state_5.sqlite"), "not sqlite", "utf8");

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

test("node version guard explains node:sqlite requirement", () => {
  assert.equal(getUnsupportedNodeVersionMessage("24.0.0"), null);
  assert.match(
    getUnsupportedNodeVersionMessage("20.18.1"),
    /requires Node\.js 24\+ because it uses node:sqlite/
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
