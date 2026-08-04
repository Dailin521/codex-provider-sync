import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { createBackup, restoreBackup } from "../src/backup.js";
import { DB_FILE_BASENAME, SQLITE_DIR_BASENAME } from "../src/constants.js";
import { openDatabase } from "../src/sqlite.js";
import {
  configureSqliteWriteDurability,
  createSqliteOnlineBackup,
  updateSqliteProvider
} from "../src/sqlite-state.js";

async function tempDatabase(prefix) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  const dbPath = path.join(root, "state_5.sqlite");
  return {
    root,
    dbPath,
    location: { path: dbPath, source: "explicit" }
  };
}

test("SQLite writes configure synchronous=FULL and Node/.NET update counters agree", async () => {
  const fixture = await tempDatabase("provider-sync-sqlite-durability-");
  const db = await openDatabase(fixture.dbPath);
  try {
    db.exec(`
      PRAGMA synchronous = OFF;
      CREATE TABLE threads (
        id TEXT PRIMARY KEY,
        model_provider TEXT,
        model TEXT
      );
      INSERT INTO threads VALUES ('a', 'legacy', 'old');
      INSERT INTO threads VALUES ('b', 'openai', 'old');
    `);
    assert.equal(Number(db.prepare("PRAGMA synchronous").get().synchronous), 0);
    assert.deepEqual(configureSqliteWriteDurability(db), { synchronous: "full", value: 2 });
    assert.equal(Number(db.prepare("PRAGMA synchronous").get().synchronous), 2);
  } finally {
    db.close();
  }

  const result = await updateSqliteProvider(fixture.location, "openai", {
    targetModel: "new"
  });
  assert.deepEqual(
    {
      updatedRows: result.updatedRows,
      providerRowsUpdated: result.providerRowsUpdated,
      modelRowsUpdated: result.modelRowsUpdated
    },
    { updatedRows: 3, providerRowsUpdated: 1, modelRowsUpdated: 2 }
  );

  const verified = await openDatabase(fixture.dbPath, { readOnly: true });
  try {
    assert.deepEqual(
      verified.prepare("SELECT model_provider, model FROM threads ORDER BY id").all()
        .map((row) => ({ model_provider: row.model_provider, model: row.model })),
      [
        { model_provider: "openai", model: "new" },
        { model_provider: "openai", model: "new" }
      ]
    );
  } finally {
    verified.close();
    await fs.rm(fixture.root, { recursive: true, force: true });
  }
});

test("official SQLite online backup captures live WAL into one standalone main file", async () => {
  const fixture = await tempDatabase("provider-sync-sqlite-online-backup-");
  const backupPath = path.join(fixture.root, "backup", "state_5.sqlite");
  const source = await openDatabase(fixture.dbPath);
  try {
    source.exec("PRAGMA page_size = 8192");
    source.exec("VACUUM");
    assert.equal(source.prepare("PRAGMA journal_mode = WAL").get().journal_mode, "wal");
    source.exec("PRAGMA user_version = 73");
    source.exec("PRAGMA application_id = 1129333840");
    source.exec("CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT)");
    source.prepare("INSERT INTO threads VALUES (?, ?)").run("wal-row", "openai");
    assert.ok((await fs.stat(`${fixture.dbPath}-wal`)).size > 0);

    const result = await createSqliteOnlineBackup(fixture.location, backupPath);
    assert.equal(result.databasePresent, true);
    assert.ok(["node:sqlite", "better-sqlite3"].includes(result.driver));
    assert.deepEqual(result.metadata.source, {
      journalMode: "wal",
      pageSize: 8192,
      userVersion: 73,
      applicationId: 1129333840
    });
    assert.deepEqual(result.metadata.backup, result.metadata.source);
    assert.deepEqual(result.metadata.preserved, {
      journalMode: true,
      pageSize: true,
      userVersion: true,
      applicationId: true
    });
    assert.equal(await fs.stat(backupPath).then((stat) => stat.isFile()), true);
    await assert.rejects(fs.access(`${backupPath}-wal`), { code: "ENOENT" });
    await assert.rejects(fs.access(`${backupPath}-shm`), { code: "ENOENT" });
  } finally {
    source.close();
  }

  const backup = await openDatabase(backupPath);
  try {
    assert.equal(
      backup.prepare("SELECT model_provider FROM threads WHERE id = ?").get("wal-row").model_provider,
      "openai"
    );
  } finally {
    backup.close();
    await fs.rm(fixture.root, { recursive: true, force: true });
  }
});

test("online backup never recreates a source database that disappeared", async () => {
  const fixture = await tempDatabase("provider-sync-sqlite-online-missing-source-");
  const backupPath = path.join(fixture.root, "backup", "state_5.sqlite");
  const source = await openDatabase(fixture.dbPath);
  source.exec("CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT)");
  source.close();
  await fs.rm(fixture.dbPath);

  try {
    await assert.rejects(
      () => createSqliteOnlineBackup(fixture.location, backupPath),
      /unable to open database|does not exist|cannot open/i
    );
    await assert.rejects(fs.access(fixture.dbPath), { code: "ENOENT" });
    await assert.rejects(fs.access(backupPath), { code: "ENOENT" });
  } finally {
    await fs.rm(fixture.root, { recursive: true, force: true });
  }
});

test("managed backup snapshots live WAL and manifests only standalone main databases", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-managed-online-backup-"));
  const codexHome = path.join(root, ".codex");
  const sqliteHome = path.join(codexHome, SQLITE_DIR_BASENAME);
  const dbPath = path.join(sqliteHome, DB_FILE_BASENAME);
  const configPath = path.join(codexHome, "config.toml");
  await fs.mkdir(sqliteHome, { recursive: true });
  await fs.writeFile(configPath, 'model_provider = "openai"\n', "utf8");

  const source = await openDatabase(dbPath);
  try {
    assert.equal(source.prepare("PRAGMA journal_mode = WAL").get().journal_mode, "wal");
    source.exec(`
      CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT);
      INSERT INTO threads VALUES ('live-wal-row', 'apigather');
    `);
    assert.ok((await fs.stat(`${dbPath}-wal`)).size > 0);

    const backupDir = await createBackup({
      codexHome,
      targetProvider: "openai",
      sessionChanges: [],
      configPath
    });
    const metadata = JSON.parse(await fs.readFile(path.join(backupDir, "metadata.json"), "utf8"));
    assert.deepEqual(metadata.sqliteDbFiles, [DB_FILE_BASENAME]);
    assert.deepEqual(
      metadata.dbFiles.map((fileName) => fileName.replaceAll("\\", "/")),
      [`${SQLITE_DIR_BASENAME}/${DB_FILE_BASENAME}`]
    );

    const canonicalBackupPath = path.join(backupDir, "db", "sqlite-home", DB_FILE_BASENAME);
    const legacyMirrorPath = path.join(
      backupDir,
      "db",
      SQLITE_DIR_BASENAME,
      DB_FILE_BASENAME
    );
    for (const backupPath of [canonicalBackupPath, legacyMirrorPath]) {
      assert.equal((await fs.stat(backupPath)).isFile(), true);
      await assert.rejects(fs.access(`${backupPath}-wal`), { code: "ENOENT" });
      await assert.rejects(fs.access(`${backupPath}-shm`), { code: "ENOENT" });
    }

    const backup = await openDatabase(canonicalBackupPath, { readOnly: true });
    try {
      assert.equal(
        backup.prepare("SELECT model_provider FROM threads WHERE id = ?")
          .get("live-wal-row").model_provider,
        "apigather"
      );
    } finally {
      backup.close();
    }

    source.prepare("UPDATE threads SET model_provider = ? WHERE id = ?")
      .run("live-after-backup", "live-wal-row");
    source.prepare("INSERT INTO threads VALUES (?, ?)")
      .run("live-only-row", "live-after-backup");
    assert.ok((await fs.stat(`${dbPath}-wal`)).size > 0);

    await restoreBackup(backupDir, codexHome, {
      restoreConfig: false,
      restoreGlobalState: false,
      restoreSessions: false
    });
    assert.equal(
      source.prepare("SELECT model_provider FROM threads WHERE id = ?")
        .get("live-wal-row").model_provider,
      "apigather"
    );
    assert.equal(
      source.prepare("SELECT model_provider FROM threads WHERE id = ?")
        .get("live-only-row"),
      undefined
    );

    source.prepare("UPDATE threads SET model_provider = ? WHERE id = ?")
      .run("live-before-failed-restore", "live-wal-row");
    source.prepare("INSERT INTO threads VALUES (?, ?)")
      .run("failed-restore-must-preserve", "live-before-failed-restore");
    const walBeforeFailure = await fs.readFile(`${dbPath}-wal`);
    await fs.writeFile(canonicalBackupPath, "not a sqlite database", "utf8");

    await assert.rejects(
      () => restoreBackup(backupDir, codexHome, {
        restoreConfig: false,
        restoreGlobalState: false,
        restoreSessions: false
      }),
      /malformed|not a database/i
    );
    assert.deepEqual(await fs.readFile(`${dbPath}-wal`), walBeforeFailure);
    assert.equal(
      source.prepare("SELECT model_provider FROM threads WHERE id = ?")
        .get("live-wal-row").model_provider,
      "live-before-failed-restore"
    );
    assert.equal(
      source.prepare("SELECT model_provider FROM threads WHERE id = ?")
        .get("failed-restore-must-preserve").model_provider,
      "live-before-failed-restore"
    );
  } finally {
    source.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});
