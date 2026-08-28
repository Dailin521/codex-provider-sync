import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

import { createCoreFacade } from "@codex-provider-sync/core";

import { defaultBackupRoot } from "../src/constants.js";
import {
  readTransactionJournal,
  TRANSACTION_JOURNAL_BASENAME
} from "../src/transaction-journal.js";

function digest(value) {
  return createHash("sha256").update(value).digest("hex");
}

async function fileDigest(filePath) {
  try {
    return digest(await fs.readFile(filePath));
  } catch (error) {
    if (error?.code === "ENOENT") return null;
    throw error;
  }
}

async function treeDigest(root) {
  const entries = [];
  async function visit(directory) {
    const children = await fs.readdir(directory, { withFileTypes: true });
    children.sort((left, right) => left.name.localeCompare(right.name));
    if (children.length === 0) {
      entries.push([`${path.relative(root, directory).replaceAll("\\", "/")}/`, "directory"]);
    }
    for (const child of children) {
      const absolute = path.join(directory, child.name);
      const relative = path.relative(root, absolute).replaceAll("\\", "/");
      if (child.isDirectory()) await visit(absolute);
      else if (child.isFile()) entries.push([relative, digest(await fs.readFile(absolute))]);
      else throw new Error(`Unsupported desktop fixture entry: ${relative}`);
    }
  }
  try {
    await visit(root);
    return entries;
  } catch (error) {
    if (error?.code === "ENOENT") return null;
    throw error;
  }
}

function plainRows(rows) {
  return rows.map((row) => Object.fromEntries(Object.entries(row)));
}

function sqliteCanonicalState(stateDbPath) {
  const database = new DatabaseSync(stateDbPath, { readOnly: true });
  try {
    return {
      userVersion: Number(database.prepare("PRAGMA user_version").get().user_version),
      schema: plainRows(database.prepare(`
        SELECT type, name, tbl_name AS tableName, sql
        FROM sqlite_schema
        WHERE name NOT LIKE 'sqlite_%'
        ORDER BY type, name
      `).all()),
      threads: plainRows(database.prepare(`
        SELECT id, model_provider, cwd, archived, first_user_message, model,
               has_user_event, updated_at, updated_at_ms
        FROM threads
        ORDER BY id
      `).all())
    };
  } finally {
    database.close();
  }
}

async function sqliteFileDigests(stateDbPath) {
  const basename = path.basename(stateDbPath);
  return Promise.all(["", "-wal", "-shm", "-journal"].map(async (suffix) => [
    `${basename}${suffix}`,
    await fileDigest(`${stateDbPath}${suffix}`)
  ]));
}

function createSyntheticStateDatabase(stateDbPath, provider, model) {
  const database = new DatabaseSync(stateDbPath);
  try {
    database.exec(`
      CREATE TABLE threads (
        id TEXT PRIMARY KEY,
        model_provider TEXT,
        cwd TEXT NOT NULL DEFAULT '',
        archived INTEGER NOT NULL DEFAULT 0,
        first_user_message TEXT NOT NULL DEFAULT '',
        model TEXT,
        has_user_event INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL DEFAULT 0
      );
    `);
    database.prepare(`
      INSERT INTO threads (
        id, model_provider, cwd, archived, first_user_message, model,
        has_user_event, updated_at, updated_at_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      "c7-desktop-session",
      provider,
      "C:\\synthetic\\desktop-project",
      0,
      "",
      model,
      1,
      1787702400,
      1787702400000
    );
  } finally {
    database.close();
  }
}

function readSessionMeta(text) {
  for (const line of text.split(/\r?\n/)) {
    if (!line.trim()) continue;
    const entry = JSON.parse(line);
    if (entry?.type === "session_meta") return entry.payload;
  }
  throw new Error("Writable desktop fixture has no session_meta entry.");
}

function readTurnContext(text) {
  for (const line of text.split(/\r?\n/)) {
    if (!line.trim()) continue;
    const entry = JSON.parse(line);
    if (entry?.type === "turn_context") return entry.payload;
  }
  throw new Error("Writable desktop fixture has no turn_context entry.");
}

export async function createDesktopSyncSwitchFixture() {
  const fixtureRoot = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-c7-desktop-"));
  const codexHome = path.join(fixtureRoot, "codex-home");
  const userData = path.join(fixtureRoot, "user-data");
  const rolloutPath = path.join(
    codexHome,
    "sessions",
    "2026",
    "08",
    "26",
    "rollout-c7-desktop.jsonl"
  );
  const stateDbPath = path.join(codexHome, "sqlite", "state_5.sqlite");
  const targetSqliteHome = path.join(fixtureRoot, "relocation-target-sqlite");
  const targetStateDbPath = path.join(targetSqliteHome, "state_5.sqlite");
  const configPath = path.join(codexHome, "config.toml");
  const globalStatePath = path.join(codexHome, ".codex-global-state.json");
  const globalStateBackupPath = `${globalStatePath}.bak`;
  const gateMarkerPath = path.join(fixtureRoot, "runtime-gate.json");
  const coreProfile = { profileId: "c7-desktop-fixture", profileRevision: "fixture-r1" };
  const core = createCoreFacade({
    resolveProfile: async (selector) => {
      if (selector.profileId !== coreProfile.profileId
          || (selector.profileRevision !== undefined
            && selector.profileRevision !== coreProfile.profileRevision)) {
        throw new Error("Unexpected C7 desktop fixture profile selector.");
      }
      return {
        id: coreProfile.profileId,
        revision: coreProfile.profileRevision,
        codexHome
      };
    }
  });
  await fs.mkdir(path.dirname(rolloutPath), { recursive: true });
  await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
  await fs.mkdir(path.dirname(stateDbPath), { recursive: true });
  await fs.mkdir(targetSqliteHome, { recursive: true });
  await fs.mkdir(userData, { recursive: true });
  await fs.writeFile(path.join(userData, "profiles.v1.json"), `${JSON.stringify({
    schemaVersion: 1,
    profiles: [
      {
        id: "relocation-target",
        name: "Relocation target",
        codexHome,
        sqliteHome: targetSqliteHome
      },
      {
        id: "no-sqlite-target",
        name: "No SQLite target",
        codexHome
      }
    ]
  }, null, 2)}\n`, "utf8");
  await fs.writeFile(
    configPath,
    [
      'model_provider = "openai"',
      'model = "gpt-5"',
      "",
      "[model_providers.relay]",
      'model = "relay-model"',
      'base_url = "https://relay.invalid"',
      ""
    ].join("\n"),
    "utf8"
  );
  const globalState = `${JSON.stringify({
    "electron-saved-workspace-roots": ["C:\\synthetic\\previous-project"],
    "project-order": ["C:\\synthetic\\previous-project"]
  }, null, 2)}\n`;
  await fs.writeFile(globalStatePath, globalState, "utf8");
  await fs.writeFile(globalStateBackupPath, globalState, "utf8");
  await fs.writeFile(rolloutPath, `${[
    {
      type: "session_meta",
      timestamp: "2026-08-26T00:00:00.000Z",
      payload: {
        id: "c7-desktop-session",
        cwd: "C:\\synthetic\\desktop-project",
        model_provider: "legacy-provider",
        model: "legacy-model"
      }
    },
    {
      type: "turn_context",
      timestamp: "2026-08-26T00:00:30.000Z",
      payload: {
        model: "legacy-model",
        collaboration_mode: { settings: { model: "legacy-model" } }
      }
    },
    {
      type: "event_msg",
      timestamp: "2026-08-26T00:01:00.000Z",
      payload: { type: "user_message", message: "C7_DESKTOP_BODY_ONLY_MARKER" }
    }
  ].map((entry) => JSON.stringify(entry)).join("\n")}\n`, "utf8");
  createSyntheticStateDatabase(stateDbPath, "legacy-provider", "legacy-model");
  createSyntheticStateDatabase(targetStateDbPath, "target-before", "target-model");
  let closed = false;
  return {
    fixtureRoot,
    codexHome,
    userData,
    rolloutPath,
    stateDbPath,
    targetSqliteHome,
    targetStateDbPath,
    configPath,
    gateMarkerPath,
    async snapshotSqlite(databasePath = stateDbPath) {
      const value = sqliteCanonicalState(databasePath);
      return { ...value, hash: digest(JSON.stringify(value)) };
    },
    async snapshotTargets() {
      const value = {
        config: await fileDigest(configPath),
        globalState: await fileDigest(globalStatePath),
        globalStateBackup: await fileDigest(globalStateBackupPath),
        sessions: await treeDigest(path.join(codexHome, "sessions")),
        archivedSessions: await treeDigest(path.join(codexHome, "archived_sessions")),
        // SQLite online backup/restore may produce a byte-different but fully
        // equivalent database. Compare the complete synthetic schema and rows
        // here; byte-preservation checks below still hash the actual DB files.
        sqlite: sqliteCanonicalState(stateDbPath)
      };
      return { ...value, hash: digest(JSON.stringify(value)) };
    },
    async snapshotProtected() {
      const value = {
        config: await fileDigest(configPath),
        globalState: await fileDigest(globalStatePath),
        globalStateBackup: await fileDigest(globalStateBackupPath),
        sessions: await treeDigest(path.join(codexHome, "sessions")),
        archivedSessions: await treeDigest(path.join(codexHome, "archived_sessions")),
        // Resource-lock directories are coordination artifacts, not protected
        // business state. Hash the real SQLite files and exclude those locks.
        sqlite: await sqliteFileDigests(stateDbPath),
        backups: await treeDigest(defaultBackupRoot(codexHome))
      };
      return { ...value, hash: digest(JSON.stringify(value)) };
    },
    async appendConfigDrift() {
      await fs.appendFile(configPath, "# C7 deterministic plan drift\n", "utf8");
    },
    holdSqliteWriteLock() {
      const database = new DatabaseSync(stateDbPath);
      database.exec("BEGIN IMMEDIATE");
      let released = false;
      return {
        release() {
          if (released) return;
          released = true;
          try { database.exec("ROLLBACK"); } finally { database.close(); }
        }
      };
    },
    async readJournals() {
      const result = [];
      for (const backupId of (await this.inspect()).backupIds) {
        const journalPath = path.join(defaultBackupRoot(codexHome), backupId, TRANSACTION_JOURNAL_BASENAME);
        try {
          const journal = await readTransactionJournal(journalPath);
          result.push({ backupId, state: journal.state, terminal: journal.terminal, invalidTail: journal.invalidTail });
        } catch (error) {
          if (error?.code !== "ENOENT") throw error;
        }
      }
      return result;
    },
    async restoreManagedBackup(backupId) {
      const plan = await core.prepareRestore({
        profile: coreProfile,
        backupId,
        restoreConfig: true,
        restoreDatabase: true,
        restoreSessions: true
      });
      return core.applyRestore({ schemaVersion: 1, planId: plan.planId });
    },
    async inspect() {
      const configText = await fs.readFile(configPath, "utf8");
      const rolloutText = await fs.readFile(rolloutPath, "utf8");
      const rollout = readSessionMeta(rolloutText);
      const turnContext = readTurnContext(rolloutText);
      const db = new DatabaseSync(stateDbPath, { readOnly: true });
      let sqlite;
      try {
        sqlite = db.prepare(
          "SELECT model_provider AS provider, model, updated_at AS updatedAt, updated_at_ms AS updatedAtMs FROM threads WHERE id = ?"
        ).get("c7-desktop-session");
      } finally {
        db.close();
      }
      let backupIds = [];
      try {
        backupIds = (await fs.readdir(defaultBackupRoot(codexHome), { withFileTypes: true }))
          .filter((entry) => entry.isDirectory())
          .map((entry) => entry.name)
          .sort();
      } catch (error) {
        if (error?.code !== "ENOENT") throw error;
      }
      return { configText, rollout, turnContext, sqlite, backupIds };
    },
    async close() {
      if (closed) return;
      closed = true;
      await fs.rm(fixtureRoot, {
        recursive: true,
        force: true,
        maxRetries: 20,
        retryDelay: 100
      });
    }
  };
}
