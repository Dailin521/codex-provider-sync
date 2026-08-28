import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

import { defaultBackupRoot } from "../src/constants.js";
import { TransactionJournal } from "../src/transaction-journal.js";

async function hashTree(root) {
  const result = {};
  async function visit(directory) {
    const entries = await fs.readdir(directory, { withFileTypes: true });
    entries.sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      const absolute = path.join(directory, entry.name);
      const relative = path.relative(root, absolute).replaceAll("\\", "/");
      if (entry.isDirectory()) await visit(absolute);
      else if (entry.isFile()) {
        result[relative] = createHash("sha256").update(await fs.readFile(absolute)).digest("hex");
      } else {
        throw new Error(`Desktop fixture contains an unsupported entry: ${relative}`);
      }
    }
  }
  await visit(root);
  return result;
}

export async function createDesktopReadOnlyFixture() {
  const fixtureRoot = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-c6-desktop-"));
  const codexHome = path.join(fixtureRoot, "codex-home");
  const userData = path.join(fixtureRoot, "user-data");
  const rolloutPath = path.join(
    codexHome,
    "sessions",
    "2026",
    "08",
    "26",
    "rollout-c6-desktop.jsonl"
  );
  await fs.mkdir(path.dirname(rolloutPath), { recursive: true });
  await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
  await fs.mkdir(path.join(codexHome, "sqlite"), { recursive: true });
  await fs.mkdir(userData, { recursive: true });
  await fs.writeFile(
    path.join(codexHome, "config.toml"),
    'model_provider = "openai"\nmodel = "gpt-5"\n',
    "utf8"
  );
  await fs.writeFile(rolloutPath, `${[
    {
      type: "session_meta",
      timestamp: "2026-08-26T00:00:00.000Z",
      payload: {
        id: "c6-desktop-session",
        cwd: "C:\\synthetic\\desktop-project",
        model_provider: "openai",
        model: "gpt-5"
      }
    },
    {
      type: "event_msg",
      timestamp: "2026-08-26T00:01:00.000Z",
      payload: { type: "user_message", message: "C6_DESKTOP_BODY_ONLY_MARKER" }
    },
    {
      type: "event_msg",
      timestamp: "2026-08-26T00:02:00.000Z",
      payload: { type: "assistant_message", message: "Synthetic desktop response." }
    }
  ].map((entry) => JSON.stringify(entry)).join("\n")}\n`, "utf8");
  const database = new DatabaseSync(path.join(codexHome, "sqlite", "state_5.sqlite"));
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
      "c6-desktop-session",
      "openai",
      "C:\\synthetic\\desktop-project",
      0,
      "",
      "gpt-5",
      1,
      1787702400,
      1787702400000
    );
  } finally {
    database.close();
  }
  const pendingBackupDir = path.join(defaultBackupRoot(codexHome), "c6-pending-journal");
  await fs.mkdir(pendingBackupDir, { recursive: true });
  await TransactionJournal.create(pendingBackupDir, {
    codexHome,
    targetProvider: "openai",
    potentialTargets: []
  });
  const before = await hashTree(codexHome);
  let closed = false;
  return {
    fixtureRoot,
    codexHome,
    userData,
    before,
    async assertUnchanged() {
      const after = await hashTree(codexHome);
      if (JSON.stringify(after) !== JSON.stringify(before)) {
        throw new Error("Desktop read-only fixture was modified.");
      }
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
