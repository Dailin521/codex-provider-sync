import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

import { runUseOfficial, runUseRelay } from "../src/service.js";

const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-smoke-"));
const codexHome = path.join(root, ".codex");
await fs.mkdir(path.join(codexHome, "sessions", "2026", "06", "14"), { recursive: true });
await fs.mkdir(path.join(codexHome, "archived_sessions", "2026", "06", "13"), { recursive: true });

const configPath = path.join(codexHome, "config.toml");
await fs.writeFile(
  configPath,
  [
    'model_provider = "openai"',
    'sandbox_mode = "danger-full-access"',
    "",
    "[model_providers.OpenAI]",
    'base_url = "https://relay.example.com/v1"',
    'api_key_env_var = "OPENAI_API_KEY"',
    ""
  ].join("\n"),
  "utf8"
);

async function writeRollout(filePath, id, provider, message) {
  const timestamp = "2026-06-14T00:00:00.000Z";
  const first = {
    timestamp,
    type: "session_meta",
    payload: {
      id,
      timestamp,
      cwd: "E:\\Code\\demo",
      source: "cli",
      cli_version: "0.115.0",
      model_provider: provider
    }
  };
  const second = {
    timestamp,
    type: "event_msg",
    payload: {
      type: "user_message",
      message
    }
  };
  await fs.writeFile(filePath, `${JSON.stringify(first)}\n${JSON.stringify(second)}\n`, "utf8");
}

const sessionPath = path.join(codexHome, "sessions", "2026", "06", "14", "rollout-live.jsonl");
const archivedPath = path.join(codexHome, "archived_sessions", "2026", "06", "13", "rollout-archived.jsonl");
await writeRollout(sessionPath, "thread-live", "openai", "DO_NOT_TOUCH_LIVE_MESSAGE");
await writeRollout(archivedPath, "thread-archived", "openai", "DO_NOT_TOUCH_ARCHIVED_MESSAGE");

const dbPath = path.join(codexHome, "state_5.sqlite");
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
  const insert = db.prepare(
    "INSERT INTO threads (id, model_provider, cwd, archived, first_user_message) VALUES (?, ?, ?, ?, ?)"
  );
  insert.run("thread-live", "openai", "E:\\Code\\demo", 0, "DO_NOT_TOUCH_LIVE_MESSAGE");
  insert.run("thread-archived", "openai", "E:\\Code\\demo", 1, "DO_NOT_TOUCH_ARCHIVED_MESSAGE");
} finally {
  db.close();
}

function readThreadRows() {
  const current = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return current
      .prepare("SELECT id, model_provider, first_user_message FROM threads ORDER BY id")
      .all();
  } finally {
    current.close();
  }
}

async function readRolloutSummary(filePath) {
  const lines = (await fs.readFile(filePath, "utf8")).trimEnd().split("\n");
  const meta = JSON.parse(lines[0]);
  const message = JSON.parse(lines[1]);
  return {
    provider: meta.payload.model_provider,
    message: message.payload.message
  };
}

async function snapshot(label, result = null) {
  const config = await fs.readFile(configPath, "utf8");
  return {
    label,
    configProvider: config.match(/model_provider = "([^"]+)"/)?.[1],
    relayBaseUrlPresent: config.includes("https://relay.example.com/v1"),
    liveRollout: await readRolloutSummary(sessionPath),
    archivedRollout: await readRolloutSummary(archivedPath),
    sqlite: readThreadRows(),
    result: result
      ? {
          targetProvider: result.targetProvider,
          changedSessionFiles: result.changedSessionFiles,
          sqliteRowsUpdated: result.sqliteRowsUpdated,
          backupCreated: Boolean(result.backupDir)
        }
      : null
  };
}

const before = await snapshot("before");
const relayResult = await runUseRelay({ codexHome, keepCount: 10 });
const afterRelay = await snapshot("after use-relay", relayResult);
const officialResult = await runUseOfficial({ codexHome, keepCount: 10 });
const afterOfficial = await snapshot("after use-official", officialResult);

const checks = [
  before.configProvider === "openai",
  before.relayBaseUrlPresent,
  afterRelay.configProvider === "OpenAI",
  afterRelay.relayBaseUrlPresent,
  afterRelay.liveRollout.provider === "OpenAI",
  afterRelay.archivedRollout.provider === "OpenAI",
  afterRelay.liveRollout.message === "DO_NOT_TOUCH_LIVE_MESSAGE",
  afterRelay.archivedRollout.message === "DO_NOT_TOUCH_ARCHIVED_MESSAGE",
  afterRelay.sqlite.every((row) => row.model_provider === "OpenAI"),
  afterOfficial.configProvider === "openai",
  afterOfficial.relayBaseUrlPresent,
  afterOfficial.liveRollout.provider === "openai",
  afterOfficial.archivedRollout.provider === "openai",
  afterOfficial.liveRollout.message === "DO_NOT_TOUCH_LIVE_MESSAGE",
  afterOfficial.archivedRollout.message === "DO_NOT_TOUCH_ARCHIVED_MESSAGE",
  afterOfficial.sqlite.every((row) => row.model_provider === "openai")
];

console.log(JSON.stringify({ codexHome, before, afterRelay, afterOfficial, passed: checks.every(Boolean) }, null, 2));
if (!checks.every(Boolean)) {
  process.exitCode = 1;
}
