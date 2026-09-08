import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { getDiagnostics } from "../src/diagnostics.js";
import { openDatabase } from "../src/sqlite.js";

test("getDiagnostics reports bounded safety metadata without credentials or message bodies", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-diagnostics-"));
  const codexHome = path.join(root, ".codex");
  const rolloutPath = path.join(codexHome, "sessions", "2026", "08", "25", "rollout.jsonl");
  const secret = "credential-and-message-secret-fixture";
  try {
    await fs.mkdir(path.dirname(rolloutPath), { recursive: true });
    await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
    await fs.mkdir(path.join(codexHome, "sqlite"), { recursive: true });
    await fs.writeFile(path.join(codexHome, "config.toml"), 'model_provider = "openai"\n', "utf8");
    await fs.writeFile(path.join(codexHome, "auth.json"), JSON.stringify({ token: secret }), "utf8");
    const database = await openDatabase(path.join(codexHome, "sqlite", "state_5.sqlite"));
    try {
      database.exec(`
        CREATE TABLE threads (
          id TEXT PRIMARY KEY,
          model_provider TEXT,
          cwd TEXT NOT NULL DEFAULT '',
          archived INTEGER NOT NULL DEFAULT 0,
          first_user_message TEXT NOT NULL DEFAULT '',
          model TEXT
        )
      `);
      database.prepare("INSERT INTO threads (id, model_provider, cwd, archived, first_user_message) VALUES (?, ?, ?, ?, ?)")
        .run("thread-a", "openai", "C:\\AITemp", 0, "redacted");
    } finally {
      database.close();
    }
    await fs.writeFile(rolloutPath, [
      JSON.stringify({
        timestamp: "2026-08-25T00:00:00.000Z",
        type: "session_meta",
        payload: { id: "thread-a", model_provider: "openai", cwd: "C:\\AITemp" }
      }),
      JSON.stringify({ type: "event_msg", payload: { type: "user_message", message: secret } })
    ].join("\n") + "\n", "utf8");

    const diagnostics = await getDiagnostics({ codexHome });
    const serialized = JSON.stringify(diagnostics);
    assert.equal(diagnostics.schemaVersion, 1);
    assert.equal(diagnostics.safety.lockedRolloutCount, 0);
    assert.doesNotMatch(serialized, new RegExp(secret));
    assert.doesNotMatch(serialized, /auth\.json|token/i);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
