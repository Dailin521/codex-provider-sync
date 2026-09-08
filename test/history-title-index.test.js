import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { getHistorySession, listHistory } from "../src/history.js";
import { openDatabase } from "../src/sqlite.js";

const THREAD_ID = "history-title-index-thread";

async function fixture({ rolloutTitle = "" } = {}) {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "codex-history-title-index-"));
  const rolloutPath = path.join(home, "sessions", "2026", "09", "04", "rollout-title-index.jsonl");
  await fs.mkdir(path.dirname(rolloutPath), { recursive: true });
  await fs.writeFile(rolloutPath, `${JSON.stringify({
    type: "session_meta",
    timestamp: "2026-09-04T08:00:00.000Z",
    payload: {
      id: THREAD_ID,
      ...(rolloutTitle ? { title: rolloutTitle } : {}),
      cwd: "/work/history-title-index",
      model_provider: "openai"
    }
  })}\n${JSON.stringify({
    type: "event_msg",
    timestamp: "2026-09-04T08:01:00.000Z",
    payload: { type: "user_message", message: "safe fixture message" }
  })}\n`, "utf8");
  return { home, rolloutPath };
}

async function writeIndex(home, records) {
  await fs.writeFile(
    path.join(home, "session_index.jsonl"),
    `${records.map((record) => typeof record === "string" ? record : JSON.stringify(record)).join("\n")}\n`,
    "utf8"
  );
}

async function writeThreadTitle(home, title) {
  const dbPath = path.join(home, "sqlite", "state_5.sqlite");
  await fs.mkdir(path.dirname(dbPath), { recursive: true });
  const db = await openDatabase(dbPath);
  try {
    db.exec("CREATE TABLE threads (id TEXT PRIMARY KEY, title TEXT)");
    db.prepare("INSERT INTO threads (id, title) VALUES (?, ?)").run(THREAD_ID, title);
  } finally {
    db.close();
  }
}

test("history uses a session-index thread name for list and selected detail when rollout metadata has none", async () => {
  const { home } = await fixture();
  try {
    await writeIndex(home, [{ id: THREAD_ID, thread_name: "Indexed session title" }]);

    const list = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(list.sessions[0].title, "Indexed session title");

    const detail = await getHistorySession(home, THREAD_ID);
    assert.equal(detail.session.title, "Indexed session title");
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history session-index thread names override selected SQLite titles", async () => {
  const { home } = await fixture({ rolloutTitle: "Rollout title" });
  try {
    await writeThreadTitle(home, "SQLite title");
    await writeIndex(home, [{ id: THREAD_ID, thread_name: "Indexed title" }]);

    const list = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(list.sessions[0].title, "Indexed title");

    const detail = await getHistorySession(home, THREAD_ID);
    assert.equal(detail.session.title, "Indexed title");
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history uses the latest valid session-index rename in append order", async () => {
  const { home } = await fixture();
  try {
    await writeIndex(home, [
      { id: THREAD_ID, thread_name: "Original name" },
      { id: THREAD_ID, thread_name: "Renamed later" }
    ]);

    const list = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(list.sessions[0].title, "Renamed later");
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history retains an earlier index title when later rows are malformed, blank, or oversized", async () => {
  const { home } = await fixture();
  try {
    await writeIndex(home, [
      { id: THREAD_ID, thread_name: "Earlier valid index title" },
      "{not-json}",
      { id: THREAD_ID, thread_name: "   " },
      { id: THREAD_ID, thread_name: "x".repeat(64 * 1024) }
    ]);

    const list = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(list.sessions[0].title, "Earlier valid index title");
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history falls back to SQLite titles when session_index.jsonl is missing", async () => {
  const { home } = await fixture({ rolloutTitle: "Rollout fallback title" });
  try {
    await writeThreadTitle(home, "SQLite fallback title");

    const list = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(list.sessions[0].title, "SQLite fallback title");

    const detail = await getHistorySession(home, THREAD_ID);
    assert.equal(detail.session.title, "SQLite fallback title");
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history truncates an oversized selected SQLite title to 1024 characters", async () => {
  const { home } = await fixture();
  const title = `SQLite ${"x".repeat(1_100)}`;
  try {
    await writeThreadTitle(home, title);

    const list = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(list.sessions[0].title, title.slice(0, 1024));
    assert.equal(list.sessions[0].title.length, 1024);

    const detail = await getHistorySession(home, THREAD_ID);
    assert.equal(detail.session.title, title.slice(0, 1024));
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history ignores a linked session index outside the Codex Home", async (t) => {
  const { home } = await fixture({ rolloutTitle: "Safe rollout title" });
  const external = await fs.mkdtemp(path.join(os.tmpdir(), "codex-history-external-index-"));
  const externalIndex = path.join(external, "session_index.jsonl");
  try {
    await fs.writeFile(externalIndex, `${JSON.stringify({
      id: THREAD_ID,
      thread_name: "External index title"
    })}\n`, "utf8");
    try {
      await fs.symlink(externalIndex, path.join(home, "session_index.jsonl"), process.platform === "win32" ? "file" : "file");
    } catch (error) {
      if (["EPERM", "EACCES", "ENOTSUP"].includes(error?.code)) {
        t.skip(`file link unavailable: ${error.code}`);
        return;
      }
      throw error;
    }

    const list = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(list.sessions[0].title, "Safe rollout title");
    assert.doesNotMatch(JSON.stringify(list), /External index title/);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
    await fs.rm(external, { recursive: true, force: true });
  }
});
