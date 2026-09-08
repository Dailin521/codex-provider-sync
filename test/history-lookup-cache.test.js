import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { getHistorySession, listHistory } from "../src/history.js";

async function makeHome(prefix = "codex-history-lookup-") {
  return fs.mkdtemp(path.join(os.tmpdir(), prefix));
}

async function writeRollout(home, relativePath, id, body) {
  const file = path.join(home, relativePath);
  await fs.mkdir(path.dirname(file), { recursive: true });
  await fs.writeFile(file, [
    JSON.stringify({
      type: "session_meta",
      timestamp: "2026-09-07T08:00:00.000Z",
      payload: { id, cwd: "/work/history-cache", model_provider: "openai" }
    }),
    JSON.stringify({
      type: "event_msg",
      timestamp: "2026-09-07T08:01:00.000Z",
      payload: { type: "assistant_message", message: body }
    })
  ].join("\n") + "\n", "utf8");
  return file;
}

function trackRolloutOpens() {
  const originalOpen = fs.open;
  const opened = [];
  fs.open = async (...args) => {
    const filePath = path.resolve(String(args[0]));
    if (path.basename(filePath).startsWith("rollout-")) opened.push(filePath);
    return originalOpen(...args);
  };
  return {
    opened,
    reset() { opened.length = 0; },
    restore() { fs.open = originalOpen; }
  };
}

test("history detail reuses a listed lookup without opening every rollout again", async () => {
  const home = await makeHome();
  const target = await writeRollout(home, path.join("sessions", "rollout-000.jsonl"), "session-000", "selected body");
  for (let index = 1; index < 300; index += 1) {
    await writeRollout(home, path.join("sessions", `rollout-${String(index).padStart(3, "0")}.jsonl`), `session-${index}`, `decoy-${index}`);
  }
  const tracker = trackRolloutOpens();
  try {
    await listHistory(home, { page: 1, pageSize: 50 });
    tracker.reset();

    const detail = await getHistorySession(home, "session-000");

    assert.equal(detail.messages[0].text, "selected body");
    assert.deepEqual(new Set(tracker.opened), new Set([path.resolve(target)]));
    assert.equal(tracker.opened.length, 2, "one bounded metadata revalidation and one selected full read");
  } finally {
    tracker.restore();
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history lookup rejects a stale duplicate choice and selects the current canonical rollout", async () => {
  const home = await makeHome();
  const active = await writeRollout(home, path.join("sessions", "rollout-active.jsonl"), "shared-session", "old active body");
  const archived = await writeRollout(home, path.join("archived_sessions", "rollout-archive.jsonl"), "shared-session", "replacement archive body");
  try {
    const earlier = new Date(Date.now() - 20_000);
    const later = new Date(Date.now() - 10_000);
    await fs.utimes(archived, earlier, earlier);
    await fs.utimes(active, later, later);
    const listed = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(listed.sessions[0].archived, false);

    const newest = new Date(Date.now() + 10_000);
    await fs.utimes(archived, newest, newest);
    const detail = await getHistorySession(home, "shared-session");

    assert.equal(detail.session.archived, true);
    assert.equal(detail.messages[0].text, "replacement archive body");
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history lookup invalidates a renamed rollout and is isolated by physical Codex Home", async () => {
  const firstHome = await makeHome("codex-history-lookup-first-");
  const secondHome = await makeHome("codex-history-lookup-second-");
  const active = await writeRollout(firstHome, path.join("sessions", "rollout-move.jsonl"), "shared-id", "first home body");
  await writeRollout(secondHome, path.join("sessions", "rollout-other.jsonl"), "shared-id", "second home body");
  const moved = path.join(firstHome, "archived_sessions", "rollout-move.jsonl");
  try {
    await listHistory(firstHome, { page: 1, pageSize: 50 });
    await listHistory(secondHome, { page: 1, pageSize: 50 });
    await fs.mkdir(path.dirname(moved), { recursive: true });
    await fs.rename(active, moved);

    const first = await getHistorySession(firstHome, "shared-id");
    const second = await getHistorySession(secondHome, "shared-id");

    assert.equal(first.session.archived, true);
    assert.equal(first.messages[0].text, "first home body");
    assert.equal(second.session.archived, false);
    assert.equal(second.messages[0].text, "second home body");
  } finally {
    await fs.rm(firstHome, { recursive: true, force: true });
    await fs.rm(secondHome, { recursive: true, force: true });
  }
});
