import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { getHistorySession, listHistory } from "../src/history.js";

async function fixture(records) {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "codex-history-subagent-label-"));
  const sessions = path.join(home, "sessions", "2026", "09", "04");
  await fs.mkdir(sessions, { recursive: true });
  await Promise.all(records.map(async ({ id, payload }, index) => {
    const rollout = path.join(sessions, `rollout-${index}.jsonl`);
    await fs.writeFile(rollout, `${JSON.stringify({
      type: "session_meta",
      timestamp: `2026-09-04T08:0${index}:00.000Z`,
      payload: {
        id,
        cwd: "/work/history-subagent-label",
        model_provider: "openai",
        ...payload
      }
    })}\n`, "utf8");
  }));
  return home;
}

function byId(sessions) {
  return new Map(sessions.map((session) => [session.id, session]));
}

test("history identifies nested, direct, and parent-linked subagents without reading messages in the list", async () => {
  const home = await fixture([
    {
      id: "nested-thread-spawn",
      payload: {
        source: {
          subagent: {
            agent_path: "root/ignored-direct-path",
            thread_spawn: {
              agent_path: "root/research/leaf-worker",
              agent_nickname: "ignored nested nickname"
            }
          }
        }
      }
    },
    {
      id: "direct-subagent",
      payload: {
        agent_path: "root/direct-worker",
        source: { subagent: {} }
      }
    },
    {
      id: "parent-linked-subagent",
      payload: {
        parent_thread_id: "parent-thread",
        agent_path: "root/parent-linked-worker"
      }
    }
  ]);
  try {
    const list = await listHistory(home, { page: 1, pageSize: 50 });
    const sessions = byId(list.sessions);

    assert.equal(sessions.get("nested-thread-spawn").subagentName, "leaf-worker");
    assert.equal(sessions.get("direct-subagent").subagentName, "direct-worker");
    assert.equal(sessions.get("parent-linked-subagent").subagentName, "parent-linked-worker");
    for (const session of list.sessions) {
      assert.equal(session.messageCount, 0);
      assert.equal(session.messageCountKnown, false);
      assert.equal(Object.hasOwn(session, "messages"), false);
    }

    const detail = await getHistorySession(home, "nested-thread-spawn");
    assert.equal(detail.session.subagentName, "leaf-worker");
    assert.equal(detail.session.title, "");
    assert.deepEqual(detail.messages, []);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history uses the preferred nested nickname when a confirmed subagent has no usable path", async () => {
  const home = await fixture([
    {
      id: "nickname-fallback",
      payload: {
        source: {
          subagent: {
            agent_nickname: "direct nickname should lose",
            thread_spawn: {
              agent_path: " / ",
              agent_nickname: "Nested fallback nickname"
            }
          }
        }
      }
    }
  ]);
  try {
    const list = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(list.sessions[0].subagentName, "Nested fallback nickname");

    const detail = await getHistorySession(home, "nickname-fallback");
    assert.equal(detail.session.subagentName, "Nested fallback nickname");
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history keeps a stored title separate from the optional subagent label", async () => {
  const home = await fixture([
    {
      id: "stored-title",
      payload: {
        title: "User-owned stored title",
        agent_path: "root/title-worker",
        source: { subagent: {} }
      }
    }
  ]);
  try {
    const list = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(list.sessions[0].title, "User-owned stored title");
    assert.equal(list.sessions[0].subagentName, "title-worker");

    const detail = await getHistorySession(home, "stored-title");
    assert.equal(detail.session.title, "User-owned stored title");
    assert.equal(detail.session.subagentName, "title-worker");
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history does not expose agent paths for records without confirmed subagent metadata", async () => {
  const home = await fixture([
    {
      id: "ordinary-thread",
      payload: {
        agent_path: "root/private-path/ordinary-worker",
        agent_nickname: "ordinary nickname"
      }
    }
  ]);
  try {
    const list = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(list.sessions[0].title, "");
    assert.equal(list.sessions[0].subagentName, undefined);
    assert.doesNotMatch(JSON.stringify(list.sessions[0]), /ordinary-worker|private-path|ordinary nickname/);

    const detail = await getHistorySession(home, "ordinary-thread");
    assert.equal(detail.session.subagentName, undefined);
    assert.doesNotMatch(JSON.stringify(detail.session), /ordinary-worker|private-path|ordinary nickname/);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history bounds subagent labels and returns only the final path segment", async () => {
  const longLeaf = "x".repeat(200);
  const home = await fixture([
    {
      id: "bounded-subagent",
      payload: {
        source: {
          subagent: {
            thread_spawn: {
              agent_path: `root/private-parent/${longLeaf}`
            }
          }
        }
      }
    }
  ]);
  try {
    const list = await listHistory(home, { page: 1, pageSize: 50 });
    const expected = longLeaf.slice(0, 160);
    assert.equal(list.sessions[0].subagentName, expected);
    assert.equal(list.sessions[0].subagentName.length, 160);
    assert.doesNotMatch(JSON.stringify(list.sessions[0]), /private-parent/);

    const detail = await getHistorySession(home, "bounded-subagent");
    assert.equal(detail.session.subagentName, expected);
    assert.doesNotMatch(JSON.stringify(detail.session), /private-parent/);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});
