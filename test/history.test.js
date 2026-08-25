import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { getHistorySession, listHistory } from "../src/history.js";

test("history public inputs fail with typed invalid-input errors", async () => {
  await assert.rejects(
    () => listHistory("unused", { page: 0 }),
    (error) => error?.code === "INVALID_INPUT" && /page must/.test(error.message)
  );
  await assert.rejects(
    () => listHistory("unused", { archived: "unknown" }),
    (error) => error?.code === "INVALID_INPUT" && /archived must/.test(error.message)
  );
  await assert.rejects(
    () => getHistorySession("unused", ""),
    (error) => error?.code === "INVALID_INPUT" && /sessionId is required/.test(error.message)
  );
});

async function fixture() {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "codex-history-"));
  const file = path.join(home, "sessions", "2026", "08", "04", "rollout-one.jsonl");
  await fs.mkdir(path.dirname(file), { recursive: true });
  const lines = [
    { type: "session_meta", timestamp: "2026-08-04T08:00:00.000Z", payload: { id: "thread-one", title: "测试会话", cwd: "/work/demo", model_provider: "openai", model: "gpt-5" } },
    { type: "event_msg", timestamp: "2026-08-04T08:01:00.000Z", payload: { type: "user_message", message: "请总结这个项目" } },
    { type: "event_msg", timestamp: "2026-08-04T08:02:00.000Z", payload: { type: "assistant_message", message: "这是项目总结。" } },
    { type: "response_item", timestamp: "2026-08-04T08:03:00.000Z", payload: { type: "message", role: "assistant", content: [{ type: "output_text", text: "Agent 的详细回答。" }] } },
    { type: "event_msg", payload: { type: "tool_call", arguments: "secret" } },
    { type: "event_msg", payload: { encrypted_content: "gAAA" } }
  ];
  await fs.writeFile(file, lines.map((line) => JSON.stringify(line)).join("\n") + "\n", "utf8");
  return { home, file };
}

test("history lists readable sessions and filters message text", async () => {
  const { home } = await fixture();
  try {
    const result = await listHistory(home, { page: 1, pageSize: 50, query: "总结" });
    assert.equal(result.total, 1);
    assert.equal(result.sessions[0].id, "thread-one");
    assert.equal(result.sessions[0].messageCount, 3);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history detail returns only safe messages with a limit", async () => {
  const { home } = await fixture();
  try {
    const result = await getHistorySession(home, "thread-one", { messageLimit: 1 });
    assert.equal(result.returnedMessageCount, 1);
    assert.equal(result.truncated, true);
    assert.equal(result.messages[0].role, "assistant");
    assert.equal(result.messages[0].text, "Agent 的详细回答。");
    assert.doesNotMatch(JSON.stringify(result), /encrypted_content|tool_call|secret/);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history prefers canonical user events over response-item bootstrap and duplicate messages", async () => {
  const { home, file } = await fixture();
  try {
    const lines = [
      { type: "session_meta", timestamp: "2026-08-04T08:00:00.000Z", payload: { id: "thread-one", cwd: "/work/demo", model_provider: "openai" } },
      { type: "response_item", timestamp: "2026-08-04T08:00:10.000Z", payload: { type: "message", role: "user", content: [{ type: "input_text", text: "<recommended_plugins>internal bootstrap</recommended_plugins>" }] } },
      { type: "response_item", timestamp: "2026-08-04T08:01:00.000Z", payload: { type: "message", role: "user", content: [{ type: "input_text", text: "请检查真实标题" }] } },
      { type: "event_msg", timestamp: "2026-08-04T08:01:00.000Z", payload: { type: "user_message", message: "请检查真实标题" } },
      { type: "response_item", timestamp: "2026-08-04T08:02:00.000Z", payload: { type: "message", role: "assistant", content: [{ type: "output_text", text: "标题已检查。" }] } }
    ];
    await fs.writeFile(file, `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`, "utf8");

    const list = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(list.sessions[0].title, "请检查真实标题");
    assert.equal(list.sessions[0].firstUserMessage, "请检查真实标题");
    assert.equal(list.sessions[0].messageCount, 2);

    const detail = await getHistorySession(home, "thread-one");
    assert.deepEqual(detail.messages.map(({ role, text }) => ({ role, text })), [
      { role: "user", text: "请检查真实标题" },
      { role: "assistant", text: "标题已检查。" }
    ]);

    const legacyLines = [
      lines[0],
      { type: "response_item", timestamp: "2026-08-04T08:01:00.000Z", payload: { type: "message", role: "user", content: [{ type: "input_text", text: "旧格式用户消息" }] } },
      lines.at(-1)
    ];
    await fs.writeFile(file, `${legacyLines.map((line) => JSON.stringify(line)).join("\n")}\n`, "utf8");
    const legacy = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(legacy.sessions[0].firstUserMessage, "旧格式用户消息");
    assert.equal(legacy.sessions[0].messageCount, 2);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history keeps the newest session when rollouts share a thread id", async () => {
  const { home } = await fixture();
  try {
    const archived = path.join(home, "archived_sessions", "2026", "08", "04", "rollout-copy.jsonl");
    await fs.mkdir(path.dirname(archived), { recursive: true });
    await fs.writeFile(archived, `${JSON.stringify({ type: "session_meta", payload: { id: "thread-one", title: "新副本", cwd: "/work/demo", model_provider: "openai" } })}\n`, "utf8");
    const newer = new Date(Date.now() + 1000);
    await fs.utimes(archived, newer, newer);
    const result = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(result.total, 1);
    assert.equal(result.sessions[0].title, "新副本");
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history ignores rollout files that disappear before their content is read", async () => {
  const { home, file } = await fixture();
  try {
    await fs.rm(file);
    const result = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(result.total, 0);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history exposes a stable bounded id when a session has no thread id", async () => {
  const { home, file } = await fixture();
  try {
    await fs.writeFile(file, `${JSON.stringify({ type: "session_meta", payload: { title: "No thread id", cwd: "/work/demo", model_provider: "openai" } })}\n`, "utf8");
    const first = await listHistory(home, { page: 1, pageSize: 50 });
    const second = await listHistory(home, { page: 1, pageSize: 50 });
    assert.equal(first.total, 1);
    assert.match(first.sessions[0].id, /^rollout:[A-Za-z0-9_-]{43}$/);
    assert.equal(first.sessions[0].id, second.sessions[0].id);
    assert.ok(first.sessions[0].id.length <= 300);
    assert.equal(first.sessions[0].rolloutPath, path.resolve(file));
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});
