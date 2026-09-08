import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { getHistorySession, listHistory } from "../src/history.js";

async function makeHome() {
  return fs.mkdtemp(path.join(os.tmpdir(), "codex-history-session-actions-"));
}

async function writeRollout(home, name, payload, body = "") {
  const file = path.join(home, "sessions", "2026", "09", "04", `rollout-${name}.jsonl`);
  await fs.mkdir(path.dirname(file), { recursive: true });
  const records = [{
    type: "session_meta",
    timestamp: "2026-09-04T08:00:00.000Z",
    payload: {
      cwd: "/work/history-actions",
      model_provider: "openai",
      ...payload
    }
  }];
  if (body) {
    records.push({
      type: "event_msg",
      timestamp: "2026-09-04T08:01:00.000Z",
      payload: { type: "assistant_message", message: body }
    });
  }
  await fs.writeFile(file, `${records.map((record) => JSON.stringify(record)).join("\n")}\n`, "utf8");
  return file;
}

function trackReadBytes() {
  const originalOpen = fs.open;
  const bytesByPath = new Map();
  fs.open = async (...args) => {
    const handle = await originalOpen(...args);
    const filePath = path.resolve(String(args[0]));
    const originalRead = handle.read.bind(handle);
    handle.read = async (...readArgs) => {
      const result = await originalRead(...readArgs);
      bytesByPath.set(filePath, (bytesByPath.get(filePath) ?? 0) + result.bytesRead);
      return result;
    };
    return handle;
  };
  return {
    bytesByPath,
    restore() {
      fs.open = originalOpen;
    }
  };
}

test("metadata searches stay bounded and content searches remain explicit", async () => {
  const home = await makeHome();
  const nativeFile = await writeRollout(home, "native", {
    id: "native-session-id",
    title: "Metadata title",
    source: {
      subagent: {
        thread_spawn: {
          parent_thread_id: "nested-parent-id",
          agent_path: "root/history-worker"
        }
      }
    },
    parent_thread_id: "root-parent-id"
  }, `private-large-body-${"x".repeat(2 * 1024 * 1024)}`);
  await writeRollout(home, "fallback", {
    title: "Fallback title"
  }, "content-only-marker");
  const tracker = trackReadBytes();
  try {
    const metadataId = await listHistory(home, {
      page: 1,
      pageSize: 50,
      query: "native-session-id",
      searchScope: "metadata"
    });
    assert.equal(metadataId.total, 1);
    assert.equal(metadataId.sessions[0].id, "native-session-id");
    assert.equal(metadataId.sessions[0].nativeSessionId, "native-session-id");
    assert.equal(metadataId.sessions[0].sessionKind, "subagent");
    assert.equal(metadataId.sessions[0].parentSessionId, "nested-parent-id");
    assert.ok((tracker.bytesByPath.get(path.resolve(nativeFile)) ?? 0) <= 64 * 1024);

    const metadataContent = await listHistory(home, {
      page: 1,
      pageSize: 50,
      query: "content-only-marker",
      searchScope: "metadata"
    });
    assert.equal(metadataContent.total, 0);

    const content = await listHistory(home, {
      page: 1,
      pageSize: 50,
      query: "content-only-marker"
    });
    assert.equal(content.total, 1);
    assert.equal(content.sessions[0].title, "Fallback title");
    assert.equal(content.sessions[0].messageCountKnown, true);
  } finally {
    tracker.restore();
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history exposes real identities, explicit parent correlation, and stat file timestamps", async () => {
  const home = await makeHome();
  const nativeFile = await writeRollout(home, "native", {
    id: "native-session-id",
    source: {
      subagent: {
        thread_spawn: { parent_thread_id: "nested-parent-id" }
      }
    },
    parent_thread_id: "root-parent-id"
  }, "detail body must remain unread for metadataOnly");
  await writeRollout(home, "fallback", { title: "Fallback session" });
  await writeRollout(home, "fork-only", {
    id: "fork-only-session",
    forked_from: "must-not-be-used-as-parent"
  });
  try {
    const list = await listHistory(home, { page: 1, pageSize: 50 });
    const byId = new Map(list.sessions.map((session) => [session.id, session]));
    const native = byId.get("native-session-id");
    const fallback = [...byId.values()].find((session) => session.title === "Fallback session");
    const forkOnly = byId.get("fork-only-session");
    assert.equal(native.nativeSessionId, "native-session-id");
    assert.equal(native.sessionKind, "subagent");
    assert.equal(native.parentSessionId, "nested-parent-id");
    assert.equal(fallback.nativeSessionId, null);
    assert.match(fallback.id, /^rollout:/);
    assert.equal(fallback.sessionKind, "main");
    assert.equal(fallback.parentSessionId, null);
    assert.equal(forkOnly.sessionKind, "main");
    assert.equal(forkOnly.parentSessionId, null);
    assert.match(native.fileModifiedAt, /^\d{4}-\d{2}-\d{2}T/);

    const tracker = trackReadBytes();
    try {
      const detail = await getHistorySession(home, "native-session-id", { metadataOnly: true });
      assert.equal(detail.session.nativeSessionId, "native-session-id");
      assert.equal(detail.session.fileModifiedAt, native.fileModifiedAt);
      assert.deepEqual(detail.messages, []);
      assert.equal(detail.truncated, false);
      assert.equal(detail.returnedMessageCount, 0);
      assert.ok((tracker.bytesByPath.get(path.resolve(nativeFile)) ?? 0) <= 64 * 1024);
    } finally {
      tracker.restore();
    }
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history applies session-kind filters before pagination and validates new enums", async () => {
  const home = await makeHome();
  try {
    for (let index = 0; index < 11; index += 1) {
      await writeRollout(home, `subagent-${index}`, {
        id: `subagent-${index}`,
        source: { subagent: {} }
      });
    }
    await writeRollout(home, "main", { id: "main-session" });

    const firstPage = await listHistory(home, {
      page: 1,
      pageSize: 10,
      sessionKind: "subagent"
    });
    const secondPage = await listHistory(home, {
      page: 2,
      pageSize: 10,
      sessionKind: "subagent"
    });
    assert.equal(firstPage.total, 11);
    assert.equal(firstPage.sessions.length, 10);
    assert.equal(firstPage.hasNextPage, true);
    assert.equal(secondPage.total, 11);
    assert.equal(secondPage.sessions.length, 1);
    assert.equal(secondPage.sessions[0].sessionKind, "subagent");

    await assert.rejects(
      () => listHistory(home, { searchScope: "body" }),
      (error) => error?.code === "INVALID_INPUT"
    );
    await assert.rejects(
      () => listHistory(home, { sessionKind: "worker" }),
      (error) => error?.code === "INVALID_INPUT"
    );
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});
