import assert from "node:assert/strict";
import test from "node:test";

import { captureProfileOperation, dedupeHistorySessions, operationToast, restoreRelocationState } from "./operation-state.js";

test("captures the profile revision used by a confirmation", () => {
  const profile = { id: "work", revision: "rev-1", sqliteHome: "/data/sqlite" };
  const operation = captureProfileOperation(profile, { type: "execute" });
  profile.revision = "rev-2";
  profile.sqliteHome = "/other/sqlite";

  assert.deepEqual(operation.profileId, "work");
  assert.deepEqual(operation.profileRevision, "rev-1");
  assert.equal(operation.profile.revision, "rev-1");
  assert.equal(operation.profile.sqliteHome, "/data/sqlite");
});

test("marks locked rollout results as partial instead of success", () => {
  const toast = operationToast({ result: { outcome: "partial", skippedLockedRolloutFiles: ["/sessions/live.jsonl"] } }, {
    successTitle: "同步完成",
    partialTitle: "同步部分完成",
    message: "备份：/backup"
  });

  assert.equal(toast.tone, "warning");
  assert.match(toast.message, /1 个/);
  assert.match(toast.message, /live\.jsonl/);
});

test("requires an explicit profile SQLite home for relocation", () => {
  const state = restoreRelocationState({
    backup: { metadata: { sqliteHome: "/backup/sqlite" } },
    profile: { id: "default", revision: "rev-1", sqliteHome: "" },
    targetSqliteHome: "/current/sqlite",
    restoreDatabase: true,
    restoreConfig: false,
    sqliteSupported: true
  });

  assert.equal(state.missingExplicitTarget, true);
  assert.equal(state.canSubmit, false);
});

test("deduplicates history by thread id before normalized rollout path", () => {
  const sessions = dedupeHistorySessions([
    { id: "thread-1", rolloutPath: "/sessions/a.jsonl" },
    { id: "thread-1", rolloutPath: "/sessions/b.jsonl" },
    { filePath: "C:\\Sessions\\c.jsonl" },
    { rolloutPath: "c:/sessions/c.jsonl" },
    { rolloutPath: "/sessions/Case.jsonl" },
    { rolloutPath: "/sessions/case.jsonl" }
  ], { platform: "win32" });

  assert.deepEqual(sessions.map((session) => session.rolloutPath ?? session.filePath), ["/sessions/a.jsonl", "C:\\Sessions\\c.jsonl", "/sessions/Case.jsonl"]);
  assert.equal(dedupeHistorySessions([
    { rolloutPath: "/sessions/Case.jsonl" },
    { rolloutPath: "/sessions/case.jsonl" }
  ], { platform: "linux" }).length, 2);
});
