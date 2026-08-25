import assert from "node:assert/strict";
import test from "node:test";

import { captureProfileOperation, dedupeHistorySessions, operationToast, resolveRestoreTargetSqliteHome, restoreRelocationState } from "./operation-state.js";

test("captures the profile revision used by a confirmation", () => {
  const profile = { id: "work", revision: "rev-1", sqliteHome: "/data/sqlite" };
  const liveOperation = { type: "execute", selectedProvider: "provider-a" };
  const status = { profileId: "work", profileRevision: "rev-1", storageRevision: "storage-1", sqliteHome: "/data/sqlite" };
  const operation = captureProfileOperation(profile, liveOperation, status);
  profile.revision = "rev-2";
  profile.sqliteHome = "/other/sqlite";
  liveOperation.selectedProvider = "provider-b";
  status.storageRevision = "storage-2";
  status.sqliteHome = "/other/sqlite";

  assert.deepEqual(operation.profileId, "work");
  assert.deepEqual(operation.profileRevision, "rev-1");
  assert.equal(Object.hasOwn(operation, "storageRevision"), false);
  assert.equal(operation.selectedProvider, "provider-a");
  assert.equal(operation.profile.revision, "rev-1");
  assert.equal(operation.profile.sqliteHome, "/data/sqlite");
  assert.equal(operation.status.sqliteHome, "/data/sqlite");
  assert.equal(captureProfileOperation(profile, liveOperation, operation.status), null);
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

test("reads locked rollout details from the C3 OperationResult envelope", () => {
  const toast = operationToast({
    result: {
      outcome: "partial",
      result: { skippedLockedRolloutFiles: ["/sessions/c3-live.jsonl"] }
    }
  }, {
    successTitle: "同步完成",
    partialTitle: "同步部分完成",
    message: "备份已创建"
  });
  assert.equal(toast.tone, "warning");
  assert.match(toast.message, /c3-live\.jsonl/);
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

test("restores a missing default legacy-root database to its original candidate", () => {
  const status = {
    codexHome: "/home/user/.codex",
    sqliteHome: "/home/user/.codex/sqlite",
    sqliteHomeSource: "default",
    stateDbLocation: null
  };
  const backup = { metadata: { version: 2, sqliteHome: "/home/user/.codex" } };
  const targetSqliteHome = resolveRestoreTargetSqliteHome(status, backup);
  const state = restoreRelocationState({
    backup,
    profile: { id: "default", revision: "rev-1", sqliteHome: "" },
    targetSqliteHome,
    restoreDatabase: true,
    restoreConfig: false,
    sqliteSupported: true
  });

  assert.equal(targetSqliteHome, status.codexHome);
  assert.equal(state.requiresRelocation, false);
  assert.equal(state.missingExplicitTarget, false);
  assert.equal(state.canSubmit, true);
});

test("does not treat an unrelated backup SQLite home as a default legacy candidate", () => {
  const status = {
    codexHome: "/home/user/.codex",
    sqliteHome: "/home/user/.codex/sqlite",
    sqliteHomeSource: "default",
    stateDbLocation: null
  };
  const backup = { metadata: { version: 2, sqliteHome: "/mnt/other/sqlite" } };
  const targetSqliteHome = resolveRestoreTargetSqliteHome(status, backup);
  const state = restoreRelocationState({
    backup,
    profile: { id: "default", revision: "rev-1", sqliteHome: "" },
    targetSqliteHome,
    restoreDatabase: true,
    restoreConfig: false,
    sqliteSupported: true
  });

  assert.equal(targetSqliteHome, status.sqliteHome);
  assert.equal(state.requiresRelocation, true);
  assert.equal(state.missingExplicitTarget, true);
  assert.equal(state.canSubmit, false);
});

test("does not enable legacy fallback for a configured SQLite home", () => {
  const status = {
    codexHome: "/home/user/.codex",
    sqliteHome: "/data/configured-sqlite",
    sqliteHomeSource: "config",
    stateDbLocation: null
  };
  const backup = { metadata: { version: 2, sqliteHome: "/home/user/.codex" } };
  const targetSqliteHome = resolveRestoreTargetSqliteHome(status, backup);
  const state = restoreRelocationState({
    backup,
    profile: { id: "default", revision: "rev-1", sqliteHome: "" },
    targetSqliteHome,
    restoreDatabase: true,
    restoreConfig: false,
    sqliteSupported: true
  });

  assert.equal(targetSqliteHome, status.sqliteHome);
  assert.equal(state.requiresRelocation, true);
  assert.equal(state.missingExplicitTarget, true);
  assert.equal(state.canSubmit, false);
});

test("does not enable legacy fallback for a CLI SQLite home", () => {
  const status = {
    codexHome: "/home/user/.codex",
    sqliteHome: "/data/cli-sqlite",
    sqliteHomeSource: "cli",
    stateDbLocation: null
  };
  const backup = { metadata: { version: 2, sqliteHome: "/home/user/.codex" } };
  const targetSqliteHome = resolveRestoreTargetSqliteHome(status, backup);
  const state = restoreRelocationState({
    backup,
    profile: { id: "default", revision: "rev-1", sqliteHome: "" },
    targetSqliteHome,
    restoreDatabase: true,
    restoreConfig: false,
    sqliteSupported: true
  });

  assert.equal(targetSqliteHome, status.sqliteHome);
  assert.equal(state.requiresRelocation, true);
  assert.equal(state.missingExplicitTarget, true);
  assert.equal(state.canSubmit, false);
});

test("prefers an existing database over the backup legacy candidate", () => {
  const status = {
    codexHome: "/home/user/.codex",
    sqliteHome: "/home/user/.codex/sqlite",
    sqliteHomeSource: "default",
    stateDbLocation: { path: "/current/db/state_5.sqlite", source: "sqlite-dir" }
  };
  const backup = { metadata: { version: 2, sqliteHome: "/home/user/.codex" } };

  assert.equal(resolveRestoreTargetSqliteHome(status, backup), "/current/db");
});

test("uses the server platform rule when comparing restore paths", () => {
  const status = {
    codexHome: "/Home/User/.codex",
    sqliteHome: "/Home/User/.codex/sqlite",
    sqliteHomeSource: "default",
    stateDbLocation: null,
    pathComparisonCaseInsensitive: false
  };
  const backup = { metadata: { version: 2, sqliteHome: "/home/user/.codex" } };

  assert.equal(resolveRestoreTargetSqliteHome(status, backup), status.sqliteHome);
  assert.equal(resolveRestoreTargetSqliteHome({ ...status, pathComparisonCaseInsensitive: true }, backup), status.codexHome);
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
