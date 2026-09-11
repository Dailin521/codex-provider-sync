import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { OperationLogService } from "../dist/main/operation-log-service.js";

test("operation logs merge Prepare and Apply and preserve active versus wall time", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-operation-log-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const logs = new OperationLogService({ directory: root, maxFileBytes: 2048, maxFiles: 5 });
  await logs.initialize();
  const id = await logs.begin({ operation: "sync", profileId: "default", requestId: "prepare-request" });
  await logs.prepared(id, "p".repeat(40));
  await new Promise((resolve) => setTimeout(resolve, 10));
  await logs.resume(id, "apply-request");
  await logs.bindOperation(id, "11111111-1111-4111-8111-111111111111");
  await logs.progress(id, { stage: "create_backup", status: "start" });
  await logs.progress(id, { stage: "create_backup", status: "complete", count: 1 });
  await logs.finish(id, { status: "completed", outcome: "completed", backupId: "managed-backup", counts: { changedSessionFiles: 2 } });

  const entry = logs.get(id);
  assert.equal(entry.status, "completed");
  assert.deepEqual(entry.requestIds, ["prepare-request", "apply-request"]);
  assert.equal(entry.planId, "p".repeat(40));
  assert.equal(entry.backupId, "managed-backup");
  assert.deepEqual(entry.counts, { changedSessionFiles: 2 });
  assert.ok(entry.wallDurationMs >= entry.activeDurationMs);
  assert.equal(entry.stages.find((stage) => stage.stage === "create_backup").count, 1);
});

test("request progress closes changed stages and preserves duplicate running timing", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-request-progress-log-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const logs = new OperationLogService({ directory: root });
  await logs.initialize();
  const id = await logs.begin({ operation: "diagnostics", stage: "scan" });

  await logs.requestProgress(id, { stage: "inspect_index", status: "running", progress: 1 });
  const first = logs.get(id).stages.find((stage) => stage.stage === "inspect_index");
  await new Promise((resolve) => setTimeout(resolve, 10));
  await logs.requestProgress(id, { stage: "inspect_index", status: "running", progress: 2, count: 4 });
  const duplicate = logs.get(id).stages.find((stage) => stage.stage === "inspect_index");
  assert.equal(duplicate.startedAt, first.startedAt);
  assert.equal(duplicate.progress, 2);
  assert.equal(duplicate.count, 4);

  await logs.requestProgress(id, { stage: "inspect_backups", status: "running" });
  await logs.requestProgress(id, { stage: "finish_diagnostics", status: "completed" });
  const entry = logs.get(id);
  assert.equal(entry.stages.find((stage) => stage.stage === "scan").status, "completed");
  assert.equal(entry.stages.find((stage) => stage.stage === "inspect_index").status, "completed");
  assert.equal(entry.stages.find((stage) => stage.stage === "inspect_backups").status, "completed");
  assert.equal(entry.stages.find((stage) => stage.stage === "finish_diagnostics").status, "completed");
  assert.ok(entry.stages.find((stage) => stage.stage === "inspect_index").durationMs >= 5);
});

test("failed Sync preserves safe plan details and a timed validation stage across restart", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-sync-validation-log-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const logs = new OperationLogService({ directory: root });
  await logs.initialize();
  const id = await logs.begin({ operation: "sync", requestId: "prepare" });
  await logs.prepared(id, "plan", { target: { provider: "dal", privateField: "synthetic-private" }, impact: { rolloutFilesToChange: 3, sqliteRowsToChange: 4, lockedRolloutFiles: 1 } });
  await logs.resume(id, "apply");
  await new Promise((resolve) => setTimeout(resolve, 10));
  await logs.finishFromResponse(id, { protocolVersion: 1, requestId: "apply", ok: false, error: {
    schemaVersion: 1, code: "STALE_STATE", message: "synthetic-private", details: { reason: "config", path: "synthetic-private" }
  } });
  const entry = logs.get(id);
  assert.equal(entry.errorReason, "config");
  assert.equal(entry.targetProvider, "dal");
  assert.deepEqual(entry.previewCounts, { rolloutFilesToChange: 3, sqliteRowsToChange: 4, lockedRolloutFiles: 1 });
  assert.deepEqual(entry.counts, {});
  assert.equal(entry.backupId, undefined);
  const stage = entry.stages.find((item) => item.stage === "validate_plan");
  assert.equal(stage.status, "failed");
  assert.ok(stage.durationMs >= 5);
  assert.doesNotMatch(logs.recentJsonLines(), /synthetic-private/);
  const restarted = new OperationLogService({ directory: root });
  await restarted.initialize();
  assert.equal(restarted.get(id).errorReason, "config");
  assert.equal(restarted.get(id).targetProvider, "dal");
});

test("failed operation projects only audited failure diagnostics across restart", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-failure-diagnostic-log-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const logs = new OperationLogService({ directory: root });
  await logs.initialize();
  const id = await logs.begin({ operation: "sync", requestId: "apply" });
  await logs.resume(id, "apply-2");
  await logs.finishFromResponse(id, { protocolVersion: 1, requestId: "apply-2", ok: false, error: {
    code: "INTERNAL_ERROR",
    details: {
      failureStage: "preflight_sqlite",
      causeCode: "ETIMEDOUT",
      path: "C:/private",
      message: "private body",
      stack: "private stack"
    }
  } });
  const entry = logs.get(id);
  assert.equal(entry.failedStage, "preflight_sqlite");
  assert.equal(entry.failureCode, "ETIMEDOUT");
  assert.doesNotMatch(logs.recentJsonLines(), /private|stack|path/);
  const restarted = new OperationLogService({ directory: root });
  await restarted.initialize();
  assert.equal(restarted.get(id).failedStage, "preflight_sqlite");
  assert.equal(restarted.get(id).failureCode, "ETIMEDOUT");

  const unsafe = await logs.begin({ operation: "sync" });
  await logs.finishFromResponse(unsafe, { protocolVersion: 1, requestId: "unsafe", ok: false, error: {
    code: "INTERNAL_ERROR", details: { failureStage: "secret/path", causeCode: "SECRET" }
  } });
  assert.equal(logs.get(unsafe).failedStage, undefined);
  assert.equal(logs.get(unsafe).failureCode, "INTERNAL_ERROR");
  assert.doesNotMatch(logs.recentJsonLines(), /secret\/path|SECRET/);
});

test("Switch logs preserve a validated plan without reconstructing older records, and filter by revision", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-switch-log-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const logs = new OperationLogService({ directory: root });
  await logs.initialize();
  const id = await logs.begin({ operation: "switch", profileId: "default", profileRevision: "r2" });
  await logs.prepared(id, "switch-plan", { target: {
    provider: "relay", previousProvider: "openai", previousRootModel: "gpt-5", model: "relay-model", modelMode: "provider-default"
  } });
  await logs.finishFromResponse(id, { protocolVersion: 1, requestId: "apply", ok: true, result: { outcome: "partial", result: {} } });
  const entry = logs.get(id);
  assert.equal(entry.status, "partial");
  assert.deepEqual(entry.switchPlan, { previousProvider: "openai", targetProvider: "relay", previousRootModel: "gpt-5", targetRootModel: "relay-model", modelMode: "provider-default" });
  assert.equal(logs.list({ schemaVersion: 1, page: 1, pageSize: 10, profileId: "default", profileRevision: "r1" }).total, 0);
  assert.equal(logs.list({ schemaVersion: 1, page: 1, pageSize: 10, profileId: "default", profileRevision: "r2" }).total, 1);
  const legacy = await logs.begin({ operation: "switch", profileId: "default", profileRevision: "r2" });
  await logs.prepared(legacy, "legacy-plan", { target: { provider: "relay" } });
  assert.equal(logs.get(legacy).switchPlan, undefined);
});

test("unknown error reasons and malformed Provider/plan counts are omitted from logs", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-log-projection-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const logs = new OperationLogService({ directory: root });
  await logs.initialize();
  const id = await logs.begin({ operation: "sync" });
  await logs.prepared(id, "plan", { target: { provider: "secret/path" }, impact: { rolloutFilesToChange: -1, sqliteRowsToChange: 4, lockedRolloutFiles: 1 } });
  await logs.finishFromResponse(id, { protocolVersion: 1, requestId: "apply", ok: false, error: { code: "INVALID_INPUT", details: { reason: "secret/path" } } });
  const entry = logs.get(id);
  assert.equal(entry.targetProvider, undefined);
  assert.equal(entry.previewCounts, undefined);
  assert.equal(entry.errorReason, undefined);
  assert.doesNotMatch(logs.recentJsonLines(), /secret\/path/);
});

test("terminal JSONL archives rotate within the configured file count", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-operation-rotation-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const logs = new OperationLogService({ directory: root, maxFileBytes: 256, maxFiles: 5 });
  await logs.initialize();
  for (let index = 0; index < 8; index += 1) {
    const id = await logs.begin({ operation: "diagnostics", profileId: "fixture" });
    await logs.finish(id, { status: "completed", outcome: "completed", warnings: [`entry-${index}`] });
  }
  const archives = (await fs.readdir(root)).filter((name) => /^operations-\d+\.jsonl$/.test(name));
  assert.ok(archives.length <= 5);
  assert.equal(archives.includes("operations-5.jsonl"), false);
});

test("startup closes abandoned active records as interrupted", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-operation-interrupted-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const first = new OperationLogService({ directory: root });
  await first.initialize();
  const id = await first.begin({ operation: "restore", profileId: "named" });
  const restarted = new OperationLogService({ directory: root });
  await restarted.initialize();
  assert.equal(restarted.get(id).status, "interrupted");
  assert.equal(restarted.list({ schemaVersion: 1, page: 1, pageSize: 20, profileId: "named" }).total, 1);
});

test("rotation evicts archived memory but keeps an active confirmation", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-operation-memory-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const options = { directory: root, maxFileBytes: 512, maxFiles: 2 };
  const logs = new OperationLogService(options);
  await logs.initialize();
  const active = await logs.begin({ operation: "switch", profileRevision: "r1" });
  await logs.prepared(active, "pending-plan");
  for (let index = 0; index < 12; index += 1) {
    const id = await logs.begin({ operation: "sync", profileRevision: "r1" });
    await logs.finish(id, { status: "completed", outcome: "completed" });
  }
  const query = { schemaVersion: 1, page: 1, pageSize: 100 };
  assert.equal(logs.list(query).total, 3);
  assert.equal(logs.get(active).status, "awaiting-confirmation");
  await logs.dismiss(active);
  const restarted = new OperationLogService(options);
  await restarted.initialize();
  assert.deepEqual(JSON.parse(JSON.stringify(logs.list(query))), restarted.list(query));
  assert.equal(logs.list(query).total, 2);
});

test("partial logs retain whitelisted nested failure and count fields across restart", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-partial-log-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const logs = new OperationLogService({ directory: root });
  await logs.initialize();
  const id = await logs.begin({ operation: "sync", profileId: "fixture", profileRevision: "r1" });
  await logs.prepared(id, "plan-1");
  await logs.resume(id, "apply-1");
  await logs.progress(id, { stage: "update_sqlite", status: "start" });
  await logs.finishFromResponse(id, { protocolVersion: 1, requestId: "apply-1", ok: true, result: {
    schemaVersion: 1, operationId: "operation-1", operation: "sync", outcome: "partial",
    backup: { backupId: "backup-1" }, warnings: [],
    result: { changedSessionFiles: 2, sqliteRowsUpdated: 0, failedStage: "update_sqlite", failureCode: "SQLITE_BUSY", partialReason: "mutation-failed", retryRecommended: true,
      messages: [{ text: "synthetic-body-excluded" }], unexpectedCounter: 99, codexHome: "C:/synthetic-private" }
  } });
  const entry = logs.get(id);
  assert.equal(entry.status, "partial");
  assert.equal(entry.failedStage, "update_sqlite");
  assert.equal(entry.failureCode, "SQLITE_BUSY");
  assert.equal(entry.partialReason, "mutation-failed");
  assert.equal(entry.retryRecommended, true);
  assert.equal(entry.backupId, "backup-1");
  assert.equal(entry.profileRevision, "r1");
  assert.equal(entry.stages.find((stage) => stage.stage === "update_sqlite").status, "failed");
  assert.deepEqual(entry.counts, { changedSessionFiles: 2, sqliteRowsUpdated: 0 });
  assert.doesNotMatch(logs.recentJsonLines(), /synthetic-body-excluded|synthetic-private|unexpectedCounter/);
  const restarted = new OperationLogService({ directory: root });
  await restarted.initialize();
  assert.deepEqual(restarted.get(id), JSON.parse(JSON.stringify(entry)));
});

test("unknown partial fields cannot put arbitrary text into operation logs", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-partial-fields-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const logs = new OperationLogService({ directory: root });
  await logs.initialize();
  const id = await logs.begin({ operation: "repair" });
  await logs.finishFromResponse(id, { protocolVersion: 1, requestId: "synthetic-request", ok: true, result: {
    outcome: "partial", result: { failedStage: "synthetic sensitive message", failureCode: "synthetic sensitive message", partialReason: "synthetic sensitive message", retryRecommended: "synthetic sensitive message" }
  } });
  assert.equal(logs.get(id).failureCode, "INTERNAL_ERROR");
  assert.doesNotMatch(logs.recentJsonLines(), /synthetic sensitive message/);
});

test("restore failure outcomes never become completed logs", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-restore-outcomes-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const logs = new OperationLogService({ directory: root });
  await logs.initialize();
  for (const outcome of ["failed_rolled_back", "recovery_required", "stale"]) {
    const id = await logs.begin({ operation: "restore" });
    await logs.finishFromResponse(id, { protocolVersion: 1, requestId: "synthetic-request", ok: true, result: { outcome } });
    assert.equal(logs.get(id).status, "failed");
    assert.equal(logs.get(id).outcome, outcome);
  }
});
