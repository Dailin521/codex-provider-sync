import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { OperationLogService } from "../dist/main/operation-log-service.js";
import { validateOperationLogEntry } from "../dist/shared/operation-log-validation.js";

test("transport accepts service logs including revision and partial details", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-log-transport-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const service = new OperationLogService({ directory: root });
  await service.initialize();
  const id = await service.begin({ operation: "sync", profileId: "default", profileRevision: "r1", requestId: "request" });
  assert.equal(validateOperationLogEntry(service.get(id)).profileRevision, "r1");
  await service.finish(id, { status: "partial", outcome: "partial", failedStage: "update_sqlite", failureCode: "SQLITE_BUSY", partialReason: "mutation-failed", retryRecommended: true });
  const entry = service.get(id);
  const details = { ...entry, errorReason: "config", targetProvider: "dal", previewCounts: { rolloutFilesToChange: 1, sqliteRowsToChange: 2, lockedRolloutFiles: 0 } };
  assert.deepEqual(validateOperationLogEntry(details), details);
  for (const patch of [{ errorReason: "unsafe/path" }, { targetProvider: "unsafe/path" }, { previewCounts: { ...details.previewCounts, raw: "secret" } }, { previewCounts: { ...details.previewCounts, sqliteRowsToChange: -1 } }]) {
    assert.throws(() => validateOperationLogEntry({ ...details, ...patch }), /Invalid operation log/);
  }
  const switchPlan = { previousProvider: "openai", targetProvider: "relay", previousRootModel: "openai/gpt-5", targetRootModel: null, modelMode: "provider-default" };
  assert.deepEqual(validateOperationLogEntry({ ...details, switchPlan }).switchPlan, switchPlan);
  for (const targetRootModel of ["C:/secret", "/secret", "https://example.invalid/model", "sk-secret", "api_key=secret", "line\nbreak"]) {
    assert.throws(() => validateOperationLogEntry({ ...details, switchPlan: { ...switchPlan, targetRootModel } }), /Invalid operation log/);
  }
  assert.deepEqual(validateOperationLogEntry(entry), entry);
  for (const patch of [{ unexpected: true }, { retryRecommended: "yes" }, { partialReason: "arbitrary" }, { activeDurationMs: NaN }, { profileRevision: 123 }]) {
    assert.throws(() => validateOperationLogEntry({ ...entry, ...patch }), /Invalid operation log/);
  }
  const restarted = new OperationLogService({ directory: root });
  await restarted.initialize();
  assert.deepEqual(validateOperationLogEntry(restarted.get(id)), JSON.parse(JSON.stringify(entry)));
});
