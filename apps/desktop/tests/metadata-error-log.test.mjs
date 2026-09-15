import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { OperationLogService } from "../dist/main/operation-log-service.js";
import { createPublicCoreErrorDto } from "../../../packages/contracts/dist/index.js";

test("metadata error classification and failure stage survive log restart and export", async t => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "metadata-error-log-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const logs = new OperationLogService({ directory: root });
  await logs.initialize();
  const ids = [];
  for (const code of ["ROLLOUT_METADATA_TOO_LARGE", "ROLLOUT_METADATA_INVALID"]) {
    const id = await logs.begin({ operation: "sync" });
    ids.push([id, code]);
    await logs.finishFromResponse(id, { protocolVersion: 1, requestId: "prepare", ok: false,
      error: createPublicCoreErrorDto(code, { details: { failureStage: "prepare_rollouts", path: "PRIVATE_PATH", body: "PRIVATE_BODY" } })
    });
  }
  const restarted = new OperationLogService({ directory: root });
  await restarted.initialize();
  for (const [id, code] of ids) {
    assert.equal(restarted.get(id).errorCode, code);
    assert.equal(restarted.get(id).failedStage, "prepare_rollouts");
    assert.equal(restarted.get(id).status, "failed");
  }
  assert.doesNotMatch(restarted.recentJsonLines(), /PRIVATE_PATH|PRIVATE_BODY/);
});
