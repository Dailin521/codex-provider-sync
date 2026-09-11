import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { annotateFailureStage, withFailureStage, CoreError, toCoreErrorDto } from "../src/core-error.js";
import { sanitizePublicCoreErrorDto as sanitizeCoreError } from "../packages/contracts/dist/index.js";
import { prepareSync, runSync } from "../src/service.js";
import { runCli } from "../src/cli.js";

delete process.env.CODEX_SQLITE_HOME;
const syntheticError = () => Object.assign(new Error("PRIVATE_SYNTHETIC_DETAIL"), { code: "EIO" });

test("stage annotation preserves the first boundary, typed details and error classification", async () => {
  const error = syntheticError();
  await assert.rejects(withFailureStage("prepare_status", () => withFailureStage("prepare_config", () => { throw error; })), (caught) => {
    assert.equal(caught, error);
    assert.deepEqual(toCoreErrorDto(caught).details, { failureStage: "prepare_config", causeCode: "EIO" });
    return true;
  });
  const typed = new CoreError("STALE_STATE", "changed", { details: { reason: "config" } });
  assert.deepEqual(toCoreErrorDto(typed), typed.toDto(), "no redundant canonical cause code");
  annotateFailureStage(typed, "validate_plan");
  assert.equal(typed.code, "STALE_STATE");
  assert.ok(Object.isFrozen(typed.details));
  assert.deepEqual(typed.details, { reason: "config", failureStage: "validate_plan" });
  const wrapped = new CoreError("BACKUP_FAILED", "backup failed", { cause: syntheticError() });
  annotateFailureStage(wrapped, "create_backup");
  assert.equal(toCoreErrorDto(wrapped).details.causeCode, "EIO");
  for (const [code, name, expected] of [["EACCES", "Error", "PERMISSION_DENIED"], ["ABORT_ERR", "AbortError", "OPERATION_CANCELLED"]]) {
    const failure = Object.assign(new Error("synthetic"), { code, name });
    assert.equal(toCoreErrorDto(annotateFailureStage(failure, "create_backup")).code, expected);
  }
});

test("diagnostic annotation cannot replace frozen errors or execute details getters", () => {
  const frozen = Object.freeze(syntheticError());
  assert.equal(annotateFailureStage(frozen, "create_backup"), frozen);
  assert.equal(toCoreErrorDto(frozen).details.causeCode, "EIO");
  const error = syntheticError();
  error.details = Object.defineProperty({}, "secret", { enumerable: true, get() { throw new Error("getter must not run"); } });
  assert.equal(annotateFailureStage(error, "prepare_config"), error);
  assert.deepEqual(error.details, { failureStage: "prepare_config", causeCode: "EIO" });
  const unrecognized = syntheticError();
  assert.equal(annotateFailureStage(unrecognized, "PRIVATE_SYNTHETIC_DETAIL"), unrecognized);
  assert.equal(unrecognized.details, undefined);
});

async function withHome(run) {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "cps-stage-test-"));
  try {
    await fs.mkdir(path.join(home, "sessions"));
    await fs.writeFile(path.join(home, "config.toml"), 'model_provider = "openai"\n');
    const file = path.join(home, "sessions", "rollout-test.jsonl");
    const content = JSON.stringify({ type: "session_meta", payload: { id: "fixture", model_provider: "prov_a" } }) + '\n{"type":"event_msg","payload":{"type":"user_message","message":"synthetic body"}}\n';
    await fs.writeFile(file, content);
    await run({ home, file, content });
  } finally { await fs.rm(home, { recursive: true, force: true }); }
}

test("real Sync Prepare reports config read failure without exposing exception text", async () => withHome(async ({ home }) => {
  const original = fs.readFile;
  fs.readFile = async (file, ...args) => {
    if (file === path.join(home, "config.toml")) throw syntheticError();
    return original(file, ...args);
  };
  try {
    await assert.rejects(prepareSync({ codexHome: home }), (error) => {
      const dto = sanitizeCoreError(error);
      assert.equal(dto.code, "INTERNAL_ERROR");
      assert.deepEqual(dto.details, { failureStage: "prepare_config", causeCode: "EIO" });
      assert.doesNotMatch(JSON.stringify(dto), /PRIVATE_SYNTHETIC_DETAIL/);
      return true;
    });
  } finally { fs.readFile = original; }
}));

test("real Sync failure before backup retains stage and operation ID with zero mutation", async () => withHome(async ({ home, file, content }) => {
  await assert.rejects(runSync({ codexHome: home, faultInjector: ({ point }) => {
    if (point === "before_backup") throw syntheticError();
  } }), (error) => {
    const dto = sanitizeCoreError(error);
    assert.deepEqual(dto.details, { failureStage: "create_backup", causeCode: "EIO" });
    assert.match(dto.operationId, /^[0-9a-f-]{36}$/);
    return true;
  });
  assert.equal(await fs.readFile(file, "utf8"), content);
  assert.equal(await fs.readFile(path.join(home, "config.toml"), "utf8"), 'model_provider = "openai"\n');
  await assert.rejects(fs.stat(path.join(home, "backups_state")), { code: "ENOENT" });
}));

test("real Sync mutation failure keeps retryable partial and the original failure code", async () => withHome(async ({ home }) => {
  const result = await runSync({ codexHome: home, faultInjector: ({ point }) => {
    if (point === "after_rollout_apply") throw syntheticError();
  } });
  assert.equal(result.partial, true);
  assert.equal(result.failedStage, "rewrite_rollout_files");
  assert.equal(result.failureCode, "EIO");
  assert.ok(result.backupDir);
  const retry = await runSync({ codexHome: home });
  assert.equal(retry.partial, false);
}));

test("failed diagnostic stderr cannot replace the original JSON failure", async () => {
  let stdout = "";
  const code = await runCli(["sync", "--json"], {
    stdout: { write(value) { stdout += value; } },
    stderr: { write() { throw new Error("broken stderr"); } },
    loadCoreImpl: async () => ({ toCoreErrorDto, readConfigText: async () => "", readRootModelFromConfigText: () => null,
      runSync: async () => { throw annotateFailureStage(syntheticError(), "prepare_config"); } })
  });
  assert.equal(code, 1);
  const envelope = JSON.parse(stdout);
  assert.deepEqual(envelope.error.details, { failureStage: "prepare_config", causeCode: "EIO" });
  assert.equal(stdout.trim().split("\n").length, 1);
});
