import assert from "node:assert/strict";
import test from "node:test";

import {
  CLI_JSON_SCHEMA_VERSION,
  cliJsonExitCode,
  collectCliWarnings,
  createCliFailureEnvelope,
  createCliSuccessEnvelope,
  inferCliSuccessOutcome,
  normalizeCliErrorDto
} from "../src/cli-json.js";

const ENVELOPE_KEYS = [
  "schemaVersion",
  "command",
  "ok",
  "outcome",
  "result",
  "warnings",
  "error"
];

function dto(code, overrides = {}) {
  return {
    code,
    message: `${code} fixture`,
    severity: "error",
    retryable: true,
    recoveryRequired: false,
    ...overrides
  };
}

test("CLI JSON success envelope has the exact schema v1 top-level shape", () => {
  const result = {
    targetProvider: "openai",
    skippedLockedRolloutFiles: ["locked.jsonl"],
    autoPruneWarning: "prune warning"
  };
  const envelope = createCliSuccessEnvelope("sync", result);

  assert.equal(CLI_JSON_SCHEMA_VERSION, 1);
  assert.deepEqual(Object.keys(envelope), ENVELOPE_KEYS);
  assert.equal(envelope.schemaVersion, 1);
  assert.equal(envelope.ok, true);
  assert.equal(envelope.outcome, "partial");
  assert.notEqual(envelope.result, result);
  assert.equal("encryptedContentWarning" in envelope.result, false);
  assert.equal(envelope.result.autoPruneWarning, "Automatic backup cleanup did not complete.");
  assert.deepEqual(envelope.warnings, [
    "Automatic backup cleanup did not complete."
  ]);
  assert.equal(envelope.error, null);
  assert.equal(cliJsonExitCode(envelope), 3);
});

test("CLI JSON success outcome and warning inference is deterministic", () => {
  assert.equal(inferCliSuccessOutcome({ noop: true }), "noop");
  assert.equal(inferCliSuccessOutcome({}), "completed");
  assert.deepEqual(collectCliWarnings({
    warnings: ["one", "one", null],
    backupInventoryWarning: "two",
    modelSync: { warning: "three" }
  }), [
    "The operation completed with a warning.",
    "Backup inventory refresh did not complete.",
    "The selected provider has no default model; the root model was not changed."
  ]);
  assert.equal(cliJsonExitCode(createCliSuccessEnvelope("status", {})), 0);
});

test("CLI JSON exposes only fixed partial convergence fields and uses exit code 3", () => {
  const envelope = createCliSuccessEnvelope("sync", {
    partial: true,
    partialReason: "mutation-failed",
    failedStage: "update_sqlite",
    failureCode: "SQLITE_BUSY",
    retryRecommended: true,
    partialWarning: "secret internal exception text"
  });
  assert.equal(envelope.outcome, "partial");
  assert.equal(cliJsonExitCode(envelope), 3);
  assert.deepEqual(envelope.result, {
    partial: true,
    partialReason: "mutation-failed",
    failedStage: "update_sqlite",
    failureCode: "SQLITE_BUSY",
    retryRecommended: true
  });
  assert.deepEqual(envelope.warnings, [
    "The operation made only part of the requested change. Retry it to converge, or restore the backup manually."
  ]);
  assert.doesNotMatch(JSON.stringify(envelope), /secret internal/);
});

test("CLI JSON preserves changed rollout partial evidence", () => {
  const envelope = createCliSuccessEnvelope("sync", {
    partial: true,
    partialReason: "rollout-changed",
    retryRecommended: true,
    skippedLockedRolloutFiles: ["C:\\private\\locked.jsonl"],
    skippedChangedRolloutFiles: ["/private/changed.jsonl"]
  });
  assert.equal(envelope.outcome, "partial");
  assert.equal(cliJsonExitCode(envelope), 3);
  assert.deepEqual(envelope.result.skippedLockedRolloutFiles, ["locked.jsonl"]);
  assert.deepEqual(envelope.result.skippedChangedRolloutFiles, ["changed.jsonl"]);
  assert.equal(envelope.result.partialReason, "rollout-changed");
  assert.doesNotMatch(JSON.stringify(envelope), /private/);
});

test("CLI JSON failure exit codes follow the frozen matrix", () => {
  const cases = [
    ["INVALID_INPUT", 2, "failed"],
    ["PLAN_EXPIRED", 2, "stale"],
    ["PLAN_STALE", 2, "stale"],
    ["STALE_STATE", 2, "stale"],
    ["PROFILE_CHANGED", 2, "stale"],
    ["STORAGE_CHANGED", 2, "stale"],
    ["SYNC_FAILED_ROLLED_BACK", 1, "failed_rolled_back"],
    ["RECOVERY_REQUIRED", 4, "recovery_required"],
    ["PENDING_TRANSACTION", 4, "recovery_required"],
    ["OPERATION_BUSY", 5, "failed"],
    ["LOCK_UNVERIFIABLE", 5, "failed"],
    ["SQLITE_BUSY", 5, "failed"],
    ["OPERATION_CANCELLED", 130, "cancelled"],
    ["PERMISSION_DENIED", 1, "failed"],
    ["INTERNAL_ERROR", 1, "failed"]
  ];

  for (const [code, exitCode, outcome] of cases) {
    const overrides = code === "RECOVERY_REQUIRED" || code === "PENDING_TRANSACTION"
      ? { recoveryRequired: true }
      : {};
    const envelope = createCliFailureEnvelope("sync", dto(code, overrides));
    assert.deepEqual(Object.keys(envelope), ENVELOPE_KEYS, code);
    assert.equal(envelope.ok, false, code);
    assert.equal(envelope.outcome, outcome, code);
    assert.equal(envelope.result, null, code);
    assert.deepEqual(envelope.warnings, [], code);
    assert.equal(cliJsonExitCode(envelope), exitCode, code);
  }
});

test("CLI JSON hides arbitrary internal error text and optional properties", () => {
  const normalized = normalizeCliErrorDto({
    code: "INTERNAL_ERROR",
    message: "authToken=secret and message body",
    severity: "fatal",
    retryable: false,
    recoveryRequired: false,
    details: { authToken: "secret" },
    operationId: "secret-operation"
  });
  assert.deepEqual(normalized, {
    code: "INTERNAL_ERROR",
    message: "An internal error occurred.",
    severity: "fatal",
    retryable: false,
    recoveryRequired: false
  });
  assert.doesNotMatch(JSON.stringify(normalized), /secret|message body/);
});

test("CLI JSON rejects inherited error-code property names", () => {
  for (const code of ["__proto__", "constructor", "toString"]) {
    const normalized = normalizeCliErrorDto(dto(code));
    assert.deepEqual(normalized, {
      code: "INTERNAL_ERROR",
      message: "An internal error occurred.",
      severity: "fatal",
      retryable: false,
      recoveryRequired: false
    }, code);
  }
});

test("CLI JSON allowlists typed error details and result fields", () => {
  const envelope = createCliFailureEnvelope("sync", dto("OPERATION_BUSY", {
    message: "token=secret-token",
    operationId: "secret-operation",
    suggestedAction: "Use token=secret-token",
    details: {
      busyScope: "codex-home",
      causeCode: "SQLITE_BUSY",
      authToken: "secret-token",
      messageBody: "secret body",
      reason: "secret-reason",
      revision: "secret-revision"
    }
  }));
  assert.equal(envelope.error.message, "Another write operation is using the protected resource.");
  assert.deepEqual(envelope.error.details, {
    busyScope: "codex-home",
    causeCode: "SQLITE_BUSY"
  });
  assert.equal(Object.hasOwn(envelope.error, "operationId"), false);
  assert.equal(Object.hasOwn(envelope.error, "suggestedAction"), false);

  const success = createCliSuccessEnvelope("sync", {
    changedSessionFiles: 1,
    authToken: "secret-token",
    apiKey: "secret-api-key",
    accessKey: "secret-access-key",
    cookie: "secret-cookie",
    prompt: "secret-prompt",
    message: "secret-message",
    text: "secret-text",
    encrypted_content: "secret-content",
    nested: {
      messageBody: "secret body",
      cause: { stack: "secret stack" }
    },
    warnings: ["C:\\private\\path\\token.txt"]
  });
  assert.deepEqual(success.result, {
    changedSessionFiles: 1,
    warnings: ["The operation completed with a warning."]
  });
  assert.doesNotMatch(
    JSON.stringify(success),
    /secret-token|secret-api|secret-access|secret-cookie|secret-prompt|secret-message|secret-text|secret-content|secret body|secret stack|private/
  );
});

test("CLI JSON malformed DTOs fail closed without invoking hostile getters", () => {
  const cyclic = {};
  cyclic.self = cyclic;
  assert.equal(createCliFailureEnvelope("sync", dto("OPERATION_BUSY", {
    details: { busyScope: "codex-home", reason: cyclic }
  })).error.code, "INTERNAL_ERROR");

  const hostile = new Proxy({}, {
    get() {
      throw new Error("secret getter text");
    },
    ownKeys() {
      throw new Error("secret getter text");
    }
  });
  const normalized = normalizeCliErrorDto(hostile);
  assert.equal(normalized.code, "INTERNAL_ERROR");
  assert.doesNotMatch(JSON.stringify(normalized), /secret getter/);
});

test("CLI JSON command result schemas expose only audited fields", () => {
  const status = createCliSuccessEnvelope("status", {
    currentProvider: "openai",
    sqliteAccess: {
      supported: false,
      reason: "windows-wsl-unc",
      message: "secret diagnostic text"
    },
    rolloutCounts: {
      sessions: { openai: 2 },
      archived_sessions: { openai: 1 }
    },
    prompt: "secret prompt",
    message: "secret message"
  });
  assert.deepEqual(status.result, {
    sqliteAccess: { supported: false, reason: "windows-wsl-unc" },
    currentProvider: "openai",
    rolloutCounts: {
      sessions: { openai: 2 },
      archived_sessions: { openai: 1 }
    }
  });

  const restore = createCliSuccessEnvelope("restore", {
    version: 2,
    namespace: "provider-sync",
    codexHome: "C:\\fixture\\.codex",
    targetProvider: "openai",
    sqliteDbFiles: ["state_5.sqlite"],
    changedSessionFiles: 2,
    apiKey: "secret api key",
    metadata: { prompt: "secret prompt" }
  });
  assert.deepEqual(restore.result, {
    version: 2,
    namespace: "provider-sync",
    codexHome: "C:\\fixture\\.codex",
    targetProvider: "openai",
    sqliteDbFiles: ["state_5.sqlite"],
    changedSessionFiles: 2
  });
  assert.doesNotMatch(JSON.stringify({ status, restore }), /secret/);
  assert.throws(() => createCliSuccessEnvelope("unknown-command", {}), /Unsupported CLI JSON/);
});
