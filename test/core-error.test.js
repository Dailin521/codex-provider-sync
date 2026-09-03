import assert from "node:assert/strict";
import test from "node:test";

import { CORE_ERROR_CODES, CoreError, toCoreErrorDto } from "../src/core-error.js";

test("CoreError exposes the canonical serializable DTO shape", () => {
  const error = new CoreError("STALE_STATE", "The prepared state changed.", {
    operationId: "operation-1",
    details: { reason: "rollout", revision: 4 },
    suggestedAction: "Prepare the operation again."
  });

  assert.deepEqual(error.toDto(), {
    code: "STALE_STATE",
    message: "The prepared state changed.",
    severity: "warning",
    retryable: true,
    recoveryRequired: false,
    operationId: "operation-1",
    details: { reason: "rollout", revision: 4 },
    suggestedAction: "Prepare the operation again."
  });
  assert.doesNotThrow(() => JSON.stringify(error.toDto()));
  assert.equal(Object.hasOwn(error.toDto(), "stack"), false);
  assert.equal(Object.hasOwn(error.toDto(), "cause"), false);
});

test("the C1 canonical code set includes expiry, stale state, and unverifiable locks", () => {
  assert.ok(CORE_ERROR_CODES.includes("PLAN_EXPIRED"));
  assert.ok(CORE_ERROR_CODES.includes("STALE_STATE"));
  assert.ok(CORE_ERROR_CODES.includes("LOCK_UNVERIFIABLE"));
  assert.ok(Object.isFrozen(CORE_ERROR_CODES));
});

test("scoped lock errors require an explicit trusted resource scope", () => {
  assert.throws(
    () => new CoreError("OPERATION_BUSY", "busy"),
    /details\.busyScope/
  );
  assert.throws(
    () => new CoreError("OPERATION_BUSY", "busy", { details: { busyScope: "unknown" } }),
    /details\.busyScope/
  );
  assert.throws(
    () => new CoreError("LOCK_UNVERIFIABLE", "uncertain"),
    /details\.lockScope/
  );

  assert.equal(
    new CoreError("OPERATION_BUSY", "busy", { details: { busyScope: "state-db" } }).toDto().details.busyScope,
    "state-db"
  );
  assert.equal(
    new CoreError("LOCK_UNVERIFIABLE", "uncertain", { details: { lockScope: "codex-home" } }).toDto().details.lockScope,
    "codex-home"
  );
});

test("unknown exceptions become INTERNAL_ERROR without copying arbitrary properties", () => {
  const error = new Error("unexpected failure");
  error.code = "EUNEXPECTED";
  error.authToken = "must-not-be-copied";
  error.details = { messageBody: "must-not-be-copied" };

  const dto = toCoreErrorDto(error);
  assert.deepEqual(dto, {
    code: "INTERNAL_ERROR",
    message: "unexpected failure",
    severity: "fatal",
    retryable: false,
    recoveryRequired: false,
    details: { causeCode: "EUNEXPECTED" }
  });
  assert.doesNotMatch(JSON.stringify(dto), /must-not-be-copied/);
});

test("canonical-looking plain errors cannot inject transport details", () => {
  const error = new Error("recovery failed");
  error.code = "RECOVERY_REQUIRED";
  error.operationId = "untrusted-operation";
  error.recoveryRequired = false;
  error.details = {
    authToken: "must-not-be-copied",
    messageBody: "must-not-be-copied"
  };

  const dto = toCoreErrorDto(error);
  assert.deepEqual(dto, {
    code: "RECOVERY_REQUIRED",
    message: "recovery failed",
    severity: "error",
    retryable: true,
    recoveryRequired: true
  });
  assert.doesNotMatch(JSON.stringify(dto), /must-not-be-copied|untrusted-operation/);
});

test("CoreError recursively freezes its normalized details", () => {
  const input = { nested: { values: ["safe"] } };
  const error = new CoreError("STALE_STATE", "state changed", { details: input });

  assert.notEqual(error.details, input);
  assert.ok(Object.isFrozen(error.details));
  assert.ok(Object.isFrozen(error.details.nested));
  assert.ok(Object.isFrozen(error.details.nested.values));
  assert.throws(() => error.details.nested.values.push("changed"), TypeError);
  assert.deepEqual(error.toDto().details, { nested: { values: ["safe"] } });
});

test("typed legacy cancellation and permission errors map without parsing messages", () => {
  const cancelled = new Error("localized cancellation text");
  cancelled.name = "AbortError";
  cancelled.code = "ABORT_ERR";
  assert.equal(toCoreErrorDto(cancelled).code, "OPERATION_CANCELLED");

  const permission = new Error("localized permission text");
  permission.code = "EACCES";
  const dto = toCoreErrorDto(permission);
  assert.equal(dto.code, "PERMISSION_DENIED");
  assert.deepEqual(dto.details, { causeCode: "EACCES" });
});

test("malformed structured errors fail closed as INTERNAL_ERROR during DTO conversion", () => {
  const malformed = new Error("busy without a trusted scope");
  malformed.code = "OPERATION_BUSY";

  const dto = toCoreErrorDto(malformed);
  assert.equal(dto.code, "INTERNAL_ERROR");
  assert.deepEqual(dto.details, { causeCode: "OPERATION_BUSY" });
});
