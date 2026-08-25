import { randomBytes } from "node:crypto";

import { CoreError } from "./core-error.js";

export const PLAN_SCHEMA_VERSION = 1;
export const DEFAULT_PLAN_TTL_MS = 10 * 60 * 1000;

const OPERATIONS = new Set(["sync", "switch", "restore"]);

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function deepFreeze(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
  for (const entry of Object.values(value)) deepFreeze(entry);
  return Object.freeze(value);
}

function invalidPlanInput(message) {
  return new CoreError("INVALID_INPUT", message);
}

function unavailablePlan() {
  return new CoreError(
    "PLAN_EXPIRED",
    "The prepared operation is no longer available. Prepare and confirm it again."
  );
}

export class PlanLedger {
  constructor({
    now = () => Date.now(),
    randomId = () => randomBytes(32).toString("base64url"),
    ttlMs = DEFAULT_PLAN_TTL_MS
  } = {}) {
    if (typeof now !== "function" || typeof randomId !== "function") {
      throw new TypeError("PlanLedger clock and random id source must be functions.");
    }
    if (!Number.isInteger(ttlMs) || ttlMs <= 0) {
      throw new TypeError("PlanLedger ttlMs must be a positive integer.");
    }
    this.now = now;
    this.randomId = randomId;
    this.ttlMs = ttlMs;
    this.entries = new Map();
  }

  issue(operation, summary, internal) {
    if (!OPERATIONS.has(operation)) {
      throw new TypeError(`Unsupported plan operation: ${String(operation)}`);
    }
    const createdAtMs = this.now();
    const planId = this.randomId();
    if (typeof planId !== "string" || !/^[A-Za-z0-9_-]{32,128}$/.test(planId)) {
      throw new TypeError("Plan id source returned an invalid opaque id.");
    }
    if (this.entries.has(planId)) {
      throw new CoreError("INTERNAL_ERROR", "The plan id source produced a duplicate id.");
    }
    const issuedSummary = deepFreeze({
      ...cloneJson(summary),
      schemaVersion: PLAN_SCHEMA_VERSION,
      planId,
      operation,
      createdAt: new Date(createdAtMs).toISOString(),
      expiresAt: new Date(createdAtMs + this.ttlMs).toISOString(),
      requiresConfirmation: true
    });
    this.entries.set(planId, {
      operation,
      expiresAtMs: createdAtMs + this.ttlMs,
      summary: issuedSummary,
      internal
    });
    return issuedSummary;
  }

  consume(input, expectedOperation) {
    if (!input || typeof input !== "object" || Array.isArray(input)) {
      throw invalidPlanInput("Apply requires a schemaVersion and planId object.");
    }
    const keys = Object.keys(input).sort();
    if (keys.length !== 2 || keys[0] !== "planId" || keys[1] !== "schemaVersion") {
      throw invalidPlanInput("Apply accepts only schemaVersion and planId.");
    }
    if (input.schemaVersion !== PLAN_SCHEMA_VERSION
        || typeof input.planId !== "string"
        || !/^[A-Za-z0-9_-]{32,128}$/.test(input.planId)) {
      throw invalidPlanInput("Apply requires schemaVersion 1 and a valid opaque planId.");
    }
    if (!OPERATIONS.has(expectedOperation)) {
      throw new TypeError(`Unsupported plan operation: ${String(expectedOperation)}`);
    }
    const entry = this.entries.get(input.planId);
    if (!entry || entry.operation !== expectedOperation) throw unavailablePlan();

    // Consumption is deliberately atomic and happens before waiting for any
    // filesystem lock. A failed, stale, busy, or cancelled Apply cannot replay
    // an old confirmation.
    this.entries.delete(input.planId);
    if (this.now() >= entry.expiresAtMs) throw unavailablePlan();
    return entry;
  }

  get size() {
    return this.entries.size;
  }
}
