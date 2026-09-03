import assert from "node:assert/strict";
import test from "node:test";

import {
  DEFAULT_PLAN_TTL_MS,
  PLAN_SCHEMA_VERSION,
  PlanLedger
} from "../src/plan-ledger.js";

function fixtureLedger() {
  let now = Date.parse("2026-08-25T00:00:00.000Z");
  let sequence = 0;
  const ledger = new PlanLedger({
    now: () => now,
    randomId: () => `fixture_${String(sequence += 1).padStart(40, "0")}`
  });
  return { ledger, advance: (milliseconds) => { now += milliseconds; } };
}

function scheduledFixtureLedger() {
  let now = Date.parse("2026-08-25T00:00:00.000Z");
  let sequence = 0;
  const timers = new Set();
  const ledger = new PlanLedger({
    now: () => now,
    ttlMs: 100,
    randomId: () => `scheduled_${String(sequence += 1).padStart(40, "0")}`,
    setTimeoutImpl(callback, delay) {
      const timer = {
        callback,
        dueAt: now + delay,
        unrefCalled: false,
        unref() { this.unrefCalled = true; }
      };
      timers.add(timer);
      return timer;
    },
    clearTimeoutImpl(timer) {
      timers.delete(timer);
    }
  });
  return {
    ledger,
    timers,
    advance(milliseconds) {
      now += milliseconds;
      while (true) {
        const timer = [...timers]
          .filter((entry) => entry.dueAt <= now)
          .sort((left, right) => left.dueAt - right.dueAt)[0];
        if (!timer) break;
        timers.delete(timer);
        timer.callback();
      }
    }
  };
}

test("PlanLedger issues immutable schema v1 summaries with a ten-minute TTL", () => {
  const { ledger } = fixtureLedger();
  const summary = ledger.issue("sync", {
    schemaVersion: 999,
    planId: "attacker",
    operation: "restore",
    profile: { id: "default", revision: "profile-r1" },
    target: { provider: "openai" },
    impact: {},
    warnings: []
  }, { trusted: true });

  assert.equal(summary.schemaVersion, PLAN_SCHEMA_VERSION);
  assert.equal(summary.operation, "sync");
  assert.match(summary.planId, /^fixture_/);
  assert.equal(
    Date.parse(summary.expiresAt) - Date.parse(summary.createdAt),
    DEFAULT_PLAN_TTL_MS
  );
  assert.equal(summary.requiresConfirmation, true);
  assert.equal(Object.isFrozen(summary), true);
  assert.equal(Object.isFrozen(summary.profile), true);
});

test("PlanLedger consumes a plan once and fails closed for replay or cross-operation use", () => {
  const { ledger } = fixtureLedger();
  const plan = ledger.issue("switch", { profile: {}, target: {}, impact: {}, warnings: [] }, { marker: 1 });

  assert.equal(ledger.consume({ schemaVersion: 1, planId: plan.planId }, "switch").internal.marker, 1);
  assert.throws(
    () => ledger.consume({ schemaVersion: 1, planId: plan.planId }, "switch"),
    (error) => error?.code === "PLAN_EXPIRED"
  );

  const restore = ledger.issue("restore", { profile: {}, target: {}, impact: {}, warnings: [] }, {});
  assert.throws(
    () => ledger.consume({ schemaVersion: 1, planId: restore.planId }, "sync"),
    (error) => error?.code === "PLAN_EXPIRED"
  );
});

test("PlanLedger expires plans and rejects tampered Apply payloads without consuming valid input", () => {
  const { ledger, advance } = fixtureLedger();
  const plan = ledger.issue("sync", { profile: {}, target: {}, impact: {}, warnings: [] }, {});
  assert.throws(
    () => ledger.consume({ schemaVersion: 1, planId: plan.planId, provider: "attacker" }, "sync"),
    (error) => error?.code === "INVALID_INPUT"
  );
  assert.equal(ledger.size, 1);

  advance(DEFAULT_PLAN_TTL_MS);
  assert.throws(
    () => ledger.consume({ schemaVersion: 1, planId: plan.planId }, "sync"),
    (error) => error?.code === "PLAN_EXPIRED"
  );
  assert.equal(ledger.size, 0);
});

test("PlanLedger autonomously discards abandoned plans without keeping the process alive", () => {
  const { ledger, timers, advance } = scheduledFixtureLedger();
  ledger.issue("sync", { profile: {}, target: {}, impact: {}, warnings: [] }, {});
  assert.equal(ledger.size, 1);
  assert.equal(timers.size, 1);
  assert.equal([...timers][0].unrefCalled, true);

  advance(100);

  assert.equal(ledger.size, 0);
  assert.equal(timers.size, 0);
});
