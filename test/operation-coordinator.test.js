import assert from "node:assert/strict";
import test from "node:test";

import { OperationCoordinator } from "../src/operation-coordinator.js";

function createFakeScheduler(start = 0) {
  let now = start;
  const timers = new Set();
  return {
    now: () => now,
    setTimeoutImpl(callback, delay) {
      const timer = {
        callback,
        dueAt: now + delay,
        cleared: false,
        unrefCalled: false,
        unref() {
          this.unrefCalled = true;
        }
      };
      timers.add(timer);
      return timer;
    },
    clearTimeoutImpl(timer) {
      timer.cleared = true;
      timers.delete(timer);
    },
    advanceTo(value) {
      now = value;
      while (true) {
        const due = [...timers]
          .filter((timer) => !timer.cleared && timer.dueAt <= now)
          .sort((left, right) => left.dueAt - right.dueAt)[0];
        if (!due) break;
        timers.delete(due);
        due.callback();
      }
    },
    activeTimers: () => [...timers].filter((timer) => !timer.cleared)
  };
}

test("Watch yields to a prepared manual intent until the manual Apply ends", async () => {
  let operationSequence = 0;
  const coordinator = new OperationCoordinator({
    randomOperationId: () => `operation-${++operationSequence}`
  });
  const codexHome = "C:\\fixtures\\manual-priority";
  const planId = "manual-plan";
  coordinator.registerManualIntent(codexHome, planId, Date.now() + 5_000, "win32");

  assert.throws(
    () => coordinator.begin(codexHome, "sync", { actor: "watch", platform: "win32" }),
    (error) => error?.code === "OPERATION_BUSY"
      && error?.details?.busyScope === "codex-home"
      && error?.details?.reason === "manual-intent"
  );

  const ticket = coordinator.waitForManualOperation(codexHome, "win32");
  assert.ok(ticket);
  let priorityEnded = false;
  void ticket.promise.then(() => { priorityEnded = true; });

  const manual = coordinator.begin(codexHome, "sync", {
    actor: "manual",
    planId,
    platform: "win32"
  });
  await Promise.resolve();
  assert.equal(priorityEnded, false, "consuming the plan must not wake Watch during manual Apply");

  coordinator.end(codexHome, manual.operationId, "win32");
  await ticket.promise;
  assert.equal(priorityEnded, true);

  const watch = coordinator.begin(codexHome, "sync", { actor: "watch", platform: "win32" });
  coordinator.end(codexHome, watch.operationId, "win32");
});

test("an abandoned manual intent wakes Watch when its plan TTL expires", async () => {
  const coordinator = new OperationCoordinator({ randomOperationId: () => "watch-operation" });
  const codexHome = "/tmp/manual-intent-expiry";
  coordinator.registerManualIntent(codexHome, "expiring-plan", Date.now() + 40, "linux");
  const ticket = coordinator.waitForManualOperation(codexHome, "linux");
  assert.ok(ticket);

  await Promise.race([
    ticket.promise,
    new Promise((_resolve, reject) => setTimeout(
      () => reject(new Error("manual intent expiry did not wake Watch")),
      1_000
    ))
  ]);

  const watch = coordinator.begin(codexHome, "sync", { actor: "watch", platform: "linux" });
  coordinator.end(codexHome, watch.operationId, "linux");
});

test("manual intent expiry is cleaned autonomously without a Watch waiter", () => {
  const scheduler = createFakeScheduler(10_000);
  const coordinator = new OperationCoordinator({
    randomOperationId: () => "watch-operation",
    now: scheduler.now,
    setTimeoutImpl: scheduler.setTimeoutImpl,
    clearTimeoutImpl: scheduler.clearTimeoutImpl
  });
  const codexHome = "/tmp/manual-intent-autonomous-expiry";
  coordinator.registerManualIntent(codexHome, "abandoned-plan", 10_050, "linux");
  assert.equal(scheduler.activeTimers().length, 1);
  assert.equal(scheduler.activeTimers()[0].unrefCalled, true);

  scheduler.advanceTo(10_050);

  assert.equal(scheduler.activeTimers().length, 0);
  assert.equal(coordinator.manualIntents.size, 0);
  const watch = coordinator.begin(codexHome, "sync", { actor: "watch", platform: "linux" });
  coordinator.end(codexHome, watch.operationId, "linux");
});

test("one Home expiry timer advances across multiple manual intents and rearms on release", () => {
  const scheduler = createFakeScheduler(20_000);
  let operationSequence = 0;
  const coordinator = new OperationCoordinator({
    randomOperationId: () => `operation-${++operationSequence}`,
    now: scheduler.now,
    setTimeoutImpl: scheduler.setTimeoutImpl,
    clearTimeoutImpl: scheduler.clearTimeoutImpl
  });
  const codexHome = "/tmp/manual-intent-multiple-expiry";
  coordinator.registerManualIntent(codexHome, "first", 20_010, "linux");
  coordinator.registerManualIntent(codexHome, "second", 20_020, "linux");
  assert.equal(scheduler.activeTimers().length, 1);
  assert.equal(scheduler.activeTimers()[0].dueAt, 20_010);

  scheduler.advanceTo(20_010);
  assert.equal(scheduler.activeTimers().length, 1);
  assert.equal(scheduler.activeTimers()[0].dueAt, 20_020);
  assert.throws(
    () => coordinator.begin(codexHome, "sync", { actor: "watch", platform: "linux" }),
    (error) => error?.details?.reason === "manual-intent"
  );

  coordinator.releaseManualIntent(codexHome, "second", "linux");
  assert.equal(scheduler.activeTimers().length, 0);
  assert.equal(coordinator.manualIntents.size, 0);
  const watch = coordinator.begin(codexHome, "sync", { actor: "watch", platform: "linux" });
  coordinator.end(codexHome, watch.operationId, "linux");
});

test("a consumed manual plan releases its intent even when another operation is active", async () => {
  let operationSequence = 0;
  const coordinator = new OperationCoordinator({
    randomOperationId: () => `operation-${++operationSequence}`
  });
  const codexHome = "/tmp/manual-consumed-busy";
  const active = coordinator.begin(codexHome, "sync", { actor: "watch", platform: "linux" });
  coordinator.registerManualIntent(codexHome, "manual-plan", Date.now() + 5_000, "linux");

  assert.throws(
    () => coordinator.begin(codexHome, "restore", {
      actor: "manual",
      planId: "manual-plan",
      platform: "linux"
    }),
    (error) => error?.code === "OPERATION_BUSY"
  );
  coordinator.end(codexHome, active.operationId, "linux");

  const next = coordinator.begin(codexHome, "sync", { actor: "watch", platform: "linux" });
  coordinator.end(codexHome, next.operationId, "linux");
});
