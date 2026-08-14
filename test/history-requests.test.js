import assert from "node:assert/strict";
import test from "node:test";

import { createLatestRequestGate, scheduleDebounced } from "../web/src/history-requests.js";

test("starting a newer History request aborts the older request and rejects its response", () => {
  const gate = createLatestRequestGate();
  const first = gate.begin();
  const second = gate.begin();

  assert.equal(first.signal.aborted, true);
  assert.equal(second.signal.aborted, false);
  assert.equal(gate.isLatest(first.sequence), false);
  assert.equal(gate.isLatest(second.sequence), true);
});

test("History query debounce uses a 300ms delay and can be cancelled", () => {
  const calls = [];
  const timers = {
    setTimeout(callback, delay) { calls.push(["set", delay, callback]); return 17; },
    clearTimeout(id) { calls.push(["clear", id]); }
  };
  const cancel = scheduleDebounced(() => {}, 300, timers);
  cancel();
  assert.deepEqual(calls.slice(0, 2).map((call) => call.slice(0, 2)), [["set", 300], ["clear", 17]]);
});
