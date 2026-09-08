import test from "node:test";
import assert from "node:assert/strict";
import { captureViewport } from "../e2e/viewport-screenshot.mjs";

const timeout = () => Object.assign(new Error("page.screenshot: Timeout 10000ms exceeded."), { name: "TimeoutError" });
test("hidden viewport capture retains the same output and retries one capture timeout", async () => {
  const calls = [];
  const image = Buffer.from("synthetic image");
  const result = await captureViewport({ async screenshot(options) {
    calls.push(options);
    if (calls.length === 1) throw timeout();
    return image;
  } }, { path: "synthetic.png" });
  assert.equal(result, image);
  assert.deepEqual(calls, [{ path: "synthetic.png", timeout: 10_000 }, { path: "synthetic.png", timeout: 20_000 }]);
});

test("viewport capture does not swallow persistent timeouts or unrelated failures", async () => {
  for (const error of [timeout(), new Error("Target closed"), Object.assign(new Error("different timeout"), { name: "TimeoutError" })]) {
    let calls = 0;
    await assert.rejects(captureViewport({ async screenshot() { calls++; throw error; } }, { path: "synthetic.png" }), (actual) => actual === error);
    assert.equal(calls, error.message.startsWith("page.screenshot:") ? 2 : 1);
  }
});
