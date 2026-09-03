import assert from "node:assert/strict";
import test from "node:test";

import {
  isPackagedCdpConnectTimeout,
  shouldRetryPackagedCdpActivation
} from "../e2e/packaged-cdp-retry.mjs";

function playwrightTimeout(message = "browserType.connectOverCDP: Timeout 20000ms exceeded.") {
  const error = new Error(message);
  error.name = "TimeoutError";
  return error;
}

test("packaged CDP activation retries one clean Windows handshake timeout", () => {
  const error = playwrightTimeout();

  assert.equal(isPackagedCdpConnectTimeout(error), true);
  assert.equal(shouldRetryPackagedCdpActivation({
    platform: "win32",
    attempt: 1,
    endpointReady: true,
    browserConnected: false,
    cleanupCompleted: true,
    error
  }), true);
});

test("packaged CDP activation never retries broader launch or application failures", () => {
  const base = {
    platform: "win32",
    attempt: 1,
    endpointReady: true,
    browserConnected: false,
    cleanupCompleted: true,
    error: playwrightTimeout()
  };

  assert.equal(shouldRetryPackagedCdpActivation({ ...base, platform: "linux" }), false);
  assert.equal(shouldRetryPackagedCdpActivation({ ...base, attempt: 2 }), false);
  assert.equal(shouldRetryPackagedCdpActivation({ ...base, endpointReady: false }), false);
  assert.equal(shouldRetryPackagedCdpActivation({ ...base, browserConnected: true }), false);
  assert.equal(shouldRetryPackagedCdpActivation({ ...base, cleanupCompleted: false }), false);
  assert.equal(shouldRetryPackagedCdpActivation({ ...base, error: new Error("endpoint failed") }), false);
  assert.equal(shouldRetryPackagedCdpActivation({
    ...base,
    error: playwrightTimeout("page.waitForLoadState: Timeout 20000ms exceeded.")
  }), false);
});
