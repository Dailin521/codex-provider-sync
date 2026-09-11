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

test("packaged CDP activation retries one clean Windows or Linux handshake timeout", () => {
  const error = playwrightTimeout();

  assert.equal(isPackagedCdpConnectTimeout(error), true);
  for (const platform of ["win32", "linux"]) {
    assert.equal(shouldRetryPackagedCdpActivation({
      platform,
      attempt: 1,
      endpointReady: true,
      browserConnected: false,
      cleanupCompleted: true,
      error
    }), true);
  }
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

  assert.equal(shouldRetryPackagedCdpActivation({ ...base, platform: "darwin" }), false);
  for (const platform of ["win32", "linux"]) {
    for (const invalid of [
      { attempt: 2 },
      { endpointReady: false },
      { browserConnected: true },
      { cleanupCompleted: false },
      { error: new Error("endpoint failed") },
      { error: playwrightTimeout("page.waitForLoadState: Timeout 20000ms exceeded.") }
    ]) {
      assert.equal(shouldRetryPackagedCdpActivation({ ...base, platform, ...invalid }), false);
    }
  }
});
