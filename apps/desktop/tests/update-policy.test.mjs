import assert from "node:assert/strict";
import test from "node:test";

import {
  getDesktopUpdateUnavailableReason,
  supportedUpdateTarget
} from "../dist/main/update-policy.js";

function unavailable(overrides = {}) {
  return getDesktopUpdateUnavailableReason({
    isPackaged: false,
    platform: "win32",
    arch: "x64",
    releaseAuthorized: false,
    configured: false,
    ...overrides
  });
}

test("update policy is unavailable until a supported packaged channel is configured", () => {
  assert.equal(unavailable(), "not-packaged");
  assert.equal(unavailable({ isPackaged: true, platform: "freebsd" }), "unsupported-target");
  assert.equal(unavailable({ isPackaged: true, platform: "darwin", arch: "arm64" }), "not-authorized");
  assert.equal(unavailable({ isPackaged: true, platform: "linux", arch: "x64" }), "not-authorized");
  assert.equal(unavailable({ isPackaged: true, platform: "win32", arch: "arm64" }), "unsupported-target");
  assert.equal(unavailable({ isPackaged: true, releaseAuthorized: true }), "not-configured");
  assert.equal(unavailable({ isPackaged: true, releaseAuthorized: true, configured: true }), null);
  assert.equal(supportedUpdateTarget("darwin", "arm64"), true);
  assert.equal(supportedUpdateTarget("linux", "arm64"), false);
});
