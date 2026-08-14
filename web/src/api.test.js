import assert from "node:assert/strict";
import test from "node:test";

import { ProfileRevisionError, toRequestError } from "./api.js";

test("turns profile revision conflicts into a typed request error", () => {
  const error = toRequestError({
    code: "PROFILE_CHANGED",
    error: "Profile changed",
    profile: { id: "work", revision: "rev-2" }
  }, 409, "fallback");

  assert.ok(error instanceof ProfileRevisionError);
  assert.equal(error.code, "PROFILE_CHANGED");
  assert.equal(error.profile.revision, "rev-2");
});

test("turns storage revision conflicts into the same refresh-required error", () => {
  const error = toRequestError({
    code: "STORAGE_CHANGED",
    error: "SQLite target changed",
    profile: { id: "work", revision: "rev-1" }
  }, 409, "fallback");

  assert.ok(error instanceof ProfileRevisionError);
  assert.equal(error.code, "STORAGE_CHANGED");
});
