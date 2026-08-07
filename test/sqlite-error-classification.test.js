import test from "node:test";
import assert from "node:assert/strict";

import { wrapSqliteBusyError } from "../src/sqlite-state.js";

test("filesystem EBUSY is not classified as SQLite busy", () => {
  const error = Object.assign(
    new Error("EBUSY: resource busy or locked, open 'transaction-journal.jsonl'"),
    { code: "EBUSY" }
  );

  assert.equal(wrapSqliteBusyError(error, "update session provider metadata"), error);
});

test("explicit SQLite busy and locked codes retain the existing guidance", () => {
  for (const code of ["SQLITE_BUSY", "SQLITE_BUSY_SNAPSHOT", "SQLITE_LOCKED", "SQLITE_LOCKED_SHAREDCACHE"]) {
    const error = Object.assign(new Error("SQLite write failed"), { code });
    const wrapped = wrapSqliteBusyError(error, "update session provider metadata");

    assert.notEqual(wrapped, error);
    assert.match(wrapped.message, /state_5\.sqlite is currently in use/);
  }
});

test("SQLite lock messages retain the existing guidance when a driver omits the SQLite code", () => {
  for (const message of ["database is locked", "database table is locked: threads"]) {
    const error = new Error(message);
    const wrapped = wrapSqliteBusyError(error, "update session provider metadata");

    assert.notEqual(wrapped, error);
    assert.match(wrapped.message, /state_5\.sqlite is currently in use/);
  }
});
