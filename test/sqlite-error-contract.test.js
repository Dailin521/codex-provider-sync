import assert from "node:assert/strict";
import test from "node:test";

import { CoreError } from "../src/core-error.js";
import { wrapSqliteBusyError, wrapSqliteMalformedError } from "../src/sqlite-state.js";

function sqliteError({ code = "ERR_SQLITE_ERROR", errcode, message = "driver failure" } = {}) {
  const error = new Error(message);
  error.code = code;
  if (errcode !== undefined) error.errcode = errcode;
  return error;
}

test("SQLite driver primary result codes 5 and 6 map to SQLITE_BUSY", () => {
  for (const errcode of [5, 6, 261, 262]) {
    const error = wrapSqliteBusyError(sqliteError({ errcode }), "write test data");
    assert.ok(error instanceof CoreError);
    assert.equal(error.code, "SQLITE_BUSY");
    assert.equal(error.details.sqlitePrimaryCode, errcode & 0xff);
    assert.equal(error.cause.errcode, errcode);
  }
});

test("SQLite driver primary result codes 11 and 26 map to SQLITE_UNREADABLE", () => {
  for (const errcode of [11, 26, 267]) {
    const error = wrapSqliteMalformedError(sqliteError({ errcode }), "read test data");
    assert.ok(error instanceof CoreError);
    assert.equal(error.code, "SQLITE_UNREADABLE");
    assert.equal(error.details.sqlitePrimaryCode, errcode & 0xff);
  }
});

test("explicit SQLite symbolic codes map without depending on English text", () => {
  assert.equal(
    wrapSqliteBusyError(sqliteError({ code: "SQLITE_LOCKED", message: "localized" }), "write").code,
    "SQLITE_BUSY"
  );
  assert.equal(
    wrapSqliteMalformedError(sqliteError({ code: "SQLITE_NOTADB", message: "localized" }), "read").code,
    "SQLITE_UNREADABLE"
  );
});

test("message-only lookalikes are not treated as stable SQLite classifications", () => {
  const busyLookalike = new Error("database is locked");
  const malformedLookalike = new Error("file is not a database");

  assert.equal(wrapSqliteBusyError(busyLookalike, "write"), busyLookalike);
  assert.equal(wrapSqliteMalformedError(malformedLookalike, "read"), malformedLookalike);
});
