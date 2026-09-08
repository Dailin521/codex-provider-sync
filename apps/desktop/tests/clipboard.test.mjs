import assert from "node:assert/strict";
import test from "node:test";
import { validateDesktopClipboardInput, validateDesktopClipboardResult } from "../dist/shared/clipboard.js";

test("clipboard schema preserves exact Unicode text and rejects false acknowledgements", () => {
  const value = { schemaVersion: 1, text: "会话 ID\nC:\\合成路径\\😀\tend" };
  assert.deepEqual(validateDesktopClipboardInput(value), value);
  assert.notEqual(validateDesktopClipboardInput(value), value);
  assert.deepEqual(validateDesktopClipboardResult({ copied: true }), { copied: true });
  assert.deepEqual(validateDesktopClipboardResult({ copied: false }), { copied: false });
  for (const bad of [null, [], {}, true, { copied: "true" }, { copied: true, text: "echo" }]) {
    assert.throws(() => validateDesktopClipboardResult(bad), /Invalid clipboard response/);
  }
});
