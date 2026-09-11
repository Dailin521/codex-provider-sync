import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { UpdateReminderStore } from "../dist/main/update-preferences.js";
import { showUpdateNotification } from "../dist/main/update-notification.js";
import { validateUpdateReminderInput } from "../dist/shared/update-preferences.js";

test("reminder persists only one version, survives restart, and restores reminders", async t => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-update-reminder-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const store = new UpdateReminderStore(root);
  assert.equal(await store.load(), null);
  await store.save("1.0.2");
  assert.equal(await new UpdateReminderStore(root).load(), "1.0.2");
  await store.save(null);
  assert.equal(await new UpdateReminderStore(root).load(), null);
  assert.deepEqual(await fs.readdir(root), ["update-reminder.json"]);
  for (const contents of ["not json", "x".repeat(1024), '{"schemaVersion":1,"ignoredVersion":"../../oops"}']) {
    await fs.writeFile(path.join(root, "update-reminder.json"), contents);
    assert.equal(await store.load(), null);
  }
  await fs.mkdir(path.join(root, "blocked"));
  await fs.mkdir(path.join(root, "blocked", "update-reminder.json"));
  await assert.rejects(new UpdateReminderStore(path.join(root, "blocked")).save("1.0.2"));
});

test("reminder IPC schema rejects arbitrary paths, keys, types and oversized versions", () => {
  const input = { schemaVersion: 1, version: "1.0.2", ignored: true };
  assert.deepEqual(validateUpdateReminderInput(input), input);
  for (const invalid of [null, [], {}, { ...input, url: "https://example.invalid" },
    { ...input, ignored: "true" }, { ...input, schemaVersion: 2 }, { ...input, version: "a".repeat(65) },
    { ...input, version: "D:\\update.exe" }]) assert.throws(() => validateUpdateReminderInput(invalid));
});

test("native popup offers mute in both languages; Later, Escape and closing never mute", async () => {
  for (const chinese of [true, false]) {
    for (const response of [0, 1, -1]) {
      const ignored = [];
      await showUpdateNotification({ chinese, version: "1.0.2",
        show: async options => {
          assert.equal(options.cancelId, 0);
          assert.equal(options.defaultId, 0);
          assert.equal(options.buttons[1], chinese ? "不再提醒此版本" : "Don't remind me about this version");
          return { response };
        },
        ignore: async version => { ignored.push(version); }
      });
      assert.deepEqual(ignored, response === 1 ? ["1.0.2"] : []);
    }
  }
});

test("native popup reports persistence failure without falsely confirming mute", async () => {
  const dialogs = [];
  await showUpdateNotification({ chinese: true, version: "1.0.2",
    show: async options => { dialogs.push(options); return { response: 1 }; },
    ignore: async () => { throw new Error("private path"); }
  });
  assert.equal(dialogs.length, 2);
  assert.equal(dialogs[1].type, "warning");
  assert.doesNotMatch(JSON.stringify(dialogs), /private path/);
});
