import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { normalizeRepairSessionIds, normalizeRepairTargets } from "../packages/core/src/application/repair-targets.js";
import { prepareRepair, runRepair } from "../src/service.js";
import { openDatabase } from "../src/sqlite.js";

async function fixture() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "repair-scoped-"));
  const codexHome = path.join(root, ".codex");
  const directory = path.join(codexHome, "sessions", "2026", "09", "04");
  await fs.mkdir(directory, { recursive: true });
  await fs.writeFile(path.join(codexHome, "config.toml"), 'model_provider = "openai"\nmodel = "target-model"\n', "utf8");
  const rollout = async (id, cwd, model) => {
    const file = path.join(directory, `rollout-${id}.jsonl`);
    await fs.writeFile(file, `${JSON.stringify({ type: "session_meta", payload: { id, cwd, model_provider: "openai" } })}\n${JSON.stringify({ type: "turn_context", payload: { model } })}\n${JSON.stringify({ type: "event_msg", payload: { type: "user_message", message: `body-${id}` } })}\n`, "utf8");
    return file;
  };
  const selectedFile = await rollout("selected", "C:\\selected", "old-model");
  const otherFile = await rollout("other", "C:\\other", "other-model");
  const dbPath = path.join(codexHome, "sqlite", "state_5.sqlite");
  await fs.mkdir(path.dirname(dbPath), { recursive: true });
  const db = await openDatabase(dbPath);
  db.exec("CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT, model TEXT, cwd TEXT, has_user_event INTEGER, archived INTEGER DEFAULT 0, updated_at INTEGER)");
  const insert = db.prepare("INSERT INTO threads VALUES (?, 'openai', ?, ?, ?, 0, ?)");
  insert.run("selected", "old-model", "C:\\wrong", 0, 10);
  insert.run("other", "other-model", "C:\\other-wrong", 0, 20);
  insert.run("sqliteOnly", "old-model", "C:\\only", 0, 30);
  db.close();
  return { root, codexHome, selectedFile, otherFile, dbPath };
}

async function rows(dbPath) {
  const db = await openDatabase(dbPath, { readOnly: true });
  try { return db.prepare("SELECT * FROM threads ORDER BY id").all(); } finally { db.close(); }
}

test("scoped repair accepts bounded native session IDs only", () => {
  const targets = normalizeRepairTargets(["models", "cwd"]);
  assert.deepEqual([...normalizeRepairSessionIds(["native_1", "thread-2"], targets)], ["native_1", "thread-2"]);
  assert.throws(() => normalizeRepairSessionIds(["../rollout"], targets), { code: "INVALID_INPUT" });
  assert.throws(() => normalizeRepairSessionIds(["same", "same"], targets), { code: "INVALID_INPUT" });
});

test("workspace roots remains whole-profile only", () => {
  const targets = normalizeRepairTargets(["workspaceRoots"]);
  assert.throws(() => normalizeRepairSessionIds(["native_1"], targets), { code: "INVALID_INPUT" });
});

test("scoped repair changes only the selected rollout and SQLite row, with safe preview and verification", async () => {
  const value = await fixture();
  try {
    const otherBefore = await fs.readFile(value.otherFile, "utf8");
    const plan = await prepareRepair({ codexHome: value.codexHome, targets: ["models", "cwd", "userEvent"], sessionIds: ["selected"] });
    assert.deepEqual(plan.target, { targets: ["models", "cwd", "userEvent"], scope: "selected", sessionIds: ["selected"], model: "target-model" });
    const preview = plan.impact.repairPreview;
    assert.equal(plan.impact.sqliteRowsToChange, 3);
    assert.equal(plan.impact.sqliteModelRowsToChange, 1);
    assert.equal(plan.impact.sqliteCwdRowsToChange, 1);
    assert.equal(plan.impact.sqliteUserEventRowsToChange, 1);
    assert.equal(plan.impact.repairPreviewTotal, 1);
    assert.deepEqual(preview, [{ sessionId: "selected", changes: [
      { target: "models", before: "old-model", after: "target-model" },
      { target: "userEvent", before: "false", after: "true" },
      { target: "cwd", before: "different", after: "rollout-cwd" }
    ] }]);
    const result = await runRepair({ codexHome: value.codexHome, targets: ["models", "cwd", "userEvent"], sessionIds: ["selected"] });
    assert.equal(result.verification.status, "verified");
    assert.equal(await fs.readFile(value.otherFile, "utf8"), otherBefore);
    const [other, selected, sqliteOnly] = await rows(value.dbPath);
    assert.equal(other.model_provider, "openai"); assert.equal(other.model, "other-model"); assert.equal(other.cwd, "C:\\other-wrong"); assert.equal(other.updated_at, 20);
    assert.equal(selected.model_provider, "openai"); assert.equal(selected.model, "target-model"); assert.equal(selected.cwd, "C:\\selected"); assert.equal(selected.has_user_event, 1); assert.equal(selected.updated_at, 10);
    assert.equal(sqliteOnly.model_provider, "openai"); assert.equal(sqliteOnly.model, "old-model"); assert.equal(sqliteOnly.updated_at, 30);
  } finally { await fs.rm(value.root, { recursive: true, force: true }); }
});

test("scoped repair permits SQLite-only IDs and rejects unknown IDs during Prepare", async () => {
  const value = await fixture();
  try {
    const plan = await prepareRepair({ codexHome: value.codexHome, targets: ["models"], sessionIds: ["sqliteOnly"] });
    assert.equal(plan.impact.repairPreview[0].sessionId, "sqliteOnly");
    const result = await runRepair({ codexHome: value.codexHome, targets: ["models"], sessionIds: ["sqliteOnly"] });
    assert.equal(result.verification.status, "verified");
    assert.equal((await rows(value.dbPath)).find((row) => row.id === "sqliteOnly").model, "target-model");
    const backupsBefore = await fs.readdir(path.join(value.codexHome, "backups_state", "provider-sync"));
    await assert.rejects(() => prepareRepair({ codexHome: value.codexHome, targets: ["models"], sessionIds: ["unknown"] }), { code: "INVALID_INPUT" });
    assert.deepEqual(await fs.readdir(path.join(value.codexHome, "backups_state", "provider-sync")), backupsBefore);
  } finally { await fs.rm(value.root, { recursive: true, force: true }); }
});

test("a scoped mutation failure retains its backup and converges on retry", async () => {
  const value = await fixture();
  try {
    const partial = await runRepair({
      codexHome: value.codexHome, targets: ["models"], sessionIds: ["selected"],
      faultInjector({ point }) { if (point === "after_rollout_apply") throw new Error("injected"); }
    });
    assert.equal(partial.partial, true);
    assert.ok(partial.backupDir);
    assert.notEqual(partial.verification.status, "verified");
    const retry = await runRepair({ codexHome: value.codexHome, targets: ["models"], sessionIds: ["selected"] });
    assert.equal(retry.verification.status, "verified");
    assert.equal((await rows(value.dbPath)).find((row) => row.id === "selected").model, "target-model");
  } finally { await fs.rm(value.root, { recursive: true, force: true }); }
});

test("model preview totals stay accurate beyond the bounded preview", async () => {
  const value = await fixture();
  try {
    const db = await openDatabase(value.dbPath);
    const insert = db.prepare("INSERT INTO threads VALUES (?, 'openai', 'old-model', 'C:\\bulk', 0, 0, 0)");
    for (let index = 0; index < 101; index += 1) insert.run(`bulk-${String(index).padStart(3, "0")}`);
    db.close();
    const plan = await prepareRepair({ codexHome: value.codexHome, targets: ["models"] });
    assert.equal(plan.impact.repairPreviewTotal, 104);
    assert.equal(plan.impact.repairPreviewTruncated, true);
    assert.equal(plan.impact.repairPreview.length, 100);
    assert.ok(plan.impact.repairPreview.every((entry) => entry.changes.some((change) => change.target === "models")));
  } finally { await fs.rm(value.root, { recursive: true, force: true }); }
});
