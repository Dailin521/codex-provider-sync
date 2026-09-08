import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createCoreFacade } from "../packages/core/src/index.js";
import { assertCoreMethodInput, assertCoreMethodOutput } from "../packages/contracts/dist/index.js";
import { publicHistoryIntegrity } from "../src/history-integrity-dto.js";
import { openDatabase } from "../src/sqlite.js";
import { prepareRepair, runRepair } from "../src/service.js";
import { readSqliteRepairStats } from "../src/sqlite-state.js";

const profile = { profileId: "default", profileRevision: "r1" };

test("repair transport only accepts bounded native IDs and no global workspace subset", () => {
  assertCoreMethodInput("prepareRepair", { profile, targets: ["models"], sessionIds: ["thread-1"], keepCount: 2 });
  for (const sessionIds of [[], ["../thread"], ["C:\\private"], ["same", "same"], [null], Array.from({ length: 101 }, (_, index) => `id-${index}`)]) {
    assert.throws(() => assertCoreMethodInput("prepareRepair", { profile, targets: ["cwd"], sessionIds }));
  }
  assert.throws(() => assertCoreMethodInput("prepareRepair", { profile, targets: ["workspaceRoots"], sessionIds: ["one"] }));
  assert.throws(() => assertCoreMethodInput("applyRepair", { schemaVersion: 1, planId: "plan", sessionIds: ["one"] }));
});

test("public repair plan and verification survive facade and transport without body/path leakage", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "repair-public-"));
  try {
    await fs.mkdir(path.join(home, "sessions"));
    await fs.writeFile(path.join(home, "config.toml"), 'model = "target-model"\n');
    const file = path.join(home, "sessions", "rollout-one.jsonl");
    const records = [
      { type: "session_meta", payload: { id: "one", model_provider: "openai", cwd: "C:/private-project" } },
      { type: "turn_context", payload: { model: "old-model" } },
      { type: "event_msg", payload: { type: "user_message", message: "synthetic-secret-body" } }
    ];
    await fs.writeFile(file, records.map(JSON.stringify).join("\n") + "\n");
    const facade = createCoreFacade({ resolveProfile: async () => ({ id: "default", revision: "r1", codexHome: home }) });
    const input = { profile, targets: ["models"], sessionIds: ["one"] };
    const plan = await facade.prepareRepair(input);
    assertCoreMethodOutput("prepareRepair", plan);
    assert.equal(plan.target.scope, "selected");
    assert.deepEqual(plan.target.sessionIds, ["one"]);
    assert.equal(plan.impact.repairPreview[0].sessionId, "one");
    assert.equal(plan.impact.repairPreview[0].changes[0].after, "target-model");
    assert.doesNotMatch(JSON.stringify(plan), /synthetic-secret-body|private-project/);
    const result = await facade.applyRepair({ schemaVersion: 1, planId: plan.planId });
    assertCoreMethodOutput("applyRepair", result);
    assert.equal(result.outcome, "completed");
    assert.equal(result.result.verification.status, "verified");
    assert.ok(result.backup.backupId);
    assert.doesNotMatch(JSON.stringify(result), /synthetic-secret-body|private-project/);
    const diagnostics = await facade.getDiagnostics({ profile });
    assertCoreMethodOutput("getDiagnostics", diagnostics);
    assert.equal(diagnostics.historyIntegrity.displayIndex.status, "unsupported");
    assert.doesNotMatch(JSON.stringify(diagnostics), /synthetic-secret-body|private-project/);
    assert.throws(() => assertCoreMethodOutput("getDiagnostics", { ...diagnostics, historyIntegrity: { ...diagnostics.historyIntegrity, body: "forbidden" } }));
    assert.throws(() => assertCoreMethodOutput("getDiagnostics", { ...diagnostics, historyIntegrity: { ...diagnostics.historyIntegrity, issues: [{ code: "json-corrupt", sessionId: "../private", scope: "sessions", line: 1 }] } }));
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history integrity projection drops injected paths, codes and arbitrary payload fields", () => {
  const projected = publicHistoryIntegrity({
    outcome: "not-a-real-outcome",
    counts: { filesScanned: 1, body: "forbidden" },
    issues: [{ code: "token-value", sessionId: "C:/private", scope: "credentials", line: -1, body: "forbidden" }],
    displayIndex: { status: "healthy", token: "forbidden" }
  });
  assert.equal(projected.outcome, "inconclusive");
  assert.equal(projected.displayIndex.status, "unsupported");
  assert.equal(projected.issues[0].code, "unverified");
  assert.equal(projected.issues[0].sessionId, null);
  assert.doesNotMatch(JSON.stringify(projected), /forbidden|private|token-value|credentials/);
});

test("global repair exposes distinct chats, field counts and safe workspace categories without writes", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "repair-counts-public-"));
  try {
    await fs.mkdir(path.join(home, "sessions"));
    await fs.mkdir(path.join(home, "sqlite"));
    await fs.writeFile(path.join(home, "config.toml"), 'model = "current-model"\n');
    const body = [
      { type: "session_meta", payload: { id: "one", model_provider: "openai", cwd: "C:/private-project" } },
      { type: "event_msg", payload: { type: "user_message", message: "PRIVATE_BODY" } }
    ].map(JSON.stringify).join("\n") + "\n";
    const file = path.join(home, "sessions", "rollout-one.jsonl");
    await fs.writeFile(file, body);
    const settingsPath = path.join(home, ".codex-global-state.json");
    const settings = JSON.stringify({ "electron-saved-workspace-roots": ["C:/private-project"], "project-order": ["C:/private-project"], "active-workspace-roots": ["C:/private-project"] });
    await fs.writeFile(settingsPath, settings);
    const dbPath = path.join(home, "sqlite", "state_5.sqlite");
    const db = await openDatabase(dbPath);
    db.exec("CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT, model TEXT, cwd TEXT, has_user_event INTEGER, archived INTEGER DEFAULT 0)");
    db.prepare("INSERT INTO threads VALUES ('one', 'openai', 'old', 'C:/wrong', 0, 0)").run();
    db.close();
    const facade = createCoreFacade({ resolveProfile: () => ({ id: "default", revision: "r1", codexHome: home }) });
    const plan = await facade.prepareRepair({ profile, targets: ["workspaceRoots", "userEvent"] });
    assertCoreMethodOutput("prepareRepair", plan);
    assert.ok(plan.target.targets.includes("cwd"));
    assert.equal(plan.impact.sqliteRowsToChange, 2);
    assert.equal(plan.impact.sqliteCwdRowsToChange, 1);
    assert.equal(plan.impact.sqliteUserEventRowsToChange, 1);
    assert.equal(plan.impact.sqliteModelRowsToChange, undefined);
    assert.equal(plan.impact.repairPreviewTotal, 1);
    assert.equal(plan.impact.repairPreview[0].sessionId, "one");
    assert.ok(plan.impact.workspaceSettingsChangeKinds.includes("settingsBackup"));
    assert.equal(plan.impact.workspaceRootsToChange, plan.impact.workspaceSettingsChangeKinds.length);
    assert.doesNotMatch(JSON.stringify(plan), /private-project|PRIVATE_BODY|C:\/wrong/);
    assert.equal(await fs.readFile(file, "utf8"), body);
    assert.equal(await fs.readFile(settingsPath, "utf8"), settings);
    assert.equal((await facade.listBackups({ profile })).backups.length, 0);
    await assert.rejects(fs.access(`${settingsPath}.bak`), { code: "ENOENT" });
  } finally { await fs.rm(home, { recursive: true, force: true }); }
});

test("repair preview includes SQLite-only differences beyond 100 without reading message columns", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "repair-preview-bound-"));
  try {
    await fs.mkdir(path.join(home, "sqlite"));
    await fs.writeFile(path.join(home, "config.toml"), 'model = "target-model"\n');
    const dbPath = path.join(home, "sqlite", "state_5.sqlite");
    const db = await openDatabase(dbPath);
    db.exec("CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT DEFAULT 'openai', model TEXT, cwd TEXT, archived INTEGER DEFAULT 0, first_user_message TEXT)");
    const insert = db.prepare("INSERT INTO threads (id, model, cwd, first_user_message) VALUES (?, ?, ?, ?)");
    for (let index = 0; index < 150; index++) insert.run(`id-${String(index).padStart(3, "0")}`, "old-model", "/old", "synthetic-body-never-preview");
    db.close();
    const stats = await readSqliteRepairStats({ stateDbLocation: { path: dbPath, source: "fixture" } }, { targetModel: "target-model" });
    assert.ok(stats.previewRows.every((row) => !Object.hasOwn(row, "first_user_message")));
    const plan = await prepareRepair({ codexHome: home, targets: ["models"] });
    assert.equal(plan.impact.repairPreview.length, 100);
    assert.equal(plan.impact.repairPreviewTotal, 150);
    assert.equal(plan.impact.repairPreviewTruncated, true);
    assert.doesNotMatch(JSON.stringify(plan), /synthetic-body-never-preview/);
    const cwdMap = new Map(Array.from({ length: 150 }, (_, index) => [`id-${String(index).padStart(3, "0")}`, index === 149 ? "/new" : "/old"]));
    const cwdStats = await readSqliteRepairStats({ stateDbLocation: { path: dbPath, source: "fixture" } }, { threadCwdById: cwdMap });
    assert.equal(cwdStats.cwdRowsNeedingRepair, 1);
    assert.equal(cwdStats.previewRows[0].id, "id-149");
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("post-mutation identity change remains partial and retains backup evidence", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "repair-verify-failure-"));
  try {
    await fs.mkdir(path.join(home, "sessions"));
    await fs.writeFile(path.join(home, "config.toml"), 'model = "target-model"\n');
    const file = path.join(home, "sessions", "rollout-one.jsonl");
    const original = [
      { type: "session_meta", payload: { id: "one", model_provider: "openai" } },
      { type: "turn_context", payload: { model: "old-model" } }
    ].map(JSON.stringify).join("\n") + "\n";
    await fs.writeFile(file, original);
    const result = await runRepair({ codexHome: home, targets: ["models"], sessionIds: ["one"], faultInjector: async (event) => {
      if (event.point === "after_rollout_apply") await fs.writeFile(file, original.replace('"id":"one"', '"id":"changed-by-writer"'));
    } });
    assert.equal(result.partial, true);
    assert.equal(result.verification.status, "unavailable");
    assert.ok(result.backupDir);
    assert.equal((await fs.stat(result.backupDir)).isDirectory(), true);
    // Restore deliberately must not overwrite a different session identity.
    // This scenario proves honest verification, not permission to restore it.
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("partial repair can be explicitly restored without automatic rollback", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "repair-manual-restore-"));
  try {
    await fs.mkdir(path.join(home, "sessions"));
    await fs.writeFile(path.join(home, "config.toml"), 'model = "target-model"\n');
    const file = path.join(home, "sessions", "rollout-one.jsonl");
    const original = [
      { type: "session_meta", payload: { id: "one", model_provider: "openai" } },
      { type: "turn_context", payload: { model: "old-model" } }
    ].map(JSON.stringify).join("\n") + "\n";
    await fs.writeFile(file, original);
    const result = await runRepair({ codexHome: home, targets: ["models"], sessionIds: ["one"], faultInjector: (event) => {
      if (event.point === "after_rollout_apply") throw new Error("synthetic post-mutation failure");
    } });
    assert.equal(result.partial, true);
    assert.notEqual(await fs.readFile(file, "utf8"), original);
    const facade = createCoreFacade({ resolveProfile: async () => ({ id: "default", revision: "r1", codexHome: home }) });
    const plan = await facade.prepareRestore({ profile, backupId: path.basename(result.backupDir), restoreConfig: false, restoreDatabase: false, restoreSessions: true });
    const restored = await facade.applyRestore({ schemaVersion: 1, planId: plan.planId });
    assert.equal(restored.outcome, "completed");
    assert.equal(await fs.readFile(file, "utf8"), original);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});
