import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test, { afterEach } from "node:test";

import { applyRestore, applySync, prepareRestore, prepareSync } from "../src/service.js";
import { openDatabase } from "../src/sqlite.js";

// These tests deliberately use D:\\Temp rather than a user profile.  Besides
// exercising the Windows path rules, keeping every fixture beneath one checked
// root makes the cleanup safe when a test fails halfway through Apply.
const TEMP_ROOT = process.platform === "win32" ? path.resolve("D:/Temp") : path.resolve(os.tmpdir());
const cleanups = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

function assertTemporaryChild(value) {
  const relative = path.relative(TEMP_ROOT, value);
  assert.notEqual(relative, "", "a fixture must not use D:/Temp itself");
  assert.ok(!relative.startsWith("..") && !path.isAbsolute(relative), `fixture escaped D:/Temp: ${value}`);
}

async function temporaryRoot() {
  await fs.mkdir(TEMP_ROOT, { recursive: true });
  const root = await fs.mkdtemp(path.join(TEMP_ROOT, "provider-paginated-"));
  assertTemporaryChild(root);
  cleanups.push(async () => {
    assertTemporaryChild(root);
    await fs.rm(root, { recursive: true, force: true });
  });
  return root;
}

function header({ id, provider, ordinal = 0, historyMode = "paginated", extra = {} }) {
  return JSON.stringify({
    type: "session_meta",
    ordinal,
    payload: { id, model_provider: provider, history_mode: historyMode, fixture: "keep", ...extra }
  });
}

function contents(spec) {
  return `${header(spec)}\n${JSON.stringify({
    type: "event_msg",
    payload: { message: `body:${spec.name}`, model_provider: "body-must-not-change", nested: { keep: true } }
  })}\n`;
}

function providerOf(fileContents) {
  return JSON.parse(String(fileContents).split(/\r?\n/, 1)[0]).payload.model_provider;
}

function nonProviderHeader(fileContents) {
  const parsed = JSON.parse(String(fileContents).split(/\r?\n/, 1)[0]);
  delete parsed.payload.model_provider;
  return parsed;
}

function bodyOf(fileContents) {
  const bytes = Buffer.isBuffer(fileContents) ? fileContents : Buffer.from(fileContents);
  return bytes.subarray(bytes.indexOf(0x0a) + 1);
}

function hash(value) {
  return createHash("sha256").update(value).digest("hex");
}

async function paginatedFixture({ configProvider = "prov_a", files, rows }) {
  const root = await temporaryRoot();
  const home = path.join(root, "home");
  const dbPath = path.join(home, "sqlite", "state_5.sqlite");
  await Promise.all([
    fs.mkdir(path.join(home, "sessions"), { recursive: true }),
    fs.mkdir(path.join(home, "archived_sessions"), { recursive: true }),
    fs.mkdir(path.dirname(dbPath), { recursive: true })
  ]);
  await fs.writeFile(path.join(home, "config.toml"), [
    `model_provider = "${configProvider}"`,
    "[model_providers.custom]",
    'name = "Custom"',
    "[model_providers.prov_a]",
    'name = "Provider A"',
    "[model_providers.provider_long]",
    'name = "Long Provider"',
    ""
  ].join("\n"));

  const paths = new Map();
  for (const spec of files) {
    const directory = spec.archived ? "archived_sessions" : "sessions";
    const file = path.join(home, directory, spec.name);
    await fs.mkdir(path.dirname(file), { recursive: true });
    await fs.writeFile(file, contents(spec));
    paths.set(spec.name, file);
  }

  const db = await openDatabase(dbPath);
  try {
    db.exec("CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT, rollout_path TEXT, archived INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL)");
    const insert = db.prepare("INSERT INTO threads(id, model_provider, rollout_path, archived, updated_at) VALUES (?, ?, ?, ?, ?)");
    for (const row of rows) insert.run(row.id, row.provider, row.rolloutPath ?? paths.get(row.file) ?? null, row.archived ?? 0, row.updatedAt ?? 701);
  } finally {
    db.close();
  }

  return {
    home,
    dbPath,
    file: name => paths.get(name),
    async read(name) { return fs.readFile(paths.get(name)); },
    async rows() {
      const database = await openDatabase(dbPath);
      try {
        return database.prepare("SELECT id, model_provider, rollout_path, updated_at FROM threads ORDER BY id").all();
      } finally {
        database.close();
      }
    },
    async provider(id) {
      return (await this.rows()).find(row => row.id === id)?.model_provider;
    }
  };
}

const apply = plan => applySync({ schemaVersion: 1, planId: plan.planId });

async function mutateRows(dbPath, mutate) {
  const database = await openDatabase(dbPath);
  try { mutate(database); }
  finally { database.close(); }
}

function twoFileGroup(id = "thread-paged", providers = ["openai", "custom"]) {
  return [
    { name: `rollout-${id}-active.jsonl`, id, provider: providers[0], ordinal: 9 },
    { name: `rollout-${id}-archived.jsonl`, id, provider: providers[1], ordinal: 9, archived: true }
  ];
}

test("one paginated thread may own active and archived rollouts, including repeated ordinals, and Restore returns the whole group", async () => {
  const files = twoFileGroup();
  const value = await paginatedFixture({
    files,
    rows: [{ id: "thread-paged", provider: "openai", file: files[0].name, updatedAt: 817 }]
  });
  const before = await Promise.all(files.map(async item => ({
    bytes: await value.read(item.name),
    stat: await fs.stat(value.file(item.name), { bigint: true })
  })));

  const plan = await prepareSync({ codexHome: value.home });
  assert.equal(plan.impact.rolloutFilesToChange, 2);
  assert.equal(plan.impact.sqliteRowsToChange, 1);
  const synced = await apply(plan);
  assert.equal(synced.outcome, "completed");
  assert.equal(synced.result.changedSessionFiles, 2);
  assert.equal(synced.result.inPlaceSessionFiles, 2);
  assert.equal(synced.result.sqliteRowsUpdated, 1);
  assert.equal(await value.provider("thread-paged"), "prov_a");

  for (const [index, item] of files.entries()) {
    const after = await value.read(item.name);
    const afterStat = await fs.stat(value.file(item.name), { bigint: true });
    assert.equal(providerOf(after), "prov_a");
    assert.deepEqual(nonProviderHeader(after), nonProviderHeader(before[index].bytes));
    assert.deepEqual(bodyOf(after), bodyOf(before[index].bytes));
    assert.equal(afterStat.ino, before[index].stat.ino);
    assert.equal(afterStat.size, before[index].stat.size);
  }
  assert.equal((await value.rows())[0].updated_at, 817);

  const noop = await apply(await prepareSync({ codexHome: value.home }));
  assert.equal(noop.outcome, "completed");
  assert.equal(noop.backup, null);
  assert.equal(noop.result.noop, true);

  const restorePlan = await prepareRestore({ codexHome: value.home, backupId: synced.backup.backupId });
  const restored = await applyRestore({ schemaVersion: 1, planId: restorePlan.planId });
  assert.equal(restored.outcome, "completed");
  assert.equal(await value.provider("thread-paged"), "openai");
  for (const [index, item] of files.entries()) assert.deepEqual(await value.read(item.name), before[index].bytes);
});

test("a paginated group uses streaming replacement for a variable-length Provider and still preserves bodies and Restore scope", async () => {
  const files = twoFileGroup("thread-stream", ["custom", "custom"]);
  const value = await paginatedFixture({
    configProvider: "provider_long",
    files,
    rows: [{ id: "thread-stream", provider: "custom", file: files[1].name }]
  });
  const before = await Promise.all(files.map(item => value.read(item.name)));
  const beforeHashes = before.map(file => hash(bodyOf(file)));
  const result = await apply(await prepareSync({ codexHome: value.home }));
  assert.equal(result.outcome, "completed");
  assert.equal(result.result.changedSessionFiles, 2);
  assert.equal(result.result.inPlaceSessionFiles, 0);
  assert.equal(result.result.rewrittenSessionFiles, 2);
  assert.equal(await value.provider("thread-stream"), "provider_long");
  for (const [index, item] of files.entries()) {
    const after = await value.read(item.name);
    assert.equal(providerOf(after), "provider_long");
    assert.deepEqual(nonProviderHeader(after), nonProviderHeader(before[index]));
    assert.equal(hash(bodyOf(after)), beforeHashes[index]);
  }
  const restorePlan = await prepareRestore({ codexHome: value.home, backupId: result.backup.backupId });
  await applyRestore({ schemaVersion: 1, planId: restorePlan.planId });
  for (const [index, item] of files.entries()) assert.deepEqual(await value.read(item.name), before[index]);
});

test("a prior file-only paginated sync repairs only SQLite on retry and then becomes a backup-free noop", async () => {
  const files = twoFileGroup("thread-half", ["custom", "custom"]);
  const value = await paginatedFixture({
    configProvider: "custom",
    files,
    rows: [{ id: "thread-half", provider: "openai", file: files[0].name }]
  });
  const before = await Promise.all(files.map(item => value.read(item.name)));
  const plan = await prepareSync({ codexHome: value.home });
  assert.equal(plan.impact.rolloutFilesToChange, 0);
  assert.equal(plan.impact.sqliteRowsToChange, 1);
  const repaired = await apply(plan);
  assert.equal(repaired.outcome, "completed");
  assert.equal(repaired.result.changedSessionFiles, 0);
  assert.equal(repaired.result.sqliteRowsUpdated, 1);
  assert.ok(repaired.backup?.backupId);
  assert.equal(await value.provider("thread-half"), "custom");
  for (const [index, item] of files.entries()) assert.deepEqual(await value.read(item.name), before[index]);

  const noop = await apply(await prepareSync({ codexHome: value.home }));
  assert.equal(noop.outcome, "completed");
  assert.equal(noop.backup, null);
  assert.equal(noop.result.noop, true);
});

test("a non-applied member keeps its paginated row unchanged for locked, missing, changed, and write-failure outcomes", async () => {
  for (const mode of ["locked", "missing", "changed", "write-failure"]) {
    const files = twoFileGroup(`thread-${mode}`);
    const value = await paginatedFixture({
      files,
      rows: [{ id: `thread-${mode}`, provider: "openai", file: files[0].name }]
    });
    const second = value.file(files[1].name);
    let plan;

    if (mode === "locked") {
      const originalOpen = fs.open;
      fs.open = async (target, ...args) => {
        if (path.resolve(String(target)) === path.resolve(second)) {
          throw Object.assign(new Error("synthetic lock"), { code: "EBUSY" });
        }
        return originalOpen(target, ...args);
      };
      try { plan = await prepareSync({ codexHome: value.home }); }
      finally { fs.open = originalOpen; }
    } else {
      plan = await prepareSync({
        codexHome: value.home,
        faultInjector: async ({ point, path: file }) => {
          if (point !== "before_rollout_apply" || file !== second) return;
          if (mode === "missing") await fs.unlink(second);
          if (mode === "changed") await fs.writeFile(second, `${header({ ...files[1], provider: "old" })}\nexternal body\n`);
          if (mode === "write-failure") throw new Error("synthetic write failure");
        }
      });
    }

    const result = await apply(plan);
    assert.equal(result.outcome, "partial", mode);
    assert.equal(await value.provider(`thread-${mode}`), "openai", mode);
    assert.ok(result.result.skipSummary.total > 0 || result.result.partialReason === "mutation-failed", mode);
  }
});

test("partial Restore returns only a written paginated member and preserves a later repair of the unwritten member", async () => {
  const files = twoFileGroup("thread-restore-scope");
  const value = await paginatedFixture({
    files,
    rows: [{ id: "thread-restore-scope", provider: "openai", file: files[0].name }]
  });
  const firstBefore = await value.read(files[0].name);
  const second = value.file(files[1].name);
  const result = await apply(await prepareSync({
    codexHome: value.home,
    faultInjector: async ({ point, path: file }) => {
      if (point === "before_rollout_apply" && file === second) await fs.unlink(second);
    }
  }));
  assert.equal(result.outcome, "partial");
  assert.ok(result.backup?.backupId);
  await fs.writeFile(second, `${header({ ...files[1], provider: "prov_a", extra: { later: "repair" } })}\nlater repair body\n`);
  const laterRepair = await fs.readFile(second);

  const restorePlan = await prepareRestore({ codexHome: value.home, backupId: result.backup.backupId });
  await applyRestore({ schemaVersion: 1, planId: restorePlan.planId });
  assert.deepEqual(await value.read(files[0].name), firstBefore);
  assert.deepEqual(await fs.readFile(second), laterRepair);
});

test("a newly discovered member of the same paginated thread is deferred and blocks that row until a new preview", async () => {
  const files = twoFileGroup("thread-added");
  const value = await paginatedFixture({
    files,
    rows: [{ id: "thread-added", provider: "openai", file: files[0].name }]
  });
  const added = path.join(value.home, "sessions", "rollout-thread-added-later.jsonl");
  let addedOnce = false;
  const plan = await prepareSync({
    codexHome: value.home,
    faultInjector: async ({ point }) => {
      if (point !== "before_rollout_apply" || addedOnce) return;
      addedOnce = true;
      await fs.writeFile(added, contents({ name: "thread-added-later", id: "thread-added", provider: "openai", ordinal: 9 }));
    }
  });
  const result = await apply(plan);
  assert.equal(result.outcome, "partial");
  assert.equal(await value.provider("thread-added"), "openai");
  assert.equal(providerOf(await fs.readFile(added)), "openai");
  assert.ok(result.result.skipSummary.items.some(item => item.kind === "sqlite" && item.id === "thread-added"));

  const converged = await apply(await prepareSync({ codexHome: value.home }));
  assert.equal(converged.outcome, "completed");
  assert.equal(await value.provider("thread-added"), "prov_a");
  assert.equal(providerOf(await fs.readFile(added)), "prov_a");
});

test("a newly discovered different thread is deferred without blocking the original paginated row", async () => {
  const files = twoFileGroup("thread-known");
  const value = await paginatedFixture({
    files,
    rows: [{ id: "thread-known", provider: "openai", file: files[0].name }]
  });
  const added = path.join(value.home, "sessions", "rollout-thread-other-later.jsonl");
  let addedOnce = false;
  const result = await apply(await prepareSync({
    codexHome: value.home,
    faultInjector: async ({ point }) => {
      if (point !== "before_rollout_apply" || addedOnce) return;
      addedOnce = true;
      await fs.writeFile(added, contents({ name: "thread-other-later", id: "thread-other", provider: "openai" }));
    }
  }));
  assert.equal(await value.provider("thread-known"), "prov_a");
  assert.equal(providerOf(await fs.readFile(added)), "openai");
  assert.ok(result.result.skipSummary.items.some(item => item.kind === "rollout" && item.path === added));
});

test("an unknown rollout added during Apply conservatively preserves every known paginated SQLite candidate, including a single-file group", async () => {
  const group = twoFileGroup("thread-group");
  const single = [{ name: "rollout-thread-single.jsonl", id: "thread-single", provider: "openai", ordinal: 77 }];
  const files = [...group, ...single];
  const value = await paginatedFixture({
    files,
    rows: [
      { id: "thread-group", provider: "openai", file: group[0].name },
      { id: "thread-single", provider: "openai", file: single[0].name }
    ]
  });
  const unknown = path.join(value.home, "archived_sessions", "rollout-unknown-later.jsonl");
  let addedOnce = false;
  const result = await apply(await prepareSync({
    codexHome: value.home,
    faultInjector: async ({ point }) => {
      if (point !== "before_rollout_apply" || addedOnce) return;
      addedOnce = true;
      await fs.writeFile(unknown, '{"type":"event_msg","payload":{}}\n');
    }
  }));
  assert.equal(result.outcome, "partial");
  assert.equal(await value.provider("thread-group"), "openai");
  assert.equal(await value.provider("thread-single"), "openai");
  assert.ok(result.result.skipSummary.unconfirmed > 0);

  await fs.unlink(unknown);
  const retry = await apply(await prepareSync({ codexHome: value.home }));
  assert.equal(retry.outcome, "completed");
  assert.equal(await value.provider("thread-group"), "prov_a");
  assert.equal(await value.provider("thread-single"), "prov_a");
});

for (const mode of ["reassigned", "invalid", "unknown", "locked", "unreadable", "missing"]) {
  test(`a deferred different thread becoming ${mode} during rollout writes is revalidated before SQLite`, async () => {
    const group = twoFileGroup("thread-recheck");
    const single = { name: "rollout-thread-single.jsonl", id: "thread-single", provider: "openai" };
    const value = await paginatedFixture({
      files: [...group, single],
      rows: [
        { id: "thread-recheck", provider: "openai", file: group[0].name },
        { id: "thread-single", provider: "openai", file: single.name }
      ]
    });
    const added = path.join(value.home, "sessions", "rollout-deferred-other.jsonl");
    let changed = false;
    let externalBytes;
    const originalOpen = fs.open;
    const plan = await prepareSync({
      codexHome: value.home,
      faultInjector: async ({ point }) => {
        if (point !== "after_rollout_apply" || changed) return;
        changed = true;
        if (mode === "reassigned") await fs.writeFile(added, contents({ name: "reassigned", id: "thread-recheck", provider: "openai" }));
        if (mode === "invalid") await fs.writeFile(added, "invalid header\n");
        if (mode === "unknown") await fs.writeFile(added, '{"type":"event_msg","payload":{}}\n');
        if (mode === "missing") await fs.unlink(added);
        else externalBytes = await fs.readFile(added);
      }
    });
    // Discovery during the first Apply scan freezes this path as deferred with
    // thread-other's ID. The subsequent write hook invalidates that evidence.
    await fs.writeFile(added, contents({ name: "other", id: "thread-other", provider: "openai" }));
    fs.open = async (target, ...args) => {
      if (changed && path.resolve(String(target)) === added && ["locked", "unreadable"].includes(mode)) {
        throw Object.assign(new Error("synthetic inaccessible deferred member"), { code: mode === "locked" ? "EBUSY" : "EACCES" });
      }
      return originalOpen(target, ...args);
    };
    let result;
    try { result = await apply(plan); }
    finally { fs.open = originalOpen; }
    assert.equal(changed, true);
    assert.equal(result.outcome, "partial");
    assert.equal(await value.provider("thread-recheck"), "openai");
    assert.equal(await value.provider("thread-single"), mode === "reassigned" ? "prov_a" : "openai");
    assert.equal(result.result.changedSessionFiles, 3, "only the three prepared files may be written");
    assert.ok(result.result.skipSummary.items.some(item => item.kind === "rollout" && item.path === added && item.reason === "deferred"));
    if (mode === "missing") await assert.rejects(fs.stat(added), { code: "ENOENT" });
    else assert.deepEqual(await fs.readFile(added), externalBytes, "the deferred file must remain untouched");

    if (["invalid", "unknown"].includes(mode)) await fs.unlink(added);
    const retry = await apply(await prepareSync({ codexHome: value.home }));
    assert.equal(retry.outcome, "completed");
    assert.equal(await value.provider("thread-recheck"), "prov_a");
    assert.equal(await value.provider("thread-single"), "prov_a");
    if (["reassigned", "locked", "unreadable"].includes(mode)) assert.equal(providerOf(await fs.readFile(added)), "prov_a");
  });
}

for (const mode of ["invalid", "unreadable"]) for (const indexedProvider of ["openai", "prov_a"]) {
  test(`a ${mode} deferred file with a ${indexedProvider} SQLite path owner still protects all paginated candidates`, async () => {
    const group = twoFileGroup("thread-path-group");
    const single = { name: "rollout-thread-path-single.jsonl", id: "thread-path-single", provider: "openai" };
    const value = await paginatedFixture({
      files: [...group, single],
      rows: [
        { id: "thread-path-group", provider: "openai", file: group[0].name },
        { id: "thread-path-single", provider: "openai", file: single.name },
        { id: "thread-path-other", provider: indexedProvider }
      ]
    });
    const added = path.join(value.home, "sessions", "rollout-deferred-path-owner.jsonl");
    await mutateRows(value.dbPath, database => {
      database.prepare("UPDATE threads SET rollout_path = ? WHERE id = ?").run(added, "thread-path-other");
    });
    let changed = false;
    let externalBytes;
    const originalOpen = fs.open;
    const plan = await prepareSync({
      codexHome: value.home,
      faultInjector: async ({ point }) => {
        if (point !== "after_rollout_apply" || changed) return;
        changed = true;
        if (mode === "invalid") await fs.writeFile(added, "invalid header\n");
        externalBytes = await fs.readFile(added);
      }
    });
    await fs.writeFile(added, contents({ name: "other", id: "thread-path-other", provider: "openai" }));
    fs.open = async (target, ...args) => {
      if (changed && path.resolve(String(target)) === added && mode === "unreadable") {
        throw Object.assign(new Error("synthetic unreadable deferred member"), { code: "EACCES" });
      }
      return originalOpen(target, ...args);
    };
    let result;
    try { result = await apply(plan); }
    finally { fs.open = originalOpen; }
    assert.equal(changed, true);
    assert.equal(result.outcome, "partial");
    assert.equal(await value.provider("thread-path-group"), "openai");
    assert.equal(await value.provider("thread-path-single"), "openai");
    assert.equal(await value.provider("thread-path-other"), indexedProvider);
    assert.equal(result.result.sqliteRowsUpdated, 0);
    assert.equal(result.result.changedSessionFiles, 3);
    assert.deepEqual(await fs.readFile(added), externalBytes);
    for (const id of ["thread-path-group", "thread-path-single"]) {
      assert.ok(result.result.skipSummary.items.some(item => item.kind === "sqlite" && item.id === id && item.reason === "association-unknown"));
    }
    await fs.writeFile(added, contents({ name: "other", id: "thread-path-other", provider: "openai" }));
    const retry = await apply(await prepareSync({ codexHome: value.home }));
    assert.equal(retry.outcome, "completed");
    assert.equal(await value.provider("thread-path-group"), "prov_a");
    assert.equal(await value.provider("thread-path-single"), "prov_a");
    assert.equal(await value.provider("thread-path-other"), "prov_a");
  });
}

for (const configProvider of ["prov_a", "provider_long"]) for (const mode of ["reassigned", "invalid", "unreadable", "unchanged"]) {
  test(`legacy SQLite candidates revalidate a ${mode} deferred file after ${configProvider} rollout writes`, async () => {
    const files = ["legacy-a", "legacy-healthy"].map(id => ({ name: `rollout-${id}.jsonl`, id, provider: "custom", historyMode: null }));
    const value = await paginatedFixture({
      configProvider,
      files,
      rows: [
        ...files.map(file => ({ id: file.id, provider: "custom", file: file.name })),
        { id: "index-only", provider: "custom" }
      ]
    });
    const added = path.join(value.home, "sessions", "rollout-deferred-legacy.jsonl");
    let changed = false;
    let externalBytes;
    const originalOpen = fs.open;
    const plan = await prepareSync({
      codexHome: value.home,
      faultInjector: async ({ point }) => {
        if (point !== "after_rollout_apply" || changed) return;
        changed = true;
        if (mode === "reassigned") await fs.writeFile(added, contents({ name: "reassigned", id: "legacy-a", provider: "custom", historyMode: null }));
        if (mode === "invalid") await fs.writeFile(added, "invalid header\n");
        externalBytes = await fs.readFile(added);
      }
    });
    await fs.writeFile(added, contents({ name: "other", id: "legacy-other", provider: "custom", historyMode: null }));
    fs.open = async (target, ...args) => {
      if (changed && path.resolve(String(target)) === added && mode === "unreadable") {
        throw Object.assign(new Error("synthetic unreadable legacy member"), { code: "EACCES" });
      }
      return originalOpen(target, ...args);
    };
    let result;
    try { result = await apply(plan); }
    finally { fs.open = originalOpen; }
    assert.equal(changed, true);
    assert.equal(result.outcome, "partial");
    assert.equal(await value.provider("legacy-a"), mode === "reassigned" ? "custom" : configProvider);
    assert.equal(await value.provider("legacy-healthy"), configProvider, "our own writes must not become changed-member skips");
    assert.equal(await value.provider("index-only"), ["invalid", "unreadable"].includes(mode) ? "custom" : configProvider);
    assert.equal(result.result.changedSessionFiles, 2);
    assert.equal(result.result.inPlaceSessionFiles, configProvider === "prov_a" ? 2 : 0);
    assert.equal(result.result.rewrittenSessionFiles, configProvider === "provider_long" ? 2 : 0);
    assert.deepEqual(await fs.readFile(added), externalBytes, "the deferred file stays outside this write set");
    for (const file of files) assert.equal(providerOf(await value.read(file.name)), configProvider);
    if (["reassigned", "invalid"].includes(mode)) await fs.unlink(added);
    const retry = await apply(await prepareSync({ codexHome: value.home }));
    assert.equal(retry.outcome, "completed");
    for (const id of ["legacy-a", "legacy-healthy", "index-only"]) assert.equal(await value.provider(id), configProvider);
  });
}

for (const configProvider of ["prov_a", "provider_long"]) for (const mode of ["same-id", "other-id", "unknown"]) {
  test(`an ${mode} file first appearing after ${configProvider} rollout writes revalidates an aligned legacy member's SQLite-only candidate`, async () => {
    const aligned = { name: "rollout-legacy-aligned.jsonl", id: "legacy-aligned", provider: configProvider, historyMode: null };
    const writer = { name: "rollout-legacy-writer.jsonl", id: "legacy-writer", provider: "custom", historyMode: null };
    const value = await paginatedFixture({
      configProvider,
      files: [aligned, writer],
      rows: [
        { id: aligned.id, provider: "custom", file: aligned.name },
        { id: writer.id, provider: configProvider, file: writer.name }
      ]
    });
    const added = path.join(value.home, "sessions", "rollout-new-legacy.jsonl");
    const addedBytes = mode === "unknown" ? "invalid header\n"
      : contents({ name: "later", id: mode === "same-id" ? aligned.id : "legacy-other", provider: "custom", historyMode: null });
    const plan = await prepareSync({
      codexHome: value.home,
      faultInjector: async ({ point }) => {
        if (point === "after_rollout_apply") await fs.writeFile(added, addedBytes);
      }
    });
    assert.equal(plan.impact.sqliteRowsToChange, 1);
    assert.equal(plan.impact.rolloutFilesToChange, 1);
    const result = await apply(plan);
    assert.equal(result.outcome, "partial");
    assert.equal(await value.provider(aligned.id), mode === "same-id" ? "custom" : configProvider);
    assert.equal(await value.provider(writer.id), configProvider);
    assert.equal(result.result.changedSessionFiles, 1);
    assert.equal(result.result.inPlaceSessionFiles, configProvider === "prov_a" ? 1 : 0);
    assert.equal(result.result.rewrittenSessionFiles, configProvider === "provider_long" ? 1 : 0);
    assert.equal(await fs.readFile(added, "utf8"), addedBytes);
    assert.ok(result.result.skipSummary.items.some(item => item.path === added && item.reason === "deferred"));
    assert.ok(!result.result.skipSummary.items.some(item => item.path === value.file(writer.name)), "successful writes keep their new binding");
  });
}

test("no SQLite candidates add no acknowledgement header reads or SQLite step, and the next noop adds no backup", async () => {
  const file = { name: "rollout-legacy-no-sqlite-write.jsonl", id: "legacy", provider: "custom", historyMode: null };
  const value = await paginatedFixture({ files: [file], rows: [{ id: file.id, provider: "prov_a", file: file.name }] });
  let writing = false;
  let readOnlyOpens = 0;
  let backups = 0;
  const stages = [];
  const options = {
    codexHome: value.home,
    onProgress: event => stages.push(event.stage),
    faultInjector: ({ point }) => {
      if (point === "before_rollout_apply") writing = true;
      if (point === "after_rollout_mutation_before_applied") writing = false;
      if (point === "before_backup") backups += 1;
    }
  };
  const plan = await prepareSync(options);
  assert.equal(plan.impact.sqliteRowsToChange, 0);
  const originalOpen = fs.open;
  fs.open = async (target, ...args) => {
    if (writing && path.resolve(String(target)) === value.file(file.name) && args[0] === "r") readOnlyOpens += 1;
    return originalOpen(target, ...args);
  };
  let result;
  try { result = await apply(plan); }
  finally { fs.open = originalOpen; }
  assert.equal(result.outcome, "completed");
  assert.equal(result.result.inPlaceSessionFiles, 1);
  assert.equal(result.result.sqliteRowsUpdated, 0);
  assert.equal(readOnlyOpens, 0, "SQLite association acknowledgements are unnecessary without SQLite candidates");
  assert.ok(!stages.includes("update_sqlite"));
  assert.equal(backups, 1, "only the rollout mutation needs a backup");
  const noop = await apply(await prepareSync(options));
  assert.equal(noop.result.noop, true);
  assert.equal(noop.backup, null);
  assert.equal(backups, 1);
  assert.ok(!stages.includes("update_sqlite"));
});

test("refreshing a deferred association cannot readmit a SQLite row excluded at the first Apply scan", async () => {
  const affected = twoFileGroup("thread-excluded");
  const healthy = twoFileGroup("thread-healthy");
  const value = await paginatedFixture({
    files: [...affected, ...healthy],
    rows: [
      { id: "thread-excluded", provider: "openai", file: affected[0].name },
      { id: "thread-healthy", provider: "openai", file: healthy[0].name }
    ]
  });
  const added = path.join(value.home, "sessions", "rollout-deferred-moving.jsonl");
  let changed = false;
  const plan = await prepareSync({
    codexHome: value.home,
    faultInjector: async ({ point }) => {
      if (point !== "after_rollout_apply" || changed) return;
      changed = true;
      await fs.writeFile(added, contents({ name: "moving", id: "thread-other", provider: "openai" }));
    }
  });
  await fs.writeFile(added, contents({ name: "moving", id: "thread-excluded", provider: "openai" }));
  const result = await apply(plan);
  assert.equal(changed, true);
  assert.equal(result.outcome, "partial");
  assert.equal(await value.provider("thread-excluded"), "openai");
  assert.equal(await value.provider("thread-healthy"), "prov_a");
  assert.equal(providerOf(await fs.readFile(added)), "openai");
  const retry = await apply(await prepareSync({ codexHome: value.home }));
  assert.equal(retry.outcome, "completed");
  assert.equal(await value.provider("thread-excluded"), "prov_a");
});

test("SQLite commit revalidates every group member, including an initially aligned member changed by another writer", async () => {
  const files = twoFileGroup("thread-revalidate", ["custom", "prov_a"]);
  const value = await paginatedFixture({
    files,
    rows: [{ id: "thread-revalidate", provider: "openai", file: files[0].name }]
  });
  const aligned = value.file(files[1].name);
  let replaced = false;
  const result = await apply(await prepareSync({
    codexHome: value.home,
    faultInjector: async ({ point }) => {
      if (point !== "after_rollout_apply" || replaced) return;
      replaced = true;
      await fs.writeFile(aligned, `${header({ ...files[1], provider: "custom" })}\nexternal replacement\n`);
    }
  }));
  assert.equal(replaced, true);
  assert.equal(result.outcome, "partial");
  assert.equal(await value.provider("thread-revalidate"), "openai");
  assert.ok(result.result.skippedChangedRolloutFiles.includes(aligned));
  assert.ok(result.result.skipSummary.items.some(item => item.kind === "sqlite" && item.id === "thread-revalidate"));
});

test("SQLite owner drift after Prepare preserves the affected paginated row while an independent group converges", async () => {
  for (const timing of ["add-before-apply", "change-before-apply", "add-after-rollout"]) {
    const affected = twoFileGroup("thread-owner-a");
    const healthy = twoFileGroup("thread-owner-healthy");
    const files = [...affected, ...healthy];
    const value = await paginatedFixture({
      files,
      rows: [
        { id: "thread-owner-a", provider: "openai", file: affected[0].name },
        { id: "thread-owner-healthy", provider: "openai", file: healthy[0].name },
        // An already-aligned foreign row must still participate in ownership
        // checks; otherwise it could silently steal A's rollout after Prepare.
        { id: "thread-owner-b", provider: "prov_a", rolloutPath: null }
      ]
    });
    const ownerPath = value.file(affected[0].name);
    let changedDuringRollout = false;
    const plan = await prepareSync({
      codexHome: value.home,
      faultInjector: async ({ point }) => {
        if (timing !== "add-after-rollout" || point !== "after_rollout_apply" || changedDuringRollout) return;
        changedDuringRollout = true;
        await mutateRows(value.dbPath, database => {
          database.prepare("INSERT INTO threads(id, model_provider, rollout_path, archived, updated_at) VALUES (?, ?, ?, 0, 701)")
            .run("thread-owner-b-late", "prov_a", ownerPath);
        });
      }
    });
    assert.equal(plan.impact.sqliteRowsToChange, 2, timing);

    if (timing === "add-before-apply") {
      await mutateRows(value.dbPath, database => {
        database.prepare("INSERT INTO threads(id, model_provider, rollout_path, archived, updated_at) VALUES (?, ?, ?, 0, 701)")
          .run("thread-owner-b-late", "prov_a", ownerPath);
      });
    }
    if (timing === "change-before-apply") {
      await mutateRows(value.dbPath, database => {
        database.prepare("UPDATE threads SET rollout_path = ? WHERE id = 'thread-owner-b'").run(ownerPath);
      });
    }

    const result = await apply(plan);
    assert.equal(result.outcome, "partial", timing);
    assert.equal(await value.provider("thread-owner-a"), "openai", timing);
    assert.equal(await value.provider("thread-owner-healthy"), "prov_a", timing);
    assert.ok(result.result.skipSummary.items.some(item => item.kind === "sqlite"
      && item.id === "thread-owner-a" && item.reason === "association-conflict"), timing);
    if (timing === "add-after-rollout") assert.equal(changedDuringRollout, true);

    await mutateRows(value.dbPath, database => {
      database.prepare("DELETE FROM threads WHERE id IN ('thread-owner-b', 'thread-owner-b-late')").run();
    });
    const retry = await apply(await prepareSync({ codexHome: value.home }));
    assert.equal(retry.outcome, "completed", timing);
    assert.equal(await value.provider("thread-owner-a"), "prov_a", timing);
  }
});
