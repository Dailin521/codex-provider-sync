import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { applySessionChanges, collectSessionChanges, restoreSessionChanges } from "../src/session-files.js";
import { createBackup, restoreBackup } from "../src/backup.js";
import { runRestore, runSync } from "../src/service.js";
import { TransactionJournal, readTransactionJournal, findPendingTransactions } from "../src/transaction-journal.js";

const repo = fileURLToPath(new URL("..", import.meta.url));
const posix = { skip: process.platform === "win32" };
const header = '{"type":"session_meta","payload":{"id":"fixture","cwd":"\u4e2d\u6587","model_provider" : "openai"}}';
const tail = '\n{"type":"event_msg","payload":{"type":"user_message","message":"fixture"}}\n';

async function fixture(t, line = header, suffix = tail) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-in-place-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const codexHome = path.join(root, "codex");
  await fs.mkdir(path.join(codexHome, "sessions"), { recursive: true });
  const file = path.join(codexHome, "sessions", "rollout-fixture.jsonl");
  const configPath = path.join(codexHome, "config.toml");
  await fs.writeFile(configPath, 'model_provider = "prov_a"\n');
  await fs.writeFile(file, line + suffix);
  const mtime = new Date("2026-01-02T03:04:05Z");
  await fs.utimes(file, mtime, mtime);
  return { codexHome, file, configPath, original: Buffer.from(line + suffix), mtime };
}

async function prepare(f) {
  const { changes } = await collectSessionChanges(f.codexHome, "prov_a");
  const backup = await createBackup({ codexHome: f.codexHome, targetProvider: "prov_a", sessionChanges: changes, configPath: f.configPath });
  const manifest = JSON.parse(await fs.readFile(path.join(backup, "session-meta-backup.json"), "utf8"));
  return { changes, backup, entry: manifest.files[0] };
}

async function setBytes(file, mutation, bytes) {
  const h = await fs.open(file, "r+");
  try { await h.write(bytes, 0, bytes.length, mutation.byteOffset); await h.sync(); }
  finally { await h.close(); }
}

test("in-place scan rejects ambiguous, escaped, non-ASCII and model-changing inputs", async (t) => {
  const lines = [
    header.replace('"model_provider" : "openai"', '"model_provider":"openai","model_provider":"openai"'),
    header.replace('"model_provider"', '"model_\\u0070rovider"'),
    header.replace('"openai"', '"ope\\u006eai"'),
    header.replace('"model_provider" : "openai"', '"model_provider":"openai","nested":{"model_provider":"openai"}'),
    header.replace('"payload":', '"payload":{},"payload":')
  ];
  for (const line of lines) {
    const f = await fixture(t, line);
    const { changes } = await collectSessionChanges(f.codexHome, "prov_a");
    assert.equal(changes[0].inPlaceMutation, null);
    assert.equal((await applySessionChanges(changes)).inPlaceChanges, 0);
  }
  for (const target of ["provider_a", "", "\u4e00\u4e8c", 'bad"id']) {
    const f = await fixture(t);
    const { changes } = await collectSessionChanges(f.codexHome, target);
    assert.equal(changes[0].inPlaceMutation, null);
  }
  const f = await fixture(t, header, '\n{"type":"turn_context","payload":{"model":"before"}}\n');
  const { changes } = await collectSessionChanges(f.codexHome, "prov_a", { targetModel: "after" });
  assert.equal(changes[0].inPlaceMutation, null);
  assert.equal((await applySessionChanges(changes, { targetModel: "after" })).inPlaceChanges, 0);
});

test("provider-looking text inside a string is not a duplicate field", posix, async (t) => {
  const f = await fixture(t, header.replace('"cwd":"\u4e2d\u6587"', '"cwd":"\\\"model_provider\\\":\\\"elsewhere\\\""'));
  const { changes } = await collectSessionChanges(f.codexHome, "prov_a");
  assert.ok(changes[0].inPlaceMutation);
});

test("short writes loop to completion, preserve inode, bytes, size and mtime", posix, async (t) => {
  const f = await fixture(t);
  const { changes } = await collectSessionChanges(f.codexHome, "prov_a");
  const before = await fs.stat(f.file);
  let writes = 0;
  const result = await applySessionChanges(changes, { inPlaceWrite(h, b, o, n, p) { writes++; return h.write(b, o, Math.min(n, 2), p); } });
  assert.equal(result.inPlaceChanges, 1);
  assert.equal(writes, 4);
  const after = await fs.stat(f.file);
  assert.equal(after.ino, before.ino);
  assert.equal(after.size, before.size);
  assert.equal(Math.round(after.mtimeMs), f.mtime.getTime());
  assert.equal(await fs.readFile(f.file, "utf8"), f.original.toString().replace('"openai"', '"prov_a"'));
});

test("short-write exception, zero progress and fsync failure restore original bytes", posix, async (t) => {
  for (const kind of ["short", "zero", "sync"]) {
    const f = await fixture(t);
    const { changes } = await collectSessionChanges(f.codexHome, "prov_a");
    const before = await fs.stat(f.file);
    let writes = 0;
    const options = kind === "sync" ? { inPlaceSync: () => { throw new Error("fsync fault"); } } : {
      async inPlaceWrite(h, b, o, n, p) {
        if (kind === "zero") return { bytesWritten: 0 };
        if (writes++) throw new Error("write fault");
        return h.write(b, o, 3, p);
      }
    };
    await assert.rejects(applySessionChanges(changes, options));
    assert.deepEqual(await fs.readFile(f.file), f.original);
    assert.equal((await fs.stat(f.file)).ino, before.ino);
  }
});

test("failed immediate restoration never falls back and remains recoverable", posix, async (t) => {
  const f = await fixture(t);
  const { changes, entry } = await prepare(f);
  let writes = 0;
  await assert.rejects(applySessionChanges(changes, {
    async inPlaceWrite(h, b, o, n, p) { if (writes++) throw new Error("write fault"); return h.write(b, o, 4, p); },
    inPlaceRestoreWrite() { throw new Error("restore fault"); }
  }), { code: "IN_PLACE_RESTORE_FAILED" });
  assert.notDeepEqual(await fs.readFile(f.file), f.original);
  await restoreSessionChanges([entry]);
  assert.deepEqual(await fs.readFile(f.file), f.original);
});

test("in-place recovery accepts old/new/contiguous partial, rejects unknown bytes", posix, async (t) => {
  for (const state of ["old", "new", "prefix", "middle", "unknown", "disjoint", "header", "truncate", "replace"]) {
    const f = await fixture(t);
    const { entry } = await prepare(f);
    const m = entry.mutation;
    const original = Buffer.from(m.originalBase64, "base64"), replacement = Buffer.from(m.replacementBase64, "base64");
    const bytes = Buffer.from(original);
    if (state === "new") replacement.copy(bytes);
    if (state === "prefix") replacement.copy(bytes, 0, 0, 4);
    if (state === "middle") replacement.copy(bytes, 2, 2, 5);
    if (state === "unknown") bytes[2] = 33;
    if (state === "disjoint") { bytes[1] = replacement[1]; bytes[5] = replacement[5]; }
    await setBytes(f.file, m, bytes);
    if (state === "header") { const h = await fs.open(f.file, "r+"); await h.write(Buffer.from("!"), 0, 1, 0); await h.close(); }
    if (state === "truncate") await fs.truncate(f.file, f.original.length - 1);
    if (state === "replace") { await fs.writeFile(f.file + ".other", f.original); await fs.rename(f.file + ".other", f.file); }
    const before = await fs.readFile(f.file), stat = await fs.stat(f.file);
    if (["unknown", "disjoint", "header", "truncate", "replace"].includes(state)) {
      await assert.rejects(restoreSessionChanges([entry]), AggregateError);
      assert.deepEqual(await fs.readFile(f.file), before);
    } else {
      await restoreSessionChanges([entry]);
      await restoreSessionChanges([entry]);
      assert.deepEqual(await fs.readFile(f.file), f.original);
      assert.equal((await fs.stat(f.file)).ino, stat.ino);
    }
  }
});

test("pre-write replaced path or append is skipped without fallback", posix, async (t) => {
  for (const state of ["replace", "append"]) {
    const f = await fixture(t);
    const { changes } = await collectSessionChanges(f.codexHome, "prov_a");
    if (state === "replace") {
      await fs.writeFile(f.file + ".other", f.original);
      await fs.utimes(f.file + ".other", f.mtime, f.mtime);
      await fs.rename(f.file + ".other", f.file);
    } else await fs.appendFile(f.file, tail);
    const before = await fs.readFile(f.file);
    const result = await applySessionChanges(changes);
    assert.equal(result.appliedChanges, 0);
    assert.deepEqual(result.skippedPaths, [f.file]);
    assert.deepEqual(await fs.readFile(f.file), before);
  }
});

test("active fd appends remain visible and rollback preserves appended tail and mtime", posix, async (t) => {
  const f = await fixture(t);
  const { changes, entry } = await prepare(f);
  const writer = await fs.open(f.file, "a");
  t.after(() => writer.close());
  const before = await writer.stat();
  await applySessionChanges(changes);
  await writer.write(tail);
  const appended = await writer.stat();
  await restoreSessionChanges([entry]);
  const after = await fs.stat(f.file);
  assert.equal(after.ino, before.ino);
  assert.ok(Math.abs(after.mtimeMs - appended.mtimeMs) < 0.01);
  assert.equal(await fs.readFile(f.file, "utf8"), f.original + tail);
});

test("durable manifest and applying precede mutation; observer failure rolls back without rename", posix, async (t) => {
  const f = await fixture(t);
  const before = await fs.stat(f.file);
  let backup, immutable;
  await assert.rejects(runSync({ codexHome: f.codexHome, faultInjector: async ({ point, path: file, mutation }) => {
    if (point === "before_rollout_apply") {
      const pending = await findPendingTransactions(f.codexHome);
      backup = pending[0].backupDir;
      immutable = await fs.readFile(path.join(backup, "session-meta-backup.json"));
      assert.equal(pending[0].events.at(-1).state, "applying");
      const manifest = JSON.parse(immutable);
      assert.equal(manifest.version, 3);
      assert.ok(manifest.files[0].mutation.originalBase64);
      assert.deepEqual(await fs.readFile(file), f.original);
    }
    if (point === "after_rollout_mutation_before_applied") {
      assert.equal(mutation.result, "APPLIED_IN_PLACE");
      throw new Error("observer fault");
    }
  } }), (e) => e.code === "SYNC_FAILED_ROLLED_BACK");
  assert.deepEqual(await fs.readFile(f.file), f.original);
  assert.equal((await fs.stat(f.file)).ino, before.ino);
  assert.deepEqual(await fs.readFile(path.join(backup, "session-meta-backup.json")), immutable);
});

test("A applied, B fails: both in-place targets roll back (#69)", posix, async (t) => {
  const f = await fixture(t);
  const second = path.join(f.codexHome, "sessions", "rollout-z.jsonl");
  await fs.writeFile(second, f.original);
  const before = await fs.stat(f.file);
  await assert.rejects(runSync({ codexHome: f.codexHome, faultInjector: ({ point, targetIndex }) => {
    if (point === "before_rollout_apply" && targetIndex === 2) throw new Error("B failed");
  } }), (e) => e.code === "SYNC_FAILED_ROLLED_BACK");
  assert.deepEqual(await fs.readFile(f.file), f.original);
  assert.deepEqual(await fs.readFile(second), f.original);
  assert.equal((await fs.stat(f.file)).ino, before.ino);
});

test("unknown bytes leave a recoveryRequired journal and block later writes", posix, async (t) => {
  const f = await fixture(t);
  await assert.rejects(runSync({ codexHome: f.codexHome, faultInjector: async ({ point }) => {
    if (point === "after_rollout_mutation_before_applied") {
      const { changes } = await collectSessionChanges(f.codexHome, "openai");
      await setBytes(f.file, changes[0].inPlaceMutation, Buffer.from('"??????"'));
      throw new Error("unknown writer");
    }
  } }), (e) => e.code === "RECOVERY_REQUIRED" && e.recoveryRequired);
  assert.equal((await findPendingTransactions(f.codexHome))[0].state, "recoveryRequired");
  await assert.rejects(runSync({ codexHome: f.codexHome }), { code: "RECOVERY_REQUIRED" });
});

test("actual process exit at applying/applied boundary recovers in place", posix, async (t) => {
  for (const point of ["after_rollout_mutation_before_applied", "after_rollout_apply"]) {
    const f = await fixture(t);
    const before = await fs.stat(f.file);
    const child = spawnSync(process.execPath, ["--input-type=module", "-e", `
      import { runSync } from './src/service.js';
      await runSync({codexHome: process.argv[1], faultInjector: ({point}) => {if(point === ${JSON.stringify(point)}) process.exit(91);}});
    `, f.codexHome], { cwd: repo, encoding: "utf8" });
    assert.equal(child.status, 91, child.stderr);
    const [pending] = await findPendingTransactions(f.codexHome);
    assert.ok(pending);
    await runRestore({ codexHome: f.codexHome, backupDir: pending.backupDir, restoreConfig: false, restoreDatabase: false });
    assert.deepEqual(await fs.readFile(f.file), f.original);
    assert.equal((await fs.stat(f.file)).ino, before.ino);
  }
});

test("applying-only partial crash and torn journal recover from immutable manifest", posix, async (t) => {
  for (const torn of [false, true]) {
    const f = await fixture(t);
    const { backup, entry } = await prepare(f);
    const journal = await TransactionJournal.create(backup, { codexHome: f.codexHome, targetProvider: "prov_a", potentialTargets: [f.file] });
    await journal.applying("rollout", f.file);
    await setBytes(f.file, entry.mutation, Buffer.from(entry.mutation.replacementBase64, "base64").subarray(0, 4));
    if (torn) await fs.appendFile(journal.filePath, '{"torn":');
    await runRestore({ codexHome: f.codexHome, backupDir: backup, restoreConfig: torn, restoreDatabase: false });
    assert.deepEqual(await fs.readFile(f.file), f.original);
    assert.equal((await readTransactionJournal(journal.filePath)).state, "rolledBack");
  }
});

test("large fixture: actual rollout writes are bounded by provider bytes, not tail size", posix, async (t) => {
  const f = await fixture(t, header, tail + ("x".repeat(65535) + "\n").repeat(512));
  const { changes } = await collectSessionChanges(f.codexHome, "prov_a");
  const hash = (b) => createHash("sha256").update(b.subarray(b.indexOf(10) + 1)).digest("hex");
  let written = 0;
  const start = performance.now();
  await applySessionChanges(changes, { async inPlaceWrite(h, b, o, n, p) { const r = await h.write(b, o, n, p); written += r.bytesWritten; return r; } });
  assert.equal(written, 8);
  assert.equal(hash(await fs.readFile(f.file)), hash(f.original));
  t.diagnostic(`32 MiB fixture: ${written} rollout bytes written in ${(performance.now() - start).toFixed(1)} ms (including verification/hash read).`);
});
