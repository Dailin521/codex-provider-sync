import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test, { afterEach } from "node:test";
import { performance } from "node:perf_hooks";
import { applySessionChanges, collectSessionChanges, restoreSessionChanges } from "../src/session-files.js";
import { createBackup, restoreBackup } from "../src/backup.js";
import { runRestore, runSync } from "../src/service.js";
import { listHistory } from "../src/history.js";
import { TransactionJournal, readTransactionJournal, findPendingTransactions } from "../src/transaction-journal.js";

delete process.env.CODEX_SQLITE_HOME;

const cleanups = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });
const posix = { skip: process.platform === "win32" };
const header = '{"type":"session_meta","payload":{"id":"fixture","cwd":"\u4e2d\u6587","model_provider" : "openai"}}';
const tail = '\n{"type":"event_msg","payload":{"type":"user_message","message":"fixture"}}\n';

async function fixture(t, line = header, suffix = tail) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-in-place-"));
  cleanups.push(() => fs.rm(root, { recursive: true, force: true }));
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

test("short writes preserve inode, size, content and original mtime", posix, async (t) => {
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

test("pre-write replaced path, header or append is skipped without fallback", posix, async (t) => {
  for (const state of ["replace", "header", "append"]) {
    const f = await fixture(t);
    const { changes } = await collectSessionChanges(f.codexHome, "prov_a");
    if (state === "replace") {
      await fs.writeFile(f.file + ".other", f.original);
      await fs.utimes(f.file + ".other", f.mtime, f.mtime);
      await fs.rename(f.file + ".other", f.file);
    } else if (state === "header") {
      await fs.writeFile(f.file, f.original.toString().replace(/\n/g, " "));
      await fs.utimes(f.file, f.mtime, f.mtime);
    } else await fs.appendFile(f.file, tail);
    const before = await fs.readFile(f.file);
    const result = await applySessionChanges(changes);
    assert.equal(result.appliedChanges, 0);
    assert.deepEqual(result.skippedPaths, [f.file]);
    assert.deepEqual(await fs.readFile(f.file), before);
  }
});

test("active fd appends remain visible and rollback never backdates the appended tail", posix, async (t) => {
  const f = await fixture(t);
  const { changes, entry } = await prepare(f);
  const writer = await fs.open(f.file, "a");
  cleanups.push(() => writer.close());
  const before = await writer.stat();
  await applySessionChanges(changes);
  await writer.write(tail);
  const appended = await writer.stat();
  await restoreSessionChanges([entry]);
  const after = await fs.stat(f.file);
  assert.equal(after.ino, before.ino);
  assert.ok(after.mtimeMs >= appended.mtimeMs);
  assert.equal(await fs.readFile(f.file, "utf8"), f.original + tail);
});

test("write-time truncation or surrounding-header edits cannot report success", posix, async (t) => {
  for (const kind of ["truncate", "header"]) {
    const f = await fixture(t);
    const { changes } = await collectSessionChanges(f.codexHome, "prov_a");
    await assert.rejects(applySessionChanges(changes, {
      async inPlaceWrite(h, b, o, n, p) {
        if (kind === "truncate") await h.truncate(p);
        else await h.write(Buffer.from("!"), 0, 1, 0);
        return h.write(b, o, n, p);
      }
    }), { code: "IN_PLACE_RESTORE_FAILED" });
  }
});

test("append between patch and fsync remains visible and never calls utimes", posix, async (t) => {
  const f = await fixture(t);
  const { changes } = await collectSessionChanges(f.codexHome, "prov_a");
  const writer = await fs.open(f.file, "a");
  cleanups.push(() => writer.close());
  let appended;
  await applySessionChanges(changes, {
    async inPlaceSync(h) {
      h.utimes = () => { throw new Error("in-place must not backdate an unlocked file"); };
      await writer.write(tail);
      appended = await writer.stat();
      await h.sync();
    }
  });
  assert.equal((await fs.stat(f.file)).mtimeMs, appended.mtimeMs);
  assert.equal(await fs.readFile(f.file, "utf8"), f.original.toString().replace("openai", "prov_a") + tail);
});

test("append racing mtime restoration is never backdated, including rollback", posix, async (t) => {
  for (const restore of [false, true]) for (const timing of ["before", "after"]) {
    const f = await fixture(t);
    const { changes, entry } = await prepare(f);
    const writer = await fs.open(f.file, "a");
    cleanups.push(() => writer.close());
    let appended;
    const write = async (h, b, o, n, p) => {
      if (!appended) {
        const utimes = h.utimes.bind(h);
        h.utimes = async (...args) => {
          if (timing === "after") await utimes(...args);
          await writer.write(tail);
          appended = await writer.stat();
          if (timing === "before") await utimes(...args);
        };
      }
      return h.write(b, o, n, p);
    };
    if (restore) {
      await applySessionChanges(changes);
      await restoreSessionChanges([entry], { inPlaceRestoreWrite: write });
    } else await applySessionChanges(changes, { inPlaceWrite: write });
    assert.ok(appended);
    assert.ok((await fs.stat(f.file)).mtimeMs >= appended.mtimeMs);
    const expected = restore ? f.original.toString() : f.original.toString().replace("openai", "prov_a");
    assert.equal(await fs.readFile(f.file, "utf8"), expected + tail);
  }
});

test("provider-only updates do not make History select an older duplicate", async (t) => {
  const f = await fixture(t);
  const newer = path.join(f.codexHome, "sessions", "rollout-newer.jsonl");
  await fs.writeFile(newer, f.original.toString().replace("openai", "prov_a"));
  const date = new Date(f.mtime.getTime() + 10000);
  await fs.utimes(newer, date, date);
  assert.equal((await listHistory(f.codexHome)).sessions[0].rolloutPath, newer);
  await runSync({ codexHome: f.codexHome });
  assert.equal((await listHistory(f.codexHome)).sessions[0].rolloutPath, newer);
  assert.equal(Math.round((await fs.stat(f.file)).mtimeMs), f.mtime.getTime());
});

test("recovery repairs a crash after backdating an append even when bytes are already old", posix, async (t) => {
  const f = await fixture(t);
  const { entry } = await prepare(f);
  await fs.appendFile(f.file, tail);
  await fs.utimes(f.file, f.mtime, f.mtime);
  await restoreSessionChanges([entry]);
  const restored = await fs.stat(f.file);
  assert.ok(restored.mtimeMs > f.mtime.getTime());
  await restoreSessionChanges([entry]);
  assert.equal((await fs.stat(f.file)).mtimeMs, restored.mtimeMs);
  assert.equal(await fs.readFile(f.file, "utf8"), f.original + tail);
});

test("hardlinked files are not eligible and late links prevent byte mutation", posix, async (t) => {
  const f = await fixture(t);
  const { changes } = await collectSessionChanges(f.codexHome, "prov_a");
  await fs.link(f.file, f.file + ".link");
  assert.equal((await collectSessionChanges(f.codexHome, "prov_a")).changes[0].inPlaceMutation, null);
  assert.equal((await applySessionChanges(changes)).appliedChanges, 0);
  assert.deepEqual(await fs.readFile(f.file), f.original);
});

test("a post-mutation conflict returns partial and preserves the UndoBackup", async (t) => {
  const f = await fixture(t);
  const second = path.join(f.codexHome, "sessions", "rollout-z.jsonl");
  await fs.writeFile(second, f.original);
  const result = await runSync({ codexHome: f.codexHome, faultInjector: async ({ point, path: file }) => {
    if (point === "after_rollout_mutation_before_applied" && file === second) {
      const text = await fs.readFile(second, "utf8");
      await fs.writeFile(second, text.replace("prov_a", "??????"));
      throw new Error("B conflict");
    }
  } });
  assert.equal(result.partial, true);
  assert.equal(result.partialReason, "mutation-failed");
  assert.equal(result.failedStage, "rewrite_rollout_files");
  assert.ok(result.backupDir);
  assert.match(await fs.readFile(f.file, "utf8"), /prov_a/);
  assert.match(await fs.readFile(second, "utf8"), /\?{6}/);
  assert.deepEqual(await findPendingTransactions(f.codexHome), []);
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
