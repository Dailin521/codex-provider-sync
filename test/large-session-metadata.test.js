import assert from "node:assert/strict";
import fs from "node:fs/promises";
import fsSync from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import test, { afterEach } from "node:test";

const cleanups = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });
import { PROVIDER_SESSION_META_MAX_BYTES } from "../src/constants.js";
import { readProviderRevisionHeader } from "../src/session-files.js";
import { applySwitch, prepareSync, prepareSwitch, runSync, runSwitch, runRestore, getStatus } from "../src/service.js";
import { openDatabase } from "../src/sqlite.js";
import { CoreError } from "../src/core-error.js";
import { createPublicCoreErrorDto, isCanonicalPublicCoreErrorDto } from "../packages/contracts/dist/index.js";
import { normalizeCliErrorDto } from "../src/cli-json.js";
import { dispatchWebCoreRequest } from "../src/web-core-adapter.js";

delete process.env.CODEX_SQLITE_HOME;
const MiB = 1024 * 1024;

async function fixture(t) {
  const base = process.platform === "win32" ? "D:/Temp" : os.tmpdir();
  await fs.mkdir(base, { recursive: true });
  const home = await fs.mkdtemp(path.join(base, "large-session-metadata-"));
  cleanups.push(() => fs.rm(home, { recursive: true, force: true }));
  await fs.mkdir(path.join(home, "sessions"));
  await fs.mkdir(path.join(home, "sqlite"));
  await fs.writeFile(path.join(home, "config.toml"), 'model_provider="openai"\n[model_providers.provider_long]\nmodel="fixture"\n');
  const file = path.join(home, "sessions", "rollout-fixture.jsonl");
  const dbPath = path.join(home, "sqlite", "state_5.sqlite");
  const db = await openDatabase(dbPath);
  db.exec("CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT, archived INTEGER DEFAULT 0, updated_at INTEGER DEFAULT 123); INSERT INTO threads (id,model_provider) VALUES ('fixture','custom');");
  db.close();
  return { home, file, dbPath };
}

async function digest(file, start = 0) {
  const hash = createHash("sha256");
  for await (const chunk of fsSync.createReadStream(file, { start })) hash.update(chunk);
  return hash.digest("hex");
}

async function writeHeader(file, bytes, separator = "\r\n") {
  const prefix = '{"type":"session_meta","payload":{"id":"fixture","model_provider":"custom","instructions":"';
  const suffix = '中文"}}';
  const handle = await fs.open(file, "w");
  try {
    await handle.write(prefix);
    let remaining = bytes - Buffer.byteLength(prefix + suffix);
    const chunk = Buffer.alloc(64 * 1024, 120);
    while (remaining > 0) {
      const size = Math.min(remaining, chunk.length);
      await handle.write(chunk.subarray(0, size));
      remaining -= size;
    }
    await handle.write(suffix + separator);
    for (let i = 0; i < 32; i++) await handle.write(Buffer.alloc(MiB, 121));
  } finally { await handle.close(); }
}

test("large valid metadata: Status, in-place Sync, streaming Switch and Restore preserve data", async t => {
  const value = await fixture(t);
  const requestedSize = Number(process.env.CPS_LARGE_HEADER_MIB ?? 8) * MiB;
  // Leave room for the longer Provider, so the successful write stays readable.
  const size = Math.min(requestedSize, PROVIDER_SESSION_META_MAX_BYTES - 16);
  assert.ok(size >= 8 * MiB && size <= PROVIDER_SESSION_META_MAX_BYTES);
  await writeHeader(value.file, size);
  const original = await digest(value.file);
  const body = await digest(value.file, size + 2);
  const before = await fs.stat(value.file);
  const started = performance.now();
  const status = await getStatus({ codexHome: value.home, includeSessionActivity: false });
  assert.equal(status.rolloutScanComplete, true);
  assert.equal(status.rolloutCounts.sessions.custom, 1);
  const sync = await runSync({ codexHome: value.home });
  assert.equal(sync.partial, false);
  assert.equal(sync.inPlaceSessionFiles, 1);
  const synced = await fs.stat(value.file);
  assert.equal(synced.ino, before.ino);
  assert.equal(synced.size, before.size);
  assert.ok(Math.abs(synced.mtimeMs - before.mtimeMs) < 1);
  assert.equal(await digest(value.file, size + 2), body);
  const db = await openDatabase(value.dbPath);
  try { assert.deepEqual({ ...db.prepare("SELECT model_provider, updated_at FROM threads").get() }, { model_provider: "openai", updated_at: 123 }); }
  finally { db.close(); }
  await runRestore({ codexHome: value.home, backupDir: sync.backupDir });
  assert.equal(await digest(value.file), original);
  const switched = await runSwitch({ codexHome: value.home, provider: "provider_long", keepRootModel: true });
  assert.equal(switched.partial, false);
  assert.equal(switched.rewrittenSessionFiles, 1);
  const afterSwitch = await getStatus({ codexHome: value.home, includeSessionActivity: false });
  assert.equal(afterSwitch.rolloutScanComplete, true);
  await prepareSync({ codexHome: value.home });
  assert.equal(await digest(value.file, size + 2 + "provider_long".length - "openai".length), body);
  await runRestore({ codexHome: value.home, backupDir: switched.backupDir });
  assert.equal(await digest(value.file), original);
  t.diagnostic(JSON.stringify({ headerBytes: size, headerMiB: size / MiB, elapsedMs: Math.round(performance.now() - started), parentPeakRssKiB: process.resourceUsage().maxRSS }));
});

test("bounded reader accepts exact 128 MiB with LF, CRLF or EOF and rejects an extra byte", async () => {
  // Virtual files avoid writing three 128 MiB fixtures; the actual reader still
  // consumes every byte. Count only linear merge work, not elapsed-time guesses.
  for (const separator of ["\n", "\r\n", ""]) {
    let read = 0;
    let closed = false;
    let merged = 0;
    const originalConcat = Buffer.concat;
    const max = PROVIDER_SESSION_META_MAX_BYTES;
    const fsImpl = { async open() { return {
      async read(buffer, offset, length, position) {
        const count = Math.max(0, Math.min(length, max + separator.length - position));
        buffer.fill(120, offset, offset + count);
        for (let i = 0; i < separator.length; i++) {
          if (max + i >= position && max + i < position + count) buffer[offset + max + i - position] = separator.charCodeAt(i);
        }
        read += count;
        return { bytesRead: count };
      }, async close() { closed = true; }
    }; } };
    Buffer.concat = function(chunks, length) { if (length >= max) merged += length; return originalConcat(chunks, length); };
    try {
      const record = await readProviderRevisionHeader("virtual", { fsImpl });
      assert.equal(record.firstLine.length, max);
      assert.equal(record.separator, separator);
      assert.equal(record.offset, max + separator.length);
      assert.equal(read, max + separator.length);
      assert.ok(merged <= max + 1);
      assert.equal(closed, true);
    } finally { Buffer.concat = originalConcat; }
  }
  let read = 0;
  const fsImpl = { async open() { return {
    async read(buffer, offset, length) { buffer.fill(120); read += length; return { bytesRead: length }; },
    async close() {}
  }; } };
  await assert.rejects(readProviderRevisionHeader("virtual", { fsImpl }), { name: "RolloutMetadataLimitError" });
  assert.ok(read <= PROVIDER_SESSION_META_MAX_BYTES + 2);
});

test("header reader handles split CRLF and UTF-8 without scanning the body", async () => {
  for (const prefixLength of [65529, 65532]) {
    const firstLine = "x".repeat(prefixLength) + "中文";
    const source = Buffer.concat([Buffer.from(firstLine + "\r\n"), Buffer.alloc(8 * MiB, 121)]);
    let read = 0;
    const fsImpl = { async open() { return {
      async read(buffer, offset, length, position) {
        const count = source.copy(buffer, offset, position, position + length);
        read += count;
        return { bytesRead: count };
      }, async close() {}
    }; } };
    const record = await readProviderRevisionHeader("virtual", { fsImpl });
    assert.equal(record.firstLine, firstLine);
    assert.equal(record.separator, "\r\n");
    assert.equal(record.offset, Buffer.byteLength(firstLine) + 2);
    assert.equal(read, 128 * 1024);
  }
});

test("invalid and oversized metadata are excluded from Prepare with safe reasons and zero writes", async t => {
  const value = await fixture(t);
  for (const code of ["ROLLOUT_METADATA_INVALID", "ROLLOUT_METADATA_TOO_LARGE"]) {
    await fs.writeFile(value.file, "not-json\n");
    if (code.endsWith("TOO_LARGE")) {
      await fs.writeFile(value.file, "x");
      await fs.truncate(value.file, PROVIDER_SESSION_META_MAX_BYTES + 1);
    }
    const before = await digest(value.file);
    const config = await fs.readFile(path.join(value.home, "config.toml"));
    const db = await digest(value.dbPath);
    for (const prepare of [() => prepareSync({ codexHome: value.home }), () => prepareSwitch({ codexHome: value.home, provider: "provider_long" })]) {
      const plan = await prepare();
      assert.equal(plan.impact.rolloutFilesToChange, 0);
      assert.ok(plan.impact.skipSummary.items.some(item => item.reason === (code.endsWith("TOO_LARGE") ? "metadata-too-large" : "metadata-invalid")));
    }
    assert.equal(await digest(value.file), before);
    assert.equal(await digest(value.dbPath), db);
    assert.deepEqual(await fs.readFile(path.join(value.home, "config.toml")), config);
    await assert.rejects(fs.stat(path.join(value.home, "backups_state")), { code: "ENOENT" });
    const response = await dispatchWebCoreRequest({ async prepareSync() { throw new CoreError(code, "sensitive fixture text"); } }, {
      protocolVersion: 1, requestId: "metadata-error", method: "prepareSync", payload: { profile: { profileId: "fixture" } }
    });
    assert.equal(response.statusCode, 422);
    assert.equal(response.envelope.error.code, code);
    const raw = new CoreError(code, "sensitive fixture text", { details: { path: value.file, failureStage: "prepare_rollouts" } }).toDto();
    for (const dto of [createPublicCoreErrorDto(code, raw), normalizeCliErrorDto(raw)]) {
      assert.equal(dto.code, code);
      assert.equal(dto.retryable, false);
      assert.equal(dto.severity, "error");
      assert.equal(dto.recoveryRequired, false);
      assert.equal(isCanonicalPublicCoreErrorDto(dto), true);
      assert.doesNotMatch(JSON.stringify(dto), /sensitive|rollout-fixture/);
    }
  }
});


test("Switch skips an oversized output but still changes configuration", async t => {
  const value = await fixture(t);
  await writeHeader(value.file, PROVIDER_SESSION_META_MAX_BYTES);
  const before = await digest(value.file);
  const plan = await prepareSwitch({ codexHome: value.home, provider: "provider_long" });
  assert.equal(plan.impact.rolloutFilesToChange, 0);
  const result = await applySwitch({ schemaVersion: 1, planId: plan.planId });
  assert.equal(result.outcome, "partial");
  assert.equal(result.result.configUpdated, true);
  assert.equal(result.result.changedSessionFiles, 0);
  assert.equal(await digest(value.file), before);
  assert.match(await fs.readFile(path.join(value.home, "config.toml"), "utf8"), /model_provider = "provider_long"/);
});
