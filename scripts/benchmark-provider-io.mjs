import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { runSync } from "../src/public-api.js";

// Disposable, bounded-memory benchmark. Never use an existing Codex Home.
delete process.env.CODEX_SQLITE_HOME;
const mib = Number(process.argv[2] ?? 32);
if (!Number.isInteger(mib) || mib < 1 || mib > 256) throw new Error("Size must be 1..256 MiB.");
const root = await fsp.mkdtemp(path.join(os.tmpdir(), "provider-io-benchmark-"));
const line = JSON.stringify({ type: "event_msg", payload: { text: "x".repeat(4000) } }) + "\n";
const block = Buffer.from(line.repeat(128));

async function counters() {
  if (process.platform !== "linux") return null;
  const text = await fsp.readFile("/proc/self/io", "utf8");
  return Object.fromEntries(text.trim().split("\n").map(line => {
    const [key, value] = line.split(": ");
    return [key, Number(value)];
  }));
}

async function hashTail(file, offset) {
  const hash = createHash("sha256");
  for await (const chunk of fs.createReadStream(file, { start: offset })) hash.update(chunk);
  return hash.digest("hex");
}

try {
  const results = [];
  for (const mode of ["equal-length", "unequal-length"]) {
    const home = path.join(root, mode);
    await fsp.mkdir(path.join(home, "sessions"), { recursive: true });
    await fsp.writeFile(path.join(home, "config.toml"), 'model_provider="prov_a"\n');
    const file = path.join(home, "sessions", "rollout-fixture.jsonl");
    const header = JSON.stringify({ type: "session_meta", payload: {
      id: "fixture", model_provider: mode === "unequal-length" ? "provider_old" : "openai"
    } }) + "\n";
    const h = await fsp.open(file, "w");
    try {
      await h.writeFile(header);
      const lead = Buffer.from('{"type":"event_msg","payload":{"type":"user_message","message":"fixture"}}\n');
      const requestedBodyBytes = mib * 1024 * 1024;
      await h.writeFile(lead);
      let remaining = requestedBodyBytes - lead.length;
      while (remaining > 0) {
        const next = block.subarray(0, Math.min(remaining, block.length));
        await h.writeFile(next);
        remaining -= next.length;
      }
      await h.sync();
    } finally { await h.close(); }
    const beforeStat = await fsp.stat(file, { bigint: true });
    const beforeHash = await hashTail(file, Buffer.byteLength(header));
    const before = await counters();
    const start = performance.now();
    const result = await runSync({ codexHome: home });
    const ms = performance.now() - start;
    const after = await counters();
    const afterStat = await fsp.stat(file, { bigint: true });
    const headerAfter = header.replace(mode === "unequal-length" ? "provider_old" : "openai", "prov_a");
    assert.equal(await hashTail(file, Buffer.byteLength(headerAfter)), beforeHash);
    const check = await fsp.open(file, "r");
    const actualHeader = Buffer.alloc(Buffer.byteLength(headerAfter));
    try { await check.read(actualHeader, 0, actualHeader.length, 0); }
    finally { await check.close(); }
    assert.equal(actualHeader.toString("utf8"), headerAfter);
    const expectedInPlace = mode === "unequal-length" ? 0 : 1;
    assert.equal(result.inPlaceSessionFiles, expectedInPlace);
    assert.equal(result.rewrittenSessionFiles, mode === "unequal-length" ? 1 : 0);
    assert.equal(Number(afterStat.size), Number(beforeStat.size) + Buffer.byteLength(headerAfter) - Buffer.byteLength(header));
    if (expectedInPlace === 1) assert.equal(afterStat.ino, beforeStat.ino);
    const delta = before && Object.fromEntries(["rchar", "wchar", "read_bytes", "write_bytes"].map(k => [k, after[k] - before[k]]));
    results.push({ mode, rolloutBytes: Number(beforeStat.size), ms: Math.round(ms),
      inPlace: result.inPlaceSessionFiles, sameInode: beforeStat.ino === afterStat.ino,
      ...(delta ? { processIo: delta } : {}) });
  }
  console.log(JSON.stringify({ platform: process.platform, results,
    note: "Provider-only Sync benchmark. Equal-length changes are in-place; unequal-length changes stream to an atomic replacement. rchar/wchar are logical process I/O, not SSD wear." }, null, 2));
} finally {
  await fsp.rm(root, { recursive: true, force: true });
}
