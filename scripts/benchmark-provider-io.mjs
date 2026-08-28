import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { runSync } from "../src/service.js";

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
  for (const mode of ["full-equal", "fast-equal", "full-unequal"]) {
    const home = path.join(root, mode);
    await fsp.mkdir(path.join(home, "sessions"), { recursive: true });
    await fsp.writeFile(path.join(home, "config.toml"), 'model_provider="prov_a"\n');
    const file = path.join(home, "sessions", "rollout-fixture.jsonl");
    const header = JSON.stringify({ type: "session_meta", payload: {
      id: "fixture", model_provider: mode === "full-unequal" ? "provider_old" : "openai"
    } }) + "\n";
    const h = await fsp.open(file, "w");
    try {
      await h.writeFile(header);
      await h.writeFile('{"type":"event_msg","payload":{"type":"user_message","message":"fixture"}}\n');
      for (let n = 0; n < mib * 1024 * 1024; n += block.length) await h.writeFile(block);
      await h.sync();
    } finally { await h.close(); }
    const beforeStat = await fsp.stat(file, { bigint: true });
    const beforeHash = await hashTail(file, Buffer.byteLength(header));
    const before = await counters();
    const start = performance.now();
    const result = await runSync({ codexHome: home, fast: mode === "fast-equal" });
    const ms = performance.now() - start;
    const after = await counters();
    const afterStat = await fsp.stat(file, { bigint: true });
    const headerAfter = header.replace(mode === "full-unequal" ? "provider_old" : "openai", "prov_a");
    assert.equal(await hashTail(file, Buffer.byteLength(headerAfter)), beforeHash);
    const delta = before && Object.fromEntries(["rchar", "wchar", "read_bytes", "write_bytes"].map(k => [k, after[k] - before[k]]));
    results.push({ mode, rolloutBytes: Number(beforeStat.size), ms: Math.round(ms),
      inPlace: result.inPlaceSessionFiles, sameInode: beforeStat.ino === afterStat.ino,
      ...(delta ? { processIo: delta } : {}) });
  }
  console.log(JSON.stringify({ platform: process.platform, results,
    note: "Warm-cache synthetic benchmark. rchar/wchar are logical process I/O, not SSD wear; kernel write_bytes excludes device-internal amplification." }, null, 2));
} finally {
  await fsp.rm(root, { recursive: true, force: true });
}
