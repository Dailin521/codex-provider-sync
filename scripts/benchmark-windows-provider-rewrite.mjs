import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { applySessionChanges, collectProviderChanges, createWindowsExclusiveRewriteWorker } from "../src/session-files.js";
import { isFileUpdateTiming } from "../packages/contracts/dist/index.js";

// Development-only: generated D-drive fixtures, no caller-supplied Home/path.
// Measure the production worker without patching its code or copying its writer.
// Earlier instrumented baseline/experiment outputs remain immutable evidence.

async function hashTail(filePath, offset) {
  const hash = createHash("sha256");
  for await (const chunk of fs.createReadStream(filePath, { start: offset })) hash.update(chunk);
  return hash.digest("hex");
}

const round = value => Math.round(value * 1000) / 1000;
function summarize(samples) {
  return Object.fromEntries(Object.keys(samples[0]).map(field => {
    const values = samples.map(value => value[field]).sort((a, b) => a - b);
    return [field, { median: round(values[Math.floor(values.length / 2)]), p95: round(values[Math.ceil(values.length * 0.95) - 1]) }];
  }));
}

if (process.platform !== "win32") throw new Error("This synthetic worker benchmark requires Windows.");
const args = process.argv.slice(2);
if (args.some(value => !["--stress", "--large-only", "--representative"].includes(value) && !/^--samples=[3-7]$/.test(value))) throw new Error("Usage: node scripts/benchmark-windows-provider-rewrite.mjs [--stress|--large-only|--representative] [--samples=3..7]");
const sampleCount = Number(args.find(value => value.startsWith("--samples="))?.split("=")[1] ?? 5);
const variant = "production-native-timing";
// Aggregated size bins only (read-only filesystem metadata, 2026-09-08).
// Bucket means are rounded to 64 KiB. No source paths, IDs or chat bytes.
const representativeBins = [[188,26919], [138,152107], [623,624160], [464,1536726], [502,3035081], [320,5631286], [96,11483069], [28,23206471], [13,45157590], [11,82665606], [4,253442844]];
const representativeSizes = representativeBins.flatMap(([count, mean]) => Array(count).fill(Math.max(65536, Math.round(mean / 65536) * 65536)));
const cases = args.includes("--representative") ? [
  { name: "unequal-representative-2387", count: representativeSizes.length, bytes: null, sizes: representativeSizes, provider: "provider_old" }
] : args.includes("--large-only") ? [
  { name: "equal-2347x64KiB", count: 2347, bytes: 64 * 1024, provider: "openai" },
  { name: "unequal-2347x64KiB", count: 2347, bytes: 64 * 1024, provider: "provider_old" }
] : [
  { name: "equal-32MiB", count: 1, bytes: 32 * 1024 * 1024, provider: "openai" },
  { name: "unequal-32MiB", count: 1, bytes: 32 * 1024 * 1024, provider: "provider_old" },
  { name: "unequal-batch", count: 32, bytes: 1024 * 1024, provider: "provider_old" },
  ...(args.includes("--stress") ? [{ name: "unequal-small-file-stress", count: 2347, bytes: 64 * 1024, provider: "provider_old" }] : [])
];
// The user requires D-drive fixtures. Reject redirection and never use C:\\Temp
// or a real Codex Home, including the helper's TEMP/TMP compilation directory.
const tempParent = path.resolve("D:/Temp");
const parentStat = await fsp.lstat(tempParent);
assert.ok(parentStat.isDirectory() && !parentStat.isSymbolicLink());
assert.equal((await fsp.realpath(tempParent)).toLowerCase(), tempParent.toLowerCase());
const root = await fsp.mkdtemp(path.join(tempParent, "provider-rewrite-timing-"));
const fixtureMtime = new Date("2026-01-02T03:04:05.789Z");
const bodyChunk = Buffer.from("synthetic fixture bytes, not a parsed chat message\n".repeat(22000));

async function runSample(group, iteration) {
  const home = path.join(root, `${group.name}-${iteration}`);
  await fsp.mkdir(path.join(home, "sessions"), { recursive: true });
  const expected = new Map();
  for (let index = 0; index < group.count; index += 1) {
    const file = path.join(home, "sessions", `rollout-${index}.jsonl`);
    const header = JSON.stringify({ type: "session_meta", payload: { id: `fixture-${index}`, model_provider: group.provider } }) + "\n";
    const afterHeader = header.replace(group.provider, "prov_a");
    const handle = await fsp.open(file, "wx");
    try {
      await handle.writeFile(header);
      for (let remaining = group.sizes?.[index] ?? group.bytes; remaining > 0;) {
        const chunk = bodyChunk.subarray(0, Math.min(remaining, bodyChunk.length));
        await handle.writeFile(chunk);
        remaining -= chunk.length;
      }
    } finally { await handle.close(); }
    await fsp.utimes(file, fixtureMtime, fixtureMtime);
    expected.set(file, { before: await fsp.stat(file, { bigint: true }), afterHeader,
      bodyHash: await hashTail(file, Buffer.byteLength(header)), headerDelta: Buffer.byteLength(afterHeader) - Buffer.byteLength(header) });
  }
  const { changes } = await collectProviderChanges(home, "prov_a");
  assert.equal(changes.length, group.count);
  const timingObservations = [];
  const begin = performance.now();
  const applied = await applySessionChanges(changes, {
    windowsRewriteWorkerFactory: async () => {
      return createWindowsExclusiveRewriteWorker({
        spawnImpl(command, workerArgs, options) {
          return spawn(command, workerArgs, { ...options, env: { ...process.env, TEMP: root, TMP: root } });
        }
      });
    },
    onTiming: (timing) => timingObservations.push(timing)
  });
  const applyMs = performance.now() - begin;
  assert.equal(applied.appliedChanges, group.count);
  assert.equal(applied.inPlaceChanges, group.provider === "openai" ? group.count : 0);
  assert.equal(timingObservations.length, 1, "One aggregate per batch, not per file.");
  const timing = timingObservations[0];
  assert.ok(isFileUpdateTiming(timing), "Production must report a complete numeric timing schema.");
  assert.equal(timing.attemptedFiles, group.count);
  assert.equal(timing.measuredFiles, group.count);
  assert.equal(timing.inPlaceFiles, applied.inPlaceChanges);
  assert.equal(timing.rewrittenFiles, applied.appliedChanges - applied.inPlaceChanges);
  assert.equal(timing.skippedFiles, 0);
  for (const [file, { before, afterHeader, bodyHash, headerDelta }] of expected) {
    const after = await fsp.stat(file, { bigint: true });
    assert.equal(after.mtimeNs, before.mtimeNs, "Fixed-millisecond fixture mtime must be restored, not refreshed.");
    assert.equal(after.size, before.size + BigInt(headerDelta));
    if (group.provider === "openai") assert.equal(after.ino, before.ino);
    assert.equal(await hashTail(file, Buffer.byteLength(afterHeader)), bodyHash);
    const handle = await fsp.open(file, "r");
    try {
      const header = Buffer.alloc(Buffer.byteLength(afterHeader));
      await handle.read(header, 0, header.length, 0);
      assert.equal(header.toString("utf8"), afterHeader);
    } finally { await handle.close(); }
  }
  const measurements = { applyMs, ...Object.fromEntries(Object.entries(timing).filter(([key]) => key.endsWith("Ms"))) };
  // This exact synthetic subdirectory is owned by this run; never accept a user path.
  assert.equal(path.dirname(home), root);
  await fsp.rm(home, { recursive: true, force: true });
  return measurements;
}

try {
  const results = [];
  for (const group of cases) {
    const samples = [];
    for (let iteration = 0; iteration <= sampleCount; iteration += 1) {
      process.stderr.write(`D-drive fixture ${group.name}: ${iteration ? `sample ${iteration}/${sampleCount}` : "warm-up"}, ${group.count} files.\n`);
      const sample = await runSample(group, iteration);
      process.stderr.write(`Apply ${round(sample.applyMs)}ms; header ${round(sample.readHeaderMs)}ms; cleanup ${round(sample.cleanupMs)}ms.\n`);
      if (iteration > 0) samples.push(sample); // Discard one warm-up; each sample still includes worker startup.
    }
    const { sizes, ...groupSummary } = group;
    results.push({ ...groupSummary, totalBodyBytes: sizes ? sizes.reduce((a,b) => a+b, 0) : group.bytes * group.count,
      variant, samples: samples.length, rawMeasurementsMs: samples, measurementsMs: summarize(samples),
      verified: ["provider", "body-sha256", "size-delta", "restored-mtime", ...(group.provider === "openai" ? ["file-identity"] : [])] });
    process.stderr.write(`Verified ${group.name}: ${samples.length} samples, mtime/body/size preserved.\n`);
  }
  console.log(JSON.stringify({ platform: process.platform, node: process.version, tempParent, variant, results,
    notes: ["Synthetic temporary data only; no Core/Home/config/SQLite/backup/UI timings.",
      "Unmodified production worker; onTiming reports one aggregate. Earlier instrumented variants are historical reference only.",
      "Stage values are nested totals, not additive. Round-trip minus worker time includes protocol/IPC/scheduling overhead.",
      "restoreMtimeMs includes replacement stat/utimes and native in-place time restoration; flushMs includes native flush calls. These are nested within batch/request/worker totals.",
      "Timestamp fixture is millisecond-aligned; this does not claim greater precision than the current replacement implementation.",
      "Reference timings only; assertions, not timing thresholds, are the gate."] }, null, 2));
} finally {
  assert.ok(path.basename(root).startsWith("provider-rewrite-timing-"));
  assert.equal(path.dirname(root), tempParent);
  await fsp.rm(root, { recursive: true, force: true });
}
