import test, { afterEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { WINDOWS_LOCK_PROBE_SCRIPT, parseWindowsLockProbeResult } from "../src/windows-lock-probe.js";

const paths = ["D:\\sample\\正在使用[1].jsonl", "D:\\sample\\available.jsonl"];
const response = (overrides = {}) => JSON.stringify({ schemaVersion: 1, checkedCount: 2, lockedIndices: [0], ...overrides });
const temporaryRoots = [];
afterEach(async () => {
  for (const root of temporaryRoots.splice(0)) await fs.rm(root, { recursive: true, force: true });
});

test("Windows lock probe maps ASCII indices back to exact original Unicode paths", () => {
  assert.deepEqual(parseWindowsLockProbeResult(response(), paths), [paths[0]]);
  assert.deepEqual(parseWindowsLockProbeResult(response({ lockedIndices: [] }), paths), []);
  assert.deepEqual(parseWindowsLockProbeResult(response({ lockedIndices: [1, 0] }), paths), [paths[1], paths[0]]);
});

test("Windows lock probe rejects missing, partial or corrupt results instead of treating them as writable", () => {
  for (const output of ["", "garbled-path", "null", "{}", response() + response(),
    response({ schemaVersion: 2 }), response({ checkedCount: 1 }), response({ checkedCount: "2" }),
    response({ lockedIndices: null }), response({ lockedIndices: 0 }), response({ extra: true }),
    ...[[-1], [2], [0.5], ["0"], [0, 0]].map((lockedIndices) => response({ lockedIndices }))]) {
    assert.throws(() => parseWindowsLockProbeResult(output, paths), /Windows rollout lock probe response/);
  }
});

test("native Windows probe identifies readable-but-locked Unicode paths even with ASCII stdout", { skip: process.platform !== "win32" }, async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-lock-probe-"));
  temporaryRoots.push(root);
  const files = [path.join(root, "正在使用[1].jsonl"), path.join(root, "writable.jsonl")];
  await Promise.all(files.map((file) => fs.writeFile(file, "synthetic first line\n")));
  const manifest = path.join(root, "paths.json");
  await fs.writeFile(manifest, JSON.stringify(files), "utf8");
  // Acquire the real lock and execute the production probe in the same process,
  // with a deliberately lossy output encoding. No race or timeout-based holder.
  const script = `& {
    param([string]$manifestPath)
    $ErrorActionPreference = 'Stop'
    [Console]::OutputEncoding = [System.Text.Encoding]::ASCII
    [string[]]$paths = Get-Content -Raw -Encoding UTF8 -LiteralPath $manifestPath | ConvertFrom-Json
    $held = [System.IO.File]::Open($paths[0], 'Open', 'ReadWrite', 'Read')
    try { ${WINDOWS_LOCK_PROBE_SCRIPT} $manifestPath } finally { $held.Close() }
  }`;
  const result = spawnSync("powershell.exe", ["-NoProfile", "-NonInteractive", "-Command", script, manifest], {
    encoding: "utf8", windowsHide: true, timeout: 10_000
  });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /^[\x00-\x7f]+$/);
  assert.deepEqual(parseWindowsLockProbeResult(result.stdout, files), [files[0]]);
  assert.deepEqual(await Promise.all(files.map((file) => fs.readFile(file, "utf8"))), files.map(() => "synthetic first line\n"));
});
