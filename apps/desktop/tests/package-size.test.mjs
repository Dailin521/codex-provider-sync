import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";
import notices from "../scripts/windows-license-archive.cjs";
import afterPack from "../scripts/after-pack.cjs";

const verifier = fileURLToPath(new URL("../scripts/verify-size-budget.mjs", import.meta.url));
async function temporary(t) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-package-size-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  return root;
}

test("Windows notices stay byte-identical in an offline archive, and tampering fails", { skip: process.platform !== "win32" }, async (t) => {
  const root = await temporary(t);
  const original = Buffer.from("<!doctype html><pre>Copyright fixture\r\nAll terms remain. 中文许可\n</pre>\n".repeat(100));
  await fs.writeFile(path.join(root, "LICENSES.chromium.html"), original);
  const result = await notices.archiveChromiumNotices(root);
  assert.equal(result.originalBytes, original.length);
  assert.ok(result.archiveBytes < result.originalBytes);
  assert.deepEqual(await notices.verifyLicenseArchive(root, original), result);
  await assert.rejects(fs.stat(path.join(root, "LICENSES.chromium.html")), { code: "ENOENT" });
  const text = await fs.readFile(path.join(root, "THIRD-PARTY-NOTICES.txt"), "utf8");
  assert.match(text, /Windows Explorer/);
  assert.match(text, /无需联网/);
  await fs.writeFile(path.join(root, "LICENSES.chromium.zip"), "corrupted");
  await assert.rejects(notices.verifyLicenseArchive(root));
});

test("failed notices archive preserves source; other platforms remain untouched", async (t) => {
  const root = await temporary(t);
  const source = path.join(root, "LICENSES.chromium.html");
  await fs.writeFile(source, "fixture-license");
  // Invalid required output destination makes the final rename fail.
  await fs.mkdir(path.join(root, "LICENSES.chromium.zip"));
  if (process.platform === "win32") {
    await assert.rejects(notices.archiveChromiumNotices(root));
    assert.equal(await fs.readFile(source, "utf8"), "fixture-license");
    assert.equal((await fs.readdir(root)).some((entry) => entry.startsWith(".chromium-notices-")), false);
  }
  await afterPack({ electronPlatformName: "darwin", appOutDir: root });
  assert.equal(await fs.readFile(source, "utf8"), "fixture-license");
});

test("afterPack refuses paths outside the explicit build output", async (t) => {
  const root = await temporary(t);
  await assert.rejects(afterPack({ electronPlatformName: "win32", appOutDir: root, packager: { config: { directories: { output: path.join(root, "output") } } } }), /outside/);
});

async function sizeFixture(t) {
  const root = await temporary(t);
  await fs.mkdir(path.join(root, "win-unpacked", "resources"), { recursive: true });
  await fs.mkdir(path.join(root, "win-unpacked", "locales"));
  await fs.writeFile(path.join(root, "win-unpacked", "resources", "app.asar"), "fixture");
  await Promise.all(["en-US.pak", "zh-CN.pak"].map((name) => fs.writeFile(path.join(root, "win-unpacked", "locales", name), "fixture")));
  return root;
}

function verify(...args) {
  return spawnSync(process.execPath, [verifier, ...args], { windowsHide: true, encoding: "utf8", timeout: 10_000 });
}

test("native pack applies Windows budgets only on Windows and never hides missing Windows output", async (t) => {
  const root = await temporary(t);
  const result = verify("--output", root, "--directory", "--native-host");
  if (process.platform === "win32") {
    assert.notEqual(result.status, 0, "Native Windows must reject missing output.");
    const fixture = await sizeFixture(t);
    assert.equal(verify("--output", fixture, "--directory", "--native-host").status, 0);
    await fs.writeFile(path.join(fixture, "win-unpacked", "locales", "fr.pak"), "fixture");
    assert.notEqual(verify("--output", fixture, "--directory", "--native-host").status, 0, "Native Windows still enforces the locale budget.");
  } else {
    assert.equal(result.status, 0, result.stderr);
    assert.deepEqual(JSON.parse(result.stdout), { schemaVersion: 1, budgetPlatform: "win32", hostPlatform: process.platform, outcome: "not-applicable" });
  }
  assert.notEqual(verify("--output", root, "--directory").status, 0, "Explicit Windows verification cannot ignore missing output.");
  const manifest = JSON.parse(await fs.readFile(new URL("../package.json", import.meta.url), "utf8"));
  assert.match(manifest.scripts["pack:dir"], /verify-size-budget\.mjs --directory --native-host$/);
});

test("size verifier uses the requested candidate and version, not stale default output", async (t) => {
  const root = await sizeFixture(t);
  const version = "1.0.0-rc.456";
  await fs.writeFile(path.join(root, `CodexProviderSync-${version}-windows-x64-setup.exe`), "setup");
  await fs.writeFile(path.join(root, `CodexProviderSync-${version}-windows-x64-portable.zip`), "zip");
  const result = verify("--output", root, "--version", version);
  assert.equal(result.status, 0, result.stderr);
  const report = JSON.parse(result.stdout);
  assert.equal(report.outputRoot, root);
  assert.equal(report.version, version);
  assert.equal(report.asarBytes, 7);
  assert.equal(report.unpackedBytes, 21);
  assert.equal(report.nsisBytes, 5);
  assert.notEqual(verify("--output", root, "--version", "1.0.0-rc.999").status, 0);
});

test("directory-only verification needs no containers and enforces minified ASAR budget", async (t) => {
  const root = await sizeFixture(t);
  assert.equal(verify("--output", root, "--directory").status, 0);
  const handle = await fs.open(path.join(root, "win-unpacked", "resources", "app.asar"), "r+");
  await handle.truncate(3 * 1024 * 1024 + 1);
  await handle.close();
  assert.notEqual(verify("--output", root, "--directory").status, 0);
});

test("full verification enforces both final container budgets", async (t) => {
  const root = await sizeFixture(t);
  const version = "1.0.0-rc.456";
  const setup = path.join(root, `CodexProviderSync-${version}-windows-x64-setup.exe`);
  const zip = path.join(root, `CodexProviderSync-${version}-windows-x64-portable.zip`);
  await fs.writeFile(setup, "setup");
  await fs.writeFile(zip, "zip");
  for (const [file, limit, expectedError] of [[setup, 105, /NSIS exceeds 105 MiB/], [zip, 130, /ZIP exceeds 130 MiB/]]) {
    const handle = await fs.open(file, "r+");
    try {
      await handle.truncate(limit * 1024 * 1024);
      assert.equal(verify("--output", root, "--version", version).status, 0);
      await handle.truncate(limit * 1024 * 1024 + 1);
      const result = verify("--output", root, "--version", version);
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, expectedError);
      await handle.truncate(1);
    } finally {
      await handle.close();
    }
  }
});

test("size verifier rejects missing, repeated, unknown options and extra locales", async (t) => {
  const root = await sizeFixture(t);
  for (const args of [["--output"], ["--mystery"], ["--directory", "--directory"], ["--version", "../bad"]]) {
    assert.notEqual(verify(...args).status, 0);
  }
  await fs.writeFile(path.join(root, "win-unpacked", "locales", "fr.pak"), "fixture");
  assert.notEqual(verify("--output", root, "--directory").status, 0);
});
