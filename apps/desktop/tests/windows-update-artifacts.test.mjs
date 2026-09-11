import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { gzipSync } from "node:zlib";
import { updateAssetNames, auditWindowsUpdateAssets, auditWindowsUpdateConfiguration } from "../scripts/windows-update-artifacts.mjs";
import { prepareElectronWindowsRelease, verifyTaggedSourceVersions } from "../scripts/prepare-electron-windows-release.mjs";

test("stable-updater requires Windows stable version and immutable matching tag/source", () => {
  const input = { version: "1.0.1", releaseRef: "refs/tags/v1.0.1", expectedSha: "a".repeat(40), channel: "stable-updater" };
  assert.equal(prepareElectronWindowsRelease(input).channel, "stable-updater");
  assert.equal(verifyTaggedSourceVersions(input, () => '{"version":"1.0.1"}').version, "1.0.1");
  assert.throws(() => verifyTaggedSourceVersions(input, () => '{"version":"1.0.0"}'));
  assert.throws(() => prepareElectronWindowsRelease({ ...input, releaseRef: "refs/heads/main" }));
  assert.throws(() => prepareElectronWindowsRelease({ ...input, version: "1.0.1-rc.1" }));
  assert.deepEqual(updateAssetNames({ version: "1.0.1", target: "windows-x64", releaseChannel: "stable-updater" }),
    ["latest.yml", "CodexProviderSync-1.0.1-windows-x64-setup.exe.blockmap"]);
  assert.deepEqual(updateAssetNames({ version: "1.0.1", target: "windows-x64", releaseChannel: "stable-manual" }), []);
  assert.deepEqual(updateAssetNames({ version: "1.0.1-rc.1", target: "windows-x64" }), []);
  assert.throws(() => updateAssetNames({ version: "1.0.1", target: "linux-x64", releaseChannel: "stable-updater" }));
});

test("update metadata verifies exact installer version, path, bytes and required blockmap", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-update-artifact-test-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const installer = "CodexProviderSync-1.0.1-windows-x64-setup.exe";
  const bytes = Buffer.from("fixture installer");
  const sha512 = crypto.createHash("sha512").update(bytes).digest("base64");
  const valid = { version: "1.0.1", path: installer, sha512, files: [{ url: installer, size: bytes.length, sha512 }] };
  await fs.writeFile(path.join(root, installer), bytes);
  await fs.writeFile(path.join(root, `${installer}.blockmap`), gzipSync(JSON.stringify({
    version: "2", files: [{ name: "file", offset: 0, sizes: [bytes.length], checksums: ["fixture"] }]
  })));
  const check = async (metadata) => {
    await fs.writeFile(path.join(root, "latest.yml"), JSON.stringify(metadata));
    return auditWindowsUpdateAssets({ assetsRoot: root, version: "1.0.1" });
  };
  assert.equal((await check(valid)).installer, installer);
  for (const invalid of [
    { ...valid, version: "1.0.0" }, { ...valid, path: "../setup.exe" },
    { ...valid, sha512: "wrong" },
    { ...valid, files: [{ ...valid.files[0], size: 1 }] },
    { ...valid, files: [{ ...valid.files[0], sha512: "wrong" }] },
    { ...valid, files: [...valid.files, { url: "https://example.org/setup.exe" }] }
  ]) await assert.rejects(() => check(invalid));
  await fs.writeFile(path.join(root, `${installer}.blockmap`), "invalid gzip blockmap");
  await assert.rejects(() => check(valid));
  await fs.rm(path.join(root, `${installer}.blockmap`));
  await assert.rejects(() => check(valid));
});

test("packaged updater config binds public GitHub repository without disabling publisher verification", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-update-config-test-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const valid = { provider: "github", owner: "Dailin521", repo: "codex-provider-sync" };
  const check = async (config) => {
    await fs.writeFile(path.join(root, "app-update.yml"), JSON.stringify(config));
    return auditWindowsUpdateConfiguration(root);
  };
  assert.equal((await check(valid)).signaturePolicy, "unsigned-no-publisher");
  assert.equal((await check({ ...valid, publisherName: "Example" })).signaturePolicy, "publisher-verification");
  for (const change of [{ provider: "generic" }, { repo: "other" }, { host: "example.org" },
    { token: "fixture" }, { channel: "beta" }, { private: true }]) {
    await assert.rejects(() => check({ ...valid, ...change }));
  }
});
