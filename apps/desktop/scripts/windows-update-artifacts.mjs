import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { createRequire } from "node:module";
import { gunzipSync } from "node:zlib";
import { assertDesktopArtifactVersion } from "./desktop-artifact-version.mjs";

// Use the YAML parser supplied by the pinned updater dependency itself.
const require = createRequire(import.meta.url);
const { load } = createRequire(require.resolve("electron-updater"))("js-yaml");

export function updateAssetNames({ target, version, releaseChannel }) {
  assertDesktopArtifactVersion({ target, version, releaseChannel });
  return releaseChannel === "stable-updater"
    ? ["latest.yml", `CodexProviderSync-${version}-windows-x64-setup.exe.blockmap`] : [];
}

export async function auditWindowsUpdateConfiguration(resources) {
  const config = load(await fs.readFile(path.join(resources, "app-update.yml"), "utf8"));
  assert.equal(config.provider, "github");
  assert.equal(config.owner, "Dailin521");
  assert.equal(config.repo, "codex-provider-sync");
  assert.ok(!config.host || config.host === "github.com", "Unexpected update host.");
  assert.ok(!config.channel || config.channel === "latest", "Unexpected update channel.");
  assert.ok(!config.private && !config.token, "Public updates must not contain credentials.");
  // No signature override: electron-updater verifies a configured publisher;
  // the existing unsigned channel naturally has no publisherName.
  return { provider: config.provider, owner: config.owner, repo: config.repo,
    signaturePolicy: config.publisherName == null ? "unsigned-no-publisher" : "publisher-verification" };
}

export async function auditWindowsUpdateAssets({ assetsRoot, version }) {
  assertDesktopArtifactVersion({ version, target: "windows-x64", releaseChannel: "stable-updater" });
  const installer = `CodexProviderSync-${version}-windows-x64-setup.exe`;
  const metadata = load(await fs.readFile(path.join(assetsRoot, "latest.yml"), "utf8"));
  assert.equal(metadata.version, version, "Update version must match the tagged artifact.");
  assert.equal(metadata.path, installer, "Update path must select the exact installer.");
  assert.ok(Array.isArray(metadata.files) && metadata.files.length > 0);
  const entry = metadata.files.find((file) => file.url === installer);
  assert.ok(entry, "Update metadata must include the installer.");
  for (const file of metadata.files) {
    assert.ok([installer, `CodexProviderSync-${version}-windows-x64-portable.zip`].includes(file.url),
      "Update metadata contains an unexpected asset or external URL.");
    const assetPath = path.join(assetsRoot, file.url);
    const stat = await fs.lstat(assetPath);
    assert.ok(stat.isFile() && !stat.isSymbolicLink());
    assert.equal(file.size, stat.size, "Update size mismatch.");
    const hash = crypto.createHash("sha512");
    const handle = await fs.open(assetPath, "r");
    try { for await (const chunk of handle.createReadStream()) hash.update(chunk); }
    finally { await handle.close(); }
    assert.equal(file.sha512, hash.digest("base64"), "Update SHA512 mismatch.");
  }
  assert.equal(metadata.sha512, entry.sha512);
  const blockmap = await fs.lstat(path.join(assetsRoot, `${installer}.blockmap`));
  assert.ok(blockmap.isFile() && !blockmap.isSymbolicLink() && blockmap.size > 0,
    "Installer differential blockmap is required.");
  const blocks = JSON.parse(gunzipSync(await fs.readFile(path.join(assetsRoot, `${installer}.blockmap`)),
    { maxOutputLength: 32 * 1024 * 1024 }).toString("utf8"));
  assert.equal(blocks.version, "2", "Unsupported installer blockmap format.");
  assert.ok(Array.isArray(blocks.files) && blocks.files.length > 0);
  for (const file of blocks.files) {
    assert.ok(Number.isSafeInteger(file.offset) && file.offset >= 0);
    assert.ok(Array.isArray(file.sizes) && file.sizes.length > 0);
    assert.equal(file.checksums?.length, file.sizes.length);
    assert.ok(file.sizes.every((size) => Number.isSafeInteger(size) && size > 0));
    assert.ok(file.checksums.every((checksum) => typeof checksum === "string" && checksum.length > 0));
    assert.ok(file.offset + file.sizes.reduce((sum, size) => sum + size, 0) <= entry.size,
      "Installer blockmap extends beyond the installer.");
  }
  return { version, installer, installerSha512: entry.sha512, blockmap: `${installer}.blockmap` };
}
