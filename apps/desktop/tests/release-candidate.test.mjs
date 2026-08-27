import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import {
  assertSafeAsarEntries,
  assertSafeProductTextEntry,
  createRuntimeProjection,
  RELEASE_TARGETS
} from "../scripts/release-audit.mjs";
import { resolveCandidateBuild } from "../scripts/resolve-candidate-build.mjs";

const desktopRoot = path.resolve(import.meta.dirname, "..");
const repositoryRoot = path.resolve(desktopRoot, "../..");

async function read(relativePath) {
  return fs.readFile(path.join(repositoryRoot, ...relativePath.split("/")), "utf8");
}

test("candidate identity is injected without mutating the source package version", async () => {
  assert.deepEqual(resolveCandidateBuild({
    channel: "rc",
    runNumber: 42,
    sha: "0123456789abcdef0123456789abcdef01234567",
    target: "windows-x64"
  }), {
    version: "1.0.0-rc.42",
    buildId: "1.0.0-rc.42-0123456789ab-windows-x64",
    commit: "0123456789abcdef0123456789abcdef01234567",
    target: "windows-x64",
    channel: "rc",
    runNumber: 42
  });
  assert.throws(() => resolveCandidateBuild({ channel: "nightly", runNumber: 1, sha: "0123456", target: "windows-x64" }));
  assert.throws(() => resolveCandidateBuild({ channel: "rc", runNumber: -1, sha: "0123456", target: "windows-x64" }));
  const rootManifest = JSON.parse(await read("package.json"));
  const desktopManifest = JSON.parse(await read("apps/desktop/package.json"));
  assert.equal(rootManifest.version, "0.5.0");
  assert.equal(desktopManifest.version, "0.0.0");
  assert.equal(rootManifest.optionalDependencies["better-sqlite3"], "8.7.0");
  assert.equal(desktopManifest.dependencies["better-sqlite3"], "13.0.3");
  assert.equal(desktopManifest.devDependencies.plist, "5.0.0");
  assert.equal(desktopManifest.devDependencies.resedit, "3.1.0");
});

test("release targets use the frozen C9 artifact names", () => {
  const version = "1.0.0-rc.9";
  assert.deepEqual(RELEASE_TARGETS["windows-x64"].assets(version), [
    `CodexProviderSync-${version}-windows-x64-setup.exe`,
    `CodexProviderSync-${version}-windows-x64-portable.zip`
  ]);
  assert.deepEqual(RELEASE_TARGETS["macos-x64"].assets(version), [
    `CodexProviderSync-${version}-macos-x64.dmg`,
    `CodexProviderSync-${version}-macos-x64.zip`
  ]);
  assert.deepEqual(RELEASE_TARGETS["macos-arm64"].assets(version), [
    `CodexProviderSync-${version}-macos-arm64.dmg`,
    `CodexProviderSync-${version}-macos-arm64.zip`
  ]);
  assert.deepEqual(RELEASE_TARGETS["linux-x64"].assets(version), [
    `CodexProviderSync-${version}-linux-x64.AppImage`,
    `CodexProviderSync-${version}-linux-x64.deb`
  ]);
});

test("runtime SBOM projection includes production closure and excludes Desktop build tooling", async () => {
  const projection = await createRuntimeProjection(path.join(repositoryRoot, "package-lock.json"));
  const refs = new Set(projection.components.map((component) => component.ref));
  for (const ref of [
    "@codex-provider-sync/app-ui@0.0.0",
    "@hookform/resolvers@5.9.1",
    "better-sqlite3@13.0.3",
    "electron-updater@6.8.9",
    "react@19.2.8",
    "zod@4.4.3"
  ]) assert.equal(refs.has(ref), true, `Runtime projection is missing ${ref}.`);
  for (const ref of [
    "@playwright/test@1.62.1",
    "electron-builder@26.15.7",
    "plist@5.0.0",
    "resedit@3.1.0",
    "vite@7.3.6"
  ]) assert.equal(refs.has(ref), false, `Runtime projection contains build-only ${ref}.`);
});

test("ASAR policy rejects source maps, fixtures, credentials and key material by path", () => {
  assert.doesNotThrow(() => assertSafeAsarEntries([
    "package.json",
    "out/main/index.js",
    "node_modules/better-sqlite3/lib/index.js"
  ]));
  for (const entry of [
    "out/main/index.js.map",
    "fixtures/profile.json",
    "resources/auth.json",
    "keys/release.p12",
    "config/.env.production",
    "config/credentials.json",
    "out/main/runtime.spec.mjs",
    "rollouts/rollout-example.jsonl"
  ]) {
    assert.throws(() => assertSafeAsarEntries([entry]));
  }
  assert.doesNotThrow(() => assertSafeProductTextEntry("out/main/index.js", "const provider = 'openai';"));
  assert.throws(() => assertSafeProductTextEntry(
    "out/main/index.js",
    "const credential = 'AKIA1234567890ABCDEF';"
  ));
});

test("builder and candidate scripts enforce native fallback, fuses, audit metadata and no publishing", async () => {
  const builder = await read("apps/desktop/electron-builder.yml");
  const buildScript = await read("apps/desktop/scripts/build-candidate.mjs");
  const stageScript = await read("apps/desktop/scripts/stage-candidate.mjs");
  const smokeScript = await read("apps/desktop/scripts/smoke-candidate-artifacts.mjs");
  for (const expected of [
    "node_modules/better-sqlite3/prebuilds/${platform}-${arch}.node",
    "!node_modules/better-sqlite3/prebuilds/!(${platform}-${arch}).node",
    "!node_modules/better-sqlite3/{build,deps,src}/**",
    "enableEmbeddedAsarIntegrityValidation: true",
    "onlyLoadAppFromAsar: true",
    "loadBrowserProcessSpecificV8Snapshot: false",
    "target: nsis",
    "target: dmg",
    "target: AppImage",
    "target: deb"
  ]) assert.match(builder, new RegExp(expected.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  assert.match(buildScript, /"--publish",\s*"never"/);
  assert.match(buildScript, /--config\.extraMetadata\.version=/);
  assert.match(stageScript, /releaseAuthorized:\s*false/);
  assert.match(stageScript, /signingStatus:\s*"unsigned-candidate"/);
  assert.match(stageScript, /sbom\.cyclonedx\.json/);
  assert.match(stageScript, /ARTIFACT_AUDIT_POLICY_PATH/);
  assert.match(smokeScript, /container-verification\.v1\.json/);
  assert.match(smokeScript, /syncRestoreVerified:\s*true/);
  assert.match(smokeScript, /SHA256SUMS\.txt/);
});
