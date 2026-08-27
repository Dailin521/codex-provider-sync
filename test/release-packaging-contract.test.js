import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const testDirectory = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(testDirectory, "..");

function read(relativePath) {
  return fs.readFileSync(path.join(rootDir, ...relativePath.split("/")), "utf8");
}

test("release packaging preserves the existing GUI updater asset contract", () => {
  const updateService = read("desktop/CodexProviderSync.Core/UpdateService.cs");
  const packaging = read("scripts/package-release-assets.ps1");

  assert.match(updateService, /FindAsset\("CodexProviderSync\.exe"\)/);
  assert.match(updateService, /FindAsset\("CodexProviderSync\.exe\.sha256"\)/);
  assert.match(packaging, /Join-Path \$assetRoot "CodexProviderSync\.exe"/);
  assert.match(packaging, /Set-Content -LiteralPath "\$asset\.sha256"/);
  assert.match(packaging, /must be separate directory trees/);
});

test("release packaging creates a focused Automation ZIP with its protocol and guide", () => {
  const publish = read("scripts/publish-gui.ps1");
  const packaging = read("scripts/package-release-assets.ps1");

  assert.match(publish, /README-AUTOMATION\.zh-CN\.md/);
  assert.match(packaging, /codex-provider-sync-v\$Version-automation-win-x64\.zip/);
  assert.match(packaging, /CodexProviderSync\.Automation\.exe/);
  assert.match(packaging, /automation-protocol-v0\.4\.schema\.json/);
  assert.match(packaging, /README-AUTOMATION\.zh-CN\.md/);
});

test("publish workflow resolves a tag-bound Chinese announcement instead of hardcoding a body", () => {
  const workflow = read(".github/workflows/publish.yml");

  assert.match(workflow, /workflow_dispatch:/);
  assert.match(workflow, /release_tag:/);
  assert.match(workflow, /ref: refs\/tags\/\$\{\{ inputs\.release_tag \}\}/);
  assert.match(workflow, /tag_name: \$\{\{ inputs\.release_tag \}\}/);
  assert.doesNotMatch(workflow, /\bpush:\s*\n\s+tags:/);
  assert.doesNotMatch(workflow, /github\.ref_name|GITHUB_REF_NAME/);
  assert.match(workflow, /read-release-metadata\.js --tag/);
  assert.match(workflow, /body_path: \$\{\{ steps\.release_metadata\.outputs\.release_body_path \}\}/);
  assert.match(workflow, /name: \$\{\{ steps\.release_metadata\.outputs\.release_title \}\}/);
  assert.doesNotMatch(workflow, /^\s+body:\s*\|/m);
});

test("CI requires all four native Electron candidates and their aggregate index", () => {
  const workflow = read(".github/workflows/ci.yml");

  for (const value of [
    "windows-x64",
    "macos-x64",
    "macos-arm64",
    "linux-x64",
    "macos-15-intel",
    "macos-15",
    "desktop:pack:candidate",
    "desktop:stage:candidate",
    "desktop:smoke:candidate:artifacts",
    "desktop:verify:candidate:set"
  ]) assert.match(workflow, new RegExp(value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  assert.match(workflow, /ELECTRON_RELEASE_CANDIDATE_RESULT/);
  assert.match(workflow, /ELECTRON_CANDIDATE_SET_RESULT/);
  assert.match(workflow, /if-no-files-found: error/);
});
