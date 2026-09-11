import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import { build as buildPlist } from "plist";
import {
  assertSafeAsarEntries,
  assertSafeProductTextEntry,
  createRuntimeProjection,
  isAuditedProductTextEntry,
  parseMacInfoPlist,
  RELEASE_TARGETS
} from "../scripts/release-audit.mjs";
import { prepareElectronWindowsRelease, verifyTaggedSourceVersions } from "../scripts/prepare-electron-windows-release.mjs";
import { assertDesktopArtifactVersion } from "../scripts/desktop-artifact-version.mjs";
import { readReleaseMetadata } from "../../../scripts/read-release-metadata.js";
import { DESKTOP_RELEASE_BASE_VERSION, resolveCandidateBuild } from "../scripts/resolve-candidate-build.mjs";
import {
  hasSuccessfulCiGateJob,
  assertNoExistingRelease,
  selectExactSuccessfulCiRuns,
  verifyExactMainCiGate
} from "../scripts/verify-exact-ci-gate.mjs";

const desktopRoot = path.resolve(import.meta.dirname, "..");
const repositoryRoot = path.resolve(desktopRoot, "../..");

async function read(relativePath) {
  return fs.readFile(path.join(repositoryRoot, ...relativePath.split("/")), "utf8");
}

test("stable Windows announcement satisfies the repository release metadata contract", () => {
  const metadata = readReleaseMetadata({ rootDir: repositoryRoot, tag: "v1.0.0" });
  assert.equal(metadata.title, "v1.0.0 - Windows Electron 正式版");
  assert.match(metadata.body, /未签名/);
  assert.match(metadata.body, /手动安装/);
  assert.match(metadata.body, /更新按钮不能完成迁移/);
});

test("candidate identity is injected without mutating the source package version", async () => {
  const rootManifest = JSON.parse(await read("package.json"));
  const desktopManifest = JSON.parse(await read("apps/desktop/package.json"));
  assert.equal(DESKTOP_RELEASE_BASE_VERSION, rootManifest.version);
  assert.deepEqual(resolveCandidateBuild({
    channel: "rc",
    runNumber: 42,
    sha: "0123456789abcdef0123456789abcdef01234567",
    target: "windows-x64"
  }), {
    version: `${rootManifest.version}-rc.42`,
    buildId: `${rootManifest.version}-rc.42-0123456789ab-windows-x64`,
    commit: "0123456789abcdef0123456789abcdef01234567",
    target: "windows-x64",
    channel: "rc",
    runNumber: 42
  });
  assert.throws(() => resolveCandidateBuild({ channel: "nightly", runNumber: 1, sha: "0123456", target: "windows-x64" }));
  assert.throws(() => resolveCandidateBuild({ channel: "rc", runNumber: -1, sha: "0123456", target: "windows-x64" }));
  assert.deepEqual(resolveCandidateBuild({
    channel: "rc", runNumber: 1, sha: "0123456", target: "windows-x64", baseVersion: "1.0.1"
  }), {
    version: "1.0.1-rc.1", buildId: "1.0.1-rc.1-0123456-windows-x64", commit: "0123456",
    target: "windows-x64", channel: "rc", runNumber: 1
  });
  for (const baseVersion of ["1.0.01", "1.1.0", "1.0.1-rc.1"]) {
    assert.throws(() => resolveCandidateBuild({ channel: "rc", runNumber: 1, sha: "0123456", target: "windows-x64", baseVersion }));
  }
  assert.equal(desktopManifest.version, rootManifest.version);
  assert.equal(rootManifest.optionalDependencies["better-sqlite3"], "8.7.0");
  assert.equal(desktopManifest.dependencies["better-sqlite3"], "13.0.3");
  assert.equal(desktopManifest.homepage, "https://github.com/Dailin521/codex-provider-sync#readme");
  assert.equal(desktopManifest.devDependencies.plist, "5.0.0");
  assert.equal(desktopManifest.devDependencies.resedit, "3.1.0");
});

test("Windows release preparation accepts only an existing immutable RC tag and full SHA", () => {
  const sha = "0123456789abcdef0123456789abcdef01234567";
  assert.deepEqual(prepareElectronWindowsRelease({
    releaseRef: "refs/tags/v1.0.0-rc.42",
    version: "1.0.0-rc.42",
    expectedSha: sha
  }), {
    releaseRef: "refs/tags/v1.0.0-rc.42",
    releaseTag: "v1.0.0-rc.42",
    version: "1.0.0-rc.42",
    channel: "rc",
    commit: sha,
    target: "windows-x64",
    buildId: "1.0.0-rc.42-0123456789ab-windows-x64"
  });
  assert.throws(() => prepareElectronWindowsRelease({
    releaseRef: "refs/tags/v1.0.0",
    version: "1.0.0",
    expectedSha: sha
  }));
  assert.throws(() => prepareElectronWindowsRelease({
    releaseRef: "refs/tags/v1.0.0-beta.42",
    version: "1.0.0-beta.42",
    expectedSha: sha
  }));
  assert.throws(() => prepareElectronWindowsRelease({
    releaseRef: "refs/tags/v1.0.0-rc.41",
    version: "1.0.0-rc.42",
    expectedSha: sha
  }));
  assert.throws(() => prepareElectronWindowsRelease({
    releaseRef: "refs/tags/v1.0.0-rc.42",
    version: "1.0.0-rc.42",
    expectedSha: "0123456"
  }));
  assert.doesNotThrow(() => prepareElectronWindowsRelease({
    releaseRef: "refs/tags/v1.0.1-rc.1",
    version: "1.0.1-rc.1",
    expectedSha: sha
  }));
});

test("stable Windows preparation requires explicit manual channel and exact strict patch tag/version", () => {
  const input = { releaseRef: "refs/tags/v1.0.0", version: "1.0.0", expectedSha: "a".repeat(40), channel: "stable-manual" };
  assert.deepEqual(prepareElectronWindowsRelease(input), {
    releaseRef: input.releaseRef, releaseTag: "v1.0.0", version: "1.0.0", channel: "stable-manual",
    commit: input.expectedSha, target: "windows-x64", buildId: "1.0.0-aaaaaaaaaaaa-windows-x64"
  });
  for (const change of [
    { channel: "rc" }, { channel: "stable" }, { version: "1.0.01" }, { version: "1.1.0" },
    { version: "1.0.0-rc.1" }, { releaseRef: "refs/heads/main" }, { expectedSha: "aaaaaaa" }
  ]) assert.throws(() => prepareElectronWindowsRelease({ ...input, ...change }));
  assert.doesNotThrow(() => prepareElectronWindowsRelease({ ...input, releaseRef: "refs/tags/v1.0.1", version: "1.0.1" }));
});

test("artifact build, stage and smoke accept stable only for explicit Windows manual release", async () => {
  for (const target of Object.keys(RELEASE_TARGETS)) {
    assert.doesNotThrow(() => assertDesktopArtifactVersion({ version: "1.0.0-rc.42", target }));
    assert.throws(() => assertDesktopArtifactVersion({ version: "1.0.0", target }));
    if (target === "windows-x64") {
      assert.doesNotThrow(() => assertDesktopArtifactVersion({ version: "1.0.0", target, releaseChannel: "stable-manual" }));
    } else {
      assert.throws(() => assertDesktopArtifactVersion({ version: "1.0.0", target, releaseChannel: "stable-manual" }));
    }
  }
  assert.doesNotThrow(() => assertDesktopArtifactVersion({ version: "1.0.1", target: "windows-x64", releaseChannel: "stable-manual" }));
  assert.doesNotThrow(() => assertDesktopArtifactVersion({ version: "1.0.1-rc.1", target: "windows-x64" }));
  for (const version of ["1.0.01", "1.1.0", "1.0.0-rc.1", "1.0.0; echo unsafe"]) {
    assert.throws(() => assertDesktopArtifactVersion({ version, target: "windows-x64", releaseChannel: "stable-manual" }));
  }
  for (const file of ["build-candidate", "stage-candidate", "smoke-candidate-artifacts"]) {
    assert.match(await read(`apps/desktop/scripts/${file}.mjs`), /assertDesktopArtifactVersion\(\{ version, target, releaseChannel: process\.env\.CPS_RELEASE_CHANNEL \}\)/);
  }
});

test("release preparation verifies both manifests at the tagged SHA, not dispatch HEAD", async () => {
  for (const [version, channel] of [["1.0.1", "stable-manual"], ["1.0.1-rc.42", "rc"]]) {
    const input = { releaseRef: `refs/tags/v${version}`, version, expectedSha: "a".repeat(40), channel };
    const files = [];
    const readAtRef = (ref, file) => {
      assert.equal(ref, input.expectedSha);
      files.push(file);
      return JSON.stringify({ version: "1.0.1" });
    };
    assert.equal(verifyTaggedSourceVersions(input, readAtRef).version, version);
    assert.deepEqual(files, ["package.json", "apps/desktop/package.json"]);
    for (const badFile of files) {
      for (const badVersion of ["1.0.0", "1.0.2", "1.0.1-rc.42", undefined]) {
        assert.throws(() => verifyTaggedSourceVersions(input, (_ref, file) =>
          JSON.stringify({ version: file === badFile ? badVersion : "1.0.1" })), /Tagged .* must match/);
      }
    }
    assert.throws(() => verifyTaggedSourceVersions(input, () => "{"));
  }
  const workflow = await read(".github/workflows/publish-electron-windows.yml");
  assert.equal(workflow.match(/--verify-source-manifests/g).length, 2);
});

test("existing stable releases cannot be overwritten and ambiguous API errors fail closed", async () => {
  const input = { repository: "Dailin521/codex-provider-sync", releaseTag: "v1.0.0", token: "test-token" };
  await assertNoExistingRelease({ ...input, fetchImpl: async () => ({ ok: false, status: 404 }) });
  await assert.rejects(() => assertNoExistingRelease({ ...input, fetchImpl: async () => ({ ok: true, status: 200 }) }), /already exists/);
  await assert.rejects(() => assertNoExistingRelease({ ...input, fetchImpl: async () => ({ ok: false, status: 403 }) }), /HTTP 403/);
  for (const releaseTag of ["v1.0.1", "v1.0.1-rc.42", "v1.0.12"]) {
    await assertNoExistingRelease({ ...input, releaseTag, fetchImpl: async () => ({ ok: false, status: 404 }) });
    await assert.rejects(() => assertNoExistingRelease({ ...input, releaseTag, fetchImpl: async () => ({ ok: true, status: 200 }) }), /already exists/);
  }
  for (const releaseTag of ["v1.0.01", "v1.1.0", "v1.0.1-rc.01", "v1.0.1-beta.1"]) {
    await assert.rejects(() => assertNoExistingRelease({ ...input, releaseTag, fetchImpl: async () => assert.fail("invalid tag must not request GitHub") }), /supported Windows/);
  }
});

test("release preparation requires a same-SHA push ci-gate, never a PR result", async () => {
  const sha = "0123456789abcdef0123456789abcdef01234567";
  const runs = {
    workflow_runs: [
      { id: 10, head_sha: sha, head_branch: "release", event: "push", conclusion: "success", path: ".github/workflows/ci.yml" },
      { id: 9, head_sha: sha, head_branch: "main", event: "pull_request", conclusion: "success", path: ".github/workflows/ci.yml" },
      { id: 8, head_sha: "f".repeat(40), head_branch: "main", event: "push", conclusion: "success", path: ".github/workflows/ci.yml" },
      { id: 7, head_sha: sha.toUpperCase(), head_branch: "main", event: "push", conclusion: "success", path: ".github/workflows/ci.yml" }
    ]
  };
  assert.deepEqual(selectExactSuccessfulCiRuns(runs, sha).map((run) => run.id), [7]);
  assert.equal(hasSuccessfulCiGateJob({ jobs: [{ name: "ci-gate", conclusion: "success" }] }), true);
  assert.equal(hasSuccessfulCiGateJob({ jobs: [{ name: "ci-gate", conclusion: "failure" }] }), false);

  const calls = [];
  const fetchImpl = async (url) => {
    calls.push(url);
    if (url.includes("/workflows/ci.yml/runs?")) return { ok: true, status: 200, json: async () => runs };
    if (url.endsWith("/runs/7/jobs?filter=latest&per_page=100")) {
      return { ok: true, status: 200, json: async () => ({ jobs: [{ name: "ci-gate", conclusion: "success" }] }) };
    }
    throw new Error(`Unexpected request: ${url}`);
  };
  assert.deepEqual(await verifyExactMainCiGate({
    repository: "Dailin521/codex-provider-sync",
    expectedSha: sha,
    token: "test-token",
    fetchImpl
  }), { runId: 7, commit: sha });
  assert.equal(calls.length, 2);

  await assertNoExistingRelease({
    repository: "Dailin521/codex-provider-sync",
    releaseTag: "v1.0.0-rc.42",
    token: "test-token",
    fetchImpl: async () => ({ ok: false, status: 404, json: async () => ({}) })
  });
  await assert.rejects(() => assertNoExistingRelease({
    repository: "Dailin521/codex-provider-sync",
    releaseTag: "v1.0.0-rc.42",
    token: "test-token",
    fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({}) })
  }), /already exists/);
});

test("macOS release audit parses XML Info.plist buffers as XML", () => {
  const expected = {
    ElectronAsarIntegrity: {
      "Resources/app.asar": { algorithm: "SHA256", hash: "a".repeat(64) }
    }
  };
  assert.deepEqual(parseMacInfoPlist(Buffer.from(buildPlist(expected), "utf8")), expected);
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
  assert.equal(isAuditedProductTextEntry("out/renderer/assets/logo.svg"), true);
  assert.equal(isAuditedProductTextEntry("out/renderer/assets/manifest.webmanifest"), true);
  assert.equal(isAuditedProductTextEntry("out/renderer/assets/image.png"), false);
  assert.throws(() => assertSafeProductTextEntry(
    "out/renderer/assets/logo.svg",
    "<svg><text>ghp_123456789012345678901234567890123456</text></svg>"
  ));
  assert.throws(() => assertSafeProductTextEntry(
    "out/main/runtime.js",
    "const gate = '__CPS_DESKTOP_FORCE_BETTER_SQLITE3__';"
  ));
});

test("builder and candidate scripts enforce native fallback, fuses, audit metadata and no publishing", async () => {
  const attributes = await read(".gitattributes");
  const builder = await read("apps/desktop/electron-builder.yml");
  const buildScript = await read("apps/desktop/scripts/build-candidate.mjs");
  const stageScript = await read("apps/desktop/scripts/stage-candidate.mjs");
  const smokeScript = await read("apps/desktop/scripts/smoke-candidate-artifacts.mjs");
  const sandboxHelper = await read("apps/desktop/scripts/configure-linux-sandbox.mjs");
  const workflow = await read(".github/workflows/ci.yml");
  const desktopJob = workflow.match(
    /^  electron-desktop:\r?\n[\s\S]*?(?=^  electron-release-candidate:)/m
  )?.[0];
  assert.ok(desktopJob, "The cross-platform Electron desktop job must remain present.");
  const candidateJob = workflow.match(
    /^  electron-release-candidate:\r?\n[\s\S]*?(?=^  electron-candidate-set:)/m
  )?.[0];
  assert.ok(candidateJob, "The native release-candidate job must remain present.");
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
    "target: deb",
    "Name: Codex Provider Sync",
    "appImage:",
    "artifactName: CodexProviderSync-${version}-linux-x64.AppImage",
    "deb:",
    "artifactName: CodexProviderSync-${version}-linux-x64.deb"
  ]) assert.match(builder, new RegExp(expected.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  assert.doesNotMatch(
    builder,
    /artifactName:\s*CodexProviderSync-\$\{version\}-linux-\$\{arch\}/,
    "Linux package targets must not expose electron-builder's x86_64/amd64 arch aliases."
  );
  assert.match(buildScript, /"--publish",\s*"never"/);
  assert.match(buildScript, /--config\.extraMetadata\.version=/);
  assert.match(buildScript, /--config\.directories\.output=\$\{outputRoot\}/);
  assert.match(buildScript, /if \(target === "windows-x64"\) \{[\s\S]*?verify-size-budget\.mjs[\s\S]*?"--output", outputRoot,[\s\S]*?"--version", version/);
  assert.match(buildScript, /if \(sizeCheck\.status !== 0\) throw/);
  assert.doesNotMatch(buildScript, /"--directory"/);
  assert.match(buildScript, /CPS_DESKTOP_RELEASE_AUTHORIZED:\s*"false"/);
  assert.match(attributes, /^package-lock\.json text eol=lf$/m);
  assert.match(attributes, /^apps\/desktop\/release\/artifact-audit-policy\.v1\.json text eol=lf$/m);
  assert.match(
    buildScript,
    /"linux-x64":\s*\{[\s\S]*?configOverrides:\s*\["--config\.productName=CodexProviderSync"\]/,
    "The Linux candidate must keep its setuid sandbox install path free of spaces."
  );
  assert.match(stageScript, /releaseAuthorized:\s*false/);
  assert.match(stageScript, /signingStatus:\s*"unsigned-candidate"/);
  assert.match(stageScript, /sbom\.cyclonedx\.json/);
  assert.match(stageScript, /ARTIFACT_AUDIT_POLICY_PATH/);
  assert.match(smokeScript, /container-verification\.v1\.json/);
  assert.match(smokeScript, /syncRestoreVerified:\s*true/);
  assert.match(smokeScript, /SHA256SUMS\.txt/);
  assert.match(smokeScript, /configure-linux-sandbox\.mjs/);
  assert.match(smokeScript, /verbatimSymlinks:\s*true/);
  assert.match(
    smokeScript,
    /path\.join\(\s*debRoot,\s*"opt",\s*"CodexProviderSync",\s*"codex-provider-sync"\s*\)/,
    "The final-container smoke must execute the deb candidate from its real space-free install path."
  );
  assert.doesNotMatch(smokeScript, /run\("sudo",\s*\["(?:chown|chmod)"/);
  for (const expected of [
    "O_NOFOLLOW",
    "handle.chown(0, 0)",
    "handle.chmod(0o4755)",
    "opened.dev !== before.dev",
    "opened.ino !== before.ino",
    "opened.nlink !== 1n"
  ]) assert.match(sandboxHelper, new RegExp(expected.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  assert.match(workflow, /configure-linux-sandbox\.mjs node_modules\/electron\/dist chrome-sandbox/);
  assert.match(workflow, /configure-linux-sandbox\.mjs dist-desktop\/linux-unpacked chrome-sandbox/);
  assert.match(desktopJob, /Upload Electron failure traces/);
  assert.match(desktopJob, /if: failure\(\)/);
  assert.match(desktopJob, /apps\/desktop\/test-results\/\*\*\/trace\.zip/);
  assert.match(desktopJob, /apps\/desktop\/test-results\/\*\*\/error-context\.md/);
  assert.match(candidateJob, /Verify candidate input byte identity/);
  assert.match(candidateJob, /'package-lock\.json','apps\/desktop\/release\/artifact-audit-policy\.v1\.json'/);
  assert.match(candidateJob, /git',\['ls-files','--eol',\.\.\.files\]/);
  assert.match(candidateJob, /line\.includes\('w\/lf'\)/);
  assert.equal(
    [...workflow.matchAll(/run: node -e "require\('electron'\)"/g)].length,
    2,
    "Both Linux Electron jobs must install the pinned runtime before configuring its sandbox."
  );
  assert.doesNotMatch(workflow, /sudo\s+(?:chown|chmod)\b/);
});

test("a newer failed or pending main run cannot fall back to an older successful run", () => {
  const sha = "a".repeat(40);
  const green = { id: 1, head_sha: sha, head_branch: "main", event: "push", conclusion: "success", path: ".github/workflows/ci.yml" };
  for (const conclusion of ["failure", "cancelled", "skipped", null]) {
    assert.deepEqual(selectExactSuccessfulCiRuns({ workflow_runs: [green, { ...green, id: 2, conclusion }] }, sha), []);
  }
});

test("Windows Electron release workflow defaults to RC, explicitly allows manual stable, and stays Draft-only", async () => {
  const workflow = await read(".github/workflows/publish-electron-windows.yml");
  const preparation = await read("apps/desktop/scripts/prepare-electron-windows-release.mjs");
  const ciGate = await read("apps/desktop/scripts/verify-exact-ci-gate.mjs");
  assert.match(preparation, /PATCH_VERSION/);
  assert.doesNotMatch(preparation, /alpha\|beta\|rc/);
  assert.match(workflow, /workflow_dispatch:/);
  assert.match(workflow, /run: test "\$GITHUB_REF" = refs\/heads\/main/);
  assert.match(workflow, /contents: write\r?\n\s+actions: read/);
  assert.match(workflow, /create_draft_release:/);
  assert.match(workflow, /default: false/);
  assert.match(workflow, /refs\/tags\/v1\.0\.x-rc\.N/);
  assert.match(workflow, /git merge-base --is-ancestor/);
  assert.match(workflow, /Require a successful exact push ci-gate/);
  assert.match(ciGate, /run\.event === "push"/);
  assert.match(ciGate, /run\.head_branch === "main"/);
  assert.match(ciGate, /job\.name === "ci-gate"/);
  assert.match(workflow, /CPS_CANDIDATE_TARGET: windows-x64/);
  assert.match(workflow, /npm run desktop:pack:candidate/);
  assert.match(workflow, /npm run desktop:stage:candidate/);
  assert.match(workflow, /npm run desktop:smoke:candidate:artifacts/);
  assert.match(workflow, /windows-x64-setup\.exe/);
  assert.match(workflow, /windows-x64-portable\.zip/);
  assert.match(workflow, /sbom\.cyclonedx\.json/);
  assert.match(workflow, /SHA256SUMS\.txt/);
  assert.match(workflow, /environment: electron-windows-release/);
  assert.match(workflow, /Recheck exact push ci-gate and refuse an existing Release/);
  assert.match(ciGate, /A GitHub Release already exists for this immutable tag/);
  assert.match(workflow, /gh release create/);
  assert.match(workflow, /release_channel:[\s\S]*default: rc[\s\S]*- stable-manual/);
  assert.match(workflow, /CPS_RELEASE_CHANNEL: \$\{\{ needs\.verify-input\.outputs\.channel \}\}/);
  assert.match(workflow, /release_flags=\(--prerelease\)/);
  assert.match(workflow, /if \[ "\$CPS_RELEASE_CHANNEL" = stable-manual \]; then\s+release_flags=\(--prerelease=false\)/);
  assert.match(workflow, /--verify-tag[\s\S]*--draft "\$\{release_flags\[@\]}" --latest=false/);
  assert.match(workflow, /sha256sum --check metadata\/SHA256SUMS\.txt/);
  assert.match(workflow, /metadata\/audit-report\.v1\.json metadata\/candidate-staging\.v1\.json/);
  assert.doesNotMatch(workflow, /action-gh-release|--clobber|gh release edit/);
  assert.doesNotMatch(workflow, /npm publish|publish:npm|publish-gui\.ps1|package-release-assets\.ps1|CodexProviderSync\.Automation/i);
});
