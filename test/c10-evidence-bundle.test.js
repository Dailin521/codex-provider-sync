import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import {
  assertRedacted,
  normalizeCandidateIndex,
  normalizeRequiredJobs,
  REQUIRED_JOBS,
  REQUIRED_TARGETS
} from "../scripts/write-c10-evidence-bundle.mjs";

const testDirectory = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(testDirectory, "..");
const sha = "a".repeat(40);
const hash = "b".repeat(64);
const expectedAssets = {
  "windows-x64": ["CodexProviderSync-1.0.0-rc.42-windows-x64-portable.zip", "CodexProviderSync-1.0.0-rc.42-windows-x64-setup.exe"],
  "macos-x64": ["CodexProviderSync-1.0.0-rc.42-macos-x64.dmg", "CodexProviderSync-1.0.0-rc.42-macos-x64.zip"],
  "macos-arm64": ["CodexProviderSync-1.0.0-rc.42-macos-arm64.dmg", "CodexProviderSync-1.0.0-rc.42-macos-arm64.zip"],
  "linux-x64": ["CodexProviderSync-1.0.0-rc.42-linux-x64.AppImage", "CodexProviderSync-1.0.0-rc.42-linux-x64.deb"]
};

function candidateIndex() {
  return {
    schemaVersion: 1,
    scope: "ci-candidate-index",
    releaseAuthorized: false,
    version: "1.0.0-rc.42",
    commit: sha,
    targets: REQUIRED_TARGETS.map((target) => ({
      target,
      version: "1.0.0-rc.42",
      commit: sha,
      buildId: `1.0.0-rc.42-${target}`,
      lockfileSha256: hash,
      toolVersions: { electron: "44.0.0" },
      fusePolicy: "c9-v1",
      artifactAuditPolicy: { schemaVersion: 1, sha256: hash },
      manifestSha256: hash,
      assets: expectedAssets[target].map((name, index) => ({ name, sizeBytes: 10 + index, sha256: hash }))
    }))
  };
}

test("C10 required jobs must be exact and all successful", () => {
  const jobs = Object.fromEntries(REQUIRED_JOBS.map((id) => [id, { result: "success" }]));
  assert.deepEqual(normalizeRequiredJobs(jobs).map((entry) => entry.id), REQUIRED_JOBS);
  jobs.test.result = "skipped";
  assert.throws(() => normalizeRequiredJobs(jobs), /did not succeed: test/);
  jobs.test.result = "success";
  jobs.unreviewed = { result: "success" };
  assert.throws(() => normalizeRequiredJobs(jobs), /inventory changed/);
});

test("C10 candidate set binds four native targets to the tested commit", () => {
  const normalized = normalizeCandidateIndex(candidateIndex(), sha);
  assert.equal(normalized.commit, sha);
  assert.deepEqual(normalized.targets.map((target) => target.target), REQUIRED_TARGETS);

  const wrongCommit = candidateIndex();
  wrongCommit.commit = "c".repeat(40);
  assert.throws(() => normalizeCandidateIndex(wrongCommit, sha), /workflow-tested commit/);

  const incomplete = candidateIndex();
  incomplete.targets.pop();
  assert.throws(() => normalizeCandidateIndex(incomplete, sha));

  const wrongAsset = candidateIndex();
  wrongAsset.targets[0].assets[0].name = "unexpected.zip";
  assert.throws(() => normalizeCandidateIndex(wrongAsset, sha), /asset names are invalid/);

  const toolDrift = candidateIndex();
  toolDrift.targets[0].toolVersions.electron = "44.0.1";
  assert.throws(() => normalizeCandidateIndex(toolDrift, sha), /tool-version set/);
});

test("C10 redaction rejects protected keys, absolute paths, and credential markers", () => {
  assert.doesNotThrow(() => assertRedacted({ evidencePath: "docs/migration/evidence/C9.md", result: "success" }));
  assert.throws(() => assertRedacted({ token: "redacted" }), /forbidden key class/);
  assert.throws(() => assertRedacted({ apiKey: "redacted" }), /forbidden key class/);
  assert.throws(() => assertRedacted({ accessToken: "redacted" }), /forbidden key class/);
  assert.throws(() => assertRedacted({ value: "C:\\Users\\person\\data" }), /absolute Windows path/);
  assert.throws(() => assertRedacted({ value: "/home/person/data" }), /absolute POSIX path/);
  assert.throws(() => assertRedacted({ value: "/srv/private/key.pem" }), /absolute POSIX path/);
  assert.throws(() => assertRedacted({ value: "file:/etc/passwd" }), /absolute POSIX path/);
  assert.throws(() => assertRedacted({ value: "prefix,/srv/private/key.pem" }), /absolute POSIX path/);
  assert.throws(() => assertRedacted({ value: "note:'/var/lib/private'" }), /absolute POSIX path/);
  assert.throws(() => assertRedacted({ value: "Bearer example" }), /credential marker/);
  assert.throws(() => assertRedacted({ value: "https://example.invalid/?access_token=example" }), /credential marker/);
  assert.throws(() => assertRedacted({ value: "sk-proj-1234567890abcdef" }), /credential marker/);
  assert.throws(() => assertRedacted({ value: "AKIA1234567890ABCDEF" }), /credential marker/);
});

test("C10 JSON schema remains strict and release-false-only", () => {
  const schema = JSON.parse(fs.readFileSync(path.join(rootDir, "docs", "migration", "evidence", "C10_EVIDENCE_BUNDLE.v1.schema.json"), "utf8"));
  assert.equal(schema.additionalProperties, false);
  assert.equal(schema.properties.scope.const, "vnext-c10-evidence");
  assert.equal(schema.properties.outcome.const, "ci-verified-not-release");
  assert.deepEqual(
    schema.properties.checkpoints.prefixItems.map((entry) => entry.allOf[1].properties.id.const),
    ["C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9"]
  );
  assert.deepEqual(
    schema.properties.ci.properties.requiredJobs.prefixItems.map((entry) => entry.allOf[1].properties.id.const),
    REQUIRED_JOBS
  );
  assert.deepEqual(
    schema.properties.candidateSet.properties.targets.prefixItems.map((entry) => entry.allOf[1].properties.target.const),
    REQUIRED_TARGETS
  );
  for (const property of Object.values(schema.properties.release.properties)) assert.equal(property.const, false);
});
