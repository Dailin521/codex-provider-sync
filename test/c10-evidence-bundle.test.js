import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import {
  assertEventBaseContained,
  assertEvidenceSchema,
  assertRedacted,
  normalizeCandidateIndex,
  normalizeFormalReleaseEvidence,
  normalizeRequiredJobs,
  REQUIRED_JOBS,
  REQUIRED_TARGETS
} from "../scripts/write-c10-evidence-bundle.mjs";

const execFileAsync = promisify(execFile);

const testDirectory = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(testDirectory, "..");
const sha = "a".repeat(40);
const hash = "b".repeat(64);
const formalReleaseManifest = JSON.parse(fs.readFileSync(
  path.join(rootDir, "test-support", "formal-release-assets.v1.json"),
  "utf8"
));
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

function formalReleaseEvidence() {
  const executable = formalReleaseManifest.archiveEntries.find(
    (entry) => entry.name === "CodexProviderSync.Automation.exe"
  );
  return {
    schemaVersion: 1,
    scope: "historical-formal-release-backup-evidence",
    containsRealUserData: false,
    syntheticOnly: true,
    generatedAt: "2026-08-28T00:00:00.000Z",
    workflow: {
      repository: formalReleaseManifest.repository,
      runId: "42",
      runAttempt: 3,
      testedCommit: sha
    },
    release: {
      repository: formalReleaseManifest.repository,
      releaseId: formalReleaseManifest.release.id,
      tag: formalReleaseManifest.release.tag,
      tagObjectSha: formalReleaseManifest.release.tagObjectSha,
      commit: formalReleaseManifest.release.commit,
      publishedAt: formalReleaseManifest.release.publishedAt,
      tagSigned: formalReleaseManifest.release.tagSigned
    },
    asset: {
      id: formalReleaseManifest.assets.automationZip.id,
      name: formalReleaseManifest.assets.automationZip.name,
      size: formalReleaseManifest.assets.automationZip.size,
      sha256: formalReleaseManifest.assets.automationZip.sha256,
      checksumAssetSha256: formalReleaseManifest.assets.automationChecksum.sha256,
      releaseChecksumsSha256: formalReleaseManifest.assets.releaseChecksums.sha256
    },
    binary: {
      name: executable.name,
      size: executable.size,
      sha256: executable.sha256,
      authenticodeStatus: "NotSigned"
    },
    backup: {
      metadataVersion: 2,
      metadataSha256: hash,
      producedTreeSha256: hash
    },
    verification: {
      releaseApiPinned: true,
      releaseChecksumPinned: true,
      isolatedEnvironment: true,
      authCanaryExcluded: true,
      currentNodeRestoreVerified: true,
      pendingRecoveryCount: 0
    },
    limitation: "The formal v0.4.1 Automation binary is unsigned; synthetic fixture only."
  };
}

async function git(repository, ...args) {
  const result = await execFileAsync("git", args, { cwd: repository, encoding: "utf8" });
  return String(result.stdout).trim().toLowerCase();
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

test("C10 formal Release evidence binds the hosted binary and current workflow", () => {
  const normalized = normalizeFormalReleaseEvidence(formalReleaseEvidence(), {
    manifest: formalReleaseManifest,
    evidenceForCommit: sha,
    repository: formalReleaseManifest.repository,
    runId: "42",
    runAttempt: 3
  });
  assert.equal(normalized.release.tag, "v0.4.1");
  assert.equal(normalized.asset.sha256, formalReleaseManifest.assets.automationZip.sha256);
  assert.equal(normalized.currentNodeRestoreVerified, true);

  const wrongRun = formalReleaseEvidence();
  wrongRun.workflow.runId = "41";
  assert.throws(() => normalizeFormalReleaseEvidence(wrongRun, {
    manifest: formalReleaseManifest,
    evidenceForCommit: sha,
    repository: formalReleaseManifest.repository,
    runId: "42",
    runAttempt: 3
  }));

  const changedAsset = formalReleaseEvidence();
  changedAsset.asset.sha256 = hash;
  assert.throws(() => normalizeFormalReleaseEvidence(changedAsset, {
    manifest: formalReleaseManifest,
    evidenceForCommit: sha,
    repository: formalReleaseManifest.repository,
    runId: "42",
    runAttempt: 3
  }));

  for (const mutate of [
    (evidence) => { evidence.workflow.testedCommit = "c".repeat(40); },
    (evidence) => { evidence.binary.sha256 = hash; },
    (evidence) => { evidence.backup.metadataVersion = 1; },
    (evidence) => { evidence.verification.currentNodeRestoreVerified = false; },
    (evidence) => { evidence.verification.pendingRecoveryCount = 1; }
  ]) {
    const changed = formalReleaseEvidence();
    mutate(changed);
    assert.throws(() => normalizeFormalReleaseEvidence(changed, {
      manifest: formalReleaseManifest,
      evidenceForCommit: sha,
      repository: formalReleaseManifest.repository,
      runId: "42",
      runAttempt: 3
    }));
  }
});

test("C10 redaction rejects protected keys, absolute paths, and credential markers", () => {
  assert.doesNotThrow(() => assertRedacted({ evidencePath: "docs/migration/evidence/C9.md", result: "success" }));
  assert.throws(() => assertRedacted({ token: "redacted" }), /forbidden key class/);
  assert.throws(() => assertRedacted({ apiKey: "redacted" }), /forbidden key class/);
  assert.throws(() => assertRedacted({ accessToken: "redacted" }), /forbidden key class/);
  assert.throws(() => assertRedacted({ value: "C:\\Users\\person\\data" }), /absolute Windows path/);
  assert.throws(() => assertRedacted({ value: "//server/share/private" }), /absolute network path/);
  assert.throws(() => assertRedacted({ value: "prefix //server/share/private" }), /absolute network path/);
  assert.doesNotThrow(() => assertRedacted({ value: "https://example.invalid/path" }));
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
  assert.equal(
    schema.properties.historicalFormalRelease.properties.artifactName.const,
    "historical-formal-release-backup-evidence"
  );
  assert.deepEqual(
    Object.fromEntries(["tagObjectSha", "commit", "publishedAt"].map((name) => [
      name,
      schema.properties.historicalFormalRelease.properties.release.properties[name].const
    ])),
    {
      tagObjectSha: formalReleaseManifest.release.tagObjectSha,
      commit: formalReleaseManifest.release.commit,
      publishedAt: formalReleaseManifest.release.publishedAt
    }
  );
  assert.equal(
    schema.properties.assertions.properties.historicalFormalReleaseBackupVerified.const,
    true
  );
  for (const property of Object.values(schema.properties.release.properties)) assert.equal(property.const, false);
});

test("C10 JSON schema compiles in strict mode and rejects an incomplete bundle", {
  skip: Number(process.versions.node.split(".")[0]) < 24
}, async () => {
  await assert.rejects(
    assertEvidenceSchema({}, rootDir),
    /does not match its JSON Schema/
  );
});

test("C10 event-base evidence follows the actual Git graph for push and pull-request commits", async () => {
  const repository = await fsPromises.mkdtemp(path.join(os.tmpdir(), "c10-git-graph-"));
  try {
    await git(repository, "init");
    await git(repository, "config", "user.name", "C10 Test");
    await git(repository, "config", "user.email", "c10@example.invalid");
    await fsPromises.writeFile(path.join(repository, "evidence.txt"), "base\n", "utf8");
    await git(repository, "add", "evidence.txt");
    await git(repository, "commit", "-m", "base");
    const baseBranch = await git(repository, "branch", "--show-current");
    const eventBaseCommit = await git(repository, "rev-parse", "HEAD");

    await git(repository, "switch", "-c", "source");
    await fsPromises.appendFile(path.join(repository, "evidence.txt"), "source\n", "utf8");
    await git(repository, "commit", "-am", "source");
    const sourceHeadCommit = await git(repository, "rev-parse", "HEAD");

    await assertEventBaseContained(repository, {
      event: "push",
      evidenceForCommit: sourceHeadCommit,
      sourceHeadCommit,
      eventBaseCommit
    });

    await git(repository, "switch", baseBranch);
    await git(repository, "merge", "--no-ff", "source", "-m", "merge source");
    const testedMergeCommit = await git(repository, "rev-parse", "HEAD");
    await assertEventBaseContained(repository, {
      event: "pull_request",
      evidenceForCommit: testedMergeCommit,
      sourceHeadCommit,
      eventBaseCommit
    });
    await assert.rejects(
      assertEventBaseContained(repository, {
        event: "push",
        evidenceForCommit: testedMergeCommit,
        sourceHeadCommit,
        eventBaseCommit
      }),
      /source head to the tested commit/
    );

    await git(repository, "switch", "--orphan", "unrelated");
    await fsPromises.writeFile(path.join(repository, "unrelated.txt"), "unrelated\n", "utf8");
    await git(repository, "add", "-A");
    await git(repository, "commit", "-m", "unrelated root");
    const unrelatedHead = await git(repository, "rev-parse", "HEAD");
    await assert.rejects(
      assertEventBaseContained(repository, {
        event: "push",
        evidenceForCommit: unrelatedHead,
        sourceHeadCommit: unrelatedHead,
        eventBaseCommit
      }),
      /does not contain the workflow event base commit/
    );
  } finally {
    await fsPromises.rm(repository, { recursive: true, force: true });
  }
});
