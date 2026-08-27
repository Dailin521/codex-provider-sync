import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";
import { execFile } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";

const execFileAsync = promisify(execFile);
const scriptDirectory = path.dirname(fileURLToPath(import.meta.url));
export const REPOSITORY_ROOT = path.resolve(scriptDirectory, "..");

export const REQUIRED_JOBS = Object.freeze([
  "cross-runtime-fixtures",
  "dependency-audit",
  "desktop-linux-lock",
  "desktop-macos",
  "desktop-test",
  "electron-candidate-set",
  "electron-desktop",
  "electron-release-candidate",
  "root-package-compat",
  "test",
  "web-browser",
  "web-build",
  "workspace-contract"
]);

export const REQUIRED_TARGETS = Object.freeze([
  "linux-x64",
  "macos-arm64",
  "macos-x64",
  "windows-x64"
]);

const EXPECTED_ASSETS = Object.freeze({
  "windows-x64": (version) => [
    `CodexProviderSync-${version}-windows-x64-portable.zip`,
    `CodexProviderSync-${version}-windows-x64-setup.exe`
  ],
  "macos-x64": (version) => [
    `CodexProviderSync-${version}-macos-x64.dmg`,
    `CodexProviderSync-${version}-macos-x64.zip`
  ],
  "macos-arm64": (version) => [
    `CodexProviderSync-${version}-macos-arm64.dmg`,
    `CodexProviderSync-${version}-macos-arm64.zip`
  ],
  "linux-x64": (version) => [
    `CodexProviderSync-${version}-linux-x64.AppImage`,
    `CodexProviderSync-${version}-linux-x64.deb`
  ]
});

const CHECKPOINTS = Object.freeze([
  {
    id: "C0",
    commit: "29c84ec5c3f8e614b050d5645734a4d8df67956d",
    parentCommit: "c7ff85218a07a8e5f14132c582cad1239c52865e",
    evidenceCommit: "29c84ec5c3f8e614b050d5645734a4d8df67956d",
    evidencePath: "docs/migration/evidence/C0_BASELINE_2026-08-25.md"
  },
  {
    id: "C1",
    commit: "f008d0ea277b57fd1d027068bfab9c4f80c5ae3a",
    parentCommit: "29c84ec5c3f8e614b050d5645734a4d8df67956d",
    evidenceCommit: "f008d0ea277b57fd1d027068bfab9c4f80c5ae3a",
    evidencePath: "docs/migration/evidence/C1_PUBLIC_API_ERRORS_2026-08-25.md"
  },
  {
    id: "C2",
    commit: "13163f510a1ac0c245ac992f7a10027f30195300",
    parentCommit: "f008d0ea277b57fd1d027068bfab9c4f80c5ae3a",
    evidenceCommit: "13163f510a1ac0c245ac992f7a10027f30195300",
    evidencePath: "docs/migration/evidence/C2_CLI_JSON_2026-08-25.md"
  },
  {
    id: "C3",
    commit: "166f6ff94aa27c029e546b5e98d145dd4915bee4",
    parentCommit: "13163f510a1ac0c245ac992f7a10027f30195300",
    evidenceCommit: "166f6ff94aa27c029e546b5e98d145dd4915bee4",
    evidencePath: "docs/migration/evidence/C3_PLAN_APPLY_DUAL_LOCK_2026-08-25.md"
  },
  {
    id: "C4",
    commit: "d6b0fef593968d402b290f0dff180c84b0fc9325",
    parentCommit: "166f6ff94aa27c029e546b5e98d145dd4915bee4",
    evidenceCommit: "d6b0fef593968d402b290f0dff180c84b0fc9325",
    evidencePath: "docs/migration/evidence/C4_WORKSPACE_CORE_CLIENT_2026-08-25.md"
  },
  {
    id: "C5",
    commit: "8a53ce8d775996b59fce587ef639c0e00e62fcd1",
    parentCommit: "d6b0fef593968d402b290f0dff180c84b0fc9325",
    evidenceCommit: "8a53ce8d775996b59fce587ef639c0e00e62fcd1",
    evidencePath: "docs/migration/evidence/C5_SHARED_UI_WEB_2026-08-26.md"
  },
  {
    id: "C6",
    commit: "6820858833f4e011ba59f38d0281e1ba4a2fde06",
    parentCommit: "8a53ce8d775996b59fce587ef639c0e00e62fcd1",
    evidenceCommit: "6820858833f4e011ba59f38d0281e1ba4a2fde06",
    evidencePath: "docs/migration/evidence/C6_ELECTRON_READONLY_2026-08-26.md"
  },
  {
    id: "C7",
    commit: "1ec27a5b09b955586b472ef18029258e8ff0532c",
    parentCommit: "6820858833f4e011ba59f38d0281e1ba4a2fde06",
    evidenceCommit: "1ec27a5b09b955586b472ef18029258e8ff0532c",
    evidencePath: "docs/migration/evidence/C7_ELECTRON_SYNC_SWITCH_2026-08-26.md"
  },
  {
    id: "C8",
    commit: "1673147f6d993d3a5923615d41dec2cf9f37c293",
    parentCommit: "1ec27a5b09b955586b472ef18029258e8ff0532c",
    evidenceCommit: "1673147f6d993d3a5923615d41dec2cf9f37c293",
    evidencePath: "docs/migration/evidence/C8_RESTORE_WATCH_DIAGNOSTICS_UPDATE_2026-08-27.md"
  },
  {
    id: "C9",
    commit: "73256f3187dd337bb681a1cc9810edad8f6309bb",
    parentCommit: "1673147f6d993d3a5923615d41dec2cf9f37c293",
    evidenceCommit: "d34654994ad790b09ed4284ce8f5d87aeace8723",
    evidencePath: "docs/migration/evidence/C9_PACKAGING_CI_RELEASE_ENGINEERING_2026-08-27.md"
  }
]);

const SHA_PATTERN = /^[0-9a-f]{40}$/;
const HASH_PATTERN = /^[0-9a-f]{64}$/;
const VERSION_PATTERN = /^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$/;
const CANDIDATE_VERSION_PATTERN = /^1\.0\.0-(?:alpha|beta|rc)\.[0-9]+$/;

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

async function sha256File(filePath) {
  return sha256(await fs.readFile(filePath));
}

function exactSorted(values) {
  return [...values].sort((left, right) => left.localeCompare(right));
}

export function normalizeRequiredJobs(raw) {
  const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
  assert.ok(parsed && typeof parsed === "object" && !Array.isArray(parsed), "Required job results must be an object.");
  const ids = exactSorted(Object.keys(parsed));
  assert.deepEqual(ids, REQUIRED_JOBS, "C10 required-job inventory changed without an evidence-policy update.");
  return ids.map((id) => {
    assert.equal(parsed[id]?.result, "success", `Required CI job did not succeed: ${id}.`);
    return Object.freeze({ id, conclusion: "success" });
  });
}

function sanitizeToolVersions(value) {
  assert.ok(value && typeof value === "object" && !Array.isArray(value), "Candidate tool versions are missing.");
  const result = {};
  for (const key of exactSorted(Object.keys(value))) {
    assert.match(key, /^[A-Za-z][A-Za-z0-9]*$/, "Candidate tool-version key is unsafe.");
    assert.match(value[key], VERSION_PATTERN, `Candidate tool version is invalid: ${key}.`);
    result[key] = value[key];
  }
  assert.ok(Object.keys(result).length > 0, "Candidate tool versions are empty.");
  return result;
}

export function normalizeCandidateIndex(index, evidenceForCommit) {
  assert.equal(index?.schemaVersion, 1);
  assert.equal(index?.scope, "ci-candidate-index");
  assert.equal(index?.releaseAuthorized, false);
  assert.match(index?.version || "", CANDIDATE_VERSION_PATTERN);
  assert.equal(index?.commit, evidenceForCommit, "Candidate set must be built from the workflow-tested commit.");
  assert.match(index?.commit || "", SHA_PATTERN);
  assert.ok(Array.isArray(index?.targets), "Candidate index targets are missing.");
  assert.deepEqual(exactSorted(index.targets.map((record) => record.target)), REQUIRED_TARGETS);

  const lockfiles = new Set();
  const toolVersionSets = new Set();
  const auditPolicies = new Set();
  const targets = index.targets.map((record) => {
    assert.equal(record.version, index.version, `Candidate version mismatch: ${record.target}.`);
    assert.equal(record.commit, index.commit, `Candidate commit mismatch: ${record.target}.`);
    assert.match(record.buildId || "", /^[A-Za-z0-9._-]+$/);
    assert.match(record.lockfileSha256 || "", HASH_PATTERN);
    assert.match(record.manifestSha256 || "", HASH_PATTERN);
    assert.equal(record.fusePolicy, "c9-v1");
    assert.equal(record.artifactAuditPolicy?.schemaVersion, 1);
    assert.match(record.artifactAuditPolicy?.sha256 || "", HASH_PATTERN);
    assert.ok(Array.isArray(record.assets) && record.assets.length === 2, `Candidate assets are incomplete: ${record.target}.`);
    lockfiles.add(record.lockfileSha256);
    const toolVersions = sanitizeToolVersions(record.toolVersions);
    toolVersionSets.add(JSON.stringify(toolVersions));
    auditPolicies.add(JSON.stringify(record.artifactAuditPolicy));
    assert.deepEqual(
      exactSorted(record.assets.map((asset) => asset.name)),
      exactSorted(EXPECTED_ASSETS[record.target](index.version)),
      `Candidate asset names are invalid: ${record.target}.`
    );
    return Object.freeze({
      target: record.target,
      buildId: record.buildId,
      manifestSha256: record.manifestSha256,
      toolVersions,
      fusePolicy: record.fusePolicy,
      artifactAuditPolicy: {
        schemaVersion: 1,
        sha256: record.artifactAuditPolicy.sha256
      },
      assets: record.assets.map((asset) => {
        assert.match(asset.name || "", /^[A-Za-z0-9][A-Za-z0-9._-]+$/);
        assert.ok(Number.isSafeInteger(asset.sizeBytes) && asset.sizeBytes > 0);
        assert.match(asset.sha256 || "", HASH_PATTERN);
        return Object.freeze({ name: asset.name, sizeBytes: asset.sizeBytes, sha256: asset.sha256 });
      }).sort((left, right) => left.name.localeCompare(right.name))
    });
  }).sort((left, right) => left.target.localeCompare(right.target));
  assert.equal(lockfiles.size, 1, "Candidate targets do not share one lockfile.");
  assert.equal(toolVersionSets.size, 1, "Candidate targets do not share one tool-version set.");
  assert.equal(auditPolicies.size, 1, "Candidate targets do not share one artifact-audit policy.");
  return Object.freeze({
    version: index.version,
    commit: index.commit,
    lockfileSha256: [...lockfiles][0],
    targets
  });
}

const FORBIDDEN_KEYS = new Set([
  "auth",
  "authjson",
  "apikey",
  "backuppath",
  "clientsecret",
  "codexhome",
  "credential",
  "credentials",
  "historytitle",
  "log",
  "logs",
  "message",
  "messagebody",
  "password",
  "privatekey",
  "profileid",
  "refreshtoken",
  "rolloutcontent",
  "secret",
  "sessiontoken",
  "sshkey",
  "sqlitehome",
  "sqlitepath",
  "token",
  "xapikey",
  "accesstoken"
]);

export function assertRedacted(value) {
  function visit(current) {
    if (Array.isArray(current)) {
      current.forEach(visit);
      return;
    }
    if (current && typeof current === "object") {
      for (const [key, child] of Object.entries(current)) {
        const canonicalKey = key.toLowerCase().replace(/[^a-z0-9]/g, "");
        assert.equal(FORBIDDEN_KEYS.has(canonicalKey), false, `Evidence contains a forbidden key class: ${key}.`);
        visit(child);
      }
      return;
    }
    if (typeof current !== "string") return;
    assert.doesNotMatch(current, /(?:^|[^A-Za-z])[A-Za-z]:[\\/]/, "Evidence contains an absolute Windows path.");
    assert.doesNotMatch(current, /\\\\[^\\\s]+\\/, "Evidence contains a UNC path.");
    assert.doesNotMatch(current, /(?:^|[^A-Za-z0-9._~\/-])\/(?!\/)/, "Evidence contains an absolute POSIX path.");
    assert.doesNotMatch(
      current,
      /(?:authorization\s*:|bearer\s+|private[-_ ]key|api[-_ ]?key|access[-_ ]?token|refresh[-_ ]?token|client[-_ ]?secret|session[-_ ]?token|sk-(?:proj-)?[A-Za-z0-9_-]{8,}|AKIA[A-Z0-9]{12,})/i,
      "Evidence contains a credential marker."
    );
    assert.doesNotMatch(current, /auth\.json/i, "Evidence names a protected authentication file.");
  }
  visit(value);
}

async function git(repositoryRoot, args, options = {}) {
  const result = await execFileAsync("git", args, {
    cwd: repositoryRoot,
    encoding: options.encoding,
    maxBuffer: 8 * 1024 * 1024
  });
  return result.stdout;
}

async function assertAncestor(repositoryRoot, ancestor, descendant) {
  try {
    await git(repositoryRoot, ["merge-base", "--is-ancestor", ancestor, descendant], { encoding: "utf8" });
  } catch {
    throw new Error(`Checkpoint ${ancestor} is not an ancestor of the tested commit.`);
  }
}

async function assertTestedCheckout(repositoryRoot, evidenceForCommit) {
  const currentHead = String(await git(repositoryRoot, ["rev-parse", "HEAD"], { encoding: "utf8" })).trim().toLowerCase();
  assert.equal(currentHead, evidenceForCommit, "GITHUB_SHA must equal the checked-out commit.");
  try {
    await git(repositoryRoot, ["diff", "--quiet", "--exit-code"]);
    await git(repositoryRoot, ["diff", "--cached", "--quiet", "--exit-code"]);
  } catch {
    throw new Error("C10 evidence requires a clean tracked checkout of the tested commit.");
  }
}

async function collectCheckpoints(repositoryRoot, evidenceForCommit) {
  const records = [];
  for (const descriptor of CHECKPOINTS) {
    const parents = String(await git(repositoryRoot, ["show", "-s", "--format=%P", descriptor.commit], { encoding: "utf8" })).trim().split(/\s+/);
    assert.equal(parents.length, 1, `Checkpoint ${descriptor.id} must be a single-parent commit.`);
    assert.equal(parents[0], descriptor.parentCommit, `Checkpoint ${descriptor.id} parent changed.`);
    if (descriptor.evidenceCommit !== descriptor.commit) {
      const evidenceParents = String(await git(repositoryRoot, ["show", "-s", "--format=%P", descriptor.evidenceCommit], { encoding: "utf8" })).trim().split(/\s+/);
      assert.deepEqual(evidenceParents, [descriptor.commit], `Checkpoint ${descriptor.id} evidence commit must immediately follow its implementation.`);
    }
    await assertAncestor(repositoryRoot, descriptor.evidenceCommit, evidenceForCommit);
    const evidenceBlob = await git(repositoryRoot, ["show", `${descriptor.evidenceCommit}:${descriptor.evidencePath}`]);
    records.push(Object.freeze({
      ...descriptor,
      evidenceSha256: sha256(evidenceBlob),
      status: "candidate-evidence"
    }));
  }
  return records;
}

function requiredEnvironment(environment, key, pattern) {
  const value = environment[key];
  assert.equal(typeof value, "string", `${key} is required.`);
  assert.match(value, pattern, `${key} is invalid.`);
  return value;
}

function pendingItems({ event, ref, sourceVersions }) {
  const pending = [];
  if (!(event === "push" && ref === "refs/heads/main")) {
    pending.push({
      id: "protected-main-merge-and-rerun",
      blocking: true,
      reason: "Pull-request evidence does not prove the protected main merge result.",
      requiredEvidence: "Run the same required CI and C10 bundle on the resulting main commit."
    });
  }
  if (sourceVersions.rootPackage !== "1.0.0" || sourceVersions.desktopPackage !== "1.0.0") {
    pending.push({
      id: "final-source-version",
      blocking: true,
      reason: "The source manifests have not been set to the gated 1.0.0 version.",
      requiredEvidence: "After RC gates pass, set both source manifests to 1.0.0 and rerun every required job."
    });
  }
  pending.push(
    {
      id: "real-wsl-unc-validation",
      blocking: true,
      reason: "The registered local WSL environment was unavailable for the real UNC scenario.",
      requiredEvidence: "Run the Windows WSL boundary case on a healthy real distribution."
    },
    {
      id: "release-authorization",
      blocking: true,
      reason: "No public release action has been authorized.",
      requiredEvidence: "Obtain explicit authorization before any tag, package publication, or hosted release."
    },
    {
      id: "signing-notarization",
      blocking: true,
      reason: "Candidate artifacts are intentionally unsigned and not notarized.",
      requiredEvidence: "Produce and verify signed and notarized release artifacts on the authorized release commit."
    },
    {
      id: "cross-version-update",
      blocking: true,
      reason: "No production update metadata or restart-upgrade path has been published.",
      requiredEvidence: "Verify an authorized cross-version update without an active write or unresolved journal."
    }
  );
  return pending;
}

export async function createEvidenceBundle({
  repositoryRoot = REPOSITORY_ROOT,
  candidateIndexPath,
  environment = process.env,
  now = new Date()
}) {
  const repository = requiredEnvironment(environment, "GITHUB_REPOSITORY", /^Dailin521\/codex-provider-sync$/);
  const evidenceForCommit = requiredEnvironment(environment, "GITHUB_SHA", SHA_PATTERN).toLowerCase();
  const runId = requiredEnvironment(environment, "GITHUB_RUN_ID", /^[0-9]+$/);
  const runAttemptText = requiredEnvironment(environment, "GITHUB_RUN_ATTEMPT", /^[1-9][0-9]*$/);
  const event = requiredEnvironment(environment, "GITHUB_EVENT_NAME", /^(?:pull_request|push)$/);
  const ref = requiredEnvironment(environment, "GITHUB_REF", /^refs\/[A-Za-z0-9._/-]+$/);
  const requiredJobs = normalizeRequiredJobs(requiredEnvironment(environment, "CPS_REQUIRED_JOB_RESULTS_JSON", /^[\s\S]+$/));
  await assertTestedCheckout(repositoryRoot, evidenceForCommit);
  const checkpointRecords = await collectCheckpoints(repositoryRoot, evidenceForCommit);
  const candidateIndex = JSON.parse(await fs.readFile(candidateIndexPath, "utf8"));
  const candidateSet = normalizeCandidateIndex(candidateIndex, evidenceForCommit);
  const rootPackage = JSON.parse(await fs.readFile(path.join(repositoryRoot, "package.json"), "utf8"));
  const desktopPackage = JSON.parse(await fs.readFile(path.join(repositoryRoot, "apps", "desktop", "package.json"), "utf8"));
  assert.match(rootPackage.version || "", VERSION_PATTERN);
  assert.match(desktopPackage.version || "", VERSION_PATTERN);
  const sourceVersions = Object.freeze({ rootPackage: rootPackage.version, desktopPackage: desktopPackage.version });
  const workflowBlob = await git(repositoryRoot, ["show", `${evidenceForCommit}:.github/workflows/ci.yml`]);
  const createdAt = now.toISOString();
  assert.equal(Number.isNaN(Date.parse(createdAt)), false, "C10 creation time is invalid.");

  const bundle = {
    schemaVersion: 1,
    scope: "vnext-c10-evidence",
    outcome: "ci-verified-not-release",
    evidenceForCommit,
    createdAt,
    repository,
    workflow: {
      path: ".github/workflows/ci.yml",
      workflowSha256: sha256(workflowBlob),
      runId,
      runAttempt: Number(runAttemptText),
      event,
      ref,
      testedCommit: evidenceForCommit
    },
    sourceVersions,
    checkpoints: checkpointRecords,
    ci: {
      policy: "all-applicable-jobs-must-succeed",
      requiredJobs
    },
    candidateSet: {
      artifactName: "electron-release-candidate-set",
      indexSha256: await sha256File(candidateIndexPath),
      ...candidateSet
    },
    assertions: {
      checkpointChainLinear: true,
      allEvidenceFilesHashed: true,
      workflowHeadMatchesEvidenceCommit: true,
      allRequiredJobsSucceeded: true,
      candidateSetComplete: true,
      candidateCommitMatchesEvidenceCommit: true,
      candidateReleaseUnauthorized: true,
      redactionScanPassed: true
    },
    pending: pendingItems({ event, ref, sourceVersions }),
    release: {
      releaseAuthorized: false,
      tagCreated: false,
      npmPublished: false,
      githubReleaseCreated: false,
      signed: false,
      notarized: false,
      updateMetadataPublished: false,
      crossVersionUpgradeVerified: false
    },
    redaction: {
      policyVersion: "c10-v1",
      secretScan: "passed",
      forbiddenKeyClasses: ["authentication", "credentials", "message-content", "storage-paths"],
      forbiddenValueClasses: ["absolute-paths", "authentication-files", "credential-markers", "raw-logs"]
    }
  };
  assertRedacted(bundle);
  return bundle;
}

export async function writeEvidenceBundle({ bundle, outputRoot }) {
  try {
    await fs.lstat(outputRoot);
    throw new Error("C10 evidence output already exists.");
  } catch (error) {
    if (!error || typeof error !== "object" || error.code !== "ENOENT") throw error;
  }
  await fs.mkdir(outputRoot, { recursive: true });
  const bundlePath = path.join(outputRoot, "evidence-bundle.v1.json");
  const body = `${JSON.stringify(bundle, null, 2)}\n`;
  await fs.writeFile(bundlePath, body, { encoding: "utf8", flag: "wx" });
  const checksum = sha256(Buffer.from(body, "utf8"));
  await fs.writeFile(path.join(outputRoot, "SHA256SUMS.txt"), `${checksum}  evidence-bundle.v1.json\n`, {
    encoding: "utf8",
    flag: "wx"
  });
  return Object.freeze({ bundlePath, checksum });
}

async function main() {
  const candidateIndexPath = path.resolve(
    process.env.CPS_CANDIDATE_INDEX || path.join(REPOSITORY_ROOT, "artifacts", "c9-index", "candidate-index.v1.json")
  );
  const outputRoot = path.resolve(
    process.env.CPS_C10_OUTPUT_ROOT || path.join(REPOSITORY_ROOT, "artifacts", "c10")
  );
  const bundle = await createEvidenceBundle({ candidateIndexPath });
  const result = await writeEvidenceBundle({ bundle, outputRoot });
  process.stdout.write(`C10 evidence bundle written: ${result.checksum}\n`);
}

if (process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url) {
  await main();
}
