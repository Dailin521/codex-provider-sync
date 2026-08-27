import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { RELEASE_REPOSITORY_ROOT, RELEASE_TARGETS, sha256File } from "./release-audit.mjs";

const downloadRoot = path.join(RELEASE_REPOSITORY_ROOT, "artifacts", "c9-download");
const indexRoot = path.join(RELEASE_REPOSITORY_ROOT, "artifacts", "c9-index");

async function findNamed(root, name) {
  const found = [];
  async function visit(directory) {
    for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
      const absolute = path.join(directory, entry.name);
      if (entry.isSymbolicLink()) throw new Error("Candidate download contains a symbolic link.");
      if (entry.isDirectory()) await visit(absolute);
      else if (entry.isFile() && entry.name === name) found.push(absolute);
    }
  }
  await visit(root);
  return found.sort((left, right) => left.localeCompare(right));
}

async function relativeFiles(root) {
  const found = [];
  async function visit(directory) {
    for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
      const absolute = path.join(directory, entry.name);
      if (entry.isSymbolicLink()) throw new Error("Candidate artifact contains a symbolic link.");
      if (entry.isDirectory()) await visit(absolute);
      else if (entry.isFile()) found.push(path.relative(root, absolute).replaceAll("\\", "/"));
      else throw new Error("Candidate artifact contains an unsupported entry.");
    }
  }
  await visit(root);
  return found.sort((left, right) => left.localeCompare(right));
}

function safeRelative(root, value) {
  const normalized = value.replaceAll("\\", "/");
  if (!normalized || normalized.startsWith("/") || normalized.split("/").includes("..")) {
    throw new Error("Candidate checksum contains an unsafe path.");
  }
  const absolute = path.resolve(root, ...normalized.split("/"));
  const relative = path.relative(root, absolute);
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error("Candidate checksum escapes its artifact root.");
  }
  return absolute;
}

const manifestFiles = await findNamed(downloadRoot, "release-manifest.v1.json");
assert.equal(manifestFiles.length, 4, "Exactly four candidate manifests are required.");
const records = [];
for (const manifestFile of manifestFiles) {
  const metadataRoot = path.dirname(manifestFile);
  const candidateRoot = path.dirname(metadataRoot);
  const checksumsFile = path.join(metadataRoot, "SHA256SUMS.txt");
  const manifest = JSON.parse(await fs.readFile(manifestFile, "utf8"));
  assert.equal(manifest.schemaVersion, 1);
  assert.equal(manifest.scope, "ci-candidate");
  assert.equal(manifest.releaseAuthorized, false);
  assert.equal(manifest.signingStatus, "unsigned-candidate");
  assert.equal(manifest.notarizationStatus, "not-authorized");
  assert.ok(RELEASE_TARGETS[manifest.target], "Candidate manifest target is unknown.");
  assert.match(manifest.lockfileSha256, /^[0-9a-f]{64}$/);
  assert.equal(manifest.audit?.fusePolicy, "c9-v1");
  assert.equal(manifest.audit?.artifactAuditPolicy?.schemaVersion, 1);
  assert.match(manifest.audit?.artifactAuditPolicy?.sha256, /^[0-9a-f]{64}$/);
  assert.ok(manifest.audit?.asarIntegrityEntryCount > 0);
  assert.equal(
    manifest.audit?.asarRuntimeIntegrity,
    manifest.platform === "linux" ? "unsupported-platform" : "verified"
  );
  assert.equal(manifest.containerVerification?.containerCount, 2);
  assert.equal(manifest.containerVerification?.fixtureStatus, true);
  assert.equal(manifest.containerVerification?.syncRestore, true);
  assert.equal(manifest.containerVerification?.nativeDriver, true);
  assert.equal(manifest.containerVerification?.gracefulExit, true);
  assert.deepEqual(
    manifest.assets.map((asset) => asset.name).sort(),
    RELEASE_TARGETS[manifest.target].assets(manifest.version).sort(),
    "Candidate asset names do not match the frozen release target."
  );

  const containerReport = JSON.parse(await fs.readFile(path.join(metadataRoot, "container-verification.v1.json"), "utf8"));
  assert.equal(containerReport.schemaVersion, 1);
  assert.equal(containerReport.scope, "final-release-containers");
  assert.equal(containerReport.target, manifest.target);
  assert.equal(containerReport.version, manifest.version);
  assert.equal(containerReport.buildId, manifest.buildId);
  assert.equal(containerReport.commit, manifest.commit);
  assert.deepEqual(
    containerReport.containers.map((container) => container.assetName).sort(),
    RELEASE_TARGETS[manifest.target].assets(manifest.version).sort(),
    "Container verification does not cover every candidate asset."
  );

  const checksumLines = (await fs.readFile(checksumsFile, "utf8")).trim().split(/\r?\n/);
  const expectedChecksummedPaths = [
    ...manifest.assets.map((asset) => `assets/${asset.name}`),
    ...manifest.metadata.map((entry) => entry.name),
    "metadata/release-manifest.v1.json"
  ].sort((left, right) => left.localeCompare(right));
  const checksumPaths = [];
  for (const line of checksumLines) {
    const match = line.match(/^([0-9a-f]{64})  ([A-Za-z0-9._/-]+)$/);
    assert.ok(match, "Candidate checksum line is malformed.");
    const checkedPath = safeRelative(candidateRoot, match[2]);
    const stat = await fs.stat(checkedPath);
    assert.equal(stat.isFile(), true, `Checksum target is not a regular file: ${match[2]}.`);
    assert.equal(await sha256File(checkedPath), match[1], `Checksum mismatch for ${match[2]}.`);
    checksumPaths.push(match[2]);
  }
  checksumPaths.sort((left, right) => left.localeCompare(right));
  assert.deepEqual(checksumPaths, expectedChecksummedPaths, "Candidate checksums must cover the exact manifest closure.");
  assert.equal(new Set(checksumPaths).size, checksumPaths.length, "Candidate checksums contain duplicate paths.");
  assert.deepEqual(
    await relativeFiles(candidateRoot),
    [...expectedChecksummedPaths, "metadata/SHA256SUMS.txt"].sort((left, right) => left.localeCompare(right)),
    "Candidate artifact contains an unmanifested file."
  );
  for (const record of [...manifest.assets.map((asset) => ({ ...asset, name: `assets/${asset.name}` })), ...manifest.metadata]) {
    const recordPath = safeRelative(candidateRoot, record.name);
    const stat = await fs.stat(recordPath);
    assert.equal(stat.size, record.sizeBytes, `Manifest size mismatch for ${record.name}.`);
    assert.equal(await sha256File(recordPath), record.sha256, `Manifest hash mismatch for ${record.name}.`);
  }
  assert.equal(
    manifest.containerVerification.reportSha256,
    manifest.metadata.find((entry) => entry.name === "metadata/container-verification.v1.json")?.sha256,
    "Container verification report hash is not bound into the manifest."
  );
  records.push(Object.freeze({
    target: manifest.target,
    version: manifest.version,
    commit: manifest.commit,
    buildId: manifest.buildId,
    lockfileSha256: manifest.lockfileSha256,
    toolVersions: manifest.toolVersions,
    fusePolicy: manifest.audit.fusePolicy,
    artifactAuditPolicy: manifest.audit.artifactAuditPolicy,
    manifestSha256: await sha256File(manifestFile),
    assets: manifest.assets.map(({ name, sizeBytes, sha256 }) => ({ name, sizeBytes, sha256 }))
  }));
}

records.sort((left, right) => left.target.localeCompare(right.target));
assert.deepEqual(records.map((record) => record.target), Object.keys(RELEASE_TARGETS).sort());
assert.equal(new Set(records.map((record) => record.version)).size, 1, "Candidate versions do not match.");
assert.equal(new Set(records.map((record) => record.commit)).size, 1, "Candidate commits do not match.");
assert.equal(new Set(records.map((record) => record.lockfileSha256)).size, 1, "Candidate lockfiles do not match.");
assert.equal(
  new Set(records.map((record) => JSON.stringify(record.toolVersions))).size,
  1,
  "Candidate tool versions do not match."
);
assert.equal(new Set(records.map((record) => record.fusePolicy)).size, 1, "Candidate fuse policies do not match.");
assert.equal(
  new Set(records.map((record) => JSON.stringify(record.artifactAuditPolicy))).size,
  1,
  "Candidate artifact audit policies do not match."
);

try {
  await fs.lstat(indexRoot);
  throw new Error("Candidate index output already exists.");
} catch (error) {
  if (!error || typeof error !== "object" || error.code !== "ENOENT") throw error;
}
await fs.mkdir(indexRoot, { recursive: true });
const index = {
  schemaVersion: 1,
  scope: "ci-candidate-index",
  releaseAuthorized: false,
  version: records[0].version,
  commit: records[0].commit,
  targets: records
};
await fs.writeFile(
  path.join(indexRoot, "candidate-index.v1.json"),
  `${JSON.stringify(index, null, 2)}\n`,
  { encoding: "utf8", flag: "wx" }
);
process.stdout.write(`Candidate set verified: ${records[0].version} ${records[0].commit}\n`);
