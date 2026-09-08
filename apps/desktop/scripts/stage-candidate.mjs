import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import {
  auditPackagedApp,
  ARTIFACT_AUDIT_POLICY_PATH,
  createCycloneDx,
  createRuntimeProjection,
  RELEASE_REPOSITORY_ROOT,
  RELEASE_TARGETS,
  sha256File
} from "./release-audit.mjs";

const VERSION_PATTERN = /^1\.0\.0-(?:alpha|beta|rc)\.\d+$/;
const BUILD_ID_PATTERN = /^[A-Za-z0-9._-]{1,128}$/;
const COMMIT_PATTERN = /^[0-9a-f]{40}$/i;
const outputRoot = path.join(RELEASE_REPOSITORY_ROOT, "dist-desktop");
const artifactRoot = path.join(RELEASE_REPOSITORY_ROOT, "artifacts", "c9");

function sorted(value) {
  if (Array.isArray(value)) return value.map(sorted);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sorted(value[key])]));
}

async function writeJson(filePath, value) {
  await fs.writeFile(filePath, `${JSON.stringify(sorted(value), null, 2)}\n`, { encoding: "utf8", flag: "wx" });
}

async function assertNewDirectory(directory) {
  try {
    await fs.lstat(directory);
    throw new Error(`Candidate output already exists: ${path.basename(directory)}`);
  } catch (error) {
    if (!error || typeof error !== "object" || error.code !== "ENOENT") throw error;
  }
  await fs.mkdir(directory, { recursive: true });
}

async function copyAsset(source, destination) {
  const stat = await fs.lstat(source);
  assert.equal(stat.isFile(), true, `Candidate asset is not a regular file: ${path.basename(source)}`);
  assert.equal(stat.isSymbolicLink(), false, `Candidate asset is a symbolic link: ${path.basename(source)}`);
  await fs.copyFile(source, destination, fs.constants.COPYFILE_EXCL);
  const copied = await fs.stat(destination);
  return Object.freeze({
    name: path.basename(destination),
    sizeBytes: copied.size,
    sha256: await sha256File(destination)
  });
}

const target = process.env.CPS_CANDIDATE_TARGET;
const version = process.env.CPS_DESKTOP_VERSION;
const buildId = process.env.CPS_DESKTOP_BUILD_ID;
const commit = process.env.CPS_CANDIDATE_SHA?.toLowerCase();
const descriptor = RELEASE_TARGETS[target];
if (!descriptor) throw new Error("CPS_CANDIDATE_TARGET is invalid.");
if (!VERSION_PATTERN.test(version || "")) throw new Error("CPS_DESKTOP_VERSION is invalid.");
if (!BUILD_ID_PATTERN.test(buildId || "")) throw new Error("CPS_DESKTOP_BUILD_ID is invalid.");
if (!COMMIT_PATTERN.test(commit || "")) throw new Error("CPS_CANDIDATE_SHA must be a full commit SHA.");
if (process.platform !== descriptor.platform || process.arch !== descriptor.arch) {
  throw new Error(`Candidate ${target} must be staged on native ${descriptor.platform}/${descriptor.arch}.`);
}

const candidateRoot = path.join(artifactRoot, target);
const assetsRoot = path.join(candidateRoot, "assets");
const metadataRoot = path.join(candidateRoot, "metadata");
const audit = await auditPackagedApp({ outputRoot, target, version, buildId });
await assertNewDirectory(candidateRoot);
await fs.mkdir(assetsRoot);
await fs.mkdir(metadataRoot);

const assetRecords = [];
for (const assetName of descriptor.assets(version)) {
  assetRecords.push(await copyAsset(path.join(outputRoot, assetName), path.join(assetsRoot, assetName)));
}
assetRecords.sort((left, right) => left.name.localeCompare(right.name));

const timestamp = new Date().toISOString();
const auditPath = path.join(metadataRoot, "audit-report.v1.json");
const sbomPath = path.join(metadataRoot, "sbom.cyclonedx.json");
const stagingPath = path.join(metadataRoot, "candidate-staging.v1.json");
await writeJson(auditPath, audit);
const runtimeProjection = await createRuntimeProjection(path.join(RELEASE_REPOSITORY_ROOT, "package-lock.json"));
await writeJson(sbomPath, createCycloneDx({ audit, timestamp, runtimeProjection }));

const staging = {
  schemaVersion: 1,
  scope: "ci-candidate-staging",
  releaseAuthorized: false,
  signingStatus: "unsigned-candidate",
  notarizationStatus: "not-authorized",
  target,
  platform: descriptor.platform,
  arch: descriptor.arch,
  version,
  buildId,
  commit,
  createdAt: timestamp,
  lockfileSha256: await sha256File(path.join(RELEASE_REPOSITORY_ROOT, "package-lock.json")),
  toolVersions: {
    electron: "44.0.0",
    electronBuilder: "26.15.7",
    electronAsar: "4.3.0",
    electronFuses: "2.1.3",
    betterSqlite3: "13.0.3",
    plist: "5.0.0",
    resedit: "3.1.0"
  },
  assets: assetRecords,
  audit: {
    asarSha256: audit.asar.sha256,
    asarHeaderSha256: audit.asar.headerSha256,
    asarEntryCount: audit.asar.entryCount,
    asarIntegrityEntryCount: audit.asar.integrityEntryCount,
    asarRuntimeIntegrity: audit.asar.runtimeIntegrity,
    nativeDriver: audit.nativeDriver.driver,
    nativeBindingSha256: audit.nativeDriver.bindingSha256,
    fusePolicy: "c9-v1",
    artifactAuditPolicy: {
      schemaVersion: 1,
      sha256: await sha256File(ARTIFACT_AUDIT_POLICY_PATH)
    }
  }
};
await writeJson(stagingPath, staging);

process.stdout.write(`Release candidate assets staged for container verification: ${target} ${version}\n`);
