import fs from "node:fs/promises";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { pathToFileURL } from "node:url";

const PATCH_VERSION = "1\\.0\\.(?:0|[1-9]\\d*)";
const RC_VERSION_PATTERN = new RegExp(`^${PATCH_VERSION}-rc\\.(?:0|[1-9]\\d*)$`);
const STABLE_VERSION_PATTERN = new RegExp(`^${PATCH_VERSION}$`);
const SHA_PATTERN = /^[0-9a-f]{40}$/i;

/**
 * Parses the immutable inputs for the Windows-only Electron release-preparation
 * workflow. This deliberately accepts only an existing, version-matched tag:
 * the workflow must never mint a tag as a side effect of building a candidate.
 */
export function prepareElectronWindowsRelease({ releaseRef, version, expectedSha, channel = "rc" }) {
  if (!(channel === "rc" && RC_VERSION_PATTERN.test(version || ""))
    && !(["stable-manual", "stable-updater"].includes(channel) && STABLE_VERSION_PATTERN.test(version || ""))) {
    throw new Error("Release version must match the explicit RC or Windows stable release channel.");
  }
  if (!SHA_PATTERN.test(expectedSha || "")) {
    throw new Error("Expected commit must be a full 40-character hexadecimal SHA.");
  }
  const expectedRef = `refs/tags/v${version}`;
  if (releaseRef !== expectedRef) {
    throw new Error(`Release ref must be the existing version tag ${expectedRef}.`);
  }

  const commit = expectedSha.toLowerCase();
  return Object.freeze({
    releaseRef,
    releaseTag: `v${version}`,
    version,
    channel,
    commit,
    target: "windows-x64",
    buildId: `${version}-${commit.slice(0, 12)}-windows-x64`
  });
}

export function verifyTaggedSourceVersions(input, readAtRef = (ref, file) =>
  execFileSync("git", ["show", `${ref}:${file}`], { encoding: "utf8" })) {
  const release = prepareElectronWindowsRelease(input);
  const baseVersion = release.version.replace(/-rc\.\d+$/, "");
  for (const file of ["package.json", "apps/desktop/package.json"]) {
    // The workflow has just verified tag -> commit. Read that immutable object,
    // not dispatch HEAD or a tag that could move between the two reads.
    const manifest = JSON.parse(readAtRef(release.commit, file));
    if (manifest.version !== baseVersion) {
      throw new Error(`Tagged ${file} must match the requested release base version ${baseVersion}.`);
    }
  }
  return release;
}

async function main() {
  const result = prepareElectronWindowsRelease({
    releaseRef: process.env.CPS_RELEASE_REF,
    version: process.env.CPS_RELEASE_VERSION,
    channel: process.env.CPS_RELEASE_CHANNEL,
    expectedSha: process.env.CPS_EXPECTED_SHA
  });
  if (process.argv.includes("--verify-source-manifests")) {
    verifyTaggedSourceVersions({
      releaseRef: result.releaseRef, version: result.version,
      expectedSha: result.commit, channel: result.channel
    });
  }
  if (process.env.GITHUB_OUTPUT) {
    await fs.appendFile(path.resolve(process.env.GITHUB_OUTPUT), [
      `release_ref=${result.releaseRef}`,
      `release_tag=${result.releaseTag}`,
      `version=${result.version}`,
      `channel=${result.channel}`,
      `commit=${result.commit}`,
      `target=${result.target}`,
      `build_id=${result.buildId}`,
      ""
    ].join("\n"), "utf8");
  }
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  await main();
}
