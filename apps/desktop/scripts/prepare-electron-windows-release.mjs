import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const VERSION_PATTERN = /^1\.0\.0-rc\.(?:0|[1-9]\d*)$/;
const SHA_PATTERN = /^[0-9a-f]{40}$/i;

/**
 * Parses the immutable inputs for the Windows-only Electron release-preparation
 * workflow. This deliberately accepts only an existing, version-matched tag:
 * the workflow must never mint a tag as a side effect of building a candidate.
 */
export function prepareElectronWindowsRelease({ releaseRef, version, expectedSha, channel = "rc" }) {
  if (!(channel === "rc" && VERSION_PATTERN.test(version || ""))
    && !(channel === "stable-manual" && version === "1.0.0")) {
    throw new Error("Release version must match the explicit RC or Windows stable-manual channel.");
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

async function main() {
  const result = prepareElectronWindowsRelease({
    releaseRef: process.env.CPS_RELEASE_REF,
    version: process.env.CPS_RELEASE_VERSION,
    channel: process.env.CPS_RELEASE_CHANNEL,
    expectedSha: process.env.CPS_EXPECTED_SHA
  });
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
