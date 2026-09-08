import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

export const DESKTOP_RELEASE_BASE_VERSION = "1.0.0";
export const DESKTOP_CANDIDATE_TARGETS = Object.freeze([
  "windows-x64",
  "macos-x64",
  "macos-arm64",
  "linux-x64"
]);

const CHANNELS = new Set(["alpha", "beta", "rc"]);
const SHA_PATTERN = /^[0-9a-f]{7,40}$/i;

export function resolveCandidateBuild({ channel, runNumber, sha, target }) {
  if (!CHANNELS.has(channel)) throw new Error("Candidate channel must be alpha, beta, or rc.");
  if (!Number.isSafeInteger(runNumber) || runNumber < 0) {
    throw new Error("Candidate run number must be a non-negative safe integer.");
  }
  if (!SHA_PATTERN.test(sha)) throw new Error("Candidate commit must be a 7-40 character hexadecimal SHA.");
  if (!DESKTOP_CANDIDATE_TARGETS.includes(target)) throw new Error("Unknown desktop candidate target.");

  const version = `${DESKTOP_RELEASE_BASE_VERSION}-${channel}.${runNumber}`;
  const commit = sha.toLowerCase();
  const buildId = `${version}-${commit.slice(0, 12)}-${target}`;
  return Object.freeze({ version, buildId, commit, target, channel, runNumber });
}

async function main() {
  const result = resolveCandidateBuild({
    channel: process.env.CPS_CANDIDATE_CHANNEL || "rc",
    runNumber: Number(process.env.CPS_CANDIDATE_RUN_NUMBER || "0"),
    sha: process.env.CPS_CANDIDATE_SHA || "0000000",
    target: process.env.CPS_CANDIDATE_TARGET || "windows-x64"
  });

  const outputPath = process.env.GITHUB_OUTPUT;
  if (outputPath) {
    await fs.appendFile(path.resolve(outputPath), [
      `version=${result.version}`,
      `build_id=${result.buildId}`,
      `commit=${result.commit}`,
      `target=${result.target}`,
      ""
    ].join("\n"), "utf8");
  }
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  await main();
}
