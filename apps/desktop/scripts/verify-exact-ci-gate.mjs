import assert from "node:assert/strict";
import path from "node:path";
import { pathToFileURL } from "node:url";

const REPOSITORY_PATTERN = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/;
const SHA_PATTERN = /^[0-9a-f]{40}$/i;

function normalizeRun(value) {
  if (!value || typeof value !== "object") return null;
  const run = /** @type {{ id?: unknown, head_sha?: unknown, head_branch?: unknown, event?: unknown, conclusion?: unknown, path?: unknown }} */ (value);
  if (!Number.isSafeInteger(run.id) || run.id <= 0) return null;
  if (typeof run.head_sha !== "string" || typeof run.head_branch !== "string" || typeof run.event !== "string") return null;
  return run;
}

/** Only the latest main push run may authorize release; an older green run cannot mask a rerun. */
export function selectExactSuccessfulCiRuns(payload, expectedSha) {
  assert.match(expectedSha, SHA_PATTERN, "Expected commit must be a full SHA.");
  const runs = payload && typeof payload === "object" && Array.isArray(payload.workflow_runs)
    ? payload.workflow_runs
    : [];
  const matching = runs
    .map(normalizeRun)
    .filter((run) => run
      && run.head_sha.toLowerCase() === expectedSha.toLowerCase()
      && run.head_branch === "main"
      && run.event === "push"
      && run.path === ".github/workflows/ci.yml")
    .sort((left, right) => right.id - left.id);
  return matching[0]?.conclusion === "success" ? [matching[0]] : [];
}

/** A successful workflow is insufficient: the required ci-gate job itself must be green. */
export function hasSuccessfulCiGateJob(payload) {
  const jobs = payload && typeof payload === "object" && Array.isArray(payload.jobs) ? payload.jobs : [];
  return jobs.some((job) => job
    && typeof job === "object"
    && job.name === "ci-gate"
    && job.conclusion === "success");
}

async function requestJson(fetchImpl, url, token) {
  const response = await fetchImpl(url, {
    headers: {
      accept: "application/vnd.github+json",
      authorization: `Bearer ${token}`,
      "x-github-api-version": "2022-11-28"
    }
  });
  if (!response.ok) throw new Error(`GitHub Actions verification failed with HTTP ${response.status}.`);
  return response.json();
}

export async function verifyExactMainCiGate({ repository, expectedSha, token, fetchImpl = fetch }) {
  if (!REPOSITORY_PATTERN.test(repository || "")) throw new Error("GitHub repository is invalid.");
  if (!SHA_PATTERN.test(expectedSha || "")) throw new Error("Expected commit must be a full SHA.");
  if (typeof token !== "string" || token.length === 0) throw new Error("GitHub Actions read token is required.");

  const root = `https://api.github.com/repos/${repository}/actions`;
  const runs = await requestJson(
    fetchImpl,
    `${root}/workflows/ci.yml/runs?event=push&head_sha=${expectedSha}&per_page=100`,
    token
  );
  for (const run of selectExactSuccessfulCiRuns(runs, expectedSha)) {
    const jobs = await requestJson(fetchImpl, `${root}/runs/${run.id}/jobs?filter=latest&per_page=100`, token);
    if (hasSuccessfulCiGateJob(jobs)) return Object.freeze({ runId: run.id, commit: expectedSha.toLowerCase() });
  }
  throw new Error("No successful main push ci-gate exists for the exact expected commit.");
}

/** Refuse to overwrite a prior draft or published release with the same immutable tag. */
export async function assertNoExistingRelease({ repository, releaseTag, token, fetchImpl = fetch }) {
  if (!REPOSITORY_PATTERN.test(repository || "")) throw new Error("GitHub repository is invalid.");
  if (!/^v1\.0\.(?:0|[1-9]\d*)(?:-rc\.(?:0|[1-9]\d*))?$/.test(releaseTag || "")) {
    throw new Error("Release tag must be a supported Windows stable or RC tag.");
  }
  if (typeof token !== "string" || token.length === 0) throw new Error("GitHub Actions read token is required.");
  const response = await fetchImpl(
    `https://api.github.com/repos/${repository}/releases/tags/${encodeURIComponent(releaseTag)}`,
    {
      headers: {
        accept: "application/vnd.github+json",
        authorization: `Bearer ${token}`,
        "x-github-api-version": "2022-11-28"
      }
    }
  );
  if (response.status === 404) return;
  if (response.ok) throw new Error("A GitHub Release already exists for this immutable tag.");
  throw new Error(`GitHub Release existence verification failed with HTTP ${response.status}.`);
}

async function main() {
  const result = await verifyExactMainCiGate({
    repository: process.env.GITHUB_REPOSITORY,
    expectedSha: process.env.CPS_EXPECTED_SHA,
    token: process.env.GITHUB_TOKEN
  });
  if (process.env.CPS_REQUIRE_NO_RELEASE === "true") {
    await assertNoExistingRelease({
      repository: process.env.GITHUB_REPOSITORY,
      releaseTag: process.env.CPS_RELEASE_TAG,
      token: process.env.GITHUB_TOKEN
    });
  }
  if (process.env.GITHUB_OUTPUT) {
    const fs = await import("node:fs/promises");
    await fs.appendFile(process.env.GITHUB_OUTPUT, `ci_run_id=${result.runId}\nci_commit=${result.commit}\n`, "utf8");
  }
  process.stdout.write(`Exact push ci-gate verified: ${result.commit} (run ${result.runId})\n`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  await main();
}
