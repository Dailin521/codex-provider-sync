import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { verifyReleaseVersion } from "./verify-release-version.js";
import { readReleaseMetadata } from "./read-release-metadata.js";

export const HEAVY_JOBS = Object.freeze([
  "test", "workspace-contract", "root-package-compat", "web-build", "web-browser",
  "electron-desktop", "electron-release-candidate", "electron-candidate-set",
  "dependency-audit", "cross-runtime-fixtures", "desktop-test", "desktop-macos",
  "desktop-linux-lock", "c10-evidence-bundle",
]);

export function isOrdinaryDoc(file) {
  return typeof file === "string" && /^(?:README\.md|CHANGELOG\.md|docs\/release-notes\/v[0-9][0-9A-Za-z.+-]*-zh\.md)$/.test(file);
}

export function classifyChanges(event, files) {
  return event === "pull_request" && Array.isArray(files) && files.length > 0 && files.every(isOrdinaryDoc)
    ? "docs" : "full";
}

export function changedFiles(root, base, head) {
  assert.match(base ?? "", /^[a-f0-9]{40}$/);
  assert.match(head ?? "", /^[a-f0-9]{40}$/);
  // Compare the whole PR, not the last commit. Disable rename detection so both paths count.
  const result = execFileSync("git", ["diff", "--name-only", "--no-renames", "-z", `${base}...${head}`, "--"],
    { cwd: root, encoding: "utf8", stdio: "pipe", maxBuffer: 16 * 1024 * 1024 });
  return result.split("\0").filter(Boolean);
}

export function verifyGate(event, needs) {
  assert.equal(needs["change-scope"]?.result, "success", "Change classification must succeed");
  const mode = needs["change-scope"].outputs?.mode;
  assert.ok(mode === "docs" || mode === "full", "Missing or invalid CI mode");
  assert.deepEqual(Object.keys(needs).sort(), ["change-scope", "docs-check", ...HEAVY_JOBS].sort(), "CI job inventory drift");
  if (mode === "docs") {
    assert.equal(event, "pull_request", "Only PRs may skip full CI");
    assert.equal(needs["docs-check"].result, "success", "Documentation checks must pass");
    for (const job of HEAVY_JOBS) assert.equal(needs[job]?.result, "skipped", `${job} must explicitly skip in docs mode`);
  } else {
    assert.equal(needs["docs-check"].result, "skipped");
    for (const job of HEAVY_JOBS) assert.equal(needs[job]?.result, "success", `${job} did not succeed`);
  }
}

export function c10BusinessJobs(needs) {
  assert.equal(needs["change-scope"]?.result, "success");
  assert.equal(needs["change-scope"]?.outputs?.mode, "full");
  const { "change-scope": control, ...jobs } = needs;
  assert.deepEqual(Object.keys(jobs).sort(), HEAVY_JOBS.filter((job) => job !== "c10-evidence-bundle").sort());
  return jobs;
}

function readLinkDestination(text, start) {
  let end = start;
  const angled = text[start] === "<";
  if (angled) end++;
  const contentStart = end;
  let depth = 0;
  for (; end < text.length; end++) {
    const char = text[end];
    if (char === "\\" && /[!"#$%&'()*+,\-./:;<=>?@[\]\\^_`{|}~]/.test(text[end + 1] ?? "")) { end++; continue; }
    if (angled) {
      if (char === ">") return { target: text.slice(contentStart, end), end: end + 1 };
      if (char === "\n" || char === "<") return null;
    } else {
      if (/\s/.test(char) || (char === ")" && depth === 0)) break;
      if (char === "(") depth++;
      if (char === ")") depth--;
    }
  }
  return !angled && depth === 0 && end > start ? { target: text.slice(start, end), end } : null;
}

function linkTargets(body) {
  const targets = [];
  for (const match of body.matchAll(/\]\(\s*/g)) {
    let backslashes = 0;
    for (let i = match.index - 1; i >= 0 && body[i] === "\\"; i--) backslashes++;
    if (backslashes % 2) continue;
    const destination = readLinkDestination(body, match.index + match[0].length);
    if (!destination) continue;
    // A destination must be followed by the link terminator or a quoted title.
    if (/^(?:\s*\)|\s+["'][^\n]*?["']\s*\))/.test(body.slice(destination.end))) targets.push(destination.target);
  }
  for (const match of body.matchAll(/^\s*\[[^\]]+\]:\s*/gm)) {
    const destination = readLinkDestination(body, match.index + match[0].length);
    if (destination) targets.push(destination.target);
  }
  return targets;
}

export function verifyLocalLinks(root, file) {
  const absolute = path.join(root, file);
  assert.ok(fs.existsSync(absolute), `${file}: missing document; deleted documents cannot pass the lightweight check`);
  assert.ok(fs.lstatSync(absolute).isFile(), `${file} must be a regular document`);
  const body = fs.readFileSync(absolute, "utf8").replace(/^\s*(```|~~~)[\s\S]*?^\s*\1.*$/gm, "");
  // Inline links/images and reference destinations; remote URLs and anchors are not fetched.
  const targets = linkTargets(body);
  for (let target of targets) {
    target = target.replace(/\\([!"#$%&'()*+,\-./:;<=>?@[\]\\^_`{|}~])/g, "$1");
    if (/^(?:[a-z][a-z0-9+.-]*:|\/\/|#)/i.test(target)) continue;
    target = decodeURIComponent(target.split(/[?#]/)[0]);
    if (!target) continue;
    const destination = target.startsWith("/") ? path.join(root, target) : path.resolve(path.dirname(absolute), target);
    const relative = path.relative(root, destination);
    assert.ok(relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative), `${file}: link escapes repository`);
    assert.ok(fs.existsSync(destination), `${file}: missing local link ${target}`);
  }
}

function main() {
  const root = process.cwd();
  const event = process.env.GITHUB_EVENT_NAME;
  const command = process.argv[2];
  if (command === "classify") {
    const files = event === "pull_request" ? changedFiles(root, process.env.CPS_PR_BASE_SHA, process.env.CPS_PR_HEAD_SHA) : [];
    const mode = classifyChanges(event, files);
    fs.appendFileSync(process.env.GITHUB_OUTPUT, `mode=${mode}\n`);
    console.log(`CI mode: ${mode}; ${files.length} changed PR paths`);
  } else if (command === "check") {
    const files = changedFiles(root, process.env.CPS_PR_BASE_SHA, process.env.CPS_PR_HEAD_SHA);
    assert.equal(classifyChanges(event, files), "docs", "Recheck entire PR before accepting docs mode");
    for (const file of files) verifyLocalLinks(root, file);
    const version = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8")).version;
    verifyReleaseVersion({ rootDir: root, tag: `v${version}` });
    for (const file of files.filter((entry) => entry.startsWith("docs/release-notes/"))) {
      readReleaseMetadata({ rootDir: root, tag: path.basename(file, "-zh.md") });
    }
    console.log("Documentation links, version consistency and changed release announcements passed.");
  } else if (command === "gate") {
    verifyGate(event, JSON.parse(process.env.CPS_CI_NEEDS));
  } else if (command === "c10") {
    const needs = c10BusinessJobs(JSON.parse(process.env.CPS_REQUIRED_JOB_RESULTS_JSON));
    // Keep the existing exact C10 business-job inventory and evidence schema unchanged.
    execFileSync(process.execPath, ["scripts/write-c10-evidence-bundle.mjs"], {
      stdio: "inherit", env: { ...process.env, CPS_REQUIRED_JOB_RESULTS_JSON: JSON.stringify(needs) },
    });
  } else throw new Error("Usage: node scripts/ci-docs.mjs classify|check|gate|c10");
}

if (process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url) main();
