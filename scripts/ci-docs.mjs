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

function maskExamples(body) {
  // Match code delimiters by their full run length; shorter runs are code content.
  // Scan in source order so fence-like text inside comments/code cannot hide later links.
  let result = "";
  for (let i = 0; i < body.length;) {
    let end = i;
    const lineStart = i === 0 || body[i - 1] === "\n";
    const indented = lineStart && /^(?: {4}| {0,3}\t)/.test(body.slice(i));
    const preceding = body.slice(0, i).trimEnd();
    // Indented code cannot interrupt a paragraph. Only start after a blank line
    // (or at BOF); consume its indented continuation and intervening blank lines.
    const blankBefore = i === 0 || /\n[ \t]*\r?\n$/.test(body.slice(0, i));
    const fence = lineStart && /^ {0,3}(`{3,}|~{3,})([^\n]*)/.exec(body.slice(i));
    if (indented && (blankBefore || !preceding)) {
      end = i;
      while (end < body.length) {
        const lineEnd = body.indexOf("\n", end);
        const next = lineEnd < 0 ? body.length : lineEnd + 1;
        const line = body.slice(end, next);
        if (!/^(?: {4}| {0,3}\t)/.test(line) && line.trim()) break;
        end = next;
      }
    } else if (fence && !(fence[1][0] === "`" && fence[2].includes("`"))) {
      const closers = /^ {0,3}(`{3,}|~{3,})[ \t]*\r?$/gm;
      closers.lastIndex = i + fence[0].length;
      end = body.length;
      let close;
      while ((close = closers.exec(body))) {
        if (close[1][0] === fence[1][0] && close[1].length >= fence[1].length) { end = closers.lastIndex; break; }
      }
    } else if (body.startsWith("<!--", i)) {
      const close = body.indexOf("-->", i + 4);
      end = close < 0 ? body.length : close + 3;
    } else if (body[i] === "`") {
      let escapes = 0;
      for (let j = i - 1; j >= 0 && body[j] === "\\"; j--) escapes++;
      const opening = /^`+/.exec(body.slice(i))[0];
      if (!(escapes % 2)) {
        const runs = /`+/g;
        runs.lastIndex = i + opening.length;
        let run;
        while ((run = runs.exec(body))) {
          if (run[0].length === opening.length) { end = runs.lastIndex; break; }
        }
      }
      if (end === i) { result += opening; i += opening.length; continue; }
    }
    if (end > i) { result += body.slice(i, end).replace(/[^\r\n]/g, " "); i = end; }
    else result += body[i++];
  }
  return result;
}

function hasLinkEnd(text) {
  if (/^\s*\)/.test(text)) return true;
  const title = /^\s+(["'(])/.exec(text);
  if (!title) return false;
  const close = title[1] === "(" ? ")" : title[1];
  for (let i = title[0].length; i < text.length; i++) {
    if (text[i] === "\\") { i++; continue; }
    if (text[i] === close) return /^\s*\)/.test(text.slice(i + 1));
  }
  return false;
}

function decodeEntities(target) {
  return target.replace(/&(#x[0-9a-f]+|#[0-9]+|[a-z][a-z0-9]+);/gi, (entity, value) => {
    if (value[0] === "#") {
      const code = value[1].toLowerCase() === "x" ? parseInt(value.slice(2), 16) : Number(value.slice(1));
      assert.ok(code > 0 && code <= 0x10ffff && !(code >= 0xd800 && code <= 0xdfff), "Invalid link character entity");
      return String.fromCodePoint(code);
    }
    const named = { amp: "&", quot: '"', apos: "'", lt: "<", gt: ">", percnt: "%" };
    assert.ok(Object.prototype.hasOwnProperty.call(named, value), `Unsupported link character entity: ${entity}`);
    return named[value];
  });
}

function decodePercent(target) {
  // Decode one UTF-8 sequence at a time, retaining literal or malformed percent text.
  return target.replace(/%(?:[0-7][0-9a-f]|[cd][0-9a-f]%[89ab][0-9a-f]|e[0-9a-f](?:%[89ab][0-9a-f]){2}|f[0-4](?:%[89ab][0-9a-f]){3})/gi,
    (sequence) => { try { return decodeURIComponent(sequence); } catch { return sequence; } });
}

function linkTargets(body) {
  const targets = [];
  for (const match of body.matchAll(/\]\(\s*/g)) {
    let backslashes = 0;
    for (let i = match.index - 1; i >= 0 && body[i] === "\\"; i--) backslashes++;
    if (backslashes % 2) continue;
    const destination = readLinkDestination(body, match.index + match[0].length);
    if (!destination) continue;
    if (hasLinkEnd(body.slice(destination.end))) targets.push(destination.target);
  }
  for (const match of body.matchAll(/^\s*\[(?!\^)[^\]]+\]:\s*/gm)) {
    const destination = readLinkDestination(body, match.index + match[0].length);
    if (destination) targets.push(destination.target);
  }
  for (const tag of body.matchAll(/<(img|a)\b(?:[^>"']|"[^"]*"|'[^']*')*>/gi)) {
    const attribute = tag[1].toLowerCase() === "img" ? "src" : "href";
    const attributes = tag[0].slice(tag[1].length + 1, -1);
    for (const match of attributes.matchAll(/([^\s=/>]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+)))?/g)) {
      if (match[1].toLowerCase() === attribute) targets.push(match[2] ?? match[3] ?? match[4] ?? "");
    }
  }
  return targets;
}

export function verifyLocalLinks(root, file) {
  const absolute = path.join(root, file);
  assert.ok(fs.existsSync(absolute), `${file}: missing document; deleted documents cannot pass the lightweight check`);
  assert.ok(fs.lstatSync(absolute).isFile(), `${file} must be a regular document`);
  const body = maskExamples(fs.readFileSync(absolute, "utf8"));
  // Inline links/images and reference destinations; remote URLs and anchors are not fetched.
  const targets = linkTargets(body);
  for (let target of targets) {
    target = decodeEntities(target.replace(/\\([!"#$%&'()*+,\-./:;<=>?@[\]\\^_`{|}~])/g, "$1"));
    if (/^(?:[a-z][a-z0-9+.-]*:|\/\/|#)/i.test(target)) continue;
    target = decodePercent(target.split(/[?#]/)[0]);
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
