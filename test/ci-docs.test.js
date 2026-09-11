import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import test from "node:test";
import { changedFiles, classifyChanges, c10BusinessJobs, HEAVY_JOBS, verifyGate, verifyLocalLinks } from "../scripts/ci-docs.mjs";

function fixture(run) {
  const base = process.platform === "win32" && fs.existsSync("D:/Temp") ? "D:/Temp" : os.tmpdir();
  const root = fs.mkdtempSync(path.join(base, "cps-docs-ci-"));
  try { return run(root); }
  finally { fs.rmSync(root, { recursive: true, force: true }); }
}

test("only explicit ordinary docs in a nonempty whole PR permit the light path", () => {
  const docs = ["README.md", "CHANGELOG.md", "docs/release-notes/v1.0.2-zh.md"];
  assert.equal(classifyChanges("pull_request", docs), "docs");
  for (const file of ["src/main.js", "package-lock.json", ".github/workflows/ci.yml", "AGENTS.md",
    "docs/adr/0042.md", "docs/architecture/contracts/CORE.md", "docs/migration/BEHAVIOR_FIXTURES_ZH.md",
    "docs/WINDOWS_ELECTRON_RELEASE_ZH.md", "docs/release-notes/../config.md", "README.md\nother.js"]) {
    assert.equal(classifyChanges("pull_request", [...docs, file]), "full", file);
  }
  for (const event of ["push", undefined, "workflow_dispatch"]) assert.equal(classifyChanges(event, docs), "full");
  for (const files of [[], undefined, null, [""]]) assert.equal(classifyChanges("pull_request", files), "full");
});

test("whole PR comparison retains earlier code changes and both sides of renames", () => fixture((root) => {
  const git = (...args) => execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
  git("init", "--quiet");
  git("config", "core.autocrlf", "false");
  git("config", "user.name", "CI fixture");
  git("config", "user.email", "fixture@example.invalid");
  fs.writeFileSync(path.join(root, "README.md"), "initial\n");
  git("add", "."); git("commit", "--quiet", "-m", "base");
  const base = git("rev-parse", "HEAD");
  fs.writeFileSync(path.join(root, "code.js"), "// product change\n");
  git("add", "."); git("commit", "--quiet", "-m", "code");
  fs.appendFileSync(path.join(root, "README.md"), "docs followup\n");
  git("add", "."); git("commit", "--quiet", "-m", "docs");
  assert.equal(classifyChanges("pull_request", changedFiles(root, base, git("rev-parse", "HEAD"))), "full");
  const beforeRename = git("rev-parse", "HEAD");
  git("mv", "code.js", "CHANGELOG.md"); git("commit", "--quiet", "-m", "rename");
  assert.deepEqual(changedFiles(root, beforeRename, git("rev-parse", "HEAD")).sort(), ["CHANGELOG.md", "code.js"]);
  assert.throws(() => changedFiles(root, "invalid", beforeRename));
  assert.throws(() => changedFiles(root, "a".repeat(40), beforeRename));
}));

function results(mode) {
  return { "change-scope": { result: "success", outputs: { mode } },
    "docs-check": { result: mode === "docs" ? "success" : "skipped" },
    ...Object.fromEntries(HEAVY_JOBS.map((job) => [job, { result: mode === "docs" ? "skipped" : "success" }])) };
}

test("gate accepts documented skips only for a verified PR docs run", () => {
  verifyGate("pull_request", results("docs"));
  verifyGate("push", results("full"));
  assert.throws(() => verifyGate("push", results("docs")));
  for (const mode of ["docs", "full"]) {
    for (const result of ["failure", "cancelled", "skipped"]) {
      const needs = results(mode); needs["change-scope"].result = result;
      assert.throws(() => verifyGate("pull_request", needs));
    }
    for (const job of HEAVY_JOBS) {
      for (const result of ["failure", "cancelled", mode === "full" ? "skipped" : "success"]) {
        const needs = results(mode); needs[job].result = result;
        assert.throws(() => verifyGate("pull_request", needs), `${mode}: ${job}: ${result}`);
      }
    }
  }
  const failedDocs = results("docs"); failedDocs["docs-check"].result = "failure";
  assert.throws(() => verifyGate("pull_request", failedDocs));
  const unknownMode = results("full"); delete unknownMode["change-scope"].outputs.mode;
  assert.throws(() => verifyGate("pull_request", unknownMode));
  const missing = results("full"); delete missing["desktop-test"];
  assert.throws(() => verifyGate("push", missing));
});

test("local links validate existing destinations without remote requests", () => fixture((root) => {
  fs.writeFileSync(path.join(root, "target.md"), "# target\n");
  const readme = path.join(root, "README.md");
  fs.writeFileSync(readme, "[local](target.md#section)\n[remote](https://example.invalid/no-network)\n[ref]: target.md\n");
  verifyLocalLinks(root, "README.md");
  fs.writeFileSync(readme, "[broken](missing.md)\n");
  assert.throws(() => verifyLocalLinks(root, "README.md"), /missing local link/);
  fs.writeFileSync(readme, "[escape](../outside.md)\n");
  assert.throws(() => verifyLocalLinks(root, "README.md"), /escapes repository/);
}));

test("deleted ordinary release notes fail even when their incoming links are unchanged", () => fixture((root) => {
  const note = "docs/release-notes/v1.0.1-zh.md";
  fs.mkdirSync(path.join(root, "docs/release-notes"), { recursive: true });
  fs.writeFileSync(path.join(root, "README.md"), `[Release](${note})\n`);
  fs.writeFileSync(path.join(root, note), "Release notes\n");
  verifyLocalLinks(root, note);
  fs.unlinkSync(path.join(root, note));
  assert.equal(classifyChanges("pull_request", [note]), "docs");
  assert.throws(() => verifyLocalLinks(root, note), /missing document/);
}));

test("local destinations preserve balanced, escaped and angle-bracket parentheses", () => fixture((root) => {
  const readme = path.join(root, "README.md");
  for (const file of ["guide(v2).md", "guide(v2(nested)).md", "guide).md", "guide (v2).md"]) {
    fs.writeFileSync(path.join(root, file), "guide\n");
  }
  fs.writeFileSync(readme, [
    '[guide](guide(v2).md "Title")',
    "![nested](guide(v2(nested)).md)",
    String.raw`[escaped](guide\(v2\).md)`,
    String.raw`[escaped close](guide\).md)`,
    '[angle](<guide (v2).md> "Title")',
    "[reference]: guide(v2(nested)).md",
    String.raw`[escaped-reference]: guide\).md`,
    '[angle-reference]: <guide (v2).md> "Title"',
  ].join("\n"));
  verifyLocalLinks(root, "README.md");
  for (const link of ["[broken](missing(v2).md)", String.raw`[broken](missing\).md)`,
    "[broken](<missing (v2).md>)", "[ref]: missing(v2).md"]) {
    fs.writeFileSync(readme, link);
    assert.throws(() => verifyLocalLinks(root, "README.md"), /missing local link/);
  }
}));

test("parenthesized titles validate active destinations", () => fixture((root) => {
  fs.writeFileSync(path.join(root, "target.md"), "target");
  const readme = path.join(root, "README.md");
  fs.writeFileSync(readme, "[guide](target.md (legacy))");
  verifyLocalLinks(root, "README.md");
  fs.writeFileSync(readme, "[guide](missing.md (legacy))");
  assert.throws(() => verifyLocalLinks(root, "README.md"), /missing local link missing.md/);
}));

test("code spans, fences and HTML comments do not expose example links", () => fixture((root) => {
  const readme = path.join(root, "README.md");
  const examples = [
    "`[example](missing.md)`", "`` ` [example](missing.md) ``",
    "```inline [example](missing.md) ```", "<!-- [example](missing.md) -->",
    "````markdown\n```\n[example](missing.md)\n````",
    "~~~html\n<img src='missing.png'>\n~~~", "`<a href='missing.md'>example</a>`",
    "<!-- <img src='missing.png'> -->",
  ].join("\n");
  fs.writeFileSync(readme, examples);
  verifyLocalLinks(root, "README.md");
  fs.writeFileSync(readme, `${examples}\n[active](missing.md (title))`);
  assert.throws(() => verifyLocalLinks(root, "README.md"), /missing local link/);
  fs.writeFileSync(readme, "<!--\n```\n-->\n[active](missing.md)");
  assert.throws(() => verifyLocalLinks(root, "README.md"), /missing local link/);
  fs.writeFileSync(readme, "````\n[example](missing.md)\n```");
  verifyLocalLinks(root, "README.md"); // Unclosed fence continues to EOF.
}));

test("literal percent and valid UTF-8 escapes coexist in local paths", () => fixture((root) => {
  const readme = path.join(root, "README.md");
  for (const name of ["100%.md", "100% 文.md", "%FF.md"]) fs.writeFileSync(path.join(root, name), "target");
  for (const target of ["100%.md", "100%25.md", "100%%20%E6%96%87.md", "%FF.md"]) {
    fs.writeFileSync(readme, `[guide](${target})`);
    verifyLocalLinks(root, "README.md");
  }
  fs.writeFileSync(readme, "[guide](missing%20%E6%96%87%.md)");
  assert.throws(() => verifyLocalLinks(root, "README.md"), /missing local link missing 文%.md/);
}));

test("rendered HTML image sources and anchor destinations are validated", () => fixture((root) => {
  const readme = path.join(root, "README.md");
  fs.writeFileSync(path.join(root, "target.md"), "target");
  fs.writeFileSync(path.join(root, "image&photo.png"), "image");
  fs.writeFileSync(readme, `<a title="x > y" HREF='target.md'>link</a>\n<img SRC="image&amp;photo.png">\n<a href=target.md>link</a>`);
  verifyLocalLinks(root, "README.md");
  for (const html of ['<img src="missing.png">', "<a href='missing.md'>link</a>", "<img src=missing.png>",
    '<img src="../outside.png">', '<img src="%2e%2e/outside.png">']) {
    fs.writeFileSync(readme, html);
    assert.throws(() => verifyLocalLinks(root, "README.md"), /missing local link|escapes repository/);
  }
  fs.writeFileSync(readme, '<img src="image&unknown;photo.png">');
  assert.throws(() => verifyLocalLinks(root, "README.md"), /Unsupported link character entity/);
}));

test("C10 retains exactly the original business results including failures", () => {
  const needs = results("full");
  delete needs["docs-check"];
  delete needs["c10-evidence-bundle"];
  needs["desktop-test"].result = "failure";
  const jobs = c10BusinessJobs(needs);
  assert.equal(Object.keys(jobs).length, 13);
  assert.equal(jobs["desktop-test"].result, "failure");
  assert.ok(needs["change-scope"], "Do not mutate caller evidence");
  assert.throws(() => c10BusinessJobs({ ...needs, unexpected: { result: "success" } }));
  assert.throws(() => c10BusinessJobs({ ...needs, "change-scope": { result: "success", outputs: { mode: "docs" } } }));
});

test("workflow wires every heavy job and C10 to full mode while always emitting the gate", () => {
  const workflow = fs.readFileSync(new URL("../.github/workflows/ci.yml", import.meta.url), "utf8");
  const jobs = Object.fromEntries([...workflow.slice(workflow.indexOf("jobs:")).matchAll(/^  ([a-z][a-z0-9-]+):\r?\n([\s\S]*?)(?=^  [a-z][a-z0-9-]+:|$(?![\s\S]))/gm)]
    .map((match) => [match[1], match[2]]));
  assert.deepEqual(Object.keys(jobs).sort(), ["change-scope", "docs-check", "ci-gate", ...HEAVY_JOBS].sort());
  for (const job of HEAVY_JOBS) {
    assert.match(jobs[job], /needs[\s\S]*change-scope/);
    assert.match(jobs[job], /if: .*needs\.change-scope\.outputs\.mode == 'full'/);
    assert.ok(jobs["ci-gate"].includes(`- ${job}\n`) || jobs["ci-gate"].includes(`- ${job}\r\n`));
  }
  assert.match(jobs["ci-gate"], /if: \$\{\{ always\(\) \}\}/);
  assert.match(jobs["ci-gate"], /CPS_CI_NEEDS: \$\{\{ toJSON\(needs\) \}\}/);
  assert.match(jobs["ci-gate"], /node scripts\/ci-docs\.mjs gate/);
  assert.match(jobs["c10-evidence-bundle"], /node scripts\/ci-docs\.mjs c10/);
  assert.match(workflow, /cancel-in-progress: \$\{\{ github\.event_name == 'pull_request' \}\}/);
});
