import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import test from "node:test";
import { changedFiles, classifyChanges, c10BusinessJobs, checkDocumentation, HEAVY_JOBS, RELEASE_JOBS, requiredJobs, scopeOutputs, verifyGate, verifyLocalLinks } from "../scripts/ci-docs.mjs";

function fixture(run) {
  const base = process.platform === "win32" && fs.existsSync("D:/Temp") ? "D:/Temp" : os.tmpdir();
  const root = fs.mkdtempSync(path.join(base, "cps-docs-ci-"));
  try { return run(root); }
  finally { fs.rmSync(root, { recursive: true, force: true }); }
}

test("only explicit ordinary docs in a nonempty whole PR permit the light path", () => {
  const docs = ["README.md", "CHANGELOG.md", "CONTRIBUTING.md", "CONTRIBUTORS.md",
    ".github/pull_request_template.md", "docs/README_CLI_ZH.md", "docs/README_JA.md",
    "docs/README_DESKTOP_EN.md", "docs/WORKING_PRINCIPLE_ZH.md", "docs/AUTOMATION_QUICKSTART_ZH.md",
    "docs/RELEASE_NOTES_V0.5.0.md", "docs/release-notes/TEMPLATE-zh.md", "docs/release-notes/v1.0.2-zh.md"];
  assert.equal(classifyChanges("pull_request", docs), "docs");
  for (const file of ["src/main.js", "package-lock.json", ".github/workflows/ci.yml", "AGENTS.md",
    "docs/adr/0042.md", "docs/architecture/contracts/CORE.md", "docs/migration/BEHAVIOR_FIXTURES_ZH.md",
    "docs/WINDOWS_ELECTRON_RELEASE_ZH.md", "docs/NPM_PUBLISHING.md", "docs/MAIN_BRANCH_PROTECTION.md",
    "docs/CI_PERFORMANCE_ZH.md", "docs/VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md", "docs/README_EN.js",
    "docs/unknown.md", "docs/release-notes/../config.md", "docs/README_..md", "docs\\README_EN.md", "README.md\nother.js"]) {
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

function results(mode, event = mode === "docs" ? "pull_request" : "push") {
  return { "change-scope": { result: "success", outputs: { mode } },
    "docs-check": { result: mode === "docs" ? "success" : "skipped" },
    ...Object.fromEntries(HEAVY_JOBS.map((job) => [job, {
      result: requiredJobs(event, mode).includes(job) ? "success" : "skipped"
    }])) };
}

test("gate permits only documented PR skips and requires complete main release evidence", () => {
  verifyGate("pull_request", results("docs"));
  verifyGate("pull_request", results("full", "pull_request"));
  verifyGate("push", results("full"));
  assert.throws(() => verifyGate("push", results("docs")));
  for (const [event, mode] of [...["docs", "web", "desktop-ui", "ui", "desktop", "legacy", "full"].map((mode) => ["pull_request", mode]), ["push", "full"]]) {
    verifyGate(event, results(mode, event));
    for (const result of ["failure", "cancelled", "skipped"]) {
      const needs = results(mode, event); needs["change-scope"].result = result;
      assert.throws(() => verifyGate(event, needs));
    }
    for (const job of HEAVY_JOBS) {
      const expected = results(mode, event)[job].result;
      for (const result of ["failure", "cancelled", expected === "success" ? "skipped" : "success"]) {
        const needs = results(mode, event); needs[job].result = result;
        assert.throws(() => verifyGate(event, needs), `${event}: ${mode}: ${job}: ${result}`);
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

test("footnotes and indented code do not create spurious destinations", () => fixture((root) => {
  const readme = path.join(root, "README.md");
  for (const body of ["[^1]: Explanatory prose", "    [sample](missing.md)\n    <img src='missing.png'>",
    "Paragraph\n\n\t[sample](missing.md)\n\n    [sample](missing.md)"]) {
    fs.writeFileSync(readme, body);
    verifyLocalLinks(root, "README.md");
  }
  for (const body of ["[^1]: See [active](missing.md)", "    [sample](ignored.md)\n\n[active](missing.md)",
    "Paragraph\n    [active](missing.md)"]) {
    fs.writeFileSync(readme, body);
    assert.throws(() => verifyLocalLinks(root, "README.md"), /missing local link/);
  }
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

test("workflow keeps safety checks on code PRs and release evidence on main pushes", () => {
  const workflow = fs.readFileSync(new URL("../.github/workflows/ci.yml", import.meta.url), "utf8");
  const jobs = Object.fromEntries([...workflow.slice(workflow.indexOf("jobs:")).matchAll(/^  ([a-z][a-z0-9-]+):\r?\n([\s\S]*?)(?=^  [a-z][a-z0-9-]+:|$(?![\s\S]))/gm)]
    .map((match) => [match[1], match[2]]));
  assert.deepEqual(Object.keys(jobs).sort(), ["change-scope", "docs-check", "ci-gate", ...HEAVY_JOBS].sort());
  for (const job of HEAVY_JOBS) {
    assert.match(jobs[job], /needs[\s\S]*change-scope/);
    const jobCondition = jobs[job].match(/^    if: (.*)$/m)[1];
    if (RELEASE_JOBS.includes(job)) {
      assert.match(jobCondition, /github\.event_name == 'push'/);
      assert.match(jobCondition, /needs\.change-scope\.outputs\.mode == 'full'/);
    } else assert.equal(jobCondition.trim(), `contains(fromJSON(needs.change-scope.outputs.jobs), '${job}')`);
    assert.ok(jobs["ci-gate"].includes(`- ${job}\n`) || jobs["ci-gate"].includes(`- ${job}\r\n`));
  }
  assert.match(jobs["ci-gate"], /if: \$\{\{ always\(\) \}\}/);
  assert.match(jobs["ci-gate"], /CPS_CI_NEEDS: \$\{\{ toJSON\(needs\) \}\}/);
  assert.match(jobs["ci-gate"], /node scripts\/ci-docs\.mjs gate/);
  assert.match(jobs["c10-evidence-bundle"], /node scripts\/ci-docs\.mjs c10/);
  assert.match(jobs["workspace-contract"], /run: npm run architecture:check/);
  assert.match(jobs.test, /run: npm test/);
  assert.match(jobs["cross-runtime-fixtures"], /run: npm run fixtures:cross-runtime/);
  assert.match(jobs["cross-runtime-fixtures"], /run: npm run fixtures:historical-tags/);
  assert.match(jobs["cross-runtime-fixtures"], /name: Verify a hosted formal Release backup[^\n]*\r?\n        if: github\.event_name == 'push'/);
  assert.match(jobs["cross-runtime-fixtures"], /name: Upload hosted formal Release backup evidence\r?\n        if: github\.event_name == 'push'/);
  assert.doesNotMatch(jobs["cross-runtime-fixtures"], /run: throw|head\.repo/);
  assert.match(workflow, /cancel-in-progress: \$\{\{ github\.event_name == 'pull_request' \}\}/);
  for (const key of ["jobs", "platforms", "electron_platforms", "node_versions"]) {
    assert.ok(jobs["change-scope"].includes(`${key}: \${{ steps.scope.outputs.${key} }}`));
  }
  assert.match(jobs["electron-desktop"], /os: \$\{\{ fromJSON\(needs\.change-scope\.outputs\.electron_platforms\) \}\}/);
  for (const job of ["test", "workspace-contract", "root-package-compat", "web-build"]) {
    assert.match(jobs[job], /os: \$\{\{ fromJSON\(needs\.change-scope\.outputs\.platforms\) \}\}/);
  }
  assert.match(jobs.test, /node-version: \$\{\{ fromJSON\(needs\.change-scope\.outputs\.node_versions\) \}\}/);
});

test("UI and Legacy scopes select their consumers without unrelated release work", () => {
  const cases = [
    ["web", ["apps/web/src/main.tsx"], ["workspace-contract", "web-build", "web-browser"]],
    ["desktop-ui", ["apps/desktop/src/renderer/styles.css"], ["workspace-contract", "electron-desktop"]],
    ["ui", ["packages/app-ui/src/home.tsx"], ["workspace-contract", "web-build", "web-browser", "electron-desktop"]],
    ["ui", ["packages/design-system/src/tokens.css"], ["workspace-contract", "web-build", "web-browser", "electron-desktop"]],
    ["ui", ["apps/web/src/main.tsx", "apps/desktop/src/renderer/main.tsx"], ["workspace-contract", "web-build", "web-browser", "electron-desktop"]],
    ["desktop", ["apps/desktop/src/main/updater.ts"], ["test", "workspace-contract", "electron-desktop"]],
    ["desktop", ["apps/desktop/scripts/build.mjs", "apps/desktop/src/renderer/main.tsx"], ["test", "workspace-contract", "electron-desktop"]],
    ["legacy", ["desktop/CodexProviderSync.Core/RestoreService.cs"], ["test", "cross-runtime-fixtures", "desktop-test", "desktop-macos", "desktop-linux-lock"]],
  ];
  for (const [mode, files, expected] of cases) {
    const outputs = scopeOutputs("pull_request", [...files, "CONTRIBUTING.md"]);
    assert.equal(outputs.mode, mode);
    assert.deepEqual(JSON.parse(outputs.jobs), expected);
    if (["web", "desktop-ui", "ui"].includes(mode)) {
      assert.deepEqual(JSON.parse(outputs.platforms), ["ubuntu-latest"]);
      assert.deepEqual(JSON.parse(outputs.electron_platforms), ["windows-latest"]);
      assert.deepEqual(JSON.parse(outputs.node_versions), ["24"]);
    } else {
      assert.deepEqual(JSON.parse(outputs.platforms), ["windows-latest", "ubuntu-latest"]);
      assert.deepEqual(JSON.parse(outputs.electron_platforms), ["windows-latest", "ubuntu-latest", "macos-latest"]);
    }
    assert.equal(scopeOutputs("push", files).mode, "full");
    assert.deepEqual(JSON.parse(scopeOutputs("push", files).jobs), HEAVY_JOBS);
  }
});

test("Core, dependencies, shared contracts, CI and unknown paths retain full safety coverage", () => {
  for (const file of ["src/session-files.js", "packages/core/src/application/restore.js",
    "packages/contracts/src/index.ts", "packages/core-client/src/index.ts", "packages/test-fixtures/static/data.json",
    "package.json", "package-lock.json", "apps/web/package.json", "apps/desktop/package.json",
    "packages/app-ui/package.json", "apps/web/tsconfig.json", "tsconfig.workspaces.json",
    ".github/workflows/ci.yml", "scripts/package-release-assets.ps1", "test/provider-sync-lite.test.js",
    "new-module/file.js", "apps/web/../core.js", "apps/web//main.ts", "apps/web/src/main.ts\n"]) {
    const outputs = scopeOutputs("pull_request", ["apps/web/src/main.tsx", file]);
    assert.equal(outputs.mode, "full", file);
    assert.deepEqual(JSON.parse(outputs.jobs), HEAVY_JOBS.filter((job) => !RELEASE_JOBS.includes(job)));
    assert.equal(JSON.parse(outputs.platforms).length, 2);
    assert.equal(JSON.parse(outputs.electron_platforms).length, 3);
    assert.deepEqual(JSON.parse(outputs.node_versions), ["16.20.2", "24"]);
  }
  assert.equal(classifyChanges("pull_request", ["desktop/Core.cs", "apps/web/src/main.tsx"]), "full");
  for (const mode of ["docs", "web", "desktop-ui", "ui", "desktop", "legacy", "unknown", "__proto__"]) {
    assert.throws(() => requiredJobs("push", mode));
  }
});

test("ordinary documentation and release templates need no product version files", () => fixture((root) => {
  fs.writeFileSync(path.join(root, "README.md"), "Documentation only\n");
  fs.mkdirSync(path.join(root, "docs/release-notes"), { recursive: true });
  fs.writeFileSync(path.join(root, "docs/release-notes/TEMPLATE-zh.md"), "Template\n");
  checkDocumentation(root, ["README.md", "docs/release-notes/TEMPLATE-zh.md"]);
  fs.writeFileSync(path.join(root, "README.md"), "[broken](missing.md)\n");
  assert.throws(() => checkDocumentation(root, ["README.md"]), /missing local link/);
  fs.writeFileSync(path.join(root, "docs/release-notes/v1.0.4-zh.md"), "Release\n");
  assert.throws(() => checkDocumentation(root, ["docs/release-notes/v1.0.4-zh.md"]), /package.json/);
}));
