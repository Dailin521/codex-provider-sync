import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import test from "node:test";

const repositoryRoot = path.resolve(new URL("..", import.meta.url).pathname.replace(/^\/(?:([A-Za-z]:))/, "$1"));
const workspacePaths = [
  "apps/cli",
  "apps/web",
  "apps/desktop",
  "packages/core",
  "packages/contracts",
  "packages/core-client",
  "packages/app-ui",
  "packages/design-system",
  "packages/test-fixtures"
];
const dependencyFields = ["dependencies", "devDependencies", "optionalDependencies", "peerDependencies"];

async function manifest(relativePath) {
  return JSON.parse(await fs.readFile(path.join(repositoryRoot, relativePath, "package.json"), "utf8"));
}

test("root npm package remains a Node 16 surface with only the audited shared Web runtime", async () => {
  const root = await manifest("");
  assert.equal(root.name, "@dailin521/codex-provider-sync");
  assert.equal(root.engines.node, ">=16.20.2");
  assert.equal(root.bin["codex-provider"], "src/cli.js");
  assert.deepEqual(root.dependencies ?? {}, {});
  assert.ok(root.files.includes("src"));
  assert.ok(root.files.includes("web/dist"));
  assert.equal(root.files.some((entry) => entry.startsWith("apps")), false);
  assert.deepEqual(
    root.files.filter((entry) => entry.startsWith("packages")).sort(),
    ["packages/contracts/dist", "packages/core/src"]
  );
  const webConfig = await fs.readFile(path.join(repositoryRoot, "apps/web/vite.config.ts"), "utf8");
  assert.match(webConfig, /sourcemap:\s*false/);
});

test("all C4 workspaces are private and direct dependency versions are exact", async () => {
  for (const workspacePath of workspacePaths) {
    const workspace = await manifest(workspacePath);
    assert.equal(workspace.private, true, workspacePath);
    for (const field of dependencyFields) {
      for (const [name, specification] of Object.entries(workspace[field] ?? {})) {
        assert.match(specification, /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/, `${workspacePath} ${field}.${name}`);
      }
    }
  }
});

test("Core bridge has one explicit transition exception and no deep implementation import", async () => {
  const source = await fs.readFile(path.join(repositoryRoot, "packages/core/src/index.js"), "utf8");
  const imports = [...source.matchAll(/from\s+["'](\.\.\/\.\.\/\.\.\/src\/[^"']+)["']/g)]
    .map((match) => match[1]);
  assert.deepEqual(imports, ["../../../src/public-api.js"]);
  assert.doesNotMatch(source, /src\/(service|locking|backup|history|watch)\.js/);
});

test("transitional root declarations match runtime exports and mark legacy adapters", async () => {
  const declarations = await fs.readFile(path.join(repositoryRoot, "src/public-api.d.ts"), "utf8");
  const declaredNames = [...declarations.matchAll(/export\s+(?:declare\s+)?(?:const|class|function)\s+([A-Za-z0-9_]+)/g)]
    .map((match) => match[1])
    .sort();
  const runtime = await import("../src/public-api.js");
  assert.deepEqual(declaredNames, Object.keys(runtime).sort());
  for (const adapter of ["runSync", "runSwitch", "runRestore", "runPruneBackups", "runWatch"]) {
    assert.match(
      declarations,
      new RegExp(`@deprecated[^]*?export function ${adapter}\\(`),
      `${adapter} must remain explicitly deprecated during migration`
    );
  }
});
