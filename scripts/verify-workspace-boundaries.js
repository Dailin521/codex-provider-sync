import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REQUIRED_WORKSPACES = [
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
const DEPENDENCY_FIELDS = ["dependencies", "devDependencies", "optionalDependencies", "peerDependencies"];

async function readJson(relativePath) {
  return JSON.parse(await fs.readFile(path.join(repositoryRoot, relativePath), "utf8"));
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function assertExactDependencies(manifest, label) {
  for (const field of DEPENDENCY_FIELDS) {
    for (const [name, specification] of Object.entries(manifest[field] ?? {})) {
      assert(
        typeof specification === "string"
          && /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(specification),
        `${label} ${field}.${name} must use an exact version; found ${String(specification)}.`
      );
    }
  }
}

async function sourceFiles(relativeRoot) {
  const root = path.join(repositoryRoot, relativeRoot);
  const result = [];
  async function visit(directory) {
    for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
      const fullPath = path.join(directory, entry.name);
      if (entry.isDirectory()) await visit(fullPath);
      else if (/\.(?:js|mjs|ts|tsx)$/.test(entry.name)) result.push(fullPath);
    }
  }
  await visit(root);
  return result;
}

const rootManifest = await readJson("package.json");
assert(rootManifest.name === "@dailin521/codex-provider-sync", "Root npm package name changed.");
assert(rootManifest.bin?.["codex-provider"] === "src/cli.js", "Root CLI bin changed.");
assert(rootManifest.engines?.node === ">=16.20.2", "Root Node 16 compatibility contract changed.");
assert(Array.isArray(rootManifest.workspaces), "Root npm workspaces are not enabled.");
assert(rootManifest.workspaces.includes("apps/*") && rootManifest.workspaces.includes("packages/*"), "Root workspace globs are incomplete.");
assert(Object.keys(rootManifest.dependencies ?? {}).length === 0, "Root runtime dependencies must stay Core-only.");
assertExactDependencies(rootManifest, "root package");
const rootDependencyNames = DEPENDENCY_FIELDS.flatMap((field) => Object.keys(rootManifest[field] ?? {}));
assert(
  !rootDependencyNames.some((name) => name === "electron" || name.startsWith("electron-")),
  "Root npm manifest must not contain Electron dependencies."
);

const rootPublishAllowlist = new Set([
  "README.md",
  "CHANGELOG.md",
  "CONTRIBUTING.md",
  "CONTRIBUTORS.md",
  "AGENTS.md",
  "docs",
  "images/README",
  "src",
  "web/dist",
  "packages/contracts/dist",
  "packages/core/src"
]);
for (const entry of rootManifest.files ?? []) {
  assert(rootPublishAllowlist.has(entry), `Root tarball allowlist contains an unapproved path: ${entry}`);
}
for (const required of rootPublishAllowlist) {
  assert(rootManifest.files?.includes(required), `Root tarball allowlist is missing ${required}.`);
}

const manifests = new Map();
for (const workspace of REQUIRED_WORKSPACES) {
  const manifest = await readJson(`${workspace}/package.json`);
  manifests.set(workspace, manifest);
  assert(manifest.private === true, `${workspace} must remain private.`);
  assertExactDependencies(manifest, workspace);
}

const coreSource = await fs.readFile(path.join(repositoryRoot, "packages/core/src/index.js"), "utf8");
const rootImports = [...coreSource.matchAll(/from\s+["'](\.\.\/\.\.\/\.\.\/src\/[^"']+)["']/g)]
  .map((match) => match[1]);
assert(rootImports.length === 1 && rootImports[0] === "../../../src/public-api.js", "Core bridge may import only root src/public-api.js.");

for (const boundary of [
  "packages/contracts/src",
  "packages/core-client/src",
  "packages/app-ui/src",
  "packages/design-system/src"
]) {
  for (const filePath of await sourceFiles(boundary)) {
    const source = await fs.readFile(filePath, "utf8");
    assert(!/from\s+["'](?:node:|electron)/.test(source), `${path.relative(repositoryRoot, filePath)} imports a forbidden platform module.`);
    assert(!/\.\.\/.*src\//.test(source), `${path.relative(repositoryRoot, filePath)} deep-imports implementation source.`);
  }
}

const desktopManifest = manifests.get("apps/desktop");
const desktopDependencies = {
  ...(desktopManifest.dependencies ?? {}),
  ...(desktopManifest.devDependencies ?? {}),
  ...(desktopManifest.optionalDependencies ?? {})
};
assert(
  desktopManifest.dependencies?.["@codex-provider-sync/test-fixtures"] === undefined
    && desktopManifest.devDependencies?.["@codex-provider-sync/test-fixtures"] === "0.0.0",
  "Desktop fault fixtures must remain an exact private devDependency, never a production dependency."
);
const approvedDesktopElectronDependencies = new Map([
  ["electron", { version: "44.0.0", field: "devDependencies", checkpoint: "C6" }],
  ["electron-vite", { version: "5.0.0", field: "devDependencies", checkpoint: "C6" }],
  ["electron-builder", { version: "26.15.7", field: "devDependencies", checkpoint: "C6" }],
  ["electron-updater", { version: "6.8.9", field: "dependencies", checkpoint: "C8" }]
]);
for (const [name, approval] of approvedDesktopElectronDependencies) {
  assert(
    desktopManifest[approval.field]?.[name] === approval.version,
    `apps/desktop must pin ${name} to the reviewed ${approval.checkpoint} version ${approval.version}.`
  );
}
for (const name of Object.keys(desktopDependencies)) {
  if (name === "electron" || name.startsWith("electron-")) {
    assert(approvedDesktopElectronDependencies.has(name), `Unreviewed Electron dependency: ${name}`);
  }
}
for (const [workspace, manifest] of manifests) {
  if (workspace === "apps/desktop") continue;
  const dependencies = DEPENDENCY_FIELDS.flatMap((field) => Object.keys(manifest[field] ?? {}));
  assert(
    !dependencies.some((name) => name === "electron" || name.startsWith("electron-")),
    `${workspace} must not depend on Electron.`
  );
}

for (const filePath of await sourceFiles("apps/desktop/src/renderer")) {
  const source = await fs.readFile(filePath, "utf8");
  assert(!/from\s+["'](?:node:|electron)/.test(source), `${path.relative(repositoryRoot, filePath)} imports Node or Electron.`);
  assert(!/@codex-provider-sync\/core(?:["'/])/.test(source), `${path.relative(repositoryRoot, filePath)} imports Core directly.`);
}
for (const filePath of await sourceFiles("apps/desktop/src/preload")) {
  const source = await fs.readFile(filePath, "utf8");
  assert(!/from\s+["']node:/.test(source), `${path.relative(repositoryRoot, filePath)} imports Node in sandboxed preload.`);
  assert(!/@codex-provider-sync\/core(?:["'/])/.test(source), `${path.relative(repositoryRoot, filePath)} imports Core in preload.`);
}
for (const filePath of await sourceFiles("apps/desktop/src/main")) {
  const source = await fs.readFile(filePath, "utf8");
  assert(!/@codex-provider-sync\/core(?:["'/])/.test(source), `${path.relative(repositoryRoot, filePath)} runs Core in Main.`);
  assert(!/\.\.\/\.\.\/\.\.\/src\//.test(source), `${path.relative(repositoryRoot, filePath)} deep-imports root implementation.`);
}
for (const filePath of await sourceFiles("apps/desktop/src/runtime")) {
  const source = await fs.readFile(filePath, "utf8");
  assert(!/\.\.\/main\//.test(source), `${path.relative(repositoryRoot, filePath)} depends on Electron Main.`);
  assert(!/\.\.\/\.\.\/\.\.\/src\//.test(source), `${path.relative(repositoryRoot, filePath)} deep-imports root implementation.`);
}

process.stdout.write("Workspace package, Electron, dependency, import and root publish boundaries are valid.\n");
