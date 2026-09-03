import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const coreInternalModules = [
  "backup.js",
  "config-file.js",
  "core-error.js",
  "history.js",
  "service.js",
  "sqlite-state.js",
  "storage-layout.js"
];

async function readRepositoryFile(relativePath) {
  return fs.readFile(path.join(repositoryRoot, relativePath), "utf8");
}

function assertNoDeepCoreImports(source, filePath) {
  for (const internalModule of coreInternalModules) {
    assert.doesNotMatch(
      source,
      new RegExp("(?:from|import)\\s*\\(?[\\\"'][^\\\"']*" + internalModule.replace(".", "\\.") + "[\\\"']"),
      filePath + " must use src/public-api.js instead of " + internalModule
    );
  }
}

async function collectDesktopEntryPoints(relativeDirectory) {
  const absoluteDirectory = path.join(repositoryRoot, relativeDirectory);
  let entries;
  try {
    entries = await fs.readdir(absoluteDirectory, { withFileTypes: true });
  } catch (error) {
    if (error?.code === "ENOENT") return [];
    throw error;
  }

  const files = [];
  for (const entry of entries) {
    const relativePath = path.join(relativeDirectory, entry.name);
    if (entry.isDirectory()) {
      files.push(...await collectDesktopEntryPoints(relativePath));
    } else if (/\.(?:[cm]?js|tsx?)$/.test(entry.name)) {
      files.push(relativePath);
    }
  }
  return files;
}

test("CLI and Web entry points import Core behavior only through the public API", async () => {
  for (const entryPoint of ["src/cli.js", "src/web-server.js"]) {
    const source = await readRepositoryFile(entryPoint);
    assert.match(source, /["']\.\/public-api\.js["']/);
    assertNoDeepCoreImports(source, entryPoint);
  }
});

test("Watch calls the internal ProviderSync service without reversing into CoreFacade", async () => {
  const source = await readRepositoryFile("packages/core/src/application/watch-runtime.js");
  assert.match(source, /from ["']\.\/service-runtime\.js["']/);
  assert.doesNotMatch(source, /public-api\.js|createCoreFacade/);
});

test("present and future desktop entry points do not deep-import Core internals", async () => {
  const desktopFiles = await collectDesktopEntryPoints("apps/desktop");
  for (const entryPoint of desktopFiles) {
    assertNoDeepCoreImports(await readRepositoryFile(entryPoint), entryPoint);
  }
});
