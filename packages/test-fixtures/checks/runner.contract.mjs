import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  createRuntimeDifference,
  readFixtureManifest,
  runFixtureInTemp,
  validateFixtureManifest
} from "../src/index.js";

async function createMinimalFixture(parent, name = "source") {
  const sourceRoot = path.join(parent, name);
  const codexHome = path.join(sourceRoot, "codex-home");
  await fs.mkdir(codexHome, { recursive: true });
  await fs.writeFile(path.join(sourceRoot, "fixture.json"), JSON.stringify({
    schemaVersion: 1,
    id: "minimal",
    description: "Synthetic fixture",
    containsRealUserData: false,
    inputs: { codexHome: "codex-home" },
    expected: {}
  }));
  await fs.writeFile(path.join(codexHome, "config.toml"), 'model_provider = "openai"\n');
  return { sourceRoot, codexHome };
}

test("fixture runner copies only into a temporary directory and cleans it", async () => {
  const testRoot = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-fixture-contract-"));
  const { sourceRoot } = await createMinimalFixture(testRoot);

  let stagedRoot;
  try {
    const result = await runFixtureInTemp(sourceRoot, async (fixture) => {
      stagedRoot = fixture.root;
      assert.notEqual(fixture.root, sourceRoot);
      assert.equal(await fs.readFile(path.join(fixture.codexHome, "config.toml"), "utf8"), 'model_provider = "openai"\n');
      return fixture.manifest.id;
    }, { tempParent: testRoot });
    assert.equal(result, "minimal");
    await assert.rejects(fs.access(stagedRoot));
  } finally {
    await fs.rm(testRoot, { recursive: true, force: true });
  }
});

test("fixture manifest validation is strict and rejects sensitive fields", () => {
  const base = {
    schemaVersion: 1,
    id: "safe-fixture",
    description: "Synthetic fixture",
    containsRealUserData: false,
    inputs: { codexHome: "codex-home" },
    expected: {}
  };
  assert.doesNotThrow(() => validateFixtureManifest(base));
  assert.throws(() => validateFixtureManifest({ ...base, extra: true }));
  assert.throws(() => validateFixtureManifest({ ...base, inputs: { ...base.inputs, path: "outside" } }));
  assert.throws(() => validateFixtureManifest({ ...base, id: "Unsafe ID" }));
  assert.throws(() => validateFixtureManifest({ ...base, inputs: { codexHome: "../outside" } }));
  assert.throws(() => validateFixtureManifest({ ...base, expected: { messageBody: "private" } }));
  assert.throws(() => validateFixtureManifest({ ...base, expected: { apiKey: "private" } }));
  assert.throws(() => validateFixtureManifest({ ...base, expected: { apiToken: "private" } }));
  assert.throws(() => validateFixtureManifest({ ...base, expected: { sessionCookie: "private" } }));
  const { expected: _expected, ...withoutExpected } = base;
  assert.throws(() => validateFixtureManifest(withoutExpected));
});

test("fixture trees reject sensitive files and nested symbolic links", async () => {
  const testRoot = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-fixture-safety-"));
  try {
    const sensitive = await createMinimalFixture(testRoot, "sensitive");
    await fs.writeFile(path.join(sensitive.codexHome, "access-token.json"), "{}");
    await assert.rejects(readFixtureManifest(sensitive.sourceRoot), /forbidden file/);

    const apiKey = await createMinimalFixture(testRoot, "api-key");
    await fs.writeFile(path.join(apiKey.codexHome, "api-key.json"), "{}");
    await assert.rejects(readFixtureManifest(apiKey.sourceRoot), /forbidden file/);

    const apiToken = await createMinimalFixture(testRoot, "api-token");
    await fs.writeFile(path.join(apiToken.codexHome, "openaiApiToken.json"), "{}");
    await assert.rejects(readFixtureManifest(apiToken.sourceRoot), /forbidden file/);

    const linked = await createMinimalFixture(testRoot, "linked");
    const outside = path.join(testRoot, "outside.txt");
    await fs.writeFile(outside, "outside");
    try {
      await fs.symlink(outside, path.join(linked.codexHome, "linked.txt"), "file");
      await assert.rejects(readFixtureManifest(linked.sourceRoot), /symbolic links/);
    } catch (error) {
      if (error?.code !== "EPERM" && error?.code !== "EACCES") throw error;
    }
  } finally {
    await fs.rm(testRoot, { recursive: true, force: true });
  }
});

test("fixture root itself cannot be a symbolic link or junction", async (t) => {
  const testRoot = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-fixture-root-link-"));
  try {
    const { sourceRoot } = await createMinimalFixture(testRoot);
    const linkRoot = path.join(testRoot, "source-link");
    try {
      await fs.symlink(sourceRoot, linkRoot, process.platform === "win32" ? "junction" : "dir");
    } catch (error) {
      if (error?.code === "EPERM" || error?.code === "EACCES") {
        t.skip("Creating a directory link is not permitted on this host.");
        return;
      }
      throw error;
    }
    await assert.rejects(readFixtureManifest(linkRoot), /real directory/);
  } finally {
    await fs.rm(testRoot, { recursive: true, force: true });
  }
});

test("fixture runner cleans staged data when the callback fails", async () => {
  const testRoot = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-fixture-cleanup-"));
  let stagedRoot;
  const expected = new Error("synthetic callback failure");
  try {
    const { sourceRoot } = await createMinimalFixture(testRoot);
    await assert.rejects(
      runFixtureInTemp(sourceRoot, async (fixture) => {
        stagedRoot = fixture.root;
        throw expected;
      }, { tempParent: testRoot }),
      (error) => error === expected
    );
    assert.ok(stagedRoot);
    await assert.rejects(fs.access(stagedRoot));
  } finally {
    await fs.rm(testRoot, { recursive: true, force: true });
  }
});

test("difference records make unresolved runtime mismatches blocking", () => {
  assert.deepEqual(createRuntimeDifference({
    fixtureId: "backup-roundtrip",
    status: "blocked",
    node: { hash: "node" },
    dotnet: { hash: "dotnet" },
    decision: "Do not enable the next write stage.",
    notes: ["hash mismatch"]
  }), {
    schemaVersion: 1,
    fixtureId: "backup-roundtrip",
    status: "blocked",
    node: { hash: "node" },
    dotnet: { hash: "dotnet" },
    decision: "Do not enable the next write stage.",
    notes: ["hash mismatch"]
  });
});
