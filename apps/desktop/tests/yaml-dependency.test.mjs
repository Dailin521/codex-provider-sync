import assert from "node:assert/strict";
import { createRequire } from "node:module";
import test from "node:test";

const require = createRequire(import.meta.url);
// Exercise the parser resolved by the production updater, not a test-only copy.
const updaterRequire = createRequire(require.resolve("electron-updater/package.json"));
const yaml = updaterRequire("js-yaml");

test("updater YAML rejects repeated empty merge sources within a bounded budget", () => {
  // GHSA-2883-xcg3-v3hh: empty mappings must consume the merge budget too.
  // Deliberately tiny fixture: proves rejection without a timing-based DoS test.
  const source = "empty: &empty {}\nvalue:\n  <<: [*empty, *empty, *empty]\n";
  assert.throws(() => yaml.load(source, { maxTotalMergeKeys: 2 }), /maxTotalMergeKeys/);
});

test("updater YAML still parses ordinary update metadata and bounded merges", () => {
  const source = [
    "defaults: &defaults",
    "  channel: stable",
    "version: 1.0.1",
    "files:",
    "  - url: fixture-setup.exe",
    "    sha512: fixture-only",
    "    size: 123",
    "release:",
    "  <<: *defaults",
    "  name: Fixture release",
    ""
  ].join("\n");
  const parsed = yaml.load(source);
  assert.equal(parsed.version, "1.0.1");
  assert.deepEqual(parsed.files, [{ url: "fixture-setup.exe", sha512: "fixture-only", size: 123 }]);
  assert.deepEqual(parsed.release, { channel: "stable", name: "Fixture release" });
});
