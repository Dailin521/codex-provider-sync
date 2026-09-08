import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";

for (const fails of [false, true]) {
  test(`root test runner executes every file and preserves its exit status (failure=${fails})`, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-root-runner-"));
    try {
      await fs.mkdir(path.join(root, "scripts"));
      await fs.mkdir(path.join(root, "test"));
      await fs.writeFile(path.join(root, "package.json"), '{"type":"module"}');
      await fs.copyFile(new URL("../scripts/run-root-tests.js", import.meta.url), path.join(root, "scripts", "run-root-tests.js"));
      await fs.writeFile(path.join(root, "test", "a.test.js"), `import test from 'node:test'; test('first', () => { ${fails ? "throw new Error('synthetic failure');" : ""} });`);
      await fs.writeFile(path.join(root, "test", "b.test.js"), "import test from 'node:test'; test('last', () => {});");
      const result = spawnSync(process.execPath, ["scripts/run-root-tests.js"], {
        cwd: root, encoding: "utf8", windowsHide: true, timeout: 10_000,
        // The child is the outer runner for its own synthetic test tree.
        env: { ...process.env, NODE_TEST_CONTEXT: undefined }
      });
      assert.equal(result.error, undefined);
      assert.equal(result.status, fails ? 1 : 0, result.stdout + result.stderr);
      assert.match(result.stdout, /(?:b\.test\.js|last)/, "A prior failure must not skip remaining files.");
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
}
