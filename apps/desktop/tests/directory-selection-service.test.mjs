import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { DirectorySelectionService } from "../dist/main/directory-selection-service.js";

test("directory selection tokens are opaque, typed and single-use", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-directory-selection-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const service = new DirectorySelectionService();
  const selected = await service.authorize("codex-home", root);
  assert.equal(selected.token.includes(root), false);
  assert.equal(service.consume(selected.token, "codex-home"), await fs.realpath(root));
  assert.throws(() => service.consume(selected.token, "codex-home"), /expired|already used/);
});

test("directory selection token cannot be consumed for another path kind", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-directory-kind-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const service = new DirectorySelectionService();
  const selected = await service.authorize("sqlite-home", root);
  assert.throws(() => service.consume(selected.token, "codex-home"), /expired|already used/);
});
