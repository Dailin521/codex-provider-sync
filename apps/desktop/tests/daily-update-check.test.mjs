import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { claimDailyUpdateCheck } from "../dist/main/daily-update-check.js";

test("claims a local day once across concurrent launches and allows the next day", async t => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-daily-update-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const day = new Date(2026, 8, 4, 23, 59);
  const claims = await Promise.all(Array.from({ length: 8 }, () => claimDailyUpdateCheck(root, day)));
  assert.equal(claims.filter(Boolean).length, 1);
  assert.equal(await claimDailyUpdateCheck(root, new Date(2026, 8, 4, 0, 1)), false);
  assert.equal(await claimDailyUpdateCheck(root, new Date(2026, 8, 5, 0, 1)), true);
  assert.deepEqual((await fs.readdir(path.join(root, "update-check-days"))).sort(), ["2026-09-04.checked", "2026-09-05.checked"]);
});

test("cannot persist a claim: skips startup network work instead of retrying each launch", async t => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-daily-update-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  await fs.writeFile(path.join(root, "update-check-days"), "fixture");
  assert.equal(await claimDailyUpdateCheck(root), false);
});
