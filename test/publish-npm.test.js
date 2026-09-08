import assert from "node:assert/strict";
import test from "node:test";
import { parseArgs, resolveNode16, runPublishingGates } from "../scripts/publish-npm.js";

const runtime = { command: "fixture-node16", npmCli: "fixture-npm8" };
function recordGates(options, failAt = null) {
  const calls = [];
  const run = (args, invocation = {}) => {
    calls.push({ args, invocation });
    if (calls.length === failAt) throw new Error("fixture gate failed");
  };
  return { calls, execute: () => runPublishingGates(options, { run, nodeVersion: "v24.11.1", resolveRuntime: () => runtime, log() {} }) };
}

test("manual npm dry-run enforces both installed runtimes without authentication or publishing", () => {
  const fixture = recordGates(parseArgs(["--dry-run"], {}));
  fixture.execute();
  assert.deepEqual(fixture.calls.map(({ args }) => args.slice(0, 2).join(" ")), [
    "run runtime:verify-node16", "run web:build", "run architecture:check", "test", "run web:test:e2e",
    "run package:smoke:lifecycle", "run package:smoke:lifecycle", "audit --omit=dev", "audit --audit-level=high", "pack --dry-run"
  ]);
  assert.equal(fixture.calls[0].invocation.node16, runtime);
  assert.equal(fixture.calls[5].invocation.node16, undefined);
  assert.equal(fixture.calls[6].invocation.node16, runtime);
  assert.ok(fixture.calls.every(({ args }) => !["whoami", "publish"].includes(args[0])));
});

test("a failure at any manual npm gate blocks publishing", () => {
  for (let failAt = 1; failAt <= 10; failAt += 1) {
    const fixture = recordGates(parseArgs([], {}), failAt);
    assert.throws(fixture.execute, /fixture gate failed/);
    assert.ok(fixture.calls.every(({ args }) => !["whoami", "publish"].includes(args[0])));
  }
});

test("manual publish authenticates only after gates and preserves tag and OTP arguments", () => {
  const fixture = recordGates(parseArgs(["--tag", "next", "--otp", "test-only-otp"], {}));
  fixture.execute();
  assert.equal(fixture.calls.at(-2).args[0], "whoami");
  assert.deepEqual(fixture.calls.at(-1).args, ["publish", "--access", "public", "--tag", "next", "--registry", "https://registry.npmjs.org/", "--otp", "test-only-otp"]);
});

test("manual publishing rejects bypasses and missing or incorrect toolchains", () => {
  assert.throws(() => parseArgs(["--skip-tests"], {}), /cannot be bypassed/);
  assert.throws(() => resolveNode16(parseArgs([], {})), /Node 16.20.2/);
  assert.throws(() => resolveNode16(parseArgs(["--node16", "relative-node"], {})), /absolute/);
  assert.throws(() => runPublishingGates(parseArgs([], {}), { nodeVersion: "v16.20.2" }), /Node 24/);
});
