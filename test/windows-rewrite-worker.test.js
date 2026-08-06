import { EventEmitter } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import test from "node:test";
import assert from "node:assert/strict";

import {
  applySessionChanges,
  createWindowsExclusiveRewriteWorker
} from "../src/session-files.js";

function createFakeSpawn({ ready = { protocolVersion: 1, type: "ready" }, respond }) {
  let spawnCount = 0;
  const spawnImpl = () => {
    spawnCount += 1;
    const child = new EventEmitter();
    child.pid = 4242;
    child.stdin = new PassThrough();
    child.stdout = new PassThrough();
    child.stderr = new PassThrough();
    let input = "";
    let exited = false;

    function exit(code, signal = null) {
      if (exited) {
        return;
      }
      exited = true;
      child.stdout.end();
      child.stderr.end();
      queueMicrotask(() => child.emit("exit", code, signal));
    }

    child.kill = () => {
      exit(1, "SIGTERM");
      return true;
    };
    child.stdin.setEncoding("utf8");
    child.stdin.on("data", (chunk) => {
      input += chunk;
      while (input.includes("\n")) {
        const newline = input.indexOf("\n");
        const line = input.slice(0, newline);
        input = input.slice(newline + 1);
        if (!line) {
          continue;
        }
        const request = JSON.parse(line);
        const response = respond(request);
        if (response && typeof response === "object" && Object.hasOwn(response, "exitCode")) {
          exit(response.exitCode, response.signal ?? null);
          continue;
        }
        child.stdout.write(
          typeof response === "string" ? `${response}\n` : `${JSON.stringify(response)}\n`,
          "utf8"
        );
      }
    });
    child.stdin.on("finish", () => exit(0));
    queueMicrotask(() => child.stdout.write(`${JSON.stringify(ready)}\n`, "utf8"));
    return child;
  };
  return { spawnImpl, getSpawnCount: () => spawnCount };
}

test("Windows rewrite worker reuses one process and preserves the closed result set", async () => {
  const expectedResults = ["APPLIED", "APPLIED_IN_PLACE", "SKIP_BUSY", "SKIP_CHANGED"];
  const fake = createFakeSpawn({
    respond(request) {
      return {
        protocolVersion: 1,
        type: "result",
        id: request.id,
        path: request.path,
        result: expectedResults[request.id - 1]
      };
    }
  });
  const worker = await createWindowsExclusiveRewriteWorker({ spawnImpl: fake.spawnImpl });
  const results = [];
  try {
    for (let index = 0; index < expectedResults.length; index += 1) {
      results.push(await worker.rewrite(
        { path: path.resolve(`rollout-${index}.jsonl`) },
        { requireOriginalMatch: true }
      ));
    }
  } finally {
    await worker.close();
  }

  assert.equal(fake.getSpawnCount(), 1);
  assert.deepEqual(results, expectedResults);
});

test("Windows rewrite worker rejects mismatched and malformed protocol responses", async (t) => {
  await t.test("invalid ready message", async () => {
    const fake = createFakeSpawn({
      ready: { protocolVersion: 1, type: "unexpected" },
      respond: () => assert.fail("invalid ready must stop before requests")
    });
    await assert.rejects(
      createWindowsExclusiveRewriteWorker({ spawnImpl: fake.spawnImpl }),
      /Unexpected Windows rewrite worker ready message/
    );
  });

  await t.test("mismatched id", async () => {
    const fake = createFakeSpawn({
      respond(request) {
        return {
          protocolVersion: 1,
          type: "result",
          id: request.id + 1,
          path: request.path,
          result: "APPLIED"
        };
      }
    });
    const worker = await createWindowsExclusiveRewriteWorker({ spawnImpl: fake.spawnImpl });
    await assert.rejects(
      worker.rewrite({ path: path.resolve("rollout-id.jsonl") }, { requireOriginalMatch: true }),
      /Unexpected Windows rewrite worker response/
    );
  });

  await t.test("malformed JSON", async () => {
    const fake = createFakeSpawn({ respond: () => "not-json" });
    const worker = await createWindowsExclusiveRewriteWorker({ spawnImpl: fake.spawnImpl });
    await assert.rejects(
      worker.rewrite({ path: path.resolve("rollout-json.jsonl") }, { requireOriginalMatch: true }),
      /malformed JSON/
    );
  });

  await t.test("worker exits before acknowledgement", async () => {
    const fake = createFakeSpawn({ respond: () => ({ exitCode: 9 }) });
    const worker = await createWindowsExclusiveRewriteWorker({ spawnImpl: fake.spawnImpl });
    await assert.rejects(
      worker.rewrite({ path: path.resolve("rollout-exit.jsonl") }, { requireOriginalMatch: true }),
      /closed stdout unexpectedly/
    );
  });

  await t.test("worker returns an error protocol message", async () => {
    const fake = createFakeSpawn({
      respond(request) {
        return {
          protocolVersion: 1,
          type: "error",
          id: request.id,
          path: request.path,
          message: "injected worker error"
        };
      }
    });
    const worker = await createWindowsExclusiveRewriteWorker({ spawnImpl: fake.spawnImpl });
    await assert.rejects(
      worker.rewrite({ path: path.resolve("rollout-error.jsonl") }, { requireOriginalMatch: true }),
      /Unexpected Windows rewrite worker response/
    );
  });
});

test("applySessionChanges creates one Windows worker and does not queue the next target", {
  skip: process.platform !== "win32"
}, async () => {
  const firstPath = path.resolve("rollout-order-a.jsonl");
  const secondPath = path.resolve("rollout-order-b.jsonl");
  const changes = [firstPath, secondPath].map((filePath) => ({
    path: filePath,
    originalMtimeMs: Date.now(),
    modelRewriteRequired: false,
    modelOnlyChange: false
  }));
  const events = [];
  let factoryCalls = 0;
  let closeCalls = 0;

  const result = await applySessionChanges(changes, {
    windowsRewriteWorkerFactory: async () => {
      factoryCalls += 1;
      return {
        async rewrite(change) {
          events.push(`rewrite:${path.basename(change.path)}`);
          return "APPLIED";
        },
        async close() {
          closeCalls += 1;
        }
      };
    },
    async onBeforeApply(change) {
      events.push(`applying:${path.basename(change.path)}`);
    },
    async onApplied(change) {
      events.push(`applied:${path.basename(change.path)}`);
    }
  });

  assert.equal(factoryCalls, 1);
  assert.equal(closeCalls, 1);
  assert.equal(result.appliedChanges, 2);
  assert.deepEqual(events, [
    "applying:rollout-order-a.jsonl",
    "rewrite:rollout-order-a.jsonl",
    "applied:rollout-order-a.jsonl",
    "applying:rollout-order-b.jsonl",
    "rewrite:rollout-order-b.jsonl",
    "applied:rollout-order-b.jsonl"
  ]);
});

test("real Windows worker handles sequential Unicode and literal wildcard paths", {
  skip: process.platform !== "win32"
}, async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-worker-test-"));
  try {
    const changes = [];
    for (let index = 0; index < 2; index += 1) {
      const filePath = path.join(root, `rollout-[测试 ${index}].jsonl`);
      const originalFirstLine = JSON.stringify({
        type: "session_meta",
        payload: { id: `thread-${index}`, model_provider: "old" }
      });
      await fs.writeFile(filePath, `${originalFirstLine}\n{"type":"event_msg","payload":{}}\n`, "utf8");
      const stat = await fs.stat(filePath);
      changes.push({
        path: filePath,
        originalFirstLine,
        originalSeparator: "\n",
        originalOffset: Buffer.byteLength(originalFirstLine) + 1,
        originalSize: stat.size,
        updatedFirstLine: JSON.stringify({
          type: "session_meta",
          payload: { id: `thread-${index}`, model_provider: "new" }
        })
      });
    }

    const worker = await createWindowsExclusiveRewriteWorker();
    const results = [];
    try {
      for (const change of changes) {
        results.push(await worker.rewrite(change, { requireOriginalMatch: true }));
      }
    } finally {
      await worker.close();
    }

    assert.deepEqual(results, ["APPLIED", "APPLIED"]);
    for (const change of changes) {
      const [firstLine] = (await fs.readFile(change.path, "utf8")).split(/\r?\n/);
      assert.equal(JSON.parse(firstLine).payload.model_provider, "new");
    }
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
