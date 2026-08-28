import { EventEmitter } from "node:events";
import { spawnSync } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import test, { afterEach } from "node:test";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";

const cleanups = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });

import {
  applySessionChanges,
  collectSessionChanges,
  restoreSessionChanges,
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

test("native Windows helper recovers short writes and flush failures", {
  skip: process.platform !== "win32"
}, () => {
  const result = spawnSync("powershell.exe", ["-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
    "-File", fileURLToPath(new URL("./windows-provider-bytes.ps1", import.meta.url))], { encoding: "utf8", timeout: 60000 });
  assert.equal(result.status, 0, result.stdout + result.stderr);
  assert.match(result.stdout, /PASS: native partial-write exception/);
});

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

test("real Windows in-place apply/recovery retains file ID and appended data", {
  skip: process.platform !== "win32"
}, async (t) => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "provider-bytes-windows-"));
  cleanups.push(() => fs.rm(home, { recursive: true, force: true }));
  await fs.mkdir(path.join(home, "sessions"));
  const file = path.join(home, "sessions", "rollout-[fixture].jsonl");
  const firstLine = JSON.stringify({ type: "session_meta", payload: { id: "fixture", model_provider: "openai" } });
  const original = firstLine + '\r\n{"type":"event_msg","payload":{}}\r\n';
  await fs.writeFile(file, original);
  const { changes } = await collectSessionChanges(home, "prov_a", { fast: true });
  const before = await fs.stat(file, { bigint: true });
  assert.equal((await applySessionChanges(changes)).inPlaceChanges, 1);
  assert.equal((await fs.stat(file, { bigint: true })).ino, before.ino);
  const entry = { ...changes[0], mutation: changes[0].inPlaceMutation };
  const h = await fs.open(file, "r+");
  const partial = Buffer.from(entry.mutation.originalBase64, "base64").subarray(0, 3);
  await h.write(partial, 0, partial.length, entry.mutation.byteOffset);
  await h.close();
  await fs.appendFile(file, "appended\r\n");
  await restoreSessionChanges([entry]);
  await restoreSessionChanges([entry]);
  assert.equal((await fs.stat(file, { bigint: true })).ino, before.ino);
  assert.equal(await fs.readFile(file, "utf8"), original + "appended\r\n");
  const unknown = await fs.open(file, "r+");
  await unknown.write(Buffer.from("!"), 0, 1, entry.mutation.byteOffset + 1);
  await unknown.close();
  await assert.rejects(restoreSessionChanges([entry]), AggregateError);
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
