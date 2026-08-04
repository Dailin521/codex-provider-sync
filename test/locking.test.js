import { spawn } from "node:child_process";
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { acquireLock, acquirePathLock } from "../src/locking.js";
import { DEFAULT_LOCK_NAME } from "../src/constants.js";

const TEST_STARTED_AT = "2024-01-02T03:04:05.000Z";
const TEST_MARKER = `test:${TEST_STARTED_AT}`;

async function makeLockHome(t) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-lock-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  return root;
}

function lockOptions(overrides = {}) {
  return {
    getProcessIdentity: async (pid) => pid === process.pid ? TEST_MARKER : null,
    getProcessStartedAtIdentity: async (pid) => pid === process.pid ? TEST_STARTED_AT : null,
    ...overrides
  };
}

async function writeCanonicalOwner(codexHome, owner) {
  const lockDir = path.join(codexHome, "tmp", DEFAULT_LOCK_NAME);
  await fs.mkdir(lockDir, { recursive: true });
  await fs.writeFile(path.join(lockDir, "owner.json"), JSON.stringify(owner), "utf8");
  return lockDir;
}

async function waitForChild(child) {
  return new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve({ code, signal }));
  });
}

test("acquireLock publishes a complete versioned owner and holds a unique claim", async (t) => {
  const codexHome = await makeLockHome(t);
  const lockDir = path.join(codexHome, "tmp", DEFAULT_LOCK_NAME);
  const claimsDir = `${lockDir}.claims`;
  const release = await acquireLock(codexHome, "sync", lockOptions());

  const owner = JSON.parse(await fs.readFile(path.join(lockDir, "owner.json"), "utf8"));
  assert.equal(owner.protocolVersion, 2);
  assert.equal(owner.pid, process.pid);
  assert.equal(owner.processId, process.pid);
  assert.equal(owner.processStartedAt, TEST_STARTED_AT);
  assert.equal(typeof owner.instanceId, "string");
  assert.deepEqual(await fs.readdir(claimsDir), [`${owner.instanceId}.json`]);

  await assert.rejects(acquireLock(codexHome, "other", lockOptions()), /live claim|Lock already exists/);
  await release();
  await assert.rejects(fs.access(lockDir), { code: "ENOENT" });
  assert.deepEqual(await fs.readdir(claimsDir), []);
});

test("acquireLock retries transient candidate creation failures", async (t) => {
  const codexHome = await makeLockHome(t);
  let candidateAttempts = 0;
  const sleepCalls = [];
  const fsImpl = new Proxy(fs, {
    get(target, property) {
      if (property === "mkdir") {
        return async (targetPath, options) => {
          if (path.basename(targetPath).includes(".candidate.")) {
            candidateAttempts += 1;
            if (candidateAttempts < 3) {
              const error = new Error("operation not permitted");
              error.code = "EPERM";
              throw error;
            }
          }
          return target.mkdir(targetPath, options);
        };
      }
      const value = target[property];
      return typeof value === "function" ? value.bind(target) : value;
    }
  });

  const release = await acquireLock(codexHome, "sync", lockOptions({
    fsImpl,
    retryDelayMs: 25,
    sleepImpl: async (delayMs) => sleepCalls.push(delayMs)
  }));
  assert.equal(candidateAttempts, 3);
  assert.deepEqual(sleepCalls, [25, 25]);
  await release();
});

test("acquirePathLock supports an arbitrary future SQLite resource path", async (t) => {
  const root = await makeLockHome(t);
  const lockPath = path.join(root, "resource-locks", "state-db.lock");
  const release = await acquirePathLock(lockPath, "sqlite-resource", lockOptions());
  const owner = JSON.parse(await fs.readFile(path.join(lockPath, "owner.json"), "utf8"));
  assert.equal(owner.label, "sqlite-resource");
  assert.deepEqual(await fs.readdir(`${lockPath}.claims`), [`${owner.instanceId}.json`]);
  await release();
  await assert.rejects(fs.access(lockPath), { code: "ENOENT" });
});

test("acquireLock reads a live legacy .NET owner fail-closed", async (t) => {
  const codexHome = await makeLockHome(t);
  const lockDir = await writeCanonicalOwner(codexHome, {
    processId: process.pid,
    processStartedAt: TEST_STARTED_AT,
    startedAt: TEST_STARTED_AT,
    label: "dotnet",
    currentDirectory: codexHome
  });

  await assert.rejects(
    acquireLock(codexHome, "node", lockOptions()),
    /PID .* still the verified owner/
  );
  assert.equal(JSON.parse(await fs.readFile(path.join(lockDir, "owner.json"), "utf8")).processId, process.pid);
  assert.deepEqual(await fs.readdir(`${lockDir}.claims`), []);
});

test("acquireLock recognizes a live version 2 .NET owner from UTC-second identity", async (t) => {
  const codexHome = await makeLockHome(t);
  const lockDir = await writeCanonicalOwner(codexHome, {
    protocolVersion: 2,
    runtime: "dotnet",
    pid: process.pid,
    processId: process.pid,
    processStartedAt: "2024-01-02T03:04:05Z",
    instanceId: "dotnet-v2-live",
    startedAt: TEST_STARTED_AT,
    label: "dotnet",
    cwd: codexHome
  });

  await assert.rejects(acquireLock(codexHome, "node", lockOptions()), /still the verified owner/);
  assert.equal(
    JSON.parse(await fs.readFile(path.join(lockDir, "owner.json"), "utf8")).instanceId,
    "dotnet-v2-live"
  );
  assert.deepEqual(await fs.readdir(`${lockDir}.claims`), []);
});

test("same-second PID reuse is rejected by the exact Node start marker", async (t) => {
  const codexHome = await makeLockHome(t);
  const lockDir = await writeCanonicalOwner(codexHome, {
    protocolVersion: 2,
    runtime: "node",
    pid: process.pid,
    processId: process.pid,
    processStartMarker: "node:previous-generation",
    processStartedAt: TEST_STARTED_AT,
    instanceId: "previous-node-generation",
    startedAt: TEST_STARTED_AT,
    label: "node",
    cwd: codexHome
  });

  const release = await acquireLock(codexHome, "node", lockOptions());
  assert.notEqual(
    JSON.parse(await fs.readFile(path.join(lockDir, "owner.json"), "utf8")).instanceId,
    "previous-node-generation"
  );
  await release();
});

test("acquireLock reclaims a dead legacy .NET owner", async (t) => {
  const codexHome = await makeLockHome(t);
  const deadPid = 2_000_000_000;
  const lockDir = await writeCanonicalOwner(codexHome, {
    processId: deadPid,
    startedAt: "2020-01-01T00:00:01.000Z",
    label: "dotnet",
    currentDirectory: codexHome
  });

  const release = await acquireLock(codexHome, "node", lockOptions());
  const owner = JSON.parse(await fs.readFile(path.join(lockDir, "owner.json"), "utf8"));
  assert.equal(owner.protocolVersion, 2);
  assert.equal(owner.processId, process.pid);
  await release();
});

test("a crash after candidate preparation leaves only nonblocking residue", async (t) => {
  const codexHome = await makeLockHome(t);
  const moduleUrl = pathToFileURL(path.resolve("src/locking.js")).href;
  const script = `
    import { acquireLock } from ${JSON.stringify(moduleUrl)};
    await acquireLock(${JSON.stringify(codexHome)}, "crash", {
      getProcessIdentity: async () => ${JSON.stringify(TEST_MARKER)},
      getProcessStartedAtIdentity: async () => ${JSON.stringify(TEST_STARTED_AT)},
      onCandidateReady() { process.exit(29); }
    });
  `;
  const child = spawn(process.execPath, ["--input-type=module", "-e", script], {
    stdio: ["ignore", "pipe", "pipe"]
  });
  const result = await waitForChild(child);
  assert.equal(result.code, 29);

  const lockDir = path.join(codexHome, "tmp", DEFAULT_LOCK_NAME);
  await assert.rejects(fs.access(lockDir), { code: "ENOENT" });
  const residue = await fs.readdir(path.dirname(lockDir));
  assert.ok(residue.some((name) => name.includes(".candidate.")));
  assert.equal((await fs.readdir(`${lockDir}.claims`)).length, 1);

  const release = await acquireLock(codexHome, "recovery", lockOptions());
  await release();
  assert.deepEqual(await fs.readdir(`${lockDir}.claims`), []);
});

test("a unique live claim serializes stale reclaimers and prevents ABA", async (t) => {
  const codexHome = await makeLockHome(t);
  const deadPid = 2_000_000_000;
  await writeCanonicalOwner(codexHome, {
    protocolVersion: 2,
    pid: deadPid,
    processId: deadPid,
    processStartedAt: "2020-01-01T00:00:00.000Z",
    instanceId: "stale-owner",
    label: "stale",
    cwd: codexHome
  });

  let releaseReclaimer;
  const reclaimerPaused = new Promise((resolve) => {
    releaseReclaimer = resolve;
  });
  let reachedReclaim;
  const reclaimReached = new Promise((resolve) => {
    reachedReclaim = resolve;
  });
  const first = acquireLock(codexHome, "first", lockOptions({
    onBeforeStaleReclaim: async () => {
      reachedReclaim();
      await reclaimerPaused;
    }
  }));
  await reclaimReached;

  const contenders = await Promise.allSettled([
    acquireLock(codexHome, "second", lockOptions()),
    acquireLock(codexHome, "third", lockOptions())
  ]);
  assert.equal(contenders.filter((entry) => entry.status === "fulfilled").length, 0);
  assert.ok(contenders.every((entry) => /live claim|Lock already exists/.test(entry.reason.message)));

  releaseReclaimer();
  const release = await first;
  await release();
});

test("failed acquisition cleanup preserves a canonical generation replaced by an older runtime", async (t) => {
  const codexHome = await makeLockHome(t);
  const lockDir = path.join(codexHome, "tmp", DEFAULT_LOCK_NAME);
  const parentDir = path.dirname(lockDir);
  const replacementOwner = {
    protocolVersion: 2,
    runtime: "dotnet",
    pid: process.pid,
    processId: process.pid,
    processStartedAt: TEST_STARTED_AT,
    instanceId: "replacement-generation",
    startedAt: TEST_STARTED_AT,
    label: "older-runtime",
    cwd: codexHome
  };
  let replaced = false;

  await assert.rejects(
    acquireLock(codexHome, "node", lockOptions({
      async syncDirectoryImpl(directoryPath) {
        if (!replaced && path.resolve(directoryPath) === path.resolve(parentDir)) {
          replaced = true;
          await fs.writeFile(
            path.join(lockDir, "owner.json"),
            JSON.stringify(replacementOwner),
            "utf8"
          );
          throw new Error("injected post-publish durability failure");
        }
      }
    })),
    /cleanup was incomplete/
  );

  assert.deepEqual(
    JSON.parse(await fs.readFile(path.join(lockDir, "owner.json"), "utf8")),
    replacementOwner
  );
  assert.equal((await fs.readdir(`${lockDir}.claims`)).length, 1);
});

test("acquireLock fails closed while a legacy canonical owner is missing", async (t) => {
  const codexHome = await makeLockHome(t);
  const lockDir = path.join(codexHome, "tmp", DEFAULT_LOCK_NAME);
  await fs.mkdir(lockDir, { recursive: true });
  await assert.rejects(
    acquireLock(codexHome, "sync", lockOptions()),
    /owner\.json is not visible yet/
  );
  assert.deepEqual(await fs.readdir(`${lockDir}.claims`), []);
});

test("acquireLock fails closed for future protocols and conflicting cross-runtime PIDs", async (t) => {
  for (const fixture of [
    {
      name: "future protocol",
      owner: {
        protocolVersion: 99,
        pid: 2_000_000_000,
        processId: 2_000_000_000,
        processStartedAt: "2020-01-01T00:00:00.000Z",
        instanceId: "future-owner"
      },
      expected: /unsupported lock protocol 99/
    },
    {
      name: "PID conflict",
      owner: {
        protocolVersion: 2,
        pid: 2_000_000_000,
        processId: 1_999_999_999,
        processStartedAt: "2020-01-01T00:00:00.000Z",
        instanceId: "conflicting-owner"
      },
      expected: /conflicting pid and processId/
    }
  ]) {
    await t.test(fixture.name, async (subtest) => {
      const codexHome = await makeLockHome(subtest);
      const lockDir = await writeCanonicalOwner(codexHome, fixture.owner);
      await assert.rejects(acquireLock(codexHome, "sync", lockOptions()), fixture.expected);
      assert.deepEqual(await fs.readdir(`${lockDir}.claims`), []);
      assert.deepEqual(
        JSON.parse(await fs.readFile(path.join(lockDir, "owner.json"), "utf8")),
        fixture.owner
      );
    });
  }
});
