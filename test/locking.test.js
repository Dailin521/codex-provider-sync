import { spawn } from "node:child_process";
import { rmSync } from "node:fs";
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
const tempLockHomes = new Set();

process.once("exit", () => {
  for (const root of tempLockHomes) {
    try {
      rmSync(root, { recursive: true, force: true });
    } catch {
      // Best-effort cleanup after the test process has released its handles.
    }
  }
});

async function makeLockHome() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-lock-"));
  tempLockHomes.add(root);
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

  await assert.rejects(
    acquireLock(codexHome, "other", lockOptions()),
    (error) => error?.code === "OPERATION_BUSY"
      && error.details?.busyScope === "codex-home"
      && /live claim|Lock already exists/.test(error.message)
  );
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

test("lock storage permission failures are typed before ownership is published", async (t) => {
  const codexHome = await makeLockHome(t);
  const fsImpl = new Proxy(fs, {
    get(target, property) {
      if (property === "mkdir") {
        return async () => {
          const error = new Error("permission denied fixture");
          error.code = "EACCES";
          throw error;
        };
      }
      const value = target[property];
      return typeof value === "function" ? value.bind(target) : value;
    }
  });

  await assert.rejects(
    acquireLock(codexHome, "sync", lockOptions({ fsImpl })),
    (error) => error?.code === "PERMISSION_DENIED"
      && error.details?.lockScope === "codex-home"
      && error.details?.causeCode === "EACCES"
  );
});

test("owner publication failure removes its empty canonical reservation and claim", async (t) => {
  const codexHome = await makeLockHome(t);
  const lockDir = path.join(codexHome, "tmp", DEFAULT_LOCK_NAME);
  const fsImpl = new Proxy(fs, {
    get(target, property) {
      if (property === "link") {
        return async () => {
          const error = new Error("injected owner publication failure");
          error.code = "EIO";
          throw error;
        };
      }
      const value = target[property];
      return typeof value === "function" ? value.bind(target) : value;
    }
  });

  await assert.rejects(
    acquireLock(codexHome, "sync", lockOptions({ fsImpl })),
    /injected owner publication failure/
  );
  await assert.rejects(fs.access(lockDir), { code: "ENOENT" });
  assert.deepEqual(await fs.readdir(`${lockDir}.claims`), []);
  assert.equal(
    (await fs.readdir(path.dirname(lockDir))).some((name) => name.includes(".candidate.")),
    false
  );
});

test("link failure preserves foreign reservation contents but releases its own claim", async (t) => {
  const codexHome = await makeLockHome(t);
  const lockDir = path.join(codexHome, "tmp", DEFAULT_LOCK_NAME);
  const fsImpl = new Proxy(fs, {
    get(target, property) {
      if (property === "link") {
        return async () => {
          await fs.writeFile(path.join(lockDir, "foreign.txt"), "keep", "utf8");
          const error = new Error("injected link failure after foreign population");
          error.code = "EIO";
          throw error;
        };
      }
      const value = target[property];
      return typeof value === "function" ? value.bind(target) : value;
    }
  });

  await assert.rejects(
    acquireLock(codexHome, "sync", lockOptions({ fsImpl })),
    /link failure after foreign population/
  );
  assert.equal(await fs.readFile(path.join(lockDir, "foreign.txt"), "utf8"), "keep");
  assert.deepEqual(await fs.readdir(`${lockDir}.claims`), []);
});

test("reservation ABA is detected by directory identity and retains an uncertain claim", async (t) => {
  const codexHome = await makeLockHome(t);
  const lockDir = path.join(codexHome, "tmp", DEFAULT_LOCK_NAME);
  const displacedReservation = `${lockDir}.displaced`;
  let swapped = false;
  const fsImpl = new Proxy(fs, {
    get(target, property) {
      if (property === "link") {
        return async (source, destination) => {
          if (!swapped && path.resolve(destination) === path.resolve(path.join(lockDir, "owner.json"))) {
            swapped = true;
            await fs.rename(lockDir, displacedReservation);
            await fs.mkdir(lockDir);
            await fs.writeFile(path.join(lockDir, "foreign.txt"), "keep", "utf8");
          }
          return target.link(source, destination);
        };
      }
      const value = target[property];
      return typeof value === "function" ? value.bind(target) : value;
    }
  });

  await assert.rejects(
    acquireLock(codexHome, "sync", lockOptions({ fsImpl })),
    /reservation changed identity/
  );
  assert.equal(await fs.readFile(path.join(lockDir, "foreign.txt"), "utf8"), "keep");
  assert.equal((await fs.readdir(`${lockDir}.claims`)).length, 1);
  assert.equal((await fs.lstat(displacedReservation)).isDirectory(), true);
});

test("link failure does not remove a swapped empty reservation and still releases its claim", async (t) => {
  const codexHome = await makeLockHome(t);
  const lockDir = path.join(codexHome, "tmp", DEFAULT_LOCK_NAME);
  const displacedReservation = `${lockDir}.displaced-link-failure`;
  const fsImpl = new Proxy(fs, {
    get(target, property) {
      if (property === "link") {
        return async () => {
          await fs.rename(lockDir, displacedReservation);
          await fs.mkdir(lockDir);
          const error = new Error("injected link failure after reservation swap");
          error.code = "EIO";
          throw error;
        };
      }
      const value = target[property];
      return typeof value === "function" ? value.bind(target) : value;
    }
  });

  await assert.rejects(
    acquireLock(codexHome, "sync", lockOptions({ fsImpl })),
    (error) => error?.code === "LOCK_UNVERIFIABLE"
      && error.details?.lockScope === "codex-home"
      && /cleanup was incomplete/.test(error.message)
  );
  assert.equal((await fs.lstat(lockDir)).isDirectory(), true);
  assert.equal((await fs.lstat(displacedReservation)).isDirectory(), true);
  assert.deepEqual(await fs.readdir(`${lockDir}.claims`), []);
});

test("acquirePathLock supports an arbitrary future SQLite resource path", async (t) => {
  const root = await makeLockHome(t);
  const lockPath = path.join(root, "resource-locks", "state-db.lock");
  const resourceOptions = lockOptions({ scope: "state-db", resourceKey: "a".repeat(64) });
  const release = await acquirePathLock(lockPath, "sqlite-resource", resourceOptions);
  const owner = JSON.parse(await fs.readFile(path.join(lockPath, "owner.json"), "utf8"));
  assert.equal(owner.label, "sqlite-resource");
  assert.equal(owner.scope, "state-db");
  assert.equal(owner.resourceKey, "a".repeat(64));
  assert.deepEqual(await fs.readdir(`${lockPath}.claims`), [`${owner.instanceId}.json`]);
  await assert.rejects(
    acquirePathLock(lockPath, "sqlite-resource-2", resourceOptions),
    (error) => error?.code === "OPERATION_BUSY"
      && error.details?.busyScope === "state-db"
  );
  await release();
  await assert.rejects(fs.access(lockPath), { code: "ENOENT" });
});

test("acquirePathLock rejects a canonical file with an explicit diagnostic", async (t) => {
  const root = await makeLockHome(t);
  const lockPath = path.join(root, "resource-locks", "state-db.lock");
  await fs.mkdir(path.dirname(lockPath), { recursive: true });
  await fs.writeFile(lockPath, "foreign", "utf8");

  await assert.rejects(
    acquirePathLock(lockPath, "sqlite-resource", lockOptions({
      scope: "state-db",
      resourceKey: "b".repeat(64)
    })),
    (error) => error?.code === "LOCK_UNVERIFIABLE"
      && error.details?.lockScope === "state-db"
      && /canonical lock path is not a directory/.test(error.message)
  );
  assert.equal(await fs.readFile(lockPath, "utf8"), "foreign");
  assert.deepEqual(await fs.readdir(`${lockPath}.claims`), []);
});

test("acquirePathLock rejects a canonical symlink without following it", async (t) => {
  const root = await makeLockHome(t);
  const lockPath = path.join(root, "resource-locks", "state-db.lock");
  const targetPath = path.join(root, "foreign-target");
  await fs.mkdir(path.dirname(lockPath), { recursive: true });
  await fs.mkdir(targetPath);
  try {
    await fs.symlink(targetPath, lockPath, process.platform === "win32" ? "junction" : "dir");
  } catch (error) {
    if (error?.code === "EPERM") {
      t.skip("The test account cannot create a directory symlink/junction.");
      return;
    }
    throw error;
  }

  await assert.rejects(
    acquirePathLock(lockPath, "sqlite-resource", lockOptions()),
    /canonical lock path is a symbolic link/
  );
  assert.equal((await fs.lstat(lockPath)).isSymbolicLink(), true);
  assert.deepEqual(await fs.readdir(targetPath), []);
  assert.deepEqual(await fs.readdir(`${lockPath}.claims`), []);
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

test("cross-runtime start-time uncertainty is LOCK_UNVERIFIABLE, not OPERATION_BUSY", async (t) => {
  const codexHome = await makeLockHome(t);
  let startTimeProbeCount = 0;
  const lockDir = await writeCanonicalOwner(codexHome, {
    protocolVersion: 2,
    runtime: "dotnet",
    pid: process.pid,
    processId: process.pid,
    processStartedAt: TEST_STARTED_AT,
    instanceId: "dotnet-v2-unverifiable",
    startedAt: TEST_STARTED_AT,
    label: "dotnet",
    cwd: codexHome
  });

  await assert.rejects(
    acquireLock(codexHome, "node", lockOptions({
      getProcessStartedAtIdentity: async () => {
        startTimeProbeCount += 1;
        if (startTimeProbeCount === 1) {
          return TEST_STARTED_AT;
        }
        throw new Error("injected cross-runtime identity failure");
      }
    })),
    (error) => error?.code === "LOCK_UNVERIFIABLE"
      && error.details?.lockScope === "codex-home"
      && /could not be verified/.test(error.message)
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

test("stale canonical reclamation is bounded under continuous replacement", async (t) => {
  const codexHome = await makeLockHome(t);
  const lockDir = await writeCanonicalOwner(codexHome, {
    processId: 2_000_000_000,
    startedAt: "2020-01-01T00:00:00.000Z",
    label: "stale-0",
    currentDirectory: codexHome
  });
  let replacements = 0;
  const fsImpl = new Proxy(fs, {
    get(target, property) {
      if (property === "rename") {
        return async (source, destination) => {
          await target.rename(source, destination);
          if (path.resolve(source) === path.resolve(lockDir)
              && path.basename(destination).includes(".stale.")) {
            replacements += 1;
            await fs.mkdir(lockDir);
            await fs.writeFile(path.join(lockDir, "owner.json"), JSON.stringify({
              processId: 2_000_000_000,
              startedAt: "2020-01-01T00:00:00.000Z",
              label: `stale-${replacements}`,
              currentDirectory: codexHome
            }), "utf8");
          }
        };
      }
      const value = target[property];
      return typeof value === "function" ? value.bind(target) : value;
    }
  });

  await assert.rejects(
    acquireLock(codexHome, "bounded", lockOptions({
      fsImpl,
      staleReclaimAttemptLimit: 2
    })),
    /bounded limit of 2 attempts/
  );
  assert.equal(replacements, 2);
  assert.equal(JSON.parse(await fs.readFile(path.join(lockDir, "owner.json"), "utf8")).label, "stale-2");
  assert.deepEqual(await fs.readdir(`${lockDir}.claims`), []);
});

test("stale reclaim restores a changed owner without rename-overwrite", async (t) => {
  const codexHome = await makeLockHome(t);
  const lockDir = await writeCanonicalOwner(codexHome, {
    processId: 2_000_000_000,
    startedAt: "2020-01-01T00:00:00.000Z",
    label: "stale",
    currentDirectory: codexHome
  });
  const releasedOldPath = `${lockDir}.released-old`;
  const replacementOwner = {
    protocolVersion: 2,
    runtime: "dotnet",
    pid: process.pid,
    processId: process.pid,
    processStartedAt: TEST_STARTED_AT,
    instanceId: "replacement-generation",
    startedAt: TEST_STARTED_AT,
    label: "replacement",
    cwd: codexHome
  };

  await assert.rejects(
    acquireLock(codexHome, "aba-contender", lockOptions({
      async onBeforeStaleReclaim() {
        await fs.rename(lockDir, releasedOldPath);
        await fs.mkdir(lockDir);
        await fs.writeFile(
          path.join(lockDir, "owner.json"),
          JSON.stringify(replacementOwner),
          "utf8"
        );
      }
    })),
    /restored without replacing another directory/
  );
  assert.deepEqual(
    JSON.parse(await fs.readFile(path.join(lockDir, "owner.json"), "utf8")),
    replacementOwner
  );
  assert.equal((await fs.lstat(releasedOldPath)).isDirectory(), true);
  assert.ok((await fs.readdir(path.dirname(lockDir))).some((name) => name.includes(".stale.")));
  assert.deepEqual(await fs.readdir(`${lockDir}.claims`), []);
});

test("release refuses a replacement directory that reuses the same owner inode", async (t) => {
  const codexHome = await makeLockHome(t);
  const lockDir = path.join(codexHome, "tmp", DEFAULT_LOCK_NAME);
  const release = await acquireLock(codexHome, "original", lockOptions());
  const originalPath = `${lockDir}.original`;
  await fs.rename(lockDir, originalPath);
  await fs.mkdir(lockDir);
  await fs.link(path.join(originalPath, "owner.json"), path.join(lockDir, "owner.json"));
  await fs.writeFile(path.join(lockDir, "foreign.txt"), "keep", "utf8");

  await assert.rejects(release(), /directory identity changed/);
  assert.equal(await fs.readFile(path.join(lockDir, "foreign.txt"), "utf8"), "keep");
  assert.equal((await fs.readdir(`${lockDir}.claims`)).length, 1);
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
    (error) => error?.code === "LOCK_UNVERIFIABLE"
      && error.details?.lockScope === "codex-home"
      && /owner\.json is not visible yet/.test(error.message)
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
      await assert.rejects(
        acquireLock(codexHome, "sync", lockOptions()),
        (error) => error?.code === "LOCK_UNVERIFIABLE"
          && error.details?.lockScope === "codex-home"
          && fixture.expected.test(error.message)
      );
      assert.deepEqual(await fs.readdir(`${lockDir}.claims`), []);
      assert.deepEqual(
        JSON.parse(await fs.readFile(path.join(lockDir, "owner.json"), "utf8")),
        fixture.owner
      );
    });
  }
});
