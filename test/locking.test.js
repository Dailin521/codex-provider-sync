import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";

import { acquireLock } from "../src/locking.js";
import { DEFAULT_LOCK_NAME } from "../src/constants.js";

test("acquireLock retries transient EPERM when creating the lock directory", async () => {
  const codexHome = "C:\\CodexHome";
  const lockDir = path.join(codexHome, "tmp", DEFAULT_LOCK_NAME);
  const calls = [];
  const sleepCalls = [];
  let lockMkdirAttempts = 0;

  const fsImpl = {
    async mkdir(targetPath, options) {
      calls.push({ fn: "mkdir", targetPath, options });
      if (targetPath === lockDir) {
        lockMkdirAttempts += 1;
        if (lockMkdirAttempts < 3) {
          const error = new Error("operation not permitted");
          error.code = "EPERM";
          throw error;
        }
      }
    },
    async writeFile(targetPath, content, encoding) {
      calls.push({ fn: "writeFile", targetPath, content, encoding });
    },
    async rm(targetPath, options) {
      calls.push({ fn: "rm", targetPath, options });
    }
  };

  const releaseLock = await acquireLock(codexHome, "sync", {
    fsImpl,
    retryCount: 3,
    retryDelayMs: 25,
    sleepImpl: async (delayMs) => {
      sleepCalls.push(delayMs);
    }
  });

  assert.equal(lockMkdirAttempts, 3);
  assert.deepEqual(sleepCalls, [25, 25]);
  assert.equal(calls.filter((call) => call.fn === "writeFile").length, 1);

  await releaseLock();
  assert.equal(calls.at(-1).fn, "rm");
  assert.equal(calls.at(-1).targetPath, lockDir);
});

test("acquireLock does not retry when the lock directory already exists", async () => {
  const codexHome = "C:\\CodexHome";
  const lockDir = path.join(codexHome, "tmp", DEFAULT_LOCK_NAME);
  let lockMkdirAttempts = 0;
  const sleepCalls = [];

  const fsImpl = {
    async mkdir(targetPath) {
      if (targetPath === lockDir) {
        lockMkdirAttempts += 1;
        const error = new Error("already exists");
        error.code = "EEXIST";
        throw error;
      }
    },
    async writeFile() {
      throw new Error("writeFile should not be called");
    },
    async rm() {
      throw new Error("rm should not be called");
    }
  };

  await assert.rejects(
    () => acquireLock(codexHome, "sync", {
      fsImpl,
      retryCount: 3,
      retryDelayMs: 25,
      sleepImpl: async (delayMs) => {
        sleepCalls.push(delayMs);
      }
    }),
    /Lock already exists/
  );

  assert.equal(lockMkdirAttempts, 1);
  assert.deepEqual(sleepCalls, []);
});
