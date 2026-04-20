import fs from "node:fs/promises";
import path from "node:path";

import { DEFAULT_LOCK_NAME } from "./constants.js";

const DEFAULT_LOCK_CREATE_RETRY_COUNT = 3;
const DEFAULT_LOCK_CREATE_RETRY_DELAY_MS = 75;

function isTransientLockCreateError(error) {
  return error?.code === "EPERM";
}

async function sleep(delayMs) {
  await new Promise((resolve) => setTimeout(resolve, delayMs));
}

async function createLockDirectory(lockDir, {
  fsImpl,
  retryCount,
  retryDelayMs,
  sleepImpl
}) {
  let attempts = 0;
  while (true) {
    try {
      await fsImpl.mkdir(lockDir);
      return;
    } catch (error) {
      if (error && error.code === "EEXIST") {
        throw new Error(`Lock already exists at ${lockDir}. Close Codex/App and retry, or remove the stale lock if you are sure no sync is running.`);
      }

      // Windows can briefly surface EPERM after a previous run releases the lock directory.
      if (!isTransientLockCreateError(error) || attempts >= retryCount) {
        throw error;
      }

      attempts += 1;
      await sleepImpl(retryDelayMs);
    }
  }
}

export async function acquireLock(codexHome, label = "codex-provider-sync", options = {}) {
  const {
    fsImpl = fs,
    retryCount = DEFAULT_LOCK_CREATE_RETRY_COUNT,
    retryDelayMs = DEFAULT_LOCK_CREATE_RETRY_DELAY_MS,
    sleepImpl = sleep
  } = options;
  const lockDir = path.join(codexHome, "tmp", DEFAULT_LOCK_NAME);
  await fsImpl.mkdir(path.dirname(lockDir), { recursive: true });
  await createLockDirectory(lockDir, {
    fsImpl,
    retryCount,
    retryDelayMs,
    sleepImpl
  });

  const ownerPath = path.join(lockDir, "owner.json");
  const owner = {
    pid: process.pid,
    startedAt: new Date().toISOString(),
    label,
    cwd: process.cwd()
  };
  await fsImpl.writeFile(ownerPath, JSON.stringify(owner, null, 2), "utf8");

  let released = false;
  return async function releaseLock() {
    if (released) {
      return;
    }
    released = true;
    await fsImpl.rm(lockDir, { recursive: true, force: true });
  };
}
