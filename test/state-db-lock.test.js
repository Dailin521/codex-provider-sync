import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";

import {
  acquireStateDbLock,
  resolveStateDbLockResource
} from "../src/state-db-lock.js";

test("State DB resource identity uses the real parent, NUL delimiter, and SHA-256 lock path", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-state-lock-"));
  const sqliteHome = path.join(root, "sqlite");
  const stateDbPath = path.join(sqliteHome, "state_5.sqlite");
  try {
    await fs.mkdir(sqliteHome, { recursive: true });
    await fs.writeFile(stateDbPath, "fixture", "utf8");
    const resource = await resolveStateDbLockResource(stateDbPath);
    const realParent = await fs.realpath(sqliteHome);
    const expectedIdentity = `${process.platform === "win32" ? realParent.toLowerCase() : realParent}\0state_5.sqlite`;
    const expectedKey = createHash("sha256").update(expectedIdentity, "utf8").digest("hex");
    assert.equal(resource.identity, expectedIdentity);
    assert.equal(resource.resourceKey, expectedKey);
    assert.equal(
      resource.lockPath,
      path.join(await fs.realpath(sqliteHome), ".codex-provider-sync", "locks", `${expectedKey}.lock`)
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("State DB locks publish scope/resourceKey and report state-db contention", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-state-lock-"));
  const sqliteHome = path.join(root, "sqlite");
  const stateDbPath = path.join(sqliteHome, "state_5.sqlite");
  try {
    await fs.mkdir(sqliteHome, { recursive: true });
    await fs.writeFile(stateDbPath, "fixture", "utf8");
    const first = await acquireStateDbLock(stateDbPath, "fixture-one");
    try {
      const owner = JSON.parse(await fs.readFile(path.join(first.resource.lockPath, "owner.json"), "utf8"));
      assert.equal(owner.scope, "state-db");
      assert.equal(owner.resourceKey, first.resource.resourceKey);
      await assert.rejects(
        acquireStateDbLock(stateDbPath, "fixture-two"),
        (error) => error?.code === "OPERATION_BUSY" && error?.details?.busyScope === "state-db"
      );
    } finally {
      await first.release();
    }
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("State DB resource identity supports a missing database only when its parent is verifiable", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-state-lock-"));
  try {
    const sqliteHome = path.join(root, "sqlite");
    await fs.mkdir(sqliteHome);
    const resource = await resolveStateDbLockResource(path.join(sqliteHome, "state_5.sqlite"));
    assert.match(resource.resourceKey, /^[a-f0-9]{64}$/);

    await assert.rejects(
      resolveStateDbLockResource(path.join(root, "missing", "state_5.sqlite")),
      (error) => error?.code === "LOCK_UNVERIFIABLE"
        && error?.details?.lockScope === "state-db"
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("a real Node process publishes a State DB lock that blocks another process", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-state-lock-process-"));
  const sqliteHome = path.join(root, "sqlite");
  const stateDbPath = path.join(sqliteHome, "state_5.sqlite");
  let child;
  let childExit;
  try {
    await fs.mkdir(sqliteHome, { recursive: true });
    await fs.writeFile(stateDbPath, "fixture", "utf8");
    const moduleUrl = pathToFileURL(path.resolve("src/state-db-lock.js")).href;
    const script = `
      import { acquireStateDbLock } from ${JSON.stringify(moduleUrl)};
      const held = await acquireStateDbLock(${JSON.stringify(stateDbPath)}, "child-winner");
      console.log(JSON.stringify({ ready: true, resourceKey: held.resource.resourceKey }));
      await new Promise((resolve) => process.stdin.once("data", resolve));
      await held.release();
    `;
    child = spawn(process.execPath, ["--input-type=module", "-e", script], {
      cwd: path.resolve("."),
      stdio: ["pipe", "pipe", "pipe"]
    });
    childExit = new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", resolve);
    });
    const lines = createInterface({ input: child.stdout });
    const readyLine = await new Promise((resolve, reject) => {
      const timeout = setTimeout(
        () => reject(new Error("child State DB lock did not become ready")),
        15000
      );
      lines.once("line", (line) => {
        clearTimeout(timeout);
        resolve(line);
      });
    });
    const ready = JSON.parse(readyLine);
    const resource = await resolveStateDbLockResource(stateDbPath);
    assert.equal(ready.resourceKey, resource.resourceKey);
    await assert.rejects(
      acquireStateDbLock(stateDbPath, "parent-contender"),
      (error) => error?.code === "OPERATION_BUSY" && error?.details?.busyScope === "state-db"
    );
    lines.close();
    child.stdin.end("release\n");
    const exitCode = await childExit;
    assert.equal(exitCode, 0);
    child = null;
  } finally {
    if (child) {
      child.stdin.end("release\n");
      await childExit;
    }
    await fs.rm(root, { recursive: true, force: true });
  }
});
