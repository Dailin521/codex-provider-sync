import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { getBackupSummary, listBackups, refreshBackupInventory } from "../src/backup.js";
import { defaultBackupRoot } from "../src/constants.js";

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

function samePath(value, expected) {
  return typeof value === "string" && path.resolve(value) === path.resolve(expected);
}

async function makeFixture() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-backup-read-race-"));
  const codexHome = path.join(root, ".codex");
  await fs.mkdir(codexHome, { recursive: true });
  return { root, codexHome };
}

async function writeManagedBackup(codexHome, id, {
  cachedInventory = false,
  cachedInventorySize = 7,
  nestedPayload = false
} = {}) {
  const backupDir = path.join(defaultBackupRoot(codexHome), id);
  const childDir = nestedPayload ? path.join(backupDir, "nested") : null;
  const payloadPath = path.join(childDir ?? backupDir, "payload.bin");
  await fs.mkdir(childDir ?? backupDir, { recursive: true });
  await fs.writeFile(payloadPath, "payload", "utf8");
  const metadata = {
    namespace: "provider-sync",
    version: 2,
    codexHome,
    ...(cachedInventory ? { sizeBytes: cachedInventorySize, fileCount: 1 } : {})
  };
  const metadataPath = path.join(backupDir, "metadata.json");
  await fs.writeFile(metadataPath, JSON.stringify(metadata), "utf8");
  return { backupDir, childDir, metadataPath, payloadPath };
}

async function withPatchedFsMethod(methodName, replacement, run) {
  const original = fs[methodName];
  fs[methodName] = replacement(original);
  try {
    return await run();
  } finally {
    fs[methodName] = original;
  }
}

function assertEmptyRead(result) {
  if (Object.hasOwn(result, "backups")) {
    assert.deepEqual(result.backups, []);
  } else {
    assert.deepEqual(result, { count: 0, totalBytes: 0 });
  }
}

for (const [name, read] of [
  ["getBackupSummary", getBackupSummary],
  ["listBackups", listBackups]
]) {
  test(`${name} skips a backup removed after directory enumeration`, async () => {
    const { root, codexHome } = await makeFixture();
    const { backupDir } = await writeManagedBackup(codexHome, "20260909T000000000Z");
    const backupRoot = defaultBackupRoot(codexHome);
    const entered = deferred();
    const release = deferred();
    try {
      const resultPromise = withPatchedFsMethod("readdir", (original) => async (...args) => {
        const result = await original(...args);
        if (samePath(args[0], backupRoot)) {
          entered.resolve();
          await release.promise;
        }
        return result;
      }, () => read(codexHome));
      await entered.promise;
      await fs.rm(backupDir, { recursive: true, force: true });
      release.resolve();
      assertEmptyRead(await resultPromise);
    } finally {
      release.resolve();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  test(`${name} skips a whole backup removed between metadata reads`, async () => {
    const { root, codexHome } = await makeFixture();
    const { backupDir, metadataPath } = await writeManagedBackup(codexHome, "20260909T000001000Z", { cachedInventory: true });
    const entered = deferred();
    const release = deferred();
    let metadataReads = 0;
    try {
      const resultPromise = withPatchedFsMethod("readFile", (original) => async (...args) => {
        if (samePath(args[0], metadataPath) && ++metadataReads === 2) {
          entered.resolve();
          await release.promise;
        }
        return original(...args);
      }, () => read(codexHome));
      await entered.promise;
      await fs.rm(backupDir, { recursive: true, force: true });
      release.resolve();
      assertEmptyRead(await resultPromise);
    } finally {
      release.resolve();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  test(`${name} skips a backup when prune removed its payload before metadata`, async () => {
    const { root, codexHome } = await makeFixture();
    const { backupDir, payloadPath } = await writeManagedBackup(codexHome, "20260909T000002000Z");
    const entered = deferred();
    const release = deferred();
    try {
      const resultPromise = withPatchedFsMethod("stat", (original) => async (...args) => {
        if (samePath(args[0], payloadPath)) {
          entered.resolve();
          await release.promise;
        }
        return original(...args);
      }, () => read(codexHome));
      await entered.promise;
      await fs.rm(payloadPath, { force: true });
      release.resolve();
      assertEmptyRead(await resultPromise);
    } finally {
      release.resolve();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  test(`${name} skips a backup when a child directory disappears during recursive readdir`, async () => {
    const { root, codexHome } = await makeFixture();
    const { childDir } = await writeManagedBackup(codexHome, "20260909T000002500Z", { nestedPayload: true });
    const entered = deferred();
    const release = deferred();
    try {
      const resultPromise = withPatchedFsMethod("readdir", (original) => async (...args) => {
        if (samePath(args[0], childDir)) {
          entered.resolve();
          await release.promise;
        }
        return original(...args);
      }, () => read(codexHome));
      await entered.promise;
      await fs.rm(childDir, { recursive: true, force: true });
      release.resolve();
      assertEmptyRead(await resultPromise);
    } finally {
      release.resolve();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  test(`${name} preserves a complete survivor while omitting the raced backup`, async () => {
    const { root, codexHome } = await makeFixture();
    const { payloadPath } = await writeManagedBackup(codexHome, "20260909T000003000Z");
    const survivor = await writeManagedBackup(codexHome, "20260909T000004000Z", {
      cachedInventory: true,
      cachedInventorySize: 19
    });
    const entered = deferred();
    const release = deferred();
    try {
      const resultPromise = withPatchedFsMethod("stat", (original) => async (...args) => {
        if (samePath(args[0], payloadPath)) {
          entered.resolve();
          await release.promise;
        }
        return original(...args);
      }, () => read(codexHome));
      await entered.promise;
      await fs.rm(payloadPath, { force: true });
      release.resolve();
      const result = await resultPromise;
      if (Object.hasOwn(result, "backups")) {
        assert.equal(result.backups.length, 1);
        assert.equal(result.backups[0].id, path.basename(survivor.backupDir));
        assert.equal(
          result.backups[0].sizeBytes,
          (await fs.stat(survivor.metadataPath)).size + (await fs.stat(survivor.payloadPath)).size
        );
      } else {
        assert.deepEqual(result, { count: 1, totalBytes: 19 });
      }
    } finally {
      release.resolve();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  test(`${name} still fails for access or invalid metadata after discovery`, async () => {
    const { root, codexHome } = await makeFixture();
    const { metadataPath } = await writeManagedBackup(codexHome, "20260909T000003000Z", { cachedInventory: true });
    let metadataReads = 0;
    try {
      await withPatchedFsMethod("readFile", (original) => async (...args) => {
        if (samePath(args[0], metadataPath) && ++metadataReads === 2) {
          const error = new Error("access denied fixture");
          error.code = "EACCES";
          throw error;
        }
        return original(...args);
      }, async () => {
        await assert.rejects(() => read(codexHome), (error) => error?.code === "EACCES");
      });

      metadataReads = 0;
      await withPatchedFsMethod("readFile", (original) => async (...args) => {
        if (samePath(args[0], metadataPath) && ++metadataReads === 2) return "{";
        return original(...args);
      }, async () => {
        await assert.rejects(() => read(codexHome), SyntaxError);
      });
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
}

test("refreshBackupInventory keeps write inventory ENOENT-strict", async () => {
  const { root, codexHome } = await makeFixture();
  const { backupDir, payloadPath } = await writeManagedBackup(codexHome, "20260909T000004000Z");
  const entered = deferred();
  const release = deferred();
  try {
    const refresh = withPatchedFsMethod("stat", (original) => async (...args) => {
      if (samePath(args[0], payloadPath)) {
        entered.resolve();
        await release.promise;
      }
      return original(...args);
    }, () => refreshBackupInventory(backupDir));
    await entered.promise;
    await fs.rm(payloadPath, { force: true });
    release.resolve();
    await assert.rejects(refresh, (error) => error?.code === "ENOENT");
  } finally {
    release.resolve();
    await fs.rm(root, { recursive: true, force: true });
  }
});
