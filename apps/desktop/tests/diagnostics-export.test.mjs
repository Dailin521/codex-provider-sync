import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { DesktopDiagnosticsExporter } from "../dist/main/diagnostics-export.js";

function readStoredEntries(archive) {
  const entries = new Map();
  let offset = 0;
  while (offset + 30 <= archive.length && archive.readUInt32LE(offset) === 0x04034b50) {
    const size = archive.readUInt32LE(offset + 18);
    const nameLength = archive.readUInt16LE(offset + 26);
    const extraLength = archive.readUInt16LE(offset + 28);
    const nameStart = offset + 30;
    const dataStart = nameStart + nameLength + extraLength;
    const name = archive.subarray(nameStart, nameStart + nameLength).toString("utf8");
    entries.set(name, archive.subarray(dataStart, dataStart + size));
    offset = dataStart + size;
  }
  return entries;
}

function diagnosticsSnapshot() {
  return {
    schemaVersion: 1,
    generatedAt: "2026-08-27T00:00:00.000Z",
    runtime: { node: "v24.0.0", platform: "win32", arch: "x64" },
    storage: { sqliteHomeSource: "default", stateDbFound: true, sqliteSupported: true },
    provider: {
      current: "openai",
      implicit: false,
      configured: ["openai"],
      rolloutCounts: { sessions: { openai: 1 }, archived_sessions: {} },
      sqliteCounts: { sessions: { openai: 1 }, archived_sessions: {} }
    },
    issues: {
      rootModelAvailable: true,
      rolloutModelFilesNeedingRepair: 0,
      sqliteModelRowsNeedingRepair: 0,
      cwdRowsNeedingRepair: 0,
      userEventRowsNeedingRepair: 0,
      workspaceRootsNeedingRepair: 0,
      encryptedContentFiles: 0
    },
    safety: {
      storageRevision: "safe-revision",
      pendingRecovery: false,
      pendingTransactions: [],
      operationInProgress: null,
      rolloutScanComplete: true,
      lockedRolloutCount: 0,
      projectThreadVisibilityAvailable: true
    }
  };
}

test("diagnostics exporter writes one valid fixed-entry redacted ZIP through a one-shot token", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-diagnostics-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const target = path.join(root, "diagnostics.zip");
  const exporter = new DesktopDiagnosticsExporter({
    appVersion: "1.0.0-test",
    isPackaged: false,
    now: () => new Date("2026-08-27T00:00:00.000Z")
  });
  const token = exporter.authorizeTarget(target);
  assert.equal(token.includes(root), false);
  const result = await exporter.export(token, diagnosticsSnapshot());
  assert.deepEqual(Object.keys(result).sort(), ["artifactId", "createdAt", "schemaVersion", "status"]);
  assert.equal(result.status, "created");
  assert.equal(JSON.stringify(result).includes(root), false);

  const archive = await fs.readFile(target);
  const entries = readStoredEntries(archive);
  assert.deepEqual([...entries.keys()], [
    "app-info.json",
    "status-summary.json",
    "storage-layout.json",
    "pending-transaction-summary.json",
    "recent-redacted-logs/README.txt"
  ]);
  const text = archive.toString("utf8");
  assert.equal(text.includes(root), false);
  assert.doesNotMatch(text, /auth\.json|encrypted_content|message body sentinel|state_5\.sqlite|rollout-.*\.jsonl/i);
  assert.equal((await exporter.export(token, diagnosticsSnapshot())).status, "failed");
});

test("diagnostics exporter rejects malformed snapshots and never accepts Renderer paths", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-diagnostics-invalid-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const exporter = new DesktopDiagnosticsExporter({ appVersion: "test", isPackaged: false });
  assert.throws(() => exporter.authorizeTarget("relative.zip"), /absolute path/);
  const target = path.join(root, "invalid.zip");
  const token = exporter.authorizeTarget(target);
  const malformed = diagnosticsSnapshot();
  malformed.storage = { ...malformed.storage, path: "C:\\secret" };
  assert.deepEqual(await exporter.export(token, malformed), {
    schemaVersion: 1,
    status: "failed",
    reason: "invalid-snapshot"
  });
  await assert.rejects(fs.access(target));
});

test("diagnostics capabilities are bounded, expire lazily, reserve targets and remain one-shot", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-diagnostics-capabilities-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  let now = new Date("2026-08-27T00:00:00.000Z");
  const exporter = new DesktopDiagnosticsExporter({
    appVersion: "test",
    isPackaged: false,
    now: () => now
  });
  const firstTarget = path.join(root, "first.zip");
  const first = exporter.authorizeTarget(firstTarget);
  assert.throws(() => exporter.authorizeTarget(firstTarget), /already reserved/);
  for (let index = 1; index < 32; index += 1) {
    exporter.authorizeTarget(path.join(root, `pending-${index}.zip`));
  }
  assert.throws(
    () => exporter.authorizeTarget(path.join(root, "overflow.zip")),
    /Too many pending/
  );

  now = new Date("2026-08-27T00:05:00.001Z");
  const replacement = exporter.authorizeTarget(firstTarget);
  assert.notEqual(replacement, first);
  assert.deepEqual(await exporter.export(first, diagnosticsSnapshot()), {
    schemaVersion: 1,
    status: "failed",
    reason: "write-failed"
  });

  const concurrent = await Promise.all([
    exporter.export(replacement, diagnosticsSnapshot()),
    exporter.export(replacement, diagnosticsSnapshot())
  ]);
  assert.equal(concurrent.filter((result) => result.status === "created").length, 1);
  assert.equal(concurrent.filter((result) => result.status === "failed").length, 1);
  assert.equal((await fs.stat(firstTarget)).isFile(), true);

  const afterCompletion = exporter.authorizeTarget(firstTarget);
  exporter.revoke(afterCompletion);
  const afterRevoke = exporter.authorizeTarget(firstTarget);
  exporter.revoke(afterRevoke);
});

test("diagnostics exporter serializes physical destinations reached through path aliases", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-diagnostics-alias-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const realParent = path.join(root, "real");
  const aliasParent = path.join(root, "alias");
  const secondAliasParent = path.join(root, "alias-two");
  await fs.mkdir(realParent);
  try {
    await fs.symlink(realParent, aliasParent, process.platform === "win32" ? "junction" : "dir");
    await fs.symlink(realParent, secondAliasParent, process.platform === "win32" ? "junction" : "dir");
  } catch (error) {
    t.skip(`path aliases are unavailable: ${error instanceof Error ? error.message : String(error)}`);
    return;
  }

  const exporter = new DesktopDiagnosticsExporter({ appVersion: "test", isPackaged: false });
  const direct = exporter.authorizeTarget(path.join(realParent, "diagnostics.zip"));
  const alias = exporter.authorizeTarget(path.join(aliasParent, "diagnostics.zip"));
  const secondAlias = exporter.authorizeTarget(path.join(secondAliasParent, "diagnostics.zip"));
  const results = await Promise.all([
    exporter.export(direct, diagnosticsSnapshot()),
    exporter.export(alias, diagnosticsSnapshot()),
    exporter.export(secondAlias, diagnosticsSnapshot())
  ]);
  assert.equal(results.filter((result) => result.status === "created").length, 1);
  assert.equal(results.filter((result) => result.status === "failed").length, 2);
  assert.equal((await fs.stat(path.join(realParent, "diagnostics.zip"))).isFile(), true);
});
