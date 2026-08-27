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
