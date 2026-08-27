import { randomBytes, randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

import {
  assertCoreMethodOutput,
  type DiagnosticsSnapshot
} from "@codex-provider-sync/contracts";

import {
  DESKTOP_BUILD_ID,
  DESKTOP_CORE_VERSION
} from "../shared/constants.js";
import type { DesktopDiagnosticsExportResult } from "../shared/diagnostics-types.js";

interface TargetCapability {
  path: string;
  expiresAt: number;
}

interface ZipEntry {
  name: string;
  data: Buffer;
}

export interface DesktopDiagnosticsExporterOptions {
  appVersion: string;
  isPackaged: boolean;
  now?: () => Date;
}

function crc32(data: Buffer): number {
  let crc = 0xffffffff;
  for (const byte of data) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) {
      crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0);
    }
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function createStoredZip(entries: readonly ZipEntry[]): Buffer {
  const localParts: Buffer[] = [];
  const centralParts: Buffer[] = [];
  let offset = 0;
  for (const entry of entries) {
    const name = Buffer.from(entry.name, "utf8");
    const checksum = crc32(entry.data);
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0);
    local.writeUInt16LE(20, 4);
    local.writeUInt16LE(0x0800, 6);
    local.writeUInt16LE(0, 8);
    local.writeUInt32LE(0x00210000, 10);
    local.writeUInt32LE(checksum, 14);
    local.writeUInt32LE(entry.data.length, 18);
    local.writeUInt32LE(entry.data.length, 22);
    local.writeUInt16LE(name.length, 26);
    local.writeUInt16LE(0, 28);
    localParts.push(local, name, entry.data);

    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50, 0);
    central.writeUInt16LE(20, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt16LE(0x0800, 8);
    central.writeUInt16LE(0, 10);
    central.writeUInt32LE(0x00210000, 12);
    central.writeUInt32LE(checksum, 16);
    central.writeUInt32LE(entry.data.length, 20);
    central.writeUInt32LE(entry.data.length, 24);
    central.writeUInt16LE(name.length, 28);
    central.writeUInt16LE(0, 30);
    central.writeUInt16LE(0, 32);
    central.writeUInt16LE(0, 34);
    central.writeUInt16LE(0, 36);
    central.writeUInt32LE(0, 38);
    central.writeUInt32LE(offset, 42);
    centralParts.push(central, name);
    offset += local.length + name.length + entry.data.length;
  }
  const centralSize = centralParts.reduce((sum, part) => sum + part.length, 0);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(0, 4);
  end.writeUInt16LE(0, 6);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(centralSize, 12);
  end.writeUInt32LE(offset, 16);
  end.writeUInt16LE(0, 20);
  return Buffer.concat([...localParts, ...centralParts, end]);
}

function jsonEntry(name: string, value: unknown): ZipEntry {
  return { name, data: Buffer.from(`${JSON.stringify(value, null, 2)}\n`, "utf8") };
}

function normalizeTarget(value: string): string {
  if (typeof value !== "string" || value.includes("\0") || !path.isAbsolute(value)) {
    throw new TypeError("Diagnostics target must be an absolute path selected by Main.");
  }
  const target = path.resolve(value);
  if (path.extname(target).toLowerCase() !== ".zip") {
    throw new TypeError("Diagnostics target must use the .zip extension.");
  }
  return target;
}

export class DesktopDiagnosticsExporter {
  readonly #appVersion: string;
  readonly #isPackaged: boolean;
  readonly #now: () => Date;
  readonly #targets = new Map<string, TargetCapability>();

  constructor(options: DesktopDiagnosticsExporterOptions) {
    this.#appVersion = options.appVersion;
    this.#isPackaged = options.isPackaged;
    this.#now = options.now ?? (() => new Date());
  }

  authorizeTarget(targetPath: string): string {
    const token = randomBytes(32).toString("base64url");
    this.#targets.set(token, {
      path: normalizeTarget(targetPath),
      expiresAt: this.#now().getTime() + 5 * 60_000
    });
    return token;
  }

  async export(token: string, snapshot: DiagnosticsSnapshot): Promise<DesktopDiagnosticsExportResult> {
    const capability = this.#targets.get(token);
    this.#targets.delete(token);
    if (!capability || capability.expiresAt <= this.#now().getTime()) {
      return { schemaVersion: 1, status: "failed", reason: "write-failed" };
    }
    try {
      assertCoreMethodOutput("getDiagnostics", snapshot);
    } catch {
      return { schemaVersion: 1, status: "failed", reason: "invalid-snapshot" };
    }
    const createdAt = this.#now().toISOString();
    const entries: ZipEntry[] = [
      jsonEntry("app-info.json", {
        schemaVersion: 1,
        appVersion: this.#appVersion,
        coreVersion: DESKTOP_CORE_VERSION,
        buildId: DESKTOP_BUILD_ID,
        packaged: this.#isPackaged,
        platform: process.platform,
        arch: process.arch,
        generatedAt: createdAt
      }),
      jsonEntry("status-summary.json", {
        schemaVersion: 1,
        generatedAt: snapshot.generatedAt,
        provider: snapshot.provider,
        safety: {
          pendingRecovery: snapshot.safety.pendingRecovery,
          operationInProgress: snapshot.safety.operationInProgress,
          rolloutScanComplete: snapshot.safety.rolloutScanComplete,
          lockedRolloutCount: snapshot.safety.lockedRolloutCount,
          projectThreadVisibilityAvailable: snapshot.safety.projectThreadVisibilityAvailable
        }
      }),
      jsonEntry("storage-layout.json", {
        schemaVersion: 1,
        generatedAt: snapshot.generatedAt,
        storage: snapshot.storage
      }),
      jsonEntry("pending-transaction-summary.json", {
        schemaVersion: 1,
        generatedAt: snapshot.generatedAt,
        pendingTransactions: snapshot.safety.pendingTransactions
      }),
      {
        name: "recent-redacted-logs/README.txt",
        data: Buffer.from(
          "No persistent application logs were included. Credentials, message bodies, rollout files, and databases are excluded.\n",
          "utf8"
        )
      }
    ];
    const archive = createStoredZip(entries);
    let temporary: string | null = null;
    try {
      await fs.mkdir(path.dirname(capability.path), { recursive: true });
      const parent = await fs.realpath(path.dirname(capability.path));
      const destination = path.join(parent, path.basename(capability.path));
      temporary = `${destination}.tmp-${process.pid}-${randomUUID()}`;
      const handle = await fs.open(temporary, "wx", 0o600);
      try {
        await handle.writeFile(archive);
        await handle.sync();
      } finally {
        await handle.close();
      }
      await fs.rename(temporary, destination);
      await fs.chmod(destination, 0o600).catch(() => {});
      const directory = await fs.open(path.dirname(destination), "r").catch(() => null);
      if (directory) {
        try { await directory.sync(); } catch {} finally { await directory.close(); }
      }
      return {
        schemaVersion: 1,
        status: "created",
        artifactId: randomUUID(),
        createdAt
      };
    } catch {
      if (temporary) await fs.rm(temporary, { force: true }).catch(() => {});
      return { schemaVersion: 1, status: "failed", reason: "write-failed" };
    }
  }
}
