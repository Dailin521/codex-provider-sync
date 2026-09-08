import { randomBytes } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

import type { DesktopProfileDirectoryKind } from "../shared/profile-types.js";

interface DirectoryCapability {
  kind: DesktopProfileDirectoryKind;
  path: string;
  expiresAt: number;
}

const TOKEN_TTL_MS = 5 * 60_000;
const MAX_TOKENS = 32;

export class DirectorySelectionService {
  readonly #tokens = new Map<string, DirectoryCapability>();

  async authorize(kind: DesktopProfileDirectoryKind, selectedPath: string): Promise<{
    token: string;
    displayName: string;
  }> {
    this.#sweep();
    if (this.#tokens.size >= MAX_TOKENS) throw new Error("Too many pending directory selections.");
    if (typeof selectedPath !== "string" || selectedPath.includes("\0") || !path.isAbsolute(selectedPath)) {
      throw new TypeError("The selected directory is invalid.");
    }
    const stat = await fs.stat(selectedPath);
    if (!stat.isDirectory()) throw new TypeError("The selected path is not a directory.");
    const trustedPath = await fs.realpath(selectedPath);
    const token = randomBytes(32).toString("base64url");
    this.#tokens.set(token, { kind, path: trustedPath, expiresAt: Date.now() + TOKEN_TTL_MS });
    return { token, displayName: path.basename(trustedPath) || trustedPath };
  }

  consume(token: string, kind: DesktopProfileDirectoryKind): string {
    this.#sweep();
    const capability = this.#tokens.get(token);
    this.#tokens.delete(token);
    if (!capability || capability.kind !== kind) {
      throw new TypeError("The directory selection expired or was already used.");
    }
    return capability.path;
  }

  #sweep(): void {
    const now = Date.now();
    for (const [token, capability] of this.#tokens) {
      if (capability.expiresAt <= now) this.#tokens.delete(token);
    }
  }
}
