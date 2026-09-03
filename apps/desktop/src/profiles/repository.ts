import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

import type { ProfileSelector } from "@codex-provider-sync/contracts";

import type { DesktopProfileSummary, TrustedDesktopProfile } from "../shared/profile-types.js";

interface StoredProfile {
  id: string;
  name: string;
  codexHome: string;
  sqliteHome?: string;
}

interface StoredProfileDocument {
  schemaVersion: 1;
  profiles: StoredProfile[];
}

export interface DesktopProfileRepositoryOptions {
  filePath: string;
  defaultCodexHome: string;
  defaultSqliteHome?: string;
}

function profileError(code: "INVALID_INPUT" | "PROFILE_CHANGED", message: string): Error {
  return Object.assign(new Error(message), { code });
}

function normalizeOptionalPath(value: unknown): string | undefined {
  if (value === undefined || value === null || String(value).trim() === "") return undefined;
  if (typeof value !== "string" || value.includes("\0") || !path.isAbsolute(value.trim())) {
    throw profileError("INVALID_INPUT", "Invalid trusted SQLite Home.");
  }
  return path.resolve(value.trim());
}

function normalizeProfile(value: unknown): StoredProfile {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw profileError("INVALID_INPUT", "Invalid trusted desktop profile.");
  }
  const source = value as Record<string, unknown>;
  const allowed = new Set(["id", "name", "codexHome", "sqliteHome"]);
  if (Object.keys(source).some((key) => !allowed.has(key))
      || typeof source.id !== "string"
      || !/^[A-Za-z0-9._-]{1,80}$/.test(source.id)
      || typeof source.name !== "string"
      || source.name.trim().length < 1
      || source.name.trim().length > 120
      || typeof source.codexHome !== "string"
      || source.codexHome.includes("\0")
      || !path.isAbsolute(source.codexHome.trim())) {
    throw profileError("INVALID_INPUT", "Invalid trusted desktop profile.");
  }
  const sqliteHome = normalizeOptionalPath(source.sqliteHome);
  return {
    id: source.id,
    name: source.name.trim(),
    codexHome: path.resolve(source.codexHome.trim()),
    ...(sqliteHome ? { sqliteHome } : {})
  };
}

function revisionOf(profile: StoredProfile): string {
  return createHash("sha256").update(JSON.stringify(profile), "utf8").digest("base64url");
}

function trustedProfile(profile: StoredProfile): TrustedDesktopProfile {
  return { ...profile, revision: revisionOf(profile) };
}

export class DesktopProfileRepository {
  readonly #filePath: string;
  readonly #defaultProfile: StoredProfile;
  #profiles: StoredProfile[];
  #initialized = false;

  constructor(options: DesktopProfileRepositoryOptions) {
    this.#filePath = path.resolve(options.filePath);
    this.#defaultProfile = normalizeProfile({
      id: "default",
      name: "Default",
      codexHome: options.defaultCodexHome,
      ...(options.defaultSqliteHome ? { sqliteHome: options.defaultSqliteHome } : {})
    });
    this.#profiles = [this.#defaultProfile];
  }

  async initialize(): Promise<void> {
    let namedProfiles: StoredProfile[] = [];
    try {
      const parsed: unknown = JSON.parse(await fs.readFile(this.#filePath, "utf8"));
      if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw profileError("INVALID_INPUT", "Invalid desktop profile document.");
      }
      const document = parsed as Record<string, unknown>;
      if (Object.keys(document).sort().join(",") !== "profiles,schemaVersion"
          || document.schemaVersion !== 1
          || !Array.isArray(document.profiles)) {
        throw profileError("INVALID_INPUT", "Invalid desktop profile document.");
      }
      namedProfiles = document.profiles
        .map(normalizeProfile)
        .filter((profile) => profile.id !== "default");
      const ids = new Set<string>(["default"]);
      for (const profile of namedProfiles) {
        if (ids.has(profile.id)) throw profileError("INVALID_INPUT", "Duplicate desktop profile ID.");
        ids.add(profile.id);
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException)?.code !== "ENOENT") throw error;
    }
    this.#profiles = [this.#defaultProfile, ...namedProfiles];
    await this.#persist();
    this.#initialized = true;
  }

  list(): DesktopProfileSummary[] {
    this.#assertInitialized();
    return this.#profiles.map((profile) => ({
      id: profile.id,
      name: profile.name,
      revision: revisionOf(profile),
      codexHomeConfigured: true,
      sqliteHomeConfigured: Boolean(profile.sqliteHome)
    }));
  }

  resolve(selector: ProfileSelector): TrustedDesktopProfile {
    this.#assertInitialized();
    const profile = this.#profiles.find((candidate) => candidate.id === selector.profileId);
    if (!profile) throw profileError("INVALID_INPUT", "Unknown desktop profile.");
    const trusted = trustedProfile(profile);
    if (selector.profileRevision !== undefined && selector.profileRevision !== trusted.revision) {
      throw profileError("PROFILE_CHANGED", "The desktop profile changed.");
    }
    return trusted;
  }

  #assertInitialized(): void {
    if (!this.#initialized) throw new Error("Desktop profile repository is not initialized.");
  }

  async #persist(): Promise<void> {
    const document: StoredProfileDocument = {
      schemaVersion: 1,
      profiles: this.#profiles
    };
    await fs.mkdir(path.dirname(this.#filePath), { recursive: true });
    const temporary = `${this.#filePath}.tmp-${process.pid}-${randomBytes(6).toString("hex")}`;
    await fs.writeFile(temporary, `${JSON.stringify(document, null, 2)}\n`, {
      encoding: "utf8",
      mode: 0o600
    });
    await fs.rename(temporary, this.#filePath);
    await fs.chmod(this.#filePath, 0o600).catch(() => {});
  }
}
