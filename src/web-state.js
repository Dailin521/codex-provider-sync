import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

const STATE_VERSION = 1;
const DEFAULT_PROFILE_ID = "default";

function hashSecret(secret) {
  return crypto.createHash("sha256").update(String(secret), "utf8").digest("hex");
}

function normalizeOptionalPath(value) {
  if (value === undefined || value === null || String(value).trim() === "") {
    return null;
  }
  return path.resolve(String(value).trim());
}

function normalizeProfile({ id, name, codexHome, sqliteHome }) {
  const normalizedId = String(id ?? "").trim();
  const normalizedName = String(name ?? "").trim();
  if (!/^[A-Za-z0-9_.-]{1,80}$/.test(normalizedId)) {
    throw new Error("profileId may only contain letters, numbers, dots, underscores, and hyphens.");
  }
  if (!normalizedName || normalizedName.length > 120) {
    throw new Error("Profile name must be between 1 and 120 characters.");
  }
  if (typeof codexHome !== "string" || !codexHome.trim()) {
    throw new Error("Codex Home is required for a storage profile.");
  }
  if (codexHome.includes("\0") || String(sqliteHome ?? "").includes("\0")) {
    throw new Error("Storage paths may not contain NUL characters.");
  }
  return {
    id: normalizedId,
    name: normalizedName,
    codexHome: path.resolve(codexHome.trim()),
    sqliteHome: normalizeOptionalPath(sqliteHome)
  };
}

function profileRevision(profile) {
  const canonical = JSON.stringify({
    id: profile.id,
    name: profile.name,
    codexHome: profile.codexHome,
    sqliteHome: profile.sqliteHome
  });
  return crypto.createHash("sha256").update(canonical, "utf8").digest("base64url");
}

function publicProfile(profile) {
  return { ...profile, revision: profileRevision(profile) };
}

async function assertDirectoryWhenPresent(value, label) {
  if (!value) {
    return;
  }
  try {
    const stats = await fs.stat(value);
    if (!stats.isDirectory()) {
      throw new Error(`${label} must refer to a directory: ${value}`);
    }
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }
}

function emptyState(defaultProfile) {
  return {
    version: STATE_VERSION,
    credentialHashes: [],
    profiles: [defaultProfile]
  };
}

export class WebUiStateStore {
  constructor({ filePath, defaultProfile }) {
    if (!filePath) {
      throw new Error("Web UI state file path is required.");
    }
    this.filePath = path.resolve(filePath);
    this.defaultProfile = normalizeProfile({
      id: DEFAULT_PROFILE_ID,
      name: "Default",
      ...defaultProfile
    });
    this.state = emptyState(this.defaultProfile);
    this.writeQueue = Promise.resolve();
  }

  async initialize({ resetAccess = false } = {}) {
    try {
      const parsed = JSON.parse(await fs.readFile(this.filePath, "utf8"));
      if (parsed?.version === STATE_VERSION && Array.isArray(parsed.profiles) && Array.isArray(parsed.credentialHashes)) {
        const namedProfiles = parsed.profiles
          .filter((profile) => profile?.id !== DEFAULT_PROFILE_ID)
          .map((profile) => normalizeProfile(profile));
        this.state = {
          version: STATE_VERSION,
          credentialHashes: parsed.credentialHashes.filter((value) => typeof value === "string" && /^[a-f0-9]{64}$/.test(value)),
          profiles: [this.defaultProfile, ...namedProfiles]
        };
      }
    } catch (error) {
      if (error?.code !== "ENOENT") {
        throw new Error(`Unable to read Web UI state at ${this.filePath}: ${error.message}`, { cause: error });
      }
    }
    if (resetAccess) {
      this.state.credentialHashes = [];
    }
    await this.persist();
    return this.snapshot();
  }

  snapshot() {
    return {
      version: this.state.version,
      credentialHashes: [...this.state.credentialHashes],
      profiles: this.state.profiles.map((profile) => ({ ...profile }))
    };
  }

  listProfiles() {
    return this.state.profiles.map(publicProfile);
  }

  hasProfile(profileId) {
    return this.state.profiles.some((profile) => profile.id === profileId);
  }

  getProfile(profileId = DEFAULT_PROFILE_ID) {
    const profile = this.state.profiles.find((entry) => entry.id === profileId);
    if (!profile) {
      throw new Error(`Unknown storage profile: ${profileId}`);
    }
    return publicProfile(profile);
  }

  async saveProfile(input) {
    const profile = normalizeProfile(input);
    if (profile.id === DEFAULT_PROFILE_ID) {
      throw new Error("The default storage profile is controlled by Web UI startup flags.");
    }
    await assertDirectoryWhenPresent(profile.codexHome, "Codex Home");
    await assertDirectoryWhenPresent(profile.sqliteHome, "SQLite Home");
    const index = this.state.profiles.findIndex((entry) => entry.id === profile.id);
    if (index >= 0) {
      this.state.profiles[index] = profile;
    } else {
      this.state.profiles.push(profile);
    }
    await this.persist();
    return publicProfile(profile);
  }

  async deleteProfile(profileId) {
    if (profileId === DEFAULT_PROFILE_ID) {
      throw new Error("The default storage profile cannot be deleted.");
    }
    const before = this.state.profiles.length;
    this.state.profiles = this.state.profiles.filter((entry) => entry.id !== profileId);
    if (this.state.profiles.length === before) {
      throw new Error(`Unknown storage profile: ${profileId}`);
    }
    await this.persist();
  }

  hasCredential(secret) {
    if (typeof secret !== "string" || secret.length < 32) {
      return false;
    }
    const candidate = Buffer.from(hashSecret(secret), "hex");
    return this.state.credentialHashes.some((stored) => {
      const expected = Buffer.from(stored, "hex");
      return expected.length === candidate.length && crypto.timingSafeEqual(expected, candidate);
    });
  }

  async addCredential(secret) {
    const digest = hashSecret(secret);
    if (!this.state.credentialHashes.includes(digest)) {
      this.state.credentialHashes.push(digest);
      await this.persist();
    }
  }

  async removeCredential(secret) {
    const digest = hashSecret(secret);
    this.state.credentialHashes = this.state.credentialHashes.filter((stored) => stored !== digest);
    await this.persist();
  }

  async resetCredentials() {
    this.state.credentialHashes = [];
    await this.persist();
  }

  async persist() {
    const serialized = `${JSON.stringify(this.state, null, 2)}\n`;
    const target = this.filePath;
    this.writeQueue = this.writeQueue.then(async () => {
      await fs.mkdir(path.dirname(target), { recursive: true });
      const temporary = `${target}.tmp-${process.pid}-${crypto.randomBytes(6).toString("hex")}`;
      await fs.writeFile(temporary, serialized, { encoding: "utf8", mode: 0o600 });
      await fs.rename(temporary, target);
      await fs.chmod(target, 0o600).catch(() => {});
    });
    return this.writeQueue;
  }
}

export function createMemoryWebUiState(defaultProfile) {
  const normalizedDefault = normalizeProfile({ id: DEFAULT_PROFILE_ID, name: "Default", ...defaultProfile });
  let credentialHashes = [];
  let profiles = [normalizedDefault];
  return {
    listProfiles: () => profiles.map(publicProfile),
    hasProfile: (profileId) => profiles.some((profile) => profile.id === profileId),
    getProfile(profileId = DEFAULT_PROFILE_ID) {
      const profile = profiles.find((entry) => entry.id === profileId);
      if (!profile) throw new Error(`Unknown storage profile: ${profileId}`);
      return publicProfile(profile);
    },
    async saveProfile(input) {
      const profile = normalizeProfile(input);
      if (profile.id === DEFAULT_PROFILE_ID) throw new Error("The default storage profile is controlled by Web UI startup flags.");
      const index = profiles.findIndex((entry) => entry.id === profile.id);
      if (index >= 0) profiles[index] = profile;
      else profiles.push(profile);
      return publicProfile(profile);
    },
    async deleteProfile(profileId) {
      if (profileId === DEFAULT_PROFILE_ID) throw new Error("The default storage profile cannot be deleted.");
      profiles = profiles.filter((entry) => entry.id !== profileId);
    },
    hasCredential(secret) {
      return credentialHashes.includes(hashSecret(secret));
    },
    async addCredential(secret) {
      const digest = hashSecret(secret);
      if (!credentialHashes.includes(digest)) credentialHashes.push(digest);
    },
    async removeCredential(secret) {
      const digest = hashSecret(secret);
      credentialHashes = credentialHashes.filter((stored) => stored !== digest);
    },
    async resetCredentials() {
      credentialHashes = [];
    },
    credentialHashes: () => [...credentialHashes]
  };
}

export { DEFAULT_PROFILE_ID, hashSecret, profileRevision };
