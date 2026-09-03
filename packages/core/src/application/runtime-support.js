// @ts-nocheck

import {
  CoreError,
  ensureCodexHome,
  isConfiguredSqliteHome,
  missingConfiguredStateDbError,
  normalizeCodexHome,
  resolveStorageLayout,
  sha256Revision,
  stableStringify,
  withStateDbLocation,
  codexStorage,
  path,
  fs
} from "../infrastructure/node-core-ports.js";

const { detectStateDb } = codexStorage.stateDb;

export function pathComparisonKey(value) {
  const resolved = path.resolve(value);
  return process.platform === "win32" ? resolved.toLowerCase() : resolved;
}

export function throwIfAborted(signal) {
  if (!signal?.aborted) return;
  const error = new Error("The provider-sync operation was cancelled.");
  error.name = "AbortError";
  error.code = "ABORT_ERR";
  throw error;
}

export async function prepareStorage({ codexHome: explicitCodexHome, sqliteHome, configText, storage, platform }) {
  if (storage) return storage;
  const codexHome = normalizeCodexHome(explicitCodexHome);
  const layout = resolveStorageLayout({ codexHome, sqliteHome, configText, platform });
  await ensureCodexHome(layout);
  if (!layout.sqliteAccess.supported) return withStateDbLocation(layout, null);
  return withStateDbLocation(layout, await detectStateDb(layout));
}

export async function physicalDirectoryComparisonKey(value) {
  try {
    const lexical = path.resolve(value);
    const first = path.resolve(await fs.realpath(lexical));
    const stat = await fs.stat(first);
    const second = path.resolve(await fs.realpath(lexical));
    if (!stat.isDirectory() || pathComparisonKey(first) !== pathComparisonKey(second)) return null;
    return pathComparisonKey(first);
  } catch {
    return null;
  }
}

export function emitProgress(onProgress, event) {
  if (typeof onProgress !== "function") return;
  try {
    const observerResult = onProgress(event);
    if (observerResult && typeof observerResult.then === "function") observerResult.catch(() => {});
  } catch {
    // Observers never affect the mutation outcome.
  }
}

export function sumCounts(counts) {
  return Object.values(counts ?? {}).reduce((total, value) => total + value, 0);
}

export function buildEncryptedContentWarning(encryptedContentCounts, targetProvider) {
  const riskyProviders = new Set();
  for (const scope of ["sessions", "archived_sessions"]) {
    for (const [provider, count] of Object.entries(encryptedContentCounts?.[scope] ?? {})) {
      if (count > 0 && provider !== targetProvider) riskyProviders.add(provider);
    }
  }
  const total = sumCounts(encryptedContentCounts?.sessions) + sumCounts(encryptedContentCounts?.archived_sessions);
  if (riskyProviders.size === 0) return null;
  return `Encrypted content warning: ${total} rollout file(s) contain encrypted_content from provider(s) ${[...riskyProviders].sort().join(", ")}. Visibility metadata can be synchronized to ${targetProvider}, but continuing or compacting those histories may fail with invalid_encrypted_content. Return to the original provider/account or start a new session if you need reliable continuation.`;
}

export function normalizeProfileId(value) {
  const profileId = value ?? "default";
  if (typeof profileId !== "string" || !/^[A-Za-z0-9._-]{1,80}$/.test(profileId)) {
    throw new CoreError("INVALID_INPUT", "The storage profile id is invalid.");
  }
  return profileId;
}

export function comparableProfilePath(value, platform) {
  if (typeof value !== "string" || !value) return null;
  const resolved = path.resolve(value);
  return platform === "win32" ? resolved.toLowerCase() : resolved;
}

export function createProfileSnapshot({ profileId, suppliedRevision, codexHome, sqliteHome, platform = process.platform }) {
  if (suppliedRevision !== undefined && suppliedRevision !== null
      && (typeof suppliedRevision !== "string" || !suppliedRevision || suppliedRevision.length > 512)) {
    throw new CoreError("INVALID_INPUT", "The storage profile revision is invalid.");
  }
  const id = normalizeProfileId(profileId);
  const revision = sha256Revision(stableStringify({
    schemaVersion: 1,
    id,
    suppliedRevision: suppliedRevision ?? null,
    codexHome: comparableProfilePath(codexHome, platform),
    sqliteHome: comparableProfilePath(sqliteHome, platform)
  }));
  return Object.freeze({
    id,
    revision,
    suppliedRevision: suppliedRevision ?? null,
    codexHome: path.resolve(codexHome),
    sqliteHome: typeof sqliteHome === "string" && sqliteHome ? path.resolve(sqliteHome) : null
  });
}

export function profileFromOptions(options, codexHome, sqliteHome, platform) {
  return createProfileSnapshot({
    profileId: options.profile?.id ?? options.profileId,
    suppliedRevision: options.profile?.revision ?? options.profileRevision,
    codexHome,
    sqliteHome,
    platform
  });
}

export function explicitSqliteHomeFromOptions(options) {
  if (typeof options.sqliteHome === "string" && options.sqliteHome.trim()) return options.sqliteHome;
  if (options.storage?.sqliteHomeSource !== "default"
      && typeof options.storage?.sqliteHome === "string"
      && options.storage.sqliteHome.trim()) return options.storage.sqliteHome;
  return undefined;
}

export function assertConfiguredStateDb(storage) {
  if (!storage.stateDbLocation && isConfiguredSqliteHome(storage)) {
    throw missingConfiguredStateDbError(storage);
  }
}
