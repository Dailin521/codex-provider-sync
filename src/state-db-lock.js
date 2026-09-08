import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

import { DB_FILE_BASENAME } from "./constants.js";
import { CoreError } from "./core-error.js";
import { acquirePathLock } from "./locking.js";

function unverifiable(message, cause) {
  return new CoreError("LOCK_UNVERIFIABLE", message, {
    cause,
    details: {
      lockScope: "state-db",
      ...(typeof cause?.code === "string" ? { causeCode: cause.code } : {})
    }
  });
}

function permissionDenied(message, cause) {
  return new CoreError("PERMISSION_DENIED", message, {
    cause,
    details: {
      lockScope: "state-db",
      ...(typeof cause?.code === "string" ? { causeCode: cause.code } : {})
    }
  });
}

function normalizeIdentityPart(value, platform) {
  return platform === "win32" ? value.toLowerCase() : value;
}

export async function resolveStateDbLockResource(
  stateDbPath,
  { fsImpl = fs, platform = process.platform } = {}
) {
  if (typeof stateDbPath !== "string" || !stateDbPath.trim()) {
    throw unverifiable("The State DB resource path is missing or invalid.");
  }
  const lexicalPath = path.resolve(stateDbPath);
  if (path.basename(lexicalPath).toLowerCase() !== DB_FILE_BASENAME.toLowerCase()) {
    throw unverifiable("The State DB resource filename is not canonical.");
  }
  const lexicalParent = path.dirname(lexicalPath);
  let realParent;
  try {
    realParent = await fsImpl.realpath(lexicalParent);
    const parentStats = await fsImpl.stat(realParent);
    if (!parentStats.isDirectory()) {
      throw unverifiable("The State DB physical parent is not a directory.");
    }
    const verifiedParent = await fsImpl.realpath(lexicalParent);
    if (normalizeIdentityPart(realParent, platform) !== normalizeIdentityPart(verifiedParent, platform)) {
      throw unverifiable("The State DB physical parent changed while its identity was resolved.");
    }
  } catch (error) {
    if (error instanceof CoreError) throw error;
    if (error?.code === "EACCES" || error?.code === "EPERM") {
      throw permissionDenied("Permission denied while resolving the State DB resource identity.", error);
    }
    throw unverifiable("The State DB physical parent identity cannot be verified.", error);
  }

  let physicalFileName = DB_FILE_BASENAME;
  try {
    const realFile = await fsImpl.realpath(lexicalPath);
    physicalFileName = path.basename(realFile);
    realParent = path.dirname(realFile);
    const fileStats = await fsImpl.stat(realFile);
    if (!fileStats.isFile()) throw unverifiable("The State DB target is not a regular file.");
    if (physicalFileName.toLowerCase() !== DB_FILE_BASENAME.toLowerCase()) {
      throw unverifiable("The State DB physical filename is not canonical.");
    }
  } catch (error) {
    if (error instanceof CoreError) throw error;
    if (error?.code !== "ENOENT") {
      if (error?.code === "EACCES" || error?.code === "EPERM") {
        throw permissionDenied("Permission denied while resolving the State DB physical target.", error);
      }
      throw unverifiable("The State DB physical target identity cannot be verified.", error);
    }
  }

  const normalizedParent = normalizeIdentityPart(path.resolve(realParent), platform);
  const normalizedFileName = normalizeIdentityPart(physicalFileName, platform);
  // NUL cannot occur in a filesystem path and therefore gives an unambiguous,
  // cross-runtime identity serialization.
  const identity = `${normalizedParent}\0${normalizedFileName}`;
  const resourceKey = createHash("sha256").update(identity, "utf8").digest("hex");
  const lockPath = path.join(
    path.resolve(realParent),
    ".codex-provider-sync",
    "locks",
    `${resourceKey}.lock`
  );
  return Object.freeze({
    identity,
    resourceKey,
    realDbParent: path.resolve(realParent),
    stateDbPath: path.join(path.resolve(realParent), physicalFileName),
    lockPath
  });
}

export async function acquireStateDbLock(stateDbPath, label = "codex-provider-sync", options = {}) {
  const resource = await resolveStateDbLockResource(stateDbPath, options);
  const release = await acquirePathLock(resource.lockPath, label, {
    ...options,
    scope: "state-db",
    resourceKey: resource.resourceKey
  });
  return { resource, release };
}

