import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

import { DEFAULT_LOCK_NAME } from "./constants.js";
import { syncDirectory } from "./atomic-file.js";

const execFileAsync = promisify(execFile);
const DEFAULT_LOCK_CREATE_RETRY_COUNT = 3;
const DEFAULT_LOCK_CREATE_RETRY_DELAY_MS = 75;
const DEFAULT_STALE_RECLAIM_ATTEMPT_LIMIT = 8;

function isTransientLockCreateError(error) {
  return error?.code === "EPERM" || error?.code === "EACCES";
}

async function sleep(delayMs) {
  await new Promise((resolve) => setTimeout(resolve, delayMs));
}

async function processExists(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error?.code === "ESRCH") {
      return false;
    }
    if (error?.code === "EPERM") {
      return true;
    }
    throw error;
  }
}

async function getProcessStartMarker(pid) {
  if (!Number.isInteger(pid) || pid <= 0 || !(await processExists(pid))) {
    return null;
  }
  if (process.platform === "win32") {
    const { stdout } = await execFileAsync("powershell.exe", [
      "-NoProfile",
      "-ExecutionPolicy",
      "Bypass",
      "-Command",
      `(Get-Process -Id ${pid} -ErrorAction Stop).StartTime.ToUniversalTime().Ticks`
    ]);
    const marker = stdout.trim();
    if (!/^\d+$/.test(marker)) {
      throw new Error(`Unable to verify process start identity for PID ${pid}.`);
    }
    return `windows:${marker}`;
  }
  if (process.platform === "linux") {
    try {
      const [stat, bootId] = await Promise.all([
        fs.readFile(`/proc/${pid}/stat`, "utf8"),
        fs.readFile("/proc/sys/kernel/random/boot_id", "utf8")
      ]);
      const closeParen = stat.lastIndexOf(")");
      const fieldsAfterCommand = stat.slice(closeParen + 2).trim().split(/\s+/);
      const startTicks = fieldsAfterCommand[19];
      if (!startTicks) {
        throw new Error("missing process start ticks");
      }
      return `linux:${bootId.trim()}:${startTicks}`;
    } catch (error) {
      if (error?.code === "ENOENT") {
        return null;
      }
      throw error;
    }
  }
  const { stdout } = await execFileAsync("ps", ["-p", String(pid), "-o", "lstart="] , {
    env: { ...process.env, LC_ALL: "C", LANG: "C" }
  });
  const marker = stdout.trim().replace(/\s+/g, " ");
  return marker ? `${process.platform}:${marker}` : null;
}

function lockExistsError(lockDir, reason) {
  return new Error(
    `Lock already exists at ${lockDir}. ${reason} Close Codex/App and retry; do not remove it unless the recorded owner is known to be gone.`
  );
}

function processStartedAtFromMarker(marker) {
  const windowsTicks = /^windows:(\d+)$/.exec(marker ?? "");
  if (windowsTicks) {
    try {
      const unixEpochTicks = 621355968000000000n;
      const milliseconds = (BigInt(windowsTicks[1]) - unixEpochTicks) / 10000n;
      return toUtcSecond(new Date(Number(milliseconds)));
    } catch {
      return null;
    }
  }

  const calendarStart = /^[^:]+:(.+)$/.exec(marker ?? "")?.[1];
  if (calendarStart && !marker.startsWith("linux:")) {
    const parsed = Date.parse(calendarStart);
    return Number.isFinite(parsed) ? toUtcSecond(new Date(parsed)) : null;
  }
  return null;
}

function toUtcSecond(value) {
  const milliseconds = value instanceof Date ? value.getTime() : Date.parse(value);
  if (!Number.isFinite(milliseconds)) {
    return null;
  }
  return new Date(Math.floor(milliseconds / 1000) * 1000).toISOString();
}

async function getProcessStartedAt(pid, marker) {
  const fromMarker = processStartedAtFromMarker(marker);
  if (fromMarker) {
    return fromMarker;
  }
  const { stdout } = await execFileAsync("ps", ["-p", String(pid), "-o", "lstart="], {
    env: { ...process.env, LC_ALL: "C", LANG: "C" }
  });
  const startedAt = stdout.trim().replace(/\s+/g, " ");
  return startedAt ? toUtcSecond(startedAt) : null;
}

function ownerGeneration(owner) {
  return owner.instanceId ?? owner.rawText;
}

function ownerMatchesExpected(actual, expected) {
  return actual.pid === expected.pid
    && ownerGeneration(actual) === ownerGeneration(expected)
    && actual.processStartMarker === expected.processStartMarker
    && actual.processStartedAt === expected.processStartedAt;
}

function liveIdentityMatchesOwner(liveMarker, liveStartedAt, owner) {
  if (!liveMarker) {
    return false;
  }
  if (owner.processStartMarker
      && (owner.runtime === "node" || owner.protocolVersion !== 2)) {
    return liveMarker === owner.processStartMarker;
  }
  if (owner.protocolVersion >= 2 && owner.processStartedAt) {
    if (!liveStartedAt) {
      // A live PID whose start time cannot be compared safely is retained.
      // This is preferable to reclaiming another runtime's active lock.
      return true;
    }
    return Math.abs(Date.parse(liveStartedAt) - Date.parse(owner.processStartedAt)) < 1000;
  }
  if (owner.processStartMarker) {
    return liveMarker === owner.processStartMarker;
  }
  if (owner.processStartedAt) {
    if (!liveStartedAt) {
      return true;
    }
    return Math.abs(Date.parse(liveStartedAt) - Date.parse(owner.processStartedAt)) < 1000;
  }
  return true;
}

async function readLockOwner(ownerPath, fsImpl) {
  let text;
  try {
    text = await fsImpl.readFile(ownerPath, "utf8");
  } catch (error) {
    if (error?.code === "ENOENT") {
      const missingOwner = lockExistsError(
        path.dirname(ownerPath),
        "owner.json is not visible yet, so ownership cannot be proven safely."
      );
      missingOwner.code = "ENOENT";
      throw missingOwner;
    }
    throw error;
  }
  let owner;
  try {
    owner = JSON.parse(text);
  } catch {
    throw lockExistsError(path.dirname(ownerPath), "owner.json is malformed, so the lock is retained fail-closed.");
  }
  const protocolVersion = owner?.protocolVersion;
  if (protocolVersion !== undefined
      && (!Number.isInteger(protocolVersion) || protocolVersion < 1 || protocolVersion > 2)) {
    throw lockExistsError(
      path.dirname(ownerPath),
      `owner.json uses unsupported lock protocol ${String(protocolVersion)}.`
    );
  }
  const hasPid = Number.isInteger(owner?.pid);
  const hasProcessId = Number.isInteger(owner?.processId);
  if (hasPid && hasProcessId && owner.pid !== owner.processId) {
    throw lockExistsError(path.dirname(ownerPath), "owner.json has conflicting pid and processId values.");
  }
  const pid = hasPid ? owner.pid : owner?.processId;
  const processStartMarker = typeof owner?.processStartMarker === "string"
    && owner.processStartMarker
    ? owner.processStartMarker
    : null;
  const processStartedAt = typeof owner?.processStartedAt === "string"
    && Number.isFinite(Date.parse(owner.processStartedAt))
    ? toUtcSecond(owner.processStartedAt)
    : null;
  const instanceId = typeof owner?.instanceId === "string" && owner.instanceId
    ? owner.instanceId
    : null;
  if (protocolVersion === 2
      && (!hasPid || !hasProcessId || !processStartedAt || !instanceId)) {
    throw lockExistsError(
      path.dirname(ownerPath),
      "owner.json is missing required version 2 identity fields."
    );
  }
  if (!Number.isInteger(pid)
      || pid <= 0
      || (!processStartMarker && !processStartedAt && !Number.isInteger(owner?.processId))) {
    throw lockExistsError(path.dirname(ownerPath), "owner.json lacks a verifiable process identity.");
  }
  return {
    ...owner,
    pid,
    processId: pid,
    processStartMarker,
    processStartedAt,
    instanceId,
    rawText: text
  };
}

function directoryIdentity(stats) {
  return `${String(stats.dev)}:${String(stats.ino)}`;
}

function sameDirectoryIdentity(left, right) {
  return left !== null && right !== null && left === right;
}

async function lstatOrNull(targetPath, fsImpl) {
  try {
    return await fsImpl.lstat(targetPath, { bigint: true });
  } catch (error) {
    if (error?.code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

async function inspectCanonicalDirectory(lockDir, fsImpl) {
  const stats = await lstatOrNull(lockDir, fsImpl);
  if (!stats) {
    return null;
  }
  if (stats.isSymbolicLink()) {
    throw lockExistsError(lockDir, "The canonical lock path is a symbolic link and is retained fail-closed.");
  }
  if (!stats.isDirectory()) {
    throw lockExistsError(lockDir, "The canonical lock path is not a directory and is retained fail-closed.");
  }
  return directoryIdentity(stats);
}

async function restoreQuarantinedOwner(
  sourceDir,
  lockDir,
  fsImpl,
  syncDirectoryImpl,
  platform
) {
  const parentDir = path.dirname(lockDir);
  let reservationIdentity = null;
  try {
    await fsImpl.mkdir(lockDir, { mode: 0o700 });
    reservationIdentity = await inspectCanonicalDirectory(lockDir, fsImpl);
    await fsImpl.link(
      path.join(sourceDir, "owner.json"),
      path.join(lockDir, "owner.json")
    );
    const publishedIdentity = await inspectCanonicalDirectory(lockDir, fsImpl);
    if (!sameDirectoryIdentity(reservationIdentity, publishedIdentity)) {
      return false;
    }
    await syncDirectoryImpl(lockDir, { fsImpl, platform });
    await syncDirectoryImpl(parentDir, { fsImpl, platform });
    return true;
  } catch {
    // The quarantined generation remains available for diagnosis. Never use
    // rename here: POSIX rename may replace a concurrently-created empty dir.
    return false;
  }
}

async function quarantineStaleLock(
  lockDir,
  expectedOwner,
  expectedDirectoryIdentity,
  fsImpl,
  syncDirectoryImpl,
  platform
) {
  const quarantinePath = `${lockDir}.stale.${Date.now()}.${randomUUID()}`;
  try {
    await fsImpl.rename(lockDir, quarantinePath);
  } catch (error) {
    if (error?.code === "ENOENT") {
      return false;
    }
    throw error;
  }

  let quarantinedOwner;
  try {
    quarantinedOwner = await readLockOwner(path.join(quarantinePath, "owner.json"), fsImpl);
  } catch (error) {
    await restoreQuarantinedOwner(
      quarantinePath,
      lockDir,
      fsImpl,
      syncDirectoryImpl,
      platform
    );
    throw error;
  }
  const quarantinedStats = await lstatOrNull(quarantinePath, fsImpl);
  const quarantinedIdentity = quarantinedStats?.isDirectory()
    ? directoryIdentity(quarantinedStats)
    : null;
  if (!ownerMatchesExpected(quarantinedOwner, expectedOwner)
      || !sameDirectoryIdentity(quarantinedIdentity, expectedDirectoryIdentity)) {
    const restored = await restoreQuarantinedOwner(
      quarantinePath,
      lockDir,
      fsImpl,
      syncDirectoryImpl,
      platform
    );
    throw lockExistsError(
      lockDir,
      restored
        ? "The lock generation changed during stale-lock reclamation, so its owner was restored without replacing another directory."
        : `The owner changed during stale-lock reclamation; its lock is preserved at ${quarantinePath}.`
    );
  }
  await fsImpl.rm(quarantinePath, { recursive: true, force: true });
  return true;
}

async function createCandidateDirectory(candidateDir, fsImpl, retryCount, retryDelayMs, sleepImpl) {
  let attempts = 0;
  while (true) {
    try {
      await fsImpl.mkdir(candidateDir, { mode: 0o700 });
      return;
    } catch (error) {
      if (!isTransientLockCreateError(error) || attempts >= retryCount) {
        throw error;
      }
      attempts += 1;
      await sleepImpl(retryDelayMs);
    }
  }
}

async function removeOwnedCanonical(
  lockDir,
  owner,
  fsImpl,
  syncDirectoryImpl,
  platform,
  expectedDirectoryIdentity,
  suffix = "release"
) {
  const parentDir = path.dirname(lockDir);
  const removalPath = `${lockDir}.${suffix}.${process.pid}.${randomUUID()}`;
  const currentDirectoryIdentity = await inspectCanonicalDirectory(lockDir, fsImpl);
  if (!sameDirectoryIdentity(currentDirectoryIdentity, expectedDirectoryIdentity)) {
    throw new Error(`Refusing to remove lock ${lockDir} because its directory identity changed.`);
  }
  await fsImpl.rename(lockDir, removalPath);
  const currentOwner = await readLockOwner(path.join(removalPath, "owner.json"), fsImpl);
  const removalStats = await lstatOrNull(removalPath, fsImpl);
  const removalIdentity = removalStats?.isDirectory()
    ? directoryIdentity(removalStats)
    : null;
  if (!ownerMatchesExpected(currentOwner, owner)
      || !sameDirectoryIdentity(removalIdentity, expectedDirectoryIdentity)) {
    const restored = await restoreQuarantinedOwner(
      removalPath,
      lockDir,
      fsImpl,
      syncDirectoryImpl,
      platform
    );
    throw new Error(
      restored
        ? `Refusing to remove lock ${lockDir} because its generation changed; its owner was restored safely.`
        : `Refusing to remove lock ${lockDir}; the changed owner is preserved at ${removalPath}.`
    );
  }
  await fsImpl.rm(removalPath, { recursive: true, force: true });
  await syncDirectoryImpl(parentDir, { fsImpl, platform });
}

async function publishClaim(claimsDir, owner, fsImpl, syncDirectoryImpl, platform) {
  const claimPath = path.join(claimsDir, `${owner.instanceId}.json`);
  const candidatePath = path.join(
    claimsDir,
    `.${owner.instanceId}.${process.pid}.${randomUUID()}.tmp`
  );
  const handle = await fsImpl.open(candidatePath, "wx", 0o600);
  try {
    await handle.writeFile(JSON.stringify(owner, null, 2), "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    await fsImpl.rename(candidatePath, claimPath);
    await syncDirectoryImpl(claimsDir, { fsImpl, platform });
    return claimPath;
  } catch (error) {
    await fsImpl.rm(candidatePath, { force: true }).catch(() => {});
    throw error;
  }
}

async function isOwnerLive(owner, getProcessIdentity, getProcessStartedAtIdentity) {
  const liveMarker = await getProcessIdentity(owner.pid);
  if (!liveMarker) {
    return false;
  }
  let liveStartedAt = null;
  try {
    liveStartedAt = await getProcessStartedAtIdentity(owner.pid, liveMarker);
  } catch {
    // The exact Node process marker remains sufficient for legacy/current
    // Node owners. Cross-runtime owners fail closed if their start time cannot
    // be inspected.
  }
  return liveIdentityMatchesOwner(liveMarker, liveStartedAt, owner);
}

async function establishUniqueClaim({
  claimsDir,
  claimPath,
  owner,
  fsImpl,
  getProcessIdentity,
  getProcessStartedAtIdentity,
  syncDirectoryImpl,
  platform
}) {
  for (let scan = 0; scan < 2; scan += 1) {
    const entries = await fsImpl.readdir(claimsDir, { withFileTypes: true });
    entries.sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      if (!entry.isFile() || !entry.name.endsWith(".json")) {
        continue;
      }
      const otherPath = path.join(claimsDir, entry.name);
      if (path.resolve(otherPath) === path.resolve(claimPath)) {
        continue;
      }
      const otherOwner = await readLockOwner(otherPath, fsImpl);
      if (!otherOwner.instanceId || entry.name !== `${otherOwner.instanceId}.json`) {
        throw lockExistsError(claimsDir, `Claim ${otherPath} has no matching immutable instance identity.`);
      }
      let live;
      try {
        live = await isOwnerLive(otherOwner, getProcessIdentity, getProcessStartedAtIdentity);
      } catch (identityError) {
        throw lockExistsError(
          claimsDir,
          `Claim ${otherPath} could not be verified (${identityError.message}).`
        );
      }
      if (live) {
        throw lockExistsError(claimsDir, `PID ${otherOwner.pid} holds live claim ${entry.name}.`);
      }
      // The filename is a never-reused instance generation. Removing this one
      // stale file cannot delete a newer claimant's record.
      await fsImpl.rm(otherPath, { force: true });
      await syncDirectoryImpl(claimsDir, { fsImpl, platform });
    }
  }

  const currentOwner = await readLockOwner(claimPath, fsImpl);
  if (currentOwner.instanceId !== owner.instanceId) {
    throw new Error(`Refusing to use claim ${claimPath} because its owner identity changed.`);
  }
}

async function removeOwnedClaim(claimPath, owner, fsImpl, syncDirectoryImpl, platform) {
  let currentOwner;
  try {
    currentOwner = await readLockOwner(claimPath, fsImpl);
  } catch (error) {
    if (error?.code === "ENOENT") {
      return;
    }
    throw error;
  }
  if (currentOwner.instanceId !== owner.instanceId) {
    throw new Error(`Refusing to remove claim ${claimPath} because its owner identity changed.`);
  }
  await fsImpl.rm(claimPath, { force: true });
  await syncDirectoryImpl(path.dirname(claimPath), { fsImpl, platform });
}

export async function acquireLock(codexHome, label = "codex-provider-sync", options = {}) {
  return acquirePathLock(
    path.join(codexHome, "tmp", DEFAULT_LOCK_NAME),
    label,
    options
  );
}

export async function acquirePathLock(lockPath, label = "codex-provider-sync", options = {}) {
  const {
    fsImpl = fs,
    retryCount = DEFAULT_LOCK_CREATE_RETRY_COUNT,
    retryDelayMs = DEFAULT_LOCK_CREATE_RETRY_DELAY_MS,
    staleReclaimAttemptLimit = DEFAULT_STALE_RECLAIM_ATTEMPT_LIMIT,
    sleepImpl = sleep,
    getProcessIdentity = getProcessStartMarker,
    getProcessStartedAtIdentity = getProcessStartedAt,
    syncDirectoryImpl = syncDirectory,
    onCandidateReady,
    onBeforeStaleReclaim,
    platform = process.platform
  } = options;
  const lockDir = path.resolve(lockPath);
  if (!Number.isInteger(staleReclaimAttemptLimit) || staleReclaimAttemptLimit < 0) {
    throw new TypeError("staleReclaimAttemptLimit must be a non-negative integer.");
  }
  const ownerPath = path.join(lockDir, "owner.json");
  const parentDir = path.dirname(lockDir);
  const claimsDir = `${lockDir}.claims`;
  const candidateDir = path.join(
    parentDir,
    `.${path.basename(lockDir)}.candidate.${process.pid}.${randomUUID()}`
  );
  await fsImpl.mkdir(parentDir, { recursive: true });
  await fsImpl.mkdir(claimsDir, { recursive: true, mode: 0o700 });
  const processStartMarker = await getProcessIdentity(process.pid);
  if (!processStartMarker) {
    throw new Error(`Unable to establish the current process identity for lock ${lockDir}.`);
  }

  const processStartedAt = await getProcessStartedAtIdentity(process.pid, processStartMarker);
  if (!processStartedAt) {
    throw new Error(`Unable to establish the current process start time for lock ${lockDir}.`);
  }
  const owner = {
    protocolVersion: 2,
    runtime: "node",
    pid: process.pid,
    processId: process.pid,
    processStartMarker,
    processStartedAt: toUtcSecond(processStartedAt),
    instanceId: randomUUID(),
    startedAt: new Date().toISOString(),
    label,
    cwd: process.cwd(),
    currentDirectory: process.cwd()
  };
  let published = false;
  let canonicalReserved = false;
  let ownerLinked = false;
  let canonicalDirectoryIdentity = null;
  let claimPath = null;
  try {
    claimPath = await publishClaim(
      claimsDir,
      owner,
      fsImpl,
      syncDirectoryImpl,
      platform
    );
    await establishUniqueClaim({
      claimsDir,
      claimPath,
      owner,
      fsImpl,
      getProcessIdentity,
      getProcessStartedAtIdentity,
      syncDirectoryImpl,
      platform
    });
    await createCandidateDirectory(
      candidateDir,
      fsImpl,
      retryCount,
      retryDelayMs,
      sleepImpl
    );
    const candidateOwnerPath = path.join(candidateDir, "owner.json");
    const ownerHandle = await fsImpl.open(candidateOwnerPath, "wx", 0o600);
    try {
      await ownerHandle.writeFile(JSON.stringify(owner, null, 2), "utf8");
      await ownerHandle.sync();
    } finally {
      await ownerHandle.close();
    }
    await syncDirectoryImpl(candidateDir, { fsImpl, platform });
    await onCandidateReady?.({ candidateDir, lockDir, ownerPath: candidateOwnerPath, owner });

    let attempts = 0;
    let staleReclaimAttempts = 0;
    while (true) {
      try {
        // Directory rename is not an exclusive publish on POSIX: it may replace
        // an existing empty directory. Reserve the canonical name with mkdir,
        // then publish the already-durable owner inode with a no-replace link.
        await fsImpl.mkdir(lockDir, { mode: 0o700 });
        canonicalReserved = true;
        canonicalDirectoryIdentity = await inspectCanonicalDirectory(lockDir, fsImpl);
        await fsImpl.link(candidateOwnerPath, ownerPath);
        ownerLinked = true;
        const publishedDirectoryIdentity = await inspectCanonicalDirectory(lockDir, fsImpl);
        if (!sameDirectoryIdentity(canonicalDirectoryIdentity, publishedDirectoryIdentity)) {
          throw lockExistsError(
            lockDir,
            "The canonical reservation changed identity while owner.json was being published; the live claim is retained because publication is uncertain."
          );
        }
        published = true;
        await syncDirectoryImpl(lockDir, { fsImpl, platform });
        await syncDirectoryImpl(parentDir, { fsImpl, platform });
        await fsImpl.rm(candidateDir, { recursive: true, force: true });
        break;
      } catch (error) {
        if (canonicalReserved) {
          throw error;
        }
        const existingDirectoryIdentity = await inspectCanonicalDirectory(lockDir, fsImpl);
        if (existingDirectoryIdentity !== null) {
          const existingOwner = await readLockOwner(ownerPath, fsImpl);
          let live;
          try {
            live = await isOwnerLive(
              existingOwner,
              getProcessIdentity,
              getProcessStartedAtIdentity
            );
          } catch (identityError) {
            throw lockExistsError(lockDir, `The recorded owner could not be verified (${identityError.message}).`);
          }
          if (live) {
            throw lockExistsError(lockDir, `PID ${existingOwner.pid} is still the verified owner.`);
          }
          if (staleReclaimAttempts >= staleReclaimAttemptLimit) {
            throw lockExistsError(
              lockDir,
              `Stale-lock reclamation exceeded the bounded limit of ${staleReclaimAttemptLimit} attempts.`
            );
          }
          staleReclaimAttempts += 1;
          await onBeforeStaleReclaim?.({ lockDir, existingOwner, owner });
          if (await quarantineStaleLock(
            lockDir,
            existingOwner,
            existingDirectoryIdentity,
            fsImpl,
            syncDirectoryImpl,
            platform
          )) {
            await syncDirectoryImpl(parentDir, { fsImpl, platform });
          }
          continue;
        }
        if (!isTransientLockCreateError(error) || attempts >= retryCount) {
          throw error;
        }
        attempts += 1;
        await sleepImpl(retryDelayMs);
      }
    }
  } catch (error) {
    const cleanupFailures = [];
    let canonicalCleanupSafe = !published && !canonicalReserved;
    if (published) {
      try {
        await removeOwnedCanonical(
          lockDir,
          owner,
          fsImpl,
          syncDirectoryImpl,
          platform,
          canonicalDirectoryIdentity,
          "acquire-failed"
        );
        canonicalCleanupSafe = true;
      } catch (cleanupError) {
        cleanupFailures.push(cleanupError);
      }
    } else if (canonicalReserved && !ownerLinked) {
      try {
        // rmdir is intentionally non-recursive: if another runtime populated
        // the reserved directory, preserve it. Since link never succeeded, our
        // independent claim can still be released safely below.
        const cleanupDirectoryIdentity = await inspectCanonicalDirectory(lockDir, fsImpl);
        if (!sameDirectoryIdentity(cleanupDirectoryIdentity, canonicalDirectoryIdentity)) {
          throw new Error(
            `Refusing to remove empty reservation ${lockDir} because its directory identity changed.`
          );
        }
        await fsImpl.rmdir(lockDir);
        await syncDirectoryImpl(parentDir, { fsImpl, platform });
        canonicalCleanupSafe = true;
      } catch (cleanupError) {
        cleanupFailures.push(cleanupError);
      }
    }
    try {
      await fsImpl.rm(candidateDir, { recursive: true, force: true });
    } catch (cleanupError) {
      cleanupFailures.push(cleanupError);
    }
    if (claimPath && (canonicalCleanupSafe || !ownerLinked)) {
      try {
        await removeOwnedClaim(claimPath, owner, fsImpl, syncDirectoryImpl, platform);
      } catch (cleanupError) {
        cleanupFailures.push(cleanupError);
      }
    }
    if (cleanupFailures.length > 0) {
      throw new AggregateError(
        [error, ...cleanupFailures],
        `Lock acquisition failed and cleanup was incomplete: ${error.message}`,
        { cause: error }
      );
    }
    throw error;
  }

  let released = false;
  return async function releaseLock() {
    if (released) {
      return;
    }
    await removeOwnedCanonical(
      lockDir,
      owner,
      fsImpl,
      syncDirectoryImpl,
      platform,
      canonicalDirectoryIdentity
    );
    await removeOwnedClaim(claimPath, owner, fsImpl, syncDirectoryImpl, platform);
    released = true;
  };
}
