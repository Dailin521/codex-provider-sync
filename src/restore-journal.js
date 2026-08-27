import fs from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";

import { defaultBackupRoot } from "./constants.js";
import { syncDirectory } from "./atomic-file.js";

export const RESTORE_JOURNAL_BASENAME = "restore-journal.v2.jsonl";
export const RESTORE_JOURNAL_SCHEMA_VERSION = 2;

const TERMINAL_STATES = new Set(["completed", "rolled-back", "recovery-required"]);
const NON_BLOCKING_STATES = new Set(["completed", "rolled-back"]);
const VALID_STATES = new Set([
  "prepared",
  "applying",
  "committing",
  "committed-pending-ack",
  "completed",
  "rollback-pending",
  "rolled-back",
  "recovery-required"
]);

const VALID_TRANSITIONS = new Map([
  ["prepared", new Set(["applying", "rollback-pending", "recovery-required"])],
  ["applying", new Set(["applying", "committing", "rollback-pending", "recovery-required"])],
  ["committing", new Set(["committed-pending-ack", "rollback-pending", "recovery-required"])],
  ["committed-pending-ack", new Set(["completed", "recovery-required"])],
  ["rollback-pending", new Set(["rollback-pending", "rolled-back", "recovery-required"])],
  ["completed", new Set()],
  ["rolled-back", new Set()],
  ["recovery-required", new Set()]
]);

async function appendDurableJsonLine(filePath, value) {
  const handle = await fs.open(filePath, "a");
  try {
    await handle.writeFile(`${JSON.stringify(value)}\n`, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
}

function validIdentity(value) {
  return value
    && typeof value === "object"
    && !Array.isArray(value)
    && typeof value.backupId === "string"
    && value.backupId.length > 0
    && typeof value.backupDir === "string"
    && path.isAbsolute(value.backupDir)
    && typeof value.revision === "string"
    && value.revision.length > 0;
}

function validDigest(value) {
  return value
    && typeof value === "object"
    && !Array.isArray(value)
    && typeof value.present === "boolean"
    && typeof value.digestKind === "string"
    && value.digestKind.length > 0
    && typeof value.digest === "string"
    && value.digest.length > 0;
}

function preparedTargetKindsMatch(event) {
  if (!Array.isArray(event.targets) || !Array.isArray(event.requiredTargetKinds)) return false;
  const targetKinds = new Set(event.targets.map((target) => target.kind));
  const requiredKinds = new Set(event.requiredTargetKinds);
  return requiredKinds.size === event.requiredTargetKinds.length
    && targetKinds.size === requiredKinds.size
    && [...targetKinds].every((kind) => requiredKinds.has(kind));
}

function validPrepared(event) {
  return event.operationKind === "restore"
    && validIdentity(event.sourceBackup)
    && validIdentity(event.preRestoreSnapshot)
    && typeof event.preRestoreSnapshot.manifestSha256 === "string"
    && event.preRestoreSnapshot.manifestSha256.length > 0
    && event.storage
    && typeof event.storage === "object"
    && !Array.isArray(event.storage)
    && typeof event.storage.codexHome === "string"
    && path.isAbsolute(event.storage.codexHome)
    && typeof event.storage.codexHomePhysical === "string"
    && path.isAbsolute(event.storage.codexHomePhysical)
    && Array.isArray(event.targets)
    && event.targets.length > 0
    && event.targets.every((target) =>
      target
      && typeof target === "object"
      && typeof target.id === "string"
      && target.id.length > 0
      && typeof target.kind === "string"
      && typeof target.targetPath === "string"
      && path.isAbsolute(target.targetPath)
      && validDigest(target.pre)
      && validDigest(target.expectedPost)
    )
    && new Set(event.targets.map((target) => target.id)).size === event.targets.length
    && Array.isArray(event.requiredTargetKinds)
    && event.requiredTargetKinds.every((value) => typeof value === "string" && value.length > 0)
    && preparedTargetKindsMatch(event)
    && Array.isArray(event.resolvesOperationIds ?? [])
    && (event.resolvesOperationIds ?? []).every((value) => typeof value === "string" && value.length > 0);
}

function validateEvents(parsedEvents) {
  const events = [];
  let validationError = null;
  let operationId = null;
  let expectedSequence = 1;
  let state = null;
  let prepared = null;
  const targetIds = new Set();
  const targetsById = new Map();
  const targetPhases = new Map();
  let committingHash = null;

  for (const event of parsedEvents) {
    const fail = (message) => {
      validationError = message;
      return false;
    };
    if (!event || typeof event !== "object" || Array.isArray(event)) {
      fail("Restore journal event is not an object.");
      break;
    }
    if (event.schemaVersion !== RESTORE_JOURNAL_SCHEMA_VERSION
        || event.protocolVersion !== RESTORE_JOURNAL_SCHEMA_VERSION
        || event.operationKind !== "restore"
        || !VALID_STATES.has(event.state)) {
      fail("Restore journal event has an unsupported schema, protocol, kind, or state.");
      break;
    }
    if (typeof event.operationId !== "string" || event.operationId.length === 0) {
      fail("Restore journal event is missing operationId.");
      break;
    }
    if (event.sequence !== expectedSequence) {
      fail(`Restore journal sequence mismatch: expected ${expectedSequence}, received ${event.sequence}.`);
      break;
    }
    if (operationId === null) {
      if (event.state !== "prepared" || !validPrepared(event)) {
        fail("Restore journal must start with a valid prepared event.");
        break;
      }
      operationId = event.operationId;
      prepared = event;
      state = "prepared";
      for (const target of event.targets) {
        targetIds.add(target.id);
        targetsById.set(target.id, target);
      }
    } else {
      if (event.operationId !== operationId) {
        fail("Restore journal operationId changed within one operation.");
        break;
      }
      if (!VALID_TRANSITIONS.get(state)?.has(event.state)) {
        fail(`Restore journal transition ${state} -> ${event.state} is invalid.`);
        break;
      }
      state = event.state;
    }

    if (event.targetId !== undefined
        || event.targetPhase !== undefined
        || event.targetDigest !== undefined) {
      if (!targetIds.has(event.targetId)
          || !new Set(["intent", "completed", "compensated"]).has(event.targetPhase)) {
        fail("Restore journal target transition is malformed or undeclared.");
        break;
      }
      const previous = targetPhases.get(event.targetId) ?? null;
      if (event.targetPhase === "intent") {
        if (event.state !== "applying" || previous !== null) {
          fail("Restore target intent is duplicated or outside applying.");
          break;
        }
      } else if (event.targetPhase === "completed") {
        if (event.state !== "applying" || previous !== "intent"
            || typeof event.targetDigest !== "string"
            || event.targetDigest.length === 0
            || event.targetDigest !== targetsById.get(event.targetId)?.expectedPost?.digest) {
          fail("Restore target completion has no matching intent or digest.");
          break;
        }
      } else {
        if (event.state !== "rollback-pending"
            || previous === "compensated"
            || typeof event.targetDigest !== "string"
            || event.targetDigest.length === 0
            || event.targetDigest !== targetsById.get(event.targetId)?.pre?.digest) {
          fail("Restore target compensation is outside rollback-pending or has the wrong digest.");
          break;
        }
      }
      targetPhases.set(event.targetId, event.targetPhase);
    }

    if (event.state === "committing") {
      if (targetPhases.size !== targetIds.size
          || [...targetIds].some((targetId) => targetPhases.get(targetId) !== "completed")
          || typeof event.postManifestSha256 !== "string"
          || event.postManifestSha256.length === 0) {
        fail("Restore cannot commit before every declared target is completed.");
        break;
      }
      committingHash = event.postManifestSha256;
    } else if (event.state === "committed-pending-ack") {
      if (typeof event.postManifestSha256 !== "string"
          || event.postManifestSha256.length === 0
          || event.postManifestSha256 !== committingHash) {
        fail("Restore commit acknowledgement hash does not match committing evidence.");
        break;
      }
    } else if (event.state === "rolled-back"
        && (targetPhases.size !== targetIds.size
          || [...targetIds].some((targetId) => targetPhases.get(targetId) !== "compensated"))) {
      fail("Restore cannot become rolled-back before every declared target is compensated.");
      break;
    }

    events.push(event);
    expectedSequence += 1;
  }

  return { events, operationId, state, prepared, validationError, targetPhases };
}

export async function readRestoreJournal(filePath) {
  const rawText = await fs.readFile(filePath, "utf8");
  const parsedEvents = [];
  let parseError = rawText.length > 0 && !rawText.endsWith("\n")
    ? "Restore journal is missing its final newline and may contain a torn append."
    : null;
  for (const line of rawText.split(/\r?\n/)) {
    if (!line.trim()) continue;
    try {
      parsedEvents.push(JSON.parse(line));
    } catch {
      parseError = "Restore journal contains a truncated or malformed JSON line.";
      break;
    }
  }
  const validated = validateEvents(parsedEvents);
  const rawPrepared = parsedEvents[0];
  const protectionReferences = {
    sourceBackupDir: typeof rawPrepared?.sourceBackup?.backupDir === "string"
      && path.isAbsolute(rawPrepared.sourceBackup.backupDir)
      ? path.resolve(rawPrepared.sourceBackup.backupDir)
      : null,
    preRestoreSnapshotDir: typeof rawPrepared?.preRestoreSnapshot?.backupDir === "string"
      && path.isAbsolute(rawPrepared.preRestoreSnapshot.backupDir)
      ? path.resolve(rawPrepared.preRestoreSnapshot.backupDir)
      : path.dirname(path.resolve(filePath))
  };
  const protectionReferencesUnverifiable = !(
    typeof rawPrepared?.sourceBackup?.backupDir === "string"
    && path.isAbsolute(rawPrepared.sourceBackup.backupDir)
    && typeof rawPrepared?.preRestoreSnapshot?.backupDir === "string"
    && path.isAbsolute(rawPrepared.preRestoreSnapshot.backupDir)
  );
  const validationError = parseError ?? validated.validationError;
  const invalidTail = validationError !== null || validated.events.length !== parsedEvents.length;
  const state = invalidTail ? "recovery-required" : (validated.state ?? "recovery-required");
  return {
    filePath: path.resolve(filePath),
    snapshotDir: path.dirname(path.resolve(filePath)),
    backupDir: path.dirname(path.resolve(filePath)),
    operationKind: "restore",
    events: validated.events,
    operationId: validated.operationId,
    prepared: validated.prepared,
    state,
    invalidTail,
    validationError,
    terminal: !invalidTail && TERMINAL_STATES.has(state),
    blocking: invalidTail || !NON_BLOCKING_STATES.has(state),
    targetPhases: validated.targetPhases,
    protectionReferences,
    protectionReferencesUnverifiable,
    rawText
  };
}

export class RestoreJournal {
  constructor(filePath, operationId, sequence = 0) {
    this.filePath = path.resolve(filePath);
    this.operationId = operationId;
    this.sequence = sequence;
  }

  static async create(snapshotDir, details) {
    const operationId = details.operationId ?? randomUUID();
    const filePath = path.join(snapshotDir, RESTORE_JOURNAL_BASENAME);
    const event = {
      ...details,
      schemaVersion: RESTORE_JOURNAL_SCHEMA_VERSION,
      protocolVersion: RESTORE_JOURNAL_SCHEMA_VERSION,
      operationKind: "restore",
      operationId,
      sequence: 1,
      state: "prepared",
      recordedAt: new Date().toISOString()
    };
    if (!validPrepared(event)) {
      throw new Error("Restore journal prepared payload is invalid.");
    }
    const handle = await fs.open(filePath, "wx", 0o600);
    try {
      await handle.writeFile(`${JSON.stringify(event)}\n`, "utf8");
      await handle.sync();
    } finally {
      await handle.close();
    }
    await syncDirectory(path.dirname(filePath));
    return new RestoreJournal(filePath, operationId, 1);
  }

  async append(state, details = {}) {
    const event = {
      ...details,
      schemaVersion: RESTORE_JOURNAL_SCHEMA_VERSION,
      protocolVersion: RESTORE_JOURNAL_SCHEMA_VERSION,
      operationKind: "restore",
      operationId: this.operationId,
      sequence: this.sequence + 1,
      state,
      recordedAt: new Date().toISOString()
    };
    try {
      await appendDurableJsonLine(this.filePath, event);
      this.sequence = event.sequence;
    } catch (error) {
      try {
        const current = await readRestoreJournal(this.filePath);
        const last = current.events.at(-1) ?? null;
        if (!current.invalidTail && JSON.stringify(last) === JSON.stringify(event)) {
          this.sequence = event.sequence;
        } else if (!current.invalidTail) {
          this.sequence = last?.sequence ?? this.sequence;
        }
      } catch {
        // Preserve the original durability failure.
      }
      throw error;
    }
  }

  async applying() {
    await this.append("applying");
  }

  async targetIntent(targetId) {
    await this.append("applying", { targetId, targetPhase: "intent" });
  }

  async targetCompleted(targetId, targetDigest) {
    await this.append("applying", { targetId, targetPhase: "completed", targetDigest });
  }

  async committing(postManifestSha256) {
    await this.append("committing", { postManifestSha256 });
  }

  async committedPendingAck(postManifestSha256) {
    await this.append("committed-pending-ack", { postManifestSha256 });
  }

  async completed() {
    await this.append("completed");
  }

  async rollbackPending(reasonCode = "restore-failed") {
    await this.append("rollback-pending", { reasonCode });
  }

  async targetCompensated(targetId, targetDigest) {
    await this.append("rollback-pending", {
      targetId,
      targetPhase: "compensated",
      targetDigest
    });
  }

  async rolledBack() {
    await this.append("rolled-back");
  }

  async recoveryRequired(reasonCode = "evidence-unverifiable") {
    await this.append("recovery-required", { reasonCode });
  }
}

export function reopenRestoreJournal(snapshot) {
  return new RestoreJournal(
    snapshot.filePath,
    snapshot.operationId,
    snapshot.events.at(-1)?.sequence ?? 0
  );
}

export async function findRestoreJournals(codexHome) {
  const root = defaultBackupRoot(codexHome);
  let entries;
  try {
    entries = await fs.readdir(root, { withFileTypes: true });
  } catch (error) {
    if (error?.code === "ENOENT") return [];
    throw error;
  }
  const journals = [];
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const filePath = path.join(root, entry.name, RESTORE_JOURNAL_BASENAME);
    try {
      journals.push(await readRestoreJournal(filePath));
    } catch (error) {
      if (error?.code !== "ENOENT") {
        journals.push({
          filePath,
          snapshotDir: path.dirname(filePath),
          backupDir: path.dirname(filePath),
          operationKind: "restore",
          events: [],
          operationId: null,
          prepared: null,
          state: "recovery-required",
          invalidTail: true,
          validationError: error instanceof Error ? error.message : String(error),
          terminal: false,
          blocking: true,
          targetPhases: new Map(),
          protectionReferences: null,
          protectionReferencesUnverifiable: true,
          rawText: ""
        });
      }
    }
  }
  return journals.sort((left, right) => left.filePath.localeCompare(right.filePath));
}

export async function findBlockingRestoreJournals(codexHome) {
  const journals = await findRestoreJournals(codexHome);
  const journalsByOperationId = new Map();
  for (const journal of journals) {
    if (!journal.operationId) continue;
    const matches = journalsByOperationId.get(journal.operationId) ?? [];
    matches.push(journal);
    journalsByOperationId.set(journal.operationId, matches);
  }
  const resolvedOperationIds = new Set();
  for (const resolver of journals) {
    if (resolver.invalidTail || resolver.state !== "completed" || !resolver.prepared) continue;
    for (const operationId of resolver.prepared.resolvesOperationIds ?? []) {
      const matches = journalsByOperationId.get(operationId) ?? [];
      if (matches.length !== 1) continue;
      const pending = matches[0];
      const resolverSource = resolver.prepared.sourceBackup;
      const pendingSource = pending.prepared?.sourceBackup;
      const resolverKinds = new Set(resolver.prepared.requiredTargetKinds ?? []);
      const pendingKinds = pending.prepared?.requiredTargetKinds;
      const [resolverSourceKey, pendingSourceKey, resolverHomeKey, pendingHomeKey] = pendingSource
        && pending.prepared
        ? await Promise.all([
            physicalPathKey(resolverSource.backupDir),
            physicalPathKey(pendingSource.backupDir),
            physicalPathKey(resolver.prepared.storage.codexHome),
            physicalPathKey(pending.prepared.storage.codexHome)
          ])
        : [null, null, null, null];
      const resolverRecordedHomeKey = persistedPhysicalPathKey(
        resolver.prepared.storage.codexHomePhysical
      );
      const pendingRecordedHomeKey = persistedPhysicalPathKey(
        pending.prepared?.storage?.codexHomePhysical
      );
      const sameSource = pendingSource
        && resolverSourceKey !== null
        && resolverSourceKey === pendingSourceKey
        && resolverSource.revision === pendingSource.revision;
      const sameHome = pending.prepared
        && resolverHomeKey !== null
        && pendingHomeKey !== null
        && resolverRecordedHomeKey !== null
        && pendingRecordedHomeKey !== null
        && resolverHomeKey === pendingHomeKey
        && resolverHomeKey === resolverRecordedHomeKey
        && pendingHomeKey === pendingRecordedHomeKey;
      const completeCoverage = Array.isArray(pendingKinds)
        && pendingKinds.every((kind) => resolverKinds.has(kind));
      if (pending.blocking && !pending.invalidTail && sameSource && sameHome && completeCoverage) {
        resolvedOperationIds.add(operationId);
      }
    }
  }
  return journals.filter((journal) =>
    journal.blocking
    && !(journal.operationId && resolvedOperationIds.has(journal.operationId))
  );
}

async function physicalPathKey(value) {
  try {
    const lexical = path.resolve(value);
    const first = path.resolve(await fs.realpath(lexical));
    const stat = await fs.stat(first);
    const second = path.resolve(await fs.realpath(lexical));
    if (!stat.isDirectory() || persistedPhysicalPathKey(first) !== persistedPhysicalPathKey(second)) {
      return null;
    }
    return persistedPhysicalPathKey(first);
  } catch {
    return null;
  }
}

function persistedPhysicalPathKey(value) {
  try {
    if (typeof value !== "string" || !path.isAbsolute(value)) return null;
    const resolved = path.resolve(value);
    return process.platform === "win32" ? resolved.toLowerCase() : resolved;
  } catch {
    return null;
  }
}
