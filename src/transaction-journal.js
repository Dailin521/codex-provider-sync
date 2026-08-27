import fs from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";

import { defaultBackupRoot } from "./constants.js";
import { writeFileAtomic, syncDirectory } from "./atomic-file.js";
import { CoreError } from "./core-error.js";
import { findBlockingRestoreJournals } from "./restore-journal.js";

export const TRANSACTION_JOURNAL_BASENAME = "transaction-journal.jsonl";
const TERMINAL_STATES = new Set(["committed", "rolledBack"]);
const TARGET_STATES = new Set(["applying", "applied", "skipped"]);
const VALID_STATES = new Set([
  "prepared",
  ...TARGET_STATES,
  "committed",
  "rollingBack",
  "rolledBack",
  "recoveryRequired"
]);
const VALID_TARGET_KINDS = new Set(["config", "rollout", "globalState", "sqlite"]);

function pathKey(value) {
  const resolved = path.resolve(value);
  return process.platform === "win32" ? resolved.toLowerCase() : resolved;
}

function targetKey(kind, targetPath) {
  return `${kind}\0${pathKey(targetPath)}`;
}

async function appendDurableJsonLine(filePath, value) {
  let existed = true;
  try {
    await fs.access(filePath);
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
    existed = false;
  }
  const handle = await fs.open(filePath, "a");
  try {
    await handle.writeFile(`${JSON.stringify(value)}\n`, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  if (!existed) {
    await syncDirectory(path.dirname(filePath));
  }
}

export class TransactionJournal {
  constructor(filePath, operationId, sequence = 0) {
    this.filePath = filePath;
    this.operationId = operationId;
    this.sequence = sequence;
  }

  static async create(backupDir, details) {
    const operationId = randomUUID();
    const filePath = path.join(backupDir, TRANSACTION_JOURNAL_BASENAME);
    const journal = new TransactionJournal(filePath, operationId);
    const potentialTargetsByKey = new Map();
    for (const value of details.potentialTargets) {
      const resolved = path.resolve(value);
      potentialTargetsByKey.set(pathKey(resolved), resolved);
    }
    const prepared = {
      protocolVersion: 1,
      operationId,
      sequence: 1,
      state: "prepared",
      recordedAt: new Date().toISOString(),
      backupDir: path.resolve(backupDir),
      codexHome: path.resolve(details.codexHome),
      targetProvider: details.targetProvider,
      potentialTargets: [...potentialTargetsByKey.values()].sort()
    };
    const exclusive = await fs.open(filePath, "wx", 0o600);
    try {
      await exclusive.writeFile(`${JSON.stringify(prepared)}\n`, "utf8");
      await exclusive.sync();
    } finally {
      await exclusive.close();
    }
    await syncDirectory(path.dirname(filePath));
    journal.sequence = 1;
    return journal;
  }

  async append(state, details = {}) {
    const nextSequence = this.sequence + 1;
    const event = {
      ...details,
      protocolVersion: 1,
      operationId: this.operationId,
      sequence: nextSequence,
      state,
      recordedAt: new Date().toISOString()
    };
    try {
      await appendDurableJsonLine(this.filePath, event);
      this.sequence = nextSequence;
    } catch (error) {
      // A host may report a flush error after the bytes reached the file. Keep
      // subsequent records sequence-compatible with the durable prefix while
      // still surfacing the original durability failure to the coordinator.
      try {
        const current = await readTransactionJournal(this.filePath);
        const lastEvent = current.events.at(-1) ?? null;
        if (!current.invalidTail
            && lastEvent !== null
            && JSON.stringify(lastEvent) === JSON.stringify(event)) {
          this.sequence = nextSequence;
        } else if (!current.invalidTail) {
          this.sequence = lastEvent?.sequence ?? this.sequence;
        }
      } catch {
        // The original append error remains authoritative.
      }
      throw error;
    }
  }

  async applying(kind, targetPath) {
    await this.append("applying", { kind, targetPath: path.resolve(targetPath) });
  }

  async applied(kind, targetPath) {
    await this.append("applied", { kind, targetPath: path.resolve(targetPath) });
  }

  async skipped(kind, targetPath) {
    await this.append("skipped", { kind, targetPath: path.resolve(targetPath) });
  }

  async committed() {
    await this.append("committed");
  }

  async rollingBack(originalError) {
    await this.append("rollingBack", { originalError: String(originalError?.message ?? originalError) });
  }

  async rolledBack() {
    await this.append("rolledBack");
  }

  async recoveryRequired(originalError, rollbackErrors) {
    await this.append("recoveryRequired", {
      originalError: String(originalError?.message ?? originalError),
      rollbackErrors: rollbackErrors.map(String)
    });
  }
}

function validateJournalEvents(parsedEvents) {
  const events = [];
  const activeTargets = new Set();
  let operationId = null;
  let expectedSequence = 1;
  let terminalSeen = false;
  let rollingBack = false;
  let recoveryRequiredSeen = false;
  let potentialTargets = null;
  let validationError = null;

  for (const event of parsedEvents) {
    const fail = (message) => {
      validationError = message;
      return false;
    };
    if (!event || typeof event !== "object" || Array.isArray(event)) {
      fail("Journal event is not an object.");
      break;
    }
    if (event.protocolVersion !== 1 || !VALID_STATES.has(event.state)) {
      fail("Journal event has an unsupported protocol or state.");
      break;
    }
    if (typeof event.operationId !== "string" || !event.operationId) {
      fail("Journal event is missing operationId.");
      break;
    }
    if (event.sequence !== expectedSequence) {
      fail(`Journal sequence mismatch: expected ${expectedSequence}, received ${event.sequence}.`);
      break;
    }
    if (operationId === null) {
      if (event.state !== "prepared") {
        fail("Journal must start with prepared.");
        break;
      }
      operationId = event.operationId;
      if (!Array.isArray(event.potentialTargets)
          || event.potentialTargets.some((value) => typeof value !== "string" || !path.isAbsolute(value))) {
        fail("Journal prepared event has invalid potentialTargets.");
        break;
      }
      potentialTargets = new Set(event.potentialTargets.map(pathKey));
      if (potentialTargets.size !== event.potentialTargets.length
          || typeof event.backupDir !== "string"
          || !path.isAbsolute(event.backupDir)
          || typeof event.codexHome !== "string"
          || !path.isAbsolute(event.codexHome)) {
        fail("Journal prepared event has duplicate targets or invalid absolute roots.");
        break;
      }
    } else if (event.operationId !== operationId) {
      fail("Journal operationId changed within one transaction.");
      break;
    } else if (event.state === "prepared") {
      fail("Journal contains more than one prepared event.");
      break;
    }
    if (terminalSeen) {
      fail("Journal contains events after a terminal state.");
      break;
    }

    if (recoveryRequiredSeen && event.state !== "rolledBack") {
      fail("Journal continued after recoveryRequired without an explicit restore.");
      break;
    }
    if (rollingBack
        && !new Set(["rollingBack", "recoveryRequired", "rolledBack"]).has(event.state)) {
      fail("Journal continued target work after rollback started.");
      break;
    }

    if (TARGET_STATES.has(event.state)) {
      if (rollingBack) {
        fail("Journal contains a target transition after rollback started.");
        break;
      }
      if (!VALID_TARGET_KINDS.has(event.kind)
          || typeof event.targetPath !== "string"
          || !path.isAbsolute(event.targetPath)) {
        fail("Journal target transition is malformed.");
        break;
      }
      if (!potentialTargets?.has(pathKey(event.targetPath))) {
        fail("Journal target is not declared in prepared.potentialTargets.");
        break;
      }
      const key = targetKey(event.kind, event.targetPath);
      if (event.state === "applying") {
        if (activeTargets.has(key)) {
          fail("Journal target was started twice without a result.");
          break;
        }
        activeTargets.add(key);
      } else if (!activeTargets.delete(key)) {
        fail(`Journal ${event.state} event has no matching applying event.`);
        break;
      }
    } else if (event.state === "rollingBack") {
      if (rollingBack) {
        fail("Journal contains duplicate rollingBack events.");
        break;
      }
      rollingBack = true;
    } else if (event.state === "recoveryRequired") {
      recoveryRequiredSeen = true;
    } else if (event.state === "committed") {
      if (rollingBack || activeTargets.size > 0) {
        fail("Journal committed with unresolved target transitions.");
        break;
      }
      terminalSeen = true;
    } else if (event.state === "rolledBack") {
      if (!rollingBack && !recoveryRequiredSeen) {
        fail("Journal rolledBack without first entering rollback or recoveryRequired.");
        break;
      }
      terminalSeen = true;
    }

    events.push(event);
    expectedSequence += 1;
  }

  return { events, operationId, validationError };
}

export async function readTransactionJournal(filePath) {
  const text = await fs.readFile(filePath, "utf8");
  const parsedEvents = [];
  let parseError = text.length > 0 && !text.endsWith("\n")
    ? "Journal is missing its final newline and may contain a torn append."
    : null;
  for (const line of text.split(/\r?\n/)) {
    if (!line.trim()) {
      continue;
    }
    try {
      parsedEvents.push(JSON.parse(line));
    } catch {
      parseError = "Journal contains a truncated or malformed JSON line.";
      break;
    }
  }
  const validated = validateJournalEvents(parsedEvents);
  const events = validated.events;
  const validationError = parseError ?? validated.validationError;
  const invalidTail = validationError !== null || events.length !== parsedEvents.length;
  const lastEvent = events.at(-1) ?? null;
  return {
    filePath,
    operationKind: "sync",
    events,
    invalidTail,
    validationError,
    operationId: validated.operationId,
    backupDir: path.dirname(filePath),
    recordedBackupDir: events[0]?.backupDir ?? null,
    state: invalidTail ? "recoveryRequired" : (lastEvent?.state ?? "recoveryRequired"),
    terminal: !invalidTail && TERMINAL_STATES.has(lastEvent?.state),
    rawText: text
  };
}

export function getJournalTargetStates(journal) {
  const targets = new Map();
  for (const event of journal?.events ?? []) {
    if (!TARGET_STATES.has(event.state)) {
      continue;
    }
    const key = targetKey(event.kind, event.targetPath);
    targets.set(key, {
      kind: event.kind,
      targetPath: path.resolve(event.targetPath),
      state: event.state,
      sequence: event.sequence
    });
  }
  return [...targets.values()];
}

export function getStartedJournalTargets(journal, kind) {
  return getJournalTargetStates(journal)
    .filter((target) => target.kind === kind && target.state !== "skipped")
    .map((target) => target.targetPath);
}

export function getAppliedJournalTargets(journal) {
  return getJournalTargetStates(journal)
    .filter((target) => target.state === "applied")
    .map((target) => target.targetPath);
}

export async function findLegacyPendingTransactions(codexHome) {
  const root = defaultBackupRoot(codexHome);
  let entries;
  try {
    entries = await fs.readdir(root, { withFileTypes: true });
  } catch (error) {
    if (error?.code === "ENOENT") {
      return [];
    }
    throw error;
  }

  const pending = [];
  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }
    const journalPath = path.join(root, entry.name, TRANSACTION_JOURNAL_BASENAME);
    try {
      const journal = await readTransactionJournal(journalPath);
      if (!journal.terminal) {
        pending.push(journal);
      }
    } catch (error) {
      if (error?.code !== "ENOENT") {
        pending.push({
          filePath: journalPath,
          backupDir: path.dirname(journalPath),
          operationKind: "sync",
          state: "recoveryRequired",
          terminal: false,
          readError: error.message
        });
      }
    }
  }
  return pending.sort((left, right) => left.filePath.localeCompare(right.filePath));
}

export async function findPendingTransactions(codexHome) {
  const [legacy, restore] = await Promise.all([
    findLegacyPendingTransactions(codexHome),
    findBlockingRestoreJournals(codexHome)
  ]);
  return [...legacy, ...restore]
    .sort((left, right) => left.filePath.localeCompare(right.filePath));
}

export class RecoveryRequiredError extends CoreError {
  constructor(pendingTransactions) {
    const backups = pendingTransactions.map((item) => item.backupDir).join(", ");
    super(
      "RECOVERY_REQUIRED",
      `An unfinished provider-sync transaction requires recovery before another write. Restore the bound backup, then retry. Backup(s): ${backups}`,
      { suggestedAction: "Restore the transaction-bound managed backup before starting another write." }
    );
    this.name = "RecoveryRequiredError";
    this.pendingTransactions = pendingTransactions;
  }
}

export async function assertNoPendingTransactions(codexHome) {
  const pending = await findPendingTransactions(codexHome);
  if (pending.length > 0) {
    throw new RecoveryRequiredError(pending);
  }
}

export async function markBackupTransactionRolledBack(backupDir) {
  const filePath = path.join(path.resolve(backupDir), TRANSACTION_JOURNAL_BASENAME);
  try {
    const current = await readTransactionJournal(filePath);
    if (current.terminal) {
      return;
    }
    if (current.invalidTail || current.events.length === 0) {
      const archivePath = path.join(
        path.dirname(filePath),
        `${TRANSACTION_JOURNAL_BASENAME}.invalid.${Date.now()}.${randomUUID()}`
      );
      await writeFileAtomic(archivePath, current.rawText, "utf8");

      let baseEvents = [];
      for (const event of current.events) {
        if (TERMINAL_STATES.has(event.state)) {
          break;
        }
        baseEvents.push(event);
      }
      if (baseEvents.length === 0) {
        let metadata = {};
        try {
          metadata = JSON.parse(await fs.readFile(path.join(backupDir, "metadata.json"), "utf8"));
        } catch {
          // A successful restore already validated metadata. This fallback is
          // only for a journal whose first record itself was damaged.
        }
        const operationId = randomUUID();
        baseEvents = [{
          protocolVersion: 1,
          operationId,
          sequence: 1,
          state: "prepared",
          recordedAt: new Date().toISOString(),
          backupDir: path.resolve(backupDir),
          codexHome: metadata.codexHome ? path.resolve(metadata.codexHome) : null,
          targetProvider: metadata.targetProvider ?? null,
          potentialTargets: []
        }];
      }
      const operationId = baseEvents[0].operationId;
      if (!new Set(["rollingBack", "recoveryRequired"]).has(baseEvents.at(-1)?.state)) {
        baseEvents.push({
          protocolVersion: 1,
          operationId,
          sequence: baseEvents.length + 1,
          state: "rollingBack",
          recordedAt: new Date().toISOString(),
          originalError: "Explicit restore repaired an invalid transaction journal."
        });
      }
      const rolledBack = {
        protocolVersion: 1,
        operationId,
        sequence: baseEvents.length + 1,
        state: "rolledBack",
        recordedAt: new Date().toISOString(),
        recoveredInvalidJournal: true,
        invalidJournalArchive: path.basename(archivePath)
      };
      await writeFileAtomic(
        filePath,
        `${[...baseEvents, rolledBack].map((event) => JSON.stringify(event)).join("\n")}\n`,
        "utf8"
      );
      const verified = await readTransactionJournal(filePath);
      if (!verified.terminal || verified.state !== "rolledBack") {
        throw new Error(`Transaction journal did not persist a valid rolledBack terminal state: ${filePath}`);
      }
      return;
    }
    const journal = new TransactionJournal(
      filePath,
      current.operationId ?? randomUUID(),
      current.events?.at(-1)?.sequence ?? 0
    );
    if (!new Set(["rollingBack", "recoveryRequired"]).has(current.state)) {
      await journal.rollingBack(new Error("Explicit restore completed."));
    }
    await journal.rolledBack();
    const verified = await readTransactionJournal(filePath);
    if (!verified.terminal || verified.state !== "rolledBack") {
      throw new Error(`Transaction journal did not persist a valid rolledBack terminal state: ${filePath}`);
    }
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }
}
