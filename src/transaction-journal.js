import fs from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";

import { defaultBackupRoot } from "./constants.js";

export const TRANSACTION_JOURNAL_BASENAME = "transaction-journal.jsonl";
const TERMINAL_STATES = new Set(["committed", "rolledBack"]);

async function appendDurableJsonLine(filePath, value) {
  const handle = await fs.open(filePath, "a");
  try {
    await handle.writeFile(`${JSON.stringify(value)}\n`, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
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
    await journal.append("prepared", {
      protocolVersion: 1,
      backupDir: path.resolve(backupDir),
      codexHome: path.resolve(details.codexHome),
      targetProvider: details.targetProvider,
      potentialTargets: [...new Set(details.potentialTargets.map((value) => path.resolve(value)))].sort()
    });
    return journal;
  }

  async append(state, details = {}) {
    this.sequence += 1;
    await appendDurableJsonLine(this.filePath, {
      protocolVersion: 1,
      operationId: this.operationId,
      sequence: this.sequence,
      state,
      recordedAt: new Date().toISOString(),
      ...details
    });
  }

  async applying(kind, targetPath) {
    await this.append("applying", { kind, targetPath: path.resolve(targetPath) });
  }

  async applied(kind, targetPath) {
    await this.append("applied", { kind, targetPath: path.resolve(targetPath) });
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

export async function readTransactionJournal(filePath) {
  const text = await fs.readFile(filePath, "utf8");
  const events = [];
  let invalidTail = false;
  for (const line of text.split(/\r?\n/)) {
    if (!line.trim()) {
      continue;
    }
    try {
      events.push(JSON.parse(line));
    } catch {
      invalidTail = true;
      break;
    }
  }
  const lastEvent = events.at(-1) ?? null;
  return {
    filePath,
    events,
    invalidTail,
    operationId: events[0]?.operationId ?? null,
    backupDir: events[0]?.backupDir ?? path.dirname(filePath),
    state: invalidTail ? "recoveryRequired" : (lastEvent?.state ?? "recoveryRequired"),
    terminal: !invalidTail && TERMINAL_STATES.has(lastEvent?.state)
  };
}

export async function findPendingTransactions(codexHome) {
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
          state: "recoveryRequired",
          terminal: false,
          readError: error.message
        });
      }
    }
  }
  return pending.sort((left, right) => left.filePath.localeCompare(right.filePath));
}

export class RecoveryRequiredError extends Error {
  constructor(pendingTransactions) {
    const backups = pendingTransactions.map((item) => item.backupDir).join(", ");
    super(`An unfinished provider-sync transaction requires recovery before another write. Restore the bound backup, then retry. Backup(s): ${backups}`);
    this.name = "RecoveryRequiredError";
    this.code = "RECOVERY_REQUIRED";
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
    const journal = new TransactionJournal(
      filePath,
      current.operationId ?? randomUUID(),
      current.events?.at(-1)?.sequence ?? 0
    );
    await journal.rolledBack();
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }
}
