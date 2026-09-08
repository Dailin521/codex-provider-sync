import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { inspectHistoryIntegrity } from "../src/history-integrity.js";

const IDS = Object.freeze({
  default: "11111111-1111-4111-8111-111111111111",
  gap: "22222222-2222-4222-8222-222222222222",
  tail: "33333333-3333-4333-8333-333333333333",
  active: "44444444-4444-4444-8444-444444444444",
  large: "55555555-5555-4555-8555-555555555555"
});

async function fixture(lines, name = "rollout-integrity.jsonl") {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "codex-history-integrity-"));
  const file = path.join(home, "sessions", "2026", "09", "04", name);
  await fs.mkdir(path.dirname(file), { recursive: true });
  await fs.writeFile(file, lines.join("\n") + "\n", "utf8");
  return { home, file };
}

function meta(id = IDS.default) {
  return JSON.stringify({ type: "session_meta", payload: { id } });
}

test("integrity scanner reports bounded sanitized JSONL findings", async () => {
  const malformedSecret = "synthetic-malformed-token-must-not-leak";
  const { home, file } = await fixture([
    meta(),
    JSON.stringify({ type: "event_msg", ordinal: 8, payload: { message: "never returned" } }),
    `{"payload":{"message":"${malformedSecret}`,
    JSON.stringify({ type: "event_msg", ordinal: 8 }),
    JSON.stringify({ type: "event_msg", ordinal: 4 })
  ]);
  try {
    const before = await fs.readFile(file);
    const result = await inspectHistoryIntegrity(home, { maxIssues: 2 });
    assert.equal(result.outcome, "findings-and-inconclusive");
    assert.equal(result.counts.jsonCorruptRecords, 1);
    assert.equal(result.counts.duplicateOrdinals, 1);
    assert.equal(result.counts.outOfOrderOrdinals, 1);
    assert.equal(result.counts.filesScanned, 1);
    assert.equal(result.displayIndex.status, "unsupported");
    assert.equal(result.issues.length, 2);
    assert.equal(result.issuesTruncated, true);
    assert.deepEqual(Object.keys(result.issues[0]).sort(), ["code", "line", "scope", "sessionId"]);
    assert.equal(result.issues[0].sessionId, IDS.default);
    assert.equal(JSON.stringify(result).includes("never returned"), false);
    assert.equal(JSON.stringify(result).includes(malformedSecret), false);
    assert.deepEqual(await fs.readFile(file), before);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("integrity scanner treats ordinal starts and gaps as observations, not corruption", async () => {
  const { home } = await fixture([
    meta("one"),
    JSON.stringify({ type: "event_msg", ordinal: 30 }),
    JSON.stringify({ type: "event_msg", ordinal: 50 })
  ]);
  try {
    const result = await inspectHistoryIntegrity(home);
    assert.equal(result.outcome, "no-findings");
    assert.equal(result.counts.sessionsWithId, 1);
    assert.equal(result.counts.duplicateOrdinals, 0);
    assert.equal(result.counts.outOfOrderOrdinals, 0);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("integrity scanner treats a valid unterminated JSONL tail as inconclusive without leaking it", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "codex-history-integrity-tail-"));
  const file = path.join(home, "sessions", "rollout-tail.jsonl");
  const secret = "synthetic-body-must-not-leak";
  try {
    await fs.mkdir(path.dirname(file), { recursive: true });
    await fs.writeFile(file, `${meta(IDS.tail)}\n${JSON.stringify({ ordinal: 9, payload: { message: secret } })}`, "utf8");
    const result = await inspectHistoryIntegrity(home);
    assert.equal(result.outcome, "inconclusive");
    assert.equal(result.counts.jsonCorruptRecords, 0);
    assert.equal(result.counts.truncatedFiles, 1);
    assert.ok(result.issues.some((issue) => issue.code === "unterminated-record"));
    assert.equal(JSON.stringify(result).includes(secret), false);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("integrity scanner reports invalid UTF-8 without replacement decoding", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "codex-history-integrity-utf8-"));
  const file = path.join(home, "sessions", "rollout-invalid-utf8.jsonl");
  try {
    await fs.mkdir(path.dirname(file), { recursive: true });
    await fs.writeFile(file, Buffer.concat([Buffer.from(`${meta("test")}\n`, "utf8"), Buffer.from([0xff, 0x0a])]));
    const result = await inspectHistoryIntegrity(home);
    assert.equal(result.outcome, "findings");
    assert.equal(result.counts.jsonCorruptRecords, 1);
    assert.ok(result.issues.some((issue) => issue.code === "invalid-utf8" && issue.sessionId === "test"));
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("integrity scanner is inconclusive when a rollout changes during its bounded read", async () => {
  const { home, file } = await fixture([meta(IDS.active), JSON.stringify({ ordinal: 1 })]);
  try {
    const result = await inspectHistoryIntegrity(home, {
      testHooks: {
        async beforeFinalCheck() {
          await fs.appendFile(file, `${JSON.stringify({ ordinal: 2 })}\n`, "utf8");
        }
      }
    });
    assert.equal(result.outcome, "inconclusive");
    assert.equal(result.counts.changedFiles, 1);
    assert.ok(result.issues.some((issue) => issue.code === "changed-during-scan" && issue.sessionId === IDS.active));
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("integrity scanner bounds oversized lines and skips links without following them", async (t) => {
  const { home } = await fixture([meta(IDS.large), JSON.stringify({ payload: { body: "x".repeat(200) } })]);
  const external = await fs.mkdtemp(path.join(os.tmpdir(), "codex-history-integrity-external-"));
  try {
    const link = path.join(home, "sessions", "linked");
    try {
      await fs.symlink(external, link, "junction");
    } catch (error) {
      if (error?.code === "EPERM" || error?.code === "EACCES") {
        t.skip("link creation is unavailable on this host");
        return;
      }
      else throw error;
    }
    const result = await inspectHistoryIntegrity(home, { maxLineBytes: 128 });
    assert.equal(result.outcome, "inconclusive");
    assert.equal(result.counts.oversizedRecords, 1);
    assert.ok(result.skipped.symlinkOrReparse >= 1);
    assert.ok(result.issues.every((issue) => !Object.values(issue).some((value) => String(value).includes(external))));
  } finally {
    await fs.rm(home, { recursive: true, force: true });
    await fs.rm(external, { recursive: true, force: true });
  }
});

test("integrity scanner marks missing or non-header formats as unsupported instead of healthy", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "codex-history-integrity-format-"));
  const empty = path.join(home, "sessions", "rollout-empty.jsonl");
  const injected = path.join(home, "sessions", "rollout-injected.jsonl");
  const untyped = path.join(home, "sessions", "rollout-untyped.jsonl");
  try {
    await fs.mkdir(path.dirname(empty), { recursive: true });
    await fs.writeFile(empty, "", "utf8");
    await fs.writeFile(injected, [
      JSON.stringify({ type: "event_msg", ordinal: 1 }),
      meta(IDS.default),
      JSON.stringify({ ordinal: 2 })
    ].join("\n") + "\n", "utf8");
    await fs.writeFile(untyped, `${meta(IDS.default)}\n${JSON.stringify({ unexpected: true })}\n`, "utf8");
    const result = await inspectHistoryIntegrity(home);
    assert.equal(result.outcome, "inconclusive");
    assert.equal(result.counts.unsupportedFiles, 3);
    assert.equal(result.counts.sessionsWithId, 1);
    assert.equal(result.issues.filter((issue) => issue.code === "unsupported-format").length, 3);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("integrity scanner applies one global file budget across active and archived roots", async () => {
  const { home } = await fixture([meta(IDS.default)]);
  const archived = path.join(home, "archived_sessions", "rollout-archived.jsonl");
  try {
    await fs.mkdir(path.dirname(archived), { recursive: true });
    await fs.writeFile(archived, `${meta(IDS.gap)}\n`, "utf8");
    const result = await inspectHistoryIntegrity(home, { maxFiles: 1 });
    assert.equal(result.counts.filesDiscovered, 2);
    assert.equal(result.counts.filesScanned, 1);
    assert.equal(result.skipped.scanLimit, 1);
    assert.equal(result.outcome, "inconclusive");
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("integrity scanner reports record limits and accepts missing session directories", async () => {
  const { home } = await fixture([meta(IDS.default), JSON.stringify({ ordinal: 1 }), JSON.stringify({ ordinal: 2 })]);
  const emptyHome = await fs.mkdtemp(path.join(os.tmpdir(), "codex-history-integrity-missing-"));
  try {
    const limited = await inspectHistoryIntegrity(home, { maxRecordsPerFile: 1 });
    assert.equal(limited.outcome, "inconclusive");
    assert.equal(limited.counts.truncatedFiles, 1);
    assert.ok(limited.issues.some((issue) => issue.code === "record-limit-reached"));
    const missing = await inspectHistoryIntegrity(emptyHome);
    assert.equal(missing.outcome, "no-findings");
    assert.equal(missing.counts.filesDiscovered, 0);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
    await fs.rm(emptyHome, { recursive: true, force: true });
  }
});
