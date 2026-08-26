import crypto from "node:crypto";
import fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";
import readline from "node:readline";

import { SESSION_DIRS } from "./constants.js";
import { CoreError } from "./core-error.js";

const DEFAULT_PAGE_SIZE = 50;
const MAX_PAGE_SIZE = 100;
const DEFAULT_MESSAGE_LIMIT = 200;

function historyFileError(error, action) {
  if (error?.code === "EACCES" || error?.code === "EPERM") {
    return new CoreError("PERMISSION_DENIED", `Permission denied while ${action}.`, {
      cause: error,
      details: { causeCode: error.code }
    });
  }
  return error;
}

function normalizedRolloutPath(rolloutPath) {
  const absolutePath = path.resolve(rolloutPath);
  return process.platform === "win32" ? absolutePath.toLowerCase() : absolutePath;
}

function fallbackSessionId(rolloutPath) {
  const digest = crypto.createHash("sha256")
    .update(normalizedRolloutPath(rolloutPath), "utf8")
    .digest("base64url");
  return `rollout:${digest}`;
}

function normalizeText(value) {
  if (typeof value !== "string") return "";
  return value.replace(/\r\n/g, "\n").trim();
}

function firstText(...values) {
  for (const value of values) {
    const text = normalizeText(value);
    if (text) return text;
  }
  return "";
}

function contentText(value) {
  if (typeof value === "string") return normalizeText(value);
  if (!Array.isArray(value)) return "";
  return value
    .filter((item) => item && typeof item === "object" && (item.type === "output_text" || item.type === "text" || item.type === "input_text"))
    .map((item) => normalizeText(item.text))
    .filter(Boolean)
    .join("\n");
}

function messageFromRecord(record) {
  if (!record || typeof record !== "object") return null;
  const timestamp = record.timestamp ?? record.payload?.timestamp ?? null;
  const eventType = record.payload?.type;
  if (record.type === "event_msg" && (eventType === "user_message" || eventType === "assistant_message")) {
    const role = eventType === "user_message" ? "user" : "assistant";
    const text = firstText(record.payload?.message, record.payload?.text);
    return text ? { role, text, timestamp, canonicalUser: role === "user" } : null;
  }

  for (const key of ["payload", "item", "msg"]) {
    const value = record[key];
    if (!value || typeof value !== "object" || !["user", "assistant"].includes(value.role)) continue;
    const text = firstText(contentText(value.content), value.message, value.text);
    return text ? { role: value.role, text, timestamp, canonicalUser: false } : null;
  }
  if (record.type === "user_message" || record.type === "assistant_message") {
    const text = firstText(record.message, record.text, record.payload?.message, record.payload?.text);
    const role = record.type === "user_message" ? "user" : "assistant";
    return text ? { role, text, timestamp, canonicalUser: role === "user" } : null;
  }
  return null;
}

async function listRolloutFiles(root) {
  const result = [];
  async function walk(directory) {
    let entries;
    try {
      entries = await fs.readdir(directory, { withFileTypes: true });
    } catch (error) {
      if (error?.code === "ENOENT") return;
      throw historyFileError(error, "scanning history rollouts");
    }
    for (const entry of entries) {
      const fullPath = path.join(directory, entry.name);
      if (entry.isDirectory()) await walk(fullPath);
      else if (entry.isFile() && entry.name.startsWith("rollout-") && entry.name.endsWith(".jsonl")) result.push(fullPath);
    }
  }
  await walk(root);
  return result;
}

async function readRollout(filePath, archived) {
  const stat = await fs.stat(filePath);
  const stream = fsSync.createReadStream(filePath, { encoding: "utf8" });
  const lines = readline.createInterface({ input: stream, crlfDelay: Infinity });
  let meta = null;
  const messages = [];
  let sequence = 0;
  try {
    for await (const line of lines) {
      if (!line.trim()) continue;
      let record;
      try { record = JSON.parse(line); } catch { continue; }
      if (!meta && record.type === "session_meta" && record.payload && typeof record.payload === "object") {
        const payload = record.payload;
        meta = {
          threadId: typeof payload.id === "string" && payload.id ? payload.id : null,
          title: firstText(payload.title, payload.name),
          cwd: firstText(payload.cwd),
          provider: firstText(payload.model_provider) || "(missing)",
          model: firstText(payload.model),
          createdAt: record.timestamp ?? payload.timestamp ?? null
        };
      }
      const message = messageFromRecord(record);
      if (message) messages.push({ ...message, sequence: ++sequence });
    }
  } finally {
    lines.close();
    stream.destroy();
  }
  if (!meta) return null;
  const hasCanonicalUserMessages = messages.some((message) => message.canonicalUser);
  const visibleMessages = messages
    .filter((message) => message.role !== "user" || !hasCanonicalUserMessages || message.canonicalUser)
    .map(({ canonicalUser: _canonicalUser, sequence: _sequence, ...message }, index) => ({ ...message, sequence: index + 1 }));
  const rolloutPath = path.resolve(filePath);
  const updatedAt = visibleMessages.at(-1)?.timestamp ?? stat.mtime.toISOString();
  return {
    ...meta,
    id: meta.threadId ?? fallbackSessionId(rolloutPath),
    rolloutPath,
    updatedAt,
    archived,
    messages: visibleMessages,
    messageCount: visibleMessages.length,
    filePath,
    mtimeMs: stat.mtimeMs
  };
}

async function collectHistory(codexHome) {
  const sessions = [];
  for (const dirName of SESSION_DIRS) {
    const files = await listRolloutFiles(path.join(codexHome, dirName));
    for (const filePath of files) {
      let session;
      try {
        session = await readRollout(filePath, dirName === "archived_sessions");
      } catch (error) {
        if (error?.code === "ENOENT") continue;
        throw historyFileError(error, "reading a history rollout");
      }
      if (session) sessions.push(session);
    }
  }
  const byId = new Map();
  for (const session of sessions) {
    const key = session.threadId
      ? `thread:${session.threadId}`
      : `path:${normalizedRolloutPath(session.rolloutPath)}`;
    const existing = byId.get(key);
    if (!existing || session.mtimeMs >= existing.mtimeMs) byId.set(key, session);
  }
  return [...byId.values()].sort((a, b) => Date.parse(b.updatedAt || 0) - Date.parse(a.updatedAt || 0) || b.mtimeMs - a.mtimeMs);
}

function publicSession(session) {
  return {
    id: session.id,
    rolloutPath: session.rolloutPath,
    title: session.title || "",
    cwd: session.cwd,
    provider: session.provider,
    model: session.model,
    archived: session.archived,
    createdAt: session.createdAt,
    updatedAt: session.updatedAt,
    messageCount: session.messageCount
  };
}

export function validateHistoryPage(pageValue, pageSizeValue = DEFAULT_PAGE_SIZE) {
  const page = pageValue === undefined ? 1 : Number(pageValue);
  const pageSize = pageSizeValue === undefined ? DEFAULT_PAGE_SIZE : Number(pageSizeValue);
  if (!Number.isInteger(page) || page < 1) {
    throw new CoreError("INVALID_INPUT", "page must be a positive integer.");
  }
  if (!Number.isInteger(pageSize) || pageSize < 10 || pageSize > MAX_PAGE_SIZE) {
    throw new CoreError(
      "INVALID_INPUT",
      `pageSize must be an integer between 10 and ${MAX_PAGE_SIZE}.`
    );
  }
  return { page, pageSize };
}

export async function listHistory(codexHome, options = {}) {
  const { page, pageSize } = validateHistoryPage(options.page, options.pageSize ?? DEFAULT_PAGE_SIZE);
  const query = normalizeText(options.query).toLowerCase();
  const project = normalizeText(options.project).toLowerCase();
  const provider = normalizeText(options.provider);
  const archived = options.archived ?? "all";
  if (!["all", "active", "archived"].includes(archived)) {
    throw new CoreError("INVALID_INPUT", "archived must be all, active, or archived.");
  }
  const sessions = await collectHistory(codexHome);
  const filtered = sessions.filter((session) => {
    if (provider && session.provider !== provider) return false;
    if (archived !== "all" && session.archived !== (archived === "archived")) return false;
    if (project && !session.cwd.toLowerCase().includes(project)) return false;
    if (query) {
      const haystack = [session.title, session.cwd, session.provider, session.messages[0]?.text, ...session.messages.map((message) => message.text)].join("\n").toLowerCase();
      if (!haystack.includes(query)) return false;
    }
    return true;
  });
  const start = (page - 1) * pageSize;
  return { page, pageSize, total: filtered.length, hasNextPage: start + pageSize < filtered.length, sessions: filtered.slice(start, start + pageSize).map(publicSession) };
}

export async function getHistorySession(codexHome, sessionId, { messageLimit = DEFAULT_MESSAGE_LIMIT } = {}) {
  if (typeof sessionId !== "string" || !sessionId.trim()) {
    throw new CoreError("INVALID_INPUT", "sessionId is required.");
  }
  const sessions = await collectHistory(codexHome);
  const session = sessions.find((item) => item.id === sessionId);
  if (!session) {
    throw new CoreError(
      "INVALID_INPUT",
      "The selected session was not found in this Codex Home."
    );
  }
  const safeLimit = Number.isInteger(messageLimit) && messageLimit > 0 ? Math.min(messageLimit, DEFAULT_MESSAGE_LIMIT) : DEFAULT_MESSAGE_LIMIT;
  const messages = session.messages.slice(-safeLimit);
  return { session: publicSession(session), messages, truncated: messages.length < session.messages.length, returnedMessageCount: messages.length };
}
