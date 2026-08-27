import crypto from "node:crypto";
import fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";

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

function pathKey(value) {
  const resolved = path.resolve(value);
  return process.platform === "win32" ? resolved.toLowerCase() : resolved;
}

function isWithinRoot(root, candidate) {
  const relative = path.relative(root, candidate);
  return relative === "" || (!path.isAbsolute(relative)
    && relative !== ".."
    && !relative.startsWith(`..${path.sep}`));
}

function fileIdentity(stat) {
  return {
    dev: String(stat.dev),
    ino: String(stat.ino),
    size: String(stat.size),
    mtimeNs: String(stat.mtimeNs),
    ctimeNs: String(stat.ctimeNs),
    birthtimeNs: String(stat.birthtimeNs)
  };
}

function sameFileObject(left, right) {
  return left.dev === right.dev
    && left.ino === right.ino
    && left.birthtimeNs === right.birthtimeNs;
}

function sameFileIdentity(left, right) {
  return sameFileObject(left, right)
    && left.size === right.size
    && left.mtimeNs === right.mtimeNs
    && left.ctimeNs === right.ctimeNs;
}

function staleHistoryError(cause) {
  return new CoreError(
    "STALE_STATE",
    "The selected session changed before its messages could be read.",
    { cause: cause instanceof Error ? cause : undefined, details: { reason: "history-rollout" } }
  );
}

async function* readHandleLines(handle) {
  const buffer = Buffer.allocUnsafe(64 * 1024);
  const decoder = new TextDecoder();
  let pending = "";
  let position = 0;
  while (true) {
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, position);
    if (bytesRead === 0) break;
    position += bytesRead;
    pending += decoder.decode(buffer.subarray(0, bytesRead), { stream: true });
    let newline;
    while ((newline = pending.indexOf("\n")) >= 0) {
      const line = pending.slice(0, newline);
      pending = pending.slice(newline + 1);
      yield line.endsWith("\r") ? line.slice(0, -1) : line;
    }
  }
  pending += decoder.decode();
  if (pending) yield pending.endsWith("\r") ? pending.slice(0, -1) : pending;
}

function hasText(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function hasContentText(value) {
  if (hasText(value)) return true;
  if (!Array.isArray(value)) return false;
  return value.some((item) => item
    && typeof item === "object"
    && (item.type === "output_text" || item.type === "text" || item.type === "input_text")
    && hasText(item.text));
}

function messageFromRecord(record, { includeText = true } = {}) {
  if (!record || typeof record !== "object") return null;
  const timestamp = record.timestamp ?? record.payload?.timestamp ?? null;
  const eventType = record.payload?.type;
  if (record.type === "event_msg" && (eventType === "user_message" || eventType === "assistant_message")) {
    const role = eventType === "user_message" ? "user" : "assistant";
    const values = [record.payload?.message, record.payload?.text];
    if (!values.some(hasText)) return null;
    return {
      role,
      ...(includeText ? { text: firstText(...values) } : {}),
      timestamp,
      canonicalUser: role === "user"
    };
  }

  for (const key of ["payload", "item", "msg"]) {
    const value = record[key];
    if (!value || typeof value !== "object" || !["user", "assistant"].includes(value.role)) continue;
    if (!hasContentText(value.content) && !hasText(value.message) && !hasText(value.text)) return null;
    return {
      role: value.role,
      ...(includeText ? { text: firstText(contentText(value.content), value.message, value.text) } : {}),
      timestamp,
      canonicalUser: false
    };
  }
  if (record.type === "user_message" || record.type === "assistant_message") {
    const values = [record.message, record.text, record.payload?.message, record.payload?.text];
    if (!values.some(hasText)) return null;
    const role = record.type === "user_message" ? "user" : "assistant";
    return {
      role,
      ...(includeText ? { text: firstText(...values) } : {}),
      timestamp,
      canonicalUser: role === "user"
    };
  }
  return null;
}

async function listRolloutFiles(root, codexHomePhysical) {
  const result = [];
  const lexicalRoot = path.resolve(root);
  let rootStat;
  let physicalRoot;
  try {
    rootStat = await fs.lstat(lexicalRoot);
    if (rootStat.isSymbolicLink() || !rootStat.isDirectory()) return result;
    physicalRoot = path.resolve(await fs.realpath(lexicalRoot));
  } catch (error) {
    if (error?.code === "ENOENT") return result;
    throw historyFileError(error, "scanning history rollouts");
  }
  if (!isWithinRoot(codexHomePhysical, physicalRoot)) return result;
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
      else if (entry.isFile() && entry.name.startsWith("rollout-") && entry.name.endsWith(".jsonl")) {
        result.push({ filePath: fullPath, lexicalRoot, physicalRoot });
      }
    }
  }
  await walk(lexicalRoot);
  return result;
}

async function readRollout(
  candidate,
  archived,
  { includeMessages = false, searchQuery = "", messageLimit = DEFAULT_MESSAGE_LIMIT, expectedIdentity = null } = {}
) {
  const { filePath, lexicalRoot, physicalRoot } = candidate;
  let handle;
  let initialIdentity;
  let physicalPath;
  try {
    const [currentRootPhysical, lexicalStat, currentPhysicalPath] = await Promise.all([
      fs.realpath(lexicalRoot),
      fs.lstat(filePath, { bigint: true }),
      fs.realpath(filePath)
    ]);
    if (pathKey(currentRootPhysical) !== pathKey(physicalRoot)
        || lexicalStat.isSymbolicLink()
        || !lexicalStat.isFile()
        || !isWithinRoot(physicalRoot, currentPhysicalPath)) {
      throw staleHistoryError();
    }
    physicalPath = path.resolve(currentPhysicalPath);
    handle = await fs.open(filePath, fsSync.constants.O_RDONLY);
    const openedStat = await handle.stat({ bigint: true });
    initialIdentity = fileIdentity(openedStat);
    if (!sameFileObject(fileIdentity(lexicalStat), initialIdentity)
        || (expectedIdentity && !sameFileIdentity(expectedIdentity, initialIdentity))) {
      throw staleHistoryError();
    }
  } catch (error) {
    await handle?.close().catch(() => {});
    if (error?.code === "STALE_STATE") throw error;
    throw historyFileError(error, "opening a history rollout");
  }
  let meta = null;
  let sequence = 0;
  let assistantCount = 0;
  let canonicalUserCount = 0;
  let legacyUserCount = 0;
  let assistantQueryMatched = false;
  let canonicalUserQueryMatched = false;
  let legacyUserQueryMatched = false;
  let lastAssistant = null;
  let lastCanonicalUser = null;
  let lastLegacyUser = null;
  const assistantMessages = [];
  const canonicalUserMessages = [];
  const legacyUserMessages = [];
  const boundedLimit = Math.max(1, Math.min(messageLimit, DEFAULT_MESSAGE_LIMIT));
  const retain = (items, message) => {
    if (!includeMessages) return;
    items.push(message);
    if (items.length > boundedLimit) items.shift();
  };
  let readFailure = null;
  try {
    for await (const line of readHandleLines(handle)) {
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
      const message = messageFromRecord(record, {
        includeText: includeMessages || Boolean(searchQuery)
      });
      if (message) {
        const descriptor = {
          role: message.role,
          timestamp: message.timestamp,
          canonicalUser: message.canonicalUser,
          sequence: ++sequence,
          ...(searchQuery ? { queryMatched: message.text.toLowerCase().includes(searchQuery) } : {}),
          ...(includeMessages ? { text: message.text } : {})
        };
        if (message.role === "assistant") {
          assistantCount += 1;
          assistantQueryMatched ||= descriptor.queryMatched === true;
          lastAssistant = descriptor;
          retain(assistantMessages, descriptor);
        } else if (message.canonicalUser) {
          canonicalUserCount += 1;
          canonicalUserQueryMatched ||= descriptor.queryMatched === true;
          lastCanonicalUser = descriptor;
          retain(canonicalUserMessages, descriptor);
        } else {
          legacyUserCount += 1;
          legacyUserQueryMatched ||= descriptor.queryMatched === true;
          lastLegacyUser = descriptor;
          retain(legacyUserMessages, descriptor);
        }
      }
    }
  } catch (error) {
    readFailure = error;
    throw historyFileError(error, "reading a history rollout");
  } finally {
    if (readFailure) await handle.close().catch(() => {});
  }
  let finalStat;
  try {
    finalStat = await handle.stat({ bigint: true });
    const [currentRootPhysical, currentPhysicalPath, namedStat] = await Promise.all([
      fs.realpath(lexicalRoot),
      fs.realpath(filePath),
      fs.lstat(filePath, { bigint: true })
    ]);
    const finalIdentity = fileIdentity(finalStat);
    const namedIdentity = fileIdentity(namedStat);
    if (pathKey(currentRootPhysical) !== pathKey(physicalRoot)
        || pathKey(currentPhysicalPath) !== pathKey(physicalPath)
        || namedStat.isSymbolicLink()
        || !namedStat.isFile()
        || !sameFileIdentity(namedIdentity, finalIdentity)
        || (expectedIdentity && !sameFileIdentity(expectedIdentity, finalIdentity))) {
      throw staleHistoryError();
    }
  } catch (error) {
    if (error?.code === "STALE_STATE") throw error;
    throw staleHistoryError(error);
  } finally {
    await handle.close().catch(() => {});
  }
  if (!meta) return null;
  const useCanonicalUsers = canonicalUserCount > 0;
  const selectedUserCount = useCanonicalUsers ? canonicalUserCount : legacyUserCount;
  const selectedUserMessages = useCanonicalUsers ? canonicalUserMessages : legacyUserMessages;
  const selectedLastUser = useCanonicalUsers ? lastCanonicalUser : lastLegacyUser;
  const messageCount = assistantCount + selectedUserCount;
  const messageQueryMatched = assistantQueryMatched
    || (useCanonicalUsers ? canonicalUserQueryMatched : legacyUserQueryMatched);
  const retainedMessages = includeMessages
    ? [...assistantMessages, ...selectedUserMessages]
      .sort((left, right) => left.sequence - right.sequence)
      .slice(-boundedLimit)
    : [];
  const visibleMessages = retainedMessages.map(
    ({ canonicalUser: _canonicalUser, sequence: _sequence, queryMatched: _queryMatched, ...message }, index) => ({
      ...message,
      sequence: messageCount - retainedMessages.length + index + 1
    })
  );
  const lastVisible = [lastAssistant, selectedLastUser]
    .filter(Boolean)
    .sort((left, right) => left.sequence - right.sequence)
    .at(-1);
  const rolloutPath = path.resolve(filePath);
  const updatedAt = lastVisible?.timestamp ?? new Date(Number(finalStat.mtimeMs)).toISOString();
  const identity = fileIdentity(finalStat);
  return {
    ...meta,
    id: meta.threadId ?? fallbackSessionId(rolloutPath),
    rolloutPath,
    updatedAt,
    archived,
    ...(includeMessages ? { messages: visibleMessages } : {}),
    messageCount,
    messageQueryMatched,
    filePath,
    lexicalRoot,
    physicalRoot,
    physicalPath,
    fileIdentity: identity,
    mtimeMs: Number(finalStat.mtimeMs)
  };
}

async function collectHistory(codexHome, options = {}) {
  const sessions = [];
  let codexHomePhysical;
  try {
    codexHomePhysical = path.resolve(await fs.realpath(path.resolve(codexHome)));
  } catch (error) {
    if (error?.code === "ENOENT") return [];
    throw historyFileError(error, "resolving the Codex Home for history");
  }
  for (const dirName of SESSION_DIRS) {
    const files = await listRolloutFiles(path.join(codexHome, dirName), codexHomePhysical);
    for (const candidate of files) {
      let session;
      try {
        session = await readRollout(candidate, dirName === "archived_sessions", options);
      } catch (error) {
        if (error?.code === "ENOENT" || error?.code === "STALE_STATE") continue;
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
  const sessions = await collectHistory(codexHome, { searchQuery: query });
  const filtered = sessions.filter((session) => {
    if (provider && session.provider !== provider) return false;
    if (archived !== "all" && session.archived !== (archived === "archived")) return false;
    if (project && !session.cwd.toLowerCase().includes(project)) return false;
    if (query) {
      const metadata = [session.title, session.cwd, session.provider].join("\n").toLowerCase();
      if (!metadata.includes(query) && !session.messageQueryMatched) return false;
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
  const summaries = await collectHistory(codexHome);
  const summary = summaries.find((item) => item.id === sessionId);
  if (!summary) {
    throw new CoreError(
      "INVALID_INPUT",
      "The selected session was not found in this Codex Home."
    );
  }
  const safeLimit = Number.isInteger(messageLimit) && messageLimit > 0
    ? Math.min(messageLimit, DEFAULT_MESSAGE_LIMIT)
    : DEFAULT_MESSAGE_LIMIT;
  let session;
  try {
    session = await readRollout({
      filePath: summary.filePath,
      lexicalRoot: summary.lexicalRoot,
      physicalRoot: summary.physicalRoot
    }, summary.archived, {
      includeMessages: true,
      messageLimit: safeLimit,
      expectedIdentity: summary.fileIdentity
    });
  } catch (error) {
    if (error?.code !== "ENOENT") throw historyFileError(error, "reading a history rollout");
  }
  if (!session || session.id !== sessionId) {
    throw new CoreError(
      "STALE_STATE",
      "The selected session changed before its messages could be read."
    );
  }
  const messages = session.messages;
  return { session: publicSession(session), messages, truncated: messages.length < session.messageCount, returnedMessageCount: messages.length };
}
