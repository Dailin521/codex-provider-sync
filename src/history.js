import crypto from "node:crypto";
import fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";

import { SESSION_DIRS } from "./constants.js";
import { CoreError } from "./core-error.js";

const DEFAULT_PAGE_SIZE = 50;
const MAX_PAGE_SIZE = 100;
const DEFAULT_MESSAGE_LIMIT = 200;
const HISTORY_METADATA_MAX_BYTES = 64 * 1024;
const HISTORY_METADATA_READ_CHUNK_BYTES = 16 * 1024;
const HISTORY_THREAD_ID_MAX_CHARS = 512;
const HISTORY_TITLE_MAX_CHARS = 1024;
const HISTORY_CWD_MAX_CHARS = 32 * 1024;
const HISTORY_PROVIDER_MAX_CHARS = 512;
const HISTORY_MODEL_MAX_CHARS = 512;
const HISTORY_TIMESTAMP_MAX_CHARS = 128;

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

function firstBoundedText(maxChars, ...values) {
  for (const value of values) {
    const text = normalizeText(value);
    if (text && text.length <= maxChars) return text;
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

function sessionMetaFromRecord(record) {
  if (record?.type !== "session_meta" || !record.payload || typeof record.payload !== "object") {
    return null;
  }
  const payload = record.payload;
  const timestamp = record.timestamp ?? payload.timestamp ?? null;
  return {
    threadId: typeof payload.id === "string"
      && payload.id.length > 0
      && payload.id.length <= HISTORY_THREAD_ID_MAX_CHARS
      ? payload.id
      : null,
    title: firstBoundedText(HISTORY_TITLE_MAX_CHARS, payload.title, payload.name),
    cwd: firstBoundedText(HISTORY_CWD_MAX_CHARS, payload.cwd),
    provider: firstBoundedText(HISTORY_PROVIDER_MAX_CHARS, payload.model_provider) || "(missing)",
    model: firstBoundedText(HISTORY_MODEL_MAX_CHARS, payload.model),
    createdAt: typeof timestamp === "string" && timestamp.length <= HISTORY_TIMESTAMP_MAX_CHARS
      ? timestamp
      : null
  };
}

async function openRolloutCandidate(candidate, expectedIdentity = null) {
  const { filePath, lexicalRoot, physicalRoot } = candidate;
  let handle;
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
    const physicalPath = path.resolve(currentPhysicalPath);
    handle = await fs.open(filePath, fsSync.constants.O_RDONLY);
    const openedStat = await handle.stat({ bigint: true });
    const openedIdentity = fileIdentity(openedStat);
    if (!sameFileObject(fileIdentity(lexicalStat), openedIdentity)
        || (expectedIdentity && !sameFileIdentity(expectedIdentity, openedIdentity))) {
      throw staleHistoryError();
    }
    return { handle, physicalPath };
  } catch (error) {
    await handle?.close().catch(() => {});
    if (error?.code === "STALE_STATE") throw error;
    throw historyFileError(error, "opening a history rollout");
  }
}

async function validateOpenedRollout(candidate, handle, physicalPath, expectedIdentity = null) {
  const { filePath, lexicalRoot, physicalRoot } = candidate;
  try {
    const finalStat = await handle.stat({ bigint: true });
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
    return { finalStat, finalIdentity };
  } catch (error) {
    if (error?.code === "STALE_STATE") throw error;
    throw staleHistoryError(error);
  }
}

async function readBoundedFirstLine(handle) {
  const chunks = [];
  let position = 0;
  let totalBytes = 0;
  while (totalBytes <= HISTORY_METADATA_MAX_BYTES) {
    const chunkLength = Math.min(
      HISTORY_METADATA_READ_CHUNK_BYTES,
      HISTORY_METADATA_MAX_BYTES + 1 - totalBytes
    );
    if (chunkLength <= 0) return null;
    const chunk = Buffer.allocUnsafe(chunkLength);
    const { bytesRead } = await handle.read(chunk, 0, chunk.length, position);
    if (bytesRead === 0) break;
    const data = chunk.subarray(0, bytesRead);
    const relativeNewline = data.indexOf(0x0a);
    position += bytesRead;
    if (relativeNewline >= 0) {
      const lineLength = totalBytes + relativeNewline;
      if (lineLength > HISTORY_METADATA_MAX_BYTES) return null;
      const line = Buffer.concat([...chunks, data.subarray(0, relativeNewline)], lineLength);
      const end = line.length > 0 && line[line.length - 1] === 0x0d ? line.length - 1 : line.length;
      return line.subarray(0, end).toString("utf8");
    }
    chunks.push(data);
    totalBytes += bytesRead;
  }
  if (totalBytes > HISTORY_METADATA_MAX_BYTES) return null;
  return Buffer.concat(chunks, totalBytes).toString("utf8");
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

async function readRolloutMetadata(candidate, archived) {
  const { filePath, lexicalRoot, physicalRoot } = candidate;
  let handle;
  try {
    const opened = await openRolloutCandidate(candidate);
    handle = opened.handle;
    const firstLine = await readBoundedFirstLine(handle);
    let record = null;
    if (firstLine !== null) {
      try {
        record = JSON.parse(firstLine);
      } catch {
        // A malformed or oversized metadata line is not safe to treat as a session.
      }
    }
    const meta = sessionMetaFromRecord(record);
    const { finalStat, finalIdentity } = await validateOpenedRollout(
      candidate,
      handle,
      opened.physicalPath
    );
    if (!meta) return null;
    const rolloutPath = path.resolve(filePath);
    return {
      ...meta,
      id: meta.threadId ?? fallbackSessionId(rolloutPath),
      rolloutPath,
      updatedAt: new Date(Number(finalStat.mtimeMs)).toISOString(),
      archived,
      messageCount: 0,
      messageCountKnown: false,
      messageQueryMatched: false,
      filePath,
      lexicalRoot,
      physicalRoot,
      physicalPath: opened.physicalPath,
      fileIdentity: finalIdentity,
      mtimeMs: Number(finalStat.mtimeMs)
    };
  } catch (error) {
    if (error?.code === "STALE_STATE") throw error;
    throw historyFileError(error, "reading history rollout metadata");
  } finally {
    await handle?.close().catch(() => {});
  }
}

async function readRollout(
  candidate,
  archived,
  { includeMessages = false, searchQuery = "", messageLimit = DEFAULT_MESSAGE_LIMIT, expectedIdentity = null } = {}
) {
  const { filePath, lexicalRoot, physicalRoot } = candidate;
  let handle;
  let physicalPath;
  const opened = await openRolloutCandidate(candidate, expectedIdentity);
  handle = opened.handle;
  physicalPath = opened.physicalPath;
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
      if (!meta) meta = sessionMetaFromRecord(record);
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
  let identity;
  try {
    const validated = await validateOpenedRollout(candidate, handle, physicalPath, expectedIdentity);
    finalStat = validated.finalStat;
    identity = validated.finalIdentity;
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
  return {
    ...meta,
    id: meta.threadId ?? fallbackSessionId(rolloutPath),
    rolloutPath,
    updatedAt,
    archived,
    ...(includeMessages ? { messages: visibleMessages } : {}),
    messageCount,
    messageCountKnown: true,
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
        session = options.metadataOnly
          ? await readRolloutMetadata(candidate, dirName === "archived_sessions")
          : await readRollout(candidate, dirName === "archived_sessions", options);
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
    messageCount: session.messageCount,
    ...(typeof session.messageCountKnown === "boolean"
      ? { messageCountKnown: session.messageCountKnown }
      : {})
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
  const sessions = await collectHistory(codexHome, {
    searchQuery: query,
    metadataOnly: !query
  });
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
  const summaries = await collectHistory(codexHome, { metadataOnly: true });
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
