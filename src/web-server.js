import crypto from "node:crypto";
import fs from "node:fs/promises";
import http from "node:http";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { listBackups } from "./backup.js";
import { readConfigText, readRootModelFromConfigText } from "./config-file.js";
import { defaultCodexHome } from "./constants.js";
import { getHistorySession, listHistory } from "./history.js";
import { getStatus, runPruneBackups, runRestore, runSwitch, runSync } from "./service.js";

const DEFAULT_PORT = 8791;
const MAX_REQUEST_BYTES = 64 * 1024;
const ACTIVITY_LIMIT = 250;
const WEB_ROOT = fileURLToPath(new URL("../web/dist/", import.meta.url));
const MIME_TYPES = new Map([
  [".css", "text/css; charset=utf-8"],
  [".html", "text/html; charset=utf-8"],
  [".ico", "image/x-icon"],
  [".js", "text/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".map", "application/json; charset=utf-8"],
  [".png", "image/png"],
  [".svg", "image/svg+xml"],
  [".webp", "image/webp"]
]);

function sendJson(response, statusCode, value) {
  const body = JSON.stringify(value);
  response.writeHead(statusCode, {
    "Cache-Control": "no-store",
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(body),
    "X-Content-Type-Options": "nosniff"
  });
  response.end(body);
}

function sendError(response, statusCode, error) {
  const message = error instanceof Error ? error.message : String(error);
  sendJson(response, statusCode, { error: message });
}

async function readJsonBody(request) {
  const chunks = [];
  let received = 0;
  for await (const chunk of request) {
    received += chunk.length;
    if (received > MAX_REQUEST_BYTES) {
      throw new Error(`Request body exceeds ${MAX_REQUEST_BYTES} bytes.`);
    }
    chunks.push(chunk);
  }
  if (chunks.length === 0) {
    return {};
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new Error("Request body must be valid JSON.");
  }
}

function requireString(value, label, { optional = false, maxLength = 4096 } = {}) {
  if (value === undefined || value === null || value === "") {
    if (optional) {
      return undefined;
    }
    throw new Error(`${label} is required.`);
  }
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`${label} must be a non-empty string.`);
  }
  const normalized = value.trim();
  if (normalized.length > maxLength) {
    throw new Error(`${label} is too long.`);
  }
  return normalized;
}

function requireProvider(value) {
  const provider = requireString(value, "provider", { maxLength: 200 });
  if (!/^[A-Za-z0-9_.-]+$/.test(provider)) {
    throw new Error("provider may only contain letters, numbers, dots, underscores, and hyphens.");
  }
  return provider;
}

function requireKeepCount(value, { allowZero = false } = {}) {
  const minimum = allowZero ? 0 : 1;
  if (!Number.isInteger(value) || value < minimum || value > 100000) {
    throw new Error(`keepCount must be an integer between ${minimum} and 100000.`);
  }
  return value;
}

function normalizeStorageInput(input = {}) {
  return {
    codexHome: requireString(input.codexHome ?? defaultCodexHome(), "codexHome"),
    sqliteHome: requireString(input.sqliteHome, "sqliteHome", { optional: true })
  };
}

function serializeStatus(status) {
  const rollout = status.rolloutCounts ?? { sessions: {}, archived_sessions: {} };
  const sqlite = status.sqliteCounts;
  const normalizeCounts = (counts) => Object.fromEntries(
    Object.entries(counts ?? {}).sort(([left], [right]) => left.localeCompare(right))
  );
  const normalizeDistribution = (distribution) => ({
    sessions: normalizeCounts(distribution?.sessions),
    archived_sessions: normalizeCounts(distribution?.archived_sessions)
  });
  const rolloutJson = JSON.stringify(normalizeDistribution(rollout));
  const sqliteComparable = sqlite && !sqlite.unreadable
    ? JSON.stringify(normalizeDistribution(sqlite))
    : null;
  return {
    ...status,
    alignment: {
      aligned: Boolean(sqliteComparable && rolloutJson === sqliteComparable),
      sqliteReadable: Boolean(sqlite && !sqlite.unreadable),
      targetProvider: status.currentProvider
    }
  };
}

function stageMessage(event) {
  const messages = {
    scan_rollout_files: "Scanning rollout files",
    check_locked_rollout_files: "Checking locked rollout files",
    create_backup: "Creating backup",
    update_config: "Updating config.toml",
    update_sqlite: "Updating SQLite metadata",
    rewrite_rollout_files: "Rewriting rollout metadata",
    clean_backups: "Cleaning old backups"
  };
  const base = messages[event?.stage] ?? event?.stage ?? "Operation progress";
  return event?.status === "complete" ? `${base} complete` : base;
}

function openLocalUrl(url, platform = process.platform) {
  let command;
  let args;
  if (platform === "win32") {
    command = "cmd";
    args = ["/d", "/s", "/c", "start", "", url];
  } else if (platform === "darwin") {
    command = "open";
    args = [url];
  } else {
    command = "xdg-open";
    args = [url];
  }
  const child = spawn(command, args, { detached: true, stdio: "ignore" });
  child.unref();
}

async function serveStatic(response, pathname, token, webRoot) {
  let relativePath = pathname === "/" ? "index.html" : pathname.slice(1);
  try {
    relativePath = decodeURIComponent(relativePath);
  } catch {
    sendError(response, 400, "Invalid URL encoding.");
    return;
  }

  const root = path.resolve(webRoot);
  let filePath = path.resolve(root, relativePath);
  if (filePath !== root && !filePath.startsWith(`${root}${path.sep}`)) {
    sendError(response, 403, "Static path is outside the Web UI root.");
    return;
  }

  let file;
  try {
    file = await fs.readFile(filePath);
  } catch (error) {
    if (error?.code !== "ENOENT" || path.extname(relativePath)) {
      sendError(response, error?.code === "ENOENT" ? 404 : 500, error);
      return;
    }
    filePath = path.join(root, "index.html");
    file = await fs.readFile(filePath);
  }

  const extension = path.extname(filePath).toLowerCase();
  if (extension === ".html") {
    const bootstrap = JSON.stringify({ apiToken: token });
    file = Buffer.from(
      file.toString("utf8").replace("__CODEX_PROVIDER_SYNC_BOOTSTRAP__", bootstrap),
      "utf8"
    );
  }
  response.writeHead(200, {
    "Cache-Control": extension === ".html" ? "no-store" : "public, max-age=3600",
    "Content-Type": MIME_TYPES.get(extension) ?? "application/octet-stream",
    "Content-Length": file.length,
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Content-Security-Policy": "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
  });
  response.end(file);
}

export function createWebUiServer({ token = crypto.randomBytes(24).toString("hex"), webRoot = WEB_ROOT, services = {} } = {}) {
  const api = {
    getStatus: services.getStatus ?? getStatus,
    listBackups: services.listBackups ?? listBackups,
    runSync: services.runSync ?? runSync,
    runSwitch: services.runSwitch ?? runSwitch,
    runRestore: services.runRestore ?? runRestore,
    runPruneBackups: services.runPruneBackups ?? runPruneBackups,
    readConfigText: services.readConfigText ?? readConfigText,
    readRootModelFromConfigText: services.readRootModelFromConfigText ?? readRootModelFromConfigText,
    listHistory: services.listHistory ?? listHistory,
    getHistorySession: services.getHistorySession ?? getHistorySession
  };
  const activity = [];
  let activityId = 0;
  let activeOperation = null;
  let origin = null;

  const record = (level, message, detail = null, operation = activeOperation?.kind ?? null) => {
    activityId += 1;
    activity.push({ id: activityId, timestamp: new Date().toISOString(), level, message, detail, operation });
    if (activity.length > ACTIVITY_LIMIT) {
      activity.splice(0, activity.length - ACTIVITY_LIMIT);
    }
  };

  const authorize = (request) => {
    if (request.headers["x-codex-provider-token"] !== token) {
      return false;
    }
    const requestOrigin = request.headers.origin;
    return !requestOrigin || !origin || requestOrigin === origin;
  };

  const withOperation = async (kind, response, operation) => {
    if (activeOperation) {
      sendJson(response, 409, { error: `Another operation is already running: ${activeOperation.kind}.` });
      return;
    }
    activeOperation = { kind, startedAt: new Date().toISOString() };
    record("info", `${kind} started`);
    try {
      const result = await operation();
      record("success", `${kind} completed`);
      sendJson(response, 200, { result });
    } catch (error) {
      record("error", `${kind} failed`, error instanceof Error ? error.message : String(error));
      sendError(response, 400, error);
    } finally {
      activeOperation = null;
    }
  };

  const server = http.createServer(async (request, response) => {
    const requestUrl = new URL(request.url ?? "/", origin ?? "http://127.0.0.1");
    const pathname = requestUrl.pathname;

    try {
      if (pathname.startsWith("/api/")) {
        if (!authorize(request)) {
          sendError(response, 403, "Invalid Web UI session token or origin.");
          return;
        }

        if (request.method === "GET" && pathname === "/api/health") {
          sendJson(response, 200, { ok: true, activeOperation });
          return;
        }

        if (request.method === "GET" && pathname === "/api/activity") {
          const after = Number.parseInt(requestUrl.searchParams.get("after") ?? "0", 10) || 0;
          sendJson(response, 200, { activity: activity.filter((entry) => entry.id > after), activeOperation });
          return;
        }

        if (request.method !== "POST") {
          sendError(response, 405, "API endpoint requires POST.");
          return;
        }

        const body = await readJsonBody(request);
        if (pathname === "/api/status") {
          const storage = normalizeStorageInput(body);
          const status = serializeStatus(await api.getStatus(storage));
          record("info", "Status refreshed", status.codexHome, null);
          sendJson(response, 200, { status });
          return;
        }

        if (pathname === "/api/backups") {
          const { codexHome } = normalizeStorageInput(body);
          sendJson(response, 200, await api.listBackups(codexHome));
          return;
        }

        if (pathname === "/api/history") {
          const storage = normalizeStorageInput(body);
          const history = await api.listHistory(storage.codexHome, body);
          sendJson(response, 200, { history });
          return;
        }

        if (pathname === "/api/history/session") {
          const storage = normalizeStorageInput(body);
          const sessionId = requireString(body.sessionId, "sessionId", { maxLength: 300 });
          const history = await api.getHistorySession(storage.codexHome, sessionId);
          sendJson(response, 200, { history });
          return;
        }

        if (pathname === "/api/sync") {
          await withOperation("sync", response, async () => {
            const storage = normalizeStorageInput(body);
            const provider = requireProvider(body.provider);
            const keepCount = requireKeepCount(body.keepCount);
            const configText = await api.readConfigText(path.join(storage.codexHome, "config.toml"));
            const model = api.readRootModelFromConfigText(configText);
            return api.runSync({
              ...storage,
              provider,
              keepCount,
              model,
              onProgress: (event) => record("progress", stageMessage(event), event)
            });
          });
          return;
        }

        if (pathname === "/api/switch") {
          await withOperation("switch", response, async () => {
            const storage = normalizeStorageInput(body);
            const provider = requireProvider(body.provider);
            const keepCount = requireKeepCount(body.keepCount);
            const model = requireString(body.model, "model", { optional: true, maxLength: 500 });
            return api.runSwitch({
              ...storage,
              provider,
              keepCount,
              model,
              keepRootModel: Boolean(body.keepRootModel),
              onProgress: (event) => record("progress", stageMessage(event), event)
            });
          });
          return;
        }

        if (pathname === "/api/restore") {
          await withOperation("restore", response, async () => {
            const storage = normalizeStorageInput(body);
            const backupId = requireString(body.backupId, "backupId", { maxLength: 300 });
            const listed = await api.listBackups(storage.codexHome);
            const backup = listed.backups.find((entry) => entry.id === backupId);
            if (!backup) {
              throw new Error("The selected backup is not a managed backup for this Codex Home.");
            }
            const restoreConfig = Boolean(body.restoreConfig);
            const restoreDatabase = Boolean(body.restoreDatabase);
            const restoreSessions = Boolean(body.restoreSessions);
            if (!restoreConfig && !restoreDatabase && !restoreSessions) {
              throw new Error("Select at least one backup content type to restore.");
            }
            return api.runRestore({
              ...storage,
              backupDir: backup.path,
              restoreConfig,
              restoreDatabase,
              restoreSessions,
              allowSqliteHomeRelocation: Boolean(body.allowSqliteHomeRelocation)
            });
          });
          return;
        }

        if (pathname === "/api/prune") {
          await withOperation("prune backups", response, async () => {
            const { codexHome } = normalizeStorageInput(body);
            return api.runPruneBackups({ codexHome, keepCount: requireKeepCount(body.keepCount, { allowZero: true }) });
          });
          return;
        }

        sendError(response, 404, "Unknown API endpoint.");
        return;
      }

      if (request.method !== "GET" && request.method !== "HEAD") {
        sendError(response, 405, "Method not allowed.");
        return;
      }
      await serveStatic(response, pathname, token, webRoot);
    } catch (error) {
      sendError(response, 500, error);
    }
  });

  return {
    server,
    token,
    setOrigin(value) {
      origin = value;
    },
    getActivity() {
      return [...activity];
    }
  };
}

export async function startWebUi({ port = DEFAULT_PORT, openBrowser = true, webRoot = WEB_ROOT, services } = {}) {
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error(`Invalid Web UI port: ${port}.`);
  }
  await fs.access(path.join(webRoot, "index.html")).catch(() => {
    throw new Error(`Web UI build not found at ${webRoot}. Run \"npm run web:build\" first.`);
  });

  const handle = createWebUiServer({ webRoot, services });
  await new Promise((resolve, reject) => {
    handle.server.once("error", reject);
    handle.server.listen(port, "127.0.0.1", resolve);
  });
  const address = handle.server.address();
  const actualPort = typeof address === "object" && address ? address.port : port;
  const url = `http://127.0.0.1:${actualPort}`;
  handle.setOrigin(url);
  if (openBrowser) {
    try {
      openLocalUrl(url);
    } catch {
      // The URL is printed by the CLI even if the OS browser opener is unavailable.
    }
  }
  return {
    ...handle,
    url,
    close: () => new Promise((resolve, reject) => handle.server.close((error) => error ? reject(error) : resolve()))
  };
}
