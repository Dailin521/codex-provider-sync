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
import { createMemoryWebUiState, WebUiStateStore } from "./web-state.js";

const DEFAULT_PORT = 8791;
const MAX_REQUEST_BYTES = 64 * 1024;
const ACTIVITY_LIMIT = 250;
const PAIRING_TTL_MS = 5 * 60 * 1000;
const DEVICE_SECRET_BYTES = 32;
const STATE_FILENAME = "provider-sync-web.json";
const RUNTIME_FILENAME = "provider-sync-web.runtime.json";
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

function sendError(response, statusCode, error, code) {
  const message = error instanceof Error ? error.message : String(error);
  sendJson(response, statusCode, { error: message, ...(code ? { code } : {}) });
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

function resolveStorageProfile(input, stateStore) {
  if (Object.hasOwn(input ?? {}, "codexHome") || Object.hasOwn(input ?? {}, "sqliteHome")) {
    throw new Error("Storage paths must be selected through a server-managed profileId.");
  }
  const profileId = requireString(input?.profileId ?? "default", "profileId", { maxLength: 80 });
  const profile = stateStore.getProfile(profileId);
  return {
    profileId: profile.id,
    codexHome: profile.codexHome,
    ...(profile.sqliteHome ? { sqliteHome: profile.sqliteHome } : {})
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
  return new Promise((resolve) => {
    let settled = false;
    const finish = (opened) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      resolve(opened);
    };
    const child = spawn(command, args, { stdio: "ignore" });
    const timeout = setTimeout(() => {
      child.unref();
      finish(true);
    }, 3000);
    child.once("error", () => finish(false));
    child.once("exit", (code) => finish(code === 0));
  });
}

function canOpenLocalUrl(platform = process.platform, environment = process.env) {
  if (platform !== "linux") return true;
  return Boolean(environment.DISPLAY || environment.WAYLAND_DISPLAY);
}

async function serveStatic(response, pathname, webRoot) {
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
    file = Buffer.from(file.toString("utf8").replace("__CODEX_PROVIDER_SYNC_BOOTSTRAP__", "{}"), "utf8");
  }
  response.writeHead(200, {
    "Cache-Control": extension === ".html" ? "no-store" : "public, max-age=3600",
    "Content-Type": MIME_TYPES.get(extension) ?? "application/octet-stream",
    "Content-Length": file.length,
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
  });
  response.end(file);
}

function randomSecret() {
  return crypto.randomBytes(DEVICE_SECRET_BYTES).toString("base64url");
}

function secretDigest(secret) {
  return crypto.createHash("sha256").update(String(secret), "utf8").digest();
}

function secretsMatch(secret, expectedDigest) {
  if (typeof secret !== "string" || !expectedDigest) return false;
  const candidate = secretDigest(secret);
  return candidate.length === expectedDigest.length && crypto.timingSafeEqual(candidate, expectedDigest);
}

function isLoopbackHostname(hostname) {
  const normalized = String(hostname).toLowerCase().replace(/^\[|\]$/g, "");
  return normalized === "127.0.0.1" || normalized === "localhost" || normalized === "::1";
}

function validBrowserOrigin(request, { required = false } = {}) {
  const value = request.headers.origin;
  if (!value) return !required;
  const host = request.headers.host;
  if (!host) return false;
  try {
    const originUrl = new URL(value);
    const hostUrl = new URL(`http://${host}`);
    return originUrl.protocol === "http:"
      && isLoopbackHostname(originUrl.hostname)
      && isLoopbackHostname(hostUrl.hostname)
      && originUrl.host.toLowerCase() === hostUrl.host.toLowerCase();
  } catch {
    return false;
  }
}

export function createWebUiServer({
  webRoot = WEB_ROOT,
  services = {},
  stateStore = createMemoryWebUiState({ codexHome: defaultCodexHome() }),
  internalSecret = randomSecret(),
  pairingTtlMs = PAIRING_TTL_MS,
  now = () => Date.now()
} = {}) {
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
  let baseUrl = null;
  let pairing = null;

  const record = (level, message, detail = null, operation = activeOperation?.kind ?? null) => {
    activityId += 1;
    activity.push({ id: activityId, timestamp: new Date().toISOString(), level, message, detail, operation });
    if (activity.length > ACTIVITY_LIMIT) {
      activity.splice(0, activity.length - ACTIVITY_LIMIT);
    }
  };

  const issuePairing = () => {
    const secret = randomSecret();
    pairing = {
      digest: secretDigest(secret),
      expiresAt: now() + pairingTtlMs
    };
    return secret;
  };

  const authorize = (request) => {
    return stateStore.hasCredential(request.headers["x-codex-provider-device"]);
  };

  const requireAuthorizedBrowser = (request, response, { originRequired = request.method !== "GET" } = {}) => {
    if (!validBrowserOrigin(request, { required: originRequired })) {
      sendError(response, 403, "Invalid browser Origin for this loopback request.", "INVALID_ORIGIN");
      return false;
    }
    if (!authorize(request)) {
      sendError(response, 403, "This browser is not paired with the Web UI.", "PAIRING_REQUIRED");
      return false;
    }
    return true;
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
    const requestUrl = new URL(request.url ?? "/", baseUrl ?? "http://127.0.0.1");
    const pathname = requestUrl.pathname;

    try {
      if (request.method === "POST" && pathname === "/api/pair") {
        if (!validBrowserOrigin(request, { required: true })) {
          sendError(response, 403, "Invalid browser Origin for pairing.", "INVALID_ORIGIN");
          return;
        }
        const supplied = request.headers["x-codex-provider-pairing"];
        if (!pairing || pairing.expiresAt < now() || !secretsMatch(supplied, pairing.digest)) {
          sendError(response, 403, "The pairing link is invalid, expired, or already used.", "PAIRING_REQUIRED");
          return;
        }
        pairing = null;
        const deviceCredential = randomSecret();
        await stateStore.addCredential(deviceCredential);
        sendJson(response, 200, { deviceCredential });
        return;
      }

      if (request.method === "POST" && pathname === "/api/internal/new-pairing") {
        if (!secretsMatch(request.headers["x-codex-provider-internal"], secretDigest(internalSecret))) {
          sendError(response, 403, "Invalid Web UI instance secret.");
          return;
        }
        const body = await readJsonBody(request);
        if (body.resetAccess) await stateStore.resetCredentials();
        sendJson(response, 200, { pairingToken: issuePairing() });
        return;
      }

      if (request.method === "GET" && pathname === "/api/health") {
        sendJson(response, 200, { ok: true, service: "codex-provider-sync", activeOperation });
        return;
      }

      if (pathname.startsWith("/api/")) {
        if (!requireAuthorizedBrowser(request, response)) {
          return;
        }

        if (request.method === "GET" && pathname === "/api/profiles") {
          sendJson(response, 200, { profiles: stateStore.listProfiles() });
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
        if (pathname === "/api/profiles/save") {
          const profile = await stateStore.saveProfile({
            id: requireString(body.profileId, "profileId", { maxLength: 80 }),
            name: requireString(body.name, "name", { maxLength: 120 }),
            codexHome: requireString(body.codexHome, "codexHome"),
            sqliteHome: requireString(body.sqliteHome, "sqliteHome", { optional: true })
          });
          sendJson(response, 200, { profile });
          return;
        }

        if (pathname === "/api/profiles/delete") {
          await stateStore.deleteProfile(requireString(body.profileId, "profileId", { maxLength: 80 }));
          sendJson(response, 200, { ok: true });
          return;
        }

        if (pathname === "/api/access/forget") {
          await stateStore.removeCredential(request.headers["x-codex-provider-device"]);
          sendJson(response, 200, { ok: true });
          return;
        }

        if (pathname === "/api/status") {
          const storage = resolveStorageProfile(body, stateStore);
          const status = serializeStatus(await api.getStatus(storage));
          status.profileId = storage.profileId;
          record("info", "Status refreshed", status.codexHome, null);
          sendJson(response, 200, { status });
          return;
        }

        if (pathname === "/api/backups") {
          const { codexHome } = resolveStorageProfile(body, stateStore);
          sendJson(response, 200, await api.listBackups(codexHome));
          return;
        }

        if (pathname === "/api/history") {
          const storage = resolveStorageProfile(body, stateStore);
          const history = await api.listHistory(storage.codexHome, body);
          sendJson(response, 200, { history });
          return;
        }

        if (pathname === "/api/history/session") {
          const storage = resolveStorageProfile(body, stateStore);
          const sessionId = requireString(body.sessionId, "sessionId", { maxLength: 300 });
          const history = await api.getHistorySession(storage.codexHome, sessionId);
          sendJson(response, 200, { history });
          return;
        }

        if (pathname === "/api/sync") {
          await withOperation("sync", response, async () => {
            const storage = resolveStorageProfile(body, stateStore);
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
            const storage = resolveStorageProfile(body, stateStore);
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
            const storage = resolveStorageProfile(body, stateStore);
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
            const { codexHome } = resolveStorageProfile(body, stateStore);
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
      await serveStatic(response, pathname, webRoot);
    } catch (error) {
      sendError(response, 500, error);
    }
  });

  return {
    server,
    internalSecret,
    issuePairing,
    setBaseUrl(value) {
      baseUrl = value;
    },
    getActivity() {
      return [...activity];
    }
  };
}

function requestExistingPairing({ port, internalSecret, resetAccess }) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ resetAccess });
    const request = http.request({
      hostname: "127.0.0.1",
      port,
      path: "/api/internal/new-pairing",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(body),
        "X-Codex-Provider-Internal": internalSecret
      },
      timeout: 1500
    }, (response) => {
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("end", () => {
        try {
          const payload = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          if (response.statusCode !== 200 || typeof payload.pairingToken !== "string") {
            reject(new Error("The existing listener is not a compatible Codex Provider Sync Web UI."));
            return;
          }
          resolve(payload.pairingToken);
        } catch (error) {
          reject(error);
        }
      });
    });
    request.once("timeout", () => request.destroy(new Error("Timed out contacting the existing Web UI.")));
    request.once("error", reject);
    request.end(body);
  });
}

async function readRuntimeDescriptor(runtimeFile) {
  try {
    const value = JSON.parse(await fs.readFile(runtimeFile, "utf8"));
    if (Number.isInteger(value?.port) && value.port > 0 && typeof value?.internalSecret === "string") {
      return value;
    }
  } catch {
    // A missing, stale, or malformed descriptor is treated as no live instance.
  }
  return null;
}

async function removeOwnedRuntimeDescriptor(runtimeFile, internalSecret) {
  const current = await readRuntimeDescriptor(runtimeFile);
  if (current?.internalSecret === internalSecret) {
    await fs.rm(runtimeFile, { force: true }).catch(() => {});
  }
}

export async function startWebUi({
  port = DEFAULT_PORT,
  openBrowser = true,
  resetAccess = false,
  codexHome,
  sqliteHome,
  stateFile,
  runtimeFile,
  webRoot = WEB_ROOT,
  services,
  platform = process.platform,
  environment = process.env,
  openUrl = openLocalUrl
} = {}) {
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error(`Invalid Web UI port: ${port}.`);
  }
  await fs.access(path.join(webRoot, "index.html")).catch(() => {
    throw new Error(`Web UI build not found at ${webRoot}. Run \"npm run web:build\" first.`);
  });

  const controlCodexHome = path.resolve(codexHome ?? process.env.CODEX_HOME ?? defaultCodexHome());
  const resolvedStateFile = path.resolve(stateFile ?? path.join(controlCodexHome, STATE_FILENAME));
  const resolvedRuntimeFile = path.resolve(runtimeFile ?? path.join(controlCodexHome, RUNTIME_FILENAME));
  const existing = await readRuntimeDescriptor(resolvedRuntimeFile);
  if (existing) {
    try {
      const pairingToken = await requestExistingPairing({
        port: existing.port,
        internalSecret: existing.internalSecret,
        resetAccess
      });
      const url = `http://127.0.0.1:${existing.port}`;
      const pairingUrl = `${url}/#pair=${encodeURIComponent(pairingToken)}`;
      const browserOpened = openBrowser && canOpenLocalUrl(platform, environment)
        ? await openUrl(pairingUrl, platform)
        : false;
      return {
        reused: true,
        url,
        pairingUrl,
        browserOpened,
        close: async () => {}
      };
    } catch {
      await fs.rm(resolvedRuntimeFile, { force: true }).catch(() => {});
    }
  }

  const stateStore = new WebUiStateStore({
    filePath: resolvedStateFile,
    defaultProfile: { codexHome: controlCodexHome, sqliteHome }
  });
  await stateStore.initialize({ resetAccess });
  const internalSecret = randomSecret();
  const handle = createWebUiServer({ webRoot, services, stateStore, internalSecret });
  try {
    await new Promise((resolve, reject) => {
      handle.server.once("error", reject);
      handle.server.listen(port, "127.0.0.1", resolve);
    });
  } catch (error) {
    if (error?.code === "EADDRINUSE") {
      throw new Error(`Web UI port ${port} is already in use by another program. Stop that program or choose --port <available-port>.`, { cause: error });
    }
    throw error;
  }
  const address = handle.server.address();
  const actualPort = typeof address === "object" && address ? address.port : port;
  const url = `http://127.0.0.1:${actualPort}`;
  handle.setBaseUrl(url);
  const pairingUrl = `${url}/#pair=${encodeURIComponent(handle.issuePairing())}`;
  await fs.mkdir(path.dirname(resolvedRuntimeFile), { recursive: true });
  await fs.writeFile(resolvedRuntimeFile, `${JSON.stringify({ port: actualPort, internalSecret, pid: process.pid })}\n`, { encoding: "utf8", mode: 0o600 });
  await fs.chmod(resolvedRuntimeFile, 0o600).catch(() => {});
  const browserOpened = openBrowser && canOpenLocalUrl(platform, environment)
    ? await openUrl(pairingUrl, platform)
    : false;
  return {
    ...handle,
    reused: false,
    url,
    pairingUrl,
    browserOpened,
    stateFile: resolvedStateFile,
    close: async () => {
      await new Promise((resolve, reject) => handle.server.close((error) => error ? reject(error) : resolve()));
      await removeOwnedRuntimeDescriptor(resolvedRuntimeFile, internalSecret);
    }
  };
}
