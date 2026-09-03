import crypto from "node:crypto";
import fs from "node:fs/promises";
import http from "node:http";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { defaultCodexHome } from "./constants.js";
import {
  applyRepair,
  applyRestore,
  applySwitch,
  applySync,
  CoreError,
  prepareRepair,
  prepareRestore,
  prepareSwitch,
  prepareSync,
  readConfigText,
  readRootModelFromConfigText,
  resolveStorageLayout,
  runPruneBackups,
  toCoreErrorDto,
} from "./public-api.js";
import { createMemoryWebUiState, ProfileRevisionConflictError, WebUiStateStore } from "./web-state.js";
import { createWebCoreFacade, dispatchWebCoreRequest } from "./web-core-adapter.js";

const DEFAULT_PORT = 8791;
const MAX_REQUEST_BYTES = 64 * 1024;
const ACTIVITY_LIMIT = 250;
const PAIRING_TTL_MS = 5 * 60 * 1000;
const INTERNAL_PROTOCOL_VERSION = 2;
const INTERNAL_NONCE_BYTES = 32;
const INTERNAL_CHALLENGE_TTL_MS = 30 * 1000;
const INTERNAL_CHALLENGE_LIMIT = 128;
const INTERNAL_REQUEST_DOMAIN = "codex-provider-sync:web-ui:internal-pairing:v2:request";
const INTERNAL_RESPONSE_DOMAIN = "codex-provider-sync:web-ui:internal-pairing:v2:response";
const DEVICE_SECRET_BYTES = 32;
const STATE_FILENAME = "provider-sync-web.json";
const RUNTIME_FILENAME = "provider-sync-web.runtime.json";
const CORE_STREAM_CONTENT_TYPE = "application/x-ndjson";
const CORE_APPLY_METHODS = new Set(["applySync", "applySwitch", "applyRepair", "applyRestore"]);
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

class WebRequestError extends Error {
  constructor(message, statusCode, code) {
    super(message);
    this.name = "WebRequestError";
    this.statusCode = statusCode;
    this.code = code;
  }
}

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
  sendJson(response, statusCode, {
    error: message,
    ...(code ? { code } : {}),
    ...(error instanceof CoreError ? { coreError: toCoreErrorDto(error) } : {})
  });
}

function startCoreStream(response) {
  response.writeHead(200, {
    "Cache-Control": "no-store",
    "Content-Type": `${CORE_STREAM_CONTENT_TYPE}; charset=utf-8`,
    "X-Content-Type-Options": "nosniff"
  });
  response.flushHeaders?.();
}

function writeCoreStream(response, value) {
  if (response.destroyed || response.writableEnded) return;
  try { response.write(`${JSON.stringify(value)}\n`); } catch {}
}

function coreErrorHttpStatus(error, fallback) {
  if (!(error instanceof CoreError)) return fallback;
  if (error.code === "INVALID_INPUT"
      || error.code === "RESTORE_VALIDATION_FAILED"
      || error.code === "SQLITE_UNSUPPORTED_PATH") return 400;
  if (error.code === "PLAN_EXPIRED" || error.code === "STALE_STATE"
      || error.code === "OPERATION_BUSY" || error.code === "LOCK_UNVERIFIABLE"
      || error.code === "RECOVERY_REQUIRED") return 409;
  return fallback;
}

async function readJsonBody(request) {
  const chunks = await new Promise((resolve, reject) => {
    const receivedChunks = [];
    let received = 0;
    let tooLarge = false;
    let settled = false;
    const cleanup = ({ keepErrorListener = false } = {}) => {
      request.removeListener("data", onData);
      request.removeListener("end", onEnd);
      if (!keepErrorListener) request.removeListener("error", onError);
      request.removeListener("aborted", onAborted);
    };
    const finish = (callback, value, cleanupOptions) => {
      if (settled) return;
      settled = true;
      cleanup(cleanupOptions);
      callback(value);
    };
    const onData = (chunk) => {
      if (tooLarge) return;
      received += chunk.length;
      if (received > MAX_REQUEST_BYTES) {
        tooLarge = true;
        receivedChunks.length = 0;
        return;
      }
      receivedChunks.push(chunk);
    };
    const onEnd = () => {
      if (tooLarge) {
        finish(reject, new WebRequestError(
          `Request body exceeds ${MAX_REQUEST_BYTES} bytes.`,
          413,
          "REQUEST_TOO_LARGE"
        ));
        return;
      }
      finish(resolve, receivedChunks);
    };
    const onError = (error) => finish(reject, error);
    const onAborted = () => finish(
      reject,
      new WebRequestError("Request was aborted.", 400, "INVALID_REQUEST"),
      // Some supported Node releases emit ECONNRESET after `aborted`.
      // Leave the once-only error listener in place to consume that terminal
      // stream event; its settle guard preserves the original error.
      { keepErrorListener: true }
    );
    request.on("data", onData);
    request.once("end", onEnd);
    request.once("error", onError);
    request.once("aborted", onAborted);
    if (request.aborted) onAborted();
  });
  if (chunks.length === 0) {
    return {};
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new WebRequestError("Request body must be valid JSON.", 400, "INVALID_JSON");
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
    profileRevision: profile.revision,
    codexHome: profile.codexHome,
    ...(profile.sqliteHome ? { sqliteHome: profile.sqliteHome } : {})
  };
}

function legacyCoreReadInput(input, allowedKeys) {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    throw new CoreError("INVALID_INPUT", "The legacy Core read input is invalid.");
  }
  const allowed = new Set(["profileId", ...allowedKeys]);
  if (Object.keys(input).some((key) => !allowed.has(key))) {
    throw new CoreError("INVALID_INPUT", "The legacy Core read input is invalid.");
  }
  const profileId = requireString(input.profileId ?? "default", "profileId", { maxLength: 80 });
  return {
    profile: { profileId },
    ...Object.fromEntries(allowedKeys
      .filter((key) => input[key] !== undefined)
      .map((key) => [key, input[key]]))
  };
}

function captureProfileRevision(profileId, suppliedRevision, stateStore, response) {
  const profile = stateStore.getProfile(profileId);
  if (typeof suppliedRevision !== "string" || !suppliedRevision) {
    sendJson(response, 409, {
      error: "This operation requires the current storage profile revision. Refresh the profile and try again.",
      code: "PROFILE_REVISION_REQUIRED",
      profile
    });
    return null;
  }
  if (suppliedRevision !== profile.revision) {
    sendJson(response, 409, {
      error: "The storage profile changed after this operation was prepared. Refresh and confirm again.",
      code: "PROFILE_CHANGED",
      profile
    });
    return null;
  }
  return profile;
}

function captureStorageProfile(input, stateStore, response) {
  if (Object.hasOwn(input ?? {}, "codexHome") || Object.hasOwn(input ?? {}, "sqliteHome")) {
    throw new Error("Storage paths must be selected through a server-managed profileId.");
  }
  const profileId = requireString(input?.profileId ?? "default", "profileId", { maxLength: 80 });
  const profile = captureProfileRevision(profileId, input?.profileRevision, stateStore, response);
  if (!profile) return null;
  const snapshot = {
    profileId: profile.id,
    codexHome: profile.codexHome,
    ...(profile.sqliteHome ? { sqliteHome: profile.sqliteHome } : {})
  };
  return Object.freeze(snapshot);
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
  let cspNonce = null;
  if (extension === ".html") {
    cspNonce = crypto.randomBytes(18).toString("base64url");
    file = Buffer.from(file.toString("utf8")
      .replace("__CODEX_PROVIDER_SYNC_BOOTSTRAP__", "{}")
      .replaceAll("__CPS_CSP_NONCE__", cspNonce), "utf8");
  }
  const nonceSource = cspNonce ? ` 'nonce-${cspNonce}'` : "";
  response.writeHead(200, {
    "Cache-Control": extension === ".html" ? "no-store" : "public, max-age=3600",
    "Content-Type": MIME_TYPES.get(extension) ?? "application/octet-stream",
    "Content-Length": file.length,
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Content-Security-Policy": `default-src 'self'; script-src 'self'${nonceSource}; style-src 'self'${nonceSource}; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'`
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

function internalChallengeRequestPayload({ port, instanceId }) {
  return {
    protocolVersion: INTERNAL_PROTOCOL_VERSION,
    port,
    instanceId
  };
}

function internalChallengePayload({ port, instanceId, nonce }) {
  return {
    protocolVersion: INTERNAL_PROTOCOL_VERSION,
    port,
    instanceId,
    nonce
  };
}

function internalRequestPayload({ port, instanceId, nonce, resetAccess }) {
  return {
    protocolVersion: INTERNAL_PROTOCOL_VERSION,
    port,
    instanceId,
    nonce,
    resetAccess: Boolean(resetAccess)
  };
}

function internalResponsePayload({ port, instanceId, nonce, resetAccess, pairingToken }) {
  return {
    protocolVersion: INTERNAL_PROTOCOL_VERSION,
    port,
    instanceId,
    nonce,
    resetAccess: Boolean(resetAccess),
    pairingToken
  };
}

function internalProof(secret, domain, payload) {
  return crypto.createHmac("sha256", secret)
    .update(`${domain}\n${JSON.stringify(payload)}`, "utf8")
    .digest("base64url");
}

function internalProofsMatch(supplied, expected) {
  if (typeof supplied !== "string" || !/^[A-Za-z0-9_-]{43}$/.test(supplied)) return false;
  const candidate = Buffer.from(supplied, "base64url");
  const expectedBuffer = Buffer.from(expected, "base64url");
  return candidate.length === expectedBuffer.length && crypto.timingSafeEqual(candidate, expectedBuffer);
}

function validSecretToken(value) {
  return typeof value === "string"
    && /^[A-Za-z0-9_-]{43}$/.test(value)
    && Buffer.from(value, "base64url").length === INTERNAL_NONCE_BYTES;
}

function validInternalNonce(value) {
  return validSecretToken(value);
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
  instanceId = randomSecret(),
  pairingTtlMs = PAIRING_TTL_MS,
  now = () => Date.now(),
  platform = process.platform,
  environment = process.env
} = {}) {
  const api = {
    applyRestore: services.applyRestore ?? applyRestore,
    applyRepair: services.applyRepair ?? applyRepair,
    applySwitch: services.applySwitch ?? applySwitch,
    applySync: services.applySync ?? applySync,
    prepareRestore: services.prepareRestore ?? prepareRestore,
    prepareRepair: services.prepareRepair ?? prepareRepair,
    prepareSwitch: services.prepareSwitch ?? prepareSwitch,
    prepareSync: services.prepareSync ?? prepareSync,
    runPruneBackups: services.runPruneBackups ?? runPruneBackups,
    readConfigText: services.readConfigText ?? readConfigText,
    readRootModelFromConfigText: services.readRootModelFromConfigText ?? readRootModelFromConfigText,
  };
  const coreFacade = services.coreFacade ?? createWebCoreFacade(stateStore);
  const activity = [];
  let activityId = 0;
  let activeOperation = null;
  let baseUrl = null;
  let pairing = null;
  const internalChallenges = new Map();
  const activeCoreOperations = new Map();

  const callLegacyCoreRead = async (method, payload, response) => {
    const dispatched = await dispatchWebCoreRequest(coreFacade, {
      protocolVersion: 1,
      requestId: crypto.randomUUID(),
      method,
      payload
    });
    if (!dispatched.envelope.ok) {
      sendJson(response, dispatched.statusCode, {
        error: dispatched.envelope.error.message,
        code: dispatched.envelope.error.code,
        coreError: dispatched.envelope.error
      });
      return null;
    }
    return dispatched.envelope.result;
  };

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
      const outcome = typeof result?.outcome === "string"
        ? result.outcome
        : (Array.isArray(result?.skippedLockedRolloutFiles) && result.skippedLockedRolloutFiles.length > 0
            ? "partial"
            : (Array.isArray(result?.skippedChangedRolloutFiles) && result.skippedChangedRolloutFiles.length > 0
                ? "partial"
                : "success"));
      const publicResult = { ...result };
      for (const key of ["skippedLockedRolloutFiles", "skippedChangedRolloutFiles"]) {
        if (Array.isArray(publicResult[key])) {
          publicResult[key] = publicResult[key]
            .filter((entry) => typeof entry === "string")
            .map((entry) => path.win32.basename(path.posix.basename(entry)));
        }
      }
      record(outcome === "partial" ? "warning" : "success", `${kind} completed`);
      sendJson(response, 200, { result: { ...publicResult, outcome } });
    } catch (error) {
      record("error", `${kind} failed`, typeof error?.code === "string" ? error.code : "INTERNAL_ERROR");
      sendError(response, coreErrorHttpStatus(error, 400), error);
    } finally {
      activeOperation = null;
    }
  };

  const capturePrepareProfile = (input, response) => {
    if (Object.hasOwn(input ?? {}, "codexHome") || Object.hasOwn(input ?? {}, "sqliteHome")) {
      throw new Error("Storage paths must be selected through a server-managed profileId.");
    }
    const profileId = requireString(input?.profileId ?? "default", "profileId", { maxLength: 80 });
    const profile = captureProfileRevision(profileId, input?.profileRevision, stateStore, response);
    if (!profile) return null;
    return Object.freeze({
      id: profile.id,
      revision: profile.revision,
      codexHome: profile.codexHome,
      ...(profile.sqliteHome ? { sqliteHome: profile.sqliteHome } : {})
    });
  };

  const resolveCurrentProfile = async (profileId) => {
    const profile = stateStore.getProfile(profileId);
    return {
      id: profile.id,
      revision: profile.revision,
      codexHome: profile.codexHome,
      ...(profile.sqliteHome ? { sqliteHome: profile.sqliteHome } : {})
    };
  };

  const requirePlanApply = (input) => {
    if (!input || typeof input !== "object" || Array.isArray(input)
        || Object.keys(input).sort().join(",") !== "planId,schemaVersion"
        || input.schemaVersion !== 1
        || typeof input.planId !== "string"
        || !input.planId) {
      throw new CoreError("INVALID_INPUT", "Apply accepts exactly { schemaVersion: 1, planId }.");
    }
    return { schemaVersion: 1, planId: input.planId };
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

      if (request.method === "POST" && pathname === "/api/internal/challenge") {
        const body = await readJsonBody(request);
        const address = server.address();
        const actualPort = typeof address === "object" && address ? address.port : null;
        if (body.protocolVersion !== INTERNAL_PROTOCOL_VERSION
            || body.port !== actualPort
            || body.instanceId !== instanceId) {
          sendError(response, 403, "Invalid Web UI authentication challenge.", "INVALID_INTERNAL_CHALLENGE");
          return;
        }
        const currentTime = now();
        for (const [nonce, expiresAt] of internalChallenges) {
          if (expiresAt <= currentTime) internalChallenges.delete(nonce);
        }
        const nonce = crypto.randomBytes(INTERNAL_NONCE_BYTES).toString("base64url");
        internalChallenges.set(nonce, currentTime + INTERNAL_CHALLENGE_TTL_MS);
        while (internalChallenges.size > INTERNAL_CHALLENGE_LIMIT) {
          internalChallenges.delete(internalChallenges.keys().next().value);
        }
        sendJson(response, 200, internalChallengePayload({ port: actualPort, instanceId, nonce }));
        return;
      }

      if (request.method === "POST" && pathname === "/api/internal/new-pairing") {
        const body = await readJsonBody(request);
        const address = server.address();
        const actualPort = typeof address === "object" && address ? address.port : null;
        if (body.protocolVersion !== INTERNAL_PROTOCOL_VERSION
            || body.port !== actualPort
            || body.instanceId !== instanceId
            || !validInternalNonce(body.nonce)
            || typeof body.resetAccess !== "boolean") {
          sendError(response, 403, "Invalid authenticated Web UI pairing request.", "INVALID_INTERNAL_PROOF");
          return;
        }
        const signedRequest = internalRequestPayload(body);
        const expectedProof = internalProof(internalSecret, INTERNAL_REQUEST_DOMAIN, signedRequest);
        if (!internalProofsMatch(request.headers["x-codex-provider-internal-proof"], expectedProof)) {
          sendError(response, 403, "Invalid authenticated Web UI pairing request.", "INVALID_INTERNAL_PROOF");
          return;
        }
        const currentTime = now();
        for (const [nonce, expiresAt] of internalChallenges) {
          if (expiresAt <= currentTime) internalChallenges.delete(nonce);
        }
        if (!internalChallenges.has(body.nonce)) {
          sendError(response, 403, "The Web UI authentication challenge is missing, expired, or already used.", "INTERNAL_CHALLENGE_REQUIRED");
          return;
        }
        internalChallenges.delete(body.nonce);
        if (body.resetAccess) await stateStore.resetCredentials();
        const payload = internalResponsePayload({
          port: actualPort,
          instanceId,
          nonce: body.nonce,
          resetAccess: body.resetAccess,
          pairingToken: issuePairing()
        });
        sendJson(response, 200, {
          ...payload,
          proof: internalProof(internalSecret, INTERNAL_RESPONSE_DOMAIN, payload)
        });
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
        if (pathname === "/api/core/cancel") {
          const allowedKeys = body?.operationId === undefined
            ? ["protocolVersion", "requestId"]
            : ["operationId", "protocolVersion", "requestId"];
          const valid = body
            && typeof body === "object"
            && !Array.isArray(body)
            && Object.keys(body).sort().join(",") === allowedKeys.sort().join(",")
            && body.protocolVersion === 1
            && typeof body.requestId === "string"
            && body.requestId.length > 0
            && body.requestId.length <= 512
            && (body.operationId === undefined
              || (typeof body.operationId === "string"
                && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(body.operationId)));
          if (!valid) {
            sendJson(response, 400, { accepted: false });
            return;
          }
          const active = activeCoreOperations.get(body.requestId);
          const accepted = Boolean(
            active
            && (!body.operationId || body.operationId === active.operationId)
          );
          if (accepted) active.controller.abort();
          sendJson(response, 200, { accepted });
          return;
        }
        if (pathname === "/api/core") {
          const contentType = String(request.headers["content-type"] ?? "").toLowerCase();
          if (!contentType.startsWith("application/json")) {
            const dispatched = await dispatchWebCoreRequest(coreFacade, {
              protocolVersion: body?.protocolVersion,
              requestId: body?.requestId,
              operationId: body?.operationId,
              method: body?.method,
              payload: { invalidContentType: true }
            });
            record("warning", "Core request rejected", dispatched.activity);
            sendJson(response, 415, dispatched.envelope);
            return;
          }
          const wantsStream = String(request.headers.accept ?? "")
            .toLowerCase()
            .split(",")
            .some((entry) => entry.trim().startsWith(CORE_STREAM_CONTENT_TYPE));
          if (wantsStream) {
            const controller = new AbortController();
            let completed = false;
            let registeredRequestId = null;
            startCoreStream(response);
            response.once("close", () => {
              if (!completed) controller.abort();
            });
            const dispatched = await dispatchWebCoreRequest(coreFacade, body, {
              signal: controller.signal,
              onRequestValidated(validatedRequest) {
                if (!CORE_APPLY_METHODS.has(validatedRequest.method)) return;
                if (activeCoreOperations.has(validatedRequest.requestId)) {
                  throw Object.assign(new Error("A Core request with this requestId is already active."), {
                    code: "OPERATION_BUSY",
                    details: { busyScope: "web-request" }
                  });
                }
                registeredRequestId = validatedRequest.requestId;
                activeCoreOperations.set(registeredRequestId, {
                  controller,
                  operationId: null
                });
              },
              onOperationStarted(event) {
                if (registeredRequestId) {
                  const active = activeCoreOperations.get(registeredRequestId);
                  if (active?.controller === controller) active.operationId = event.operationId;
                }
                writeCoreStream(response, event);
              },
              onProgress(event) {
                writeCoreStream(response, event);
              },
              onRequestSettled() {
                if (!registeredRequestId) return;
                const active = activeCoreOperations.get(registeredRequestId);
                if (active?.controller === controller) activeCoreOperations.delete(registeredRequestId);
              }
            });
            record(
              dispatched.activity.ok ? "info" : "warning",
              dispatched.activity.ok ? "Core request completed" : "Core request rejected",
              dispatched.activity
            );
            completed = true;
            writeCoreStream(response, dispatched.envelope);
            response.end();
            return;
          }
          const dispatched = await dispatchWebCoreRequest(coreFacade, body);
          record(
            dispatched.activity.ok ? "info" : "warning",
            dispatched.activity.ok ? "Core request completed" : "Core request rejected",
            dispatched.activity
          );
          sendJson(response, dispatched.statusCode, dispatched.envelope);
          return;
        }
        if (pathname === "/api/profiles/save") {
          const profileId = requireString(body.profileId, "profileId", { maxLength: 80 });
          try {
            const profile = await stateStore.saveProfile({
              id: profileId,
              name: requireString(body.name, "name", { maxLength: 120 }),
              codexHome: requireString(body.codexHome, "codexHome"),
              sqliteHome: requireString(body.sqliteHome, "sqliteHome", { optional: true })
            }, { expectedRevision: body.profileRevision });
            sendJson(response, 200, { profile });
          } catch (error) {
            if (!(error instanceof ProfileRevisionConflictError)) throw error;
            sendJson(response, 409, {
              error: error.message,
              code: error.code,
              profile: error.profile
            });
          }
          return;
        }

        if (pathname === "/api/profiles/delete") {
          const profileId = requireString(body.profileId, "profileId", { maxLength: 80 });
          if (!captureProfileRevision(profileId, body.profileRevision, stateStore, response)) return;
          await stateStore.deleteProfile(profileId);
          sendJson(response, 200, { ok: true });
          return;
        }

        if (pathname === "/api/access/forget") {
          await stateStore.removeCredential(request.headers["x-codex-provider-device"]);
          sendJson(response, 200, { ok: true });
          return;
        }

        if (pathname === "/api/status") {
          const input = legacyCoreReadInput(body, []);
          const status = await callLegacyCoreRead("getStatus", input, response);
          if (!status) return;
          record("info", "Status refreshed", { profileId: input.profile.profileId }, null);
          sendJson(response, 200, { status });
          return;
        }

        if (pathname === "/api/backups") {
          const input = legacyCoreReadInput(body, []);
          const result = await callLegacyCoreRead("listBackups", input, response);
          if (!result) return;
          sendJson(response, 200, result);
          return;
        }

        if (pathname === "/api/history") {
          const input = legacyCoreReadInput(body, ["page", "pageSize", "query", "project", "provider", "archived"]);
          const history = await callLegacyCoreRead("listHistory", input, response);
          if (!history) return;
          sendJson(response, 200, { history });
          return;
        }

        if (pathname === "/api/history/session") {
          const input = legacyCoreReadInput(body, ["sessionId", "messageLimit"]);
          const history = await callLegacyCoreRead("getHistorySession", input, response);
          if (!history) return;
          sendJson(response, 200, { history });
          return;
        }

        if (pathname === "/api/diagnostics") {
          const input = legacyCoreReadInput(body, []);
          const diagnostics = await callLegacyCoreRead("getDiagnostics", input, response);
          if (!diagnostics) return;
          sendJson(response, 200, { diagnostics });
          return;
        }

        if (pathname === "/api/sync/prepare") {
          const profile = capturePrepareProfile(body, response);
          if (!profile) return;
          if (body.provider !== undefined || body.syncMode !== undefined || body.fast !== undefined) {
            throw new CoreError("INVALID_INPUT", "Sync always uses the current config.toml Provider.");
          }
          const plan = await api.prepareSync({
            codexHome: profile.codexHome,
            ...(profile.sqliteHome ? { sqliteHome: profile.sqliteHome } : {}),
            profile: { id: profile.id, revision: profile.revision },
            profileResolver: resolveCurrentProfile,
            keepCount: requireKeepCount(body.keepCount),
            platform
          });
          sendJson(response, 200, { plan });
          return;
        }

        if (pathname === "/api/sync/apply") {
          await withOperation("sync", response, () => api.applySync(requirePlanApply(body)));
          return;
        }

        if (pathname === "/api/switch/prepare") {
          const profile = capturePrepareProfile(body, response);
          if (!profile) return;
          const modelMode = body.modelMode ?? (body.keepRootModel ? "keep-root-model" : (body.model ? "explicit" : "provider-default"));
          if (body.syncMode !== undefined || body.fast !== undefined) {
            throw new CoreError("INVALID_INPUT", "Switch no longer accepts a sync mode.");
          }
          if (!["provider-default", "keep-root-model", "explicit"].includes(modelMode)) {
            throw new CoreError("INVALID_INPUT", "modelMode must be provider-default, keep-root-model, or explicit.");
          }
          const model = modelMode === "explicit"
            ? requireString(body.model, "model", { maxLength: 500 })
            : undefined;
          if (modelMode !== "explicit" && body.model !== undefined && body.model !== null && body.model !== "") {
            throw new CoreError("INVALID_INPUT", "model is only accepted when modelMode is explicit.");
          }
          const plan = await api.prepareSwitch({
            codexHome: profile.codexHome,
            ...(profile.sqliteHome ? { sqliteHome: profile.sqliteHome } : {}),
            profile: { id: profile.id, revision: profile.revision },
            profileResolver: resolveCurrentProfile,
            provider: requireProvider(body.provider),
            model,
            keepRootModel: modelMode === "keep-root-model",
            keepCount: requireKeepCount(body.keepCount),
            platform
          });
          sendJson(response, 200, { plan });
          return;
        }

        if (pathname === "/api/switch/apply") {
          await withOperation("switch", response, () => api.applySwitch(requirePlanApply(body)));
          return;
        }

        if (pathname === "/api/repair/prepare") {
          const profile = capturePrepareProfile(body, response);
          if (!profile) return;
          const targets = Array.isArray(body.targets) ? body.targets : [];
          const plan = await api.prepareRepair({
            codexHome: profile.codexHome,
            ...(profile.sqliteHome ? { sqliteHome: profile.sqliteHome } : {}),
            profile: { id: profile.id, revision: profile.revision },
            profileResolver: resolveCurrentProfile,
            targets,
            keepCount: requireKeepCount(body.keepCount),
            platform
          });
          sendJson(response, 200, { plan });
          return;
        }

        if (pathname === "/api/repair/apply") {
          await withOperation("repair", response, () => api.applyRepair(requirePlanApply(body)));
          return;
        }

        if (pathname === "/api/restore/prepare") {
          const profile = capturePrepareProfile(body, response);
          if (!profile) return;
          const restoreConfig = Boolean(body.restoreConfig);
          const restoreDatabase = Boolean(body.restoreDatabase);
          const restoreSessions = Boolean(body.restoreSessions);
          if (!restoreConfig && !restoreDatabase && !restoreSessions) {
            throw new CoreError("INVALID_INPUT", "Select at least one backup content type to restore.");
          }
          if (body.allowSqliteHomeRelocation && !profile.sqliteHome) {
            throw new CoreError("INVALID_INPUT", "SQLite Home relocation requires a storage profile with an explicit SQLite Home target.");
          }
          const plan = await api.prepareRestore({
            codexHome: profile.codexHome,
            ...(profile.sqliteHome ? { sqliteHome: profile.sqliteHome } : {}),
            profile: { id: profile.id, revision: profile.revision },
            profileResolver: resolveCurrentProfile,
            backupId: requireString(body.backupId, "backupId", { maxLength: 300 }),
            restoreConfig,
            restoreDatabase,
            restoreSessions,
            allowSqliteHomeRelocation: Boolean(body.allowSqliteHomeRelocation),
            platform
          });
          sendJson(response, 200, { plan });
          return;
        }

        if (pathname === "/api/restore/apply") {
          await withOperation("restore", response, () => api.applyRestore(requirePlanApply(body)));
          return;
        }

        if (pathname === "/api/sync" || pathname === "/api/switch" || pathname === "/api/restore") {
          sendError(
            response,
            410,
            `Direct write endpoint ${pathname} is retired. Use ${pathname}/prepare, show the returned plan, then submit only { schemaVersion, planId } to ${pathname}/apply.`,
            "PLAN_REQUIRED"
          );
          return;
        }

        if (pathname === "/api/prune") {
          const storage = captureStorageProfile(body, stateStore, response);
          if (!storage) return;
          await withOperation("prune backups", response, async () => {
            return api.runPruneBackups({ codexHome: storage.codexHome, keepCount: requireKeepCount(body.keepCount, { allowZero: true }) });
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
      if (request.aborted || response.destroyed || response.writableEnded) return;
      if (error instanceof WebRequestError) {
        sendError(response, error.statusCode, error, error.code);
      } else {
        sendError(response, coreErrorHttpStatus(error, 500), error);
      }
    }
  });

  return {
    server,
    internalSecret,
    instanceId,
    issuePairing,
    setBaseUrl(value) {
      baseUrl = value;
    },
    getActivity() {
      return [...activity];
    }
  };
}

function requestExistingChallenge({ port, instanceId }) {
  return new Promise((resolve, reject) => {
    const challengeRequest = internalChallengeRequestPayload({ port, instanceId });
    const body = JSON.stringify(challengeRequest);
    const request = http.request({
      hostname: "127.0.0.1",
      port,
      path: "/api/internal/challenge",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(body)
      },
      timeout: 1500
    }, (response) => {
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("end", () => {
        try {
          const payload = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          if (response.statusCode !== 200
              || payload.protocolVersion !== INTERNAL_PROTOCOL_VERSION
              || payload.port !== port
              || payload.instanceId !== instanceId
              || !validInternalNonce(payload.nonce)) {
            reject(new Error("The existing listener is not a compatible Codex Provider Sync Web UI."));
            return;
          }
          resolve(internalChallengePayload(payload));
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

function requestExistingPairing({ port, instanceId, internalSecret, resetAccess, challenge }) {
  return new Promise((resolve, reject) => {
    const signedRequest = internalRequestPayload({
      port,
      instanceId,
      nonce: challenge.nonce,
      resetAccess
    });
    const body = JSON.stringify(signedRequest);
    const request = http.request({
      hostname: "127.0.0.1",
      port,
      path: "/api/internal/new-pairing",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(body),
        "X-Codex-Provider-Internal-Proof": internalProof(internalSecret, INTERNAL_REQUEST_DOMAIN, signedRequest)
      },
      timeout: 1500
    }, (response) => {
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("end", () => {
        try {
          const payload = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          if (response.statusCode !== 200
              || payload.protocolVersion !== INTERNAL_PROTOCOL_VERSION
              || payload.port !== port
              || payload.instanceId !== instanceId
              || payload.nonce !== challenge.nonce
              || payload.resetAccess !== Boolean(resetAccess)
              || typeof payload.pairingToken !== "string"
              || !payload.pairingToken
              || payload.pairingToken.length > 4096) {
            reject(new Error("The existing listener is not a compatible Codex Provider Sync Web UI."));
            return;
          }
          const signedResponse = internalResponsePayload({
            port: payload.port,
            instanceId: payload.instanceId,
            nonce: payload.nonce,
            resetAccess: payload.resetAccess,
            pairingToken: payload.pairingToken
          });
          const expectedProof = internalProof(internalSecret, INTERNAL_RESPONSE_DOMAIN, signedResponse);
          if (!internalProofsMatch(payload.proof, expectedProof)) {
            reject(new Error("The existing listener failed authenticated Web UI pairing."));
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
    if (Number.isInteger(value?.port) && value.port > 0 && validSecretToken(value?.internalSecret)) {
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

function comparableRuntimePath(value, platform) {
  return platform === "win32" ? value.toLowerCase() : value;
}

function hasRuntimeIdentity(value) {
  return typeof value?.codexHome === "string" && typeof value?.sqliteHome === "string";
}

function hasSecureRuntimeDescriptor(value) {
  return value?.protocolVersion === INTERNAL_PROTOCOL_VERSION
    && validSecretToken(value?.instanceId);
}

function runtimeIdentityMatches(existing, requested, platform) {
  return comparableRuntimePath(existing.codexHome, platform) === comparableRuntimePath(requested.codexHome, platform)
    && comparableRuntimePath(existing.sqliteHome, platform) === comparableRuntimePath(requested.sqliteHome, platform);
}

function runtimeIdentityMismatchError(existing, requested) {
  return new Error(
    `A Web UI instance is already running on port ${existing.port} for Codex Home "${existing.codexHome}" and SQLite Home "${existing.sqliteHome}", `
    + `but this launch resolved Codex Home "${requested.codexHome}" and SQLite Home "${requested.sqliteHome}". `
    + "Close the existing Web UI instance and restart with the requested storage identity."
  );
}

function legacyRuntimeDescriptorError(existing) {
  return new Error(
    `A Web UI instance is already running on port ${existing.port}, but its runtime descriptor does not contain the authenticated v2 instance and storage identity. `
    + "Close that Web UI instance and restart it so the secure runtime identity can be recorded."
  );
}

function isConnectionRefused(error) {
  return error?.code === "ECONNREFUSED" || error?.cause?.code === "ECONNREFUSED";
}

async function resolveRuntimeIdentity({ codexHome, sqliteHome, environment, platform }) {
  let configText = "";
  try {
    configText = await readConfigText(path.join(codexHome, "config.toml"));
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  const layout = resolveStorageLayout({ codexHome, sqliteHome, configText, env: environment, platform });
  return { codexHome: layout.codexHome, sqliteHome: layout.sqliteHome };
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

  const controlCodexHome = path.resolve(codexHome ?? environment.CODEX_HOME ?? defaultCodexHome());
  const resolvedStateFile = path.resolve(stateFile ?? path.join(controlCodexHome, STATE_FILENAME));
  const resolvedRuntimeFile = path.resolve(runtimeFile ?? path.join(controlCodexHome, RUNTIME_FILENAME));
  const runtimeIdentity = await resolveRuntimeIdentity({ codexHome: controlCodexHome, sqliteHome, environment, platform });
  const existing = await readRuntimeDescriptor(resolvedRuntimeFile);
  if (existing) {
    let challenge = null;
    const descriptorInstanceId = hasSecureRuntimeDescriptor(existing) ? existing.instanceId : "legacy";
    try {
      challenge = await requestExistingChallenge({
        port: existing.port,
        instanceId: descriptorInstanceId
      });
    } catch (error) {
      if (isConnectionRefused(error)) {
        await removeOwnedRuntimeDescriptor(resolvedRuntimeFile, existing.internalSecret);
      } else {
        throw new Error("The existing listener could not complete secure Web UI authentication. Close it and restart the Web UI.", { cause: error });
      }
    }
    if (challenge) {
      if (!hasSecureRuntimeDescriptor(existing) || !hasRuntimeIdentity(existing)) throw legacyRuntimeDescriptorError(existing);
      if (!runtimeIdentityMatches(existing, runtimeIdentity, platform)) {
        throw runtimeIdentityMismatchError(existing, runtimeIdentity);
      }
      let pairingToken;
      try {
        pairingToken = await requestExistingPairing({
          port: existing.port,
          instanceId: existing.instanceId,
          internalSecret: existing.internalSecret,
          resetAccess,
          challenge
        });
      } catch (error) {
        throw new Error("The existing listener failed authenticated Web UI pairing. Close it and restart the Web UI.", { cause: error });
      }
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
    }
  }

  const stateStore = new WebUiStateStore({
    filePath: resolvedStateFile,
    defaultProfile: { codexHome: controlCodexHome, sqliteHome }
  });
  await stateStore.initialize({ resetAccess });
  const internalSecret = randomSecret();
  const instanceId = randomSecret();
  const handle = createWebUiServer({ webRoot, services, stateStore, internalSecret, instanceId, platform, environment });
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
  await fs.writeFile(resolvedRuntimeFile, `${JSON.stringify({ protocolVersion: INTERNAL_PROTOCOL_VERSION, instanceId, port: actualPort, internalSecret, pid: process.pid, ...runtimeIdentity })}\n`, { encoding: "utf8", mode: 0o600 });
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
