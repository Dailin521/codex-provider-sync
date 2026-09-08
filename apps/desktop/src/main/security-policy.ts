import fs from "node:fs/promises";
import path from "node:path";

import {
  DESKTOP_APP_HOST,
  DESKTOP_APP_SCHEME,
  DESKTOP_CSP
} from "../shared/constants.js";

const MIME_TYPES: Readonly<Record<string, string>> = Object.freeze({
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".woff": "font/woff",
  ".woff2": "font/woff2"
});

function isWithinRoot(root: string, candidate: string): boolean {
  const relative = path.relative(root, candidate);
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

export async function resolveRendererAsset(
  rendererRoot: string,
  requestUrl: string
): Promise<{ filePath: string; contentType: string }> {
  const url = new URL(requestUrl);
  if (url.protocol !== `${DESKTOP_APP_SCHEME}:`
      || url.hostname !== DESKTOP_APP_HOST
      || url.username
      || url.password
      || url.port
      || url.search
      || url.hash) {
    throw new TypeError("Invalid desktop asset URL.");
  }
  let decoded: string;
  try {
    decoded = decodeURIComponent(url.pathname);
  } catch {
    throw new TypeError("Invalid desktop asset encoding.");
  }
  if (decoded.includes("\0") || decoded.includes("\\") || decoded.includes("%")) {
    throw new TypeError("Invalid desktop asset path.");
  }
  const segments = (decoded === "/" ? "/index.html" : decoded)
    .split("/")
    .filter(Boolean);
  if (segments.length < 1 || segments.some((segment) => segment === "." || segment === "..")) {
    throw new TypeError("Invalid desktop asset path.");
  }
  const root = await fs.realpath(rendererRoot);
  const candidate = path.resolve(root, ...segments);
  if (!isWithinRoot(root, candidate)) throw new TypeError("Desktop asset escaped its root.");
  const physical = await fs.realpath(candidate);
  if (!isWithinRoot(root, physical)) throw new TypeError("Desktop asset escaped its root.");
  const stats = await fs.stat(physical);
  if (!stats.isFile()) throw new TypeError("Desktop asset is not a regular file.");
  return {
    filePath: physical,
    contentType: MIME_TYPES[path.extname(physical).toLowerCase()] ?? "application/octet-stream"
  };
}

export async function createRendererAssetResponse(
  rendererRoot: string,
  request: Request
): Promise<Response> {
  if (request.method !== "GET" && request.method !== "HEAD") {
    return new Response(null, { status: 405, headers: { Allow: "GET, HEAD" } });
  }
  try {
    const asset = await resolveRendererAsset(rendererRoot, request.url);
    const body = request.method === "HEAD" ? null : await fs.readFile(asset.filePath);
    return new Response(body, {
      status: 200,
      headers: {
        "Content-Type": asset.contentType,
        "Content-Security-Policy": DESKTOP_CSP,
        "Cache-Control": "no-store",
        "Cross-Origin-Opener-Policy": "same-origin",
        "X-Content-Type-Options": "nosniff"
      }
    });
  } catch {
    return new Response(null, {
      status: 404,
      headers: {
        "Content-Security-Policy": DESKTOP_CSP,
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff"
      }
    });
  }
}

export function createSecureWebPreferences(preload: string): Readonly<Record<string, unknown>> {
  return Object.freeze({
    preload,
    nodeIntegration: false,
    nodeIntegrationInWorker: false,
    contextIsolation: true,
    sandbox: true,
    webSecurity: true,
    allowRunningInsecureContent: false,
    experimentalFeatures: false,
    webviewTag: false
  });
}
