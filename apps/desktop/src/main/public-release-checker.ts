export const PUBLIC_RELEASES_URL = "https://github.com/Dailin521/codex-provider-sync/releases";

const PUBLIC_RELEASES_API_URL = "https://api.github.com/repos/Dailin521/codex-provider-sync/releases?per_page=30";
const RESPONSE_LIMIT_BYTES = 2 * 1024 * 1024;
const DEFAULT_TIMEOUT_MS = 15_000;

type FetchResponse = Pick<Response, "ok" | "status" | "body" | "text">;
type FetchImplementation = (input: string, init: RequestInit) => Promise<FetchResponse>;

export interface PublicDesktopReleaseCheckOptions {
  appVersion: string;
  platform: NodeJS.Platform;
  arch: string;
  fetchImpl?: FetchImplementation;
  /** Test-only override; production checks time out after 15 seconds. */
  timeoutMs?: number;
}

interface ReleaseRecord {
  draft?: unknown;
  prerelease?: unknown;
  tag_name?: unknown;
  assets?: unknown;
}

interface ReleaseAsset {
  name?: unknown;
}

interface ParsedVersion {
  major: bigint;
  minor: bigint;
  patch: bigint;
  prerelease: boolean;
}

const STABLE_VERSION = /^v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
const SEMVER_PRERELEASE_IDENTIFIER = "(?:0|[1-9]\\d*|\\d*[A-Za-z-][0-9A-Za-z-]*)";
const CURRENT_VERSION = new RegExp(
  `^v?(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-(${SEMVER_PRERELEASE_IDENTIFIER}(?:\\.${SEMVER_PRERELEASE_IDENTIFIER})*))?(?:\\+[0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*)?$`
);

function parseStableVersion(value: unknown): ParsedVersion | null {
  if (typeof value !== "string") return null;
  const match = STABLE_VERSION.exec(value);
  if (!match) return null;
  return {
    major: BigInt(match[1]),
    minor: BigInt(match[2]),
    patch: BigInt(match[3]),
    prerelease: false
  };
}

function parseCurrentVersion(value: string): ParsedVersion | null {
  const match = CURRENT_VERSION.exec(value);
  if (!match) return null;
  return {
    major: BigInt(match[1]),
    minor: BigInt(match[2]),
    patch: BigInt(match[3]),
    prerelease: match[4] !== undefined
  };
}

function compareVersion(candidate: ParsedVersion, current: ParsedVersion): number {
  for (const part of ["major", "minor", "patch"] as const) {
    if (candidate[part] !== current[part]) return candidate[part] > current[part] ? 1 : -1;
  }
  return candidate.prerelease === current.prerelease ? 0 : candidate.prerelease ? -1 : 1;
}

function assetNamesFor(platform: NodeJS.Platform, arch: string, version: string): string[] {
  if (platform === "win32" && arch === "x64") {
    return [
      `CodexProviderSync-${version}-windows-x64-setup.exe`,
      `CodexProviderSync-${version}-windows-x64-portable.zip`
    ];
  }
  if (platform === "darwin" && (arch === "x64" || arch === "arm64")) {
    return [
      `CodexProviderSync-${version}-macos-${arch}.dmg`,
      `CodexProviderSync-${version}-macos-${arch}.zip`
    ];
  }
  if (platform === "linux" && arch === "x64") {
    return [
      `CodexProviderSync-${version}-linux-x64.AppImage`,
      `CodexProviderSync-${version}-linux-x64.deb`
    ];
  }
  return [];
}

async function readLimitedText(
  response: FetchResponse,
  setActiveReader: (reader: ReadableStreamDefaultReader<Uint8Array> | undefined) => void
): Promise<string> {
  const reader = response.body?.getReader();
  setActiveReader(reader);
  if (!reader) {
    const text = await response.text();
    if (new TextEncoder().encode(text).byteLength > RESPONSE_LIMIT_BYTES) {
      throw new Error("Public release response exceeds the size limit.");
    }
    return text;
  }

  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value) continue;
      total += value.byteLength;
      if (total > RESPONSE_LIMIT_BYTES) {
        await reader.cancel();
        throw new Error("Public release response exceeds the size limit.");
      }
      chunks.push(value);
    }
  } finally {
    setActiveReader(undefined);
    reader.releaseLock();
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(bytes);
}

function releaseHasTargetAsset(release: ReleaseRecord, platform: NodeJS.Platform, arch: string, version: string): boolean {
  if (!Array.isArray(release.assets)) return false;
  const expectedNames = new Set(assetNamesFor(platform, arch, version));
  return release.assets.some((asset) => asset !== null
    && typeof asset === "object"
    && expectedNames.has((asset as ReleaseAsset).name as string));
}

/**
 * Manually checks the fixed public Releases feed. This is deliberately read-only:
 * it neither downloads an asset nor exposes release URLs or notes to the renderer.
 */
export async function checkPublicDesktopRelease(
  options: PublicDesktopReleaseCheckOptions
): Promise<{ version: string } | null> {
  const current = parseCurrentVersion(options.appVersion);
  const supportedAssets = assetNamesFor(options.platform, options.arch, "0.0.0");
  if (!current || supportedAssets.length === 0) return null;

  const fetchImpl = options.fetchImpl ?? globalThis.fetch;
  if (typeof fetchImpl !== "function") throw new Error("Fetch is unavailable for the public release check.");
  const timeoutMs = Number.isFinite(options.timeoutMs)
    ? Math.max(0, options.timeoutMs as number)
    : DEFAULT_TIMEOUT_MS;
  const controller = new AbortController();
  let activeReader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(() => {
      controller.abort();
      if (activeReader) void activeReader.cancel().catch(() => {});
      reject(new Error("Public release check timed out."));
    }, timeoutMs);
    timeout.unref?.();
  });

  try {
    const work = (async () => {
      const response = await fetchImpl(PUBLIC_RELEASES_API_URL, {
        method: "GET",
        headers: { accept: "application/vnd.github+json" },
        credentials: "omit",
        signal: controller.signal
      });
      if (!response.ok || response.status !== 200) {
        throw new Error(`Public release check failed with HTTP ${response.status}.`);
      }
      const payload: unknown = JSON.parse(await readLimitedText(response, (reader) => {
        activeReader = reader;
      }));
      if (!Array.isArray(payload)) throw new Error("Public release response is invalid.");

      let newest: { version: string; parsed: ParsedVersion } | null = null;
      for (const item of payload) {
        if (item === null || typeof item !== "object") continue;
        const release = item as ReleaseRecord;
        if (release.draft !== false || release.prerelease !== false) continue;
        const parsed = parseStableVersion(release.tag_name);
        if (!parsed || compareVersion(parsed, current) <= 0) continue;
        const version = `${parsed.major.toString()}.${parsed.minor.toString()}.${parsed.patch.toString()}`;
        if (!releaseHasTargetAsset(release, options.platform, options.arch, version)) continue;
        if (!newest || compareVersion(parsed, newest.parsed) > 0) newest = { version, parsed };
      }
      return newest ? { version: newest.version } : null;
    })();
    return await Promise.race([work, deadline]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}
