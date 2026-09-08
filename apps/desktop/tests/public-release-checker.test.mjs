import assert from "node:assert/strict";
import test from "node:test";

import {
  PUBLIC_RELEASES_URL,
  checkPublicDesktopRelease
} from "../dist/main/public-release-checker.js";

const API_URL = "https://api.github.com/repos/Dailin521/codex-provider-sync/releases?per_page=30";

function asset(version, name = `CodexProviderSync-${version}-windows-x64-setup.exe`) {
  return { name };
}

function release(tag, assets, overrides = {}) {
  return { tag_name: tag, assets, draft: false, prerelease: false, ...overrides };
}

function fetchJson(payload, status = 200) {
  return async (url, init) => {
    assert.equal(url, API_URL);
    assert.equal(init.method, "GET");
    assert.equal(init.signal instanceof AbortSignal, true);
    return new Response(JSON.stringify(payload), { status });
  };
}

function check(overrides = {}) {
  return checkPublicDesktopRelease({
    appVersion: "1.2.3",
    platform: "win32",
    arch: "x64",
    ...overrides
  });
}

test("public release checker has a fixed public release-list page and picks the highest compatible stable version", async () => {
  assert.equal(PUBLIC_RELEASES_URL, "https://github.com/Dailin521/codex-provider-sync/releases");
  const result = await check({ fetchImpl: fetchJson([
    release("v1.10.0", [asset("1.10.0")]),
    release("1.9.99", [asset("1.9.99")]),
    release("v1.2.4", [asset("1.2.4")])
  ]) });
  assert.deepEqual(result, { version: "1.10.0" });
});

test("public release checker excludes old, draft, prerelease, malformed and incompatible releases", async () => {
  const result = await check({ fetchImpl: fetchJson([
    release("2.0.0", [asset("2.0.0")], { draft: true }),
    release("1.9.0", [asset("1.9.0")], { prerelease: true }),
    release("1.8", [asset("1.8")]),
    release("1.2.3", [asset("1.2.3")]),
    release("1.2.2", [asset("1.2.2")]),
    release("1.2.4", [asset("1.2.4", "CodexProviderSync-1.2.4-windows-arm64-setup.exe")])
  ]) });
  assert.equal(result, null);
});

test("public release checker requires explicit stable API flags before accepting a release", async () => {
  const result = await check({ fetchImpl: fetchJson([
    { tag_name: "2.0.0", assets: [asset("2.0.0")] },
    release("1.2.4", [asset("1.2.4")])
  ]) });
  assert.deepEqual(result, { version: "1.2.4" });
});

test("a current prerelease may upgrade to the same numeric stable release", async () => {
  const result = await check({
    appVersion: "v1.2.3-rc.1",
    fetchImpl: fetchJson([release("v1.2.3", [asset("1.2.3")])])
  });
  assert.deepEqual(result, { version: "1.2.3" });
});

test("a build metadata hyphen does not make the current stable version a prerelease", async () => {
  const result = await check({
    appVersion: "1.2.3+build-with-hyphen",
    fetchImpl: fetchJson([release("v1.2.3", [asset("1.2.3")])])
  });
  assert.equal(result, null);
});

test("version comparison is exact above Number's safe integer range", async () => {
  const result = await check({
    appVersion: "9007199254740992.0.0",
    fetchImpl: fetchJson([
      release("9007199254740993.0.0", [asset("9007199254740993.0.0")]),
      release("9007199254740992.0.1", [asset("9007199254740992.0.1")])
    ])
  });
  assert.deepEqual(result, { version: "9007199254740993.0.0" });
});

test("asset matching follows the electron-builder macOS and Linux naming", async () => {
  const mac = await check({
    platform: "darwin",
    arch: "arm64",
    fetchImpl: fetchJson([release("1.2.4", [{ name: "CodexProviderSync-1.2.4-macos-arm64.zip" }])])
  });
  const linux = await check({
    platform: "linux",
    arch: "x64",
    fetchImpl: fetchJson([release("1.2.4", [{ name: "CodexProviderSync-1.2.4-linux-x64.deb" }])])
  });
  assert.deepEqual(mac, { version: "1.2.4" });
  assert.deepEqual(linux, { version: "1.2.4" });
});

test("non-200 responses and oversized bodies fail closed rather than claiming a latest release", async () => {
  await assert.rejects(check({ fetchImpl: fetchJson([], 404) }), /HTTP 404/);
  await assert.rejects(check({ fetchImpl: async () => new Response("x".repeat(2 * 1024 * 1024 + 1)) }), /size limit/);
});

test("the timeout aborts a slow injected fetch implementation", async () => {
  let aborted = false;
  await assert.rejects(check({
    timeoutMs: 1,
    fetchImpl: async (_url, init) => new Promise((_resolve, reject) => {
      init.signal.addEventListener("abort", () => {
        aborted = true;
        reject(new Error("aborted"));
      }, { once: true });
    })
  }), /timed out/);
  assert.equal(aborted, true);
});

test("the deadline covers a body that stalls after response headers and cancels its reader", async () => {
  let cancelled = false;
  const stalledBody = new ReadableStream({
    pull() {
      return new Promise(() => {});
    },
    cancel() {
      cancelled = true;
    }
  });
  await assert.rejects(check({
    timeoutMs: 1,
    fetchImpl: async () => new Response(stalledBody)
  }), /timed out/);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(cancelled, true);
});

test("unsupported targets and invalid current versions do not make a network request", async () => {
  let calls = 0;
  const fetchImpl = async () => { calls += 1; return new Response("[]"); };
  assert.equal(await check({ arch: "arm64", fetchImpl }), null);
  assert.equal(await check({ appVersion: "not-a-version", fetchImpl }), null);
  assert.equal(calls, 0);
});
