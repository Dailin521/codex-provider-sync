import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  createRendererAssetResponse,
  createSecureWebPreferences,
  resolveRendererAsset
} from "../dist/main/security-policy.js";

test("custom protocol resolves only regular assets under the renderer root", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-c6-assets-"));
  try {
    await fs.mkdir(path.join(root, "assets"), { recursive: true });
    await fs.writeFile(path.join(root, "index.html"), "<html>safe</html>", "utf8");
    await fs.writeFile(path.join(root, "assets", "app.js"), "export {};", "utf8");
    assert.equal(
      (await resolveRendererAsset(root, "cps-app://app/index.html")).filePath,
      await fs.realpath(path.join(root, "index.html"))
    );
    assert.equal(
      (await resolveRendererAsset(root, "cps-app://app/assets/app.js")).contentType,
      "text/javascript; charset=utf-8"
    );
    for (const candidate of [
      "cps-app://evil/index.html",
      "cps-app://app/index.html?path=outside",
      "cps-app://app/%252e%252e/secret",
      "cps-app://app/C:%5CWindows%5Cwin.ini",
      "file:///etc/passwd"
    ]) await assert.rejects(resolveRendererAsset(root, candidate));
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("custom protocol responses carry strict CSP and fixed MIME headers", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-c6-response-"));
  try {
    await fs.writeFile(path.join(root, "index.html"), "<html>safe</html>", "utf8");
    const response = await createRendererAssetResponse(
      root,
      new Request("cps-app://app/index.html")
    );
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("content-type"), "text/html; charset=utf-8");
    assert.match(response.headers.get("content-security-policy"), /script-src 'self'/);
    assert.doesNotMatch(response.headers.get("content-security-policy"), /unsafe-inline|unsafe-eval/);
    assert.equal(await response.text(), "<html>safe</html>");
    assert.equal((await createRendererAssetResponse(
      root,
      new Request("cps-app://app/index.html", { method: "POST" })
    )).status, 405);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("BrowserWindow preferences are fail-closed and immutable", () => {
  const preferences = createSecureWebPreferences("C:\\synthetic\\preload.cjs");
  assert.deepEqual(preferences, {
    preload: "C:\\synthetic\\preload.cjs",
    nodeIntegration: false,
    nodeIntegrationInWorker: false,
    contextIsolation: true,
    sandbox: true,
    webSecurity: true,
    allowRunningInsecureContent: false,
    experimentalFeatures: false,
    webviewTag: false
  });
  assert.equal(Object.isFrozen(preferences), true);
});
