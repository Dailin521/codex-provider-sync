import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import test from "node:test";

const desktopRoot = path.resolve(import.meta.dirname, "..");

async function read(relativePath) {
  return fs.readFile(path.join(desktopRoot, relativePath), "utf8");
}

async function filesUnder(relativeRoot) {
  const root = path.join(desktopRoot, relativeRoot);
  const files = [];
  async function visit(directory) {
    for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
      const absolute = path.join(directory, entry.name);
      if (entry.isDirectory()) await visit(absolute);
      else if (entry.isFile()) files.push(absolute);
    }
  }
  await visit(root);
  return files;
}

test("Renderer has no Node, Electron, filesystem, Core or arbitrary IPC imports", async () => {
  const sources = await filesUnder("src/renderer");
  const text = (await Promise.all(sources.map((file) => fs.readFile(file, "utf8")))).join("\n");
  assert.doesNotMatch(text, /from\s+["'](?:node:|electron(?:\/|["']))/);
  assert.doesNotMatch(text, /@codex-provider-sync\/core(?:["'/])/);
  assert.doesNotMatch(text, /ipcRenderer|BrowserWindow|child_process|node:fs|node:path/);
  assert.match(text, /DesktopCoreClient/);
  assert.match(text, /SYNC_SWITCH_APP_UI_CAPABILITIES/);
});

test("Preload exposes one frozen purpose-built bridge and no raw IPC surface", async () => {
  const source = await read("src/preload/index.ts");
  assert.match(source, /exposeInMainWorld\("codexProvider"/);
  assert.match(source, /requestReadOnly/);
  assert.match(source, /requestSyncSwitch/);
  assert.match(source, /subscribeOperation/);
  assert.match(source, /cancelOperation/);
  assert.match(source, /ipcRenderer\.on\(DESKTOP_IPC_CHANNELS\.operationEvent/);
  assert.doesNotMatch(source, /ipcRenderer\.(?:send|sendSync|once|postMessage)\s*\(/);
  assert.doesNotMatch(source, /node:(?:fs|path|child_process)|@codex-provider-sync\/core["']/);
  assert.doesNotMatch(source, /exposeInMainWorld\([^,]+,\s*ipcRenderer/);
  assert.match(source, /__CPS_DESKTOP_TEST_BUILD__/);
});

test("Main security policy fixes the BrowserWindow, CSP, protocol and deny defaults", async () => {
  const policy = await read("src/main/security-policy.ts");
  const security = await read("src/main/security.ts");
  const constants = await read("src/shared/constants.ts");
  for (const expected of [
    "nodeIntegration: false",
    "nodeIntegrationInWorker: false",
    "contextIsolation: true",
    "sandbox: true",
    "webSecurity: true",
    "allowRunningInsecureContent: false",
    "experimentalFeatures: false",
    "webviewTag: false"
  ]) assert.match(policy, new RegExp(expected.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  assert.match(constants, /script-src 'self'/);
  assert.doesNotMatch(constants, /unsafe-inline|unsafe-eval/);
  assert.match(security, /setWindowOpenHandler\(\(\) => \(\{ action: "deny" \}\)\)/);
  assert.match(security, /setPermissionCheckHandler\(\(\) => false\)/);
  assert.match(security, /setPermissionRequestHandler/);
  assert.match(security, /will-navigate/);
  assert.match(security, /will-attach-webview/);
});

test("Utility imports only the Core public package and C7 exact Sync/Switch methods", async () => {
  const runtime = await read("src/runtime/host.ts");
  const protocol = await read("src/shared/runtime-protocol.ts");
  const clientPolicy = await read("../../packages/core-client/src/desktop.ts");
  assert.match(runtime, /from "@codex-provider-sync\/core"/);
  assert.doesNotMatch(runtime, /src\/(?:public-api|service|backup|locking|history|watch)/);
  assert.doesNotMatch(runtime, /\.\.\/main\//);
  for (const allowed of ["prepareSync", "applySync", "prepareSwitch", "applySwitch"]) {
    assert.match(clientPolicy, new RegExp(`\\"${allowed}\\"`));
  }
  for (const denied of [
    "prepareRestore",
    "applyRestore",
    "pruneBackups",
    "startWatch",
    "stopWatch",
    "getWatchStatus"
  ]) assert.doesNotMatch(clientPolicy, new RegExp(`\\"${denied}\\"`));
  assert.match(protocol, /dispatchId/);
  assert.match(protocol, /operation-event/);
  assert.match(protocol, /RuntimeCancelFrame/);
});

test("electron-vite emits a CJS sandbox preload and keeps source maps disabled", async () => {
  const config = await read("electron.vite.config.ts");
  assert.match(config, /format: "cjs"/);
  assert.match(config, /entryFileNames: "\[name\]\.cjs"/);
  assert.match(config, /inlineDynamicImports: true/);
  assert.equal((config.match(/sourcemap: false/g) ?? []).length, 3);
  assert.match(config, /external: \["electron"\]/);
  assert.match(config, /__CPS_DESKTOP_TEST_BUILD__:\s*JSON\.stringify\(mode === "test"\)/);
});

test("packaging always replaces test output with a verified production bundle", async () => {
  const packageDocument = JSON.parse(await read("package.json"));
  assert.equal(
    packageDocument.scripts["pack:dir"],
    "npm run build && npm run build:electron && npm run verify:production-bundle && electron-builder --dir --config electron-builder.yml"
  );
});
