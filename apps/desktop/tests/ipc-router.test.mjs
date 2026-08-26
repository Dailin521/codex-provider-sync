import assert from "node:assert/strict";
import test from "node:test";

import { createCoreRequestEnvelope } from "@codex-provider-sync/contracts";

import { registerDesktopIpc } from "../dist/main/ipc-router.js";
import { DESKTOP_IPC_CHANNELS } from "../dist/shared/constants.js";

function harness() {
  const handlers = new Map();
  const ipcMain = {
    handle(channel, handler) {
      assert.equal(handlers.has(channel), false);
      handlers.set(channel, handler);
    },
    removeHandler(channel) {
      handlers.delete(channel);
    }
  };
  const frame = { url: "cps-app://app/index.html" };
  const webContents = { mainFrame: frame };
  const window = {
    webContents,
    isDestroyed: () => false
  };
  const event = { sender: webContents, senderFrame: frame };
  const calls = [];
  const cleanup = registerDesktopIpc({
    ipcMain,
    getWindow: () => window,
    rendererOrigin: "cps-app://app",
    profiles: {
      list: () => [{
        id: "default",
        name: "Default",
        revision: "r1",
        codexHomeConfigured: true,
        sqliteHomeConfigured: false
      }]
    },
    supervisor: {
      async request(request) {
        calls.push(request);
        return { protocolVersion: 1, requestId: request.requestId, ok: true, result: {} };
      },
      crashForTest: () => true
    }
  });
  return { handlers, event, calls, cleanup };
}

test("IPC accepts only a top-level local sender and a validated read method", async () => {
  const value = harness();
  try {
    const request = createCoreRequestEnvelope(
      "getStatus",
      { profile: { profileId: "default", profileRevision: "r1" } },
      "ipc-status"
    );
    const response = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreRead)(value.event, request);
    assert.equal(response.ok, true);
    assert.equal(value.calls.length, 1);

    const evilFrame = { url: "cps-app://evil/index.html" };
    const denied = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreRead)(
      { sender: value.event.sender, senderFrame: evilFrame },
      request
    );
    assert.equal(denied.ok, false);
    assert.equal(denied.error.code, "PERMISSION_DENIED");
    assert.equal(value.calls.length, 1);
  } finally {
    value.cleanup();
  }
});

test("IPC hard-denies writes, protocol drift and oversized read payloads", async () => {
  const value = harness();
  try {
    const profile = { profileId: "default", profileRevision: "r1" };
    const write = createCoreRequestEnvelope(
      "prepareSync",
      { profile, keepCount: 5 },
      "ipc-write"
    );
    const denied = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreRead)(value.event, write);
    assert.equal(denied.ok, false);
    assert.equal(denied.error.code, "PERMISSION_DENIED");

    const mismatch = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreRead)(value.event, {
      ...createCoreRequestEnvelope("getStatus", { profile }, "ipc-version"),
      protocolVersion: 99
    });
    assert.equal(mismatch.ok, false);
    assert.equal(mismatch.error.code, "PROTOCOL_VERSION_MISMATCH");

    const oversized = await value.handlers.get(DESKTOP_IPC_CHANNELS.coreRead)(value.event, {
      ...createCoreRequestEnvelope("listHistory", { profile }, "ipc-large"),
      payload: { profile, query: "x".repeat(70 * 1024) }
    });
    assert.equal(oversized.ok, false);
    assert.equal(oversized.error.code, "INVALID_INPUT");
    assert.equal(value.calls.length, 0);
  } finally {
    value.cleanup();
  }
});

test("Profile IPC returns only the redacted schema and cleanup removes every channel", async () => {
  const value = harness();
  const response = await value.handlers.get(DESKTOP_IPC_CHANNELS.profilesList)(value.event, null);
  assert.deepEqual(response, {
    schemaVersion: 1,
    profiles: [{
      id: "default",
      name: "Default",
      revision: "r1",
      codexHomeConfigured: true,
      sqliteHomeConfigured: false
    }]
  });
  assert.equal("codexHome" in response.profiles[0], false);
  value.cleanup();
  assert.equal(value.handlers.size, 0);
});
