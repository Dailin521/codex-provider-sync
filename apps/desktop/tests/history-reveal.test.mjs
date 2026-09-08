import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { createCoreSuccessEnvelope } from "@codex-provider-sync/contracts";

import { registerDesktopIpc } from "../dist/main/ipc-router.js";
import { DESKTOP_IPC_CHANNELS } from "../dist/shared/constants.js";

async function temporaryHistoryHome() {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "cps-desktop-history-reveal-"));
  const rollout = path.join(home, "sessions", "2026", "09", "04", "rollout-session.jsonl");
  await fs.mkdir(path.dirname(rollout), { recursive: true });
  await fs.writeFile(rollout, "synthetic metadata only\n", "utf8");
  return { home, rollout };
}

function routerHarness({ home, resultPath, mutateProfileAfterRequest, revealHistoryFile } = {}) {
  const handlers = new Map();
  const ipcMain = {
    handle(channel, handler) { handlers.set(channel, handler); },
    removeHandler(channel) { handlers.delete(channel); }
  };
  const frame = { url: "cps-app://app/index.html" };
  const webContents = { id: 7, mainFrame: frame, send() {} };
  const window = { webContents, isDestroyed: () => false };
  const event = { sender: webContents, senderFrame: frame };
  const calls = [];
  let selected = { id: "default", revision: "r1", codexHome: home };
  const profiles = {
    resolve(selector) {
      if (selector.profileId !== selected.id
          || (selector.profileRevision !== undefined && selector.profileRevision !== selected.revision)) {
        throw new Error("profile changed");
      }
      return { ...selected };
    }
  };
  const supervisor = {
    snapshot: { state: "ready", generation: 1, recoveryBlocked: false, writeInProgress: false, lastHandshakeAt: null },
    subscribeOperation() { return () => {}; },
    subscribeWatchStopped() { return () => {}; },
    async request(request) {
      calls.push(request);
      mutateProfileAfterRequest?.(() => { selected = { ...selected, revision: "r2" }; });
      return createCoreSuccessEnvelope(request, {
        session: {
          id: request.payload.sessionId,
          title: "",
          provider: "openai",
          archived: false,
          updatedAt: "2026-09-04T08:00:00.000Z",
          messageCount: 0,
          messageCountKnown: false
        },
        storage: { cwd: "/synthetic", rolloutPath: resultPath },
        messages: [],
        truncated: false,
        returnedMessageCount: 0
      });
    },
    async requestManaged() { throw new Error("not used"); }
  };
  const registration = registerDesktopIpc({
    ipcMain,
    getWindow: () => window,
    rendererOrigin: "cps-app://app",
    profiles,
    directorySelections: {},
    operationLogs: {
      subscribeOperation() {},
      async begin() { return "log"; }, async prepared() {}, async resume() {}, async bindOperation() {}, async progress() {},
      async dismiss() {}, async finishFromResponse() {}, async finish() {}, list() { return {}; }, get() { return null; }
    },
    selectProfileDirectory: async () => null,
    revealProfileDirectory: async () => false,
    ...(revealHistoryFile ? { revealHistoryFile } : {}),
    isProfileMutationBlocked: () => false,
    supervisor,
    diagnosticsExporter: {},
    selectDiagnosticsTarget: async () => null,
    updates: { restartPending: false, status() { return {}; }, async check() { return {}; }, async download() { return {}; }, async install() { return {}; } }
  });
  return {
    invoke(value, suppliedEvent = event) {
      return handlers.get(DESKTOP_IPC_CHANNELS.historyReveal)(suppliedEvent, value);
    },
    calls,
    registration,
    event,
    badEvent: { sender: { id: 99, mainFrame: frame }, senderFrame: frame }
  };
}

function request(sessionId = "session-one") {
  return {
    schemaVersion: 1,
    profile: { profileId: "default", profileRevision: "r1" },
    sessionId
  };
}

test("history reveal accepts only an allowlisted session request and trusted sender", async () => {
  const { home, rollout } = await temporaryHistoryHome();
  const harness = routerHarness({ home, resultPath: rollout });
  try {
    await assert.rejects(
      () => harness.invoke({ ...request(), rolloutPath: rollout }),
      /Invalid history reveal request/
    );
    await assert.rejects(() => harness.invoke(request(), harness.badEvent), /Desktop history reveal rejected/);
    await assert.rejects(
      () => harness.invoke({ ...request(), profile: { profileId: "default", profileRevision: "wrong" } }),
      /profile changed/
    );
    assert.equal(harness.calls.length, 0);
  } finally {
    harness.registration();
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history reveal requests metadata only and invokes the injected shell callback for a validated rollout", async () => {
  const { home, rollout } = await temporaryHistoryHome();
  const revealed = [];
  const harness = routerHarness({
    home,
    resultPath: rollout,
    revealHistoryFile: async (filePath) => { revealed.push(filePath); return true; }
  });
  try {
    assert.deepEqual(await harness.invoke(request()), { revealed: true });
    assert.equal(harness.calls.length, 1);
    assert.equal(harness.calls[0].method, "getHistorySession");
    assert.deepEqual(harness.calls[0].payload, {
      profile: { profileId: "default", profileRevision: "r1" },
      sessionId: "session-one",
      metadataOnly: true
    });
    assert.deepEqual(revealed, [rollout]);
  } finally {
    harness.registration();
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history reveal refuses stale profiles and rollout paths outside the selected sessions roots", async () => {
  const { home, rollout } = await temporaryHistoryHome();
  const escaped = path.join(home, "not-a-rollout.txt");
  await fs.writeFile(escaped, "synthetic", "utf8");
  const revealed = [];
  const escapedHarness = routerHarness({
    home,
    resultPath: escaped,
    revealHistoryFile: async (filePath) => { revealed.push(filePath); return true; }
  });
  const staleHarness = routerHarness({
    home,
    resultPath: rollout,
    mutateProfileAfterRequest: (mutate) => mutate(),
    revealHistoryFile: async (filePath) => { revealed.push(filePath); return true; }
  });
  try {
    assert.deepEqual(await escapedHarness.invoke(request()), { revealed: false });
    assert.deepEqual(await staleHarness.invoke(request()), { revealed: false });
    assert.deepEqual(revealed, []);
  } finally {
    escapedHarness.registration();
    staleHarness.registration();
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("history reveal refuses a symlinked rollout target", async (t) => {
  const { home, rollout } = await temporaryHistoryHome();
  const outside = path.join(home, "outside-rollout.jsonl");
  const linked = path.join(home, "sessions", "2026", "09", "04", "rollout-link.jsonl");
  await fs.writeFile(outside, "synthetic", "utf8");
  try {
    try {
      await fs.symlink(outside, linked, "file");
    } catch (error) {
      if (error?.code === "EPERM") {
        t.skip("file symlink unavailable");
        return;
      }
      throw error;
    }
    const revealed = [];
    const harness = routerHarness({
      home,
      resultPath: linked,
      revealHistoryFile: async (filePath) => { revealed.push(filePath); return true; }
    });
    try {
      assert.deepEqual(await harness.invoke(request()), { revealed: false });
      assert.deepEqual(revealed, []);
    } finally {
      harness.registration();
    }
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});
