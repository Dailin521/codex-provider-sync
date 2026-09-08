import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  parseWindowState,
  fitWindowToDisplays,
  restoreWindowState,
  WindowStateController,
  WindowStateStore
} from "../dist/main/window-state.js";

const primary = { id: 1, workArea: { x: 0, y: 0, width: 1920, height: 1040 } };

function fakeScreen(displays, matching = displays[0]) {
  return {
    getAllDisplays: () => displays,
    getPrimaryDisplay: () => displays.find((display) => display.id === primary.id) ?? displays[0],
    getDisplayMatching: () => matching
  };
}

function state(overrides = {}) {
  return {
    schemaVersion: 1,
    normalBounds: { x: 100, y: 120, width: 1100, height: 700 },
    maximized: false,
    displayId: "1",
    ...overrides
  };
}

class FakeWindow extends EventEmitter {
  constructor(normalBounds, maximized = false) {
    super();
    this.normalBounds = normalBounds;
    this.maximized = maximized;
  }

  getNormalBounds() { return this.normalBounds; }
  isMaximized() { return this.maximized; }
}

test("restores a valid negative-coordinate display and keeps maximized separate from normal bounds", () => {
  const left = { id: 2, workArea: { x: -1600, y: 0, width: 1600, height: 900 } };
  const restored = restoreWindowState(
    state({ normalBounds: { x: -1500, y: 80, width: 1000, height: 700 }, maximized: true, displayId: "2" }),
    fakeScreen([primary, left], left),
    { x: 0, y: 0, width: 1000, height: 700 },
    { width: 760, height: 560 }
  );
  assert.deepEqual(restored, {
    bounds: { x: -1500, y: 80, width: 1000, height: 700 },
    maximized: true
  });
});

test("falls back and clamps a removed display or changed work area fully into view", () => {
  const reducedPrimary = { id: 1, workArea: { x: 0, y: 0, width: 1200, height: 800 } };
  const restored = restoreWindowState(
    state({ normalBounds: { x: 3200, y: 900, width: 1600, height: 1000 }, displayId: "removed" }),
    fakeScreen([reducedPrimary], reducedPrimary),
    { x: 0, y: 0, width: 900, height: 650 },
    { width: 760, height: 560 }
  );
  assert.deepEqual(restored, {
    bounds: { x: 0, y: 0, width: 1200, height: 800 },
    maximized: false
  });
});

test("refits a live maximized window after unplug or high-DPI work-area change", () => {
  const small = { id: 1, workArea: { x: 0, y: 0, width: 640, height: 480 } };
  const calls = [];
  const window = new FakeWindow({ x: -1500, y: 80, width: 1000, height: 700 }, true);
  window.setMinimumSize = (width, height) => calls.push(["minimum", width, height]);
  window.unmaximize = () => { window.maximized = false; calls.push("unmaximize"); };
  window.setBounds = (bounds) => { window.normalBounds = bounds; calls.push("bounds"); };
  window.maximize = () => { window.maximized = true; calls.push("maximize"); };
  fitWindowToDisplays(window, fakeScreen([small]));
  assert.deepEqual(window.normalBounds, { x: 0, y: 0, width: 640, height: 480 });
  assert.equal(window.maximized, true);
  assert.deepEqual(calls, [["minimum", 640, 480], "unmaximize", "bounds", "maximize"]);
  calls.length = 0;
  fitWindowToDisplays(window, fakeScreen([small]));
  assert.deepEqual(calls, [["minimum", 640, 480]]);
});

test("rejects malformed or oversized state JSON", async (t) => {
  assert.equal(parseWindowState("{not json"), null);
  assert.equal(parseWindowState(JSON.stringify(state({ displayId: "" }))), null);
  assert.equal(parseWindowState("x".repeat(2049)), null);

  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-window-state-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const filePath = path.join(root, "window-state.json");
  await fs.writeFile(filePath, "x".repeat(2049), "utf8");
  assert.equal(await new WindowStateStore({ filePath }).read(), null);
});

test("atomically persists the normal bounds and ignores a failed preference write", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-window-state-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const filePath = path.join(root, "window-state.json");
  const store = new WindowStateStore({ filePath });
  await store.write(state());
  assert.deepEqual(await store.read(), state());
  assert.deepEqual((await fs.readdir(root)).sort(), ["window-state.json"]);

  const blockedParent = path.join(root, "not-a-directory");
  await fs.writeFile(blockedParent, "fixture", "utf8");
  await new WindowStateStore({ filePath: path.join(blockedParent, "window-state.json") }).write(state());
  assert.equal(await fs.readFile(blockedParent, "utf8"), "fixture");
});

test("hidden and secondary test modes can disable both reading and writing preferences", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-window-state-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const filePath = path.join(root, "window-state.json");
  await fs.writeFile(filePath, JSON.stringify(state({ displayId: "sentinel" })), "utf8");
  const store = new WindowStateStore({ filePath, enabled: false });
  assert.equal(await store.read(), null);
  await store.write(state({ displayId: "replacement" }));
  assert.equal((await fs.readFile(filePath, "utf8")).includes("sentinel"), true);
});

test("window lifecycle captures normal bounds while maximized and flushes on exit", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-window-state-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const store = new WindowStateStore({ filePath: path.join(root, "window-state.json") });
  const controller = new WindowStateController({ store, screen: fakeScreen([primary], primary), debounceMs: 10_000 });
  const window = new FakeWindow({ x: 210, y: 160, width: 1000, height: 680 }, true);
  const detach = controller.attach(window);
  window.emit("maximize");
  await controller.flush(window);
  detach();
  assert.deepEqual(await store.read(), state({
    normalBounds: { x: 210, y: 160, width: 1000, height: 680 },
    maximized: true
  }));
});
