import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import {
  installWindowsLauncher,
  WINDOWS_CMD_LAUNCHER_FILENAME,
  WINDOWS_VBS_LAUNCHER_FILENAME
} from "../src/launcher.js";

test("installWindowsLauncher creates cmd and vbs launchers", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-launcher-"));
  const codexHome = "C:\\Users\\Example User\\.codex";

  const result = await installWindowsLauncher({ dir, codexHome });

  assert.equal(result.targetDir, dir);
  assert.equal(result.cmdPath, path.join(dir, WINDOWS_CMD_LAUNCHER_FILENAME));
  assert.equal(result.vbsPath, path.join(dir, WINDOWS_VBS_LAUNCHER_FILENAME));
  assert.equal(result.codexHome, path.resolve(codexHome));

  const cmdText = await fs.readFile(result.cmdPath, "utf8");
  const vbsText = await fs.readFile(result.vbsPath, "utf8");

  assert.match(cmdText, /codex-provider sync --codex-home "C:\\Users\\Example User\\.codex"/);
  assert.match(vbsText, /Synchronization finished\./);
  assert.match(vbsText, /Codex Provider Sync/);
  assert.match(vbsText, /codex-provider sync --codex-home ""C:\\Users\\Example User\\.codex""/);
});

test("installWindowsLauncher preserves an explicit UNC SQLite home", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-launcher-"));
  const sqliteHome = "\\\\wsl.localhost\\Ubuntu\\home\\user\\.codex\\sqlite";

  const result = await installWindowsLauncher({ dir, sqliteHome });
  const cmdText = await fs.readFile(result.cmdPath, "utf8");
  const vbsText = await fs.readFile(result.vbsPath, "utf8");

  assert.match(cmdText, /--sqlite-home "\\\\wsl\.localhost\\Ubuntu\\home\\user\\\.codex\\sqlite"/);
  assert.match(vbsText, /--sqlite-home ""\\\\wsl\.localhost\\Ubuntu\\home\\user\\\.codex\\sqlite""/);
});
