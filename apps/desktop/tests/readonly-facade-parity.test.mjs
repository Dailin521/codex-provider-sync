import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  createCoreRequestEnvelope,
  sanitizePublicCoreErrorDto
} from "@codex-provider-sync/contracts";
import { createCoreFacade } from "@codex-provider-sync/core";

import { DesktopProfileRepository } from "../dist/profiles/repository.js";
import { createDesktopRuntimeHost } from "../dist/runtime/host.js";

function normalized(method, value) {
  if (method === "getStatus") return { ...value, snapshotAt: "<snapshot-at>" };
  if (method === "getDiagnostics") return { ...value, generatedAt: "<generated-at>" };
  return value;
}

async function hashTree(root) {
  const hash = createHash("sha256");
  async function visit(directory, prefix = "") {
    const entries = await fs.readdir(directory, { withFileTypes: true });
    entries.sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
      const absolute = path.join(directory, entry.name);
      hash.update(`${entry.isDirectory() ? "d" : "f"}:${relative}\0`);
      if (entry.isDirectory()) await visit(absolute, relative);
      else hash.update(await fs.readFile(absolute));
    }
  }
  await visit(root);
  return hash.digest("hex");
}

async function fixture() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-desktop-readonly-parity-"));
  const codexHome = path.join(root, "codex-home");
  const rollout = path.join(codexHome, "sessions", "2026", "08", "27", "rollout-parity.jsonl");
  await fs.mkdir(path.dirname(rollout), { recursive: true });
  await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
  await fs.mkdir(path.join(codexHome, "sqlite"), { recursive: true });
  await fs.writeFile(path.join(codexHome, "config.toml"), 'model_provider = "openai"\n', "utf8");
  await fs.writeFile(rollout, [
    {
      type: "session_meta",
      timestamp: "2026-08-27T00:00:00.000Z",
      payload: {
        id: "parity-session",
        title: "Parity session",
        cwd: path.join(root, "private-project"),
        model_provider: "openai",
        encrypted_content: "ciphertext-must-not-leak"
      }
    },
    {
      type: "event_msg",
      timestamp: "2026-08-27T00:01:00.000Z",
      payload: { type: "user_message", message: "body-visible-only-in-explicit-detail" }
    },
    {
      type: "event_msg",
      payload: { type: "tool_call", arguments: "tool-secret-must-not-leak" }
    }
  ].map((line) => JSON.stringify(line)).join("\n") + "\n", "utf8");
  const profiles = new DesktopProfileRepository({
    filePath: path.join(root, "host", "profiles.json"),
    defaultCodexHome: codexHome
  });
  await profiles.initialize();
  const profile = profiles.list()[0];
  const selector = { profileId: profile.id, profileRevision: profile.revision };
  return { root, codexHome, profiles, selector };
}

test("Desktop Utility host read-only methods match the standalone Core facade", async () => {
  const value = await fixture();
  try {
    const facade = createCoreFacade({ resolveProfile: (selector) => value.profiles.resolve(selector) });
    const host = createDesktopRuntimeHost(value.profiles);
    const cases = [
      ["getStatus", { profile: value.selector }],
      ["listBackups", { profile: value.selector }],
      ["listHistory", { profile: value.selector, page: 1, pageSize: 10 }],
      ["getHistorySession", { profile: value.selector, sessionId: "parity-session", messageLimit: 1 }],
      ["getDiagnostics", { profile: value.selector }]
    ];
    const before = await hashTree(value.codexHome);

    for (const [method, payload] of cases) {
      const expected = await facade[method](payload);
      const request = createCoreRequestEnvelope(method, payload, `parity-${method}`);
      const response = await host.dispatch(request);
      assert.equal(response.ok, true, `${method} should succeed through the Utility host`);
      assert.deepEqual(normalized(method, response.result), normalized(method, expected));
      if (method !== "getHistorySession") {
        assert.doesNotMatch(JSON.stringify(response), /body-visible-only|ciphertext-must-not-leak|tool-secret-must-not-leak|private-project/i);
      }
    }

    assert.equal(await hashTree(value.codexHome), before, "read-only parity calls must not mutate the fixture");
  } finally {
    await fs.rm(value.root, { recursive: true, force: true });
  }
});

test("Desktop Utility host preserves Core read-only error DTO semantics", async () => {
  const value = await fixture();
  try {
    const facade = createCoreFacade({ resolveProfile: (selector) => value.profiles.resolve(selector) });
    const host = createDesktopRuntimeHost(value.profiles);
    const cases = [
      ["getStatus", { profile: { ...value.selector, profileRevision: "stale-revision" } }],
      ["listBackups", { profile: { ...value.selector, profileRevision: "stale-revision" } }],
      ["listHistory", { profile: { ...value.selector, profileRevision: "stale-revision" }, page: 1, pageSize: 10 }],
      ["getHistorySession", { profile: value.selector, sessionId: "missing-session", messageLimit: 1 }],
      ["getDiagnostics", { profile: { ...value.selector, profileRevision: "stale-revision" } }]
    ];

    for (const [method, payload] of cases) {
      let expectedError;
      try {
        await facade[method](payload);
        assert.fail(`${method} should reject`);
      } catch (error) {
        expectedError = sanitizePublicCoreErrorDto(error);
      }
      const request = createCoreRequestEnvelope(method, payload, `parity-error-${method}`);
      const response = await host.dispatch(request);
      assert.equal(response.ok, false, `${method} should fail through the Utility host`);
      assert.deepEqual(response.error, expectedError);
      assert.doesNotMatch(JSON.stringify(response.error), /stale-revision|missing-session|private-project|codex-home/i);
    }
  } finally {
    await fs.rm(value.root, { recursive: true, force: true });
  }
});
