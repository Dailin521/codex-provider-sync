import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { createWebUiServer } from "../src/web-server.js";
import { createMemoryWebUiState } from "../src/web-state.js";

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

export async function createWebUiFixture() {
  const fixtureRoot = await fs.mkdtemp(path.join(os.tmpdir(), "provider-sync-c5-browser-"));
  const codexHome = path.join(fixtureRoot, ".codex");
  const rolloutPath = path.join(codexHome, "sessions", "2026", "08", "25", "rollout-c5-browser.jsonl");
  await fs.mkdir(path.dirname(rolloutPath), { recursive: true });
  await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
  await fs.mkdir(path.join(codexHome, "sqlite"), { recursive: true });
  await fs.writeFile(path.join(codexHome, "config.toml"), 'model_provider = "openai"\nmodel = "gpt-5"\n', "utf8");
  await fs.writeFile(rolloutPath, [
    { type: "session_meta", timestamp: "2026-08-25T00:00:00.000Z", payload: { id: "c5-browser-session", title: "Synthetic History", cwd: "C:\\synthetic\\project", model_provider: "openai", model: "gpt-5" } },
    { type: "event_msg", timestamp: "2026-08-25T00:01:00.000Z", payload: { type: "user_message", message: "C5_BODY_ONLY_MARKER" } },
    { type: "event_msg", timestamp: "2026-08-25T00:02:00.000Z", payload: { type: "assistant_message", message: "Synthetic response for browser validation." } }
  ].map((entry) => JSON.stringify(entry)).join("\n") + "\n", "utf8");

  const stateStore = createMemoryWebUiState({ codexHome });
  const handle = createWebUiServer({
    webRoot: path.join(repositoryRoot, "web", "dist"),
    stateStore
  });
  try {
    await new Promise((resolve, reject) => {
      handle.server.once("error", reject);
      handle.server.listen(0, "127.0.0.1", resolve);
    });
  } catch (error) {
    await fs.rm(fixtureRoot, { recursive: true, force: true });
    throw error;
  }
  const address = handle.server.address();
  if (!address || typeof address === "string") {
    await fs.rm(fixtureRoot, { recursive: true, force: true });
    throw new Error("Fixture Web server did not bind a TCP port.");
  }
  const origin = `http://127.0.0.1:${address.port}`;
  handle.setBaseUrl(origin);
  const issuePairingUrl = () => `${origin}/#pair=${encodeURIComponent(handle.issuePairing())}`;
  let closing = false;
  return {
    origin,
    pairingUrl: issuePairingUrl(),
    issuePairingUrl,
    async close() {
      if (closing) return;
      closing = true;
      await new Promise((resolve) => handle.server.close(resolve));
      await fs.rm(fixtureRoot, { recursive: true, force: true });
    }
  };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const fixture = await createWebUiFixture();
  process.stdout.write(`CPS_FIXTURE_URL=${fixture.pairingUrl}\n`);
  let closing = false;
  const close = async () => {
    if (closing) return;
    closing = true;
    await fixture.close();
  };
  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.once(signal, () => void close().finally(() => process.exit(0)));
  }
  process.stdin.resume();
}
