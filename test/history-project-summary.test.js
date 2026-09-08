import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createCoreFacade } from "../packages/core/src/index.js";
import { assertCoreMethodOutput } from "../packages/contracts/dist/index.js";

test("History groups recorded project metadata without paths, bodies or directory access", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "history-project-summary-"));
  try {
    await fs.mkdir(path.join(home, "sessions"));
    await fs.writeFile(path.join(home, "config.toml"), 'model_provider = "openai"\n');
    const directories = ["C:\\private\\Alpha", "c:/private/ALPHA/", "D:\\elsewhere\\Alpha", "/workspace/Alpha", "/workspace/alpha", "\\\\server\\share\\Alpha", "\\\\SERVER\\SHARE\\ALPHA\\", "relative/project", "", "C:\\bad\npath", "\\\\?\\C:\\private\\Alpha"];
    for (let index = 0; index < directories.length; index++) {
      await fs.writeFile(path.join(home, "sessions", `rollout-${index}.jsonl`), `${JSON.stringify({ type: "session_meta", payload: { id: `session-${index}`, cwd: directories[index], model_provider: "openai", title: `Title ${index}` } })}\n${JSON.stringify({ type: "event_msg", payload: { type: "user_message", message: "BODY_MUST_NOT_LEAK" } })}\n`);
    }
    const core = createCoreFacade({ resolveProfile: () => ({ id: "default", revision: "r1", codexHome: home }) });
    const result = await core.listHistory({ profile: { profileId: "default", profileRevision: "r1" }, page: 1, pageSize: 50 });
    assertCoreMethodOutput("listHistory", result);
    const project = (index) => result.sessions.find((session) => session.id === `session-${index}`).project;
    assert.equal(project(0).name, "Alpha");
    assert.equal(project(0).id, project(1).id);
    assert.notEqual(project(0).id, project(2).id, "same basename in different roots remains separate");
    assert.notEqual(project(3).id, project(4).id, "POSIX is case sensitive");
    assert.equal(project(5).id, project(6).id, "UNC uses Windows identity rules");
    assert.equal(project(7), null);
    assert.equal(project(8), null);
    assert.equal(project(9), null);
    assert.equal(project(10).name, "Alpha");
    assert.doesNotMatch(JSON.stringify(result), /BODY_MUST_NOT_LEAK|private|elsewhere|workspace|server|rolloutPath|"cwd"/);
    assert.ok(result.sessions.every((session) => session.messageCountKnown === false));
    const detail = await core.getHistorySession({ profile: { profileId: "default" }, sessionId: "session-0", metadataOnly: true });
    assert.deepEqual(detail.session.project, project(0));
    assert.deepEqual(detail.messages, []);
    const invalid = structuredClone(result);
    invalid.sessions[0].project.path = "C:\\private";
    assert.throws(() => assertCoreMethodOutput("listHistory", invalid));
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});
