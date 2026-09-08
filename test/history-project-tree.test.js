import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { createCoreFacade } from "../packages/core/src/index.js";
import { assertCoreMethodOutput } from "../packages/contracts/dist/index.js";
import { readHistoryWorkspaceMetadata } from "../src/history-projects.js";

async function fixture(records) {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "history-project-tree-"));
  const sessions = path.join(home, "sessions", "2026", "09", "04");
  await fs.mkdir(sessions, { recursive: true });
  await fs.writeFile(path.join(home, ".codex-global-state.json"), `${JSON.stringify({
    "project-order": ["C:\\Work\\Alpha"],
    "electron-workspace-root-labels": { "C:\\Work\\Alpha": "Alpha Alias" }
  })}\n`);
  await Promise.all(records.map((record, index) => fs.writeFile(path.join(sessions, `rollout-${index}.jsonl`), `${JSON.stringify({
    type: "session_meta", timestamp: `2026-09-04T08:${String(index).padStart(2, "0")}:00.000Z`, payload: {
      id: record.id, cwd: record.cwd ?? "C:\\Work\\Alpha", model_provider: "openai", title: record.title ?? record.id, ...record.payload
    }
  })}\n${JSON.stringify({ type: "event_msg", payload: { type: "user_message", message: "BODY_MUST_NOT_LEAK" } })}\n`)));
  return home;
}

test("projects History groups saved roots before paging and exposes children only on demand", async () => {
  const home = await fixture([
    { id: "parent", title: "parent" },
    { id: "child", title: "needle", cwd: "C:\\Work\\Alpha\\nested", payload: { parent_thread_id: "parent" } },
    { id: "same-basename", cwd: "D:\\Elsewhere\\Alpha" },
    { id: "missing-parent", payload: { parent_thread_id: "gone" } },
    { id: "missing-grandchild", payload: { parent_thread_id: "missing-parent" } },
    { id: "cycle-a", payload: { parent_thread_id: "cycle-b" } },
    { id: "cycle-b", payload: { parent_thread_id: "cycle-a" } },
    { id: "cycle-child", payload: { parent_thread_id: "cycle-a" } }
  ]);
  const core = createCoreFacade({ resolveProfile: () => ({ id: "default", revision: "r1", codexHome: home }) });
  const input = { profile: { profileId: "default", profileRevision: "r1" }, view: "projects" };
  try {
    const first = await core.listHistory(input);
    assertCoreMethodOutput("listHistory", first);
    assert.equal(first.view, "projects");
    assert.equal(first.pageSize, 10);
    assert.equal(first.projects.at(-1).kind, "orphans");
    const workspace = first.projects.find((project) => project.name === "Alpha Alias");
    assert.ok(workspace);
    assert.equal(first.projectId, workspace.id);
    assert.deepEqual(first.sessions.map((session) => session.id), ["parent"]);
    assert.equal(first.sessions[0].childCount, 1);
    assert.equal(first.sessions[0].project.name, "Alpha Alias");
    assert.doesNotMatch(JSON.stringify(first), /BODY_MUST_NOT_LEAK|C:\\Work|Elsewhere|nested/);

    const children = await core.listHistory({ ...input, projectId: workspace.id, parentId: "parent" });
    assert.deepEqual(children.sessions.map((session) => session.id), ["child"]);
    assert.equal(children.sessions[0].project.name, "Alpha Alias", "explicit parent inherits root project");

    const searched = await core.listHistory({ ...input, query: "needle", searchScope: "metadata" });
    assert.deepEqual(searched.sessions.map((session) => session.id), ["parent"], "matching child retains its parent context");
    const searchedChildren = await core.listHistory({ ...input, projectId: workspace.id, parentId: "parent", query: "needle", searchScope: "metadata" });
    assert.deepEqual(searchedChildren.sessions.map((session) => session.id), ["child"]);

    const orphans = await core.listHistory({ ...input, projectId: "orphans" });
    assert.equal(orphans.projectId, "orphans");
    assertCoreMethodOutput("listHistory", orphans);
    assert.ok(orphans.sessions.every((session) => session.project.id === "orphans"));
    assert.deepEqual(new Set(orphans.sessions.map((session) => session.id)), new Set(["missing-parent", "missing-grandchild", "cycle-a", "cycle-b", "cycle-child"]));
    const mainChildren = await core.listHistory({ ...input, projectId: workspace.id, parentId: "parent", sessionKind: "main" });
    assert.deepEqual(mainChildren.sessions, []);
    const subtasks = await core.listHistory({ ...input, sessionKind: "subagent" });
    assert.deepEqual(subtasks.sessions.map((session) => session.id), ["parent"], "subtask filter keeps main context reachable");
    assert.equal(subtasks.sessions[0].childCount, 1);
    const byWorkspaceName = await core.listHistory({ ...input, searchScope: "metadata", query: "Alpha Alias" });
    assert.deepEqual(byWorkspaceName.sessions.map((session) => session.id), ["parent"]);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("projects paginate within a project and infer the parent project without a global-page dependency", async () => {
  const records = Array.from({ length: 51 }, (_, index) => ({ id: `many-${index}`, cwd: "C:\\Many" }));
  records.push(...Array.from({ length: 60 }, (_, index) => ({ id: `other-${index}`, cwd: `D:\\Other\\P${index}` })));
  records.push({ id: "late-parent", cwd: "E:\\Target" }, { id: "late-child", cwd: "E:\\Target\\nested", payload: { parent_thread_id: "late-parent" } });
  const home = await fixture(records);
  const core = createCoreFacade({ resolveProfile: () => ({ id: "default", revision: "r1", codexHome: home }) });
  const input = { profile: { profileId: "default", profileRevision: "r1" }, view: "projects" };
  try {
    const catalog = await core.listHistory(input);
    const many = catalog.projects.find((project) => project.name === "Many");
    const target = catalog.projects.find((project) => project.name === "Target");
    assert.ok(many); assert.ok(target);
    const sixth = await core.listHistory({ ...input, projectId: many.id, page: 6, pageSize: 10 });
    assert.equal(sixth.total, 51);
    assert.equal(sixth.sessions.length, 1, "the sixth page is scoped to one project, not the global first 50");
    const children = await core.listHistory({ ...input, parentId: "late-parent" });
    assert.equal(children.projectId, target.id, "parent-only request derives its own project");
    assert.deepEqual(children.sessions.map((session) => session.id), ["late-child"]);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("projects omit an empty orphan group", async () => {
  const home = await fixture([{ id: "only-main", cwd: "C:\\Clean" }]);
  const core = createCoreFacade({ resolveProfile: () => ({ id: "default", revision: "r1", codexHome: home }) });
  try {
    const result = await core.listHistory({ profile: { profileId: "default", profileRevision: "r1" }, view: "projects" });
    assert.equal(result.projects.some((project) => project.kind === "orphans"), false);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("workspace metadata normalizes extended Windows paths and rejects an oversized state file", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "history-project-metadata-"));
  const statePath = path.join(home, ".codex-global-state.json");
  try {
    await fs.writeFile(statePath, JSON.stringify({
      "project-order": ["\\\\?\\C:\\Root\\", "C:\\"],
      "electron-workspace-root-labels": { "C:\\Root": "Root Alias" }
    }));
    const roots = await readHistoryWorkspaceMetadata(home);
    assert.equal(roots[0].name, "Root Alias");
    assert.equal(roots[1].value, "C:\\", "a drive root remains absolute after normalization");
    await fs.writeFile(statePath, " ".repeat(1024 * 1024 + 1));
    assert.deepEqual(await readHistoryWorkspaceMetadata(home), []);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});
