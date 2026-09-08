import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { DesktopProfileRepository } from "../dist/profiles/repository.js";

test("desktop profile repository projects paths out of Renderer responses", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-c6-profiles-"));
  try {
    const codexHome = path.join(root, "codex-home");
    const sqliteHome = path.join(root, "sqlite-home");
    const repository = new DesktopProfileRepository({
      filePath: path.join(root, "user-data", "profiles.v1.json"),
      defaultCodexHome: codexHome,
      defaultSqliteHome: sqliteHome
    });
    await repository.initialize();
    const profiles = repository.list();
    assert.equal(profiles.length, 1);
    assert.deepEqual(Object.keys(profiles[0]).sort(), [
      "codexHomeConfigured",
      "id",
      "name",
      "revision",
      "sqliteHomeConfigured"
    ]);
    assert.doesNotMatch(JSON.stringify(profiles), new RegExp(codexHome.replaceAll("\\", "\\\\")));
    const resolved = repository.resolve({
      profileId: "default",
      profileRevision: profiles[0].revision
    });
    assert.equal(resolved.codexHome, path.resolve(codexHome));
    assert.equal(resolved.sqliteHome, path.resolve(sqliteHome));
    assert.throws(
      () => repository.resolve({ profileId: "default", profileRevision: "stale" }),
      (error) => error?.code === "PROFILE_CHANGED"
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("desktop profile document rejects relative and duplicate trusted paths", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-c6-profile-invalid-"));
  try {
    const filePath = path.join(root, "profiles.v1.json");
    await fs.writeFile(filePath, JSON.stringify({
      schemaVersion: 1,
      profiles: [{ id: "bad", name: "Bad", codexHome: "relative" }]
    }), "utf8");
    const repository = new DesktopProfileRepository({
      filePath,
      defaultCodexHome: path.join(root, "default")
    });
    await assert.rejects(repository.initialize(), (error) => error?.code === "INVALID_INPUT");
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("named profiles are Main-generated, revision-checked, editable and removable", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-profile-crud-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const repository = new DesktopProfileRepository({
    filePath: path.join(root, "profiles.v1.json"),
    defaultCodexHome: path.join(root, "default")
  });
  await repository.initialize();
  const created = await repository.save({ name: "Work", codexHome: path.join(root, "work") });
  assert.match(created.id, /^profile-[0-9a-f]{24}$/);
  assert.equal(created.sqliteHomeConfigured, false);
  const updated = await repository.save({
    profileId: created.id,
    profileRevision: created.revision,
    name: "Work 2",
    sqliteHome: path.join(root, "sqlite")
  });
  assert.equal(updated.sqliteHomeConfigured, true);
  await assert.rejects(
    repository.save({ profileId: created.id, profileRevision: created.revision, name: "stale" }),
    (error) => error?.code === "PROFILE_CHANGED"
  );
  await repository.delete(updated.id, updated.revision);
  assert.equal(repository.list().length, 1);
  await assert.rejects(repository.delete("default", repository.list()[0].revision), (error) => error?.code === "INVALID_INPUT");
});
