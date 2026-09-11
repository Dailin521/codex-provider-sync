import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test, { afterEach } from "node:test";
import { acquireLock, inspectPathLock } from "../src/locking.js";

const startedAt = "2024-01-02T03:04:05.000Z";
const marker = "test:2024-01-02T03:04:05.000Z";
const tempHomes = new Set();
afterEach(async () => {
  for (const home of tempHomes) {
    await fs.rm(home, { recursive: true, force: true });
    tempHomes.delete(home);
  }
});
const owner = (instanceId, pid = 42, runtime = "node") => ({
  protocolVersion: 2, runtime, pid, processId: pid, instanceId,
  processStartMarker: marker, processStartedAt: startedAt, scope: "codex-home"
});

async function fixture(t, canonical = null, claims = []) {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "cps-lock-inspection-"));
  tempHomes.add(home);
  const lock = path.join(home, "tmp", "provider-sync.lock");
  await fs.mkdir(`${lock}.claims`, { recursive: true });
  if (canonical) {
    await fs.mkdir(lock);
    await fs.writeFile(path.join(lock, "owner.json"), JSON.stringify(canonical));
  }
  for (const item of claims) await fs.writeFile(path.join(`${lock}.claims`, `${item.instanceId}.json`), JSON.stringify(item));
  const options = {
    getProcessIdentity: async (pid) => pid === process.pid ? marker : null,
    getProcessStartedAtIdentity: async () => startedAt
  };
  return { home, lock, options };
}

async function snapshot(dir) {
  const result = {};
  for (const entry of await fs.readdir(dir, { withFileTypes: true })) {
    result[entry.name] = entry.isDirectory()
      ? await snapshot(path.join(dir, entry.name))
      : (await fs.readFile(path.join(dir, entry.name))).toString("base64");
  }
  return result;
}

for (const [name, canonical, claims] of [
  ["canonical", owner("old"), []],
  ["canonical and claims", owner("old"), [owner("old"), owner("other")]],
  ["claims only", null, [owner("a"), owner("b")]],
  ["legacy dotnet", { processId: 42, processStartedAt: startedAt }, []],
  ["dotnet v2", owner("dotnet", 42, "dotnet"), []]
]) {
  test(`read-only stale ${name} is preserved and reclaimed only by acquire`, async (t) => {
    const f = await fixture(t, canonical, claims);
    const before = await snapshot(f.home);
    for (let attempt = 0; attempt < 2; attempt++) {
      assert.equal((await inspectPathLock(f.lock, f.options)).state, "stale");
      assert.deepEqual(await snapshot(f.home), before);
    }
    const release = await acquireLock(f.home, "sync", f.options);
    assert.equal((await inspectPathLock(f.lock, f.options)).state, "active");
    await release();
    assert.equal((await inspectPathLock(f.lock, f.options)).state, "absent");
  });
}

for (const canonical of [null, owner("old")]) {
  test(`dead first claim never hides later live claim (canonical=${!!canonical})`, async (t) => {
    const f = await fixture(t, canonical, [owner("a-dead"), owner("z-live", process.pid)]);
    const before = await snapshot(f.home);
    const result = await inspectPathLock(f.lock, f.options);
    assert.equal(result.state, "active");
    assert.equal(result.owner.instanceId, "z-live");
    assert.deepEqual(await snapshot(f.home), before);
  });
}

for (const kind of ["malformed", "missing-owner", "wrong-claim-name", "unreadable-identity"]) {
  test(`unknown ${kind} stays blocked and unchanged`, async (t) => {
    const f = await fixture(t, owner("old"), [owner("a-dead"), owner("z-other")]);
    if (kind === "malformed") await fs.writeFile(path.join(`${f.lock}.claims`, "z-other.json"), "{");
    if (kind === "missing-owner") await fs.unlink(path.join(f.lock, "owner.json"));
    if (kind === "wrong-claim-name") await fs.rename(path.join(`${f.lock}.claims`, "z-other.json"), path.join(`${f.lock}.claims`, "wrong.json"));
    if (kind === "unreadable-identity") f.options.getProcessIdentity = async () => { throw new Error("denied"); };
    const before = await snapshot(f.home);
    await assert.rejects(inspectPathLock(f.lock, f.options), { code: "LOCK_UNVERIFIABLE" });
    assert.deepEqual(await snapshot(f.home), before);
  });
}

for (const runtime of ["node", "dotnet"]) {
  test(`${runtime} PID generation reuse is stale, unreadable generation is blocked`, async (t) => {
    const f = await fixture(t, owner("old", 42, runtime));
    assert.equal((await inspectPathLock(f.lock, {
      ...f.options, getProcessIdentity: async () => "test:new-generation",
      getProcessStartedAtIdentity: async () => "2025-01-02T03:04:05.000Z"
    })).state, "stale");
    await assert.rejects(inspectPathLock(f.lock, {
      ...f.options, getProcessIdentity: async () => { throw new Error("denied"); }
    }), { code: "LOCK_UNVERIFIABLE" });
  });
}

for (const target of ["canonical", "claim"]) {
  test(`read-only inspection rejects a symlink ${target} owner`, async (t) => {
    const f = await fixture(t, owner("old"), [owner("a-dead")]);
    const source = path.join(f.home, "linked-owner.json");
    const linked = target === "canonical" ? path.join(f.lock, "owner.json") : path.join(`${f.lock}.claims`, "a-dead.json");
    await fs.rename(linked, source);
    try {
      await fs.symlink(source, linked, "file");
    } catch (error) {
      if (error.code === "EPERM" || error.code === "EACCES") { t.skip("file symlink privilege unavailable"); return; }
      throw error;
    }
    const before = await fs.readFile(source);
    await assert.rejects(inspectPathLock(f.lock, f.options), { code: "LOCK_UNVERIFIABLE" });
    assert.deepEqual(await fs.readFile(source), before);
    assert.equal((await fs.lstat(linked)).isSymbolicLink(), true);
  });
}

for (const change of ["claim-added", "claim-replaced", "canonical-replaced"]) {
  test(`inspection fails closed when ${change}`, async (t) => {
    const f = await fixture(t, owner("old"), [owner("a-dead")]);
    let probes = 0;
    await assert.rejects(inspectPathLock(f.lock, {
      ...f.options,
      getProcessIdentity: async () => {
        if (++probes === 2) {
          if (change === "claim-added") await fs.writeFile(path.join(`${f.lock}.claims`, "new.json"), JSON.stringify(owner("new", process.pid)));
          if (change === "claim-replaced") await fs.writeFile(path.join(`${f.lock}.claims`, "a-dead.json"), JSON.stringify(owner("a-dead", process.pid)));
          if (change === "canonical-replaced") await fs.writeFile(path.join(f.lock, "owner.json"), JSON.stringify(owner("new", process.pid)));
        }
        return null;
      }
    }), { code: "LOCK_UNVERIFIABLE" });
  });
}
