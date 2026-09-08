import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";

// A real, hidden Windows resource owner for disposable test data only. This is
// not a Codex client and never starts an app-server or accesses a user's Home.
export async function createWriterSessionFixture(codexHome, ids, { beforeStart = async () => {} } = {}) {
  assert.equal(process.platform, "win32");
  const home = await fs.realpath(codexHome);
  const relative = path.relative(await fs.realpath(os.tmpdir()), home);
  assert.ok(relative && !relative.startsWith("..") && !path.isAbsolute(relative), "writer fixture requires a temp Home");
  const directory = path.join(home, "thread-writer-locks");
  await fs.mkdir(directory);
  await fs.writeFile(path.join(directory, ".coordination.lock"), "", { flag: "wx" });
  const files = ids.map((id) => {
    assert.match(id, /^[0-9a-f]{8}-[0-9a-f-]{27}$/i);
    return path.join(directory, `${id}.lock`);
  });
  for (const file of files) await fs.writeFile(file, "", { flag: "wx" });
  await beforeStart();
  const helperDirectory = await fs.mkdtemp(path.join(os.tmpdir(), "writer-owner-exe-"));
  const executable = path.join(helperDirectory, "codex.exe");
  const source = `using System; using System.IO; using System.Collections.Generic;
    class Fixture { static void Main(string[] args) {
      var handles = new List<FileStream>();
      try { foreach(var file in args) {
        var handle = File.Open(file, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        handle.Lock(0, 1); handles.Add(handle);
      }
      Console.WriteLine("READY"); Console.ReadLine();
      } finally { foreach(var handle in handles) handle.Dispose(); }
    } }`;
  const quote = (value) => `'${value.replaceAll("'", "''")}'`;
  let child;
  try {
    await promisify(execFile)("powershell.exe", ["-NoLogo", "-NoProfile", "-NonInteractive", "-Command",
      `Add-Type -TypeDefinition ${quote(source)} -OutputAssembly ${quote(executable)} -OutputType ConsoleApplication`], { windowsHide: true, timeout: 15_000 });
    child = spawn(executable, files, { windowsHide: true, stdio: ["pipe", "pipe", "pipe"] });
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("Fixture owner startup timed out")), 10_000);
      child.once("error", (error) => { clearTimeout(timer); reject(error); });
      child.once("exit", () => { clearTimeout(timer); reject(new Error("Fixture owner exited before ready")); });
      child.stdout.once("data", (data) => {
        clearTimeout(timer);
        if (String(data).trim() === "READY") resolve(); else reject(new Error("Unexpected fixture output"));
      });
      child.stderr.resume();
    });
  } catch (error) {
    child?.kill();
    await fs.rm(helperDirectory, { recursive: true, force: true });
    throw error;
  }
  let closed = false;
  return {
    files,
    async stop() {
      if (closed) return;
      closed = true;
      if (child.exitCode === null) {
        const exit = once(child, "exit");
        child.stdin.end("\n");
        const timer = setTimeout(() => child.kill(), 3000);
        try { await exit; } finally { clearTimeout(timer); }
      }
      await fs.rm(helperDirectory, { recursive: true, force: true });
    }
  };
}
