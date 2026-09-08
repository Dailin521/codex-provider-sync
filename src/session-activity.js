// @ts-check

import fs from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";

/** @typedef {{state: "checked", count: number} | {state: "unavailable" | "unsupported", count: null}} SessionActivity */
const unavailable = () => /** @type {SessionActivity} */ ({ state: "unavailable", count: null });
const UUID_LOCK = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.lock$/i;
const MAX_LOCKS = 512;

// Query existing OS resource owners only. Never open/acquire/release a Codex
// writer lock, and never call RmShutdown/RmRestart. One hidden process per snapshot.
const OWNER_SCRIPT = String.raw`
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Console]::InputEncoding = New-Object System.Text.UTF8Encoding($false)
Add-Type -TypeDefinition @'
using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
public static class WriterOwners {
  [StructLayout(LayoutKind.Sequential)] public struct FileTime { public uint Low; public uint High; }
  [StructLayout(LayoutKind.Sequential)] public struct UniqueProcess { public uint Pid; public FileTime Started; }
  [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)] public struct ProcessInfo {
    public UniqueProcess Process;
    [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 256)] public string AppName;
    [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 64)] public string ServiceName;
    public uint AppType; public uint Status; public uint SessionId;
    [MarshalAs(UnmanagedType.Bool)] public bool Restartable;
  }
  [DllImport("rstrtmgr.dll", CharSet = CharSet.Unicode)] static extern int RmStartSession(out uint handle, uint flags, StringBuilder key);
  [DllImport("rstrtmgr.dll", CharSet = CharSet.Unicode)] static extern int RmRegisterResources(uint handle, uint nFiles, string[] files, uint nApps, UniqueProcess[] apps, uint nServices, string[] services);
  [DllImport("rstrtmgr.dll")] static extern int RmGetList(uint handle, out uint needed, ref uint count, [In, Out] ProcessInfo[] info, ref uint reasons);
  [DllImport("rstrtmgr.dll")] static extern int RmEndSession(uint handle);
  public static bool HeldByCodex(string file) {
    uint handle;
    if (RmStartSession(out handle, 0, new StringBuilder(33)) != 0) throw new Exception("owner-query");
    try {
      if (RmRegisterResources(handle, 1, new string[] { file }, 0, null, 0, null) != 0) throw new Exception("owner-register");
      uint size = 0, needed, reasons = 0;
      ProcessInfo[] info = null;
      for (int attempt = 0; attempt < 4; attempt++) {
        int result = RmGetList(handle, out needed, ref size, info, ref reasons);
        if (result == 234 && needed <= 512) { size = needed; info = new ProcessInfo[size]; continue; }
        if (result != 0) throw new Exception("owner-query");
        for (int i = 0; i < size; i++) {
          var owner = info[i].Process;
          Process process;
          try { process = Process.GetProcessById((int)owner.Pid); }
          catch (ArgumentException) { continue; } // Exited after the OS snapshot.
          using (process) {
            long start = ((long)owner.Started.High << 32) | owner.Started.Low;
            if (process.HasExited) continue;
            // A reused PID is not the owner returned by Restart Manager.
            if (process.StartTime.ToUniversalTime().ToFileTimeUtc() != start) continue;
            if (String.Equals(process.ProcessName, "codex", StringComparison.OrdinalIgnoreCase)
                && String.Equals(System.IO.Path.GetFileName(process.MainModule.FileName), "codex.exe", StringComparison.OrdinalIgnoreCase)) return true;
          }
        }
        return false;
      }
      throw new Exception("owner-changed");
    } finally { if (RmEndSession(handle) != 0) throw new Exception("owner-close"); }
  }
}
'@
$paths = ConvertFrom-Json -InputObject ([Console]::In.ReadToEnd())
$owners = @($paths | ForEach-Object { [WriterOwners]::HeldByCodex($_) })
ConvertTo-Json -Compress -InputObject $owners
`;

/** @param {string[]} paths @param {{spawnProcess?: typeof spawn, timeoutMs?: number}} [options] @returns {Promise<boolean[]>} */
export function readWindowsWriterOwners(paths, { spawnProcess = spawn, timeoutMs = 8000 } = {}) {
  if (paths.length === 0) return Promise.resolve([]);
  return new Promise((resolve, reject) => {
    const executable = path.join(process.env.SystemRoot ?? "C:\\Windows", "System32", "WindowsPowerShell", "v1.0", "powershell.exe");
    const child = spawnProcess(executable, ["-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", Buffer.from(OWNER_SCRIPT, "utf16le").toString("base64")], {
      windowsHide: true, stdio: ["pipe", "pipe", "pipe"]
    });
    let output = "";
    let failed = false;
    let settled = false;
    /** @type {ReturnType<typeof setTimeout> | undefined} */
    let stoppingTimer;
    const rejectOnce = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      clearTimeout(stoppingTimer);
      reject(new Error("Writer ownership unavailable."));
    };
    const fail = () => {
      if (failed || settled) return;
      failed = true;
      clearTimeout(timer);
      // Await close after terminating our own helper. A failed spawn has no PID.
      stoppingTimer = setTimeout(rejectOnce, 1000);
      try { child.kill(); } catch { /* Already exited or failed to spawn. */ }
      if (!child.pid) rejectOnce();
    };
    const timer = setTimeout(fail, timeoutMs);
    child.on("error", fail);
    child.stdin.on("error", fail);
    child.stdout.on("data", (chunk) => {
      output += chunk.toString("utf8");
      if (output.length > 16 * 1024) fail();
    });
    // Native/PowerShell errors may contain paths. Never expose or log them.
    child.stderr.resume();
    child.on("close", (code) => {
      clearTimeout(timer);
      clearTimeout(stoppingTimer);
      if (failed) { rejectOnce(); return; }
      if (settled) return;
      try {
        const owners = JSON.parse(output.replace(/^\uFEFF/, "").trim());
        if (code !== 0 || !Array.isArray(owners) || owners.length !== paths.length
            || owners.some((held) => typeof held !== "boolean")) throw new Error();
        settled = true;
        resolve(owners);
      } catch { rejectOnce(); }
    });
    child.stdin.end(JSON.stringify(paths));
  });
}

/** @param {string} directory */
async function inventory(directory) {
  // The Home passed by the caller is already canonical. Reject redirected
  // children before AND after enumeration, not just a stable junction at lstat.
  const canonical = await fs.realpath(directory);
  if (!samePath(canonical, directory)) throw new Error("Redirected writer directory.");
  const stat = await fs.lstat(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error("Unknown writer directory.");
  const entries = await fs.readdir(directory, { withFileTypes: true });
  if (entries.length > MAX_LOCKS + 1 || !entries.some((entry) => entry.name === ".coordination.lock")) {
    throw new Error("Unknown writer protocol.");
  }
  const files = [];
  const sessionNames = new Set();
  for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
    if ((entry.name !== ".coordination.lock" && !UUID_LOCK.test(entry.name)) || !entry.isFile() || entry.isSymbolicLink()) {
      throw new Error("Unknown writer protocol.");
    }
    if (sessionNames.has(entry.name.toLowerCase())) throw new Error("Duplicate writer identity.");
    sessionNames.add(entry.name.toLowerCase());
    const file = path.join(directory, entry.name);
    const metadata = await fs.lstat(file);
    if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.size !== 0) throw new Error("Unknown writer protocol.");
    files.push({ file, identity: `${metadata.dev}:${metadata.ino}:${metadata.size}:${metadata.mtimeMs}` });
  }
  if (!samePath(await fs.realpath(directory), canonical)) throw new Error("Redirected writer directory.");
  const after = await fs.lstat(directory);
  if (!after.isDirectory() || after.isSymbolicLink() || after.dev !== stat.dev || after.ino !== stat.ino) throw new Error("Changed writer directory.");
  return { files, signature: JSON.stringify([stat.dev, stat.ino, files]) };
}

/** @param {string} left @param {string} right */
function samePath(left, right) {
  return process.platform === "win32" ? left.toLowerCase() === right.toLowerCase() : left === right;
}

/**
 * Currently held Codex thread-writer sessions in this physical Home. Includes
 * waiting-for-input and child sessions; not a foreground/generating-turn count.
 * Empty stale lock files and unrelated owners are not counted. Missing/unknown
 * protocols are unknown, not zero. No config, rollouts, DB, logs or bodies read.
 * @param {string} codexHome
 * @param {{platform?: string, readOwners?: (paths: string[]) => Promise<boolean[]>}} [options]
 * @returns {Promise<SessionActivity>}
 */
export async function readSessionActivity(codexHome, { platform = process.platform, readOwners = readWindowsWriterOwners } = {}) {
  if (platform !== "win32" || /^\\\\/.test(codexHome)) return { state: "unsupported", count: null };
  try {
    const home = await fs.realpath(codexHome);
    if (/^\\\\/.test(home)) return { state: "unsupported", count: null };
    const directory = path.join(home, "thread-writer-locks");
    const before = await inventory(directory);
    const paths = before.files.filter(({ file }) => path.basename(file) !== ".coordination.lock").map(({ file }) => file);
    const owners = await readOwners(paths);
    if (!Array.isArray(owners) || owners.length !== paths.length || owners.some((held) => typeof held !== "boolean")) return unavailable();
    const after = await inventory(directory);
    if (before.signature !== after.signature || await fs.realpath(codexHome) !== home) return unavailable();
    return { state: "checked", count: owners.filter(Boolean).length };
  } catch { return unavailable(); }
}
