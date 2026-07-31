import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

export const WINDOWS_CMD_LAUNCHER_FILENAME = "Codex Provider Sync.cmd";
export const WINDOWS_VBS_LAUNCHER_FILENAME = "Codex Provider Sync.vbs";

function resolveLauncherDirectory(explicitDir) {
  return path.resolve(explicitDir ?? path.join(os.homedir(), "Desktop"));
}

function quoteForBatch(value) {
  return `"${String(value).replace(/"/g, "\"\"")}"`;
}

function quoteForVbs(value) {
  return String(value).replace(/"/g, "\"\"");
}

function buildBatchScript({ codexHome, sqliteHome }) {
  const command = [
    "codex-provider",
    "sync",
    ...(codexHome ? ["--codex-home", quoteForBatch(codexHome)] : []),
    ...(sqliteHome ? ["--sqlite-home", quoteForBatch(sqliteHome)] : [])
  ].join(" ");

  return [
    "@echo off",
    "setlocal",
    command,
    "exit /b %ERRORLEVEL%"
  ].join("\r\n") + "\r\n";
}

function buildVbsScript({ codexHome, sqliteHome }) {
  const syncCommand = [
    "codex-provider",
    "sync",
    ...(codexHome ? [`--codex-home ""${quoteForVbs(codexHome)}""`] : []),
    ...(sqliteHome ? [`--sqlite-home ""${quoteForVbs(sqliteHome)}""`] : [])
  ].join(" ");

  return [
    "Option Explicit",
    "",
    'Const TITLE = "Codex Provider Sync"',
    "Const MAX_OUTPUT = 3000",
    "",
    "Function TruncateOutput(value)",
    "  If Len(value) <= MAX_OUTPUT Then",
    "    TruncateOutput = value",
    "  Else",
    '    TruncateOutput = Left(value, MAX_OUTPUT) & vbCrLf & vbCrLf & "... output truncated ..."',
    "  End If",
    "End Function",
    "",
    "Dim shell, exec, command, stdoutText, stderrText, combined, message, exitCode",
    `command = "cmd.exe /d /c ${syncCommand}"`,
    'Set shell = CreateObject("WScript.Shell")',
    "Set exec = shell.Exec(command)",
    "",
    "Do While exec.Status = 0",
    "  WScript.Sleep 200",
    "Loop",
    "",
    "stdoutText = Trim(exec.StdOut.ReadAll)",
    "stderrText = Trim(exec.StdErr.ReadAll)",
    "combined = stdoutText",
    "",
    "If Len(stderrText) > 0 Then",
    "  If Len(combined) > 0 Then",
    "    combined = combined & vbCrLf & vbCrLf",
    "  End If",
    "  combined = combined & stderrText",
    "End If",
    "",
    "If Len(combined) = 0 Then",
    '  combined = "(no output)"',
    "End If",
    "",
    "exitCode = exec.ExitCode",
    "",
    "If exitCode = 0 Then",
    '  message = "Synchronization finished." & vbCrLf & vbCrLf & TruncateOutput(combined)',
    '  MsgBox message, vbOKOnly + vbInformation, TITLE',
    "Else",
    '  message = "Synchronization failed (exit code " & exitCode & ")." & vbCrLf & vbCrLf & TruncateOutput(combined)',
    '  MsgBox message, vbOKOnly + vbCritical, TITLE',
    "End If",
    ""
  ].join("\r\n");
}

export async function installWindowsLauncher({
  dir,
  codexHome,
  sqliteHome
} = {}) {
  const targetDir = resolveLauncherDirectory(dir);
  await fs.mkdir(targetDir, { recursive: true });

  const cmdPath = path.join(targetDir, WINDOWS_CMD_LAUNCHER_FILENAME);
  const vbsPath = path.join(targetDir, WINDOWS_VBS_LAUNCHER_FILENAME);

  await fs.writeFile(cmdPath, buildBatchScript({ codexHome, sqliteHome }), "utf8");
  await fs.writeFile(vbsPath, buildVbsScript({ codexHome, sqliteHome }), "utf8");

  return {
    targetDir,
    cmdPath,
    vbsPath,
    codexHome: codexHome ? path.resolve(codexHome) : null,
    sqliteHome: sqliteHome ? path.resolve(sqliteHome) : null
  };
}
