#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import {
  cliJsonExitCode,
  createCliFailureEnvelope,
  createCliSuccessEnvelope
} from "./cli-json.js";
import { DEFAULT_BACKUP_RETENTION_COUNT } from "./constants.js";
import { formatBytes, renderStatus } from "./cli-presenter.js";
import { installWindowsLauncher } from "./launcher.js";
import { assertSupportedNodeVersion } from "./node-version.js";

async function loadCore() {
  assertSupportedNodeVersion();
  return import("./public-api.js");
}

const HELP_TEXT = `codex-provider

Usage:
  codex-provider status [--json] [--codex-home PATH] [--sqlite-home PATH]
  codex-provider sync [--json] [--provider ID] [--keep N] [--codex-home PATH] [--sqlite-home PATH]
  codex-provider switch <provider-id> [--json] [--model NAME] [--keep-root-model] [--keep N] [--codex-home PATH] [--sqlite-home PATH]
  codex-provider watch [--codex-home PATH] [--sqlite-home PATH] [--debounce-ms N] [--once] [--no-state-db]
  codex-provider web [--port N] [--no-open] [--reset-access] [--codex-home PATH] [--sqlite-home PATH]
  codex-provider prune-backups [--json] [--keep N] [--codex-home PATH]
  codex-provider restore <backup-dir> [--json] [--no-config] [--no-db] [--no-sessions] [--allow-sqlite-home-relocation] [--codex-home PATH] [--sqlite-home PATH]
  codex-provider install-windows-launcher [--json] [--dir PATH] [--codex-home PATH] [--sqlite-home PATH]

JSON mode:
  --json               emit one schemaVersion 1 envelope on stdout; progress goes to stderr
                       (not supported by long-running watch or web commands)

switch flags:
  --model NAME         override root-level model field with NAME (e.g. "MiniMax-M3")
  --keep-root-model    do not touch the root-level model field; only switch model_provider

watch flags:
  --codex-home PATH    override CODEX_HOME (default: ~/.codex or $CODEX_HOME)
  --sqlite-home PATH   override sqlite_home and CODEX_SQLITE_HOME
  --debounce-ms N      wait N milliseconds after a change before syncing (default 750)
  --once               exit after the first successful sync
  --no-state-db        only watch config.toml, ignore SQLite state events

web flags:
  --port N             bind the local Web UI to 127.0.0.1:N (default 8791)
  --no-open            do not open the system browser automatically
  --reset-access       invalidate all paired browsers before creating a new pairing
  --codex-home PATH    set the default server-managed storage profile
  --sqlite-home PATH   set the default profile SQLite Home override
`;

function printHelp(writeLine) {
  writeLine(HELP_TEXT);
}

function parseArgs(argv) {
  const positionals = [];
  const flags = Object.create(null);
  const flagCounts = Object.create(null);

  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (!value.startsWith("--")) {
      positionals.push(value);
      continue;
    }
    const separatorIndex = value.indexOf("=");
    const flagName = separatorIndex >= 0 ? value.slice(0, separatorIndex) : value;
    const inlineValue = separatorIndex >= 0 ? value.slice(separatorIndex + 1) : undefined;
    const normalizedName = flagName.slice(2);
    flagCounts[normalizedName] = (flagCounts[normalizedName] ?? 0) + 1;
    if (inlineValue !== undefined) {
      flags[normalizedName] = inlineValue;
      continue;
    }
    if (normalizedName === "json") {
      flags[normalizedName] = true;
      continue;
    }
    const nextValue = argv[index + 1];
    if (nextValue && !nextValue.startsWith("--")) {
      flags[normalizedName] = nextValue;
      index += 1;
    } else {
      flags[normalizedName] = true;
    }
  }

  return { positionals, flags, flagCounts };
}

const JSON_COMMAND_CONTRACTS = Object.freeze({
  status: {
    flags: ["json", "help", "codex-home", "sqlite-home"],
    valueFlags: ["codex-home", "sqlite-home"],
    booleanFlags: ["json", "help"],
    positionalCount: 1
  },
  sync: {
    flags: ["json", "help", "provider", "keep", "codex-home", "sqlite-home"],
    valueFlags: ["provider", "keep", "codex-home", "sqlite-home"],
    booleanFlags: ["json", "help"],
    positionalCount: 1
  },
  switch: {
    flags: ["json", "help", "model", "keep-root-model", "keep", "codex-home", "sqlite-home"],
    valueFlags: ["model", "keep", "codex-home", "sqlite-home"],
    booleanFlags: ["json", "help", "keep-root-model"],
    positionalCount: 2
  },
  "prune-backups": {
    flags: ["json", "help", "keep", "codex-home"],
    valueFlags: ["keep", "codex-home"],
    booleanFlags: ["json", "help"],
    positionalCount: 1
  },
  restore: {
    flags: [
      "json",
      "help",
      "no-config",
      "no-db",
      "no-sessions",
      "allow-sqlite-home-relocation",
      "codex-home",
      "sqlite-home"
    ],
    valueFlags: ["codex-home", "sqlite-home"],
    booleanFlags: [
      "json",
      "help",
      "no-config",
      "no-db",
      "no-sessions",
      "allow-sqlite-home-relocation"
    ],
    positionalCount: 2
  },
  "install-windows-launcher": {
    flags: ["json", "help", "dir", "codex-home", "sqlite-home"],
    valueFlags: ["dir", "codex-home", "sqlite-home"],
    booleanFlags: ["json", "help"],
    positionalCount: 1
  }
});

const JSON_KNOWN_COMMANDS = new Set([
  ...Object.keys(JSON_COMMAND_CONTRACTS),
  "watch",
  "web",
  "help"
]);

function invalidInputError(message) {
  const error = new Error(message);
  error.code = "INVALID_INPUT";
  return error;
}

function validateJsonFlag(flags) {
  if (Object.hasOwn(flags, "json") && flags.json !== true) {
    throw invalidInputError("--json is a standalone boolean flag and does not accept a value.");
  }
}

function validateJsonCommandArgs(command, parsed) {
  const contract = Object.hasOwn(JSON_COMMAND_CONTRACTS, command)
    ? JSON_COMMAND_CONTRACTS[command]
    : undefined;
  if (!contract) {
    if (command === "watch" || command === "web") {
      throw invalidInputError(
        `${command} does not support --json because it is a long-running command. Use Human mode.`
      );
    }
    throw invalidInputError(`Unknown command: ${command}`);
  }

  const allowedFlags = new Set(contract.flags);
  const valueFlags = new Set(contract.valueFlags);
  const booleanFlags = new Set(contract.booleanFlags);
  for (const [name, count] of Object.entries(parsed.flagCounts)) {
    if (!allowedFlags.has(name)) {
      throw invalidInputError(`Unknown option for ${command}: --${name}`);
    }
    if (count !== 1) {
      throw invalidInputError(`Option --${name} may only be specified once in JSON mode.`);
    }
    if (valueFlags.has(name)
        && (parsed.flags[name] === true || String(parsed.flags[name]).length === 0)) {
      throw invalidInputError(`Option --${name} requires a value.`);
    }
    if (booleanFlags.has(name) && parsed.flags[name] !== true) {
      throw invalidInputError(`Option --${name} is a boolean flag and does not accept a value.`);
    }
  }
  if (parsed.positionals.length !== contract.positionalCount) {
    const expected = contract.positionalCount - 1;
    throw invalidInputError(
      `${command} expects exactly ${expected} positional argument${expected === 1 ? "" : "s"}.`
    );
  }
}

function summarizeSync(result, label) {
  const lines = [
    `${label} provider: ${result.targetProvider}`,
    `Codex home: ${result.codexHome}`,
    `SQLite home: ${result.sqliteHome} (source: ${result.sqliteHomeSource})`,
    `Backup: ${result.backupDir}`,
    `Backup creation time: ${formatDuration(result.backupDurationMs ?? 0)}`,
    `Updated rollout files: ${result.changedSessionFiles}`,
    `Updated SQLite rows: ${result.sqliteRowsUpdated}${result.sqlitePresent ? "" : " (state_5.sqlite not found)"}`
  ];
  if (result.sqliteUserEventRowsUpdated) {
    lines.push(`Updated SQLite user-event flags: ${result.sqliteUserEventRowsUpdated}`);
  }
  if (result.sqliteCwdRowsUpdated) {
    lines.push(`Updated SQLite cwd paths: ${result.sqliteCwdRowsUpdated}`);
  }
  if (result.updatedWorkspaceRoots) {
    lines.push(`Updated workspace roots: ${result.updatedWorkspaceRoots}`);
  }
  if (result.skippedLockedRolloutFiles?.length) {
    const preview = result.skippedLockedRolloutFiles.slice(0, 5).join(", ");
    const extraCount = result.skippedLockedRolloutFiles.length - Math.min(result.skippedLockedRolloutFiles.length, 5);
    lines.push(`Skipped locked rollout files: ${result.skippedLockedRolloutFiles.length}`);
    lines.push(`Locked file(s): ${preview}${extraCount > 0 ? ` (+${extraCount} more)` : ""}`);
  }
  if (result.encryptedContentWarning) {
    lines.push(result.encryptedContentWarning);
  }
  if (result.autoPruneResult) {
    lines.push(
      `Backup cleanup: deleted ${result.autoPruneResult.deletedCount}, remaining ${result.autoPruneResult.remainingCount}, freed ${formatBytes(result.autoPruneResult.freedBytes)}`
    );
  }
  if (result.autoPruneWarning) {
    lines.push(`Backup cleanup warning: ${result.autoPruneWarning}`);
  }
  return lines.join("\n");
}

function summarizePrune(result) {
  return [
    `Backup root: ${result.backupRoot}`,
    `Deleted backups: ${result.deletedCount}`,
    `Remaining backups: ${result.remainingCount}`,
    `Freed space: ${formatBytes(result.freedBytes)}`
  ].join("\n");
}

function formatDuration(durationMs) {
  if (!Number.isFinite(durationMs) || durationMs < 1000) {
    return `${Math.max(0, Math.round(durationMs ?? 0))} ms`;
  }

  const seconds = durationMs / 1000;
  if (seconds < 60) {
    return `${seconds.toFixed(seconds >= 10 ? 1 : 2).replace(/\.0$/, "")} s`;
  }

  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds - (minutes * 60);
  return `${minutes}m ${remainingSeconds.toFixed(remainingSeconds >= 10 ? 0 : 1).replace(/\.0$/, "")}s`;
}

const SYNC_PROGRESS_STAGES = [
  ["scan_rollout_files", "Scanning rollout files..."],
  ["check_locked_rollout_files", "Checking locked rollout files..."],
  ["create_backup", "Creating backup..."],
  ["update_sqlite", "Updating SQLite..."],
  ["rewrite_rollout_files", "Rewriting rollout files..."],
  ["clean_backups", "Cleaning backups..."]
];

const SYNC_PROGRESS_STAGE_INDEX = new Map(
  SYNC_PROGRESS_STAGES.map(([stage], index) => [stage, index + 1])
);

function createSyncProgressReporter(writeLine, { includeBackupPath = true } = {}) {
  return (event) => {
    if (event?.stage === "update_config" && event.status === "start") {
      writeLine(includeBackupPath
        ? `Updating config.toml root model_provider to ${event.provider}...`
        : "Updating config.toml root model_provider...");
      return;
    }

    const stageIndex = SYNC_PROGRESS_STAGE_INDEX.get(event?.stage);
    if (!stageIndex || event.status !== "start") {
      if (event?.stage === "create_backup" && event.status === "complete") {
        writeLine(includeBackupPath
          ? `     Backup created in ${formatDuration(event.durationMs)}: ${event.backupDir}`
          : `     Backup created in ${formatDuration(event.durationMs)}`);
      }
      return;
    }

    writeLine(`[${stageIndex}/${SYNC_PROGRESS_STAGES.length}] ${SYNC_PROGRESS_STAGES[stageIndex - 1][1]}`);
  };
}

function parseKeepCount(rawValue, { allowZero = false } = {}) {
  if (rawValue === undefined) {
    return DEFAULT_BACKUP_RETENTION_COUNT;
  }
  const normalized = String(rawValue).trim();
  if (!/^\d+$/.test(normalized)) {
    const minimum = allowZero ? 0 : 1;
    throw invalidInputError(`Invalid --keep value: ${rawValue}. Expected an integer greater than or equal to ${minimum}.`);
  }
  const keepCount = Number.parseInt(normalized, 10);
  const minimum = allowZero ? 0 : 1;
  if (!Number.isInteger(keepCount) || keepCount < minimum) {
    throw invalidInputError(`Invalid --keep value: ${rawValue}. Expected an integer greater than or equal to ${minimum}.`);
  }
  return keepCount;
}

function createLineWriter(stream) {
  return (value) => stream.write(`${String(value)}\n`);
}

const BEST_EFFORT_STREAMS = new WeakSet();

function createBestEffortLineWriter(stream) {
  if (stream && typeof stream === "object"
      && typeof stream.on === "function"
      && !BEST_EFFORT_STREAMS.has(stream)) {
    stream.on("error", () => {});
    BEST_EFFORT_STREAMS.add(stream);
  }
  return (value) => {
    try {
      stream.write(`${String(value)}\n`, () => {});
    } catch {
      // Progress and diagnostics are observer output and cannot change the operation result.
    }
  };
}

function writeJsonDocument(stream, serialized) {
  const document = `${serialized}\n`;
  if (typeof stream.once !== "function"
      || typeof stream.removeListener !== "function"
      || stream.write.length < 2) {
    stream.write(document);
    return Promise.resolve();
  }

  return new Promise((resolve, reject) => {
    let settled = false;
    const removeErrorListener = () => stream.removeListener("error", onError);
    const finish = (error) => {
      if (settled) return;
      settled = true;
      setImmediate(removeErrorListener);
      if (error) reject(error);
      else resolve();
    };
    const onError = (error) => finish(error);
    stream.once("error", onError);
    try {
      stream.write(document, (error) => finish(error));
    } catch (error) {
      finish(error);
    }
  });
}

function validateJsonHelpArgs(parsed) {
  if (parsed.positionals.length > 1
      || (parsed.positionals[0] && !JSON_KNOWN_COMMANDS.has(parsed.positionals[0]))) {
    throw invalidInputError("Unknown command in JSON help request.");
  }
  for (const [name, count] of Object.entries(parsed.flagCounts)) {
    if (!new Set(["json", "help"]).has(name)) {
      throw invalidInputError(`Unknown option in JSON help request: --${name}`);
    }
    if (count !== 1 || parsed.flags[name] !== true) {
      throw invalidInputError(`Option --${name} must be a standalone flag specified once.`);
    }
  }
}

function fallbackCliErrorDto(error) {
  if (error?.code === "INVALID_INPUT") {
    return {
      code: "INVALID_INPUT",
      message: error instanceof Error ? error.message : "Invalid CLI input.",
      severity: "error",
      retryable: true,
      recoveryRequired: false
    };
  }
  if (error?.name === "AbortError" && error?.code === "ABORT_ERR") {
    return {
      code: "OPERATION_CANCELLED",
      message: "The operation was cancelled.",
      severity: "info",
      retryable: true,
      recoveryRequired: false
    };
  }
  return {
    code: "INTERNAL_ERROR",
    message: "An internal error occurred.",
    severity: "fatal",
    retryable: false,
    recoveryRequired: false
  };
}

async function cliErrorDto(error, loadCoreImpl) {
  if (error?.code === "INVALID_INPUT"
      || (error?.name === "AbortError" && error?.code === "ABORT_ERR")) {
    return fallbackCliErrorDto(error);
  }
  try {
    const core = await loadCoreImpl();
    if (typeof core.toCoreErrorDto !== "function") {
      return fallbackCliErrorDto(error);
    }
    return core.toCoreErrorDto(error);
  } catch {
    return fallbackCliErrorDto(error);
  }
}

async function executeCommand({ positionals, flags }, context) {
  const {
    command,
    jsonMode,
    stdoutLine,
    stderrLine,
    environment,
    signalTarget,
    loadCoreImpl,
    startWebUiImpl,
    installWindowsLauncherImpl
  } = context;

  if (command === "status") {
    const { getStatus } = await loadCoreImpl();
    const status = await getStatus({
      codexHome: flags["codex-home"],
      sqliteHome: flags["sqlite-home"]
    });
    if (!jsonMode) stdoutLine(renderStatus(status));
    return status;
  }

  if (command === "sync") {
    const { runSync, readConfigText, readRootModelFromConfigText } = await loadCoreImpl();
    const { defaultCodexHome } = await import("./constants.js");
    const codexHome = path.resolve(
      flags["codex-home"] ?? environment.CODEX_HOME ?? defaultCodexHome()
    );
    const configPath = path.join(codexHome, "config.toml");
    let rootModel = null;
    try {
      const cfg = await readConfigText(configPath);
      rootModel = readRootModelFromConfigText(cfg);
    } catch {
      // Degraded compatibility path: continue without a per-thread model rewrite.
    }
    const result = await runSync({
      codexHome: flags["codex-home"],
      sqliteHome: flags["sqlite-home"],
      provider: flags.provider,
      keepCount: parseKeepCount(flags.keep),
      onProgress: createSyncProgressReporter(jsonMode ? stderrLine : stdoutLine, {
        includeBackupPath: !jsonMode
      }),
      model: rootModel
    });
    if (!jsonMode) stdoutLine(summarizeSync(result, "Synchronized"));
    return result;
  }

  if (command === "switch") {
    const { runSwitch } = await loadCoreImpl();
    const provider = positionals[1] ?? flags.provider;
    const result = await runSwitch({
      codexHome: flags["codex-home"],
      sqliteHome: flags["sqlite-home"],
      provider,
      model: flags.model,
      keepRootModel: Boolean(flags["keep-root-model"]),
      keepCount: parseKeepCount(flags.keep),
      onProgress: createSyncProgressReporter(jsonMode ? stderrLine : stdoutLine, {
        includeBackupPath: !jsonMode
      })
    });
    if (!jsonMode) {
      stdoutLine(summarizeSync(result, "Switched to"));
      if (result.modelSync) {
        const { applied, source, model, warning } = result.modelSync;
        if (applied) {
          stdoutLine(`Root-level model: ${model} (source: ${source})`);
        } else if (warning) {
          stdoutLine(`Root-level model: unchanged (${warning})`);
        } else {
          stdoutLine("Root-level model: unchanged (keep-root-model flag set)");
        }
      }
    }
    return result;
  }

  if (command === "prune-backups") {
    const { runPruneBackups } = await loadCoreImpl();
    const result = await runPruneBackups({
      codexHome: flags["codex-home"],
      keepCount: parseKeepCount(flags.keep, { allowZero: true })
    });
    if (!jsonMode) stdoutLine(summarizePrune(result));
    return result;
  }

  if (command === "watch") {
    const { runWatch } = await loadCoreImpl();
    const debounceMs = flags["debounce-ms"] !== undefined
      ? parseKeepCount(flags["debounce-ms"], { allowZero: true })
      : undefined;
    const handle = await runWatch({
      codexHome: flags["codex-home"],
      sqliteHome: flags["sqlite-home"],
      debounceMs,
      includeStateDb: !flags["no-state-db"],
      once: Boolean(flags.once)
    });
    await new Promise((resolve) => {
      let settled = false;
      const finish = async (source) => {
        if (settled) return;
        settled = true;
        try {
          await handle.stop();
        } catch {
          // Best effort: stop() may already be in flight.
        }
        resolve(source);
      };
      handle.done.then(() => finish("done"), () => finish("done-rejected"));
      if (handle.signalPromise) {
        handle.signalPromise.then(() => finish("signal"), () => finish("signal-rejected"));
      }
      signalTarget.once("SIGINT", () => finish("SIGINT"));
      signalTarget.once("SIGTERM", () => finish("SIGTERM"));
    });
    return {};
  }

  if (command === "web") {
    const startWebUi = startWebUiImpl ?? (await import("./web-server.js")).startWebUi;
    const port = flags.port === undefined ? 8791 : parseKeepCount(flags.port, { allowZero: true });
    const handle = await startWebUi({
      port,
      openBrowser: !flags["no-open"],
      resetAccess: Boolean(flags["reset-access"]),
      codexHome: flags["codex-home"],
      sqliteHome: flags["sqlite-home"]
    });
    stdoutLine(`Codex Provider Sync Web UI: ${handle.url}`);
    if (flags["no-open"] || !handle.browserOpened) {
      stdoutLine(`One-time pairing link: ${handle.pairingUrl}`);
    }
    if (handle.reused) {
      stdoutLine("Opened the existing Codex Provider Sync Web UI instance.");
      return {};
    }
    stdoutLine("The server only listens on 127.0.0.1. Press Ctrl+C to stop it.");
    await new Promise((resolve) => {
      let closing = false;
      const close = async () => {
        if (closing) return;
        closing = true;
        await handle.close().catch(() => {});
        resolve();
      };
      signalTarget.once("SIGINT", close);
      signalTarget.once("SIGTERM", close);
    });
    return {};
  }

  if (command === "restore") {
    const { runRestore } = await loadCoreImpl();
    const backupDir = positionals[1] ?? flags.backup;
    const result = await runRestore({
      codexHome: flags["codex-home"],
      sqliteHome: flags["sqlite-home"],
      backupDir,
      restoreConfig: !flags["no-config"],
      restoreDatabase: !flags["no-db"],
      restoreSessions: !flags["no-sessions"],
      allowSqliteHomeRelocation: Boolean(flags["allow-sqlite-home-relocation"])
    });
    const jsonResult = { ...result, backupDir: path.resolve(backupDir) };
    if (!jsonMode) {
      stdoutLine(`Restored backup from ${path.resolve(backupDir)}`);
      stdoutLine(`Codex home: ${result.codexHome}`);
      stdoutLine(`Provider at backup time: ${result.targetProvider}`);
      if (result.backupInventoryWarning) {
        stdoutLine(`Backup inventory warning: ${result.backupInventoryWarning}`);
      }
    }
    return jsonResult;
  }

  if (command === "install-windows-launcher") {
    const result = await installWindowsLauncherImpl({
      dir: flags.dir,
      codexHome: flags["codex-home"],
      sqliteHome: flags["sqlite-home"]
    });
    if (!jsonMode) {
      stdoutLine("Installed Windows launcher files:");
      stdoutLine(`  Hidden double-click launcher: ${result.vbsPath}`);
      stdoutLine(`  Visible console launcher: ${result.cmdPath}`);
      stdoutLine(`  Target directory: ${result.targetDir}`);
      if (result.codexHome) {
        stdoutLine(`  Fixed CODEX_HOME: ${result.codexHome}`);
      } else {
        stdoutLine("  CODEX_HOME: default current environment / ~/.codex");
      }
      if (result.sqliteHome) {
        stdoutLine(`  Fixed SQLite home: ${result.sqliteHome}`);
      } else {
        stdoutLine("  SQLite home: config / environment / Codex default");
      }
    }
    return result;
  }

  throw invalidInputError(`Unknown command: ${command}`);
}

export async function runCli(argv, options = {}) {
  const stdout = options.stdout ?? process.stdout;
  const stderr = options.stderr ?? process.stderr;
  const parsed = parseArgs(argv);
  const jsonMode = Object.hasOwn(parsed.flags, "json");
  const stdoutLine = createLineWriter(stdout);
  const stderrLine = jsonMode
    ? createBestEffortLineWriter(stderr)
    : createLineWriter(stderr);
  const requestedCommand = parsed.positionals[0];
  let command = requestedCommand ?? "help";
  if (jsonMode && !JSON_KNOWN_COMMANDS.has(command)) command = "unknown";
  let terminalWritten = false;
  const loadCoreImpl = options.loadCoreImpl ?? loadCore;

  const writeEnvelope = async (envelope) => {
    const serialized = JSON.stringify(envelope);
    if (terminalWritten) {
      throw new Error("CLI JSON terminal envelope was already written.");
    }
    terminalWritten = true;
    await writeJsonDocument(stdout, serialized);
  };

  try {
    validateJsonFlag(parsed.flags);
    if (!requestedCommand || requestedCommand === "help" || parsed.flags.help) {
      command = "help";
      if (jsonMode) {
        validateJsonHelpArgs(parsed);
        const envelope = createCliSuccessEnvelope("help", {
          text: HELP_TEXT.trimEnd(),
          requestedCommand: requestedCommand && requestedCommand !== "help"
            ? requestedCommand
            : null
        });
        await writeEnvelope(envelope);
        return cliJsonExitCode(envelope);
      }
      printHelp(stdoutLine);
      return 0;
    }

    assertSupportedNodeVersion();
    if (jsonMode) validateJsonCommandArgs(command, parsed);
    const result = await executeCommand(parsed, {
      command,
      jsonMode,
      stdoutLine,
      stderrLine,
      environment: options.environment ?? process.env,
      signalTarget: options.signalTarget ?? process,
      loadCoreImpl,
      startWebUiImpl: options.startWebUiImpl,
      installWindowsLauncherImpl: options.installWindowsLauncherImpl ?? installWindowsLauncher
    });
    if (jsonMode) {
      const envelope = createCliSuccessEnvelope(command, result);
      await writeEnvelope(envelope);
      return cliJsonExitCode(envelope);
    }
    return 0;
  } catch (error) {
    if (!jsonMode) {
      stderrLine(error instanceof Error ? error.message : String(error));
      return 1;
    }
    const envelope = createCliFailureEnvelope(command, await cliErrorDto(error, loadCoreImpl));
    if (!terminalWritten) await writeEnvelope(envelope);
    return cliJsonExitCode(envelope);
  }
}

async function isDirectExecution() {
  if (!process.argv[1]) return false;
  try {
    return pathToFileURL(await fs.realpath(process.argv[1])).href === import.meta.url;
  } catch {
    return pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url;
  }
}

if (await isDirectExecution()) {
  try {
    process.exitCode = await runCli(process.argv.slice(2));
  } catch {
    process.exitCode = 1;
  }
}
