#!/usr/bin/env node

import path from "node:path";

import { DEFAULT_BACKUP_RETENTION_COUNT } from "./constants.js";
import { formatBytes, renderStatus } from "./cli-presenter.js";
import { installWindowsLauncher } from "./launcher.js";
import { assertSupportedNodeVersion } from "./node-version.js";

async function loadCore() {
  assertSupportedNodeVersion();
  return import("./public-api.js");
}

function printHelp() {
  console.log(`codex-provider

Usage:
  codex-provider status [--codex-home PATH] [--sqlite-home PATH]
  codex-provider sync [--provider ID] [--keep N] [--codex-home PATH] [--sqlite-home PATH]
  codex-provider switch <provider-id> [--model NAME] [--keep-root-model] [--keep N] [--codex-home PATH] [--sqlite-home PATH]
  codex-provider watch [--codex-home PATH] [--sqlite-home PATH] [--debounce-ms N] [--once] [--no-state-db]
  codex-provider web [--port N] [--no-open] [--reset-access] [--codex-home PATH] [--sqlite-home PATH]
  codex-provider prune-backups [--keep N] [--codex-home PATH]
  codex-provider restore <backup-dir> [--no-config] [--no-db] [--no-sessions] [--allow-sqlite-home-relocation] [--codex-home PATH] [--sqlite-home PATH]
  codex-provider install-windows-launcher [--dir PATH] [--codex-home PATH] [--sqlite-home PATH]

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
`);
}

function parseArgs(argv) {
  const positionals = [];
  const flags = {};

  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (!value.startsWith("--")) {
      positionals.push(value);
      continue;
    }
    const [flagName, inlineValue] = value.split("=", 2);
    const normalizedName = flagName.slice(2);
    if (inlineValue !== undefined) {
      flags[normalizedName] = inlineValue;
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

  return { positionals, flags };
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

function createSyncProgressReporter() {
  return (event) => {
    if (event?.stage === "update_config" && event.status === "start") {
      console.log(`Updating config.toml root model_provider to ${event.provider}...`);
      return;
    }

    const stageIndex = SYNC_PROGRESS_STAGE_INDEX.get(event?.stage);
    if (!stageIndex || event.status !== "start") {
      if (event?.stage === "create_backup" && event.status === "complete") {
        console.log(`     Backup created in ${formatDuration(event.durationMs)}: ${event.backupDir}`);
      }
      return;
    }

    console.log(`[${stageIndex}/${SYNC_PROGRESS_STAGES.length}] ${SYNC_PROGRESS_STAGES[stageIndex - 1][1]}`);
  };
}

function parseKeepCount(rawValue, { allowZero = false } = {}) {
  if (rawValue === undefined) {
    return DEFAULT_BACKUP_RETENTION_COUNT;
  }
  const normalized = String(rawValue).trim();
  if (!/^\d+$/.test(normalized)) {
    const minimum = allowZero ? 0 : 1;
    throw new Error(`Invalid --keep value: ${rawValue}. Expected an integer greater than or equal to ${minimum}.`);
  }
  const keepCount = Number.parseInt(normalized, 10);
  const minimum = allowZero ? 0 : 1;
  if (!Number.isInteger(keepCount) || keepCount < minimum) {
    throw new Error(`Invalid --keep value: ${rawValue}. Expected an integer greater than or equal to ${minimum}.`);
  }
  return keepCount;
}

async function main() {
  const { positionals, flags } = parseArgs(process.argv.slice(2));
  const command = positionals[0];

  if (!command || command === "help" || flags.help) {
    printHelp();
    return;
  }

  assertSupportedNodeVersion();

  if (command === "status") {
    const { getStatus } = await loadCore();
    const status = await getStatus({
      codexHome: flags["codex-home"],
      sqliteHome: flags["sqlite-home"]
    });
    console.log(renderStatus(status));
    return;
  }

  if (command === "sync") {
    const { runSync, readConfigText, readRootModelFromConfigText } = await loadCore();
    const { defaultCodexHome } = await import("./constants.js");
    const codexHome = path.resolve(
      flags["codex-home"] ?? process.env.CODEX_HOME ?? defaultCodexHome()
    );
    const configPath = path.join(codexHome, "config.toml");
    let rootModel = null;
    try {
      const cfg = await readConfigText(configPath);
      rootModel = readRootModelFromConfigText(cfg);
    } catch {
      // config may be missing in degraded scenarios; carry on without a
      // model rewrite so the rest of the sync still runs.
    }
    const result = await runSync({
      codexHome: flags["codex-home"],
      sqliteHome: flags["sqlite-home"],
      provider: flags.provider,
      keepCount: parseKeepCount(flags.keep),
      onProgress: createSyncProgressReporter(),
      model: rootModel
    });
    console.log(summarizeSync(result, "Synchronized"));
    return;
  }

  if (command === "switch") {
    const { runSwitch } = await loadCore();
    const provider = positionals[1] ?? flags.provider;
    const result = await runSwitch({
      codexHome: flags["codex-home"],
      sqliteHome: flags["sqlite-home"],
      provider,
      model: flags.model,
      keepRootModel: Boolean(flags["keep-root-model"]),
      keepCount: parseKeepCount(flags.keep),
      onProgress: createSyncProgressReporter()
    });
    console.log(summarizeSync(result, "Switched to"));
    if (result.modelSync) {
      const { applied, source, model, warning } = result.modelSync;
      if (applied) {
        console.log(`Root-level model: ${model} (source: ${source})`);
      } else if (warning) {
        console.log(`Root-level model: unchanged (${warning})`);
      } else {
        console.log("Root-level model: unchanged (keep-root-model flag set)");
      }
    }
    return;
  }

  if (command === "prune-backups") {
    const { runPruneBackups } = await loadCore();
    const result = await runPruneBackups({
      codexHome: flags["codex-home"],
      keepCount: parseKeepCount(flags.keep, { allowZero: true })
    });
    console.log(summarizePrune(result));
    return;
  }

  if (command === "watch") {
    const { runWatch } = await loadCore();
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
    // Race the watcher's own `done` promise (which resolves when
    // `--once` completes or the consecutive-failure auto-shutdown
    // fires) against the external SIGINT/SIGTERM handler. Whichever
    // wins, we stop the watcher cleanly and let the process exit.
    // Without this race, the CLI sits in the event loop forever
    // after a `--once` run, because Node only exits on its own
    // when there are no more pending handles.
    await new Promise((resolve) => {
      let settled = false;
      const finish = async (source) => {
        if (settled) {
          return;
        }
        settled = true;
        try {
          await handle.stop();
        } catch {
          // best effort: stop() may already be in flight
        }
        resolve(source);
      };
      handle.done.then(() => finish("done"), () => finish("done-rejected"));
      if (handle.signalPromise) {
        handle.signalPromise.then(() => finish("signal"), () => finish("signal-rejected"));
      }
      process.once("SIGINT", () => finish("SIGINT"));
      process.once("SIGTERM", () => finish("SIGTERM"));
    });
    return;
  }

  if (command === "web") {
    const { startWebUi } = await import("./web-server.js");
    const port = flags.port === undefined ? 8791 : parseKeepCount(flags.port, { allowZero: true });
    const handle = await startWebUi({
      port,
      openBrowser: !flags["no-open"],
      resetAccess: Boolean(flags["reset-access"]),
      codexHome: flags["codex-home"],
      sqliteHome: flags["sqlite-home"]
    });
    console.log(`Codex Provider Sync Web UI: ${handle.url}`);
    if (flags["no-open"] || !handle.browserOpened) {
      console.log(`One-time pairing link: ${handle.pairingUrl}`);
    }
    if (handle.reused) {
      console.log("Opened the existing Codex Provider Sync Web UI instance.");
      return;
    }
    console.log("The server only listens on 127.0.0.1. Press Ctrl+C to stop it.");
    await new Promise((resolve) => {
      let closing = false;
      const close = async () => {
        if (closing) {
          return;
        }
        closing = true;
        await handle.close().catch(() => {});
        resolve();
      };
      process.once("SIGINT", close);
      process.once("SIGTERM", close);
    });
    return;
  }

  if (command === "restore") {
    const { runRestore } = await loadCore();
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
    console.log(`Restored backup from ${path.resolve(backupDir)}`);
    console.log(`Codex home: ${result.codexHome}`);
    console.log(`Provider at backup time: ${result.targetProvider}`);
    if (result.backupInventoryWarning) {
      console.log(`Backup inventory warning: ${result.backupInventoryWarning}`);
    }
    return;
  }

  if (command === "install-windows-launcher") {
    const result = await installWindowsLauncher({
      dir: flags.dir,
      codexHome: flags["codex-home"],
      sqliteHome: flags["sqlite-home"]
    });
    console.log("Installed Windows launcher files:");
    console.log(`  Hidden double-click launcher: ${result.vbsPath}`);
    console.log(`  Visible console launcher: ${result.cmdPath}`);
    console.log(`  Target directory: ${result.targetDir}`);
    if (result.codexHome) {
      console.log(`  Fixed CODEX_HOME: ${result.codexHome}`);
    } else {
      console.log("  CODEX_HOME: default current environment / ~/.codex");
    }
    if (result.sqliteHome) {
      console.log(`  Fixed SQLite home: ${result.sqliteHome}`);
    } else {
      console.log("  SQLite home: config / environment / Codex default");
    }
    return;
  }

  throw new Error(`Unknown command: ${command}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
