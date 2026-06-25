// Watch daemon: monitor ~/.codex/config.toml and the Codex state database
// for external changes and run a sync whenever the active provider goes
// out of sync. This is the "auto resync" companion to `codex-provider sync`.
//
// Usage:
//   codex-provider watch [--codex-home PATH] [--debounce-ms N] [--once] [--no-state-db]
//
// --once    : exit after the first successful sync (or after debounce settles)
//             useful for one-shot automation without keeping a process around.
// --no-state-db : only watch config.toml, ignore SQLite state events.

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

import { defaultCodexHome } from "./constants.js";
import { detectStateDb } from "./sqlite-state.js";

function normalizeCodexHome(explicitCodexHome) {
  return path.resolve(explicitCodexHome ?? process.env.CODEX_HOME ?? defaultCodexHome());
}

function defaultDebounceMs() {
  return 750;
}

function describeEvent(eventType, filename) {
  return `${eventType ?? "change"}${filename ? `:${filename}` : ""}`;
}

function makeDebouncer(delayMs, run) {
  let timer = null;
  let pending = null;

  const fire = () => {
    timer = null;
    const args = pending;
    pending = null;
    run(...args);
  };

  return function schedule(...args) {
    pending = args;
    if (timer) {
      clearTimeout(timer);
    }
    timer = setTimeout(fire, delayMs);
  };
}

async function pathExists(target) {
  try {
    await fsp.access(target);
    return true;
  } catch {
    return false;
  }
}

export async function runWatch({
  codexHome: explicitCodexHome,
  debounceMs = defaultDebounceMs(),
  includeStateDb = true,
  once = false,
  onSync,
  onLog,
  onShutdown,
  runSyncImpl,
  signal,
  sleepImpl
} = {}) {
  if (!Number.isInteger(debounceMs) || debounceMs < 0) {
    throw new Error(`Invalid --debounce-ms value: ${debounceMs}. Expected a non-negative integer.`);
  }

  const codexHome = normalizeCodexHome(explicitCodexHome);
  const configPath = path.join(codexHome, "config.toml");
  await fsp.access(codexHome).catch(() => {
    throw new Error(`Codex home not found at ${codexHome}`);
  });
  await fsp.access(configPath).catch(() => {
    throw new Error(`config.toml not found at ${configPath}`);
  });

  const log = (message) => {
    if (typeof onLog === "function") {
      onLog(message);
    } else {
      console.log(message);
    }
  };

  const invokeSync = async (reason) => {
    if (typeof onSync === "function") {
      return onSync({ reason, codexHome });
    }
    if (typeof runSyncImpl === "function") {
      return runSyncImpl({ codexHome, reason });
    }
    // Lazy import to avoid pulling in the full service module until needed.
    const { runSync } = await import("./service.js");
    return runSync({ codexHome, onProgress: (event) => {
      if (event?.stage && event.status === "start") {
        log(`  · ${event.stage}`);
      }
    } });
  };

  let stopped = false;
  let watchers = [];
  let stateDbInfo = null;
  // Track the currently-running sync (if any) so that stop()/SIGINT can
  // wait for it to drain instead of yanking the watcher out from under
  // a half-written SQLite transaction.
  let inFlight = null;

  const debouncedSync = makeDebouncer(debounceMs, (reason) => {
    if (stopped) {
      return;
    }
    log(`[${new Date().toISOString()}] Detected change (${reason}); running sync...`);
    const task = (async () => {
      try {
        const result = await invokeSync(reason);
        log(`[${new Date().toISOString()}] Sync complete: provider=${result.targetProvider}, rollout_files=${result.changedSessionFiles}, sqlite_rows=${result.sqliteRowsUpdated}${result.skippedLockedRolloutFiles?.length ? `, skipped_locked=${result.skippedLockedRolloutFiles.length}` : ""}`);
        if (once) {
          await shutdown("once-mode-complete");
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        // SQLite being in use is a normal transient condition while Codex
        // is actively writing. Don't crash; just retry on the next event.
        if (/state_5\.sqlite is currently in use/i.test(message)) {
          log(`[${new Date().toISOString()}] Sync skipped: ${message} (will retry on next change)`);
        } else {
          log(`[${new Date().toISOString()}] Sync failed: ${message}`);
        }
      } finally {
        if (inFlight === task) {
          inFlight = null;
        }
      }
    })();
    inFlight = task;
  });

  const configWatcher = fs.watch(configPath, { persistent: true }, (eventType, filename) => {
    if (stopped) {
      return;
    }
    log(`[${new Date().toISOString()}] config.toml ${describeEvent(eventType, filename)}`);
    debouncedSync("config.toml");
  });
  watchers.push(configWatcher);

    if (includeStateDb) {
      try {
        stateDbInfo = await detectStateDb(codexHome);
      } catch (error) {
        log(`[${new Date().toISOString()}] Could not locate state database: ${error.message}`);
      }
      if (stateDbInfo?.path) {
        const stateDir = path.dirname(stateDbInfo.path);
        const exists = await pathExists(stateDir);
        if (exists) {
          // Accept the SQLite file plus its WAL/SHM siblings. The basename
          // is taken from the actual state db path so we never match
          // unrelated "state*.sqlite" files that happen to share the dir.
          const stateBase = path.basename(stateDbInfo.path);
          const allowed = new Set([
            stateBase,
            `${stateBase}-wal`,
            `${stateBase}-shm`,
            `${stateBase}-journal`
          ]);
          const stateWatcher = fs.watch(stateDir, { persistent: true }, (eventType, filename) => {
            if (stopped) {
              return;
            }
            // fs.watch on Windows frequently reports filename === null.
            // Treat null as "something in the dir changed" and fall back to
            // comparing against the full path; the event is harmless to
            // trigger a sync even if it was a neighbouring file.
            const eventFile = filename ?? stateBase;
            if (!allowed.has(eventFile)) {
              return;
            }
            log(`[${new Date().toISOString()}] state_db ${describeEvent(eventType, filename ?? stateBase)}`);
            debouncedSync("state_db");
          });
          watchers.push(stateWatcher);
        } else {
        log(`[${new Date().toISOString()}] State directory ${stateDir} not found yet; watching only config.toml`);
      }
    }
  }

  log(`[${new Date().toISOString()}] Watching ${configPath}${includeStateDb && stateDbInfo?.path ? ` and ${stateDbInfo.path}` : ""} (debounce ${debounceMs}ms${once ? ", once" : ""})`);

  const shutdown = async (reason) => {
    if (stopped) {
      return;
    }
    stopped = true;
    for (const watcher of watchers) {
      try {
        watcher.close();
      } catch {
        // best-effort
      }
    }
    // Drain any sync that is still in flight so we do not yank the watcher
    // out from under a half-written SQLite transaction or backup.
    if (inFlight) {
      try {
        await inFlight;
      } catch {
        // errors are already logged by the debouncedSync handler
      }
    }
    log(`[${new Date().toISOString()}] Watcher stopped (${reason})`);
    if (typeof onShutdown === "function") {
      await onShutdown(reason);
    }
  };

  if (signal) {
    const abortHandler = () => shutdown("signal");
    if (signal.aborted) {
      await shutdown("signal");
    } else {
      signal.addEventListener("abort", abortHandler, { once: true });
    }
  }

  return {
    codexHome,
    watchedConfigPath: configPath,
    watchedStateDbPath: stateDbInfo?.path ?? null,
    stop: () => shutdown("external")
  };
}
