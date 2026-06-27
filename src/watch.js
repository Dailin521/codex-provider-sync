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

import fs, { existsSync } from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

import { defaultCodexHome } from "./constants.js";
import { detectStateDb, stateDbCandidates } from "./sqlite-state.js";
import { readConfigText, readRootModelFromConfigText } from "./config-file.js";

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
    // Read the current root-level model on every fire so the per-thread
    // model rewrite picks up the latest value the user has in config.toml.
    let rootModel = null;
    try {
      const cfg = await readConfigText(path.join(codexHome, "config.toml"));
      rootModel = readRootModelFromConfigText(cfg);
    } catch {
      // Missing/unreadable config; carry on with a null model.
    }
    if (typeof onSync === "function") {
      return onSync({ reason, codexHome, model: rootModel });
    }
    if (typeof runSyncImpl === "function") {
      return runSyncImpl({ codexHome, reason, model: rootModel });
    }
    // Lazy import to avoid pulling in the full service module until needed.
    const { runSync } = await import("./service.js");
    return runSync({
      codexHome,
      model: rootModel,
      onProgress: (event) => {
        if (event?.stage && event.status === "start") {
          log(`  · ${event.stage}`);
        }
      }
    });
  };

  let stopped = false;
  let watchers = [];
  let stateDbInfo = null;
  // Track the currently-running sync (if any) so that stop()/SIGINT can
  // wait for it to drain instead of yanking the watcher out from under
  // a half-written SQLite transaction.
  let inFlight = null;
  // Counter of consecutive non-busy sync failures. A "busy" SQLite
  // error is normal transient behaviour (Codex has the DB open);
  // anything else (config corruption, codex home moved, disk
  // full, permission denied, ...) would otherwise fire on every
  // config/state event forever. We shut the watcher down after a
  // small threshold so the user gets a clean exit signal instead
  // of a log-spamming daemon.
  let consecutiveNonBusyFailures = 0;
  const MAX_CONSECUTIVE_NON_BUSY_FAILURES = 5;

  const debouncedSync = makeDebouncer(debounceMs, (reason) => {
    if (stopped) {
      return;
    }
    log(`[${new Date().toISOString()}] Detected change (${reason}); running sync...`);
    const task = (async () => {
      try {
        const result = await invokeSync(reason);
        log(`[${new Date().toISOString()}] Sync complete: provider=${result.targetProvider}, rollout_files=${result.changedSessionFiles}, sqlite_rows=${result.sqliteRowsUpdated}${result.skippedLockedRolloutFiles?.length ? `, skipped_locked=${result.skippedLockedRolloutFiles.length}` : ""}`);
        // A successful sync resets the consecutive-failure counter
        // so a transient error followed by recovery does not
        // poison subsequent invocations.
        consecutiveNonBusyFailures = 0;
        if (once) {
          await shutdown("once-mode-complete", task);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        // SQLite being in use is a normal transient condition while Codex
        // is actively writing. Don't crash; just retry on the next event.
        if (/state_5\.sqlite is currently in use/i.test(message)) {
          log(`[${new Date().toISOString()}] Sync skipped: ${message} (will retry on next change)`);
          // Busy is normal — reset the consecutive-failure counter
          // so a long-running Codex session that keeps the DB open
          // for many seconds does not push us toward the auto-shutdown
          // threshold once Codex finally releases the lock.
          consecutiveNonBusyFailures = 0;
        } else {
          log(`[${new Date().toISOString()}] Sync failed: ${message}`);
          // Other errors (config corruption, disk full, codex home
          // moved, permission denied, ...) would otherwise fire on
          // every config/state event forever, hammering the failure
          // surface without ever recovering. Track consecutive
          // non-busy failures and shut the watcher down once we
          // exceed the threshold so the user notices via the
          // `codex-provider watch` exit instead of finding the log
          // spammed at 3am.
          consecutiveNonBusyFailures += 1;
          if (consecutiveNonBusyFailures >= MAX_CONSECUTIVE_NON_BUSY_FAILURES) {
            log(`[${new Date().toISOString()}] Watcher giving up after ${consecutiveNonBusyFailures} consecutive non-busy failures; shutting down. Rerun "codex-provider watch" once the underlying issue is fixed.`);
            await shutdown("consecutive-failures", task);
            return;
          }
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
      // Walk every candidate database, not just the first one. The
      // Codex App keeps both `<home>/sqlite/state_5.sqlite` (newer
      // layout) and `<home>/state_5.sqlite` (legacy root) alive for
      // older project sessions, so watching only the first hit means
      // changes to the other DB never trigger a sync. We register
      // one watcher per existing DB directory and accept events
      // for either basename.
      const candidates = stateDbCandidates(codexHome).filter((c) => existsSync(c.path));
      const watchedDirs = new Set();
      for (const candidate of candidates) {
        const stateDir = path.dirname(candidate.path);
        const stateBase = path.basename(candidate.path);
        const dirExists = await pathExists(stateDir);
        if (!dirExists) {
          log(`[${new Date().toISOString()}] State directory ${stateDir} does not exist yet; skipping watcher`);
          continue;
        }
        if (watchedDirs.has(stateDir)) {
          // Defence-in-depth: the legacy root DB and the new sqlite
          // DB never share a directory, but `stateDbCandidates`
          // could in theory return two entries pointing at the same
          // folder — dedupe so we only register one watcher per dir.
          continue;
        }
        watchedDirs.add(stateDir);
        // Accept the SQLite file plus its WAL/SHM siblings. The
        // basename is taken from the actual state db path so we
        // never match unrelated "state*.sqlite" files that happen
        // to share the dir.
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
      }
      if (candidates.length === 0) {
        log(`[${new Date().toISOString()}] No state database found in ${codexHome}; skipping watcher`);
      }
    } else {
      log(`[${new Date().toISOString()}] No state database found in ${codexHome}; skipping watcher`);
    }
  }

  log(`[${new Date().toISOString()}] Watching ${configPath}${includeStateDb && stateDbInfo?.path ? ` and ${stateDbInfo.path}` : ""} (debounce ${debounceMs}ms${once ? ", once" : ""})`);

  const shutdown = async (reason, currentTask = null) => {
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
    // Skip the caller's own task to avoid self-deadlock when shutdown
    // is invoked from inside the task's catch block (e.g. the
    // consecutive-failure path).
    if (inFlight && inFlight !== currentTask) {
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

  // Wire up an optional AbortSignal so the caller can shut the
// watcher down from anywhere (e.g. an outer SIGINT handler that
// fans out to multiple long-running tasks). We expose the
// pending shutdown promise on the returned handle as
// `signalPromise` so the caller can `await` it from outside
// instead of having to call `stop()` manually. The previous
// implementation only added an abort listener and never awaited
// it, so the signal path was effectively a no-op for callers
// relying on graceful shutdown.
let signalPromise = null;
if (signal) {
    if (signal.aborted) {
      signalPromise = shutdown("signal");
    } else {
      const abortHandler = () => {
        signalPromise = shutdown("signal");
      };
      signal.addEventListener("abort", abortHandler, { once: true });
    }
  }

  return {
    codexHome,
    watchedConfigPath: configPath,
    watchedStateDbPath: stateDbInfo?.path ?? null,
    stop: () => shutdown("external"),
    signalPromise
  };
}
