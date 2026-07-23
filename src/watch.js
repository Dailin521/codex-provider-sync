// Watch daemon: monitor ~/.codex/config.toml and the Codex state database
// (including its WAL sidecar) for external changes and run a sync whenever
// the active provider goes out of sync. This is the "auto resync" companion
// to `codex-provider sync`.
//
// Usage:
//   codex-provider watch [--codex-home PATH] [--debounce-ms N] [--once] [--no-state-db]
//
// --once    : exit after the first successful sync (or after debounce settles).
//             Useful for one-shot automation without keeping a process around.
// --no-state-db : only watch config.toml, ignore SQLite state events.

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

import { defaultCodexHome } from "./constants.js";
import { detectStateDb } from "./sqlite-state.js";
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
    // We only consider the top-level (root) `model = "..."` line — anything
    // inside a `[model_providers.*]` section must be ignored, otherwise a
    // user who has a `model = "..."` entry in a provider section will see
    // the wrong model propagated to every rollout file.
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

  // A promise that resolves when the watcher has finished its
  // internal shutdown. The CLI uses this to await exit so the
  // process terminates cleanly after `--once` completes or after
  // the consecutive-failure auto-shutdown, instead of sitting in
  // the event loop waiting for SIGINT/SIGTERM that will never
  // arrive. External callers (e.g. tests) can also `await` it.
  let donePromise;
  let resolveDone;
  donePromise = new Promise((resolve) => {
    resolveDone = resolve;
  });

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
      // Watch the active database *and* its WAL sidecar. SQLite by
      // default runs in WAL journal mode: new transactions are
      // appended to `state_5.sqlite-wal` and only checkpointed back
      // to the main file on a checkpoint, so watching the main
      // file alone would miss every write Codex makes between
      // checkpoints. We watch both, plus the -shm shared-memory
      // file so a checkpoint still fires the change event for
      // long-running sessions.
      const stateDbFile = stateDbInfo.path;
      const walFile = `${stateDbFile}-wal`;
      const shmFile = `${stateDbFile}-shm`;
      for (const target of [stateDbFile, walFile, shmFile]) {
        // Watch each file directly instead of its parent
        // directory. Watching a directory on Windows enters a
        // libuv path that asserts in src/win/fs-event.c around
        // line 72 when any sibling under the directory is renamed
        // during startup (a race condition Node 22 and 24 started
        // hitting reliably on Windows runners). Watching a single
        // file bypasses that path entirely. The downside is that
        // SQLite's atomic-rename of the database (or its WAL)
        // can leave us listening on a stale handle; we re-attach
        // the watcher on any `rename` event so the next write
        // burst still reaches us.
        attachStateWatcher(target, target === stateDbFile ? "state_db" : "state_db-wal");
      }
    } else {
      log(`[${new Date().toISOString()}] No state database found in ${codexHome}; skipping watcher`);
    }
  }

  log(`[${new Date().toISOString()}] Watching ${configPath}${includeStateDb && stateDbInfo?.path ? `, ${stateDbInfo.path}, ${stateDbInfo.path}-wal, ${stateDbInfo.path}-shm` : ""} (debounce ${debounceMs}ms${once ? ", once" : ""})`);

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
    // out from under a half-written SQLite transaction or backup. Skip
    // the caller's own task to avoid self-deadlock when shutdown is
    // invoked from inside the task's catch block (e.g. the
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
      try {
        await onShutdown(reason);
      } catch {
        // shutdown callbacks must not block the daemon from exiting
      }
    }
    if (resolveDone) {
      resolveDone(reason);
      resolveDone = null;
    }
  };

  // Wire up an optional AbortSignal so the caller can shut the
  // watcher down from anywhere (e.g. an outer SIGINT handler that
  // fans out to multiple long-running tasks). We expose the
  // pending shutdown promise on the returned handle as
  // `signalPromise` so the caller can `await` it from outside
  // instead of having to call `stop()` manually.
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

  function attachStateWatcher(stateDbFile, reasonLabel) {
    // Attach (or re-attach after a rename) a single-file fs.watch
    // for the active SQLite database (or its WAL/SHM sidecar).
    // Returns when the watcher is attached so we can drive startup
    // synchronously.
    let current = null;
    const tryAttach = () => {
      if (stopped || current !== null) {
        return;
      }
      // WAL and SHM sidecars may not exist when the watcher starts.
      // Keep trying quietly so a later Codex launch is still observed.
      if (!fs.existsSync(stateDbFile)) {
        setTimeout(tryAttach, 250);
        return;
      }
      let watcher;
      try {
        watcher = fs.watch(stateDbFile, { persistent: true }, (eventType, filename) => {
          if (stopped) {
            return;
          }
          if (eventType === "rename") {
            // SQLite may have rotated the file (atomic-rename)
            // or deleted it. Drop the stale handle and re-attach
            // once the file is back so the next write fires again.
            try {
              watcher.close();
            } catch {
              // best-effort
            }
            current = null;
            const idx = watchers.indexOf(watcher);
            if (idx !== -1) {
              watchers.splice(idx, 1);
            }
            setTimeout(tryAttach, 50);
            return;
          }
          // Either "change" or null eventType is enough to fire
          // a sync. We deliberately do NOT filter on filename: when
          // watching a single file the only events we get are
          // about that file, and Node sometimes reports null on
          // Windows whenever the underlying FILE_OBJECT is
          // re-opened. Either way a sync should run.
          void filename;
          log(`[${new Date().toISOString()}] ${reasonLabel} change${filename ? `:${filename}` : ""}`);
          debouncedSync(reasonLabel);
        });
      } catch (error) {
        // Path may have gone away. Wait a moment and try again;
        // the next config.toml change restarts the whole watcher
        // so this is the only fallback we need.
        log(`[${new Date().toISOString()}] Could not watch ${stateDbFile}: ${error instanceof Error ? error.message : String(error)}; will retry`);
        setTimeout(tryAttach, 250);
        return;
      }
      watchers.push(watcher);
      current = watcher;
    };
    tryAttach();
  }

  return {
    codexHome,
    watchedConfigPath: configPath,
    watchedStateDbPath: stateDbInfo?.path ?? null,
    stop: () => shutdown("external"),
    signalPromise,
    done: donePromise
  };
}
