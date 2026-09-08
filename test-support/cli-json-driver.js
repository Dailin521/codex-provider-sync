import { CoreError, toCoreErrorDto } from "../src/public-api.js";
import { runCli } from "../src/cli.js";

const scenario = process.env.CODEX_PROVIDER_SYNC_CLI_SCENARIO ?? "completed";

function errorForScenario() {
  if (scenario === "error-secret-details") {
    return new CoreError("OPERATION_BUSY", "The operation is busy.", {
      details: {
        busyScope: "codex-home",
        authToken: "fixture-secret-token",
        messageBody: "fixture secret body"
      }
    });
  }
  const code = scenario.startsWith("error:") ? scenario.slice("error:".length) : null;
  if (!code) return null;
  const details = code === "OPERATION_BUSY"
    ? { busyScope: "codex-home" }
    : code === "LOCK_UNVERIFIABLE"
      ? { lockScope: "state-db" }
      : undefined;
  return new CoreError(code, `${code} fixture`, { details });
}

function completedSyncResult() {
  return {
    targetProvider: "openai",
    codexHome: "C:\\fixture\\.codex",
    sqliteHome: "C:\\fixture\\.codex\\sqlite",
    sqliteHomeSource: "default",
    backupDir: "C:\\fixture\\.codex\\backups_state\\provider-sync\\fixture",
    backupDurationMs: 25,
    changedSessionFiles: 1,
    sqliteRowsUpdated: 1,
    sqlitePresent: true,
    skippedLockedRolloutFiles: scenario === "partial" ? ["locked-rollout.jsonl"] : [],
    autoPruneWarning: scenario === "warning" ? "cleanup warning" : null
  };
}

const core = {
  toCoreErrorDto,
  readConfigText: async () => 'model_provider = "openai"\n',
  readRootModelFromConfigText: () => null,
  getStatus: async () => ({
    schemaVersion: 1,
    currentProvider: "openai",
    codexHome: "C:\\fixture\\.codex"
  }),
  runSync: async ({ onProgress }) => {
    onProgress?.({ stage: "scan_rollout_files", status: "start" });
    onProgress?.({
      stage: "create_backup",
      status: "complete",
      durationMs: 25,
      backupDir: "C:\\fixture\\secret-backup-path"
    });
    const error = errorForScenario();
    if (error) throw error;
    if (scenario === "cyclic-result") {
      const result = {};
      result.self = result;
      return result;
    }
    return completedSyncResult();
  },
  runSwitch: async () => ({
    ...completedSyncResult(),
    modelSync: { applied: true, source: "explicit", model: "fixture-model", warning: null }
  }),
  runPruneBackups: async () => ({
    backupRoot: "C:\\fixture\\backups",
    deletedCount: 0,
    remainingCount: 1,
    freedBytes: 0
  }),
  runRestore: async () => ({
    codexHome: "C:\\fixture\\.codex",
    targetProvider: "openai",
    backupInventoryWarning: scenario === "warning" ? "inventory warning" : null
  })
};

const exitCode = await runCli(process.argv.slice(2), {
  loadCoreImpl: async () => core,
  installWindowsLauncherImpl: async () => ({
    vbsPath: "C:\\fixture\\Codex Provider Sync.vbs",
    cmdPath: "C:\\fixture\\Codex Provider Sync.cmd",
    targetDir: "C:\\fixture",
    codexHome: null,
    sqliteHome: null
  })
});
process.exitCode = exitCode;
