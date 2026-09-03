// @ts-nocheck

import {
  DEFAULT_BACKUP_RETENTION_COUNT,
  DEFAULT_PROVIDER,
  CoreError,
  assertSqliteAccessSupported,
  isConfiguredSqliteHome,
  missingConfiguredStateDbError,
  normalizeCodexHome,
  operationCoordinator,
  codexStorage,
  path
} from "../infrastructure/node-core-ports.js";
import { prepareProviderPlan, executeProviderSyncMutation } from "./provider-sync.js";
import { prepareStorage, emitProgress } from "./runtime-support.js";
import { operationRuntime } from "./runtime-context.js";

const {
  configDeclaresProvider,
  listConfiguredProviderIds,
  readConfigText,
  readProviderModel,
  readRootModelFromConfigText,
  setRootModelInConfigText,
  setRootProviderInConfigText,
  writeConfigText
} = codexStorage.config;

export function buildSwitchIntent(originalConfigText, provider, model, keepRootModel) {
  if (!configDeclaresProvider(originalConfigText, provider)) {
    throw new CoreError(
      "INVALID_INPUT",
      `Provider "${provider}" is not available in config.toml. Configure it first or use one of: ${listConfiguredProviderIds(originalConfigText).join(", ")}`
    );
  }
  if (model !== undefined && model !== null && keepRootModel) {
    throw new CoreError("INVALID_INPUT", "--model and --keep-root-model are mutually exclusive. Pick one.");
  }

  let nextConfigText = setRootProviderInConfigText(originalConfigText, provider);
  let modelSync = { applied: false, source: "none", model: null, warning: null };
  if (model !== undefined && model !== null) {
    if (typeof model !== "string" || model.length === 0) {
      throw new CoreError(
        "INVALID_INPUT",
        `Invalid --model value: ${model}. Expected a non-empty string.`
      );
    }
    nextConfigText = setRootModelInConfigText(nextConfigText, model);
    modelSync = { applied: true, source: "explicit", model, warning: null };
  } else if (!keepRootModel) {
    const providerModel = readProviderModel(originalConfigText, provider);
    if (providerModel) {
      nextConfigText = setRootModelInConfigText(nextConfigText, providerModel);
      modelSync = { applied: true, source: "provider-section", model: providerModel, warning: null };
    } else if (provider !== DEFAULT_PROVIDER) {
      modelSync = {
        applied: false,
        source: "none",
        model: null,
        warning: `Provider "${provider}" has no model field in [model_providers.${provider}]; root-level model left unchanged. Use --model <name> to set it explicitly, or --keep-root-model to suppress this warning.`
      };
    }
  }
  const rootModel = modelSync.applied && modelSync.model
    ? modelSync.model
    : readRootModelFromConfigText(nextConfigText);
  return { nextConfigText, modelSync, rootModel };
}

export async function executeProviderSwitch({
  codexHome: explicitCodexHome,
  sqliteHome,
  storage: providedStorage,
  expectedConfigText,
  provider,
  model,
  keepRootModel = false,
  keepCount = DEFAULT_BACKUP_RETENTION_COUNT,
  onProgress,
  platform,
  faultInjector,
  signal,
  expectedPlanState
}) {
  if (!provider) {
    throw new CoreError(
      "INVALID_INPUT",
      "Missing provider id. Usage: codex-provider switch <provider-id>"
    );
  }

  const codexHome = providedStorage?.codexHome ?? normalizeCodexHome(explicitCodexHome);
  const configPath = path.join(codexHome, "config.toml");
  const originalConfigText = await readConfigText(configPath);
  if (expectedConfigText !== undefined && originalConfigText !== expectedConfigText) {
    throw new CoreError(
      "PLAN_STALE",
      "config.toml changed after the operation was confirmed. Refresh and retry."
    );
  }
  const storage = await prepareStorage({ codexHome, sqliteHome, configText: originalConfigText, storage: providedStorage, platform });
  assertSqliteAccessSupported(storage, "switch");
  if (!storage.stateDbLocation && isConfiguredSqliteHome(storage)) {
    throw missingConfiguredStateDbError(storage);
  }
  await faultInjector?.({
    point: "after_switch_storage_preflight",
    stateDbPath: storage.stateDbLocation?.path ?? null
  });
  const { nextConfigText, modelSync } = buildSwitchIntent(
    originalConfigText,
    provider,
    model,
    keepRootModel
  );
  const syncResult = await executeProviderSyncMutation(
    {
      codexHome,
      sqliteHome,
      keepCount,
      onProgress,
      faultInjector,
      signal,
      expectedPlanState,
      platform
    },
    {
      targetProvider: provider,
      configBackupText: originalConfigText,
      configMutationExpected: nextConfigText !== originalConfigText,
      afterBackup: async () => {
        emitProgress(onProgress, {
          stage: "update_config",
          status: "start",
          provider
        });
        await writeConfigText(configPath, nextConfigText);
        emitProgress(onProgress, {
          stage: "update_config",
          status: "complete",
          provider
        });
      }
    }
  );
  return {
    ...syncResult,
    configUpdated: nextConfigText !== originalConfigText,
    modelSync
  };
}


export async function prepareSwitch(options = {}) {
  if (!options.provider) {
    throw new CoreError("INVALID_INPUT", "Missing provider id. Usage: codex-provider switch <provider-id>");
  }
  const codexHome = options.storage?.codexHome ?? normalizeCodexHome(options.codexHome);
  if (operationCoordinator.isActive(codexHome, options.platform)) {
    throw new CoreError("OPERATION_BUSY", "Lock already exists for this Codex Home; another write operation is active.", {
      details: { busyScope: "codex-home" }
    });
  }
  const configText = await readConfigText(path.join(codexHome, "config.toml"));
  if (options.expectedConfigText !== undefined && configText !== options.expectedConfigText) {
    throw new CoreError("PLAN_STALE", "config.toml changed after the operation was confirmed. Refresh and retry.");
  }
  for (const removed of ["fast", "syncMode"]) {
    if (Object.hasOwn(options, removed)) {
      throw new CoreError("INVALID_INPUT", `prepareSwitch no longer accepts ${removed}.`);
    }
  }
  const keepRootModel = Boolean(options.keepRootModel);
  const intent = buildSwitchIntent(configText, options.provider, options.model, keepRootModel);
  return prepareProviderPlan("switch", { ...options, keepRootModel }, {
    provider: options.provider,
    rootModel: intent.rootModel,
    modelSync: intent.modelSync,
    configMutationExpected: intent.nextConfigText !== configText,
    modelMode: options.model !== undefined && options.model !== null
      ? "explicit"
      : (keepRootModel ? "keep-root-model" : "provider-default")
  });
}


export async function applySwitch(input, control) {
  return operationRuntime.applyPrepared(input, "switch", (options) => executeProviderSwitch(options), control);
}

export function createProviderSwitchUseCase() {
  return Object.freeze({ prepareSwitch, applySwitch });
}
