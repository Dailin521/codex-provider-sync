import { CORE_ERROR_CODES } from "@codex-provider-sync/contracts";

type Translate = (key: string, options?: Record<string, unknown>) => string;
export const OPERATION_STAGES = new Set([
  "prepare", "validate_plan", "scan", "scan_rollout_files", "check_locked_rollout_files", "create_backup",
  "rewrite_rollout_files", "repair_workspace_roots", "update_sqlite", "update_config", "verify_repair",
  "clean_backups", "create_restore_pre_snapshot", "persist_restore_journal", "apply_restore_targets",
  "commit_restore", "acknowledge_restore_commit", "rollback_restore", "prune", "start", "stop",
  "automatic-sync", "create", "update", "delete", "export", "check", "download", "install", "startup-check"
]);
const FAILURE_CODES = new Set<string>([...CORE_ERROR_CODES, "WRITE_FAILED", "EIO", "EPERM", "EACCES", "EBUSY", "ENOSPC", "ENOENT", "SQLITE_LOCKED", "SQLITE_READONLY", "SQLITE_FULL"]);

export function operationStageLabel(stage: string, t: Translate): string {
  return OPERATION_STAGES.has(stage) ? t(`logs.stages.${stage}`, { defaultValue: t("logs.unknownStage") }) : t("logs.unknownStage");
}

export function operationFailureLabel(code: string, t: Translate): string {
  const knownCode = FAILURE_CODES.has(code) ? code : "INTERNAL_ERROR";
  return `${t(`errors.${knownCode}`, { defaultValue: t("errors.fallback") })} (${knownCode})`;
}
