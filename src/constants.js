import os from "node:os";
import path from "node:path";

export const DEFAULT_PROVIDER = "openai";
export const DEFAULT_LOCK_NAME = "provider-sync.lock";
export const BACKUP_NAMESPACE = "provider-sync";
export const DB_FILE_BASENAME = "state_5.sqlite";
export const DEFAULT_BACKUP_RETENTION_COUNT = 5;
export const SESSION_DIRS = ["sessions", "archived_sessions"];

export function defaultCodexHome() {
  return path.join(os.homedir(), ".codex");
}

export function defaultBackupRoot(codexHome) {
  return path.join(codexHome, "backups_state", BACKUP_NAMESPACE);
}
