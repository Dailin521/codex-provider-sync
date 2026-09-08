import { DEFAULT_BACKUP_RETENTION_COUNT, keepCountSchema } from "../schemas.js";
import type { PreferenceStore } from "../types.js";

/** One installation/browser preference, applied independently to each Home's pool. */
export function readBackupRetention(preferences: PreferenceStore): number {
  try {
    const value = preferences.getBackupRetention?.();
    return keepCountSchema.safeParse(value).success ? value! : DEFAULT_BACKUP_RETENTION_COUNT;
  } catch { return DEFAULT_BACKUP_RETENTION_COUNT; }
}

export function createBackupPreferences(storage: Pick<Storage, "getItem" | "setItem">, namespace: string): Pick<PreferenceStore, "getBackupRetention" | "setBackupRetention"> {
  const key = `${namespace}.backup.retention`;
  return {
    getBackupRetention() {
      const raw = storage.getItem(key);
      if (!raw || !/^\d{1,4}$/.test(raw)) return null;
      const value = Number(raw);
      return keepCountSchema.safeParse(value).success ? value : null;
    },
    setBackupRetention(value) {
      keepCountSchema.parse(value);
      storage.setItem(key, String(value));
    }
  };
}
