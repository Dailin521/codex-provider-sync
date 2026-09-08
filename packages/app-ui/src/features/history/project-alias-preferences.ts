import type { PreferenceStore } from "../../types.js";

/** Host-owned display preferences only. Never persist session summaries or messages. */
export function createProjectAliasPreferences(storage: {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}, namespace: string): Pick<PreferenceStore, "getHistoryProjectAlias" | "setHistoryProjectAlias"> {
  const key = (scope: string, id: string) => {
    if (scope.length > 512 || !/^[a-f0-9]{64}$/.test(id)) throw new Error("Invalid project preference.");
    return `${namespace}.history.project-alias.${JSON.stringify([scope, id])}`;
  };
  const valid = (value: unknown): value is string => typeof value === "string" && value.length <= 160 && !/[\x00-\x1f\x7f]/.test(value);
  return {
    getHistoryProjectAlias(scope, id) {
      try {
        const value = storage.getItem(key(scope, id));
        return valid(value) && value.trim() ? value.trim() : null;
      } catch { return null; }
    },
    setHistoryProjectAlias(scope, id, alias) {
      if (!valid(alias)) throw new Error("Invalid project display name.");
      const storageKey = key(scope, id);
      if (alias.trim()) storage.setItem(storageKey, alias.trim());
      else storage.removeItem(storageKey);
    }
  };
}
