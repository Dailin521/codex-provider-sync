import { describe, expect, it } from "vitest";
import { createProjectAliasPreferences } from "../src/features/history/project-alias-preferences.js";

describe("project display preferences", () => {
  it("persists only a bounded alias and isolates profiles, revisions, projects and hosts", () => {
    const values = new Map<string, string>();
    const storage = { getItem: (key: string) => values.get(key) ?? null, setItem: (key: string, value: string) => { values.set(key, value); }, removeItem: (key: string) => { values.delete(key); } };
    const preferences = createProjectAliasPreferences(storage, "desktop");
    const a = "a".repeat(64), b = "b".repeat(64);
    preferences.setHistoryProjectAlias!("profile:r1", a, " My project ");
    expect(preferences.getHistoryProjectAlias!("profile:r1", a)).toBe("My project");
    expect(createProjectAliasPreferences(storage, "desktop").getHistoryProjectAlias!("profile:r1", a)).toBe("My project");
    expect(preferences.getHistoryProjectAlias!("profile:r2", a)).toBeNull();
    expect(preferences.getHistoryProjectAlias!("other:r1", a)).toBeNull();
    expect(preferences.getHistoryProjectAlias!("profile:r1", b)).toBeNull();
    expect(createProjectAliasPreferences(storage, "web").getHistoryProjectAlias!("profile:r1", a)).toBeNull();
    expect([...values.values()]).toEqual(["My project"]);
    preferences.setHistoryProjectAlias!("profile:r1", a, "");
    expect(values.size).toBe(0);
  });

  it("rejects invalid display names and never pretends a failed write succeeded", () => {
    const preferences = createProjectAliasPreferences({ getItem: () => "bad\nname", setItem: () => { throw new Error("storage denied"); }, removeItem: () => { throw new Error("storage denied"); } }, "desktop");
    const id = "a".repeat(64);
    expect(preferences.getHistoryProjectAlias!("p", id)).toBeNull();
    expect(() => preferences.setHistoryProjectAlias!("p", id, "bad\nname")).toThrow();
    expect(() => preferences.setHistoryProjectAlias!("p", id, "x".repeat(161))).toThrow();
    expect(() => preferences.setHistoryProjectAlias!("p", "C:\\raw-path", "name")).toThrow();
    expect(() => preferences.setHistoryProjectAlias!("p", id, "valid")).toThrow("storage denied");
    expect(() => preferences.setHistoryProjectAlias!("p", id, "")).toThrow("storage denied");
  });
});
