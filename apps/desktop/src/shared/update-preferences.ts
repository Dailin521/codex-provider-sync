export interface DesktopUpdateReminderInput {
  schemaVersion: 1;
  version: string;
  ignored: boolean;
}

export function isUpdateVersion(value: unknown): value is string {
  return typeof value === "string" && /^[0-9A-Za-z][0-9A-Za-z.+-]{0,63}$/.test(value);
}

export function validateUpdateReminderInput(value: unknown): DesktopUpdateReminderInput {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new TypeError("Invalid update reminder request.");
  const input = value as Record<string, unknown>;
  if (Object.keys(input).sort().join(",") !== "ignored,schemaVersion,version"
      || input.schemaVersion !== 1 || !isUpdateVersion(input.version) || typeof input.ignored !== "boolean") {
    throw new TypeError("Invalid update reminder request.");
  }
  return { schemaVersion: 1, version: input.version, ignored: input.ignored };
}
