import fs from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { isUpdateVersion } from "../shared/update-preferences.js";

/** A Host preference only: no Codex data, network or updater side effects. */
export class UpdateReminderStore {
  readonly #file: string;
  constructor(userData: string) {
    this.#file = path.join(userData, "update-reminder.json");
  }

  async load(): Promise<string | null> {
    try {
      const file = await fs.open(this.#file, "r");
      try {
        const bytes = Buffer.alloc(512);
        const { bytesRead } = await file.read(bytes, 0, bytes.length, 0);
        if (bytesRead === bytes.length) return null;
        const value = JSON.parse(bytes.subarray(0, bytesRead).toString("utf8"));
        return value?.schemaVersion === 1 && isUpdateVersion(value.ignoredVersion) ? value.ignoredVersion : null;
      } finally { await file.close(); }
    } catch { return null; }
  }

  async save(ignoredVersion: string | null): Promise<void> {
    if (ignoredVersion !== null && !isUpdateVersion(ignoredVersion)) throw new TypeError("Invalid update version.");
    const temp = `${this.#file}.${randomUUID()}.tmp`;
    try {
      await fs.mkdir(path.dirname(this.#file), { recursive: true });
      const file = await fs.open(temp, "wx", 0o600);
      try {
        await file.writeFile(JSON.stringify({ schemaVersion: 1, ignoredVersion }));
        await file.sync();
      } finally { await file.close(); }
      await fs.rename(temp, this.#file);
    } finally { await fs.rm(temp, { force: true }).catch(() => {}); }
  }
}
