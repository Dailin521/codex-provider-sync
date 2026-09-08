import { MAX_DESKTOP_IPC_BYTES } from "./constants.js";

export interface DesktopClipboardInput { schemaVersion: 1; text: string; }
export interface DesktopClipboardResult { copied: boolean; }

export function validateDesktopClipboardInput(value: unknown): DesktopClipboardInput {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Invalid clipboard request.");
  }
  const input = value as Record<string, unknown>;
  if (Object.keys(input).sort().join(",") !== "schemaVersion,text"
      || input.schemaVersion !== 1 || typeof input.text !== "string"
      || input.text.length > MAX_DESKTOP_IPC_BYTES
      || input.text.includes("\0")
      || new TextEncoder().encode(JSON.stringify(input)).byteLength > MAX_DESKTOP_IPC_BYTES) {
    throw new TypeError("Invalid clipboard request.");
  }
  return { schemaVersion: 1, text: input.text };
}

export function validateDesktopClipboardResult(value: unknown): DesktopClipboardResult {
  if (!value || typeof value !== "object" || Array.isArray(value)
      || Object.keys(value).join(",") !== "copied"
      || typeof (value as DesktopClipboardResult).copied !== "boolean") {
    throw new TypeError("Invalid clipboard response.");
  }
  return { copied: (value as DesktopClipboardResult).copied };
}
