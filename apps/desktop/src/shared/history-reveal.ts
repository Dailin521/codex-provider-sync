import type { ProfileSelector } from "@codex-provider-sync/contracts";

export interface DesktopHistoryRevealInput {
  schemaVersion: 1;
  profile: ProfileSelector;
  sessionId: string;
}

export interface DesktopHistoryRevealResult {
  revealed: boolean;
}

function validProfileSelector(value: unknown): value is ProfileSelector {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const profile = value as Record<string, unknown>;
  const keys = Object.keys(profile).sort().join(",");
  return (keys === "profileId" || keys === "profileId,profileRevision")
    && typeof profile.profileId === "string"
    && /^[A-Za-z0-9._-]{1,80}$/.test(profile.profileId)
    && (profile.profileRevision === undefined
      || (typeof profile.profileRevision === "string"
        && profile.profileRevision.length > 0
        && profile.profileRevision.length <= 512));
}

export function validateDesktopHistoryRevealInput(value: unknown): DesktopHistoryRevealInput {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Invalid history reveal request.");
  }
  const input = value as Record<string, unknown>;
  if (Object.keys(input).sort().join(",") !== "profile,schemaVersion,sessionId"
      || input.schemaVersion !== 1
      || !validProfileSelector(input.profile)
      || typeof input.sessionId !== "string"
      || input.sessionId.length < 1
      || input.sessionId.length > 512
      || /[\x00-\x1f]/.test(input.sessionId)) {
    throw new TypeError("Invalid history reveal request.");
  }
  return structuredClone(input) as unknown as DesktopHistoryRevealInput;
}

export function validateDesktopHistoryRevealResult(value: unknown): DesktopHistoryRevealResult {
  if (!value || typeof value !== "object" || Array.isArray(value)
      || Object.keys(value).sort().join(",") !== "revealed"
      || typeof (value as { revealed?: unknown }).revealed !== "boolean") {
    throw new TypeError("Invalid history reveal response.");
  }
  return { revealed: (value as { revealed: boolean }).revealed };
}
