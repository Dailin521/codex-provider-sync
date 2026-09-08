import type { CoreRuntimeSupervisor } from "./runtime-supervisor.js";

export type DesktopUpdateUnavailableReason =
  | "not-packaged"
  | "not-authorized"
  | "not-configured"
  | "unsupported-target";

export type DesktopInstallBlockedReason =
  | "write-in-progress"
  | "watch-active"
  | "pending-recovery"
  | "recovery-unverified";

export interface DesktopUpdateAvailabilityInput {
  isPackaged: boolean;
  platform: NodeJS.Platform;
  arch: string;
  releaseAuthorized: boolean;
  configured: boolean;
}

export interface DesktopInstallGateInput {
  supervisor: Pick<CoreRuntimeSupervisor, "snapshot">;
  hasActiveWatches: boolean;
  recoveryVerified: boolean;
}

export function supportedUpdateTarget(platform: NodeJS.Platform, arch: string): boolean {
  return (platform === "win32" && arch === "x64")
    || (platform === "darwin" && (arch === "x64" || arch === "arm64"))
    || (platform === "linux" && arch === "x64");
}

export function getDesktopUpdateUnavailableReason(
  input: DesktopUpdateAvailabilityInput
): DesktopUpdateUnavailableReason | null {
  if (!input.isPackaged) return "not-packaged";
  if (!supportedUpdateTarget(input.platform, input.arch)) return "unsupported-target";
  if (!input.releaseAuthorized) return "not-authorized";
  if (!input.configured) return "not-configured";
  return null;
}

export function getDesktopInstallBlockedReason(
  input: DesktopInstallGateInput
): DesktopInstallBlockedReason | null {
  if (input.supervisor.snapshot.recoveryBlocked) {
    return "pending-recovery";
  }
  if (input.supervisor.snapshot.writeInProgress) {
    return "write-in-progress";
  }
  if (input.hasActiveWatches) return "watch-active";
  if (!input.recoveryVerified) return "recovery-unverified";
  return null;
}
