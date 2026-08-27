export type DesktopUpdateState =
  | "disabled"
  | "idle"
  | "checking"
  | "available"
  | "downloading"
  | "downloaded"
  | "not-available"
  | "error"
  | "installing";

export type DesktopUpdateReason =
  | "not-packaged"
  | "not-configured"
  | "unsupported-target"
  | "check-failed"
  | "download-failed"
  | "install-failed";

export type DesktopUpdateInstallBlockedReason =
  | "write-in-progress"
  | "watch-active"
  | "pending-recovery"
  | "recovery-unverified";

export type DesktopUpdateStatus = {
  schemaVersion: 2;
  state: DesktopUpdateState;
  installAllowed: boolean;
  reason?: DesktopUpdateReason;
  version?: string;
  progressPercent?: number;
  installBlockedReason?: DesktopUpdateInstallBlockedReason;
};
