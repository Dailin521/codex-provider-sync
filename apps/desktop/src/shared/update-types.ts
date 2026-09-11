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
  | "not-authorized"
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
  currentVersion?: string;
  mode?: "manual";
  schemaVersion: 2;
  state: DesktopUpdateState;
  installAllowed: boolean;
  reason?: DesktopUpdateReason;
  version?: string;
  reminderIgnored?: boolean;
  progressPercent?: number;
  installBlockedReason?: DesktopUpdateInstallBlockedReason;
};
