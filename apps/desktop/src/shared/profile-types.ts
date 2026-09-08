export interface TrustedDesktopProfile {
  id: string;
  name: string;
  revision: string;
  codexHome: string;
  sqliteHome?: string;
}

export interface DesktopProfileSummary {
  id: string;
  name: string;
  revision: string;
  codexHomeConfigured: boolean;
  sqliteHomeConfigured: boolean;
}

export interface DesktopProfileListResponse {
  schemaVersion: 1;
  profiles: DesktopProfileSummary[];
}

export type DesktopProfileDirectoryKind = "codex-home" | "sqlite-home";

export type DesktopDirectorySelectionResult =
  | { schemaVersion: 1; status: "cancelled" }
  | { schemaVersion: 1; status: "selected"; token: string; displayName: string };

export interface DesktopProfileSaveInput {
  schemaVersion: 1;
  name: string;
  profileId?: string;
  profileRevision?: string;
  codexHomeSelectionToken?: string;
  sqliteHomeMode: "preserve" | "inherit" | "selected";
  sqliteHomeSelectionToken?: string;
}

export interface DesktopProfileDeleteInput {
  schemaVersion: 1;
  profileId: string;
  profileRevision: string;
}

export interface DesktopProfileRevealInput extends DesktopProfileDeleteInput {
  target: "codex-home" | "sqlite-home";
}
