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
