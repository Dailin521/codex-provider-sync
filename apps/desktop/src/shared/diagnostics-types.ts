import type { ProfileSelector } from "@codex-provider-sync/contracts";

export interface DesktopDiagnosticsExportInput {
  schemaVersion: 1;
  profile: ProfileSelector;
}

export type DesktopDiagnosticsExportResult =
  | {
      schemaVersion: 1;
      status: "created";
      artifactId: string;
      createdAt: string;
    }
  | {
      schemaVersion: 1;
      status: "cancelled";
    }
  | {
      schemaVersion: 1;
      status: "failed";
      reason: "runtime-unavailable" | "invalid-snapshot" | "write-failed";
    };
