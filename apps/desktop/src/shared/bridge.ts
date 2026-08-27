import type {
  CoreRequestEnvelope,
  CoreResponseEnvelope
} from "@codex-provider-sync/contracts";
import type {
  DesktopCoreBridge,
  DesktopReadMethod
} from "@codex-provider-sync/core-client";

import type { DesktopProfileListResponse } from "./profile-types.js";
import type {
  DesktopDiagnosticsExportInput,
  DesktopDiagnosticsExportResult
} from "./diagnostics-types.js";
import type { DesktopUpdateStatus } from "./update-types.js";

export interface DesktopBridgeApi {
  readonly version: 1;
  readonly core: DesktopCoreBridge;
  readonly profiles: {
    list(): Promise<DesktopProfileListResponse>;
  };
  readonly diagnostics: {
    export(input: DesktopDiagnosticsExportInput): Promise<DesktopDiagnosticsExportResult>;
  };
  readonly updates: {
    getStatus(): Promise<DesktopUpdateStatus>;
    check(): Promise<DesktopUpdateStatus>;
    download(): Promise<DesktopUpdateStatus>;
    install(): Promise<DesktopUpdateStatus>;
  };
  readonly test?: {
    crashRuntime(): Promise<{ crashed: boolean }>;
    requestRaw(envelope: CoreRequestEnvelope<DesktopReadMethod>): Promise<CoreResponseEnvelope>;
  };
}
