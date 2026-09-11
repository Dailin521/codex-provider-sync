import type {
  CoreRequestEnvelope,
  CoreResponseEnvelope
} from "@codex-provider-sync/contracts";
import type {
  DesktopCoreBridge,
  DesktopReadMethod
} from "@codex-provider-sync/core-client";

import type {
  DesktopDirectorySelectionResult,
  DesktopProfileDeleteInput,
  DesktopProfileDirectoryKind,
  DesktopProfileListResponse,
  DesktopProfileRevealInput,
  DesktopProfileSaveInput,
  DesktopProfileSummary
} from "./profile-types.js";
import type { OperationLogDetailInput, OperationLogEntry, OperationLogListInput, OperationLogListResponse } from "./operation-log-types.js";
import type {
  DesktopDiagnosticsExportInput,
  DesktopDiagnosticsExportResult
} from "./diagnostics-types.js";
import type { DesktopUpdateStatus } from "./update-types.js";
import type { DesktopHistoryRevealInput, DesktopHistoryRevealResult } from "./history-reveal.js";
import type { DesktopClipboardInput, DesktopClipboardResult } from "./clipboard.js";
import type { DesktopWatchStoppedEvent } from "./runtime-protocol.js";

export interface DesktopBridgeApi {
  readonly version: 1;
  readonly clipboard: {
    writeText(input: DesktopClipboardInput): Promise<DesktopClipboardResult>;
  };
  readonly core: DesktopCoreBridge;
  readonly watch: {
    subscribeStopped(listener: (event: DesktopWatchStoppedEvent) => void): () => void;
  };
  readonly profiles: {
    list(): Promise<DesktopProfileListResponse>;
    selectDirectory(kind: DesktopProfileDirectoryKind): Promise<DesktopDirectorySelectionResult>;
    save(input: DesktopProfileSaveInput): Promise<DesktopProfileSummary>;
    delete(input: DesktopProfileDeleteInput): Promise<{ deleted: boolean }>;
    reveal(input: DesktopProfileRevealInput): Promise<{ revealed: boolean }>;
  };
  readonly history: {
    reveal(input: DesktopHistoryRevealInput): Promise<DesktopHistoryRevealResult>;
  };
  readonly operationLogs: {
    list(input: OperationLogListInput): Promise<OperationLogListResponse>;
    get(input: OperationLogDetailInput): Promise<OperationLogEntry | null>;
    dismissPlan(planId: string): Promise<{ dismissed: boolean }>;
  };
  readonly diagnostics: {
    export(input: DesktopDiagnosticsExportInput): Promise<DesktopDiagnosticsExportResult>;
  };
  readonly updates: {
    subscribe(listener: (status: DesktopUpdateStatus) => void): () => void;
    getStatus(): Promise<DesktopUpdateStatus>;
    check(): Promise<DesktopUpdateStatus>;
    download(): Promise<DesktopUpdateStatus>;
    install(): Promise<DesktopUpdateStatus>;
    setReminder(input: import("./update-preferences.js").DesktopUpdateReminderInput): Promise<DesktopUpdateStatus>;
  };
  readonly test?: {
    crashRuntime(): Promise<{ crashed: boolean }>;
    requestRaw(envelope: CoreRequestEnvelope<DesktopReadMethod>): Promise<CoreResponseEnvelope>;
  };
}
